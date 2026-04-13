"""
owner_state_sync.py

Owner-side state model, suppression policy, terminal success persistence,
owner-state diagnostics shaping.

Извлечено из reports_processor.py (Phase 2.2 — medium-risk extraction).

Dependency Injection:
    Модуль поддерживает injection кастомной storage реализации через
    параметр `store` в функциях. Если не передан — используется default
    Google Sheets store для обратной совместимости.
"""
from datetime import datetime
import random
import time
from typing import Optional

from .storage.failover_state import (
    STATUS_CLAIM_EXPIRED,
    STATUS_FAILOVER_FAILED,
    STATUS_FAILOVER_SUCCESS,
    STATUS_OWNER_FAILED,
    STATUS_OWNER_SUCCESS,
    build_failover_state_record,
    create_failover_state_logger,
)
from .storage.failover_state_protocol import FailoverStateStore

# Извлечённые helpers из reports_summary (Phase 1.1)
from .reports_summary import extract_batch_failures
from .config.scripts.reports_processor_config import BACKFILL_CONFIG

# ── Retry helpers for KPI_FAILOVER_STATE reads ──

_RETRYABLE_MARKERS = (
    "[429]",
    "quota exceeded",
    "[503]",
    "service is currently unavailable",
    "temporarily unavailable",
    "timeout",
    "timed out",
)


def _is_retryable_failover_state_error(error_text: str) -> bool:
    """Проверяет, является ли ошибка retryable для KPI_FAILOVER_STATE."""
    normalized = str(error_text or "").lower()
    return any(marker in normalized for marker in _RETRYABLE_MARKERS)


def _get_rows_by_keys_with_retry(
    *,
    store: FailoverStateStore,
    keys: list[dict[str, str]],
    logger,
    max_attempts: int = 3,
    base_delay: float = 2.0,
    max_delay: float = 8.0,
    jitter: float = 1.0,
) -> dict[tuple, dict[str, any]]:
    """Batch-read из KPI_FAILOVER_STATE с retry + jitter.

    Retry только для retryable ошибок (429, 503, timeout).
    Non-retryable ошибки пробрасываются сразу.
    """
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            return store.get_rows_by_keys(keys=keys)
        except Exception as exc:
            last_error = exc
            if not _is_retryable_failover_state_error(str(exc)):
                raise
            if attempt >= max_attempts:
                break
            delay = min(base_delay * (2 ** (attempt - 1)), max_delay) + random.uniform(-jitter, jitter)
            delay = max(delay, 0.5)
            logger.warning(
                f"Retryable error при owner state prefetch: {exc}; "
                f"attempt={attempt}/{max_attempts}, retry в {delay:.1f}s"
            )
            time.sleep(delay)
    raise RuntimeError(
        f"Не удалось прочитать KPI_FAILOVER_STATE после {max_attempts} попыток: {last_error}"
    ) from last_error

# ── Dependency Injection helpers ──

_default_store_instance: Optional[FailoverStateStore] = None


def set_default_store(store: FailoverStateStore) -> None:
    """Установить default storage для всего модуля."""
    global _default_store_instance
    _default_store_instance = store


def get_default_store() -> FailoverStateStore:
    """Получить текущий default storage."""
    global _default_store_instance
    if _default_store_instance is None:
        from .storage.failover_state import get_default_store as _get_gs_store
        _default_store_instance = _get_gs_store()
    return _default_store_instance


def _resolve_store(store: Optional[FailoverStateStore]) -> FailoverStateStore:
    """Resolve store: injected или default."""
    return store if store is not None else get_default_store()


def mark_dates_with_owner_status(execution_dates, owner_object_name, status, logger=None, source_run_id="", store: Optional[FailoverStateStore] = None):
    """Помечает даты в KPI_FAILOVER_STATE статусом owner."""
    resolved_store = _resolve_store(store)
    logger = logger or create_failover_state_logger()
    results = []
    for execution_date in execution_dates or []:
        results.append(
            resolved_store.mark_state(
                execution_date=execution_date,
                target_object_name=owner_object_name,
                owner_object_name=owner_object_name,
                status=status,
                source_run_id=source_run_id,
            )
        )
    return results


def classify_owner_success_history(existing_state_row):
    """Классифицирует history owner state для принятия решения о persist success.

    Returns:
        dict с ключами: classification, status, should_persist_success_if_enabled
    """
    if not existing_state_row:
        return {
            "classification": "no_state",
            "status": "",
            "should_persist_success_if_enabled": False,
        }

    status = str(existing_state_row.get("status", "") or "").strip()
    if status in {
        STATUS_OWNER_FAILED,
        STATUS_CLAIM_EXPIRED,
        STATUS_FAILOVER_FAILED,
        "failover_claimed",
        STATUS_FAILOVER_SUCCESS,
    }:
        return {
            "classification": "incident_related",
            "status": status,
            "should_persist_success_if_enabled": True,
        }

    if status == STATUS_OWNER_SUCCESS:
        return {
            "classification": "terminal_success_only",
            "status": status,
            "should_persist_success_if_enabled": False,
        }

    return {
        "classification": "other",
        "status": status,
        "should_persist_success_if_enabled": False,
    }


def should_persist_owner_success_from_history(existing_state_row):
    """Решает, нужно ли persist owner_success для данной даты.

    Healthy-new success -> suppress.
    Prior incident-related state -> persist owner_success.
    Duplicate prior owner_success -> suppress duplicate rewrite.

    Returns:
        dict с ключами: persisted, classification, reason, status
    """
    classification = classify_owner_success_history(existing_state_row)
    classification_name = classification["classification"]
    if classification_name == "incident_related":
        return {
            "persisted": True,
            "classification": classification_name,
            "reason": "incident_history_present",
            "status": classification["status"],
        }
    if classification_name == "no_state":
        return {
            "persisted": False,
            "classification": classification_name,
            "reason": "healthy_new_success",
            "status": classification["status"],
        }
    if classification_name == "terminal_success_only":
        return {
            "persisted": False,
            "classification": classification_name,
            "reason": "already_terminal_success",
            "status": classification["status"],
        }
    return {
        "persisted": True,
        "classification": classification_name,
        "reason": "unexpected_prior_state_conservative_persist",
        "status": classification["status"],
    }


def build_owner_final_failover_state_records(
    *,
    owner_object_name,
    missing_dates,
    batch_result,
    upload_result=None,
    source_run_id="",
    existing_state_rows_by_date=None,
):
    """Строит финальные failover state records для owner после parse+upload.

    - failed_dates → STATUS_OWNER_FAILED с error
    - successful_dates → STATUS_OWNER_SUCCESS (если persist decision позволяет)
    - healthy-new success → suppressed

    Returns:
        dict с ключами: records, successful_dates, failed_dates,
        suppressed_success_dates, success_persistence_by_date
    """
    upload_result_provided = upload_result is not None
    upload_result = upload_result or {}
    failed_by_date = extract_batch_failures(batch_result)
    successful_dates = []
    failed_dates = []
    suppressed_success_dates = []
    success_persistence_by_date = {}
    records = []
    upload_success = (not upload_result_provided) or bool(upload_result.get("success", False))
    upload_error = str(upload_result.get("error", "") or "upload_failed")
    existing_state_rows_by_date = existing_state_rows_by_date or {}

    for execution_date in missing_dates or []:
        if execution_date in failed_by_date:
            failed_dates.append(execution_date)
            records.append(
                build_failover_state_record(
                    execution_date=execution_date,
                    target_object_name=owner_object_name,
                    owner_object_name=owner_object_name,
                    status=STATUS_OWNER_FAILED,
                    source_run_id=source_run_id,
                    last_error=failed_by_date[execution_date],
                )
            )
            success_persistence_by_date[execution_date] = {
                "persisted": True,
                "classification": "forced_failure_write",
                "reason": "owner_parse_failed",
                "status": STATUS_OWNER_FAILED,
            }
            continue

        if not upload_success:
            failed_dates.append(execution_date)
            records.append(
                build_failover_state_record(
                    execution_date=execution_date,
                    target_object_name=owner_object_name,
                    owner_object_name=owner_object_name,
                    status=STATUS_OWNER_FAILED,
                    source_run_id=source_run_id,
                    last_error=upload_error,
                )
            )
            success_persistence_by_date[execution_date] = {
                "persisted": True,
                "classification": "forced_failure_write",
                "reason": "owner_upload_failed",
                "status": STATUS_OWNER_FAILED,
            }
            continue

        successful_dates.append(execution_date)
        persist_decision = should_persist_owner_success_from_history(existing_state_rows_by_date.get(execution_date))
        success_persistence_by_date[execution_date] = persist_decision
        if persist_decision["persisted"]:
            records.append(
                build_failover_state_record(
                    execution_date=execution_date,
                    target_object_name=owner_object_name,
                    owner_object_name=owner_object_name,
                    status=STATUS_OWNER_SUCCESS,
                    source_run_id=source_run_id,
                )
            )
        else:
            suppressed_success_dates.append(execution_date)

    return {
        "records": records,
        "successful_dates": successful_dates,
        "failed_dates": failed_dates,
        "suppressed_success_dates": suppressed_success_dates,
        "success_persistence_by_date": success_persistence_by_date,
    }


def sync_owner_failover_state_from_batch_result(
    *,
    owner_object_name,
    missing_dates,
    batch_result,
    upload_result=None,
    logger=None,
    source_run_id="",
    store: Optional[FailoverStateStore] = None,
):
    """Полный цикл owner-state sync: prefetch → classify → build → upsert.

    Args:
        store: Optional FailoverStateStore для DI. Если None — default Google Sheets.

    Returns:
        dict с ключами: successful_dates, failed_dates, suppressed_success_dates,
        success_persistence_by_date, persisted_rows_count,
        existing_state_prefetch_keys_count, existing_state_prefetch_rows_found,
        upsert_diagnostics, results
    """
    resolved_store = _resolve_store(store)
    logger = logger or create_failover_state_logger()
    keys_to_read = [
        {"work_date": execution_date, "target_object_name": owner_object_name}
        for execution_date in (missing_dates or [])
    ]
    existing_rows = _get_rows_by_keys_with_retry(
        store=resolved_store,
        keys=keys_to_read,
        logger=logger,
        max_attempts=int(BACKFILL_CONFIG.get("owner_state_sync_max_attempts", 3) or 3),
        base_delay=float(BACKFILL_CONFIG.get("owner_state_sync_base_delay_seconds", 2.0) or 2.0),
        max_delay=float(BACKFILL_CONFIG.get("owner_state_sync_max_delay_seconds", 8.0) or 8.0),
        jitter=float(BACKFILL_CONFIG.get("owner_state_sync_jitter_seconds", 1.0) or 1.0),
    )
    existing_state_rows_by_date = {}
    for (normalized_date, _normalized_target), row in existing_rows.items():
        raw_date = str(row.get("work_date", "") or "").strip()
        if raw_date:
            try:
                normalized_runtime_date = datetime.strptime(raw_date, "%d.%m.%Y").strftime("%Y-%m-%d")
            except ValueError:
                normalized_runtime_date = raw_date
            existing_state_rows_by_date[normalized_runtime_date] = row

    built_result = build_owner_final_failover_state_records(
        owner_object_name=owner_object_name,
        missing_dates=missing_dates,
        batch_result=batch_result,
        upload_result=upload_result,
        source_run_id=source_run_id,
        existing_state_rows_by_date=existing_state_rows_by_date,
    )
    upsert_result = {"success": True, "results": []}
    if built_result["records"]:
        upsert_result = resolved_store.upsert_records(built_result["records"])
        if not upsert_result.get("success", False):
            raise RuntimeError("Не удалось синхронизировать owner final statuses в KPI_FAILOVER_STATE")

    return {
        "successful_dates": built_result["successful_dates"],
        "failed_dates": built_result["failed_dates"],
        "suppressed_success_dates": built_result["suppressed_success_dates"],
        "success_persistence_by_date": built_result["success_persistence_by_date"],
        "persisted_rows_count": len(built_result["records"]),
        "existing_state_prefetch_keys_count": len(missing_dates or []),
        "existing_state_prefetch_rows_found": len(existing_rows),
        "upsert_diagnostics": upsert_result.get("diagnostics", {}),
        "results": upsert_result.get("results", []),
    }

