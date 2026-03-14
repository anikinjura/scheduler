from __future__ import annotations

import json
import urllib.request
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, Optional

from scheduler_runner.tasks.reports.config.reports_paths import REPORTS_PATHS
from scheduler_runner.tasks.reports.config.scripts.kpi_failover_state_google_sheets_config import (
    KPI_FAILOVER_STATE_GOOGLE_SHEETS_CONFIG,
)
from scheduler_runner.tasks.reports.config.scripts.reports_processor_config import BACKFILL_CONFIG
from scheduler_runner.utils.logging import TRACE_LEVEL, configure_logger
from scheduler_runner.utils.uploader.implementations.google_sheets_uploader import GoogleSheetsUploader


STATUS_OWNER_PENDING = "owner_pending"
STATUS_OWNER_SUCCESS = "owner_success"
STATUS_OWNER_FAILED = "owner_failed"
STATUS_FAILOVER_CLAIMED = "failover_claimed"
STATUS_FAILOVER_SUCCESS = "failover_success"
STATUS_FAILOVER_FAILED = "failover_failed"
STATUS_CLAIM_EXPIRED = "claim_expired"

TERMINAL_STATUSES = {STATUS_OWNER_SUCCESS, STATUS_FAILOVER_SUCCESS}


def create_failover_state_logger():
    return configure_logger(
        user="reports_domain",
        task_name="FailoverState",
        log_levels=[TRACE_LEVEL],
        single_file_for_levels=False,
    )


def prepare_failover_state_connection_params() -> Dict[str, Any]:
    return {
        "CREDENTIALS_PATH": str(REPORTS_PATHS["GOOGLE_SHEETS_CREDENTIALS"]),
        "SPREADSHEET_ID": KPI_FAILOVER_STATE_GOOGLE_SHEETS_CONFIG["SPREADSHEET_ID"],
        "WORKSHEET_NAME": KPI_FAILOVER_STATE_GOOGLE_SHEETS_CONFIG["WORKSHEET_NAME"],
        "TABLE_CONFIG": deepcopy(KPI_FAILOVER_STATE_GOOGLE_SHEETS_CONFIG["TABLE_CONFIG"]),
        "REQUIRED_CONNECTION_PARAMS": list(KPI_FAILOVER_STATE_GOOGLE_SHEETS_CONFIG["REQUIRED_CONNECTION_PARAMS"]),
    }


def _now() -> datetime:
    return datetime.now()


def format_sheet_date(date_value: str) -> str:
    return datetime.strptime(date_value, "%Y-%m-%d").strftime("%d.%m.%Y")


def format_sheet_timestamp(dt: datetime) -> str:
    return dt.strftime("%d.%m.%Y %H:%M:%S")


def parse_sheet_timestamp(value: str | None) -> Optional[datetime]:
    if not value:
        return None
    for fmt in ("%d.%m.%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(str(value), fmt)
        except ValueError:
            continue
    return None


def build_failover_request_id(execution_date: str, target_pvz: str) -> str:
    return f"{execution_date}|{target_pvz}"


def get_failover_claim_backend() -> str:
    return str(BACKFILL_CONFIG.get("failover_claim_backend", "sheets") or "sheets").strip().lower()


def get_failover_apps_script_config() -> Dict[str, Any]:
    return {
        "url": REPORTS_PATHS.get("FAILOVER_APPS_SCRIPT_URL", ""),
        "shared_secret": REPORTS_PATHS.get("FAILOVER_SHARED_SECRET", ""),
        "timeout_seconds": int(BACKFILL_CONFIG.get("failover_apps_script_timeout_seconds", 15) or 15),
    }


def build_failover_state_record(
    *,
    execution_date: str,
    target_pvz: str,
    owner_pvz: str,
    status: str,
    claimed_by: str = "",
    claim_expires_at: str = "",
    attempt_no: int = 0,
    source_run_id: str = "",
    last_error: str = "",
    updated_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    now = updated_at or _now()
    return {
        "request_id": build_failover_request_id(execution_date, target_pvz),
        "Дата": execution_date,
        "target_pvz": target_pvz,
        "owner_pvz": owner_pvz,
        "status": status,
        "claimed_by": claimed_by,
        "claim_expires_at": claim_expires_at,
        "attempt_no": attempt_no,
        "source_run_id": source_run_id,
        "last_error": last_error,
        "updated_at": format_sheet_timestamp(now),
    }


@contextmanager
def failover_state_connection(logger=None):
    logger = logger or create_failover_state_logger()
    uploader = GoogleSheetsUploader(config=prepare_failover_state_connection_params(), logger=logger)
    if not uploader.connect():
        raise RuntimeError("Не удалось подключиться к KPI_FAILOVER_STATE")
    try:
        yield uploader
    finally:
        uploader.disconnect()


def upsert_failover_state(record: Dict[str, Any], logger=None) -> Dict[str, Any]:
    logger = logger or create_failover_state_logger()
    with failover_state_connection(logger=logger) as uploader:
        return uploader._perform_upload(record, strategy="update_or_append")


def get_failover_state(execution_date: str, target_pvz: str, logger=None) -> Optional[Dict[str, Any]]:
    logger = logger or create_failover_state_logger()
    with failover_state_connection(logger=logger) as uploader:
        reporter = uploader.sheets_reporter
        use_config = uploader.table_config
        return reporter.get_row_by_unique_keys(
            unique_key_values={
                "Дата": execution_date,
                "target_pvz": target_pvz,
            },
            config=use_config,
            return_raw=True,
        )


def list_failover_state_rows(
    *,
    statuses: Optional[Iterable[str]] = None,
    target_pvz: Optional[str] = None,
    claimed_by: Optional[str] = None,
    logger=None,
) -> list[Dict[str, Any]]:
    logger = logger or create_failover_state_logger()
    with failover_state_connection(logger=logger) as uploader:
        records = uploader.sheets_reporter.worksheet.get_all_records(expected_headers=uploader.table_config.column_names)

    filtered = []
    allowed_statuses = set(statuses or [])
    for record in records:
        if allowed_statuses and record.get("status") not in allowed_statuses:
            continue
        if target_pvz and record.get("target_pvz") != target_pvz:
            continue
        if claimed_by and record.get("claimed_by") != claimed_by:
            continue
        filtered.append(record)
    return filtered


def is_claim_active(state_row: Optional[Dict[str, Any]], now: Optional[datetime] = None) -> bool:
    if not state_row:
        return False
    if state_row.get("status") != STATUS_FAILOVER_CLAIMED:
        return False
    expires_at = parse_sheet_timestamp(state_row.get("claim_expires_at"))
    if not expires_at:
        return False
    return expires_at > (now or _now())


def verify_claim_ownership(
    *,
    execution_date: str,
    target_pvz: str,
    claimer_pvz: str,
    source_run_id: str = "",
    logger=None,
) -> Dict[str, Any]:
    logger = logger or create_failover_state_logger()
    persisted_state = get_failover_state(
        execution_date=execution_date,
        target_pvz=target_pvz,
        logger=logger,
    )
    ownership_confirmed = bool(
        persisted_state
        and persisted_state.get("status") == STATUS_FAILOVER_CLAIMED
        and persisted_state.get("claimed_by") == claimer_pvz
        and (not source_run_id or persisted_state.get("source_run_id") == source_run_id)
    )
    return {
        "success": True,
        "verified": ownership_confirmed,
        "state": persisted_state,
    }


def try_claim_failover_via_apps_script(
    *,
    execution_date: str,
    target_pvz: str,
    owner_pvz: str,
    claimer_pvz: str,
    ttl_minutes: int = 15,
    source_run_id: str = "",
    logger=None,
) -> Dict[str, Any]:
    logger = logger or create_failover_state_logger()
    config = get_failover_apps_script_config()
    apps_script_url = str(config.get("url") or "").strip()
    shared_secret = str(config.get("shared_secret") or "").strip()
    timeout_seconds = int(config.get("timeout_seconds", 15) or 15)

    if not apps_script_url:
        return {"success": False, "claimed": False, "reason": "apps_script_url_missing"}
    if not shared_secret:
        return {"success": False, "claimed": False, "reason": "apps_script_shared_secret_missing"}

    payload = {
        "action": "try_claim_failover",
        "shared_secret": shared_secret,
        "execution_date": execution_date,
        "target_pvz": target_pvz,
        "owner_pvz": owner_pvz,
        "claimer_pvz": claimer_pvz,
        "ttl_minutes": ttl_minutes,
        "source_run_id": source_run_id,
    }
    request = urllib.request.Request(
        apps_script_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        logger.error(f"Apps Script failover claim error: {exc}")
        return {
            "success": False,
            "claimed": False,
            "reason": "apps_script_request_failed",
            "error": str(exc),
        }

    response_payload.setdefault("success", False)
    response_payload.setdefault("claimed", False)
    response_payload.setdefault("reason", "apps_script_unknown")
    return response_payload


def try_claim_failover_via_sheets(
    *,
    execution_date: str,
    target_pvz: str,
    owner_pvz: str,
    claimer_pvz: str,
    ttl_minutes: int = 15,
    source_run_id: str = "",
    logger=None,
) -> Dict[str, Any]:
    logger = logger or create_failover_state_logger()
    now = _now()
    current_state = get_failover_state(execution_date=execution_date, target_pvz=target_pvz, logger=logger)

    if current_state and current_state.get("status") in TERMINAL_STATUSES:
        return {
            "success": True,
            "claimed": False,
            "reason": "already_completed",
            "state": current_state,
        }

    if is_claim_active(current_state, now=now):
        return {
            "success": True,
            "claimed": False,
            "reason": "already_claimed",
            "state": current_state,
        }

    previous_attempt_no = int(current_state.get("attempt_no") or 0) if current_state else 0
    claim_expires_at = format_sheet_timestamp(now + timedelta(minutes=max(ttl_minutes, 1)))
    claimed_record = build_failover_state_record(
        execution_date=execution_date,
        target_pvz=target_pvz,
        owner_pvz=owner_pvz,
        status=STATUS_FAILOVER_CLAIMED,
        claimed_by=claimer_pvz,
        claim_expires_at=claim_expires_at,
        attempt_no=previous_attempt_no + 1,
        source_run_id=source_run_id,
        last_error=current_state.get("last_error", "") if current_state else "",
        updated_at=now,
    )
    upload_result = upsert_failover_state(claimed_record, logger=logger)
    if not upload_result.get("success", False):
        return {
            "success": False,
            "claimed": False,
            "reason": "upload_failed",
            "state": claimed_record,
            "upload_result": upload_result,
        }

    verification_result = verify_claim_ownership(
        execution_date=execution_date,
        target_pvz=target_pvz,
        claimer_pvz=claimer_pvz,
        source_run_id=source_run_id,
        logger=logger,
    )
    return {
        "success": bool(verification_result.get("success", False)),
        "claimed": bool(verification_result.get("verified", False)),
        "reason": "claimed" if verification_result.get("verified", False) else "claim_verification_failed",
        "state": verification_result.get("state") or claimed_record,
        "upload_result": upload_result,
        "verification_result": verification_result,
    }


def try_claim_failover(
    *,
    execution_date: str,
    target_pvz: str,
    owner_pvz: str,
    claimer_pvz: str,
    ttl_minutes: int = 15,
    source_run_id: str = "",
    logger=None,
) -> Dict[str, Any]:
    claim_backend = get_failover_claim_backend()
    if claim_backend == "apps_script":
        return try_claim_failover_via_apps_script(
            execution_date=execution_date,
            target_pvz=target_pvz,
            owner_pvz=owner_pvz,
            claimer_pvz=claimer_pvz,
            ttl_minutes=ttl_minutes,
            source_run_id=source_run_id,
            logger=logger,
        )
    return try_claim_failover_via_sheets(
        execution_date=execution_date,
        target_pvz=target_pvz,
        owner_pvz=owner_pvz,
        claimer_pvz=claimer_pvz,
        ttl_minutes=ttl_minutes,
        source_run_id=source_run_id,
        logger=logger,
    )


def mark_failover_state(
    *,
    execution_date: str,
    target_pvz: str,
    owner_pvz: str,
    status: str,
    claimed_by: str = "",
    ttl_minutes: int = 0,
    source_run_id: str = "",
    last_error: str = "",
    logger=None,
) -> Dict[str, Any]:
    logger = logger or create_failover_state_logger()
    current_state = get_failover_state(execution_date=execution_date, target_pvz=target_pvz, logger=logger) or {}
    claim_expires_at = ""
    if ttl_minutes > 0:
        claim_expires_at = format_sheet_timestamp(_now() + timedelta(minutes=ttl_minutes))

    record = build_failover_state_record(
        execution_date=execution_date,
        target_pvz=target_pvz,
        owner_pvz=owner_pvz,
        status=status,
        claimed_by=claimed_by,
        claim_expires_at=claim_expires_at,
        attempt_no=int(current_state.get("attempt_no") or 0),
        source_run_id=source_run_id or current_state.get("source_run_id", ""),
        last_error=last_error,
    )
    result = upsert_failover_state(record, logger=logger)
    result["state"] = record
    return result
