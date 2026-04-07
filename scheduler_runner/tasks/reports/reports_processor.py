#!/usr/bin/env python3
"""
reports_processor.py

Процессор поддомена reports:
1. Определяет отсутствующие записи в Google Sheets
2. Парсит только missing dates
3. Загружает результат пачкой
4. Отправляет агрегированное уведомление

Single-date режим сохранен для обратной совместимости.
"""
__version__ = '0.1.0'

import argparse
import logging
import os
import sys
import time
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta

from config.base_config import PVZ_ID
from scheduler_runner.tasks.reports.failover_state import (
    STATUS_CLAIM_EXPIRED,
    STATUS_FAILOVER_FAILED,
    STATUS_FAILOVER_SUCCESS,
    STATUS_OWNER_FAILED,
    STATUS_OWNER_PENDING,
    STATUS_OWNER_SUCCESS,
    build_failover_state_record,
    create_failover_state_logger,
    failover_state_connection,
    get_failover_state_rows_by_keys,
    list_candidate_failover_rows_fast,
    mark_failover_state,
    try_claim_failover,
    upsert_failover_state_records,
)
from scheduler_runner.tasks.reports.failover_policy import (
    evaluate_claimable_rows_by_policy,
    filter_claimable_rows_by_policy,
    get_capability_targets_for_helper,
    get_priority_list,
    get_selection_mode,
    has_explicit_priority_rule,
)
from scheduler_runner.tasks.reports.config.scripts.kpi_google_sheets_config import KPI_GOOGLE_SHEETS_CONFIG
from scheduler_runner.tasks.reports.config.scripts.reports_processor_config import BACKFILL_CONFIG, FAILOVER_POLICY_CONFIG
from scheduler_runner.utils.parser import (
    build_parser_definition,
    build_jobs_for_pvz,
    convert_job_results_to_batch_result,
    create_parser_logger,
    execute_parser_internal,
    execute_parser_jobs_for_pvz,
    invoke_available_pvz_discovery,
    invoke_parser_for_grouped_jobs,
    invoke_parser_for_pvz,
    invoke_parser_for_single_date,
)
from scheduler_runner.utils.logging import TRACE_LEVEL, configure_logger
from scheduler_runner.utils.notifications import (
    send_notification,
)
from scheduler_runner.utils.system import SystemUtils
from scheduler_runner.utils.uploader import (
    check_missing_items,
    test_connection as test_upload_connection,
    upload_batch_data,
)


project_root = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)


@dataclass(frozen=True)
class PVZExecutionResult:
    pvz_id: str
    coverage_result: dict
    batch_result: dict
    upload_result: dict
    notification_data: dict

    @property
    def missing_dates_count(self):
        return len(self.coverage_result.get("missing_dates", []))

    @property
    def successful_jobs_count(self):
        return len(self.batch_result.get("successful_dates", []))

    @property
    def failed_jobs_count(self):
        return len(self.batch_result.get("failed_dates", []))

    @property
    def uploaded_records(self):
        return self.upload_result.get("uploaded_records", 0)


@dataclass(frozen=True)
class ReportsBackfillExecutionResult:
    date_from: str | None
    date_to: str | None
    processed_pvz_count: int
    missing_dates_count: int
    successful_jobs_count: int
    failed_jobs_count: int
    uploaded_records: int
    pvz_results: dict


@dataclass(frozen=True)
class OwnerRunSummary:
    pvz_id: str
    coverage_success: bool
    missing_dates: list
    missing_dates_count: int
    truncated: bool
    parse_success: bool
    successful_dates: list
    successful_dates_count: int
    failed_dates: list
    failed_dates_count: int
    uploaded_records: int
    upload_success: bool
    errors: list


@dataclass(frozen=True)
class FailoverRunSummary:
    enabled: bool
    attempted: bool
    discovery_success: bool | None
    owner_state_sync_attempted: bool
    owner_state_sync_success: bool | None
    owner_state_sync_error: str
    candidate_scan_attempted: bool
    candidate_scan_success: bool | None
    candidate_scan_error: str
    available_pvz: list
    candidate_rows_count: int
    claimed_rows_count: int
    recovered_pvz_count: int
    recovered_dates_count: int
    failed_recovery_dates_count: int
    uploaded_records: int
    results_by_pvz: dict


@dataclass(frozen=True)
class ReportsRunSummary:
    mode: str
    configured_pvz_id: str
    date_from: str | None
    date_to: str | None
    final_status: str
    owner: OwnerRunSummary | None
    multi_pvz: ReportsBackfillExecutionResult | None
    failover: FailoverRunSummary | None


def build_pvz_execution_result(pvz_id, coverage_result=None, batch_result=None, upload_result=None, notification_data=None):
    return PVZExecutionResult(
        pvz_id=pvz_id,
        coverage_result=deepcopy(coverage_result or {}),
        batch_result=deepcopy(batch_result or {}),
        upload_result=deepcopy(upload_result or {}),
        notification_data=deepcopy(notification_data or {}),
    )



def create_uploader_logger():
    return configure_logger(
        user="reports_domain",
        task_name="Uploader",
        log_levels=[TRACE_LEVEL, logging.DEBUG],
        single_file_for_levels=False,
    )


def create_notification_logger():
    return configure_logger(
        user="reports_domain",
        task_name="Notification",
        log_levels=[TRACE_LEVEL, logging.DEBUG],
        single_file_for_levels=False,
    )







def build_processor_run_id(pvz_id=PVZ_ID, started_at=None):
    started_at = started_at or datetime.now()
    return f"{started_at.strftime('%Y%m%d%H%M%S')}|{pvz_id}"


def is_retryable_google_sheets_upload_error(error_text):
    normalized_error = str(error_text or "").lower()
    retryable_markers = (
        "[503]",
        "service is currently unavailable",
        "temporarily unavailable",
        "timeout",
        "timed out",
        "connection reset",
        "connection aborted",
        "remote end closed connection",
    )
    return any(marker in normalized_error for marker in retryable_markers)


def run_google_sheets_upload_with_retry(*, upload_callable, logger=None):
    logger = logger or create_uploader_logger()
    max_attempts = max(int(BACKFILL_CONFIG.get("google_sheets_upload_max_attempts", 3) or 3), 1)
    delay_seconds = max(float(BACKFILL_CONFIG.get("google_sheets_upload_retry_delay_seconds", 5) or 5), 0.0)
    last_result = {"success": False, "error": "upload_not_started"}

    for attempt in range(1, max_attempts + 1):
        if attempt > 1:
            logger.warning(f"Повторная попытка batch upload в Google Sheets: attempt={attempt}/{max_attempts}")

        last_result = upload_callable() or {"success": False, "error": "empty_upload_result"}
        if last_result.get("success", False):
            return last_result

        error_text = last_result.get("error", "")
        if attempt >= max_attempts or not is_retryable_google_sheets_upload_error(error_text):
            return last_result

        logger.warning(
            f"Retryable upload error при batch upload в Google Sheets: {error_text}; "
            f"sleep={delay_seconds}s before next attempt"
        )
        if delay_seconds > 0:
            time.sleep(delay_seconds)

    return last_result


def extract_batch_failures(batch_result=None):
    failures = {}
    for execution_date, date_result in (batch_result or {}).get("results_by_date", {}).items():
        if date_result.get("success", False):
            continue
        failures[execution_date] = date_result.get("error", "parse_failed")
    return failures


def build_filtered_batch_result(batch_result=None, execution_dates=None):
    requested_dates = set(execution_dates or [])
    filtered_results_by_date = {
        execution_date: date_result
        for execution_date, date_result in (batch_result or {}).get("results_by_date", {}).items()
        if execution_date in requested_dates
    }
    successful_dates = [
        execution_date
        for execution_date, date_result in filtered_results_by_date.items()
        if date_result.get("success", False)
    ]
    failed_dates = [
        execution_date
        for execution_date, date_result in filtered_results_by_date.items()
        if not date_result.get("success", False)
    ]
    return {
        "success": not failed_dates,
        "mode": (batch_result or {}).get("mode", "batch"),
        "total_dates": len(filtered_results_by_date),
        "successful_dates": successful_dates,
        "failed_dates": failed_dates,
        "results_by_date": filtered_results_by_date,
    }


def _as_date_list(value):
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if value is None:
        return []
    if isinstance(value, int):
        return []
    if isinstance(value, str):
        return [value] if value else []
    return [str(value)]


def _count_batch_successful_dates(batch_result=None):
    successful_dates = (batch_result or {}).get("successful_dates", [])
    if isinstance(successful_dates, list):
        return len(successful_dates)
    if isinstance(successful_dates, int):
        return successful_dates
    return 0


def _count_batch_failed_dates(batch_result=None):
    failed_dates = (batch_result or {}).get("failed_dates", [])
    if isinstance(failed_dates, list):
        return len(failed_dates)
    if isinstance(failed_dates, int):
        return failed_dates
    return 0


def build_owner_run_summary(*, pvz_id, coverage_result=None, batch_result=None, upload_result=None):
    coverage_result = coverage_result or {}
    batch_result = batch_result or {}
    upload_result = upload_result or {}
    failed_by_date = extract_batch_failures(batch_result)
    successful_dates_raw = batch_result.get("successful_dates", [])
    failed_dates_raw = batch_result.get("failed_dates", [])

    errors = []
    if not coverage_result.get("success", True):
        errors.append(coverage_result.get("error", "coverage_check_failed"))
    if batch_result and batch_result.get("error"):
        errors.append(batch_result.get("error"))
    errors.extend(error for error in failed_by_date.values() if error)
    if upload_result and upload_result.get("error"):
        errors.append(upload_result.get("error"))

    return OwnerRunSummary(
        pvz_id=pvz_id,
        coverage_success=coverage_result.get("success", False),
        missing_dates=deepcopy(coverage_result.get("missing_dates", [])),
        missing_dates_count=len(coverage_result.get("missing_dates", [])),
        truncated=bool(coverage_result.get("truncated", False)),
        parse_success=bool(batch_result.get("success", False)),
        successful_dates=deepcopy(_as_date_list(successful_dates_raw)),
        successful_dates_count=_count_batch_successful_dates(batch_result),
        failed_dates=deepcopy(_as_date_list(failed_dates_raw)),
        failed_dates_count=_count_batch_failed_dates(batch_result),
        uploaded_records=int(upload_result.get("uploaded_records", 0) or 0),
        upload_success=bool(upload_result.get("success", False)),
        errors=[error for error in errors if error],
    )


def build_failover_run_summary(*, enabled=False, failover_result=None):
    failover_result = failover_result or {}
    discovery_result = failover_result.get("discovery_result")
    owner_state_sync = failover_result.get("owner_state_sync") or {}
    candidate_scan = failover_result.get("candidate_scan") or {}
    return FailoverRunSummary(
        enabled=bool(enabled),
        attempted=bool(failover_result.get("attempted", False)),
        discovery_success=(
            discovery_result.get("success", False)
            if isinstance(discovery_result, dict)
            else None
        ),
        owner_state_sync_attempted=bool(owner_state_sync.get("attempted", False)),
        owner_state_sync_success=owner_state_sync.get("success"),
        owner_state_sync_error=str(owner_state_sync.get("error", "") or ""),
        candidate_scan_attempted=bool(candidate_scan.get("attempted", False)),
        candidate_scan_success=candidate_scan.get("success"),
        candidate_scan_error=str(candidate_scan.get("error", "") or ""),
        available_pvz=deepcopy(failover_result.get("available_pvz", [])),
        candidate_rows_count=int(failover_result.get("candidate_rows_count", 0) or 0),
        claimed_rows_count=int(failover_result.get("claimed_rows_count", 0) or 0),
        recovered_pvz_count=int(failover_result.get("recovered_pvz_count", 0) or 0),
        recovered_dates_count=int(failover_result.get("recovered_dates_count", 0) or 0),
        failed_recovery_dates_count=int(failover_result.get("failed_recovery_dates_count", 0) or 0),
        uploaded_records=int(failover_result.get("uploaded_records", 0) or 0),
        results_by_pvz=deepcopy(failover_result.get("results_by_pvz", {})),
    )


def _owner_has_work(owner):
    return bool(owner and owner.missing_dates_count > 0)


def _is_owner_skipped_no_missing(owner):
    return bool(
        owner
        and owner.coverage_success is True
        and owner.missing_dates_count == 0
        and owner.successful_dates_count == 0
        and owner.failed_dates_count == 0
        and owner.uploaded_records == 0
        and not owner.errors
    )


def _owner_had_meaningful_success(owner):
    return bool(
        owner
        and _owner_has_work(owner)
        and owner.upload_success
        and owner.successful_dates_count > 0
        and owner.failed_dates_count == 0
    )


def _owner_had_meaningful_failure(owner):
    return bool(
        owner
        and _owner_has_work(owner)
        and (owner.failed_dates_count > 0 or owner.upload_success is False)
    )


def _failover_had_meaningful_success(failover):
    return bool(
        failover
        and failover.enabled
        and (
            failover.recovered_dates_count > 0
            or failover.recovered_pvz_count > 0
            or failover.uploaded_records > 0
        )
    )


def _failover_had_meaningful_failure(failover):
    return bool(
        failover
        and failover.enabled
        and failover.failed_recovery_dates_count > 0
    )


def _failover_sync_had_failure(failover):
    return bool(
        failover
        and failover.enabled
        and failover.owner_state_sync_attempted
        and failover.owner_state_sync_success is False
    )


def _failover_candidate_scan_had_failure(failover):
    return bool(
        failover
        and failover.enabled
        and failover.candidate_scan_attempted
        and failover.candidate_scan_success is False
    )


def _failover_had_any_work(failover):
    return bool(
        failover
        and failover.enabled
        and (
            _failover_sync_had_failure(failover)
            or _failover_candidate_scan_had_failure(failover)
            or failover.owner_state_sync_attempted
            or failover.candidate_rows_count > 0
            or failover.claimed_rows_count > 0
            or _failover_had_meaningful_success(failover)
            or _failover_had_meaningful_failure(failover)
        )
    )


def resolve_final_run_status(*, owner=None, multi_pvz=None, failover=None):
    if owner:
        if owner.coverage_success is False:
            return "failed"
        if _owner_has_work(owner) and owner.successful_dates_count == 0 and owner.failed_dates_count > 0:
            return "failed"
        if _owner_had_meaningful_failure(owner):
            return "partial"

    if multi_pvz:
        if multi_pvz.processed_pvz_count == 0:
            return "skipped"
        if multi_pvz.successful_jobs_count == 0 and multi_pvz.failed_jobs_count > 0:
            return "failed"
        if multi_pvz.failed_jobs_count > 0:
            return "partial"

    if _failover_had_meaningful_failure(failover):
        return "partial"
    if _failover_sync_had_failure(failover):
        return "partial"
    if _failover_candidate_scan_had_failure(failover):
        return "partial"

    if _owner_had_meaningful_success(owner):
        return "success"
    if multi_pvz and multi_pvz.processed_pvz_count > 0:
        return "success"
    if _failover_had_meaningful_success(failover):
        return "success"
    return "skipped"


def build_reports_run_summary(
    *,
    mode,
    configured_pvz_id,
    date_from=None,
    date_to=None,
    owner=None,
    multi_pvz=None,
    failover=None,
):
    final_status = resolve_final_run_status(owner=owner, multi_pvz=multi_pvz, failover=failover)
    return ReportsRunSummary(
        mode=mode,
        configured_pvz_id=configured_pvz_id,
        date_from=date_from,
        date_to=date_to,
        final_status=final_status,
        owner=owner,
        multi_pvz=multi_pvz,
        failover=failover,
    )


def _format_failed_dates(failed_dates):
    failed_dates = failed_dates or []
    return ", ".join(str(item) for item in failed_dates[:5]) if failed_dates else "-"


def is_failover_candidate_scan_retryable_error(exc):
    message = str(exc or "").lower()
    return (
        "kpi_failover_state" in message
        or "quota" in message
        or "429" in message
        or "read requests per minute per user" in message
    )


def format_reports_run_notification_message(summary):
    lines = [
        "KPI reports run",
        f"Статус: {summary.final_status}",
        f"Объект: {summary.configured_pvz_id}",
        f"Диапазон: {summary.date_from or '-'} .. {summary.date_to or '-'}",
    ]

    if summary.owner:
        owner_lines = [
            "",
            "Свои данные:",
            f"- ПВЗ: {summary.owner.pvz_id}",
        ]
        if _is_owner_skipped_no_missing(summary.owner):
            owner_lines.append("- missing dates не было")
        else:
            owner_lines.extend(
                [
                    f"- missing dates: {summary.owner.missing_dates_count}",
                    f"- успешно спарсено: {summary.owner.successful_dates_count}",
                    f"- неуспешные даты: {_format_failed_dates(summary.owner.failed_dates)}",
                    f"- загружено записей: {summary.owner.uploaded_records}",
                ]
            )
            if summary.owner.errors:
                owner_lines.append(
                    f"- errors: {'; '.join(str(error) for error in summary.owner.errors[:3])}"
                )
        lines.extend(owner_lines)

    if summary.multi_pvz:
        lines.extend(
            [
                "",
                "Обработка выбранных ПВЗ:",
                f"- обработано ПВЗ: {summary.multi_pvz.processed_pvz_count}",
                f"- найдено missing dates: {summary.multi_pvz.missing_dates_count}",
                f"- успешно jobs: {summary.multi_pvz.successful_jobs_count}",
                f"- неуспешно jobs: {summary.multi_pvz.failed_jobs_count}",
                f"- загружено записей: {summary.multi_pvz.uploaded_records}",
            ]
        )
        details = []
        for pvz_id, pvz_result in summary.multi_pvz.pvz_results.items():
            details.append(
                f"  - {pvz_id}: missing={pvz_result.missing_dates_count}, "
                f"ok={pvz_result.successful_jobs_count}, "
                f"failed={pvz_result.failed_jobs_count}, "
                f"uploaded={pvz_result.uploaded_records}"
            )
        if details:
            lines.extend(["- детали:"] + details[:5])

    if summary.failover and summary.failover.enabled:
        failover_lines = [
            "",
            "Помощь коллегам:",
            f"- attempted: {'yes' if summary.failover.attempted else 'no'}",
            f"- discovery: {summary.failover.discovery_success if summary.failover.discovery_success is not None else '-'}",
            f"- доступные ПВЗ: {', '.join(summary.failover.available_pvz) if summary.failover.available_pvz else '-'}",
        ]
        if summary.failover.owner_state_sync_attempted:
            failover_lines.append(
                f"- owner state sync: {'ok' if summary.failover.owner_state_sync_success else 'failed'}"
            )
            if summary.failover.owner_state_sync_error:
                failover_lines.append(
                    f"- owner state sync error: {summary.failover.owner_state_sync_error}"
                )
        if summary.failover.candidate_scan_attempted:
            failover_lines.append(
                f"- candidate scan: {'ok' if summary.failover.candidate_scan_success else 'failed'}"
            )
            if summary.failover.candidate_scan_error:
                failover_lines.append(
                    f"- candidate scan error: {summary.failover.candidate_scan_error}"
                )
        if not _failover_had_any_work(summary.failover):
            failover_lines.extend(
                [
                    "- coordination включен, recovery работа не потребовалась",
                    f"- candidate rows: {summary.failover.candidate_rows_count}",
                    f"- claimed rows: {summary.failover.claimed_rows_count}",
                    f"- восстановлено дат: {summary.failover.recovered_dates_count}",
                ]
            )
        else:
            failover_lines.extend(
                [
                    f"- candidate rows: {summary.failover.candidate_rows_count}",
                    f"- claimed rows: {summary.failover.claimed_rows_count}",
                    f"- восстановлено ПВЗ: {summary.failover.recovered_pvz_count}",
                    f"- восстановлено дат: {summary.failover.recovered_dates_count}",
                    f"- неуспешных recovery дат: {summary.failover.failed_recovery_dates_count}",
                    f"- загружено записей: {summary.failover.uploaded_records}",
                ]
            )
        lines.extend(failover_lines)
        failover_details = []
        for target_pvz, target_result in summary.failover.results_by_pvz.items():
            failover_details.append(
                f"  - {target_pvz}: recovered={len(target_result.get('recoverable_dates', []))}, "
                f"failed={len(extract_batch_failures(target_result.get('batch_result', {})))}, "
                f"uploaded={target_result.get('upload_result', {}).get('uploaded_records', 0)}"
            )
        if failover_details:
            lines.extend(["- детали:"] + failover_details[:5])

    return "\n".join(lines)


def mark_dates_with_owner_status(execution_dates, owner_pvz, status, logger=None, source_run_id=""):
    logger = logger or create_failover_state_logger()
    results = []
    for execution_date in execution_dates or []:
        results.append(
            mark_failover_state(
                execution_date=execution_date,
                target_pvz=owner_pvz,
                owner_pvz=owner_pvz,
                status=status,
                source_run_id=source_run_id,
                logger=logger,
            )
        )
    return results


def classify_owner_success_history(existing_state_row):
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
    owner_pvz,
    missing_dates,
    batch_result,
    upload_result=None,
    source_run_id="",
    existing_state_rows_by_date=None,
):
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
                    target_pvz=owner_pvz,
                    owner_pvz=owner_pvz,
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
                    target_pvz=owner_pvz,
                    owner_pvz=owner_pvz,
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
                    target_pvz=owner_pvz,
                    owner_pvz=owner_pvz,
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
    owner_pvz,
    missing_dates,
    batch_result,
    upload_result=None,
    logger=None,
    source_run_id="",
):
    logger = logger or create_failover_state_logger()
    existing_rows = get_failover_state_rows_by_keys(
        keys=[
            {"Дата": execution_date, "target_pvz": owner_pvz}
            for execution_date in (missing_dates or [])
        ],
        logger=logger,
    )
    existing_state_rows_by_date = {}
    for (normalized_date, _normalized_target), row in existing_rows.items():
        raw_date = str(row.get("Дата", "") or "").strip()
        if raw_date:
            try:
                normalized_runtime_date = datetime.strptime(raw_date, "%d.%m.%Y").strftime("%Y-%m-%d")
            except ValueError:
                normalized_runtime_date = raw_date
            existing_state_rows_by_date[normalized_runtime_date] = row

    built_result = build_owner_final_failover_state_records(
        owner_pvz=owner_pvz,
        missing_dates=missing_dates,
        batch_result=batch_result,
        upload_result=upload_result,
        source_run_id=source_run_id,
        existing_state_rows_by_date=existing_state_rows_by_date,
    )
    upsert_result = {"success": True, "results": []}
    if built_result["records"]:
        upsert_result = upsert_failover_state_records(
            built_result["records"],
            logger=logger,
        )
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


def collect_claimable_failover_rows(
    *,
    accessible_pvz_ids,
    configured_pvz_id=PVZ_ID,
    max_claims=None,
    logger=None,
    uploader=None,
    return_evaluation=False,
):
    logger = logger or create_failover_state_logger()
    rows = list_candidate_failover_rows_fast(
        statuses=[STATUS_OWNER_FAILED, STATUS_CLAIM_EXPIRED, STATUS_FAILOVER_FAILED],
        logger=logger,
        uploader=uploader,
    )
    if not FAILOVER_POLICY_CONFIG.get("enabled", True):
        filtered_rows = []
        decisions = []
        normalized_accessible = {normalize_pvz_id(pvz_id) for pvz_id in (accessible_pvz_ids or [])}
        normalized_configured_pvz_id = normalize_pvz_id(configured_pvz_id)
        for row in rows:
            target_pvz = row.get("target_pvz")
            if not target_pvz:
                continue
            normalized_target_pvz = normalize_pvz_id(target_pvz)
            decision_item = {
                "execution_date": row.get("Дата", ""),
                "target_pvz": target_pvz,
                "status": row.get("status"),
                "eligible": False,
                "reason": "",
            }
            if normalized_target_pvz == normalized_configured_pvz_id:
                decision_item["reason"] = "own_target_pvz"
                decisions.append(decision_item)
                continue
            if normalized_target_pvz not in normalized_accessible:
                decision_item["reason"] = "not_accessible"
                decisions.append(decision_item)
                continue
            decision_item["eligible"] = True
            decision_item["reason"] = "eligible"
            decisions.append(decision_item)
            filtered_rows.append(row)
        filtered_rows.sort(key=lambda row: (row.get("Дата", ""), row.get("target_pvz", "")))
        selected_rows = filtered_rows[:max_claims] if max_claims else filtered_rows
        selected_keys = {(row.get("Дата", ""), row.get("target_pvz", "")) for row in selected_rows}
        for decision_item in decisions:
            decision_item["selected_for_claim"] = (
                decision_item.get("eligible", False)
                and (decision_item["execution_date"], decision_item["target_pvz"]) in selected_keys
            )
        evaluation = {
            "mode": "policy_disabled",
            "decisions": decisions,
            "eligible_rows": filtered_rows,
            "selected_rows": selected_rows,
            "eligible_count": len(filtered_rows),
            "selected_count": len(selected_rows),
            "rejected_count": max(len(decisions) - len(filtered_rows), 0),
            "rejected_reasons": {
                reason: len([item for item in decisions if not item.get("eligible", False) and item.get("reason") == reason])
                for reason in {item.get("reason") for item in decisions if not item.get("eligible", False)}
            },
        }
        return evaluation if return_evaluation else selected_rows

    evaluation = evaluate_claimable_rows_by_policy(
        rows=rows,
        configured_pvz_id=configured_pvz_id,
        available_pvz=accessible_pvz_ids,
        max_claims=max_claims,
    )
    return evaluation if return_evaluation else evaluation["selected_rows"]


def normalize_claimable_failover_evaluation(candidate_evaluation) -> dict:
    if isinstance(candidate_evaluation, dict):
        return candidate_evaluation
    selected_rows = list(candidate_evaluation or [])
    return {
        "mode": get_selection_mode(),
        "decisions": [],
        "eligible_rows": selected_rows,
        "selected_rows": selected_rows,
        "eligible_count": len(selected_rows),
        "selected_count": len(selected_rows),
        "rejected_count": 0,
        "rejected_reasons": {},
    }


def should_scan_failover_candidates(
    *,
    configured_pvz_id=PVZ_ID,
    accessible_pvz_ids=None,
):
    if not FAILOVER_POLICY_CONFIG.get("enabled", True):
        return {"should_scan": True, "reason": "policy_disabled"}

    if get_selection_mode() == "capability_ranked":
        return should_scan_failover_candidates_capability_ranked(
            configured_pvz_id=configured_pvz_id,
            accessible_pvz_ids=accessible_pvz_ids,
        )

    return should_scan_failover_candidates_legacy(
        configured_pvz_id=configured_pvz_id,
        accessible_pvz_ids=accessible_pvz_ids,
    )


def should_scan_failover_candidates_legacy(
    *,
    configured_pvz_id=PVZ_ID,
    accessible_pvz_ids=None,
):
    if not has_explicit_priority_rule(configured_pvz_id):
        return {"should_scan": True, "reason": "no_explicit_rule"}

    priority_list = get_priority_list(configured_pvz_id)
    if not priority_list:
        return {"should_scan": False, "reason": "empty_priority_list"}

    normalized_accessible = {normalize_pvz_id(pvz_id) for pvz_id in (accessible_pvz_ids or [])}
    normalized_priority = {normalize_pvz_id(pvz_id) for pvz_id in priority_list}
    if normalized_accessible & normalized_priority:
        return {"should_scan": True, "reason": "accessible_priority_candidates"}

    return {"should_scan": False, "reason": "priority_candidates_not_accessible"}


def should_scan_failover_candidates_capability_ranked(
    *,
    configured_pvz_id=PVZ_ID,
    accessible_pvz_ids=None,
):
    capability_targets = get_capability_targets_for_helper(configured_pvz_id)
    if not capability_targets:
        return {"should_scan": False, "reason": "empty_capability_list"}

    normalized_accessible = {normalize_pvz_id(pvz_id) for pvz_id in (accessible_pvz_ids or [])}
    normalized_accessible.discard(normalize_pvz_id(configured_pvz_id))
    normalized_targets = {normalize_pvz_id(pvz_id) for pvz_id in capability_targets}
    if normalized_accessible & normalized_targets:
        return {"should_scan": True, "reason": "accessible_capability_targets"}

    return {"should_scan": False, "reason": "capability_targets_not_accessible"}


def collect_failover_scan_decisions(
    *,
    configured_pvz_id=PVZ_ID,
    accessible_pvz_ids=None,
):
    active_decision = should_scan_failover_candidates(
        configured_pvz_id=configured_pvz_id,
        accessible_pvz_ids=accessible_pvz_ids,
    )
    decisions = {
        "active_mode": get_selection_mode(),
        "active": active_decision,
    }
    if FAILOVER_POLICY_CONFIG.get("dry_run_capability_ranked", False):
        decisions["dry_run_capability_ranked"] = should_scan_failover_candidates_capability_ranked(
            configured_pvz_id=configured_pvz_id,
            accessible_pvz_ids=accessible_pvz_ids,
        )
    return decisions


def claim_failover_rows(
    *,
    candidate_rows,
    claimer_pvz,
    ttl_minutes,
    source_run_id="",
    logger=None,
    uploader=None,
):
    logger = logger or create_failover_state_logger()
    claimed_rows = []
    for row in candidate_rows or []:
        claim_result = try_claim_failover(
            execution_date=row.get("Р”Р°С‚Р°"),
            target_pvz=row.get("target_pvz"),
            owner_pvz=row.get("owner_pvz") or row.get("target_pvz"),
            claimer_pvz=claimer_pvz,
            ttl_minutes=ttl_minutes,
            source_run_id=source_run_id,
            logger=logger,
            uploader=uploader,
        )
        if claim_result.get("claimed", False):
            claimed_rows.append(row)
    return claimed_rows


def run_claimed_failover_backfill(
    *,
    claimed_rows,
    parser_api,
    parser_logger=None,
    failover_logger=None,
    claimer_pvz=PVZ_ID,
    source_run_id="",
):
    parser_logger = parser_logger or create_parser_logger()
    failover_logger = failover_logger or create_failover_state_logger()
    claimed_dates_by_pvz = {}
    owner_by_key = {}
    for row in claimed_rows or []:
        target_pvz = row["target_pvz"]
        execution_date = row["Р”Р°С‚Р°"]
        claimed_dates_by_pvz.setdefault(target_pvz, []).append(execution_date)
        owner_by_key[(target_pvz, execution_date)] = row.get("owner_pvz") or target_pvz

    execution_results = {}
    uploaded_records_total = 0
    recovered_dates_total = 0
    failed_recovery_dates_total = 0
    for target_pvz, execution_dates in claimed_dates_by_pvz.items():
        unique_dates = sorted(set(execution_dates))
        jobs = build_jobs_for_pvz(pvz_id=target_pvz, execution_dates=unique_dates)
        batch_result = invoke_parser_for_pvz(
            parser_api=parser_api,
            jobs=jobs,
            logger=parser_logger,
        )
        coverage_result = detect_missing_report_dates(
            date_from=min(unique_dates),
            date_to=max(unique_dates),
            logger=create_uploader_logger(),
            max_missing_dates=len(unique_dates),
            pvz_id=target_pvz,
        )
        recoverable_dates = set(unique_dates)
        if coverage_result.get("success", False):
            recoverable_dates = set(coverage_result.get("missing_dates", [])) & set(unique_dates)
        filtered_batch_result = build_filtered_batch_result(batch_result, recoverable_dates)
        upload_result = (
            run_upload_batch_microservice(filtered_batch_result)
            if recoverable_dates
            else {"success": True, "uploaded_records": 0, "skipped_as_already_covered": True}
        )
        failed_by_date = extract_batch_failures(batch_result)
        uploaded_records_total += int(upload_result.get("uploaded_records", 0) or 0)
        recovered_dates_total += len(recoverable_dates)
        failed_recovery_dates_total += len(failed_by_date)

        for execution_date in unique_dates:
            common_kwargs = {
                "execution_date": execution_date,
                "target_pvz": target_pvz,
                "owner_pvz": owner_by_key[(target_pvz, execution_date)],
                "claimed_by": claimer_pvz,
                "source_run_id": source_run_id,
                "logger": failover_logger,
            }
            if execution_date in failed_by_date:
                mark_failover_state(
                    status=STATUS_FAILOVER_FAILED,
                    last_error=failed_by_date[execution_date],
                    **common_kwargs,
                )
            elif execution_date not in recoverable_dates:
                mark_failover_state(
                    status=STATUS_FAILOVER_SUCCESS,
                    last_error="skipped_upload_already_covered",
                    **common_kwargs,
                )
            else:
                mark_failover_state(
                    status=STATUS_FAILOVER_SUCCESS,
                    **common_kwargs,
                )

        execution_results[target_pvz] = {
            "execution_dates": unique_dates,
            "batch_result": batch_result,
            "filtered_batch_result": filtered_batch_result,
            "coverage_result": coverage_result,
            "recoverable_dates": sorted(recoverable_dates),
            "upload_result": upload_result,
        }

    return {
        "results_by_pvz": execution_results,
        "recovered_pvz_count": len(execution_results),
        "recovered_dates_count": recovered_dates_total,
        "failed_recovery_dates_count": failed_recovery_dates_total,
        "uploaded_records": uploaded_records_total,
    }


def run_failover_coordination_pass(
    *,
    configured_pvz_id=PVZ_ID,
    parser_api,
    parser_logger=None,
    processor_logger=None,
    source_run_id="",
):
    processor_logger = processor_logger or configure_logger(user="reports_domain", task_name="Processor")
    failover_logger = create_failover_state_logger()
    discovery_scope = discover_available_pvz_scope(
        configured_pvz_id=configured_pvz_id,
        logger=processor_logger,
        parser_logger=parser_logger,
    )
    result = {
        "attempted": True,
        "discovery_result": discovery_scope.get("discovery_result", {}),
        "available_pvz": discovery_scope.get("available_pvz", []),
        "scan_policy_mode": get_selection_mode(),
        "scan_decisions": {},
        "candidate_scan": {
            "attempted": False,
            "success": None,
            "error": "",
        },
        "candidate_rows": [],
        "candidate_rows_count": 0,
        "candidate_policy_evaluation": {},
        "claimed_rows": [],
        "claimed_rows_count": 0,
        "results_by_pvz": {},
        "recovered_pvz_count": 0,
        "recovered_dates_count": 0,
        "failed_recovery_dates_count": 0,
        "uploaded_records": 0,
    }
    scan_decisions = collect_failover_scan_decisions(
        configured_pvz_id=configured_pvz_id,
        accessible_pvz_ids=discovery_scope.get("available_pvz", []),
    )
    result["scan_decisions"] = scan_decisions
    scan_decision = scan_decisions.get("active", {})
    if "dry_run_capability_ranked" in scan_decisions:
        processor_logger.info(
            f"Failover coordination dry-run: capability_ranked decision={scan_decisions['dry_run_capability_ranked']}"
        )
    if not scan_decision.get("should_scan", True):
        processor_logger.info(
            "Failover coordination: candidate scan skipped, "
            f"mode={result['scan_policy_mode']}, reason={scan_decision.get('reason', 'unknown')}"
        )
        return result

    result["candidate_scan"]["attempted"] = True
    try:
        with failover_state_connection(logger=failover_logger) as failover_uploader:
            candidate_evaluation = collect_claimable_failover_rows(
                accessible_pvz_ids=discovery_scope.get("available_pvz", []),
                configured_pvz_id=configured_pvz_id,
                max_claims=BACKFILL_CONFIG.get("failover_max_claims_per_run"),
                logger=failover_logger,
                uploader=failover_uploader,
                return_evaluation=True,
            )
            candidate_evaluation = normalize_claimable_failover_evaluation(candidate_evaluation)
            result["candidate_policy_evaluation"] = candidate_evaluation
            result["candidate_rows"] = candidate_evaluation.get("selected_rows", [])
            result["candidate_rows_count"] = len(result["candidate_rows"])
            result["candidate_scan"]["success"] = True
            processor_logger.info(
                "Failover coordination arbitration: "
                f"mode={candidate_evaluation.get('mode')}, "
                f"eligible={candidate_evaluation.get('eligible_count', 0)}, "
                f"selected={candidate_evaluation.get('selected_count', 0)}, "
                f"rejected={candidate_evaluation.get('rejected_count', 0)}, "
                f"rejected_reasons={candidate_evaluation.get('rejected_reasons', {})}"
            )
            if not result["candidate_rows"]:
                processor_logger.info("Failover coordination: claimable colleague rows not found")
                return result

            claimed_rows = claim_failover_rows(
                candidate_rows=result["candidate_rows"],
                claimer_pvz=configured_pvz_id,
                ttl_minutes=BACKFILL_CONFIG.get("failover_claim_ttl_minutes", 15),
                source_run_id=source_run_id,
                logger=failover_logger,
                uploader=failover_uploader,
            )
            result["claimed_rows"] = claimed_rows
            result["claimed_rows_count"] = len(claimed_rows)
    except Exception as exc:
        if not is_failover_candidate_scan_retryable_error(exc):
            raise
        result["candidate_scan"]["success"] = False
        result["candidate_scan"]["error"] = str(exc)
        processor_logger.warning(
            f"Failover coordination degraded: candidate scan failed, skip claim phase. error={exc}"
        )
        return result
    if not result["claimed_rows"]:
        processor_logger.info("Failover coordination: claimable rows were scanned but claim was not acquired")
        return result

    processor_logger.info(
        f"Failover coordination: claimed_rows={len(result['claimed_rows'])}, targets={sorted(set(row['target_pvz'] for row in result['claimed_rows']))}"
    )
    execution_result = run_claimed_failover_backfill(
        claimed_rows=result["claimed_rows"],
        parser_api=parser_api,
        parser_logger=parser_logger,
        failover_logger=failover_logger,
        claimer_pvz=configured_pvz_id,
        source_run_id=source_run_id,
    )
    result.update(execution_result)
    return result


def build_jobs_from_missing_dates_by_pvz(missing_dates_by_pvz, definition=None, extra_params_by_pvz=None):
    parser_definition = definition or build_parser_definition()
    jobs = []
    for pvz_id, execution_dates in (missing_dates_by_pvz or {}).items():
        jobs.extend(
            build_jobs_for_pvz(
                pvz_id=pvz_id,
                execution_dates=execution_dates,
                definition=parser_definition,
                extra_params=(extra_params_by_pvz or {}).get(pvz_id),
            )
        )
    return jobs


def group_jobs_by_pvz(jobs):
    grouped_jobs = {}
    for job in jobs or []:
        grouped_jobs.setdefault(job.pvz_id, []).append(job)
    return grouped_jobs













def prepare_connection_params():
    from scheduler_runner.tasks.reports.config.reports_paths import REPORTS_PATHS

    return {
        "CREDENTIALS_PATH": str(REPORTS_PATHS["GOOGLE_SHEETS_CREDENTIALS"]),
        "SPREADSHEET_ID": KPI_GOOGLE_SHEETS_CONFIG["SPREADSHEET_ID"],
        "WORKSHEET_NAME": KPI_GOOGLE_SHEETS_CONFIG["WORKSHEET_NAME"],
        "TABLE_CONFIG": deepcopy(KPI_GOOGLE_SHEETS_CONFIG["TABLE_CONFIG"]),
        "REQUIRED_CONNECTION_PARAMS": ["CREDENTIALS_PATH", "SPREADSHEET_ID", "WORKSHEET_NAME", "TABLE_CONFIG"],
    }


def normalize_pvz_id(pvz_id):
    transliterated = SystemUtils.cyrillic_to_translit(str(pvz_id or ""))
    return transliterated.strip().lower()


def should_run_automatic_failover_coordination(
    *,
    enabled,
    raw_pvz_ids=None,
    resolved_pvz_ids=None,
    current_pvz_id=None,
    configured_pvz_id=PVZ_ID,
):
    if not enabled:
        return False
    if raw_pvz_ids:
        return False
    if resolved_pvz_ids is not None and len(resolved_pvz_ids) > 1:
        return False
    if current_pvz_id is not None and normalize_pvz_id(current_pvz_id) != normalize_pvz_id(configured_pvz_id):
        return False
    return True


def resolve_pvz_ids(raw_pvz_ids=None):
    if not raw_pvz_ids:
        return [PVZ_ID]

    resolved_pvz_ids = []
    seen_pvz_ids = set()
    for pvz_id in raw_pvz_ids:
        normalized_pvz_id = str(pvz_id or "").strip()
        if not normalized_pvz_id or normalized_pvz_id in seen_pvz_ids:
            continue
        resolved_pvz_ids.append(normalized_pvz_id)
        seen_pvz_ids.add(normalized_pvz_id)

    return resolved_pvz_ids or [PVZ_ID]


def discover_available_pvz_scope(configured_pvz_id=PVZ_ID, logger=None, parser_logger=None):
    logger = logger or configure_logger(user="reports_domain", task_name="Processor")
    parser_logger = parser_logger or create_parser_logger()

    discovery_result = invoke_available_pvz_discovery(
        pvz_id=configured_pvz_id,
        logger=parser_logger,
    )
    available_pvz = []
    normalized_available_pvz = set()

    if discovery_result.get("success", False):
        for pvz_id in discovery_result.get("available_pvz", []) or []:
            normalized_pvz_id = normalize_pvz_id(pvz_id)
            if not normalized_pvz_id or normalized_pvz_id in normalized_available_pvz:
                continue
            normalized_available_pvz.add(normalized_pvz_id)
            available_pvz.append(pvz_id)
        if normalize_pvz_id(configured_pvz_id) not in normalized_available_pvz:
            available_pvz.append(configured_pvz_id)
            normalized_available_pvz.add(normalize_pvz_id(configured_pvz_id))
        logger.info(
            f"Discovery доступных ПВЗ завершен: configured_pvz_id={configured_pvz_id}, "
            f"available_pvz={available_pvz}"
        )
    else:
        logger.warning(
            "Discovery доступных ПВЗ завершился ошибкой, fallback только на собственный PVZ: "
            f"{configured_pvz_id}; error={discovery_result.get('error', 'unknown_error')}"
        )
        available_pvz = [configured_pvz_id]
        normalized_available_pvz = {normalize_pvz_id(configured_pvz_id)}

    return {
        "success": discovery_result.get("success", False),
        "configured_pvz_id": configured_pvz_id,
        "available_pvz": available_pvz,
        "normalized_available_pvz": normalized_available_pvz,
        "discovery_result": discovery_result,
    }


def resolve_accessible_pvz_ids(raw_pvz_ids=None, configured_pvz_id=PVZ_ID, logger=None, parser_logger=None):
    requested_pvz_ids = resolve_pvz_ids(raw_pvz_ids)
    normalized_configured_pvz_id = normalize_pvz_id(configured_pvz_id)
    requested_colleague_pvz_ids = [
        pvz_id for pvz_id in requested_pvz_ids if normalize_pvz_id(pvz_id) != normalized_configured_pvz_id
    ]

    if not requested_colleague_pvz_ids:
        return {
            "accessible_pvz_ids": requested_pvz_ids,
            "skipped_pvz_ids": [],
            "discovery_scope": None,
        }

    discovery_scope = discover_available_pvz_scope(
        configured_pvz_id=configured_pvz_id,
        logger=logger,
        parser_logger=parser_logger,
    )
    normalized_available_pvz = discovery_scope.get("normalized_available_pvz", set())
    accessible_pvz_ids = []
    skipped_pvz_ids = []

    for pvz_id in requested_pvz_ids:
        if normalize_pvz_id(pvz_id) in normalized_available_pvz:
            accessible_pvz_ids.append(pvz_id)
        else:
            skipped_pvz_ids.append(pvz_id)

    if logger and skipped_pvz_ids:
        logger.warning(
            f"Недоступные для текущей учетной записи PVZ исключены из backfill: {skipped_pvz_ids}"
        )

    return {
        "accessible_pvz_ids": accessible_pvz_ids,
        "skipped_pvz_ids": skipped_pvz_ids,
        "discovery_scope": discovery_scope,
    }


def prepare_coverage_filters(date_from, date_to, pvz_id):
    return {
        "Дата_from": date_from,
        "Дата_to": date_to,
        "ПВЗ": [normalize_pvz_id(pvz_id)],
    }


def parse_sheet_date_to_iso(sheet_date):
    return datetime.strptime(sheet_date, "%d.%m.%Y").strftime("%Y-%m-%d")


def detect_missing_report_dates(date_from, date_to, logger=None, max_missing_dates=None, pvz_id=PVZ_ID):
    logger = logger or create_uploader_logger()
    connection_params = prepare_connection_params()
    filters = prepare_coverage_filters(date_from=date_from, date_to=date_to, pvz_id=pvz_id)

    logger.info(f"Проверка покрытия Google Sheets за диапазон {date_from}..{date_to} для PVZ {PVZ_ID}")
    result = check_missing_items(
        filters=filters,
        connection_params=connection_params,
        logger=logger,
        strict_headers=BACKFILL_CONFIG.get("strict_headers", True),
        max_scan_rows=BACKFILL_CONFIG.get("max_scan_rows"),
        max_expected_keys=BACKFILL_CONFIG.get("max_expected_keys", 1000),
    )

    if not result.get("success", False):
        return {
            "success": False,
            "pvz_id": pvz_id,
            "date_from": date_from,
            "date_to": date_to,
            "missing_dates": [],
            "coverage_result": result,
            "error": result.get("error", "coverage_check_failed"),
        }

    missing_dates = []
    for item in result.get("data", {}).get("missing_items", []):
        sheet_date = item.get("Дата")
        if sheet_date:
            missing_dates.append(parse_sheet_date_to_iso(sheet_date))

    missing_dates = sorted(set(missing_dates))
    limit = max_missing_dates if max_missing_dates is not None else BACKFILL_CONFIG.get("max_missing_dates_per_run")
    truncated = False
    if limit and len(missing_dates) > limit:
        missing_dates = missing_dates[:limit]
        truncated = True

    logger.info(f"Coverage-check завершен: missing_dates={len(missing_dates)}, truncated={truncated}")
    return {
        "success": True,
        "pvz_id": pvz_id,
        "date_from": date_from,
        "date_to": date_to,
        "missing_dates": missing_dates,
        "coverage_result": result,
        "truncated": truncated,
    }


def detect_missing_report_dates_by_pvz(date_from, date_to, pvz_ids, logger=None, max_missing_dates=None):
    logger = logger or create_uploader_logger()
    resolved_pvz_ids = resolve_pvz_ids(pvz_ids)
    missing_dates_by_pvz = {}
    coverage_results_by_pvz = {}
    truncated_pvz_ids = []

    for pvz_id in resolved_pvz_ids:
        coverage_result = detect_missing_report_dates(
            date_from=date_from,
            date_to=date_to,
            logger=logger,
            max_missing_dates=max_missing_dates,
            pvz_id=pvz_id,
        )
        if not coverage_result.get("success", False):
            return {
                "success": False,
                "pvz_id": pvz_id,
                "date_from": date_from,
                "date_to": date_to,
                "missing_dates_by_pvz": missing_dates_by_pvz,
                "coverage_results_by_pvz": coverage_results_by_pvz,
                "error": coverage_result.get("error", "coverage_check_failed"),
            }

        missing_dates_by_pvz[pvz_id] = coverage_result.get("missing_dates", [])
        coverage_results_by_pvz[pvz_id] = coverage_result
        if coverage_result.get("truncated"):
            truncated_pvz_ids.append(pvz_id)

    return {
        "success": True,
        "date_from": date_from,
        "date_to": date_to,
        "pvz_ids": resolved_pvz_ids,
        "missing_dates_by_pvz": missing_dates_by_pvz,
        "coverage_results_by_pvz": coverage_results_by_pvz,
        "truncated_pvz_ids": truncated_pvz_ids,
    }


def prepare_upload_data(parsing_result=None):
    upload_data_list = []

    if parsing_result and isinstance(parsing_result, dict):
        formatted_record = {}

        if "execution_date" in parsing_result:
            original_date = parsing_result["execution_date"]
            try:
                parsed_date = datetime.strptime(original_date, "%Y-%m-%d")
                formatted_record["Дата"] = parsed_date.strftime("%d.%m.%Y")
            except ValueError:
                formatted_record["Дата"] = original_date

        if "location_info" in parsing_result:
            formatted_record["ПВЗ"] = parsing_result["location_info"]

        if "summary" in parsing_result and isinstance(parsing_result["summary"], dict):
            summary = parsing_result["summary"]

            if "giveout" in summary and isinstance(summary["giveout"], dict) and "value" in summary["giveout"]:
                formatted_record["Количество выдач"] = summary["giveout"]["value"]

            if "direct_flow_total" in summary and isinstance(summary["direct_flow_total"], dict):
                if "total_carriages" in summary["direct_flow_total"]:
                    formatted_record["Прямой поток"] = summary["direct_flow_total"]["total_carriages"]

            if "return_flow_total" in summary and isinstance(summary["return_flow_total"], dict):
                if "total_carriages" in summary["return_flow_total"]:
                    formatted_record["Возвратный поток"] = summary["return_flow_total"]["total_carriages"]

        for key, value in parsing_result.items():
            if key not in ["summary", "location_info", "execution_date", "extraction_timestamp", "source_url"]:
                formatted_record[key.title()] = value

        formatted_record["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if "Дата" in formatted_record and "ПВЗ" in formatted_record:
            upload_data_list.append(formatted_record)
        else:
            upload_record = transform_record_for_upload(parsing_result)
            if upload_record:
                upload_data_list.append(upload_record)

    return upload_data_list


def prepare_upload_data_batch(batch_parsing_result=None):
    upload_data_list = []
    if not batch_parsing_result or not isinstance(batch_parsing_result, dict):
        return upload_data_list

    for date_result in batch_parsing_result.get("results_by_date", {}).values():
        if not date_result.get("success", False):
            continue
        parsing_data = date_result.get("data")
        if parsing_data:
            upload_data_list.extend(prepare_upload_data(parsing_data))

    return upload_data_list


def run_upload_microservice(parsing_result=None):
    logger = create_uploader_logger()
    logger.info("Запуск изолированного микросервиса загрузчика данных в Google Sheets")

    connection_params = prepare_connection_params()
    upload_data_list = prepare_upload_data(parsing_result)

    connection_result = test_upload_connection(connection_params, logger=logger)
    logger.info(f"Результат проверки подключения: {connection_result}")
    if not connection_result.get("success", False):
        return {"success": False, "error": "Не удалось подключиться к Google Sheets"}

    upload_result = upload_batch_data(
        data_list=upload_data_list,
        connection_params=connection_params,
        logger=logger,
        strategy="update_or_append",
    )
    return upload_result


def run_upload_batch_microservice(batch_parsing_result=None):
    logger = create_uploader_logger()
    logger.info("Запуск batch upload в Google Sheets")

    connection_params = prepare_connection_params()
    upload_data_list = prepare_upload_data_batch(batch_parsing_result)

    if not upload_data_list:
        logger.warning("Для batch upload нет подготовленных записей")
        return {"success": False, "error": "Нет данных для загрузки", "uploaded_records": 0}

    def perform_upload_attempt():
        connection_result = test_upload_connection(connection_params, logger=logger)
        logger.info(f"Результат проверки подключения: {connection_result}")
        if not connection_result.get("success", False):
            return {
                "success": False,
                "error": "Не удалось подключиться к Google Sheets",
                "uploaded_records": 0,
                "connection_params_valid": connection_result.get("connection_params_valid"),
            }

        return upload_batch_data(
            data_list=upload_data_list,
            connection_params=connection_params,
            logger=logger,
            strategy="update_or_append",
        )

    upload_result = run_google_sheets_upload_with_retry(
        upload_callable=perform_upload_attempt,
        logger=logger,
    )
    upload_result["uploaded_records"] = len(upload_data_list)
    return upload_result


def transform_record_for_upload(record):
    if not isinstance(record, dict):
        return None

    upload_record = {}
    field_mapping = {
        "date": "Дата",
        "pvz": "ПВЗ",
        "issued_packages": "Количество выдач",
        "direct_flow": "Прямой поток",
        "return_flow": "Возвратный поток",
    }

    for source_field, target_field in field_mapping.items():
        if source_field in record:
            upload_record[target_field] = record[source_field]

    for key, value in record.items():
        if key not in field_mapping and key not in ["summary", "details", "timestamp"]:
            upload_record[key.replace("_", " ").title()] = value

    if "Дата" not in upload_record:
        upload_record["Дата"] = datetime.now().strftime("%Y-%m-%d")
    if "ПВЗ" not in upload_record:
        upload_record["ПВЗ"] = "DEFAULT_PVZ"

    return upload_record


def prepare_notification_data(parsing_result=None):
    notification_data = {}

    if parsing_result and isinstance(parsing_result, dict):
        if "execution_date" in parsing_result:
            original_date = parsing_result["execution_date"]
            try:
                parsed_date = datetime.strptime(original_date, "%Y-%m-%d")
                notification_data["date"] = parsed_date.strftime("%d.%m.%Y")
            except ValueError:
                notification_data["date"] = original_date

        if "location_info" in parsing_result:
            notification_data["pvz"] = parsing_result["location_info"]

        if "summary" in parsing_result and isinstance(parsing_result["summary"], dict):
            summary = parsing_result["summary"]
            if "giveout" in summary and isinstance(summary["giveout"], dict) and "value" in summary["giveout"]:
                notification_data["issued_packages"] = summary["giveout"]["value"]
            if "direct_flow_total" in summary and isinstance(summary["direct_flow_total"], dict):
                if "total_carriages" in summary["direct_flow_total"]:
                    notification_data["direct_flow"] = summary["direct_flow_total"]["total_carriages"]
            if "return_flow_total" in summary and isinstance(summary["return_flow_total"], dict):
                if "total_carriages" in summary["return_flow_total"]:
                    notification_data["return_flow"] = summary["return_flow_total"]["total_carriages"]

    return notification_data


def format_notification_message(notification_data):
    return (
        f"KPI отчет за {notification_data.get('date', 'Неизвестно')}\n"
        f"ПВЗ: {notification_data.get('pvz', 'Неизвестно')}\n"
        f"Выдач: {notification_data.get('issued_packages', 0)}\n"
        f"Прямой поток: {notification_data.get('direct_flow', 0)}\n"
        f"Возвратный поток: {notification_data.get('return_flow', 0)}"
    )


def prepare_batch_notification_data(batch_result=None, upload_result=None, coverage_result=None, pvz_id=PVZ_ID):
    batch_result = batch_result or {}
    upload_result = upload_result or {}
    coverage_result = coverage_result or {}

    failed_dates = [
        date for date, result in batch_result.get("results_by_date", {}).items()
        if not result.get("success", False)
    ]

    return {
        "pvz": pvz_id,
        "date_from": coverage_result.get("date_from"),
        "date_to": coverage_result.get("date_to"),
        "missing_dates_count": len(coverage_result.get("missing_dates", [])),
        "successful_dates": batch_result.get("successful_dates", 0),
        "failed_dates": failed_dates,
        "uploaded_records": upload_result.get("uploaded_records", 0),
        "upload_success": upload_result.get("success", False),
    }


def format_batch_notification_message(notification_data):
    failed_dates = notification_data.get("failed_dates", [])
    failed_suffix = ", ".join(failed_dates[:5]) if failed_dates else "-"
    return (
        "KPI backfill\n"
        f"ПВЗ: {notification_data.get('pvz', '-')}\n"
        f"Диапазон: {notification_data.get('date_from', '-')} .. {notification_data.get('date_to', '-')}\n"
        f"Отсутствовало дат: {notification_data.get('missing_dates_count', 0)}\n"
        f"Успешно спарсено: {notification_data.get('successful_dates', 0)}\n"
        f"Загружено записей: {notification_data.get('uploaded_records', 0)}\n"
        f"Неуспешные даты: {failed_suffix}"
    )


def build_aggregated_backfill_summary(pvz_results=None, date_from=None, date_to=None):
    normalized_pvz_results = {}
    for pvz_id, pvz_result in (pvz_results or {}).items():
        normalized_pvz_results[pvz_id] = (
            pvz_result
            if isinstance(pvz_result, PVZExecutionResult)
            else build_pvz_execution_result(
                pvz_id=pvz_id,
                coverage_result=pvz_result.get("coverage_result", {}),
                batch_result=pvz_result.get("batch_result", {}),
                upload_result=pvz_result.get("upload_result", {}),
                notification_data=pvz_result.get("notification_data", {}),
            )
        )
    processed_pvz_count = len(normalized_pvz_results)
    missing_dates_count = 0
    successful_jobs_count = 0
    failed_jobs_count = 0
    uploaded_records = 0

    for pvz_result in normalized_pvz_results.values():
        missing_dates_count += pvz_result.missing_dates_count
        successful_jobs_count += pvz_result.successful_jobs_count
        failed_jobs_count += pvz_result.failed_jobs_count
        uploaded_records += pvz_result.uploaded_records

    return ReportsBackfillExecutionResult(
        date_from=date_from,
        date_to=date_to,
        processed_pvz_count=processed_pvz_count,
        missing_dates_count=missing_dates_count,
        successful_jobs_count=successful_jobs_count,
        failed_jobs_count=failed_jobs_count,
        uploaded_records=uploaded_records,
        pvz_results=normalized_pvz_results,
    )


def format_aggregated_backfill_notification_message(summary):
    pvz_parts = []
    for pvz_id, pvz_result in summary.pvz_results.items():
        pvz_parts.append(
            f"{pvz_id}: missing={pvz_result.missing_dates_count}, "
            f"ok={pvz_result.successful_jobs_count}, "
            f"failed={pvz_result.failed_jobs_count}, "
            f"uploaded={pvz_result.uploaded_records}"
        )

    details = "\n".join(pvz_parts) if pvz_parts else "-"
    return (
        "KPI multi-PVZ backfill\n"
        f"Диапазон: {summary.date_from or '-'} .. {summary.date_to or '-'}\n"
        f"Обработано PVZ: {summary.processed_pvz_count}\n"
        f"Найдено missing dates: {summary.missing_dates_count}\n"
        f"Успешно jobs: {summary.successful_jobs_count}\n"
        f"Неуспешно jobs: {summary.failed_jobs_count}\n"
        f"Загружено записей: {summary.uploaded_records}\n"
        f"Детали:\n{details}"
    )


def send_notification_microservice(notification_message, logger=None):
    logger = logger or create_notification_logger()
    logger.info("Подготовка к отправке уведомления через микросервис notifications...")

    try:
        from scheduler_runner.tasks.reports.config.reports_paths import REPORTS_PATHS

        connection_params = REPORTS_PATHS.get("NOTIFICATION_CONNECTION_PARAMS", {})
        provider = connection_params.get("NOTIFICATION_PROVIDER", "telegram")

        if not connection_params:
            logger.error("Отсутствуют параметры подключения для notifications transport")
            return {"success": False, "error": "Отсутствуют параметры подключения для notifications transport"}

        logger.info(f"Выбран notification provider: {provider}")

        notification_result = send_notification(
            message=notification_message,
            connection_params=connection_params,
            logger=logger,
        )
        logger.info(f"Результат отправки уведомления: {notification_result}")
        return notification_result

    except Exception as e:
        logger.error(f"Ошибка при отправке уведомления: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


def main():
    parser = argparse.ArgumentParser(description="Продуктовый процессор домена reports")
    parser.add_argument("--execution_date", "-d", help="Дата выполнения в формате YYYY-MM-DD для single-режима")
    parser.add_argument("--date_from", help="Начало backfill-диапазона в формате YYYY-MM-DD")
    parser.add_argument("--date_to", help="Конец backfill-диапазона в формате YYYY-MM-DD")
    parser.add_argument(
        "--backfill_days",
        type=int,
        default=BACKFILL_CONFIG.get("default_days", 7),
        help="Окно backfill в днях при автоматическом вычислении диапазона",
    )
    parser.add_argument("--mode", choices=["single", "backfill"], default=None, help="Режим запуска")
    parser.add_argument(
        "--max_missing_dates",
        type=int,
        default=BACKFILL_CONFIG.get("max_missing_dates_per_run", 7),
        help="Максимальное количество missing dates за один batch-run",
    )
    parser.add_argument("--detailed_logs", action="store_true", help="Включить детализированное логирование")

    parser.add_argument(
        "--parser_api",
        choices=["legacy", "new"],
        default=BACKFILL_CONFIG.get("default_parser_api", "legacy"),
        help="Путь вызова parser API",
    )

    parser.add_argument(
        "--enable_failover_coordination",
        action="store_true",
        default=BACKFILL_CONFIG.get("enable_failover_coordination", False),
        help="Enable coordination pass via KPI_FAILOVER_STATE worksheet",
    )
    parser.add_argument("--pvz", action="append", default=None, help="PVZ for backfill; may be passed multiple times")
    args = parser.parse_args()
    args.enable_failover_coordination = getattr(
        args,
        "enable_failover_coordination",
        BACKFILL_CONFIG.get("enable_failover_coordination", False),
    )
    processor_logger = configure_logger(user="reports_domain", task_name="Processor", detailed=args.detailed_logs)
    parser_logger = create_parser_logger()
    effective_mode = args.mode or ("single" if args.execution_date else "backfill")
    source_run_id = build_processor_run_id(PVZ_ID)

    try:
        processor_logger.info(f"Запуск reports_processor в режиме: {effective_mode}")

        if effective_mode == "single":
            execution_date = args.execution_date or datetime.now().strftime("%Y-%m-%d")
            parsing_result = invoke_parser_for_single_date(
                execution_date=execution_date,
                parser_api=args.parser_api,
                pvz_id=PVZ_ID,
                logger=parser_logger,
            )

            if parsing_result and isinstance(parsing_result, dict) and ("summary" in parsing_result or "issued_packages" in parsing_result):
                upload_result = run_upload_microservice(parsing_result)
                if upload_result and upload_result.get("success", False):
                    notification_data = prepare_notification_data(parsing_result)
                    notification_message = format_notification_message(notification_data)
                    send_notification_microservice(notification_message, logger=create_notification_logger())
                else:
                    create_uploader_logger().warning("Микросервис загрузчика завершился с ошибкой, пропускаем отправку уведомления")
            else:
                parser_logger.warning("Микросервис парсера не завершился успешно, пропускаем загрузку данных и уведомление")
        else:
            date_to = args.date_to or datetime.now().strftime("%Y-%m-%d")
            if args.date_from:
                date_from = args.date_from
            else:
                date_from = (
                    datetime.strptime(date_to, "%Y-%m-%d") - timedelta(days=max(args.backfill_days - 1, 0))
                ).strftime("%Y-%m-%d")

            access_scope = resolve_accessible_pvz_ids(
                raw_pvz_ids=args.pvz,
                configured_pvz_id=PVZ_ID,
                logger=processor_logger,
                parser_logger=parser_logger,
            )
            resolved_pvz_ids = access_scope.get("accessible_pvz_ids", [])
            should_run_failover_coordination = should_run_automatic_failover_coordination(
                enabled=args.enable_failover_coordination,
                raw_pvz_ids=args.pvz,
                resolved_pvz_ids=resolved_pvz_ids,
                configured_pvz_id=PVZ_ID,
            )
            if not resolved_pvz_ids:
                processor_logger.info("Backfill остановлен: среди запрошенных PVZ нет доступных для текущей учетной записи")
                if should_run_failover_coordination:
                    run_failover_coordination_pass(
                        configured_pvz_id=PVZ_ID,
                        parser_api=args.parser_api,
                        parser_logger=parser_logger,
                        processor_logger=processor_logger,
                        source_run_id=source_run_id,
                    )
                return
            if len(resolved_pvz_ids) > 1:
                coverage_result = detect_missing_report_dates_by_pvz(
                    date_from=date_from,
                    date_to=date_to,
                    pvz_ids=resolved_pvz_ids,
                    logger=create_uploader_logger(),
                    max_missing_dates=args.max_missing_dates,
                )
                if not coverage_result.get("success", False):
                    raise Exception(coverage_result.get("error", "coverage_check_failed"))

                jobs = build_jobs_from_missing_dates_by_pvz(coverage_result.get("missing_dates_by_pvz", {}))
                grouped_jobs = group_jobs_by_pvz(jobs)
                batch_results_by_pvz = invoke_parser_for_grouped_jobs(
                    grouped_jobs=grouped_jobs,
                    pvz_ids=resolved_pvz_ids,
                    parser_api=args.parser_api,
                    logger=parser_logger,
                )
                had_missing_dates = False
                pvz_results = {}
                for pvz_id in resolved_pvz_ids:
                    pvz_jobs = grouped_jobs.get(pvz_id, [])
                    missing_dates = [job.execution_date for job in pvz_jobs]
                    if not missing_dates:
                        processor_logger.info(f"Backfill for PVZ {pvz_id} is not required: no missing dates found")
                        continue

                    had_missing_dates = True
                    batch_result = batch_results_by_pvz[pvz_id]
                    upload_result = run_upload_batch_microservice(batch_result)
                    notification_data = prepare_batch_notification_data(
                        batch_result=batch_result,
                        upload_result=upload_result,
                        coverage_result=coverage_result.get("coverage_results_by_pvz", {}).get(pvz_id, {}),
                        pvz_id=pvz_id,
                    )
                    pvz_results[pvz_id] = build_pvz_execution_result(
                        pvz_id=pvz_id,
                        coverage_result=coverage_result.get("coverage_results_by_pvz", {}).get(pvz_id, {}),
                        batch_result=batch_result,
                        upload_result=upload_result,
                        notification_data=notification_data,
                    )

                if not had_missing_dates:
                    processor_logger.info("Backfill РЅРµ С‚СЂРµР±СѓРµС‚СЃСЏ: РѕС‚СЃСѓС‚СЃС‚РІСѓСЋС‰РёС… РґР°С‚ РЅРµ РЅР°Р№РґРµРЅРѕ РЅРё РґР»СЏ РѕРґРЅРѕРіРѕ PVZ")
                if had_missing_dates:
                    aggregated_summary = build_aggregated_backfill_summary(
                        pvz_results=pvz_results,
                        date_from=date_from,
                        date_to=date_to,
                    )
                    reports_run_summary = build_reports_run_summary(
                        mode="backfill_multi_pvz",
                        configured_pvz_id=PVZ_ID,
                        date_from=date_from,
                        date_to=date_to,
                        multi_pvz=aggregated_summary,
                        failover=build_failover_run_summary(enabled=False),
                    )
                    notification_message = format_reports_run_notification_message(reports_run_summary)
                    send_notification_microservice(notification_message, logger=create_notification_logger())
                return

            pvz_id = resolved_pvz_ids[0]
            coverage_result = detect_missing_report_dates(
                date_from=date_from,
                date_to=date_to,
                logger=create_uploader_logger(),
                max_missing_dates=args.max_missing_dates,
                pvz_id=pvz_id,
            )
            if not coverage_result.get("success", False):
                raise Exception(coverage_result.get("error", "coverage_check_failed"))

            missing_dates = coverage_result.get("missing_dates", [])
            should_run_failover_coordination_for_owner = should_run_automatic_failover_coordination(
                enabled=args.enable_failover_coordination,
                raw_pvz_ids=args.pvz,
                resolved_pvz_ids=resolved_pvz_ids,
                current_pvz_id=pvz_id,
                configured_pvz_id=PVZ_ID,
            )

            batch_result = {}
            upload_result = {}
            owner_state_sync_result = {
                "attempted": False,
                "success": None,
                "error": "",
            }
            if missing_dates:
                jobs = build_jobs_for_pvz(pvz_id=pvz_id, execution_dates=missing_dates)
                batch_result = invoke_parser_for_pvz(parser_api=args.parser_api, jobs=jobs, logger=parser_logger)
                upload_result = run_upload_batch_microservice(batch_result)

                if should_run_failover_coordination_for_owner:
                    if upload_result.get("success", False):
                        owner_state_sync_result["attempted"] = True
                        try:
                            owner_state_sync_payload = sync_owner_failover_state_from_batch_result(
                                owner_pvz=pvz_id,
                                missing_dates=missing_dates,
                                batch_result=batch_result,
                                upload_result=upload_result,
                                logger=create_failover_state_logger(),
                                source_run_id=source_run_id,
                            )
                            owner_state_sync_result["success"] = True
                            owner_state_sync_result["payload"] = owner_state_sync_payload
                            processor_logger.info(
                                "Owner state sync metrics: "
                                f"prefetch_keys={owner_state_sync_payload.get('existing_state_prefetch_keys_count', 0)}, "
                                f"prefetch_rows_found={owner_state_sync_payload.get('existing_state_prefetch_rows_found', 0)}, "
                                f"persisted_rows={owner_state_sync_payload.get('persisted_rows_count', 0)}, "
                                f"suppressed_success={len(owner_state_sync_payload.get('suppressed_success_dates', []))}, "
                                f"upsert_updated={owner_state_sync_payload.get('upsert_diagnostics', {}).get('updated_count', 0)}, "
                                f"upsert_appended={owner_state_sync_payload.get('upsert_diagnostics', {}).get('appended_count', 0)}, "
                                f"upsert_prefetch_matches={owner_state_sync_payload.get('upsert_diagnostics', {}).get('prefetch_matches_count', 0)}"
                            )
                        except Exception as exc:
                            owner_state_sync_result["success"] = False
                            owner_state_sync_result["error"] = str(exc)
                            create_failover_state_logger().error(
                                f"Не удалось синхронизировать owner state в KPI_FAILOVER_STATE: {exc}",
                                exc_info=True,
                            )
                    else:
                        processor_logger.warning(
                            "Owner state sync skipped because KPI upload failed"
                        )
            elif not should_run_failover_coordination_for_owner:
                processor_logger.info("Backfill не требуется: отсутствующих дат не найдено")
                return
            else:
                processor_logger.info("Своих missing dates нет, переходим сразу к failover coordination pass")

            owner_summary = build_owner_run_summary(
                pvz_id=pvz_id,
                coverage_result=coverage_result,
                batch_result=batch_result,
                upload_result=upload_result,
            )

            failover_result = {}
            failover_result["owner_state_sync"] = owner_state_sync_result
            owner_upload_allows_failover = (not missing_dates) or upload_result.get("success", False)
            can_run_failover_pass = (
                should_run_failover_coordination_for_owner
                and owner_upload_allows_failover
                and owner_state_sync_result.get("success") is not False
            )
            if can_run_failover_pass:
                failover_result = run_failover_coordination_pass(
                    configured_pvz_id=PVZ_ID,
                    parser_api=args.parser_api,
                    parser_logger=parser_logger,
                    processor_logger=processor_logger,
                    source_run_id=source_run_id,
                )
                failover_result["owner_state_sync"] = owner_state_sync_result
            elif should_run_failover_coordination_for_owner and missing_dates and not upload_result.get("success", False):
                processor_logger.warning(
                    "Failover coordination pass skipped because owner KPI upload failed"
                )

            reports_run_summary = build_reports_run_summary(
                mode="backfill_single_pvz",
                configured_pvz_id=PVZ_ID,
                date_from=date_from,
                date_to=date_to,
                owner=owner_summary,
                failover=build_failover_run_summary(
                    enabled=should_run_failover_coordination_for_owner,
                    failover_result=failover_result,
                ),
            )
            notification_message = format_reports_run_notification_message(reports_run_summary)
            send_notification_microservice(notification_message, logger=create_notification_logger())

    except Exception as e:
        processor_logger.error(f"Произошла ошибка в продуктовом процессоре: {e}", exc_info=True)
        raise

    processor_logger.info("Продуктовый процессор домена reports завершен успешно")


if __name__ == "__main__":
    main()

