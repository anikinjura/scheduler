"""
reports_summary.py

Единая модель итогов run и единая логика финального статуса.

Извлечено из reports_processor.py (Phase 1.1 — low-risk extraction).
"""
from copy import deepcopy
from dataclasses import dataclass


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


# ──────────────────────────────────────────────
# Build helpers
# ──────────────────────────────────────────────

def build_pvz_execution_result(pvz_id, coverage_result=None, batch_result=None, upload_result=None, notification_data=None):
    return PVZExecutionResult(
        pvz_id=pvz_id,
        coverage_result=deepcopy(coverage_result or {}),
        batch_result=deepcopy(batch_result or {}),
        upload_result=deepcopy(upload_result or {}),
        notification_data=deepcopy(notification_data or {}),
    )


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


# ──────────────────────────────────────────────
# Status resolution helpers
# ──────────────────────────────────────────────

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

