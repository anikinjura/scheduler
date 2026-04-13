from __future__ import annotations

import json
import urllib.request
from contextlib import contextmanager, nullcontext
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, Optional

from ..config.reports_paths import REPORTS_PATHS
from ..config.scripts.kpi_failover_state_google_sheets_config import (
    KPI_FAILOVER_STATE_GOOGLE_SHEETS_CONFIG,
)
from ..config.scripts.reports_processor_config import BACKFILL_CONFIG
from scheduler_runner.utils.logging import TRACE_LEVEL, configure_logger
from scheduler_runner.utils.uploader.core.providers.google_sheets.google_sheets_core import retry_on_api_error
from scheduler_runner.utils.uploader.core.providers.google_sheets.google_sheets_data_models import (
    ColumnType,
    _index_to_column_letter,
)
from scheduler_runner.utils.uploader.implementations.google_sheets_uploader import GoogleSheetsUploader


STATUS_OWNER_PENDING = "owner_pending"
STATUS_OWNER_SUCCESS = "owner_success"
STATUS_OWNER_FAILED = "owner_failed"
STATUS_FAILOVER_CLAIMED = "failover_claimed"
STATUS_FAILOVER_SUCCESS = "failover_success"
STATUS_FAILOVER_FAILED = "failover_failed"
STATUS_CLAIM_EXPIRED = "claim_expired"

TERMINAL_STATUSES = {STATUS_OWNER_SUCCESS, STATUS_FAILOVER_SUCCESS}
CANDIDATE_SCAN_COLUMN_NAMES = [
    "work_date",
    "target_object_name",
    "owner_object_name",
    "status",
    "claimed_by",
    "claim_expires_at",
    "attempt_no",
    "source_run_id",
    "last_error",
    "updated_at",
]
FAILOVER_STATE_UPSERT_KEY_COLUMNS = ["work_date", "target_object_name"]
FAILOVER_STATE_ALL_COLUMN_NAMES = [
    "request_id",
    "work_date",
    "target_object_name",
    "owner_object_name",
    "status",
    "claimed_by",
    "claim_expires_at",
    "attempt_no",
    "source_run_id",
    "last_error",
    "updated_at",
    "timestamp",
]


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


def build_failover_request_id(execution_date: str, target_object_name: str) -> str:
    return f"{execution_date}|{target_object_name}"


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
    target_object_name: str,
    owner_object_name: str,
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
        "request_id": build_failover_request_id(execution_date, target_object_name),
        "work_date": execution_date,
        "target_object_name": target_object_name,
        "owner_object_name": owner_object_name,
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


def upsert_failover_state(record: Dict[str, Any], logger=None, uploader=None) -> Dict[str, Any]:
    logger = logger or create_failover_state_logger()
    connection_context = nullcontext(uploader) if uploader is not None else failover_state_connection(logger=logger)
    with connection_context as active_uploader:
        return active_uploader._perform_upload(record, strategy="update_or_append")


def _normalize_failover_lookup_value(reporter, value: Any) -> str:
    return reporter._normalize_for_comparison(reporter._prepare_value_for_search(value))


def _build_failover_record_lookup_key(record: Dict[str, Any], reporter) -> tuple[str, str]:
    return tuple(
        _normalize_failover_lookup_value(reporter, record.get(column_name, ""))
        for column_name in FAILOVER_STATE_UPSERT_KEY_COLUMNS
    )


def _flatten_single_column_batch_values(values):
    flattened = []
    for item in values or []:
        if isinstance(item, list):
            flattened.append(item[0] if item else "")
        else:
            flattened.append(item)
    return flattened


def get_failover_state_rows_by_keys(
    *,
    keys: list[Dict[str, str]],
    logger=None,
    uploader=None,
) -> Dict[tuple[str, str], Dict[str, Any]]:
    logger = logger or create_failover_state_logger()
    normalized_keys_input = [key for key in (keys or []) if key]
    if not normalized_keys_input:
        return {}

    connection_context = nullcontext(uploader) if uploader is not None else failover_state_connection(logger=logger)
    with connection_context as active_uploader:
        reporter = active_uploader.sheets_reporter
        worksheet = reporter.worksheet
        table_config = active_uploader.table_config

        target_keys = {
            (
                _normalize_failover_lookup_value(reporter, key.get("work_date", "")),
                _normalize_failover_lookup_value(reporter, key.get("target_object_name", "")),
            )
            for key in normalized_keys_input
        }

        date_column_index = table_config.get_column_index("Дата") or 2
        last_data_row = reporter.get_last_row_with_data(column_index=date_column_index)
        if last_data_row < 2:
            return {}

        ranges = []
        column_names = []
        for column_name in FAILOVER_STATE_ALL_COLUMN_NAMES:
            column_letter = table_config.get_column_letter(column_name)
            if not column_letter:
                column_index = table_config.get_column_index(column_name)
                if not column_index:
                    continue
                column_letter = _index_to_column_letter(column_index)
            ranges.append(f"{column_letter}2:{column_letter}{last_data_row}")
            column_names.append(column_name)

        if not ranges:
            return {}

        batch_values = worksheet.batch_get(ranges)

    column_values = {
        column_name: _flatten_single_column_batch_values(values)
        for column_name, values in zip(column_names, batch_values)
    }
    max_len = max((len(values) for values in column_values.values()), default=0)
    matched_rows: Dict[tuple[str, str], Dict[str, Any]] = {}

    for index in range(max_len):
        row = {}
        has_meaningful_data = False
        for column_name in column_names:
            values = column_values.get(column_name, [])
            value = values[index] if index < len(values) else ""
            if value not in ("", None):
                has_meaningful_data = True
            row[column_name] = value

        if not has_meaningful_data:
            continue

        row_key = (
            _normalize_failover_lookup_value(reporter, row.get("work_date", "")),
            _normalize_failover_lookup_value(reporter, row.get("target_object_name", "")),
        )
        if row_key not in target_keys or row_key in matched_rows:
            continue

        row["_row_number"] = index + 2
        matched_rows[row_key] = row

    return matched_rows


def _build_existing_failover_row_lookup(records: list[Dict[str, Any]], uploader) -> tuple[Dict[tuple[str, str], int], int]:
    reporter = uploader.sheets_reporter
    table_config = uploader.table_config
    date_column_index = table_config.get_column_index("work_date") or 2
    last_data_row = reporter.get_last_row_with_data(column_index=date_column_index)
    if last_data_row < 2:
        return {}, last_data_row

    target_keys = {
        _build_failover_record_lookup_key(record, reporter)
        for record in records
        if record
    }
    if not target_keys:
        return {}, last_data_row

    ranges = []
    key_columns = []
    for column_name in FAILOVER_STATE_UPSERT_KEY_COLUMNS:
        column_letter = table_config.get_column_letter(column_name)
        if not column_letter:
            column_index = table_config.get_column_index(column_name)
            if not column_index:
                continue
            column_letter = _index_to_column_letter(column_index)
        ranges.append(f"{column_letter}2:{column_letter}{last_data_row}")
        key_columns.append(column_name)

    if len(key_columns) != len(FAILOVER_STATE_UPSERT_KEY_COLUMNS):
        return {}, last_data_row

    batch_values = reporter.worksheet.batch_get(ranges)
    column_values = {
        column_name: _flatten_single_column_batch_values(values)
        for column_name, values in zip(key_columns, batch_values)
    }
    max_len = max((len(values) for values in column_values.values()), default=0)
    row_lookup: Dict[tuple[str, str], int] = {}

    for index in range(max_len):
        key_parts = []
        has_meaningful_data = False
        for column_name in FAILOVER_STATE_UPSERT_KEY_COLUMNS:
            values = column_values.get(column_name, [])
            raw_value = values[index] if index < len(values) else ""
            if raw_value not in ("", None):
                has_meaningful_data = True
            key_parts.append(_normalize_failover_lookup_value(reporter, raw_value))

        if not has_meaningful_data:
            continue

        key_tuple = tuple(key_parts)
        if key_tuple in target_keys and key_tuple not in row_lookup:
            row_lookup[key_tuple] = index + 2

    return row_lookup, last_data_row


def _prepare_failover_row_values(headers, config, data: Dict[str, Any], row_number: int) -> list[Any]:
    values = []
    for header in headers:
        col_def = config.get_column(header)
        if not col_def:
            values.append("")
            continue

        if col_def.column_type == ColumnType.FORMULA and col_def.formula_template:
            values.append(col_def.formula_template.replace("{row}", str(row_number)))
        elif col_def.name in data:
            values.append(data[col_def.name])
        else:
            values.append("")
    return values


@retry_on_api_error(max_retries=3, base_delay=1.0, max_delay=10.0)
def _append_failover_state_rows_bulk(uploader, *, rows_to_append: list[tuple[int, Dict[str, Any]]]) -> list[Dict[str, Any]]:
    reporter = uploader.sheets_reporter
    config = uploader.table_config
    headers = reporter.worksheet.row_values(1)
    values = [
        _prepare_failover_row_values(headers, config, record, row_number)
        for row_number, record in rows_to_append
    ]
    reporter.worksheet.append_rows(values=values, value_input_option="USER_ENTERED")

    return [
        reporter._create_result(
            success=True,
            action="appended",
            message=f"Строка {row_number} добавлена",
            data=record,
            row_number=row_number,
        )
        for row_number, record in rows_to_append
    ]


def upsert_failover_state_records(records: list[Dict[str, Any]], logger=None) -> Dict[str, Any]:
    logger = logger or create_failover_state_logger()
    normalized_records = [record for record in (records or []) if record]
    if not normalized_records:
        return {
            "success": True,
            "updated": 0,
            "results": [],
            "diagnostics": {
                "requested_records_count": 0,
                "prefetch_last_data_row": 0,
                "prefetch_keys_count": 0,
                "prefetch_matches_count": 0,
                "updated_count": 0,
                "appended_count": 0,
            },
        }

    results = []
    with failover_state_connection(logger=logger) as uploader:
        row_lookup, last_data_row = _build_existing_failover_row_lookup(normalized_records, uploader)
        reporter = uploader.sheets_reporter
        append_queue = []
        next_append_row = max(last_data_row, 1) + 1

        for record in normalized_records:
            existing_row = row_lookup.get(_build_failover_record_lookup_key(record, reporter))
            if existing_row:
                results.append(
                    reporter._update_existing_row(
                        row_number=existing_row,
                        data=record,
                        config=uploader.table_config,
                        formula_row_placeholder="{row}",
                    )
                )
                continue

            append_queue.append((next_append_row, record))
            next_append_row += 1

        if append_queue:
            results.extend(_append_failover_state_rows_bulk(uploader, rows_to_append=append_queue))

    diagnostics = {
        "requested_records_count": len(normalized_records),
        "prefetch_last_data_row": last_data_row,
        "prefetch_keys_count": len({
            (record.get("work_date", ""), record.get("target_object_name", ""))
            for record in normalized_records
        }),
        "prefetch_matches_count": len(row_lookup),
        "updated_count": sum(1 for result in results if result.get("action") == "updated"),
        "appended_count": sum(1 for result in results if result.get("action") == "appended"),
    }

    return {
        "success": all(result.get("success", False) for result in results),
        "updated": len(results),
        "results": results,
        "diagnostics": diagnostics,
    }


def get_failover_state(execution_date: str, target_object_name: str, logger=None, uploader=None) -> Optional[Dict[str, Any]]:
    logger = logger or create_failover_state_logger()
    connection_context = nullcontext(uploader) if uploader is not None else failover_state_connection(logger=logger)
    with connection_context as active_uploader:
        reporter = active_uploader.sheets_reporter
        use_config = active_uploader.table_config
        return reporter.get_row_by_unique_keys(
            unique_key_values={
                "work_date": execution_date,
                "target_object_name": target_object_name,
            },
            config=use_config,
            return_raw=True,
        )


def list_failover_state_rows(
    *,
    statuses: Optional[Iterable[str]] = None,
    target_object_name: Optional[str] = None,
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
        if target_object_name and record.get("target_object_name") != target_object_name:
            continue
        if claimed_by and record.get("claimed_by") != claimed_by:
            continue
        filtered.append(record)
    return filtered


def _flatten_batch_column_values(values):
    flattened = []
    for item in values or []:
        if isinstance(item, list):
            flattened.append(item[0] if item else "")
        else:
            flattened.append(item)
    return flattened


def list_candidate_failover_rows_fast(
    *,
    statuses: Optional[Iterable[str]] = None,
    logger=None,
    uploader=None,
) -> list[Dict[str, Any]]:
    logger = logger or create_failover_state_logger()
    connection_context = nullcontext(uploader) if uploader is not None else failover_state_connection(logger=logger)

    with connection_context as active_uploader:
        reporter = active_uploader.sheets_reporter
        worksheet = reporter.worksheet
        table_config = active_uploader.table_config

        date_column_index = table_config.get_column_index("work_date") or 2
        last_data_row = reporter.get_last_row_with_data(column_index=date_column_index)
        if last_data_row < 2:
            return []

        ranges = []
        column_names = []
        for column_name in CANDIDATE_SCAN_COLUMN_NAMES:
            column_letter = table_config.get_column_letter(column_name)
            if not column_letter:
                column_index = table_config.get_column_index(column_name)
                if not column_index:
                    continue
                column_letter = _index_to_column_letter(column_index)
            ranges.append(f"{column_letter}2:{column_letter}{last_data_row}")
            column_names.append(column_name)

        if not ranges:
            return []

        batch_values = worksheet.batch_get(ranges)

    column_values = {
        column_name: _flatten_batch_column_values(values)
        for column_name, values in zip(column_names, batch_values)
    }
    max_len = max((len(values) for values in column_values.values()), default=0)
    allowed_statuses = set(statuses or [])
    records = []

    for index in range(max_len):
        record = {}
        has_meaningful_data = False
        for column_name in column_names:
            values = column_values.get(column_name, [])
            value = values[index] if index < len(values) else ""
            if value not in ("", None):
                has_meaningful_data = True
            record[column_name] = value

        if not has_meaningful_data:
            continue
        if allowed_statuses and record.get("status") not in allowed_statuses:
            continue
        records.append(record)

    return records


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
    target_object_name: str,
    claimer_pvz: str,
    source_run_id: str = "",
    logger=None,
    uploader=None,
) -> Dict[str, Any]:
    logger = logger or create_failover_state_logger()
    persisted_state = get_failover_state(
        execution_date=execution_date,
        target_object_name=target_object_name,
        logger=logger,
        uploader=uploader,
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
    target_object_name: str,
    owner_object_name: str,
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
        "target_object_name": target_object_name,
        "owner_object_name": owner_object_name,
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
    target_object_name: str,
    owner_object_name: str,
    claimer_pvz: str,
    ttl_minutes: int = 15,
    source_run_id: str = "",
    logger=None,
    uploader=None,
) -> Dict[str, Any]:
    logger = logger or create_failover_state_logger()
    now = _now()
    current_state = get_failover_state(
        execution_date=execution_date,
        target_object_name=target_object_name,
        logger=logger,
        uploader=uploader,
    )

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
        target_object_name=target_object_name,
        owner_object_name=owner_object_name,
        status=STATUS_FAILOVER_CLAIMED,
        claimed_by=claimer_pvz,
        claim_expires_at=claim_expires_at,
        attempt_no=previous_attempt_no + 1,
        source_run_id=source_run_id,
        last_error=current_state.get("last_error", "") if current_state else "",
        updated_at=now,
    )
    upload_result = upsert_failover_state(claimed_record, logger=logger, uploader=uploader)
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
        target_object_name=target_object_name,
        claimer_pvz=claimer_pvz,
        source_run_id=source_run_id,
        logger=logger,
        uploader=uploader,
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
    target_object_name: str,
    owner_object_name: str,
    claimer_pvz: str,
    ttl_minutes: int = 15,
    source_run_id: str = "",
    logger=None,
    uploader=None,
) -> Dict[str, Any]:
    claim_backend = get_failover_claim_backend()
    if claim_backend == "apps_script":
        return try_claim_failover_via_apps_script(
            execution_date=execution_date,
            target_object_name=target_object_name,
            owner_object_name=owner_object_name,
            claimer_pvz=claimer_pvz,
            ttl_minutes=ttl_minutes,
            source_run_id=source_run_id,
            logger=logger,
        )
    return try_claim_failover_via_sheets(
        execution_date=execution_date,
        target_object_name=target_object_name,
        owner_object_name=owner_object_name,
        claimer_pvz=claimer_pvz,
        ttl_minutes=ttl_minutes,
        source_run_id=source_run_id,
        logger=logger,
        uploader=uploader,
    )


def mark_failover_state(
    *,
    execution_date: str,
    target_object_name: str,
    owner_object_name: str,
    status: str,
    claimed_by: str = "",
    ttl_minutes: int = 0,
    source_run_id: str = "",
    last_error: str = "",
    logger=None,
) -> Dict[str, Any]:
    logger = logger or create_failover_state_logger()
    current_state = get_failover_state(execution_date=execution_date, target_object_name=target_object_name, logger=logger) or {}
    claim_expires_at = ""
    if ttl_minutes > 0:
        claim_expires_at = format_sheet_timestamp(_now() + timedelta(minutes=ttl_minutes))

    record = build_failover_state_record(
        execution_date=execution_date,
        target_object_name=target_object_name,
        owner_object_name=owner_object_name,
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

