"""
reports_upload.py

Coverage-check logic, upload payload preparation, KPI upload orchestration и retry behavior.

Извлечено из reports_processor.py (Phase 1.3 — low-risk extraction).
"""
import logging
import time
from copy import deepcopy
from datetime import datetime

from config.base_config import PVZ_ID
from .config.scripts.kpi_google_sheets_config import KPI_GOOGLE_SHEETS_CONFIG
from .config.scripts.reports_processor_config import BACKFILL_CONFIG
from .reports_utils import normalize_pvz_id
from scheduler_runner.utils.logging import TRACE_LEVEL, configure_logger
from scheduler_runner.utils.uploader import (
    check_missing_items,
    test_connection as test_upload_connection,
    upload_batch_data,
)


# ──────────────────────────────────────────────
# Logger factory
# ──────────────────────────────────────────────

def create_uploader_logger():
    return configure_logger(
        user="reports_domain",
        task_name="Uploader",
        log_levels=[TRACE_LEVEL, logging.DEBUG],
        single_file_for_levels=False,
    )


# ──────────────────────────────────────────────
# Connection & PVZ helpers
# ──────────────────────────────────────────────

def prepare_connection_params():
    from .config.reports_paths import REPORTS_PATHS

    return {
        "CREDENTIALS_PATH": str(REPORTS_PATHS["GOOGLE_SHEETS_CREDENTIALS"]),
        "SPREADSHEET_ID": KPI_GOOGLE_SHEETS_CONFIG["SPREADSHEET_ID"],
        "WORKSHEET_NAME": KPI_GOOGLE_SHEETS_CONFIG["WORKSHEET_NAME"],
        "TABLE_CONFIG": deepcopy(KPI_GOOGLE_SHEETS_CONFIG["TABLE_CONFIG"]),
        "REQUIRED_CONNECTION_PARAMS": ["CREDENTIALS_PATH", "SPREADSHEET_ID", "WORKSHEET_NAME", "TABLE_CONFIG"],
    }


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


# ──────────────────────────────────────────────
# Coverage-check
# ──────────────────────────────────────────────

def prepare_coverage_filters(date_from, date_to, pvz_id):
    return {
        "work_date_from": date_from,
        "work_date_to": date_to,
        "object_name": [normalize_pvz_id(pvz_id)],
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
        sheet_date = item.get("work_date")
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


def resolve_pvz_ids(raw_pvz_ids=None):
    if raw_pvz_ids:
        return list(dict.fromkeys(raw_pvz_ids))
    return [PVZ_ID]


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


# ──────────────────────────────────────────────
# Upload preparation
# ──────────────────────────────────────────────

def transform_record_for_upload(record):
    if not isinstance(record, dict):
        return None

    upload_record = {}
    field_mapping = {
        "date": "work_date",
        "pvz": "object_name",
        "issued_packages": "issued_packages",
        "direct_flow": "direct_flow",
        "return_flow": "return_flow",
    }

    for source_field, target_field in field_mapping.items():
        if source_field in record:
            upload_record[target_field] = record[source_field]

    for key, value in record.items():
        if key not in field_mapping and key not in ["summary", "details", "timestamp"]:
            upload_record[key.replace("_", " ").title()] = value

    if "work_date" not in upload_record:
        upload_record["work_date"] = datetime.now().strftime("%Y-%m-%d")
    if "object_name" not in upload_record:
        upload_record["object_name"] = "DEFAULT_PVZ"

    return upload_record


def prepare_upload_data(parsing_result=None):
    upload_data_list = []

    if parsing_result and isinstance(parsing_result, dict):
        formatted_record = {}

        if "execution_date" in parsing_result:
            original_date = parsing_result["execution_date"]
            try:
                parsed_date = datetime.strptime(original_date, "%Y-%m-%d")
                formatted_record["work_date"] = parsed_date.strftime("%d.%m.%Y")
            except ValueError:
                formatted_record["work_date"] = original_date

        if "location_info" in parsing_result:
            formatted_record["object_name"] = parsing_result["location_info"]

        if "summary" in parsing_result and isinstance(parsing_result["summary"], dict):
            summary = parsing_result["summary"]

            if "giveout" in summary and isinstance(summary["giveout"], dict) and "value" in summary["giveout"]:
                formatted_record["issued_packages"] = summary["giveout"]["value"]

            if "direct_flow_total" in summary and isinstance(summary["direct_flow_total"], dict):
                if "total_carriages" in summary["direct_flow_total"]:
                    formatted_record["direct_flow"] = summary["direct_flow_total"]["total_carriages"]

            if "return_flow_total" in summary and isinstance(summary["return_flow_total"], dict):
                if "total_carriages" in summary["return_flow_total"]:
                    formatted_record["return_flow"] = summary["return_flow_total"]["total_carriages"]

        for key, value in parsing_result.items():
            if key not in ["summary", "location_info", "execution_date", "extraction_timestamp", "source_url"]:
                formatted_record[key.title()] = value

        formatted_record["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if "work_date" in formatted_record and "object_name" in formatted_record:
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


# ──────────────────────────────────────────────
# Upload orchestration
# ──────────────────────────────────────────────

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

