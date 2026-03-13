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
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta

from config.base_config import PVZ_ID
from scheduler_runner.tasks.reports.config.scripts.kpi_google_sheets_config import KPI_GOOGLE_SHEETS_CONFIG
from scheduler_runner.tasks.reports.config.scripts.reports_processor_config import BACKFILL_CONFIG
from scheduler_runner.utils.parser import (
    build_parser_definition,
    build_jobs_for_pvz,
    convert_job_results_to_batch_result,
    create_parser_logger,
    execute_parser_internal,
    execute_parser_jobs_for_pvz,
    invoke_parser_for_grouped_jobs,
    invoke_parser_for_pvz,
    invoke_parser_for_single_date,
)
from scheduler_runner.utils.logging import TRACE_LEVEL, configure_logger
from scheduler_runner.utils.notifications import (
    send_notification,
    test_connection as test_notification_connection,
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

    connection_result = test_upload_connection(connection_params, logger=logger)
    logger.info(f"Результат проверки подключения: {connection_result}")
    if not connection_result.get("success", False):
        return {"success": False, "error": "Не удалось подключиться к Google Sheets", "uploaded_records": 0}

    upload_result = upload_batch_data(
        data_list=upload_data_list,
        connection_params=connection_params,
        logger=logger,
        strategy="update_or_append",
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
    logger.info("Подготовка к отправке уведомления в Telegram...")

    try:
        from scheduler_runner.tasks.reports.config.reports_paths import REPORTS_PATHS

        token = REPORTS_PATHS.get("TELEGRAM_TOKEN")
        chat_id = REPORTS_PATHS.get("TELEGRAM_CHAT_ID")

        if not token or not chat_id:
            logger.error("Отсутствуют параметры подключения для Telegram")
            return {"success": False, "error": "Отсутствуют параметры подключения для Telegram"}

        connection_params = {
            "TELEGRAM_BOT_TOKEN": token,
            "TELEGRAM_CHAT_ID": chat_id,
        }

        connection_result = test_notification_connection(connection_params, logger=logger)
        logger.info(f"Результат проверки подключения к Telegram: {connection_result}")
        if not connection_result.get("success", False):
            return {"success": False, "error": "Не удалось подключиться к Telegram"}

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

    parser.add_argument("--pvz", action="append", default=None, help="PVZ for backfill; may be passed multiple times")
    args = parser.parse_args()
    processor_logger = configure_logger(user="reports_domain", task_name="Processor", detailed=args.detailed_logs)
    effective_mode = args.mode or ("single" if args.execution_date else "backfill")

    try:
        processor_logger.info(f"Запуск reports_processor в режиме: {effective_mode}")

        if effective_mode == "single":
            execution_date = args.execution_date or datetime.now().strftime("%Y-%m-%d")
            parsing_result = invoke_parser_for_single_date(
                execution_date=execution_date,
                parser_api=args.parser_api,
                pvz_id=PVZ_ID,
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
                create_parser_logger().warning("Микросервис парсера не завершился успешно, пропускаем загрузку данных и уведомление")
        else:
            date_to = args.date_to or datetime.now().strftime("%Y-%m-%d")
            if args.date_from:
                date_from = args.date_from
            else:
                date_from = (
                    datetime.strptime(date_to, "%Y-%m-%d") - timedelta(days=max(args.backfill_days - 1, 0))
                ).strftime("%Y-%m-%d")

            resolved_pvz_ids = resolve_pvz_ids(args.pvz)
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
                    notification_message = format_aggregated_backfill_notification_message(aggregated_summary)
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
            if not missing_dates:
                processor_logger.info("Backfill не требуется: отсутствующих дат не найдено")
                return

            jobs = build_jobs_for_pvz(pvz_id=pvz_id, execution_dates=missing_dates)
            batch_result = invoke_parser_for_pvz(parser_api=args.parser_api, jobs=jobs)
            upload_result = run_upload_batch_microservice(batch_result)
            notification_data = prepare_batch_notification_data(
                batch_result=batch_result,
                upload_result=upload_result,
                coverage_result=coverage_result,
                pvz_id=pvz_id,
            )
            notification_message = format_batch_notification_message(notification_data)
            send_notification_microservice(notification_message, logger=create_notification_logger())

    except Exception as e:
        processor_logger.error(f"Произошла ошибка в продуктовом процессоре: {e}", exc_info=True)
        raise

    processor_logger.info("Продуктовый процессор домена reports завершен успешно")


if __name__ == "__main__":
    main()
