"""
reports_notifications.py

Messaging, notification payload shaping и отправка через общий notifications layer.

Извлечено из reports_processor.py (Phase 1.2 — low-risk extraction).
"""
from copy import deepcopy
from datetime import datetime
import logging

from scheduler_runner.utils.logging import TRACE_LEVEL, configure_logger
from scheduler_runner.utils.notifications import send_notification

# Импорт из reports_summary (Phase 1.1)
from .reports_summary import (
    PVZExecutionResult,
    ReportsBackfillExecutionResult,
    extract_batch_failures,
    build_pvz_execution_result,
    _is_owner_skipped_no_missing,
    _format_failed_dates,
    _failover_had_any_work,
)


# ──────────────────────────────────────────────
# Logger factory
# ──────────────────────────────────────────────

def create_notification_logger():
    return configure_logger(
        user="reports_domain",
        task_name="Notification",
        log_levels=[TRACE_LEVEL, logging.DEBUG],
        single_file_for_levels=False,
    )


# ──────────────────────────────────────────────
# Single-date notification
# ──────────────────────────────────────────────

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


# ──────────────────────────────────────────────
# Batch notification
# ──────────────────────────────────────────────

def prepare_batch_notification_data(batch_result=None, upload_result=None, coverage_result=None, pvz_id=""):
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


# ──────────────────────────────────────────────
# Aggregated backfill summary & notification
# ──────────────────────────────────────────────

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


# ──────────────────────────────────────────────
# Full run notification (summary-based)
# ──────────────────────────────────────────────

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
                    f"- неуспешных recovery даты: {summary.failover.failed_recovery_dates_count}",
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


# ──────────────────────────────────────────────
# Send helper
# ──────────────────────────────────────────────

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

