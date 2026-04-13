#!/usr/bin/env python3
"""
reports_processor.py — тонкий orchestration entrypoint.

После декомпозиции этот файл содержит только:
- main() — CLI entrypoint (scheduler вызывает его)
- build_processor_run_id() — утилита только для main()

Вся бизнес-логика делегирована извлечённым модулям.
"""
__version__ = '0.2.0'

import argparse
from datetime import datetime, timedelta

from config.base_config import PVZ_ID

# Извлечённые модули
from .reports_scope import (
    resolve_accessible_pvz_ids,
    should_run_automatic_failover_coordination,
    build_parser_definition,
    build_jobs_from_missing_dates_by_pvz,
    group_jobs_by_pvz,
    normalize_pvz_id,
)
from .reports_upload import (
    detect_missing_report_dates,
    detect_missing_report_dates_by_pvz,
    run_upload_microservice,
    run_upload_batch_microservice,
    create_uploader_logger,
)
from .reports_summary import (
    build_pvz_execution_result,
    build_owner_run_summary,
    build_failover_run_summary,
    build_reports_run_summary,
)
from .reports_notifications import (
    prepare_notification_data,
    format_notification_message,
    prepare_batch_notification_data,
    format_reports_run_notification_message,
    send_notification_microservice,
    create_notification_logger,
    build_aggregated_backfill_summary,
)
from .owner_state_sync import (
    sync_owner_failover_state_from_batch_result,
)
from .failover_orchestration import (
    run_failover_coordination_pass,
)

# ──────────────────────────────────────────────
# Orchestration-level helpers (remain in processor)
# ──────────────────────────────────────────────

def build_processor_run_id(pvz_id=PVZ_ID, started_at=None):
    """Orchestration-level run ID builder. Used only in main()."""
    started_at = started_at or datetime.now()
    return f"{started_at.strftime('%Y%m%d%H%M%S')}|{pvz_id}"


# ──────────────────────────────────────────────
# Parser facade (остаётся в utils)
# ──────────────────────────────────────────────
from scheduler_runner.utils.parser import (
    build_jobs_for_pvz,
    invoke_parser_for_single_date,
    invoke_parser_for_pvz,
    invoke_parser_for_grouped_jobs,
    create_parser_logger,
)
from .storage.failover_state import (
    create_failover_state_logger,
)
from .config.scripts.reports_processor_config import BACKFILL_CONFIG
from scheduler_runner.utils.logging import configure_logger


def main():
    """Главный entrypoint reports-домена. Вызывается scheduler'ом."""
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
            # ─── Single-date mode ───
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
            # ─── Backfill mode ───
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
                # ─── Multi-PVZ backfill ───
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
                    processor_logger.info("Backfill не требуется: отсутствующих дат не найдено ни для одного PVZ")
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

            # ─── Single-PVZ backfill ───
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
                                owner_object_name=pvz_id,
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

