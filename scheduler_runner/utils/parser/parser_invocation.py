import logging
from copy import deepcopy
from datetime import datetime

from config.base_config import PVZ_ID
from scheduler_runner.utils.parser.configs.implementations.multi_step_ozon_config import MULTI_STEP_OZON_CONFIG
from scheduler_runner.utils.parser.core.contracts import ParserJob, ParserRuntimeContext, ReportDefinition
from scheduler_runner.utils.parser.implementations.multi_step_ozon_parser import MultiStepOzonParser
from scheduler_runner.utils.logging import TRACE_LEVEL, configure_logger


def create_parser_logger():
    return configure_logger(
        user="reports_domain",
        task_name="Parser",
        log_levels=[TRACE_LEVEL, logging.DEBUG],
        single_file_for_levels=False,
    )


def apply_pvz_to_parser_config(config, pvz_id):
    normalized_config = deepcopy(config)
    additional_params = deepcopy(normalized_config.get("additional_params", {}))
    additional_params["location_id"] = pvz_id
    normalized_config["additional_params"] = additional_params
    return normalized_config


def build_parser_definition(config=None):
    return ReportDefinition(
        report_type="ozon_reports",
        config=deepcopy(config or MULTI_STEP_OZON_CONFIG),
    )


def build_parser_runtime_context(save_to_file=False, output_format="json"):
    return ParserRuntimeContext(
        save_to_file=save_to_file,
        output_format=output_format,
    )


def build_parser_job(execution_date, pvz_id=PVZ_ID, definition=None, extra_params=None):
    parser_definition = definition or build_parser_definition()
    return ParserJob(
        report_type=parser_definition.report_type,
        pvz_id=pvz_id,
        execution_date=execution_date,
        extra_params=deepcopy(extra_params or {}),
    )


def build_jobs_for_pvz(pvz_id, execution_dates, definition=None, extra_params=None):
    parser_definition = definition or build_parser_definition()
    normalized_dates = [execution_date for execution_date in (execution_dates or []) if execution_date]
    return [
        build_parser_job(
            execution_date=execution_date,
            pvz_id=pvz_id,
            definition=parser_definition,
            extra_params=extra_params,
        )
        for execution_date in normalized_dates
    ]


def convert_job_results_to_batch_result(job_results):
    normalized_results = {}

    for job_result in job_results or []:
        normalized_results[job_result.execution_date] = (
            {"success": True, "data": job_result.data}
            if job_result.success
            else {
                "success": False,
                "error": job_result.error_message or job_result.error_code or "parser_job_failed",
            }
        )

    successful_dates = sorted([date for date, result in normalized_results.items() if result.get("success")])
    failed_dates = sorted([date for date, result in normalized_results.items() if not result.get("success")])

    return {
        "success": not failed_dates,
        "mode": "batch",
        "total_dates": len(normalized_results),
        "successful_dates": successful_dates,
        "failed_dates": failed_dates,
        "results_by_date": normalized_results,
    }


def build_empty_batch_result():
    return {
        "success": True,
        "mode": "batch",
        "total_dates": 0,
        "successful_dates": [],
        "failed_dates": [],
        "results_by_date": {},
    }


def execute_parser_internal(
    *,
    parser_api="legacy",
    pvz_id=PVZ_ID,
    execution_dates=None,
    jobs=None,
    result_mode="batch",
    save_to_file=False,
    output_format="json",
    logger=None,
):
    logger = logger or create_parser_logger()
    normalized_dates_source = execution_dates or [job.execution_date for job in (jobs or [])]
    normalized_jobs = build_jobs_for_pvz(pvz_id=pvz_id, execution_dates=normalized_dates_source)
    normalized_dates = [job.execution_date for job in normalized_jobs]

    if result_mode == "single":
        if not normalized_dates:
            normalized_dates = [datetime.now().strftime("%Y-%m-%d")]
            normalized_jobs = build_jobs_for_pvz(pvz_id=pvz_id, execution_dates=normalized_dates)

        execution_date = normalized_dates[0]
        if parser_api == "new":
            logger.info("Запуск single-date парсинга Ozon через internal executor и job API")
            definition = build_parser_definition(config=apply_pvz_to_parser_config(MULTI_STEP_OZON_CONFIG.copy(), pvz_id))
            runtime_context = build_parser_runtime_context(save_to_file=save_to_file, output_format=output_format)
            parser = MultiStepOzonParser(deepcopy(definition.config), logger=logger)
            job_result = parser.run_job(normalized_jobs[0], definition=definition, runtime=runtime_context)
            if not job_result.success:
                raise RuntimeError(job_result.error_message or job_result.error_code or "parser_job_failed")
            logger.info(f"Single-date job API парсинг завершен успешно. Результат: {job_result.data}")
            return job_result.data

        logger.info("Запуск single-date парсинга Ozon через internal executor и legacy API")
        config = apply_pvz_to_parser_config(MULTI_STEP_OZON_CONFIG.copy(), pvz_id)
        config["execution_date"] = execution_date
        parser = MultiStepOzonParser(config, logger=logger)
        result = parser.run_parser(save_to_file=save_to_file, output_format=output_format)
        logger.info(f"Single-date legacy парсинг завершен успешно. Результат: {result}")
        return result

    if not normalized_dates:
        return build_empty_batch_result()

    if parser_api == "new":
        logger.info(f"Запуск batch-парсинга Ozon через internal executor и job API. Количество дат: {len(normalized_dates)}")
        definition = build_parser_definition(config=apply_pvz_to_parser_config(MULTI_STEP_OZON_CONFIG.copy(), pvz_id))
        runtime_context = build_parser_runtime_context(save_to_file=save_to_file, output_format=output_format)
        parser = MultiStepOzonParser(deepcopy(definition.config), logger=logger)
        job_results = parser.run_jobs_for_pvz(jobs=normalized_jobs, definition=definition, runtime=runtime_context)
        result = convert_job_results_to_batch_result(job_results)
        logger.info(f"Batch-парсинг через internal executor и job API завершен. Результат: {result}")
        return result

    logger.info(f"Запуск batch-парсинга Ozon через internal executor и legacy API. Количество дат: {len(normalized_dates)}")
    config = apply_pvz_to_parser_config(MULTI_STEP_OZON_CONFIG.copy(), pvz_id)
    parser = MultiStepOzonParser(config, logger=logger)
    result = parser.run_parser_batch(
        execution_dates=normalized_dates,
        save_to_file=save_to_file,
        output_format=output_format,
    )
    logger.info(f"Batch-парсинг через internal executor и legacy API завершен. Результат: {result}")
    return result


def run_parsing_microservice(execution_date=None, pvz_id=PVZ_ID, logger=None):
    return execute_parser_internal(
        parser_api="legacy",
        pvz_id=pvz_id,
        execution_dates=[execution_date or datetime.now().strftime("%Y-%m-%d")],
        result_mode="single",
        save_to_file=True,
        output_format="json",
        logger=logger,
    )


def run_batch_parsing_microservice(execution_dates=None, pvz_id=PVZ_ID, logger=None):
    return execute_parser_internal(
        parser_api="legacy",
        pvz_id=pvz_id,
        execution_dates=execution_dates,
        result_mode="batch",
        save_to_file=False,
        output_format="json",
        logger=logger,
    )


def run_parsing_microservice_new_api(execution_date=None, pvz_id=PVZ_ID, logger=None):
    return execute_parser_internal(
        parser_api="new",
        pvz_id=pvz_id,
        execution_dates=[execution_date or datetime.now().strftime("%Y-%m-%d")],
        result_mode="single",
        save_to_file=True,
        output_format="json",
        logger=logger,
    )


def run_batch_parsing_microservice_new_api(execution_dates=None, pvz_id=PVZ_ID, logger=None):
    return execute_parser_internal(
        parser_api="new",
        pvz_id=pvz_id,
        execution_dates=execution_dates,
        result_mode="batch",
        save_to_file=False,
        output_format="json",
        logger=logger,
    )


def invoke_parser_for_single_date(*, execution_date=None, parser_api="legacy", pvz_id=PVZ_ID, logger=None):
    normalized_execution_date = execution_date or datetime.now().strftime("%Y-%m-%d")
    return execute_parser_internal(
        parser_api=parser_api,
        pvz_id=pvz_id,
        execution_dates=[normalized_execution_date],
        result_mode="single",
        save_to_file=True,
        output_format="json",
        logger=logger,
    )


def execute_parser_jobs_for_pvz(jobs, parser_api="legacy", logger=None):
    if not jobs:
        return build_empty_batch_result()

    return execute_parser_internal(
        parser_api=parser_api,
        pvz_id=jobs[0].pvz_id,
        jobs=jobs,
        result_mode="batch",
        save_to_file=False,
        output_format="json",
        logger=logger,
    )


def invoke_parser_for_pvz(*, parser_api="legacy", pvz_id=None, execution_dates=None, jobs=None, logger=None):
    normalized_jobs = jobs or build_jobs_for_pvz(pvz_id=pvz_id, execution_dates=execution_dates or [])
    return execute_parser_internal(
        parser_api=parser_api,
        pvz_id=(pvz_id or (normalized_jobs[0].pvz_id if normalized_jobs else PVZ_ID)),
        jobs=normalized_jobs,
        result_mode="batch",
        save_to_file=False,
        output_format="json",
        logger=logger,
    )


def invoke_parser_for_grouped_jobs(*, grouped_jobs, pvz_ids=None, parser_api="legacy", logger=None):
    batch_results_by_pvz = {}
    ordered_pvz_ids = pvz_ids or list((grouped_jobs or {}).keys())

    for pvz_id in ordered_pvz_ids:
        pvz_jobs = (grouped_jobs or {}).get(pvz_id, [])
        if not pvz_jobs:
            continue
        batch_results_by_pvz[pvz_id] = invoke_parser_for_pvz(
            parser_api=parser_api,
            pvz_id=pvz_id,
            jobs=pvz_jobs,
            logger=logger,
        )

    return batch_results_by_pvz

