# Parser Invocation Facade

`parser_invocation.py` - canonical facade layer для внешнего вызова parser package.

## Logger и config helpers

- `create_parser_logger()`
- `apply_pvz_to_parser_config(config, pvz_id)`
- `apply_headless_override_to_parser_config(config, headless_enabled)`
- `build_parser_definition(config=None)`
- `build_parser_runtime_context(save_to_file=False, output_format='json')`
- `build_parser_runtime_context_with_headless(headless, save_to_file=False, output_format='json')`

## Job builders

- `build_parser_job(execution_date, pvz_id=PVZ_ID, definition=None, extra_params=None)`
- `build_jobs_for_pvz(pvz_id, execution_dates, definition=None, extra_params=None)`
- `convert_job_results_to_batch_result(job_results)`
- `build_empty_batch_result()`

## Retry / failure classification

- `is_headless_requested(parser_config)`
- `is_session_level_error(error_text)`
- `batch_result_contains_session_failure(batch_result)`
- `should_retry_batch_in_visible_browser(batch_result, parser_config)`

Эти функции обеспечивают mitigation для session-level Selenium failures.

## Internal execution functions

- `execute_legacy_batch_once(...)`
- `execute_new_batch_once(...)`
- `execute_parser_internal(...)`

`execute_parser_internal(...)` - главный низкоуровневый facade execution engine.

## Public facade entrypoints

- `run_parsing_microservice(execution_date=None, pvz_id=PVZ_ID, logger=None)`
- `run_batch_parsing_microservice(execution_dates=None, pvz_id=PVZ_ID, logger=None)`
- `run_parsing_microservice_new_api(execution_date=None, pvz_id=PVZ_ID, logger=None)`
- `run_batch_parsing_microservice_new_api(execution_dates=None, pvz_id=PVZ_ID, logger=None)`
- `invoke_parser_for_single_date(execution_date=None, parser_api='legacy', pvz_id=PVZ_ID, logger=None)`
- `execute_parser_jobs_for_pvz(jobs, parser_api='legacy', logger=None)`
- `invoke_parser_for_pvz(parser_api='legacy', pvz_id=None, execution_dates=None, jobs=None, logger=None)`
- `invoke_parser_for_grouped_jobs(grouped_jobs, pvz_ids=None, parser_api='legacy', logger=None)`
- `invoke_available_pvz_discovery(pvz_id=PVZ_ID, logger=None, save_to_file=False, output_format='json')`

## Рекомендация

Внешний код должен предпочитать facade entrypoints вместо прямого создания parser classes, если не требуется низкоуровневый runtime control.
