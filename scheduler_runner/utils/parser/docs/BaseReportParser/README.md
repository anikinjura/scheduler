# BaseReportParser API

`BaseReportParser` добавляет к `BaseParser` report-oriented lifecycle и config-driven processing.

## Публичные методы

- `__init__(config, args=None, logger=None)`
- `get_report_type() -> str`
- `extract_report_data() -> dict`
- `run_parser(save_to_file=True, output_format='json') -> dict`
  - single-date lifecycle.
- `run_parser_batch(execution_dates, save_to_file=False, output_format='json') -> dict`
  - batch lifecycle `1 PVZ x N dates` в одной browser session.
- `run_job(job, definition, runtime) -> ParserJobResult`
  - минимальная единица job-based execution.
- `run_jobs_for_pvz(jobs, definition, runtime) -> list[ParserJobResult]`
  - обработка jobs одного PVZ в одной session.
- `run_jobs_batch(jobs, definition, runtime) -> list[ParserJobResult]`
  - общий batch API для job-based execution.
- `format_report_output(...) -> dict`
- `save_report(...) -> str | None`
- `get_common_report_info(all_step_results=None) -> dict`
- `navigate_to_target() -> bool`

## Runtime helpers

- `_parse_arguments(args=None)`
- `_update_execution_date()`
- `_run_single_date_in_current_session(...) -> dict`
- `_close_parser_session()`
- `_apply_job_to_config(job, definition)`

Эти методы обеспечивают reuse одной browser session для серии дат или jobs.

## Multi-step extraction

- `_build_url_filter() -> str`
- `_execute_multi_step_processing(multi_step_config)`
- `_execute_single_step(step_config)`
- `_update_config_for_step(step_config)`
- `_handle_simple_extraction(step_config)`
- `_handle_table_extraction(step_config)`
- `_handle_table_nested_extraction(step_config)`
- `_handle_nested_processing(nested_config, identifiers)`
- `_aggregate_nested_results(...)`
- `_combine_step_results(...)`
- `_calculate_run_status(all_step_results) -> str`
- `_aggregate_values(values, aggregation_method, target_field)`
- `_apply_post_processing(value, post_processing_config)`
- `_extract_value_by_config(extraction_config)`
- `__filter_structure_by_available_keys(data, available_keys)`
- `_replace_placeholders_recursive(data, replacements)`
- `_get_common_url_prefix(urls)`

## Execution models

### Single-date

`run_parser(...)`:
- поднимает browser;
- логинится;
- обрабатывает одну дату;
- формирует result/output;
- закрывает session.

### Batch by dates

`run_parser_batch(...)`:
- открывает browser один раз;
- логинится один раз;
- обрабатывает даты последовательно;
- закрывает session после завершения batch.

### Job-based

`run_job(...)`, `run_jobs_for_pvz(...)`, `run_jobs_batch(...)`:
- предназначены для facade/orchestration consumers;
- позволяют использовать parser как reusable execution engine без знания внутренних lifecycle деталей.
