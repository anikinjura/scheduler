# BaseParser API

`BaseParser` - низкоуровневый Selenium runtime layer.

## Публичные методы

- `__init__(config, logger=None)`
  - инициализирует parser state, config и logger.
- `setup_browser(browser_config=None) -> bool`
  - поднимает Edge WebDriver, применяет browser config, подготавливает environment.
- `login() -> bool`
  - абстрактная точка входа авторизации/валидации сессии.
- `navigate_to_target() -> bool`
  - абстрактный переход к целевой странице.
- `extract_data() -> dict`
  - абстрактный метод извлечения данных.
- `logout() -> bool`
  - абстрактная точка выхода.
- `close_browser()`
  - безопасно закрывает браузер и driver session.
- `dump_debug_artifacts(label) -> dict`
  - сохраняет screenshot/html/json metadata рядом с логами parser-а.
- `get_element_value(...) -> str`
  - универсальное чтение текста/атрибутов/значений input.
- `set_element_value(...) -> bool`
  - универсальная установка значений, включая dropdown flow.
- `extract_table_data(table_config_key=None, table_config=None) -> list`
  - извлекает табличные данные по config-driven схеме.
- `run_parser() -> dict`
  - базовый lifecycle: browser -> login -> navigate -> extract -> logout -> close.

## Browser startup helpers

- `_start_edge_driver_with_retries(...)`
- `_build_edge_options(...)`
- `_ensure_selenium_manager_environment()`
- `_resolve_edge_binary_location()`
- `_log_user_data_dir_diagnostics(...)`
- `_log_startup_environment(...)`
- `_log_attempt_runtime_context(...)`
- `_log_post_failed_attempt_state(...)`
- `_log_known_startup_crash_signature(...)`
- `_is_startup_crash_signature(error) -> bool`

Эти методы отвечают за устойчивый startup Edge, диагностику и fallback после headless/session startup failures.

## Environment и filesystem helpers

- `_safe_get_current_user()`
- `_get_default_browser_user_data_dir(username=None) -> str`
- `_resolve_existing_edge_user_data_dir() -> Optional[str]`
- `_get_current_user() -> str`
- `_cleanup_lock_files(user_data_dir)`
- `_terminate_browser_processes()`
- `_get_file_mtime(path)`
- `_get_disk_free_mb(path)`
- `_get_command_output(command)`
- `_get_process_count(process_name)`

## Element interaction helpers

- `_click_element(...) -> bool`
- `_get_checkbox_state(element) -> str`
- `_set_checkbox_state(element, target_state) -> bool`
- `_select_option_from_dropdown(...)`
- `_extract_standard_table_data(table_element, columns_config) -> list`

## Что важно

- `BaseParser` не должен знать о business semantics отчетов.
- Все report-specific и Ozon-specific решения должны жить выше, в `BaseReportParser` и `OzonReportParser`.
