# Метод `setup_browser()`

## Версия
**0.0.2**

## Описание
Метод `setup_browser()` настраивает и запускает Edge WebDriver с диагностикой startup-сбоев и аварийным обходом.

Ключевая логика:
- Подготовка окружения: завершение процессов браузера, определение `user_data_dir`, проверка директории профиля.
- Основной запуск (`phase=primary`) с ретраями.
- Если запрошен `headless=True` и обнаружена известная сигнатура startup-падения, выполняется аварийный обход:
  - переключение на `headless=False`;
  - повторный запуск (`phase=fallback`) с теми же ретраями.

## Сигнатура
```python
def setup_browser(self, browser_config: Optional[Dict[str, Any]] = None) -> bool
```

## Параметры
- **browser_config** (`Optional[Dict[str, Any]]`): переопределение параметров браузера поверх `self.config['browser_config']`.

## Возвращаемое значение
- **bool**: `True`, если браузер успешно поднят (на primary или fallback фазе), иначе `False`.

## Поддерживаемые параметры конфигурации
- `user_data_dir` / `EDGE_USER_DATA_DIR` - путь к профилю Edge.
- `headless` / `HEADLESS` - режим headless.
- `window_size` - размер окна.

## Диагностические маркеры в логах
- `ENV_BROWSER_STARTUP_CONTEXT` - снимок окружения перед запуском.
- `BROWSER_START_ATTEMPT_CONTEXT` - контекст конкретной попытки старта.
- `BROWSER_STARTUP_CRASH_SIGNATURE` - совпадение с известной сигнатурой падения.
- `BROWSER_POST_FAILURE_STATE` - состояние после неуспешной попытки.
- `BROWSER_FALLBACK_TRIGGERED` - активирован аварийный обход (`headless=False`).
- `BROWSER_FALLBACK_SUCCESS` / `BROWSER_FALLBACK_FAILED` - результат fallback-фазы.

## Внутренние helper-методы
- `_start_edge_driver_with_retries(...)`
- `_build_edge_options(...)`
- `_log_startup_environment(...)`
- `_log_attempt_runtime_context(...)`
- `_log_post_failed_attempt_state(...)`
- `_is_startup_crash_signature(...)`
- `_log_user_data_dir_diagnostics(...)`

## Пример
```python
success = parser.setup_browser()
if not success:
    raise RuntimeError("Не удалось запустить браузер")
```
