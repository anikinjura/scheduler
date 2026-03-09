# Метод `setup_browser()` в `BaseReportParser`

## Версия
**0.0.2**

## Важно
`BaseReportParser` не переопределяет `setup_browser()`. Используется реализация родительского класса `BaseParser`.

## Где смотреть фактическую реализацию
- `scheduler_runner/tasks/reports/parser/core/base_parser.py`
- Подробное описание метода: `docs/BaseParser/setup_browser().md`

## Актуальное поведение (наследуется из `BaseParser`)
- Запуск Edge с ретраями (`primary`).
- Диагностика startup-сбоев через структурированные debug-маркеры.
- Аварийный обход: при `headless=True` и известной сигнатуре падения выполняется fallback-запуск с `headless=False`.

## Возвращаемое значение
- **bool**: `True`, если браузер успешно запущен (включая fallback), иначе `False`.

## Логи для анализа
- `ENV_BROWSER_STARTUP_CONTEXT`
- `BROWSER_START_ATTEMPT_CONTEXT`
- `BROWSER_STARTUP_CRASH_SIGNATURE`
- `BROWSER_POST_FAILURE_STATE`
- `BROWSER_FALLBACK_TRIGGERED`
- `BROWSER_FALLBACK_SUCCESS` / `BROWSER_FALLBACK_FAILED`
