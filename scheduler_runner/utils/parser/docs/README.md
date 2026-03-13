# Parser Docs

Документация в этой директории описывает Selenium-парсер отчетов Ozon ПВЗ,
который используется в поддомене `scheduler_runner.tasks.reports`.

Канонический runtime-путь parser-пакета после переноса:
`scheduler_runner/utils/parser/`.

Канонический путь документации parser-пакета после переноса:
`scheduler_runner/utils/parser/docs/`.

Старый путь `scheduler_runner/tasks/reports/parser/` больше не является
основным runtime-пакетом. Старый docs-путь
`scheduler_runner/tasks/reports/parser/docs/` должен рассматриваться только как
legacy redirect marker.

## Архитектура

Базовая иерархия классов:

```text
BaseParser
  -> BaseReportParser
    -> OzonReportParser
      -> MultiStepOzonParser
```

Основные каталоги:

- [core/](/C:/tools/scheduler/scheduler_runner/utils/parser/core/) -
  базовые классы и общая логика выполнения.
- [implementations/](/C:/tools/scheduler/scheduler_runner/utils/parser/implementations/) -
  конкретные реализации парсеров.
- [configs/](/C:/tools/scheduler/scheduler_runner/utils/parser/configs/) -
  селекторы, URL-фильтры и runtime-конфигурация.
- [docs/](/C:/tools/scheduler/scheduler_runner/utils/parser/docs/) -
  документация по ключевым методам и debug-сценариям.

## Режимы запуска

В [base_report_parser.py](/C:/tools/scheduler/scheduler_runner/utils/parser/core/base_report_parser.py)
поддерживаются два сценария:

- `run_parser(...)` - обработка одной даты.
- `run_parser_batch(execution_dates, ...)` - обработка списка дат в одной browser
  session.

### Single-date flow

`run_parser(...)` выполняет полный lifecycle:

1. `setup_browser()`
2. `login()`
3. обработка одной даты
4. `logout()`
5. `close_browser()`

Этот путь нужен для обратной совместимости и одиночных запусков.

### Batch flow

`run_parser_batch(...)` используется для backfill-сценария в
[reports_processor.py](/C:/tools/scheduler/scheduler_runner/tasks/reports/reports_processor.py).

Поведение batch-режима:

1. браузер открывается один раз;
2. выполняется один `login()`;
3. все даты обрабатываются последовательно в текущей session;
4. после завершения выполняется один `logout()` и один `close_browser()`.

Это убирает дорогой цикл многократного открытия и закрытия браузера на каждую
отсутствующую дату.

## Поведение при ошибках

- Ошибка одной даты в `run_parser_batch(...)` не должна ронять весь batch.
- Неуспешная дата фиксируется в результирующей структуре как `success=False`.
- Успешные даты продолжают обрабатываться и могут быть затем загружены в Google Sheets.
- При fail-fast сценариях авторизации и навигации срабатывают существующие защитные
  проверки `BaseReportParser`.

## Связь с reports processor

Новый `backfill` режим процессора использует parser так:

1. coverage-check в Google Sheets определяет missing dates;
2. parser получает только отсутствующие даты;
3. parser возвращает список результатов по датам;
4. в uploader уходят только успешные результаты.

Таким образом parser больше не отвечает за выбор дат для дозагрузки, а только за
эффективную обработку уже подготовленного списка.

## Проверка

Основные тесты parser-слоя:

```powershell
.venv\Scripts\python.exe -m pytest scheduler_runner\utils\parser\core\tests\test_base_report_parser.py -q
```

Для анализа инцидентов:

- смотрите логи в `logs/reports_domain/Parser/`
- сопоставляйте их с текущим кодом в `scheduler_runner/utils/parser/core/` и
  `scheduler_runner/utils/parser/implementations/`
- используйте [DEBUG_GUIDE.md](/C:/tools/scheduler/scheduler_runner/utils/parser/docs/DEBUG_GUIDE.md)
  для известных проблем startup/auth/navigation
