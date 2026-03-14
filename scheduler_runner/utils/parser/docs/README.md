# Parser Package

`utils/parser` - изолированный Selenium-based parser package для работы с web-интерфейсами Ozon ПВЗ.

Пакет проектируется как самостоятельный runtime-модуль:
- без жесткой привязки к конкретному orchestration-слою;
- с собственными facade entrypoints;
- с собственными `configs/`, `core/`, `implementations/`, `tests/` и `docs/`.

Документация в этой директории должна описывать сам package, а не внешний consumer.

## Структура пакета

```text
utils/parser/
  core/
    base_parser.py
    base_report_parser.py
    ozon_report_parser.py
  implementations/
    multi_step_ozon_parser.py
    ozon_available_pvz_parser.py
  configs/
    base_configs/
    implementations/
  parser_invocation.py
  tests/
  docs/
```

## Runtime-модель

Основная иерархия классов:

```text
BaseParser
  -> BaseReportParser
    -> OzonReportParser
      -> MultiStepOzonParser
      -> OzonAvailablePvzParser
```

Роли слоев:

- `BaseParser` - browser lifecycle, Selenium utilities, element interactions, debug artifacts.
- `BaseReportParser` - single-date, batch и job-based execution model, report formatting, file output.
- `OzonReportParser` - Ozon-specific navigation, PVZ selection, overlay handling, available PVZ discovery helpers.
- `implementations/` - concrete runtime implementations с готовым output contract.
- `parser_invocation.py` - package facade для внешнего вызова parser-а без знания деталей классов.

## Текущие реализации

### MultiStepOzonParser

Основной parser отчетов Ozon ПВЗ.

Назначение:
- сбор KPI/summary за одну дату;
- batch-run по нескольким датам в одной browser session;
- job-based execution для orchestration consumers.

### OzonAvailablePvzParser

Lightweight discovery implementation.

Назначение:
- открыть Ozon с сохраненной browser session;
- определить текущий PVZ;
- собрать список PVZ, доступных данной учетной записи;
- вернуть structured discovery result без report summary contract.

## Facade API

Основные facade функции описаны в [ParserInvocation/README.md](/C:/tools/scheduler/scheduler_runner/utils/parser/docs/ParserInvocation/README.md).

Ключевые entrypoints:

- `invoke_parser_for_single_date(...)`
- `invoke_parser_for_pvz(...)`
- `invoke_parser_for_grouped_jobs(...)`
- `invoke_available_pvz_discovery(...)`

## Execution Modes

Package поддерживает четыре уровня запуска:

1. single-date
2. single-PVZ batch
3. grouped jobs by PVZ
4. available PVZ discovery

Подробности:
- [BaseReportParser/README.md](/C:/tools/scheduler/scheduler_runner/utils/parser/docs/BaseReportParser/README.md)
- [OzonReportParser/README.md](/C:/tools/scheduler/scheduler_runner/utils/parser/docs/OzonReportParser/README.md)

## Logging и артефакты

Parser package использует централизованный logger, но не зависит от конкретного orchestration consumer.

По умолчанию:
- логи parser-а пишутся в `logs/reports_domain/Parser/`
- debug artifacts пишутся в `logs/reports_domain/Parser/artifacts/`
- fallback artifacts могут использоваться только если logger отсутствует

Перед чистыми e2e-прогонами рекомендуется очищать:

```powershell
Get-ChildItem logs\reports_domain\Parser -File | Remove-Item -Force
Get-ChildItem logs\reports_domain\Parser\artifacts -File | Remove-Item -Force
```

## Smoke Entry Points

В `tests/` лежат manual smoke scripts:

- `run_single_date_smoke.py`
- `run_single_pvz_batch_smoke.py`
- `run_multi_pvz_multi_date_smoke.py`
- `run_available_pvz_discovery_smoke.py`

Они предназначены для ручного e2e/debug запуска и не должны восприниматься как unit-tests.

## API Reference

Актуальные индексы методов:

- [BaseParser/README.md](/C:/tools/scheduler/scheduler_runner/utils/parser/docs/BaseParser/README.md)
- [BaseReportParser/README.md](/C:/tools/scheduler/scheduler_runner/utils/parser/docs/BaseReportParser/README.md)
- [OzonReportParser/README.md](/C:/tools/scheduler/scheduler_runner/utils/parser/docs/OzonReportParser/README.md)
- [Implementations/README.md](/C:/tools/scheduler/scheduler_runner/utils/parser/docs/Implementations/README.md)
- [ParserInvocation/README.md](/C:/tools/scheduler/scheduler_runner/utils/parser/docs/ParserInvocation/README.md)

Legacy per-method markdown files в поддиректориях сохранены как детальные заметки, но canonical overview должен поддерживаться именно через эти README/API index страницы.

## Verification

Минимальная проверка package:

```powershell
.venv\Scripts\python.exe -m pytest scheduler_runner\utils\parser\core\tests\test_base_parser.py scheduler_runner\utils\parser\core\tests\test_base_report_parser.py scheduler_runner\utils\parser\core\tests\test_ozon_report_parser.py -q
```

Для live troubleshooting используйте [DEBUG_GUIDE.md](/C:/tools/scheduler/scheduler_runner/utils/parser/docs/DEBUG_GUIDE.md).
