# Reports Task

`tasks/reports` - orchestration layer для получения KPI-отчетов Ozon ПВЗ, подготовки данных к загрузке и отправки итоговых уведомлений.

Это не parser package.  
Parser вынесен в отдельный dependency layer:
- runtime: [`utils/parser/`](C:/tools/scheduler/scheduler_runner/utils/parser)
- docs: [`utils/parser/docs/`](C:/tools/scheduler/scheduler_runner/utils/parser/docs)

Задача `reports` отвечает за:
- определение missing dates в Google Sheets;
- выбор execution scope по PVZ;
- вызов parser facade;
- batch upload;
- notification flow;
- failover coordination между коллегами через `KPI_FAILOVER_STATE`.

## Точка Входа

Основная точка входа:
- [`reports_processor.py`](C:/tools/scheduler/scheduler_runner/tasks/reports/reports_processor.py)

## Границы Ответственности

`tasks/reports`:
- orchestration
- coverage-check
- upload contract
- notification contract
- CLI режимы `single/backfill`
- pre-check доступных PVZ для fallback parsing по коллегам
- coordination state и claim policy для failover

`utils/parser`:
- browser lifecycle
- Selenium runtime
- parsing/report extraction
- PVZ switching
- available PVZ discovery
- parser smoke/debug entrypoints

## Режимы Запуска

`reports_processor` поддерживает два режима:

- `single`
  - один PVZ
  - одна дата
  - legacy-compatible flow

- `backfill`
  - диапазон дат
  - один или несколько PVZ
  - coverage-check -> parsing -> upload -> notification

Если явно не передан `--mode`, то:
- при наличии `--execution_date` используется `single`
- иначе используется `backfill`

## Single Flow

`single` режим делает:

1. вызывает parser facade на одну дату;
2. при успешном parse готовит upload payload;
3. вызывает uploader;
4. отправляет notification.

Этот путь сохраняется для обратной совместимости и точечных запусков.

## Backfill Flow

`backfill` режим делает:

1. вычисляет `date_from/date_to`;
2. проверяет покрытие Google Sheets по ключу `Дата + ПВЗ`;
3. строит список missing dates;
4. вызывает parser только для отсутствующих данных;
5. загружает только успешные результаты;
6. отправляет агрегированное уведомление по прогону.

## Multi-PVZ Flow

Для backfill по нескольким PVZ текущий flow такой:

1. нормализуется запрошенный список PVZ;
2. если среди запроса есть коллеги, запускается available PVZ discovery текущей учеткой;
3. недоступные коллеги исключаются до parsing stage;
4. оставшийся scope идет либо:
   - в grouped multi-PVZ path, если доступных PVZ больше одного;
   - в single-PVZ backfill path, если после фильтра остался один PVZ.

Это важное текущее поведение:
- execution model может деградировать `multi -> single`, если discovery scope сузился до одного доступного объекта;
- это нормальный и ожидаемый fallback.

## Failover Coordination

Failover coordination теперь разбит на два слоя:

- capability layer
  - определить, какие PVZ доступны текущей Ozon account/session;
  - это делается через available PVZ discovery parser facade;

- coordination layer
  - согласовать, кто именно из доступных коллег берет failed дату;
  - это делается через worksheet `KPI_FAILOVER_STATE`.

Текущая реализация:
- state worksheet: [`config/scripts/kpi_failover_state_google_sheets_config.py`](C:/tools/scheduler/scheduler_runner/tasks/reports/config/scripts/kpi_failover_state_google_sheets_config.py)
- state helpers: [`failover_state.py`](C:/tools/scheduler/scheduler_runner/tasks/reports/failover_state.py)
- detailed runbook: [`FAILOVER_COORDINATION.md`](C:/tools/scheduler/scheduler_runner/tasks/reports/FAILOVER_COORDINATION.md)

Что уже работает:
- owner пишет `owner_pending / owner_success / owner_failed` по missing dates своего PVZ;
- processor может сделать один bounded failover pass по доступным коллегам;
- claim по умолчанию идет через Google Apps Script Web App под `LockService`;
- перед upload recovery path делает повторный coverage-check и исключает уже закрытые даты.

Текущие ограничения:
- автоматический coordination flow пока `opt-in` через `--enable_failover_coordination`;
- automatic failover сейчас ограничен обычным single-PVZ backfill path без явного `--pvz`;
- grouped manual multi-PVZ path и automatic failover policy пока не смешиваются;
- окончательная arbitration policy между несколькими коллегами еще может эволюционировать.

## Основные Модули

- [`reports_processor.py`](C:/tools/scheduler/scheduler_runner/tasks/reports/reports_processor.py)
  - основной orchestration script
- [`failover_state.py`](C:/tools/scheduler/scheduler_runner/tasks/reports/failover_state.py)
  - coordination state helpers и claim backend switch
- [`config/scripts/reports_processor_config.py`](C:/tools/scheduler/scheduler_runner/tasks/reports/config/scripts/reports_processor_config.py)
  - runtime, scheduler и failover config
- [`config/scripts/kpi_google_sheets_config.py`](C:/tools/scheduler/scheduler_runner/tasks/reports/config/scripts/kpi_google_sheets_config.py)
  - KPI data sheet schema/connection config
- [`config/scripts/kpi_failover_state_google_sheets_config.py`](C:/tools/scheduler/scheduler_runner/tasks/reports/config/scripts/kpi_failover_state_google_sheets_config.py)
  - failover state worksheet schema/connection config
- [`tests/test_reports_processor.py`](C:/tools/scheduler/scheduler_runner/tasks/reports/tests/test_reports_processor.py)
  - unit coverage orchestration logic
- [`tests/test_failover_state.py`](C:/tools/scheduler/scheduler_runner/tasks/reports/tests/test_failover_state.py)
  - unit coverage failover state helpers

## CLI

Single date:

```powershell
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.reports_processor --execution_date 2026-03-10
```

Single-PVZ backfill:

```powershell
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.reports_processor --backfill_days 7
```

Backfill по явному диапазону:

```powershell
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.reports_processor --date_from 2026-03-04 --date_to 2026-03-10
```

Backfill по нескольким PVZ:

```powershell
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.reports_processor --mode backfill --pvz "ЧЕБОКСАРЫ_144" --pvz "ЧЕБОКСАРЫ_182" --date_from 2026-03-04 --date_to 2026-03-10
```

Single-PVZ backfill с automatic failover coordination:

```powershell
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.reports_processor --backfill_days 7 --enable_failover_coordination
```

## Ожидаемое Поведение

- `single`
  - browser/session открывается под parser package для одной даты
- `backfill`, `1 PVZ`
  - один browser lifecycle на набор missing dates этого PVZ
- `backfill`, `N PVZ`
  - parser reuse идет по модели `1 session per PVZ`
  - перед запуском выполняется pre-check доступных коллег
  - недоступные PVZ отбрасываются до parser run
- `backfill + failover coordination`
  - owner sync-ит свои failed dates в `KPI_FAILOVER_STATE`
  - processor делает bounded claim pass по доступным коллегам
  - claim backend по умолчанию `apps_script`

## Логи

Сервисные logger-ы разделены по ролям:
- `Processor`
- `Parser`
- `Uploader`
- `Notification`
- `FailoverState`

Parser runtime и его артефакты живут в parser package logging area:
- `logs/reports_domain/Parser/`
- `logs/reports_domain/Parser/artifacts/`

## Проверка Изменений

Основные unit-тесты orchestration-слоя:

```powershell
.venv\Scripts\python.exe -m pytest scheduler_runner\tasks\reports\tests\test_reports_processor.py scheduler_runner\tasks\reports\tests\test_failover_state.py -q
```

Смежная parser/report suite:

```powershell
.venv\Scripts\python.exe -m pytest scheduler_runner\utils\parser\core\tests\test_base_parser.py scheduler_runner\utils\parser\core\tests\test_base_report_parser.py scheduler_runner\utils\parser\core\tests\test_ozon_report_parser.py scheduler_runner\tasks\reports\tests\test_reports_processor.py -q
```

Реальный smoke coverage-check:

```powershell
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.test_coverage_check_real --pvz "ЧЕБОКСАРЫ_340" --days 7 --json
```

Реальный smoke claim через Apps Script backend:

```powershell
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.tests.run_failover_claim_smoke --claim_backend apps_script --pretty
```
