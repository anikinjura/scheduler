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
- notification flow.

## Точка входа

Основная точка входа:
- [`reports_processor.py`](C:/tools/scheduler/scheduler_runner/tasks/reports/reports_processor.py)

## Границы ответственности

`tasks/reports`:
- orchestration
- coverage-check
- upload contract
- notification contract
- CLI режимы single/backfill
- pre-check доступных PVZ для fallback parsing по коллегам

`utils/parser`:
- browser lifecycle
- Selenium runtime
- parsing/report extraction
- PVZ switching
- available PVZ discovery
- parser smoke/debug entrypoints

## Режимы запуска

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

## Colleague Fallback

В `reports_processor` уже встроен capability pre-check:
- [`discover_available_pvz_scope(...)`](C:/tools/scheduler/scheduler_runner/tasks/reports/reports_processor.py)
- [`resolve_accessible_pvz_ids(...)`](C:/tools/scheduler/scheduler_runner/tasks/reports/reports_processor.py)

Что уже есть:
- процессор умеет определить, какие PVZ доступны текущей Ozon account/session;
- процессор не пытается запускать parser по недоступным коллегам;
- при провале discovery используется safe fallback только на собственный configured PVZ.

Что пока еще открыто:
- policy-arbitration между коллегами:
  - кто именно должен подхватывать чужой объект;
  - по каким правилам принимать решение о failover;
  - как избегать гонок и дублирующего парсинга.

Этот слой координации пока не реализован.  
Сейчас подготовлен только фундамент: безопасное определение доступного execution scope.

## Основные модули

- [`reports_processor.py`](C:/tools/scheduler/scheduler_runner/tasks/reports/reports_processor.py)
  - основной orchestration script
- [`config/scripts/reports_processor_config.py`](C:/tools/scheduler/scheduler_runner/tasks/reports/config/scripts/reports_processor_config.py)
  - runtime и scheduler config для task
- [`config/scripts/kpi_google_sheets_config.py`](C:/tools/scheduler/scheduler_runner/tasks/reports/config/scripts/kpi_google_sheets_config.py)
  - Google Sheets schema/connection config
- [`tests/test_reports_processor.py`](C:/tools/scheduler/scheduler_runner/tasks/reports/tests/test_reports_processor.py)
  - unit coverage orchestration logic

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

## Ожидаемое поведение

- `single`
  - browser/session открывается под parser package для одной даты
- `backfill`, `1 PVZ`
  - один browser lifecycle на набор missing dates этого PVZ
- `backfill`, `N PVZ`
  - parser reuse идет по модели `1 session per PVZ`
  - перед запуском выполняется pre-check доступных коллег
  - недоступные PVZ отбрасываются до parser run

## Логи

Сервисные logger-и разделены по ролям:
- `Processor`
- `Parser`
- `Uploader`
- `Notification`

Parser runtime и его артефакты живут в parser package logging area:
- `logs/reports_domain/Parser/`
- `logs/reports_domain/Parser/artifacts/`

## Проверка изменений

Основные unit-тесты orchestration-слоя:

```powershell
.venv\Scripts\python.exe -m pytest scheduler_runner\tasks\reports\tests\test_reports_processor.py -q
```

Смежная parser/report suite:

```powershell
.venv\Scripts\python.exe -m pytest scheduler_runner\utils\parser\core\tests\test_base_parser.py scheduler_runner\utils\parser\core\tests\test_base_report_parser.py scheduler_runner\utils\parser\core\tests\test_ozon_report_parser.py scheduler_runner\tasks\reports\tests\test_reports_processor.py -q
```

При необходимости реальный smoke coverage-check:

```powershell
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.test_coverage_check_real --pvz "ЧЕБОКСАРЫ_340" --days 7 --json
```
