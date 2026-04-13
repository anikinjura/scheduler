# Reports Task — Refactored Modular Version

Orchestration layer для получения KPI-отчетов Ozon ПВЗ, загрузки данных и failover координации между коллегами.

Это **модульная версия** после декомпозиции `reports_processor.py` на логические подмодули.
Parser runtime остается в отдельном dependency layer: `utils/parser/`.

## Модульная Структура

```
scheduler_runner/tasks/reports/
│── reports_processor.py              ← тонкий orchestrator (~350 строк)
│── reports_summary.py                ← dataclasses + status resolution (~340 строк)
│── reports_notifications.py          ← notification formatting (~290 строк)
│── reports_upload.py                 ← coverage-check + upload orchestration (~290 строк)
│── reports_scope.py                  ← PVZ discovery + job grouping (~160 строк)
│── reports_utils.py                  ← общие утилиты (normalize_pvz_id) (~10 строк)
│── failover_orchestration.py         ← failover coordination flow (~490 строк)
│── owner_state_sync.py               ← owner state model + suppression (~335 строк)
│── failover_policy.py                ← policy rules + arbitration (~275 строк)
│
└── storage/                          ← Storage abstraction layer
    ├── __init__.py                   ← re-exports + DI helpers
    ├── failover_state_protocol.py    ← ABC интерфейс (FailoverStateStore)
    ├── failover_state.py             ← backward-compatible re-export + DI
    ├── google_sheets_store.py        ← Google Sheets implementation
    └── google_sheets_failover_store.py ← GoogleSheetsFailoverStore adapter
```

## Ответственность Модулей

| Модуль | Назначение | Зависимости |
|---|---|---|
| `reports_processor.py` | Orchestrator: CLI entry point, single/backfill modes, sequence control | Все остальные модули |
| `reports_summary.py` | Dataclasses + status resolution + batch result helpers | Только stdlib |
| `reports_notifications.py` | Notification formatting + send via VK/Telegram | `reports_summary.py`, `utils/notifications` |
| `reports_upload.py` | Coverage-check + KPI upload + retry logic | `utils/uploader` |
| `reports_scope.py` | PVZ discovery + accessibility check + job building | `utils/parser` |
| `reports_utils.py` | Общие утилиты: `normalize_pvz_id` | `utils/system` |
| `failover_orchestration.py` | Full failover coordination: scan → claim → recovery | `storage/`, `failover_policy.py`, `utils/parser` |
| `owner_state_sync.py` | Owner state persistence + success suppression policy | `storage/`, `reports_summary.py` |
| `failover_policy.py` | Policy rules: priority_map, capability_ranked, arbitration | Только config |
| `storage/` | Storage abstraction + Google Sheets implementation | `utils/uploader` |

## Dependency Graph

```
                    reports_processor.py
                           │
     ┌─────────┬───────────┼───────────┬──────────┐
     ▼         ▼           ▼           ▼          ▼
  summary   upload   notifications  owner_sync   failover_orch
     │         │           │         │            │
     │         │           │         ▼            │
     │         │           │     storage/ ────────┘
     │         │           │
     ▼         │           │
  scope ───────┘           │
                           ▼
                      failover_policy
```

**Направление зависимостей:** Orchestrator → модули → storage. Нет обратных вызовов.
`reports_utils.py` — leaf-модуль без зависимостей на другие reports-модули.

## Режимы Запуска

### Single Mode

```powershell
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.reports_processor --execution_date 2026-03-10
```

Flow: parser → upload → notification

### Single-PVZ Backfill

```powershell
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.reports_processor --backfill_days 7
```

Flow: coverage-check → parse missing → upload → owner sync → failover coord (opt-in) → notification

### Multi-PVZ Backfill

```powershell
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.reports_processor --pvz "ЧЕБОКСАРЫ_144" --pvz "ЧЕБОКСАРЫ_182" --date_from 2026-03-04 --date_to 2026-03-10
```

Flow: discovery → filter accessible → per-PVZ loop → aggregated notification

### With Failover Coordination

```powershell
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.reports_processor --backfill_days 7 --enable_failover_coordination
```

Добавляется: owner sync → failover scan → claim → recovery parse → recovery upload

## Точка Входа

Основная точка входа: `reports_processor.py` — функция `main()`.

Scheduler вызывает её через subprocess:
```
scheduler_runner/runner.py → reports_processor.main()
```

## CLI Аргументы

| Аргумент | Назначение | Default |
|---|---|---|
| `--execution_date`, `-d` | Дата для single mode | today |
| `--date_from` | Начало backfill диапазона | auto |
| `--date_to` | Конец backfill диапазона | today |
| `--backfill_days` | Окно backfill в днях | 7 |
| `--mode` | single / backfill | auto |
| `--max_missing_dates` | Limit missing dates per run | 7 |
| `--parser_api` | legacy / new | legacy |
| `--enable_failover_coordination` | Включить failover pass | False |
| `--pvz` | Явный список PVZ (multi) | None |
| `--detailed_logs` | Детальное логирование | False |

## Storage Abstraction

`storage/` package определяет контракт `FailoverStateStore(ABC)` и текущую Google Sheets реализацию.

### Protocol

```python
from scheduler_runner.tasks.reports.storage import FailoverStateStore

class FailoverStateStore(ABC):
    def get_row(self, execution_date, target_object_name): ...
    def get_rows_by_keys(self, keys): ...
    def list_rows(self, statuses=None, target_object_name=None): ...
    def list_candidate_rows(self, statuses=None): ...
    def upsert_record(self, record): ...
    def upsert_records(self, records): ...
    def mark_state(self, execution_date, target_object_name, owner_object_name, status, ...): ...
    def is_claim_active(self, state_row, now=None): ...
    def try_claim(self, execution_date, target_object_name, owner_object_name, claimer_pvz, ttl_minutes, ...): ...
    def get_store_type(self) -> str: ...
```

### Dependency Injection

Для замены storage backend:

```python
from scheduler_runner.tasks.reports.storage import set_default_store
from scheduler_runner.tasks.reports.storage import FailoverStateStore

# Custom implementation
class MyPostgreSQLStore(FailoverStateStore):
    def get_store_type(self): return "postgresql"
    # ... implement all methods

set_default_store(MyPostgreSQLStore())
```

Текущая реализация: `GoogleSheetsFailoverStore`.

## Логи

### Централизованная Система Логирования

Все модули используют `scheduler_runner.utils.logging.configure_logger()`:

- **Ротация**: `RotatingFileHandler`, 10MB, 5 backup-файлов
- **Автоочистка**: файлы старше 5 дней удаляются
- **Кеширование**: предотвращает дублирование хендлеров

| Logger | task_name | log_levels | Файл |
|---|---|---|---|
| Processor | `Processor` | TRACE или DEBUG | `logs/reports_domain/Processor/{date}.log` |
| Parser | `Parser` | TRACE, DEBUG, INFO | `logs/reports_domain/Parser/{date}.log` (+ `_detailed.log`, `_trace.log`) |
| Uploader | `Uploader` | TRACE, DEBUG, INFO | `logs/reports_domain/Uploader/{date}.log` (+ `_debug.log`, `_trace.log`) |
| Notification | `Notification` | TRACE, DEBUG, INFO | `logs/reports_domain/Notification/{date}.log` (+ `_debug.log`, `_trace.log`) |
| FailoverState | `FailoverState` | TRACE | `logs/reports_domain/FailoverState/{date}.log` (+ `_trace.log`) |

### Полезные Сигналы в `Processor` Логах

- `Запуск reports_processor в режиме: backfill`
- `Owner state sync metrics: prefetch_keys=N, prefetch_rows_found=N, persisted_rows=N, suppressed_success=N, upsert_updated=N, upsert_appended=N, upsert_prefetch_matches=N`
- `Retryable error при owner state prefetch: ...; attempt=1/3, retry в N.Ns` — retry после 429
- `Failover coordination dry-run: capability_ranked decision=...`
- `Failover coordination arbitration: mode=..., eligible=..., selected=..., rejected=..., rejected_reasons=...`
- `Продуктовый процессор домена reports завершен успешно`

### Инцидент: Каскадный 429 (10.04.2026)

4 объекта стартовали одновременно (~21:30) → 2 получили 429 на `KPI_FAILOVER_STATE`.
Добавлен retry с jitter в `owner_state_sync.py` (конфигурируемый через `BACKFILL_CONFIG`).

## Тесты

### Unit Tests

```powershell
# All refactored modules tests
.venv\Scripts\python.exe -m pytest scheduler_runner/tasks/reports/tests/ -q
```

### Smoke Tests

```powershell
# Failover state upsert
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.tests.run_failover_state_upsert_smoke --pretty

# Owner success suppression policy
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.tests.run_failover_state_owner_success_policy_smoke --pretty

# KPI reward formulas
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.tests.run_kpi_reward_formulas_e2e_smoke --pretty

# Failover claim (requires Apps Script URL)
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.tests.run_failover_claim_smoke --claim_backend apps_script --pretty

# Telegram notification (⚠️ Telegram заблокирован в РФ — тест не работает без VPN/proxy)
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.tests.run_telegram_notification_e2e_smoke --pretty

# VK notification (production provider)
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.tests.run_vk_notification_e2e_smoke --pretty
```

## Ограничения

- Automatic failover coordination: `opt-in` через `--enable_failover_coordination`
- Active selection mode: `priority_map_legacy`
- Dry-run telemetry: `capability_ranked`
- Pilot priority map:
  - `ЧЕБОКСАРЫ_143 → [ЧЕБОКСАРЫ_144]`
  - `ЧЕБОКСАРЫ_182 → [ЧЕБОКСАРЫ_144]`
  - `ЧЕБОКСАРЫ_144 → [ЧЕБОКСАРЫ_182, ЧЕБОКСАРЫ_143]`
  - `СОСНОВКА_10 → [ЧЕБОКСАРЫ_144]`
  - `ЧЕБОКСАРЫ_340 → []` (isolated)

## Связанные Документы

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — архитектурная диаграмма модулей
- [docs/FAILOVER_COORDINATION.md](docs/FAILOVER_COORDINATION.md) — детальное описание failover flow
- [storage/INFRASTRUCTURE_GOOGLE_SHEETS.md](storage/INFRASTRUCTURE_GOOGLE_SHEETS.md) — подготовка Google Sheets + Apps Script
- [storage/README.md](storage/README.md) — storage protocol и implementations
- [utils/parser/README.md](../../../utils/parser/docs/README.md) — parser runtime docs
