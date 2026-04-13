# Storage Abstraction Layer

`storage/` — abstraction layer для работы с `KPI_FAILOVER_STATE`.

Определяет контракт `FailoverStateStore(ABC)` и текущую Google Sheets реализацию.

## Structure

```
storage/
├── __init__.py                   ← re-exports + DI helpers
├── failover_state_protocol.py    ← ABC интерфейс (~210 строк)
├── failover_state.py             ← backward-compatible re-export + DI (~70 строк)
├── google_sheets_store.py        ← Google Sheets implementation (~670 строк)
└── google_sheets_failover_store.py ← GoogleSheetsFailoverStore adapter (~70 строк)
```

## Protocol

### `FailoverStateStore(ABC)`

Абстрактный класс, определяющий контракт для любого storage backend:

```python
from scheduler_runner.tasks.reports.storage import FailoverStateStore

class MyCustomStore(FailoverStateStore):
    def get_store_type(self) -> str:
        return "my_custom"

    def get_row(self, execution_date: str, target_object_name: str):
        # ...

    def get_rows_by_keys(self, keys: list[dict]) -> dict[tuple, dict]:
        # ...

    def list_rows(self, statuses=None, target_object_name=None):
        # ...

    def list_candidate_rows(self, statuses=None):
        # ...

    def upsert_record(self, record: dict) -> dict:
        # ...

    def upsert_records(self, records: list[dict]) -> dict:
        # ...

    def mark_state(self, execution_date, target_object_name, owner_object_name, status, ...):
        # ...

    def is_claim_active(self, state_row, now=None) -> bool:
        # ...

    def try_claim(self, execution_date, target_object_name, owner_object_name, claimer_pvz, ttl_minutes, ...):
        # ...
```

### Contract Rules

- Ключ записи: `(execution_date: str YYYY-MM-DD, target_object_name: str)`
- Все даты возвращаются/принимаются в формате `YYYY-MM-DD`
- `record dict` содержит все поля строки состояния
- Timestamps: `DD.MM.YYYY HH:MM:SS`

## Status Constants

Экспортируются из `failover_state_protocol.py`:

```python
from scheduler_runner.tasks.reports.storage import (
    STATUS_OWNER_PENDING,
    STATUS_OWNER_SUCCESS,
    STATUS_OWNER_FAILED,
    STATUS_FAILOVER_CLAIMED,
    STATUS_FAILOVER_SUCCESS,
    STATUS_FAILOVER_FAILED,
    STATUS_CLAIM_EXPIRED,
    TERMINAL_STATUSES,
)
```

## Helpers

### `build_failover_state_record(...)`

Создает record dict для upsert:

```python
from scheduler_runner.tasks.reports.storage import build_failover_state_record

record = build_failover_state_record(
    execution_date="2026-04-01",
    target_object_name="PVZ1",
    owner_object_name="PVZ1",
    status="owner_failed",
    source_run_id="20260401210000|PVZ1",
    last_error="parser_failed",
)
```

### `build_failover_request_id(...)`

Создает уникальный request_id: `"{execution_date}|{target_object_name}"`

### `is_claim_active_stateless(...)`

Проверяет активен ли claim без обращения к storage.

## Google Sheets Implementation

### `GoogleSheetsFailoverStore`

Adapter class — делегирует существующим функциям Google Sheets:

```python
from scheduler_runner.tasks.reports.storage.google_sheets_failover_store import GoogleSheetsFailoverStore

store = GoogleSheetsFailoverStore()
store.get_store_type()  # → "google_sheets"
store.get_row("2026-04-01", "PVZ1")
store.upsert_records([record1, record2])
store.try_claim("2026-04-01", "PVZ2", "PVZ2", "PVZ1", ttl_minutes=15)
```

### Features

- **Batch read optimization**: `get_rows_by_keys()` использует `batch_get` вместо per-row requests
- **Batch write optimization**: `upsert_records()` prefetch → update existing → append new
- **Apps Script claim backend**: `try_claim()` вызывает Apps Script Web App под `LockService`
- **Fallback claim**: `try_claim_via_sheets()` — Sheets-side race condition fallback

### Configuration

Connection params из `kpi_failover_state_google_sheets_config.py`:
- `SPREADSHEET_ID`
- `WORKSHEET_NAME`
- `TABLE_CONFIG` (column schema)

Apps Script URL и secret из `reports_paths.py` или env vars:
- `FAILOVER_APPS_SCRIPT_URL`
- `FAILOVER_SHARED_SECRET`

## Dependency Injection

### Установка Default Store

```python
from scheduler_runner.tasks.reports.storage import set_default_store, get_default_store

# Проверить текущий
store = get_default_store()
print(store.get_store_type())  # → "google_sheets"

# Заменить на кастомный
set_default_store(MyPostgreSQLStore())
```

### Per-Call Injection

Функции, принимающие `store` параметр:

```python
from scheduler_runner.tasks.reports.owner_state_sync import sync_owner_failover_state_from_batch_result

sync_owner_failover_state_from_batch_result(
    owner_object_name="PVZ1",
    missing_dates=["2026-04-01"],
    batch_result=batch_result,
    upload_result=upload_result,
    store=my_custom_store,  # ← DI
)
```

## Adding New Backend

1. Создать файл: `storage/my_backend_store.py`
2. Реализовать protocol:

```python
from scheduler_runner.tasks.reports.storage.failover_state_protocol import FailoverStateStore

class MyBackendStore(FailoverStateStore):
    def get_store_type(self): return "my_backend"
    # ... implement all 9 methods
```

3. Использовать:

```python
from scheduler_runner.tasks.reports.storage import set_default_store
from scheduler_runner.tasks.reports.storage.my_backend_store import MyBackendStore

set_default_store(MyBackendStore())
```

**Никакие другие модули не меняются.**

## Testing

### Unit Tests

Storage tests скопированы из боевой версии с адаптированными импортами:

```powershell
.venv\Scripts\python.exe -m pytest scheduler_runner/tasks/reports/tests/test_failover_state.py -q
```

### Smoke Tests

```powershell
# Upsert batch path
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.tests.run_failover_state_upsert_smoke --pretty

# Owner success suppression
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.tests.run_failover_state_owner_success_policy_smoke --pretty
```

## Backward Compatibility

Все старые импорты продолжают работать через `failover_state.py` re-export layer:

```python
# Старый стиль (работает)
from scheduler_runner.tasks.reports.storage.failover_state import (
    STATUS_OWNER_FAILED,
    build_failover_state_record,
    upsert_failover_state_records,
)

# Новый стиль (protocol-based)
from scheduler_runner.tasks.reports.storage import (
    FailoverStateStore,
    STATUS_OWNER_FAILED,
    build_failover_state_record,
    get_default_store,
)
```

