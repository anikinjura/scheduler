# Architecture

Архитектура модулярной версии `scheduler_runner/tasks/reports`.

## Principles

1. **Single Responsibility** — каждый модуль отвечает за одну область
2. **Dependency Inversion** — storage через абстрактный протокол
3. **No Cycles** — зависимости направлены вниз: orchestrator → модули → storage
4. **Backward Compatibility** — CLI contract, логгеры, exit codes не меняются
5. **Extract-Only Refactor** — без behavioral changes

## Module Map

```
┌─────────────────────────────────────────────────────────────────┐
│                    reports_processor.py                         │
│                    Orchestrator (~300 lines)                     │
│  main() → CLI parsing → mode dispatch → sequence control       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────────────┐    │
│  │reports_summary│ │reports_upload│ │reports_notifications │    │
│  │  ~400 lines   │ │  ~370 lines  │ │    ~330 lines        │    │
│  │               │ │              │ │                      │    │
│  │ - dataclasses │ │ - coverage   │ │ - message formatting │    │
│  │ - status res  │ │ - upload     │ │ - VK/Telegram send   │    │
│  │ - helpers     │ │ - retry      │ │ - aggregation        │    │
│  └──────────────┘ └──────────────┘ └──────────────────────┘    │
│                                                                 │
│  ┌──────────────┐ ┌───────────────────┐ ┌──────────────────┐   │
│  │reports_scope │ │failover_orchestra-│ │ owner_state_sync │   │
│  │  ~200 lines  │ │      tion.py      │ │   ~315 lines     │   │
│  │              │ │   ~540 lines      │ │                  │   │
│  │ - PVZ discov │ │ - scan → claim    │ │ - state model    │   │
│  │ - job build  │ │ - recovery        │ │ - suppression    │   │
│  │ - degrade    │ │ - diagnostics     │ │ - diagnostics    │   │
│  └──────────────┘ └───────────────────┘ └──────────────────┘   │
│                                                                 │
│  ┌──────────────┐ ┌───────────────────────────────────────────┐ │
│  │failover_policy│ │              storage/                     │ │
│  │  ~340 lines  │ │  failover_state_protocol.py  ~250 lines   │ │
│  │              │ │  failover_state.py           ~80 lines    │ │
│  │ - priority   │ │  google_sheets_store.py     ~780 lines    │ │
│  │ - capability │ │  google_sheets_failover_store ~90 lines   │ │
│  │ - arbitration│ │                                           │ │
│  └──────────────┘ └───────────────────────────────────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Dependency Directions

### ✅ Correct (acyclic)

```
reports_processor.py
    ↓
┌───┼───────────────┬────────────────┬──────────────┐
▼   ▼               ▼                ▼              ▼
summary upload  notifications    owner_state_sync failover_orchestration
    │   │               │                │              │
    │   │               │                ▼              ▼
    │   │               │          storage/      failover_policy
    │   │               │                │
    ▼   │               │                ▼
 scope ─┘               └──────→ utils/* (parser, uploader, notifications)
```

### ❌ Forbidden (cycles)

```
НЕТ модуль → orchestrator
НЕТ storage → business logic (policy, summary, notifications)
НЕТ utils → reports modules
```

## Storage Protocol Design

### ABC Interface

`FailoverStateStore` — абстрактный класс с 10 методами:

| Метод | Назначение |
|---|---|
| `get_row(date, target_pvz)` | Read single row |
| `get_rows_by_keys(keys)` | Batch read |
| `list_rows(statuses, target_pvz)` | Filtered list |
| `list_candidate_rows(statuses)` | Candidate scan |
| `upsert_record(record)` | Single upsert |
| `upsert_records(records)` | Batch upsert |
| `mark_state(...)` | Status update |
| `is_claim_active(row, now)` | Check claim TTL |
| `try_claim(...)` | Atomic claim |
| `get_store_type()` | Store identifier |

### Implementations

| Реализация | Backend | Строк |
|---|---|---|
| `GoogleSheetsFailoverStore` | Google Sheets API | ~90 (adapter) |
| `PostgreSQLStore` (future) | PostgreSQL + Redis TTL | ~300 (planned) |

### Adding New Backend

1. Создать `storage/postgresql_store.py`
2. Реализовать `class PostgreSQLStore(FailoverStateStore)`
3. Вызвать `set_default_store(PostgreSQLStore())` в инициализации

**Никакие другие модули не меняются.**

## Orchestration Flow

### Single Mode

```
main()
  └→ invoke_parser_for_single_date()
  └→ run_upload_microservice()         ← reports_upload
  └→ prepare_notification_data()       ← reports_notifications
  └→ send_notification_microservice()  ← reports_notifications
```

### Single-PVZ Backfill

```
main()
  └→ resolve_accessible_pvz_ids()      ← reports_scope
  └→ detect_missing_report_dates()     ← reports_upload
  └→ invoke_parser_for_pvz()           ← utils/parser
  └→ run_upload_batch_microservice()   ← reports_upload
  └→ sync_owner_failover_state()       ← owner_state_sync → storage
  └→ run_failover_coordination_pass()  ← failover_orchestration → storage + policy
  └→ build_reports_run_summary()       ← reports_summary
  └→ send_notification_microservice()  ← reports_notifications
```

### Multi-PVZ Backfill

```
main()
  └→ resolve_accessible_pvz_ids()      ← reports_scope
  └→ detect_missing_report_dates_by_pvz() ← reports_upload
  └→ build_jobs_from_missing_dates_by_pvz() ← reports_scope
  └→ invoke_parser_for_grouped_jobs()  ← utils/parser
  └→ run_upload_batch_microservice() × N  ← reports_upload (per PVZ)
  └→ build_aggregated_backfill_summary()  ← reports_notifications
  └→ send_notification_microservice()     ← reports_notifications
```

## Status Resolution Logic

```
resolve_final_run_status(owner, multi_pvz, failover)
  ├→ "failed"     — owner coverage failed OR all parse failed
  ├→ "skipped"    — no work needed (no missing dates, no failover work)
  ├→ "partial"    — some success + some failure OR failover sync failed
  ├→ "success"    — meaningful success with no failures
  └→ "skipped"    — default fallback
```

## Failover Coordination Flow

```
run_failover_coordination_pass()
  ├→ discover_available_pvz_scope()     ← reports_scope
  ├→ collect_failover_scan_decisions()  ← failover_orchestration
  ├→ [if should_scan]
  │   ├→ collect_claimable_failover_rows()  ← policy filter
  │   ├→ claim_failover_rows()              ← storage.try_claim()
  │   └→ run_claimed_failover_backfill()    ← parser + upload + mark
  └→ return result
```

## Module Sizes

| Module | Lines | Tests | Smoke |
|---|---|---|---|
| `reports_processor.py` | 377 | 16 orchestrator tests | — |
| `reports_summary.py` | 405 | 27 tests | — |
| `reports_notifications.py` | 334 | 8 tests | — |
| `reports_upload.py` | 371 | 18 tests | — |
| `reports_scope.py` | 198 | 13 tests | — |
| `failover_orchestration.py` | 539 | 12 tests | — |
| `owner_state_sync.py` | 316 | 14 tests | — |
| `failover_policy.py` | 340 | 19 tests | — |
| `storage/` | 1,190 | 18 tests (copied) | 3 smokes |
| **Total** | **4,070** | **158 tests** | **6 smokes** |

## Migration from Monolith

Было: `reports_processor.py` → 2,172 строки (монолит)
Стало: 8 модулей + storage/ package → 4,070 строк (+1,898 строк за счет тестов и documentation)

Изменения:
- 700+ строк извлечено в отдельные модули
- ~250 строк добавлено (storage protocol, DI, tests, docs)
- 0 behavioral changes

## Future Considerations

### Self-Hosted Migration

При переходе на PostgreSQL:

| Что | Изменится | Останется |
|---|---|---|
| `storage/google_sheets_store.py` | Заменится на `postgresql_store.py` | Protocol (ABC) |
| `storage/google_sheets_failover_store.py` | Заменится на `redis_claim_store.py` | Protocol (ABC) |
| `owner_state_sync.py` | 0 строк (DI injects new store) | Вся логика |
| `failover_orchestration.py` | 0 строк (DI injects new store) | Вся логика |
| `failover_policy.py` | 0 строк | Вся логика |
| `reports_summary.py` | 0 строк | Вся логика |
| `reports_processor.py` | 0 строк | Вся логика |

### Recommended Stack

| Layer | Tech | Why |
|---|---|---|
| Backend | Python FastAPI | Same language, reuse policy logic |
| Database | PostgreSQL | Concurrent access, row-level locking for claims |
| Cache/Locks | Redis | TTL-based claim expiry (replaces Apps Script LockService) |
| Job Queue | Celery + Redis | Async parser runs |
| Scheduler | APScheduler / cron | Replace scheduler_runner/runner.py |
