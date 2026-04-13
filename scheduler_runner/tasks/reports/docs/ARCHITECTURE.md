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
│                    Orchestrator (~350 lines)                     │
│  main() → CLI parsing → mode dispatch → sequence control       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────────────┐    │
│  │reports_summary│ │reports_upload│ │reports_notifications │    │
│  │  ~340 lines   │ │  ~290 lines  │ │    ~290 lines        │    │
│  │               │ │              │ │                      │    │
│  │ - dataclasses │ │ - coverage   │ │ - message formatting │    │
│  │ - status res  │ │ - upload     │ │ - VK/Telegram send   │    │
│  │ - helpers     │ │ - retry      │ │ - aggregation        │    │
│  └──────────────┘ └──────────────┘ └──────────────────────┘    │
│                                                                 │
│  ┌──────────────┐ ┌───────────────────┐ ┌──────────────────┐   │
│  │reports_scope │ │failover_orchestra-│ │ owner_state_sync │   │
│  │  ~160 lines  │ │      tion.py      │ │   ~335 lines     │   │
│  │              │ │   ~490 lines      │ │                  │   │
│  │ - PVZ discov │ │ - scan → claim    │ │ - state model    │   │
│  │ - job build  │ │ - recovery        │ │ - suppression    │   │
│  │ - degrade    │ │ - diagnostics     │ │ - diagnostics    │   │
│  └──────────────┘ └───────────────────┘ └──────────────────┘   │
│                                                                 │
│  ┌──────────────┐ ┌──────────────┐ ┌─────────────────────────┐ │
│  │failover_policy│ │reports_utils │ │         storage/        │ │
│  │  ~275 lines  │ │  ~10 lines   │ │ failover_state_protocol │ │
│  │              │ │              │ │   ~210 lines            │ │
│  │ - priority   │ │ - normalize  │ │ failover_state ~70      │ │
│  │ - capability │ │   pvz_id     │ │ google_sheets_store     │ │
│  │ - arbitration│ │              │ │   ~670 lines            │ │
│  └──────────────┘ └──────────────┘ │ google_sheets_store     │ │
│                                    │   adapter ~70 lines     │ │
│                                    └─────────────────────────┘ │
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
| `get_row(date, target_object_name)` | Read single row |
| `get_rows_by_keys(keys)` | Batch read |
| `list_rows(statuses, target_object_name)` | Filtered list |
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
| `reports_processor.py` | 346 | 16 orchestrator tests | — |
| `reports_summary.py` | 340 | 28 tests | — |
| `reports_notifications.py` | 287 | 8 tests | — |
| `reports_upload.py` | 290 | 18 tests | — |
| `reports_scope.py` | 160 | 13 tests | — |
| `reports_utils.py` | 9 | (shared, tested via upload/scope) | — |
| `failover_orchestration.py` | 492 | 12 tests | — |
| `owner_state_sync.py` | 335 | 21 tests | — |
| `failover_policy.py` | 275 | 19 tests | — |
| `storage/` | 1,075 | 18 tests (copied) | 4 smokes |
| **Total runtime** | **3,425** | **163 tests** | **7 smokes** |

## Migration from Monolith

Было: `reports_processor.py` → 2,172 строки (монолит)
Стало: 9 модулей + storage/ package → 3,425 строк (+1,253 строк за счет тестов и документации)

Изменения:
- 700+ строк извлечено в отдельные модули
- ~250 строк добавлено (storage protocol, DI, tests, docs)
- ~40 строк добавлено (owner state sync retry с jitter)
- 0 behavioral changes

## Централизованная Система Логирования

Все модули reports используют `scheduler_runner.utils.logging.configure_logger()` — централизованную систему логирования с ротацией и автоочисткой.

### Logger Роли

| Logger | user | task_name | log_levels | Назначение |
|---|---|---|---|---|
| Processor | `reports_domain` | `Processor` | TRACE или DEBUG | orchestration flow, owner sync metrics, failover decisions |
| Parser | `reports_domain` | `Parser` | TRACE, DEBUG, INFO | parser invocation, page navigation, data extraction |
| Uploader | `reports_domain` | `Uploader` | TRACE, DEBUG, INFO | coverage-check, upload results, retry attempts |
| Notification | `reports_domain` | `Notification` | TRACE, DEBUG, INFO | message formatting, VK/Telegram delivery status |
| FailoverState | `reports_domain` | `FailoverState` | TRACE | failover state reads/writes, claim operations, 429 retry |

### Структура Лог-Файлов

На рабочем объекте логи формируются в:
```
logs/reports_domain/Processor/{YYYY-MM-DD}.log
logs/reports_domain/Parser/{YYYY-MM-DD}.log
logs/reports_domain/Parser/{YYYY-MM-DD}_detailed.log
logs/reports_domain/Parser/{YYYY-MM-DD}_trace.log
logs/reports_domain/Uploader/{YYYY-MM-DD}.log
logs/reports_domain/Uploader/{YYYY-MM-DD}_debug.log
logs/reports_domain/Uploader/{YYYY-MM-DD}_trace.log
logs/reports_domain/Notification/{YYYY-MM-DD}.log
logs/reports_domain/Notification/{YYYY-MM-DD}_debug.log
logs/reports_domain/Notification/{YYYY-MM-DD}_trace.log
logs/reports_domain/FailoverState/{YYYY-MM-DD}.log
logs/reports_domain/FailoverState/{YYYY-MM-DD}_trace.log
```

### Особенности

- **Ротация**: `RotatingFileHandler`, 10MB максимум, 5 backup-файлов
- **Автоочистка**: файлы старше 5 дней удаляются автоматически
- **Кеширование**: `_LOGGERS` dict предотвращает дублирование хендлеров
- **Формат**: `%(asctime)s %(levelname)s [{user}.{task_name}] %(message)s`
- **TRACE_LEVEL = 5**: кастомный уровень ниже DEBUG для трассировки методов

### Полезные Сигналы в Логах

**Processor:**
- `Запуск reports_processor в режиме: backfill`
- `Owner state sync metrics: prefetch_keys=N, prefetch_rows_found=N, persisted_rows=N, suppressed_success=N, ...`
- `Retryable error при owner state prefetch: ...; attempt=1/3, retry в N.Ns` — retry после 429
- `Failover coordination dry-run: capability_ranked decision=...`
- `Failover coordination arbitration: mode=..., eligible=..., selected=..., rejected=..., rejected_reasons=...`
- `Продуктовый процессор домена reports завершен успешно`

**Uploader:**
- `Проверка покрытия Google Sheets за диапазон ...`
- `Повторная попытка batch upload в Google Sheets: attempt=2/3`

**FailoverState:**
- `Попытка подключения к целевой системе...`
- `Retryable error при owner state prefetch: [429] ...` — retry с jitter

### Инцидент: Каскадный 429 (10.04.2026)

Все 4 объекта стартовали одновременно (~21:30) и получили 429 на `KPI_FAILOVER_STATE`.
- 21:32:45 — **143 получил 429** → retry (2s + jitter) → retry (4s + jitter) → fail
- 21:32:52 — **182 получил 429** → retry (2s + jitter) → retry (4s + jitter) → fail
- 21:33:09 — **340 успел** (первый)
- 21:33:17 — **144 успел** (второй)

Митигация: retry с jitter добавлен в `owner_state_sync.py` (`_get_rows_by_keys_with_retry`).
Все параметры конфигурируются через `BACKFILL_CONFIG`.

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
