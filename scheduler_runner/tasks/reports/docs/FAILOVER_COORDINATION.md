# Failover Coordination

Coordination layer для failover recovery между коллегами через `KPI_FAILOVER_STATE`.

План доработки (молчащие ПВЗ, брошенные захваты, возврат ПВЗ): [FAILOVER_REDESIGN_PLAN.md](FAILOVER_REDESIGN_PLAN.md).

## Назначение

`KPI` worksheet — конечное хранилище KPI-данных.
`KPI_FAILOVER_STATE` — coordination state:
- кто не смог собрать свою дату;
- кто взял failover claim;
- кто успешно закрыл recovery;
- кто провалил recovery.

## Расположение Модулей

После декомпозиции модули расположены так:

```
scheduler_runner/tasks/reports/
├── failover_orchestration.py    ← полный failover coordination flow
├── failover_policy.py           ← policy rules + arbitration
├── owner_state_sync.py          ← owner state model + suppression
└── storage/
    ├── failover_state_protocol.py  ← ABC интерфейс
    ├── failover_state.py           ← re-export + DI
    ├── google_sheets_store.py      ← Google Sheets implementation
    └── google_sheets_failover_store.py ← adapter class
```

## Storage Protocol

### `FailoverStateStore(ABC)`

Абстрактный интерфейс с 9 методами + 1 идентификатор.

Любая реализация storage (Google Sheets, PostgreSQL, etc.) должна реализовать все методы:

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

### Текущая Реализация

`GoogleSheetsFailoverStore` — adapter class, делегирует существующим функциям:
- `get_rows_by_keys()` → batch_get оптимизация
- `upsert_records()` → prefetch → update → append
- `try_claim()` → Apps Script LockService

## Runtime Flow

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

## Статусы

```
owner_pending → owner_failed / owner_success
                    ↓
           failover_claimed
                    ↓
        failover_success / failover_failed
```

Terminal statuses: `owner_success`, `failover_success`.

Кандидаты помощника: `owner_failed`, `failover_failed`, `claim_expired` и `failover_claimed` с истекшим
`claim_expires_at` (брошенный захват: помощник упал или компьютер выключился; Apps Script такой захват
перехватывает). Действующий захват отсекается (`claim_active`). Срок захвата — `failover_claim_ttl_minutes` (30).

### Состояние владельца (с 01.10.2026, этап A плана)

После своего парсинга владелец **при любом исходе** записывает итог по своим missing dates (если включена
coordination): `owner_failed` с причиной — по датам, которые не собраны (ошибка даты, нет результата по дате,
падение парсера) или не загружены (сбой Google Sheets). «Нет данных для загрузки» (`no_data`, ни одна дата не собрана)
сбоем загрузки не считается — причиной в `last_error` остается ошибка парсинга. Падение парсера (браузер не
запустился, `AUTH_REQUIRED`) не прерывает запуск: `OWNER_PARSE_CRASHED` в логе, все даты — `owner_failed`, уведомление
отправляется.

Failover-проход (помощь коллегам) после этого пропускается с `FAILOVER_PASS_SKIPPED reason=...`:
- `own_dates_all_failed` — ни одна своя дата не собрана (сессия Ozon, скорее всего, мертва);
- `owner_upload_failed` — своя загрузка в Google Sheets не прошла;
- `owner_state_sync_failed` — не удалось записать свое состояние.

### Молчащие ПВЗ (этап C, флаг `failover_detect_silent_owners`)

ПВЗ, который не запускался, строк состояния не пишет. Помощник при включенном флаге в том же проходе:
1. берет цели из `priority_map` (где он в списке; в `capability_ranked` — где он предпочтительный), доступные его учетной записи;
2. одним batched coverage-check ищет пропуски в `KPI` за окно `candidate_window_days` **до вчерашнего дня**
   (сегодняшнюю дату сосед, возможно, еще собирает); помощник ранга r > 1 берет только даты старше еще на
   `(r-1) * failover_silent_rank_lag_days` дней;
3. отбрасывает даты, по которым уже есть строка в `KPI_FAILOVER_STATE` (их ведет обычный путь);
4. захватывает с `create_if_missing` — Apps Script версии 2 добавляет строку сразу захваченной
   (`created_and_claimed`, `last_error=owner_silent`) под той же блокировкой.

Старый Apps Script отвечает `row_not_found` — в лог `FAILOVER_SILENT_CLAIM_UNSUPPORTED`, дата пропускается.

### Owner Success Suppression

- healthy-new success rows → **suppress** (не пишутся)
- prior incident-related history → **persist** `owner_success`
- duplicate prior `owner_success` → **suppress** rewrite

## Policy Layer

Расположение: `failover_policy.py`

### Текущая Policy Поддерживает

- reject own target
- reject active claim (`claim_active`) and dates older than `candidate_window_days` (`outside_window`)
- reject not accessible target
- enforce `max_attempts_per_date`
- explicit `priority_map`
- rank-based delay
- dual-mode selection API:
  - active mode: `priority_map_legacy`
  - dry-run: `capability_ranked`

### Pilot Policy Map

```
ЧЕБОКСАРЫ_143 → [ЧЕБОКСАРЫ_144]
ЧЕБОКСАРЫ_182 → [ЧЕБОКСАРЫ_144]
ЧЕБОКСАРЫ_144 → [ЧЕБОКСАРЫ_182, ЧЕБОКСАРЫ_143]
СОСНОВКА_10   → [ЧЕБОКСАРЫ_144]
ЧЕБОКСАРЫ_340 → []  (isolated)
```

## Claim Backend

Default: `apps_script`

Runtime keys:
- `failover_claim_backend`
- `failover_claim_ttl_minutes`
- `failover_max_claims_per_run`

Apps Script URL / secret задаются только в `.env/secrets.env` (читает `config/reports_paths.py`):
- `FAILOVER_APPS_SCRIPT_URL`, `FAILOVER_SHARED_SECRET` — production;
- `FAILOVER_APPS_SCRIPT_URL_TEST`, `FAILOVER_SHARED_SECRET_TEST` — `ENV_MODE=test`.

Пустые значения — захват невозможен (`apps_script_url_missing`), failover молча не работает.

## Dependency Injection

Для замены storage backend:

```python
from scheduler_runner.tasks.reports.storage import set_default_store, FailoverStateStore

class MyPostgreSQLStore(FailoverStateStore):
    def get_store_type(self): return "postgresql"
    # ... implement all methods

set_default_store(MyPostgreSQLStore())
```

## Smoke Tests

```powershell
# Upsert batch path
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.tests.run_failover_state_upsert_smoke --pretty

# Owner success suppression
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.tests.run_failover_state_owner_success_policy_smoke --pretty

# Claim (requires Apps Script URL)
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.tests.run_failover_claim_smoke --claim_backend apps_script --pretty

# Policy eligibility
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.tests.run_failover_policy_smoke --claim_backend apps_script --pretty
```

## Observability

Полезные сигналы в `Processor` логах:

```
Owner state sync metrics: prefetch_keys=N, prefetch_rows_found=N, persisted_rows=N, suppressed_success=N, upsert_updated=N, upsert_appended=N, upsert_prefetch_matches=N
Failover coordination dry-run: capability_ranked decision=...
Failover coordination arbitration: mode=..., eligible=..., selected=..., rejected=..., rejected_reasons=...
FAILOVER_PASS_SKIPPED reason=own_dates_all_failed|owner_upload_failed|owner_state_sync_failed
FAILOVER_SILENT_OWNER_DATES target=... rank=... dates=[...]
FAILOVER_SILENT_SCAN_FAILED: <ошибка>   (проход продолжается по строкам состояния)
FAILOVER_SILENT_CLAIM_UNSUPPORTED: ...  (Apps Script без create_if_missing)
FAILOVER_PVZ_RESTORE → FAILOVER_PVZ_RESTORED / FAILOVER_PVZ_RESTORE_FAILED   (возврат своего ПВЗ после пакета соседа)
OWNER_PARSE_CRASHED: <ошибка парсера>
Retryable error при owner state prefetch: [429] ...; attempt=1/3, retry в N.Ns
```

### Каскадный 429 и Retry

При одновременном старте нескольких объектов возникает Google Sheets API 429
(`Read requests per minute per user`). Начиная с модульной версии, `owner_state_sync`
обёрнут в retry с exponential backoff + random jitter:

- **max_attempts**: 3 (конфигурируется: `owner_state_sync_max_attempts`)
- **backoff**: 2s → 4s (exponential, configurable: `base_delay_seconds`, `max_delay_seconds`)
- **jitter**: ±1s (configurable: `owner_state_sync_jitter_seconds`)

Non-retryable ошибки (auth, connection) пробрасываются немедленно.

С 30.09.2026 под этим retry работает транспортный повтор `QuotaBackoffHTTPClient` (для всех запросов к Google Sheets):
429 повторяется через 65+ с до 5 попыток, так как квота восстанавливается поминутно, а паузы 2–4 с ее не дожидаются.
Ошибка API при чтении состояния (`get_failover_state`, поиск по ключам) пробрасывается, а не трактуется как
«записи нет» — решение о claim не принимается по неполным данным.

## Quota Discipline

- один coordination pass за запуск
- bounded `failover_max_claims_per_run`
- batch_get для read оптимизации
- bulk prefetch + update/append для write оптимизации
- suppression лишних healthy-success writes
- один batched coverage-check на recovery batch

## Ограничения

- Automatic coordination: `opt-in` через `--enable_failover_coordination`
- Active selection mode: `priority_map_legacy`
- `dry_run_capability_ranked=True` — telemetry only
- Pilot map — не финальная business policy
- `ЧЕБОКСАРЫ_340` изолирован — требует organizational fix

## Проверка Кода

```powershell
# All refactored module tests
.venv\Scripts\python.exe -m pytest scheduler_runner/tasks/reports/tests/ -q
```
