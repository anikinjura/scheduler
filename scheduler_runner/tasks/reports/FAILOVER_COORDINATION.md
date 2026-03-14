# Failover Coordination

Этот документ описывает текущий coordination layer для `tasks/reports`, который позволяет коллегам подхватывать failed даты друг друга.

## Назначение

`KPI` worksheet остается конечным хранилищем KPI-данных.  
`KPI_FAILOVER_STATE` используется только для coordination state:
- кто не смог собрать свою дату;
- кто взял failover claim;
- кто успешно закрыл recovery;
- кто провалил recovery.

Это разделение обязательно:
- `KPI` отвечает на вопрос "есть ли итоговые данные?";
- `KPI_FAILOVER_STATE` отвечает на вопрос "кто сейчас должен их добывать?".

## Worksheet

Worksheet:
- `KPI_FAILOVER_STATE`

Schema описана в:
- [`config/scripts/kpi_failover_state_google_sheets_config.py`](C:/tools/scheduler/scheduler_runner/tasks/reports/config/scripts/kpi_failover_state_google_sheets_config.py)

Основные поля:
- `request_id`
- `Дата`
- `target_pvz`
- `owner_pvz`
- `status`
- `claimed_by`
- `claim_expires_at`
- `attempt_no`
- `source_run_id`
- `last_error`
- `updated_at`

Уникальный ключ:
- `Дата + target_pvz`

## Статусы

Текущие статусы:
- `owner_pending`
- `owner_success`
- `owner_failed`
- `failover_claimed`
- `failover_success`
- `failover_failed`
- `claim_expired`

Terminal statuses:
- `owner_success`
- `failover_success`

## Runtime Flow

Текущий flow в `reports_processor` такой:

1. owner object делает обычный single-PVZ backfill по своему `PVZ_ID`;
2. для missing dates пишет `owner_pending`;
3. по итогам parse:
   - successful dates -> `owner_success`
   - failed dates -> `owner_failed`
4. если включен `--enable_failover_coordination`, processor делает один bounded failover pass:
   - определяет доступных коллег через parser discovery;
   - читает claimable rows из `KPI_FAILOVER_STATE`;
   - фильтрует их через policy layer;
   - пытается claim-ить их;
   - запускает parser за доступные claimed target PVZ;
   - перед upload делает повторный coverage-check;
   - уже закрытые даты не грузит повторно;
   - по результату пишет `failover_success` или `failover_failed`.

## Claim Backend

По умолчанию claim backend сейчас:
- `apps_script`

Config:
- [`config/scripts/reports_processor_config.py`](C:/tools/scheduler/scheduler_runner/tasks/reports/config/scripts/reports_processor_config.py)

Runtime keys:
- `failover_claim_backend`
- `failover_claim_ttl_minutes`
- `failover_max_claims_per_run`
- `failover_apps_script_timeout_seconds`

Apps Script URL и secret сейчас читаются из:
- [`config/reports_paths.py`](C:/tools/scheduler/scheduler_runner/tasks/reports/config/reports_paths.py)

Override через env:
- `FAILOVER_APPS_SCRIPT_URL`
- `FAILOVER_SHARED_SECRET`

## Policy Layer

Policy layer живет отдельно от claim backend:
- [`failover_policy.py`](C:/tools/scheduler/scheduler_runner/tasks/reports/failover_policy.py)

Его задача:
- решать, имеет ли текущий объект право пытаться claim-ить конкретную failed row сейчас;
- не допускать лишних повторных попыток;
- обеспечить детерминированную arbitration policy между коллегами.

Текущая policy поддерживает:
- reject own target;
- reject not accessible target;
- enforce `max_attempts_per_date`;
- explicit `priority_map`;
- rank-based delay.

Config живет в:
- [`config/scripts/reports_processor_config.py`](C:/tools/scheduler/scheduler_runner/tasks/reports/config/scripts/reports_processor_config.py)

Основные policy keys:
- `enabled`
- `priority_map`
- `default_rank_delay_minutes`
- `max_attempts_per_date`
- `max_claims_per_run`
- `allow_unlisted_fallback`

Текущий pilot policy map:
- `ЧЕБОКСАРЫ_143 -> [ЧЕБОКСАРЫ_144]`
- `ЧЕБОКСАРЫ_182 -> [ЧЕБОКСАРЫ_144]`
- `ЧЕБОКСАРЫ_144 -> [ЧЕБОКСАРЫ_182, ЧЕБОКСАРЫ_143]`
- `СОСНОВКА_10 -> [ЧЕБОКСАРЫ_144]`
- `ЧЕБОКСАРЫ_340 -> []`

Практический смысл текущей pilot map:
- `ЧЕБОКСАРЫ_144` выступает primary recovery buddy для `ЧЕБОКСАРЫ_143`, `ЧЕБОКСАРЫ_182` и `СОСНОВКА_10`;
- recovery для `ЧЕБОКСАРЫ_144` идет по rank order: сначала `ЧЕБОКСАРЫ_182`, затем `ЧЕБОКСАРЫ_143` после delay;
- `ЧЕБОКСАРЫ_340` policy-aware слоем считается изолированным target через явное пустое правило `[]`.

## Google Apps Script

Claim Web App нужен для того, чтобы вынести race-sensitive операцию `read -> modify -> write` под `LockService`.

Локальный актуальный script artifact:
- [`.tmp/failover_apps_script_try_claim.gs`](C:/tools/scheduler/.tmp/failover_apps_script_try_claim.gs)

Что должен делать Apps Script:
- принимать `try_claim_failover` request;
- брать `LockService.getScriptLock()`;
- читать текущую строку `KPI_FAILOVER_STATE`;
- если claim уже занят или задача завершена, возвращать отказ;
- иначе записывать `failover_claimed` и возвращать новый state.

Script property, которая должна существовать:
- `FAILOVER_SHARED_SECRET`

Deployment:
- Web App
- `Execute as: Me`
- `Who has access: Anyone` или другой осознанный вариант

## Smoke Check

Manual smoke-script:
- [`tests/run_failover_claim_smoke.py`](C:/tools/scheduler/scheduler_runner/tasks/reports/tests/run_failover_claim_smoke.py)
- [`tests/run_failover_policy_smoke.py`](C:/tools/scheduler/scheduler_runner/tasks/reports/tests/run_failover_policy_smoke.py)

Команда:

```powershell
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.tests.run_failover_claim_smoke --claim_backend apps_script --pretty
```

Что делает smoke:
- создает или обновляет synthetic row:
  - `execution_date = 2099-12-31`
  - `target_pvz = SMOKE_FAILOVER_TARGET`
- выставляет `owner_failed`
- вызывает claim через configured backend
- перечитывает строку из Google Sheets
- печатает JSON result

Что считается успешным:
- `claim_result.success = true`
- `claim_result.claimed = true`
- в `state_after`:
  - `status = failover_claimed`
  - `claimed_by = <claimer_pvz>`
  - `source_run_id = smoke-claim-run`

Synthetic policy-aware smoke:

```powershell
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.tests.run_failover_policy_smoke --claim_backend apps_script --pretty
```

Что делает policy smoke:
- seed-ит синтетические строки в `KPI_FAILOVER_STATE`;
- прогоняет policy eligibility для текущего `claimer_pvz`;
- claim-ит только строки, которые прошли policy filter;
- печатает decisions и claim results в JSON.

Для чего нужен:
- проверить `priority_map`, `own_target`, `isolated target`, `max_attempts` и terminal-state логику без реального multi-object запуска;
- использовать development machine, где текущий объект имитируется через `PVZ_ID` из `pvz_config.ini` и нет возможности реально поднять несколько независимых objects одновременно.

Практический нюанс:
- это synthetic сценарий, а не реальный distributed e2e;
- при частом повторении может упираться в Google Sheets quota по `read requests per minute per user`, поэтому его стоит запускать точечно.

## API/Quota Discipline

Квоты Google нужно экономить, поэтому coordination не должен быть high-frequency.

Текущие правила:
- никакого polling loop
- один coordination pass за запуск
- bounded `failover_max_claims_per_run`
- один reread после claim verification
- один batched coverage-check на recovery batch одного `target_pvz`
- без per-date claim/read loops поверх этого

## Ограничения

Что уже улучшено:
- direct Sheets race для claim больше не используется по умолчанию;
- critical claim section вынесена в Apps Script под `LockService`;
- перед upload recovery path повторно проверяет, не закрыта ли дата уже кем-то другим.

Что еще остается:
- полная arbitration policy между несколькими коллегами еще может меняться;
- grouped manual multi-PVZ path пока не смешан с automatic failover coordination;
- state update paths кроме claim пока остаются на Python-side через Google Sheets API.
- `priority_map` уже заполнен pilot-матрицей для e2e и controlled failover-прогонов, но это еще не финальная business policy для всех объектов;
- текущая карта строится на подтвержденных discovery-связях и операционных гипотезах, поэтому требует дальнейшей валидации на реальных recovery-сценариях.

## Проверка Кода

Unit-tests:

```powershell
.venv\Scripts\python.exe -m pytest scheduler_runner\tasks\reports\tests\test_reports_processor.py scheduler_runner\tasks\reports\tests\test_failover_state.py scheduler_runner\tasks\reports\tests\test_failover_policy.py -q
```
