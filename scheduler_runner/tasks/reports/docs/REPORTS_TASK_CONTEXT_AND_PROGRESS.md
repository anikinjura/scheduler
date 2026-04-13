# Контекст И Прогресс По `scheduler_runner/tasks/reports`

Дата актуализации: `2026-04-09`

## 1. Что это за задача

`scheduler_runner/tasks/reports` — это централизованная scheduler-задача, которая запускается по расписанию из:
- [`scheduler_runner/runner.py`](../../../runner.py)

Она отвечает за:
- проверку покрытия данных в Google Sheets;
- определение missing dates;
- запуск Ozon parser facade;
- загрузку KPI-данных в `KPI`;
- отправку итоговых уведомлений;
- failover coordination между объектами через `KPI_FAILOVER_STATE`.

Важно:
- это не отдельный сервис;
- это не самостоятельный daemon;
- это именно orchestration-task, встроенная в общий scheduler.

## 2. Цель текущей работы

Ключевая бизнес-цель текущего направления:
- сделать так, чтобы объекты могли помогать друг другу при сбое owner-run.

Искомая логика выглядит так:

1. определить, **кому я могу помочь**;
2. определить, **кто нуждается в помощи**;
3. **согласовать**, кто именно будет помогать нуждающемуся;
4. выполнить recovery и закрыть missing data.

Источники для этих шагов:
- live accessibility берется из `available_pvz` через dropdown Ozon;
- нуждающиеся определяются через `KPI_FAILOVER_STATE`;
- согласование выполняется policy/arbitration-слоем;
- recovery идет через parser + upload + failover state update.

## 3. Основные runtime-файлы

На текущий момент центральные runtime-файлы распределены по модулям (декомпозиция завершена `2026-04-11`):

- [`reports_processor.py`](../reports_processor.py) — тонкий orchestrator, **346 строк** (было ~2,172 до декомпозиции)
- [`reports_summary.py`](../reports_summary.py) — dataclasses + status resolution, **340 строк**
- [`reports_upload.py`](../reports_upload.py) — coverage-check + upload, **290 строк**
- [`reports_notifications.py`](../reports_notifications.py) — notification formatting, **287 строк**
- [`reports_scope.py`](../reports_scope.py) — PVZ discovery + job building, **160 строк**
- [`reports_utils.py`](../reports_utils.py) — общие утилиты, **9 строк**
- [`failover_orchestration.py`](../failover_orchestration.py) — full failover flow, **492 строки**
- [`owner_state_sync.py`](../owner_state_sync.py) — owner state + suppression, **335 строк**
- [`failover_policy.py`](../failover_policy.py) — policy rules + arbitration, **275 строк**
- `storage/` — storage abstraction layer с `FailoverStateStore(ABC)`, **~1,072 строк**

## 4. Что было сделано в рамках этой задачи

### 4.1. Исправление discovery `available_pvz`

Проблема:
- вместо реального списка доступных ПВЗ parser вытягивал пункты UI-меню Ozon.

Что сделали:
- обновили selectors и runtime-фильтрацию;
- начали брать реальные значения dropdown.

Эффект:
- в логах больше нет мусора вроде `Выдача заказов`, `Отчеты`, `Ещё`;
- `available_pvz` теперь отражает реальные доступные ПВЗ текущей сессии.

### 4.2. Улучшение поведения при ошибках KPI upload

Проблема:
- при failed upload в `KPI` система могла продолжать owner-state/failover flow так, будто owner upload завершился корректно.

Что сделали:
- добавили более жесткую orchestration-семантику;
- после failed owner upload:
  - не выполняется ложный `owner_state_sync`;
  - не запускается failover pass;
  - summary/logика больше не маскирует отсутствие owner upload.

Эффект:
- исчезли ложные positive-path случаи после upload failure.

### 4.3. Подготовка нового failover redesign

Что сделали:
- добавили dual-mode policy groundwork;
- добавили explainable arbitration evaluation;
- включили `dry_run_capability_ranked=True` при активном legacy mode.

Текущий status:
- active mode: `priority_map_legacy`
- dry-run mode: `capability_ranked`

Эффект:
- в `Processor` логах появились dry-run сигналы и arbitration telemetry;
- можно сравнивать legacy path и новую capability-ranked модель без переключения production behavior.

### 4.4. Перевод notification flow на VK

Что сделано ранее в смежной части работ:
- transport-layer уведомлений стал provider-aware;
- production notifications переключены с Telegram на VK.

Текущее состояние:
- `reports` и `cameras` используют общий notifications layer;
- production provider сейчас `vk`.

### 4.5. Оптимизация owner-state pressure на `KPI_FAILOVER_STATE`

Это последние ключевые оптимизации.

#### Пункт 3

Что сделали:
- `upsert_failover_state_records()` перестал делать старый per-record lookup path;
- вместо этого используется:
  - bulk prefetch existing rows;
  - direct update для существующих;
  - grouped append для новых.

Цель:
- снизить read pressure на `KPI_FAILOVER_STATE`;
- уменьшить вероятность `429 Read requests per minute per user`.

#### Пункт 4

Что сделали:
- перестали писать лишние healthy-new `owner_success` rows;
- terminal `owner_success` теперь сохраняется только для дат с prior incident-related history.

Новая semantics:
- healthy-new success -> suppress;
- prior incident-related state -> persist `owner_success`;
- duplicate prior `owner_success` -> suppress duplicate rewrite.

Цель:
- уменьшить write/read pressure;
- сохранить explainability для реально проблемных дат.

#### Observability patch

Что добавили:
- diagnostics по owner-state sync load.

Новый полезный лог:

```text
Owner state sync metrics: prefetch_keys=..., prefetch_rows_found=..., persisted_rows=..., suppressed_success=..., upsert_updated=..., upsert_appended=..., upsert_prefetch_matches=...
```

Это позволяет анализировать:
- сколько ключей object префетчил;
- сколько existing rows нашлось;
- сколько rows реально записалось;
- сколько healthy-success dates было suppress.

## 5. Статистика и промежуточные итоги по логам

### 5.1. Период `25.03.2026`-`03.04.2026`

По доступным логам было проанализировано `32` вечерних owner-run.

Итог:
- успешных owner-run: `27/32`
- неуспешных owner-run: `5/32`
- случаев реальной помощи коллегам: `0/32`

Основные типы сбоев:
- `AUTH_REQUIRED` / session invalidation;
- browser startup instability;
- отдельные quota/connectivity кейсы Google Sheets.

По объектам:
- `182` — самый стабильный объект периода;
- `340` — самый проблемный.

### 5.2. Период `04.04.2026`-`06.04.2026`

Это период, когда уже наблюдалась dry-run failover telemetry.

Dry-run coverage:
- всего потенциальных evening runs: `12`
- telemetry дошла до `Processor`: `10/12`
- `2/12` оборвались раньше из-за `429` на `KPI_FAILOVER_STATE`

Повторяющиеся dry-run reasons:
- `capability_targets_not_accessible`
- для `340` стабильно `empty_capability_list`

Практический вывод:
- telemetry заработала;
- но система по-прежнему почти не доходила до реального arbitration/recovery.

### 5.3. Логи `07.04.2026`

Это первый полноценный вечерний цикл после rollout последних owner-state optimizations.

Что подтвердилось:
- на `143`, `144`, `182` healthy owner-run больше не писал лишние `owner_success` rows;
- в `Processor` появились новые `Owner state sync metrics`;
- на `owner_state_sync` в этом цикле не было `429`.

Фактические сигналы:
- `prefetch_keys=1`
- `prefetch_rows_found=0`
- `persisted_rows=0`
- `suppressed_success=1`

Что произошло по объектам:
- `143`, `144`, `182` успешно закрыли owner-run;
- `340` снова упал на `AUTH_REQUIRED`.

Главный вывод:
- owner-state optimization дала первый хороший production-сигнал;
- но реальной межобъектной помощи все еще не произошло.

## 6. Почему помощи коллегам пока нет

По текущим логам основной blocker выглядит так:

1. объекты часто видят только себя в `available_pvz`;
2. `340` регулярно падает на `AUTH_REQUIRED`;
3. следовательно, система не доходит до фактического working reachability между объектами.

То есть сейчас главная проблема выглядит не как "поломан arbitration code", а как комбинация:
- runtime topology доступов Ozon session/account;
- нестабильность отдельных owner-сессий.

На `07.04.2026` это проявилось так:
- `143` увидел только `['ЧЕБОКСАРЫ_143']`
- `144` увидел только `['ЧЕБОКСАРЫ_144']`
- `182` увидел только `['ЧЕБОКСАРЫ_182']`
- `340` вообще не дошел до usable state из-за `AUTH_REQUIRED`

В результате:
- dry-run давал `capability_targets_not_accessible`;
- legacy mode давал `priority_candidates_not_accessible`;
- помощь не начиналась.

### 6.1. Организационная задача: права доступа сотрудников

После расследования `08.04.2026` стало ясно: **код работает корректно**. Discovery показывает именно то, что видит сотрудник в dropdown Ozon. Сотрудники с доступом только к своему ПВЗ физически не видят коллег в системе — это не баг парсера, а **ограничение на уровне прав доступа Ozon account**.

Это означает, что failover-механизм упирается не в код, а в **организационные права доступа**. Пока сотрудники работают изолированно — система не сможет начать межобъектную помощь, потому что:
- `available_pvz` = только свой ПВЗ;
- `capability_targets_not_accessible` = все коллеги недоступны;
- arbitration не начинается — некому помочь.

**Варианты решения** (предмет следующего обсуждения):
1. **Наделить сотрудников одного из объектов правами на все объекты** — тогда этот объект станет постоянным failover-helper для всех остальных;
2. **Распределить объекты по сотрудникам** таким образом, чтобы у каждого был доступ к нескольким ПВЗ, forming a helper-capable topology;
3. **Создать отдельную учетную запись с мульти-PVZ доступом** специально для failover-задач, не привязанную к конкретному сотруднику.

Это организационная задача, а не техническая — код уже готов к межобъектной помощи, когда появится runtime reachability.

## 7. Что уже подтверждено

Подтверждено боевыми логами:

1. исправление `available_pvz` selectors;
2. корректная failover semantics после failed owner upload;
3. работа dry-run capability-ranked telemetry;
4. работа VK notification provider;
5. исчезновение лишних healthy `owner_success` writes после последних оптимизаций;
6. появление полезных owner-state load metrics.

## 8. Что пока не подтверждено

Пока еще не подтверждено:
- стабильное реальное межобъектное recovery;
- рабочая topology, при которой объекты регулярно видят друг друга как доступные target/helper candidates;
- устойчивое исчезновение `429` на длинном горизонте наблюдения.

## 9. Нюансы, которые важно помнить

1. `tasks/reports` остается централизованной scheduler-task.
2. Нельзя проектировать refactor так, будто это отдельный сервис.
3. `reports_processor.py` перегружен, но рефакторинг не должен ломать внешний entrypoint.
4. `failover_policy.py` пока еще сравнительно компактный и не является главным кандидатом на разделение.
5. `failover_state.py` вырос, но first priority на декомпозицию сейчас не он, а `reports_processor.py`.

## 10. Что делать дальше

~~На текущий момент правильная стратегия такая:~~

1. ~~продолжать наблюдать боевые логи еще несколько циклов~~ — ✅ наблюдения завершены;
2. ~~не делать немедленный большой refactor~~ — ✅ декомпозиция уже завершена `2026-04-11`;
3. ~~организовать обсуждение прав доступа сотрудников~~ — см. раздел 6.1;

### Дальнейшие шаги

- Наблюдать production-логи на предмет появления реальной межобъектной помощи;
- При переходе на self-hosted — заменить storage backend через `FailoverStateStore(ABC)`;
- Обновлять `.ai/` контекст при значимых production-изменениях.

## 11. Где смотреть подробный план декомпозиции

Подробное приложение находится здесь:
- [REFACTORING_PLAN_APPENDIX.md](REFACTORING_PLAN_APPENDIX.md)

Это приложение описывает:
- целевую структуру модулей;
- какие функции куда переносить;
- в каком порядке делать extraction;
- какие риски есть;
- почему этот refactor лучше выполнять отдельной maintenance-фазой.
