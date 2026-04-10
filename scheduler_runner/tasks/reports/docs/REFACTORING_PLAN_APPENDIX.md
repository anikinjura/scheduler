# Приложение: План Декомпозиции `scheduler_runner/tasks/reports`

Дата актуализации: `2026-04-09`

Этот документ является приложением к:
- [REPORTS_TASK_CONTEXT_AND_PROGRESS.md](C:/tools/scheduler/scheduler_runner/tasks/reports/docs/REPORTS_TASK_CONTEXT_AND_PROGRESS.md)

Он фиксирует подробный технический план будущей декомпозиции `tasks/reports`, но не означает немедленной реализации.

## 1. Почему декомпозиция нужна

Текущая проблема:
- [`reports_processor.py`](C:/tools/scheduler/scheduler_runner/tasks/reports/reports_processor.py) одновременно содержит orchestration, summary logic, upload helpers, notifications, owner-state logic, failover scan orchestration и scope-resolution.

Это приводит к:
- высокому cognitive load;
- тяжелым диффам;
- усложнению инцидентного анализа;
- повышенному риску при каждом следующем изменении.

## 2. Почему декомпозицию не стоит делать немедленно

Сейчас production-наблюдение еще продолжается по свежим behavioral changes:
- optimized failover-state upsert;
- healthy-success suppression;
- owner-state diagnostics.

Если начать большой structural refactor немедленно, то:
- поведенческие сигналы смешаются с рефакторингом;
- станет труднее анализировать логи и regressions;
- maintenance-фаза потеряет чистую границу.

Вывод:
- декомпозиция нужна;
- но ее лучше делать отдельно, после стабилизации наблюдений.

## 3. Базовый принцип

Что должно остаться неизменным:
- task по-прежнему запускается централизованно из [`scheduler_runner/runner.py`](C:/tools/scheduler/scheduler_runner/runner.py)
- внешний CLI/runtime contract остается тем же;
- scheduler semantics не меняются;
- декомпозиция выполняется как extract-only maintenance refactor, без behavioral changes.

## 4. Целевая структура

```text
scheduler_runner/tasks/reports/
  reports_processor.py
  failover_policy.py
  failover_state.py
  owner_state_sync.py
  failover_orchestration.py
  reports_scope.py
  reports_upload.py
  reports_notifications.py
  reports_summary.py
  report_types.py
```

## 5. Назначение будущих модулей

### 5.1. `reports_processor.py`

Оставить:
- `main()`
- `build_processor_run_id(...)` — orchestration-level утилита, используется только в `main()`
- high-level orchestration
- sequence control:
  - coverage
  - parse
  - upload
  - owner sync
  - failover
  - notification

Цель:
- превратить файл в тонкий orchestration entrypoint.

### 5.2. `owner_state_sync.py`

Вынести сюда:
- `mark_dates_with_owner_status(...)`
- `classify_owner_success_history(...)`
- `should_persist_owner_success_from_history(...)`
- `build_owner_final_failover_state_records(...)`
- `sync_owner_failover_state_from_batch_result(...)`

Ответственность:
- owner-side state model;
- suppression policy;
- terminal success persistence;
- owner-state diagnostics shaping.

### 5.3. `failover_orchestration.py`

Вынести сюда:
- `extract_batch_failures(...)` — извлечение failed dates из batch_result для recovery
- `build_filtered_batch_result(...)` — фильтрация batch_result по execution_dates (recovery dedup)
- `is_failover_candidate_scan_retryable_error(...)` — классификация retryable ошибок candidate scan
- `collect_claimable_failover_rows(...)`
- `normalize_claimable_failover_evaluation(...)`
- `should_scan_failover_candidates(...)`
- `should_scan_failover_candidates_legacy(...)`
- `should_scan_failover_candidates_capability_ranked(...)`
- `collect_failover_scan_decisions(...)`
- `claim_failover_rows(...)`
- `run_claimed_failover_backfill(...)`
- `run_failover_coordination_pass(...)`

Ответственность:
- orchestration всего failover-phase;
- candidate evaluation;
- claim path;
- recovery execution.

### 5.4. `reports_scope.py`

Вынести сюда:
- `build_jobs_from_missing_dates_by_pvz(...)`
- `group_jobs_by_pvz(...)`
- `normalize_pvz_id(...)`
- `resolve_pvz_ids(...)`
- `discover_available_pvz_scope(...)`
- `resolve_accessible_pvz_ids(...)`
- `should_run_automatic_failover_coordination(...)`

Ответственность:
- scope-resolution;
- live accessibility;
- degrade `multi -> single`;
- pre-check доступности ПВЗ.

### 5.5. `reports_upload.py`

Вынести сюда:
- `create_uploader_logger()` — factory для upload-специфичного логгера
- `prepare_connection_params()` — подготовка параметров Google Sheets connection
- `detect_missing_report_dates(...)` — coverage-check для одного ПВЗ
- `detect_missing_report_dates_by_pvz(...)` — coverage-check для нескольких ПВЗ
- `prepare_coverage_filters(...)` — подготовка фильтров для coverage-check
- `parse_sheet_date_to_iso(...)` — парсинг даты из Google Sheets
- `prepare_upload_data(...)`
- `prepare_upload_data_batch(...)`
- `run_upload_microservice(...)`
- `run_upload_batch_microservice(...)` — внутренне делегирует `run_upload_microservice`
- `transform_record_for_upload(...)`
- `is_retryable_google_sheets_upload_error(...)`
- `run_google_sheets_upload_with_retry(...)`

Ответственность:
- coverage-check logic;
- upload payload preparation;
- KPI upload orchestration;
- retry behavior.

### 5.6. `reports_notifications.py`

Вынести сюда:
- `create_notification_logger()` — factory для notification-специфичного логгера
- `prepare_notification_data(...)`
- `format_notification_message(...)`
- `prepare_batch_notification_data(...)`
- `format_batch_notification_message(...)`
- `send_notification_microservice(...)`
- `format_reports_run_notification_message(...)`
- `format_aggregated_backfill_notification_message(...)`
- `_format_failed_dates(...)` — helper для форматирования списка failed dates

Ответственность:
- messaging;
- notification payload shaping;
- отправка через общий notifications layer.

### 5.7. `reports_summary.py`

Вынести сюда:
- summary dataclasses:
  - `PVZExecutionResult`
  - `ReportsBackfillExecutionResult`
  - `OwnerRunSummary`
  - `FailoverRunSummary` — частично инициализируется из dict (`failover_result`), частично из typed-полей
  - `ReportsRunSummary`
- status/summary helpers:
  - `build_pvz_execution_result(...)`
  - `build_owner_run_summary(...)`
  - `build_failover_run_summary(...)`
  - `build_reports_run_summary(...)`
  - `resolve_final_run_status(...)` — **обязательно** вынести вместе с `_owner_*` и `_failover_*` хелперами
  - `build_aggregated_backfill_summary(...)`
- owner/failover status хелперы, используемые `resolve_final_run_status` и `format_reports_run_notification_message`:
  - `_owner_has_work(...)`
  - `_is_owner_skipped_no_missing(...)`
  - `_owner_had_meaningful_success(...)`
  - `_owner_had_meaningful_failure(...)`
  - `_failover_had_meaningful_success(...)`
  - `_failover_had_meaningful_failure(...)`
  - `_failover_sync_had_failure(...)`
  - `_failover_candidate_scan_had_failure(...)`
  - `_failover_had_any_work(...)`
- batch result helpers, используемые `build_owner_run_summary`:
  - `_as_date_list(...)` — нормализация значений дат
  - `_count_batch_successful_dates(...)` — подсчёт успешных дат
  - `_count_batch_failed_dates(...)` — подсчёт failed дат

Ответственность:
- единая модель итогов run;
- единая логика финального статуса;
- интерпретация owner/failover outcome для status resolution и notification formatting.

Важное замечание про циклические импорты:
- `resolve_final_run_status` вызывает все `_owner_*` и `_failover_*` хелперы
- `format_reports_run_notification_message` (в `reports_notifications.py`) вызывает `_is_owner_skipped_no_missing` и другие через `summary`
- `resolve_final_run_status` **должен** остаться в `reports_summary.py`, а не в `reports_processor.py`
- Иначе возникает риск цикла: `reports_processor` → `reports_summary` → `reports_notifications` → `reports_processor`
- `reports_notifications.py` зависит только от `reports_summary.py` (импортирует dataclasses), это безопасно

### 5.8. `report_types.py`

Опциональный модуль.

Использовать только если понадобится вынести:
- dataclasses отдельно от summary logic;
- typed aliases;
- small result payload contracts.

## 6. Что пока не трогать

### `failover_policy.py`

Почему пока не трогать:
- файл сравнительно компактный;
- тематически цельный;
- является наименее проблемной частью текущей архитектуры.

### `failover_state.py`

Почему пока не first priority:
- файл вырос, но еще остается тематически связанным;
- его можно делить позже второй волной, если это действительно понадобится.

Потенциальная future-декомпозиция:
- `failover_state_storage.py`
- `failover_claim_backends.py`
- `failover_state_records.py`

Но сейчас это преждевременно.

## 7. Предлагаемый порядок внедрения

### Фаза 0. Наблюдение

Ничего не рефакторить, пока не накоплены еще несколько боевых циклов после свежих owner-state optimizations.

Условие перехода:
- новые логи не показывают regressions;
- owner-state logic выглядит устойчиво;
- нет срочного production-инцидента, который требует behavioral fix вместо refactor.

### Фаза 1. Low-risk extraction

Самая безопасная последовательность:

1. `reports_summary.py`
2. `reports_notifications.py`
3. `reports_upload.py`

Почему:
- здесь больше pure helpers;
- ниже риск случайно поменять orchestration behavior.

### Фаза 2. Medium-risk extraction

Затем:

1. `reports_scope.py`
2. `owner_state_sync.py`

Почему:
- здесь уже больше предметной логики;
- но она достаточно локализуема.

### Фаза 3. High-risk extraction

Последним:

1. `failover_orchestration.py`

Почему:
- это самый чувствительный участок для coordination behavior;
- сюда лучше приходить уже после стабилизации остальной декомпозиции.

## 8. Правила безопасной реализации

1. Не менять внешний CLI contract.
2. Не менять scheduler entry semantics.
3. Не переименовывать logger names без причины.
4. Не смешивать refactor и behavioral changes в одном коммите.
5. Делать extract-only коммиты.
6. После каждого этапа прогонять профильные тесты.
7. После каждого этапа обновлять docs и `.ai`.

## 9. Тестовая стратегия

### Минимальный набор после каждой extraction-фазы

```powershell
.venv\Scripts\python.exe -m pytest scheduler_runner\tasks\reports\tests\test_reports_processor.py scheduler_runner\tasks\reports\tests\test_failover_state.py scheduler_runner\tasks\reports\tests\test_failover_policy.py -q
```

### Runtime import-cycle test (после каждой фазы)

```powershell
.venv\Scripts\python.exe -m pytest scheduler_runner\tasks\reports\tests\test_module_imports.py -q
```

Содержание `test_module_imports.py`:

```python
"""Проверка отсутствия циклических импортов после декомпозиции."""

def test_no_circular_imports():
    from scheduler_runner.tasks.reports import reports_processor
    from scheduler_runner.tasks.reports import reports_summary
    from scheduler_runner.tasks.reports import reports_notifications
    from scheduler_runner.tasks.reports import reports_upload
    from scheduler_runner.tasks.reports import reports_scope
    from scheduler_runner.tasks.reports import owner_state_sync
    from scheduler_runner.tasks.reports import failover_orchestration

def test_processor_imports_extracted_modules():
    from scheduler_runner.tasks.reports.reports_processor import main
    from scheduler_runner.tasks.reports.reports_summary import resolve_final_run_status
    from scheduler_runner.tasks.reports.reports_notifications import format_reports_run_notification_message
    from scheduler_runner.tasks.reports.reports_upload import detect_missing_report_dates
    from scheduler_runner.tasks.reports.reports_scope import discover_available_pvz_scope
    from scheduler_runner.tasks.reports.owner_state_sync import sync_owner_failover_state_from_batch_result
    from scheduler_runner.tasks.reports.failover_orchestration import run_failover_coordination_pass
```

### Если затрагивается parser-facing scope

```powershell
.venv\Scripts\python.exe -m pytest scheduler_runner\utils\parser\core\tests\test_ozon_report_parser.py scheduler_runner\tasks\reports\tests\test_reports_processor.py -q
```

### Если затрагивается upload/notification formatting

```powershell
.venv\Scripts\python.exe -m pytest scheduler_runner\tasks\reports\tests\test_reports_processor.py scheduler_runner\utils\uploader\core\tests\test_google_sheets_formula_columns.py -q
```

### Если затрагивается coverage-check

```powershell
.venv\Scripts\python.exe -m pytest scheduler_runner\tasks\reports\tests\test_reports_processor.py -q -k coverage
```

### Если затрагивается resolve_final_run_status или status helpers

```powershell
.venv\Scripts\python.exe -m pytest scheduler_runner\tasks\reports\tests\test_reports_processor.py -q -k run_status
```

### После завершения всей серии

Дополнительно:
- `run_failover_claim_smoke.py`
- `run_failover_policy_smoke.py`
- `run_failover_state_upsert_smoke.py`
- `run_failover_state_owner_success_policy_smoke.py`
- `run_notification_e2e_smoke.py` (если notification formatting изменился)
- `run_kpi_reward_formulas_e2e_smoke.py` (если upload path изменился)

## 10. Риски

Основные риски:
- циклические импорты (особенно между `reports_summary`, `reports_notifications` и `reports_processor`);
- случайное изменение orchestration behavior;
- разъезд logger output;
- слишком большой diff;
- смешивание refactor noise с production debugging;
- потеря функций `extract_batch_failures` / `build_filtered_batch_result` при извлечении failover-части.

Митигация циклических импортов:
- `reports_summary.py` должен содержать **все** `_owner_*` и `_failover_*` хелперы, которые вызывает `resolve_final_run_status`;
- `reports_notifications.py` зависит только от `reports_summary.py` (через dataclasses), не от `reports_processor.py`;
- `reports_processor.py` импортирует извлечённые модули, а не наоборот;
- runtime import-cycle test после каждой фазы (см. раздел 9).

Известные cross-module зависимости (не циклические, но важные):
- `failover_orchestration.py` → `reports_upload.py`: `run_claimed_failover_backfill` вызывает `detect_missing_report_dates`, `run_upload_batch_microservice`
- `reports_notifications.py` → `reports_summary.py`: `format_reports_run_notification_message` принимает `ReportsRunSummary` dataclass
- `failover_orchestration.py` → `failover_state.py`: стандартная зависимость через существующий модуль
- `owner_state_sync.py` → `failover_state.py`: стандартная зависимость через существующий модуль
- `reports_upload.py` → `utils/uploader`: стандартная зависимость через общий uploader
- `reports_scope.py` → `utils/parser`: стандартная зависимость через parser facade

## 11. Рекомендуемое решение на текущий момент

На `2026-04-09` правильный подход такой:

1. продолжить наблюдение production после последних owner-state optimizations;
2. если новые логи не покажут regressions:
3. начать декомпозицию с low-risk extraction;
4. не трогать `failover_policy.py` первым этапом;
5. не дробить `failover_state.py` до завершения первой волны refactor;
6. перед началом извлечения — пройтись по checklist из раздела 12 и убедиться, что все функции учтены;
7. после Phase 1 — обязательно запустить import-cycle test.

## 12. Чеклист перед началом извлечения

Перед стартом Phase 1 убедиться:

- [ ] Все 71 функция (66 + 5 dataclasses) из `reports_processor.py` распределены по целевым модулям
- [ ] `_owner_*` и `_failover_*` хелперы явно привязаны к `reports_summary.py`
- [ ] `extract_batch_failures` и `build_filtered_batch_result`, `is_failover_candidate_scan_retryable_error` привязаны к `failover_orchestration.py`
- [ ] `detect_missing_report_dates`, `detect_missing_report_dates_by_pvz`, `prepare_coverage_filters`, `parse_sheet_date_to_iso`, `create_uploader_logger`, `prepare_connection_params` привязаны к `reports_upload.py`
- [ ] `create_notification_logger`, `_format_failed_dates` привязаны к `reports_notifications.py`
- [ ] `_as_date_list`, `_count_batch_successful_dates`, `_count_batch_failed_dates` привязаны к `reports_summary.py`
- [ ] `build_processor_run_id` остаётся в `reports_processor.py`
- [ ] `resolve_final_run_status` остаётся в `reports_summary.py`, НЕ в `reports_processor.py`
- [ ] `test_module_imports.py` создан и проходит до начала извлечения (импорты через `reports_processor` работают)
- [ ] `FailoverRunSummary` явно документирован как partially-dict-initialized
- [ ] Cross-module зависимости проверены: `failover_orchestration.py` → `reports_upload.py` (использует `detect_missing_report_dates`, `run_upload_batch_microservice`)

## 13. Итог

Декомпозиция `tasks/reports` архитектурно оправдана и уже спроектирована.

Но правильный operational порядок такой:
- сначала подтверждаем стабильность текущего behavior;
- затем делаем отдельную чистую maintenance-фазу;
- и только после этого проводим controlled extraction по описанным выше шагам.

План актуализирован `2026-04-09` с поправками:
- 6 ранее пропущенных coverage/upload функций добавлены в целевые модули;
- 3 ранее пропущенных helper-функции (`_as_date_list`, `_count_batch_*`, `_format_failed_dates`, `is_failover_candidate_scan_retryable_error`) привязаны к правильным модулям;
- `create_uploader_logger`, `create_notification_logger` привязаны к соответствующим модулям;
- `prepare_connection_params` привязан к `reports_upload.py`;
- `build_processor_run_id` явно оставлен в `reports_processor.py`;
- риск циклических импортов устранён явным включением `_owner_*`/`_failover_*` в `reports_summary.py`;
- known cross-module dependencies документированы явно;
- runtime import-cycle test добавлен в тестовую стратегию;
- чеклист перед началом извлечения добавлен как раздел 12;
- анализ структуры каталогов добавлен как раздел 14.

## 14. Структура каталогов

### 14.1. Текущий план: плоская структура

Начальный этап декомпозиции использует **плоскую структуру** — все модули лежат в `scheduler_runner/tasks/reports/`:

```text
scheduler_runner/tasks/reports/
  reports_processor.py              # тонкий orchestrator (оставшееся)
  failover_state.py                 # уже существует
  failover_policy.py                # уже существует
  owner_state_sync.py               # новый
  failover_orchestration.py         # новый
  reports_scope.py                  # новый
  reports_upload.py                 # новый
  reports_notifications.py          # новый
  reports_summary.py                # новый
  report_types.py                   # опционально
  config/                           # уже существует
  tests/                            # уже существует
  docs/                             # уже существует
```

**~13 файлов** + 3 поддиректории = пограничная зона, но всё ещё manageable.

Плюсы:
- Простые импорты: `from scheduler_runner.tasks.reports.reports_summary import ...`
- Нет overhead на `__init__.py` и re-exports
- Всё видно сразу в одном `ls`
- Меньше boilerplate и миграционных рисков
- Существующие тесты и импорты ломаются минимально

Минусы:
- 13 файлов в корне — визуально перегружено
- Навигация требует сканирования имён
- При будущем росте станет хуже

### 14.2. Альтернатива: вложенная логическая структура

```text
scheduler_runner/tasks/reports/
  reports_processor.py                  # тонкий orchestrator
  __init__.py                           # re-exports для convenience
  
  scope/
    __init__.py
    reports_scope.py
    pvz_discovery.py                    # если вырастет
  
  upload/
    __init__.py
    reports_upload.py
    coverage_check.py                   # если вынесется отдельно
  
  notifications/
    __init__.py
    reports_notifications.py
  
  summary/
    __init__.py
    reports_summary.py
    report_types.py                     # если понадобится
  
  failover/
    __init__.py
    failover_state.py                   # переедет сюда
    failover_policy.py                  # переедет сюда
    failover_orchestration.py
    owner_state_sync.py
```

Плюсы:
- Логическая группировка — сразу понятно, где что
- Легко расширять внутри подмодулей
- Навигация: зашёл в `upload/` — там всё про upload
- Чище `ls` корневого каталога

Минусы:
- Импорты длиннее: `from scheduler_runner.tasks.reports.upload.reports_upload import ...`
- Нужно 5+ `__init__.py` с re-exports для удобства
- Существующие импорты (`failover_state`, `failover_policy`) сломаются — нужна миграция
- Больше файлов двигать при refactor
- Усложняет import-cycle test и отладку

### 14.3. Критерии перехода к вложенной структуре

Переход от плоской к вложенной структуре оправдан при выполнении **любого** из условий:

1. Любой модуль вырастет >500 строк после извлечения;
2. Появятся новые подмодули (`pvz_discovery.py`, `coverage_check.py`, `kpi_reward_helpers.py`);
3. Навигация станет ощутимо неудобной (>15 файлов в `reports/`);
4. Разные модули начнут активно развиваться разными людьми/командами;
5. Появится потребность в отдельных `tests/upload/`, `tests/notifications/` и т.д.

### 14.4. Рекомендуемая стратегия

**Начать с плоской структуры.**

Почему:
- 13 файлов — это manageable;
- Flat-структура проще для первого шага — меньше рисков сломать импорты;
- Когда модули стабилизируются и станет ясно, какие из них «растут» — тогда группируем;
- Не стоит усложнять структуру до стабилизации content.

**После завершения всех extraction-фаз:**
1. Оценить реальную картину — сколько строк в каждом модуле;
2. Если какой-то модуль >500 строк — подумать о его внутреннем разделении;
3. Если >15 файлов — перейти к вложенной структуре;
4. Переход делать отдельным maintenance-коммитом, НЕ смешивать с behavioral changes.

### 14.5. Если переход к вложенной структуре всё же понадобится

Рекомендуемый порядок группировки (от простого к сложному):

1. `summary/` — содержит только dataclasses и чистые функции, минимум cross-dependencies;
2. `notifications/` — зависит только от `summary/`, изолирован;
3. `upload/` — зависит от `utils/uploader`, изолирован от failover;
4. `scope/` — зависит от `utils/parser`, изолирован;
5. `failover/` — **самый рискованный**, так как `failover_state.py` и `failover_policy.py` уже импортируются из `reports_processor.py` — переезд сломает все импорты.

Для каждого шага:
- один коммит;
- прогонить `compileall` + профильные тесты;
- обновить импорты в `reports_processor.py` и тестах.
