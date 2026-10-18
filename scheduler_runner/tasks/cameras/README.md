# cameras - Мониторинг и обслуживание видеоархива

## Назначение

Поддомен `scheduler_runner/tasks/cameras` автоматизирует:
- контроль наличия свежих записей с камер;
- копирование архива с локальных дисков в целевой архив (сетевой/съемный диск);
- очистку устаревших файлов;
- контроль доступности целевого архива;
- контроль времени открытия объекта по первому файлу.

## Актуальная логика путей

Базовая конфигурация находится в `config/cameras_paths.py`.
Политика сроков хранения находится в `config/cameras_retention.py`.

Ключевые поля:
- `CAMERAS_LOCAL` - базовый локальный путь (legacy/fallback).
- `LOCAL_ROOTS` - словарь локальных корней для объектов с несколькими дисками.
- `CAMERAS_NETWORK` - целевой архив (в production может быть сетевой диск, для отдельных объектов - съемный).
- `TELEGRAM_TOKEN`, `TELEGRAM_CHAT_ID`.

### Индивидуальные пути камер

В `config/cameras_list.py` камера может иметь `root_key`.

Пример:
```python
{"id": "xiaomi_001", "uid": "78df72028ea2", "root_key": "local_2"}
```

`root_key` должен совпадать с ключом в `LOCAL_ROOTS`.

Если `root_key` не задан, используется fallback на `CAMERAS_LOCAL`.

## Конфиги скриптов

- `config/scripts/videomonitor_config.py`
  - `local`: использует `CHECK_DIR` + `LOCAL_ROOTS`.
  - `network`: использует `CHECK_DIR=CAMERAS_NETWORK`.
- `config/scripts/copy_config.py`
  - `INPUT_DIRS` (fallback: `INPUT_DIR`), `OUTPUT_DIR`.
- `config/scripts/cleanup_config.py`
  - `local`: `CLEANUP_DIRS` (fallback: `CLEANUP_DIR`), `MAX_AGE_DAYS` из `cameras_retention.py`.
  - `network`: `CLEANUP_DIR`, `MAX_AGE_DAYS` из `cameras_retention.py`; `DATED_FOLDERS`, `TIME_BUDGET_SECONDS`,
    `DIR_CHECK_TIMEOUT_SECONDS` (раздел «Очистка облака»).
- `config/scripts/openingmonitor_config.py`
  - `SEARCH_DIRS` (fallback: `SEARCH_DIR`).
- `config/scripts/cloudmonitor_config.py`
  - `CHECK_DIR=CAMERAS_NETWORK`, `PROBE_TIMEOUT_SECONDS`, `REMIND_HOURS`, `STATE_FILE`.

## Зависание целевого архива («Диск-О:»)

Облачный диск `O:` может не пропасть, а зависнуть: любое обращение к нему (даже проверка существования папки) не
возвращается. Так было на ЧЕБОКСАРЫ_144 05–10.10.2026: задачи молча убивал таймаут runner (60 с) раньше, чем они
отправляли уведомление; CopyScript падал, компьютер не выключался, а записи 5 дней не уходили в облако — и никто не знал.

Теперь все обращения к целевому архиву ограничены по времени (`call_with_timeout` в `utils/filesystem.py`):
- `CloudMonitorScript` (каждый час 09–21): попытка — не дольше `PROBE_TIMEOUT_SECONDS` (15 с), зависшее хранилище не
  повторяет. Уведомление — при первом сбое, затем раз в `REMIND_HOURS` (3 ч), и «снова доступно» после восстановления;
  состояние — `reports/cloud_monitor_state.json` (вне git).
- `VideoMonitorScript`: вся проверка — не дольше `SCAN_TIMEOUT_SECONDS` (40 с), иначе уведомление «хранилище не отвечает».
- `CopyScript`: проверка `OUTPUT_DIR` — не дольше `DEST_CHECK_TIMEOUT_SECONDS` (60 с). Любой сбой копирования, в том
  числе ошибки по отдельным файлам (облако отключилось посреди копирования — 144, 10.10.2026: 12139 из 14715 файлов), —
  уведомление, код выхода 1 (задача не считается успешной) и, при `SHUTDOWN_ON_FAILURE`, выключение компьютера как обычно.
- Страховка от всего остального — тревога `COPY_STALE` мониторинга (`tasks/system`, `HealthMonitorScript`): копирование не
  проходило успешно дольше 36 ч.

Что делать по уведомлению: перезапустить «Диск-О:» под «Оператором» (или перезагрузить компьютер) и проверить, что
диск `O:` открывается. CopyScript копирует только последние `MAX_AGE_DAYS` (3) дня — более старые записи за время
простоя докопировать вручную (`CopyScript --max_age_days N`, без `--shutdown`).

## Очистка облака (`CleanupScript_network`)

Раньше очистка обходила все файлы `O:\cameras\<ПВЗ>` и запрашивала дату каждого по сети: за 120 дней хранения это
сотни тысяч запросов, задачу каждый день убивал таймаут 60 с, и облако на 143/144 фактически не очищалось (10.2026).

Теперь (`DATED_FOLDERS: True`) удаляются целиком папки записей с датой в имени — UNV `unv_camera\<uid>\YYYYMMDD`,
Xiaomi `xiaomi_camera_videos\<uid>\YYYYMMDDHH` (`remove_old_dated_folders` в `utils/filesystem.py`):
- листаются только каталоги до уровня даты, внутрь папок с датой — не заходим;
- папка удаляется, если весь ее период (сутки/час) старше срока хранения;
- самые старые — первыми; не дольше `TIME_BUDGET_SECONDS` (480 с) за запуск, остаток — при следующем запуске
  (в логе «Время очистки … исчерпано»); таймаут задачи — 600 с;
- проверка доступа к облаку — не дольше `DIR_CHECK_TIMEOUT_SECONDS` (60 с), зависшее облако — выход с ошибкой
  (уведомит CloudMonitor);
- итог — строка «Сводка (…): удалено папок с датой=…, осталось старых=…, самая старая оставшаяся дата=…».

Файлы вне папок с датой не удаляются. Очистка локальных дисков (`CleanupScript_local`) — прежняя, по файлам.

## Запуск скриптов вручную

Рекомендуется запуск через модульный формат:

```bash
.venv\Scripts\python.exe -m scheduler_runner.tasks.cameras.VideoMonitorScript --check_type local --detailed_logs
.venv\Scripts\python.exe -m scheduler_runner.tasks.cameras.VideoMonitorScript --check_type network
.venv\Scripts\python.exe -m scheduler_runner.tasks.cameras.CopyScript --max_age_days 3 --conflict_mode skip
.venv\Scripts\python.exe -m scheduler_runner.tasks.cameras.CleanupScript --input_dir_scenario local
.venv\Scripts\python.exe -m scheduler_runner.tasks.cameras.CleanupScript --input_dir_scenario network
.venv\Scripts\python.exe -m scheduler_runner.tasks.cameras.CloudMonitorScript --retries 3 --delay 5
.venv\Scripts\python.exe -m scheduler_runner.tasks.cameras.OpeningMonitorScript --detailed_logs
```

## Тесты

Юнит-тесты:
```bash
.venv\Scripts\python.exe -m pytest scheduler_runner/tasks/cameras/tests -q -p no:tmpdir -p no:cacheprovider
```

Генерация синтетики:
```bash
.venv\Scripts\python.exe tests\TestEnvironment\run_camera_structure_generator.py --start-date 20260307 --end-date 20260308
```

Для `ENV_MODE=test` генератор использует тестовые пути из `cameras_paths.py`, включая `LOCAL_ROOTS` и `CAMERAS_NETWORK`.

### Индивидуальные пороги очистки

В `config/cameras_retention.py`:
- `RETENTION_DEFAULTS_DAYS` задает дефолты (`local=8`, `network=120`);
- `RETENTION_OVERRIDES_DAYS` позволяет задать per-PVZ значения;
- `cleanup_config.py` автоматически берет пороги через `get_retention_days(PVZ_ID, scenario)`.

## Логи

Логи пишутся в `logs/<user>/<task>/`.

Ключевые задачи:
- `VideoMonitorScript_local`
- `VideoMonitorScript_network`
- `CopyScript`
- `CleanupScript_local`
- `CleanupScript_network`
- `CloudMonitorScript`
- `OpeningMonitorScript`
