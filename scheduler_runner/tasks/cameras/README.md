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
  - `local`: `CLEANUP_DIRS` (fallback: `CLEANUP_DIR`).
  - `network`: `CLEANUP_DIR`.
- `config/scripts/openingmonitor_config.py`
  - `SEARCH_DIRS` (fallback: `SEARCH_DIR`).
- `config/scripts/cloudmonitor_config.py`
  - `CHECK_DIR=CAMERAS_NETWORK`.

## Запуск скриптов вручную

Рекомендуется запуск через модульный формат:

```bash
.venv\Scripts\python.exe -m scheduler_runner.tasks.cameras.VideoMonitorScript --check_type local --detailed_logs
.venv\Scripts\python.exe -m scheduler_runner.tasks.cameras.VideoMonitorScript --check_type network
.venv\Scripts\python.exe -m scheduler_runner.tasks.cameras.CopyScript --max_age_days 3 --conflict_mode skip
.venv\Scripts\python.exe -m scheduler_runner.tasks.cameras.CleanupScript --input_dir_scenario local
.venv\Scripts\python.exe -m scheduler_runner.tasks.cameras.CleanupScript --input_dir_scenario network
.venv\Scripts\python.exe -m scheduler_runner.tasks.cameras.CloudMonitorScript --retries 4 --delay 10
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