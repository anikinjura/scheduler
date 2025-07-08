# cameras — Задача мониторинга и обслуживания видеонаблюдения

## Описание

Задача **cameras** предназначена для автоматизации процессов мониторинга, копирования, очистки и контроля доступности видеозаписей с камер видеонаблюдения на объектах. Она интегрируется с ядром планировщика (`scheduler_runner/runner.py`) и включает четыре основных подпроцесса:

- **`VideoMonitorScript`** — мониторинг наличия свежих видеозаписей с камер (локально или в облаке).
  - Проверяет наличие записей за последние N часов (настраивается в конфиге).
  - Отправляет уведомления в Telegram при отсутствии записей.

- **`CopyScript`** — копирование новых файлов из локального архива в облачное хранилище.
  - Копирует файлы не старше заданного возраста (в днях).
  - Поддерживает обработку конфликтов имен файлов (пропуск или переименование).

- **`CleanupScript`** — удаление устаревших файлов и пустых папок.
  - Работает с локальными и облачными директориями.
  - Удаляет файлы старше заданного порога возраста.

- **`CloudMonitorScript`** — контроль доступности облачного хранилища.
  - Проверяет возможность записи в облачную директорию.
  - Уведомляет через Telegram при недоступности.

- **`OpeningMonitorScript`** — контроль времени начала работы объекта.
  - Ищет самый ранний видеофайл за текущий день в заданном временном интервале (например, с 8 до 10 утра).
  - Отправляет в Telegram сообщение о времени первого найденного файла или об их отсутствии.
  - Учитывает разные форматы имен файлов для определения времени (камеры UNV, Xiaomi).

Задача разработана для гибкой настройки через конфигурационные файлы и поддерживает централизованное логирование и юнит-тестирование.

---

## Структура проекта

```
tasks/cameras/
├── VideoMonitorScript.py        # Скрипт мониторинга записей
├── CopyScript.py                # Скрипт копирования файлов
├── CleanupScript.py             # Скрипт очистки директорий
├── CloudMonitorScript.py        # Скрипт мониторинга облака
├── OpeningMonitorScript.py      # Скрипт контроля начала работы
├── config/                      # Конфигурационные файлы
│   ├── cameras_list.py          # Справочник камер по объектам (PVZ_ID)
│   ├── cameras_paths.py         # Пути к директориям и параметры Telegram
│   ├── cameras_schedule.py      # Общее расписание задачи cameras
│   └── scripts/                 # Конфиги отдельных скриптов
│       ├── videomonitor_config.py
│       ├── copy_config.py
│       ├── cleanup_config.py
│       ├── cloudmonitor_config.py
│       └── openingmonitor_config.py
└── tests/                       # Юнит-тесты
    ├── test_videomonitor_script.py
    ├── test_copy_script.py
    ├── test_cleanup_script.py
    ├── test_cloud_monitor_script.py
    └── test_opening_monitor_script.py
```

---

## Основные параметры

- **Пути и Telegram**: Задаются централизованно в `config/cameras_paths.py` через переменные среды, зависящие от режима (`production` или `test`):
  - `CAMERAS_LOCAL`: Локальная директория с видеоархивом.
  - `CAMERAS_NETWORK`: Сетевая (облачная) директория.
  - `TELEGRAM_TOKEN`, `TELEGRAM_CHAT_ID`: Токен и чат для уведомлений.

- **Список камер**: Определяется в `config/cameras_list.py` как словарь `CAMERAS_BY_PVZ` с камерами по зонам для каждого объекта (PVZ_ID).

- **Конфигурация скриптов**: Хранится в `config/scripts/*_config.py`:
  - Параметры запуска (например, `MAX_AGE_DAYS`, `CHECK_DIR`).
  - Расписания задач (`SCHEDULE`) для интеграции с планировщиком.

- **Расписание**: Собирается в `config/cameras_schedule.py` из всех скриптовых конфигов через `collect_task_schedule`.

---

## Конфигурационные файлы

### `cameras_paths.py`
- Определяет пути и параметры Telegram в зависимости от `ENV_MODE`:
  - Production: `D:/camera` (локально), `O:/cameras/<PVZ_ID>` (сетевая).
  - Test: `C:/TestEnvironment/D_camera` (локально), `C:/TestEnvironment/O_cameras/<PVZ_ID>` (сетевая).
  - Использует переменные среды: `TELEGRAM_TOKEN_PROD`, `TELEGRAM_CHAT_ID_PROD` (или `_TEST` для тестовой среды).

### `cameras_list.py`
- Содержит словарь `CAMERAS_BY_PVZ`:
  ```python
  CAMERAS_BY_PVZ = {
      10: {
          "склад": [{"id": "unv_001", "uid": "sklad", "локация": "над стелажами"}, ...],
          "клиентская зона": [...]
      },
      ...
  }
  ```
- Используется для идентификации камер и проверки их записей.

### `cameras_schedule.py`
- Экспортирует `TASK_SCHEDULE` — список задач для ядра планировщика, собираемый из всех `SCHEDULE` в `scripts/*_config.py`.

### Скриптовые конфиги (`scripts/*_config.py`)
- **`videomonitor_config.py`**:
  - Локальная проверка: `MAX_LOOKBACK_HOURS=2`, `CHECK_DIR=CAMERAS_LOCAL`.
  - Сетевая проверка: `MAX_LOOKBACK_HOURS=24`, `CHECK_DIR=CAMERAS_NETWORK`.
- **`copy_config.py`**:
  - `INPUT_DIR=CAMERAS_LOCAL`, `OUTPUT_DIR=CAMERAS_NETWORK`, `MAX_AGE_DAYS=3`, `ON_CONFLICT="skip"`.
- **`cleanup_config.py`**:
  - Локально: `CLEANUP_DIR=CAMERAS_LOCAL`, `MAX_AGE_DAYS=8`.
  - Сетевая: `CLEANUP_DIR=CAMERAS_NETWORK`, `MAX_AGE_DAYS=120`.
- **`cloudmonitor_config.py`**:
  - `CHECK_DIR=CAMERAS_NETWORK`, `RETRIES=4`, `DELAY=10`.
- **`openingmonitor_config.py`**:
  - `SEARCH_DIR=CAMERAS_LOCAL`: Директория для поиска файлов.
  - `START_TIME="08:00:00"`: Начало временного окна для поиска.
  - `END_TIME="10:00:00"`: Конец временного окна.
  - `timeout: 120`: Индивидуальный таймаут для задачи (в секундах), так как сканирование может быть длительным.

---

## Запуск подпроцессов вручную

Каждый скрипт можно запустить вручную из корня проекта с параметрами через Python. Примеры:

### Мониторинг видеозаписей
```bash
python -m scheduler_runner.tasks.cameras.VideoMonitorScript --check_type local --min_files 2 --detailed_logs
```
- `--check_type`: `local` или `network`.
- `--min_files`: Минимальное количество файлов для успешной проверки (по умолчанию 1).

### Копирование файлов
```bash
python -m scheduler_runner.tasks.cameras.CopyScript --max_age_days 3 --conflict_mode skip --detailed_logs --shutdown 10
```
- `--max_age_days`: Максимальный возраст файлов для копирования.
- `--conflict_mode`: `skip` (пропустить) или `rename` (переименовать).
- `--shutdown`: Выключить компьютер после завершения. Можно указать задержку в минутах (например, `--shutdown 10`).

### Очистка директорий
```bash
python -m scheduler_runner.tasks.cameras.CleanupScript --input_dir_scenario local --max_age_days 7 --detailed_logs
```
- `--input_dir_scenario`: `local` или `network`.
- `--max_age_days`: Порог возраста файлов для удаления.

### Мониторинг облака
```bash
python -m scheduler_runner.tasks.cameras.CloudMonitorScript --retries 5 --delay 15 --detailed_logs
```
- `--retries`: Количество попыток проверки.
- `--delay`: Задержка между попытками (в секундах).

### Контроль времени открытия
```bash
python -m scheduler_runner.tasks.cameras.OpeningMonitorScript --detailed_logs
```
- Скрипт не принимает внешних аргументов для настройки времени или путей, так как все параметры (включая временной интервал) жестко заданы в `openingmonitor_config.py`.
- Флаг `--detailed_logs` позволяет включить расширенное логирование.

---

## Переменные среды

Для работы уведомлений через Telegram необходимо задать переменные среды:

- **Production**:
  - `TELEGRAM_TOKEN_PROD`
  - `TELEGRAM_CHAT_ID_PROD`
- **Test**:
  - `TELEGRAM_TOKEN_TEST`
  - `TELEGRAM_CHAT_ID_TEST`

Переменные подтягиваются в `cameras_paths.py` в зависимости от `ENV_MODE`.

---

## Логирование

- Все скрипты используют централизованное логирование через `scheduler_runner.utils.logging`.
- Логи записываются в `logs/<user>/<task>/`.
- Флаг `--detailed_logs` включает детализированные логи (уровень DEBUG).

---

## Тесты

Юнит-тесты находятся в `tasks/cameras/tests/` и покрывают основные функции скриптов:
- `test_videomonitor_script.py`: Проверка наличия записей, обработка ошибок.
- `test_copy_script.py`: Тестирование копирования с конфликтами (`skip`, `rename`).
- `test_cleanup_script.py`: Удаление старых файлов и пустых папок.
- `test_cloud_monitor_script.py`: Проверка доступности облака и уведомлений.

### Запуск тестов
```bash
pytest scheduler_runner/tasks/cameras/tests/
```

## Автоматизация

Задачи предназначены для запуска через ядро планировщика:
```bash
pythonw -m scheduler_runner.runner --user operator
```
- Расписание задается в `config/scripts/*_config.py` и собирается в `cameras_schedule.py`.
- Примеры расписаний:
  - `VideoMonitorScript_local`: Ежечасно.
  - `CopyScript`: Ежедневно в 21:10.
  - `CleanupScript_network`: Ежедневно в 20:55.

Для принудительного запуска конкретной задачи:
```bash
pythonw -m scheduler_runner.runner --user operator --task CopyScript
```

---

## Примечания

- Код поддерживает версионность (например, `CopyScript` — `1.2.0`, `CleanupScript` — `0.0.2`).
- Автор всех компонентов: `anikinjura`.