# system — Задача системного обслуживания

## Описание

Задача **system** предназначена для выполнения системных операций, таких как автоматическое обновление проекта из Git-репозитория. Она интегрируется с ядром планировщика (`scheduler_runner/runner.py`) и включает в себя один основной подпроцесс:

- **`UpdaterScript`** — автоматическое обновление проекта из Git-репозитория.
  - Проверяет наличие обновлений в удаленном репозитории.
  - Выполняет `git pull` для загрузки изменений.
  - Поддерживает "dry-run" режим для проверки без применения обновлений.
  - Может перезапускать себя в случае собственного обновления.

Задача разработана для гибкой настройки через конфигурационные файлы и поддерживает централизованное логирование.

---

## Структура проекта

```
tasks/system/
├── UpdaterScript.py             # Скрипт обновления проекта
├── config/                      # Конфигурационные файлы
│   ├── system_schedule.py       # Общее расписание задачи system
│   └── scripts/                 # Конфиги отдельных скриптов
│       └── updater_config.py
└── tests/                       # Юнит-тесты
    └── test_updater_script.py
```

---

## Основные параметры

- **Параметры Git**: Задаются в `config/scripts/updater_config.py`:
  - `BRANCH`: Ветка для проверки обновлений.
  - `REPO_DIR`: Локальный путь к репозиторию.
  - `REPO_URL`: URL удаленного репозитория.

- **Расписание**: Собирается в `config/system_schedule.py` из всех скриптовых конфигов через `collect_task_schedule`.

---

## Конфигурационные файлы

### `updater_config.py`
- Определяет параметры для `UpdaterScript`:
  - `BRANCH`: Ветка для обновления (наприм��р, "main").
  - `REPO_DIR`: Путь к локальному репозиторию.
  - `REPO_URL`: URL удаленного репозитория (например, "https://github.com/anikinjura/scheduler.git").
- Содержит расписание (`SCHEDULE`) для интеграции с планировщиком.

### `system_schedule.py`
- Экспортирует `TASK_SCHEDULE` — список задач для ядра планировщика, собираемый из всех `SCHEDULE` в `scripts/*_config.py`.

---

## Запуск подпроцессов вручную

Скрипт можно запустить вручную из корня проекта с параметрами через Python.

### Обновление проекта
```bash
python -m scheduler_runner.tasks.system.UpdaterScript --branch main --detailed_logs
```
- `--branch`: Ветка для обновления.
- `--dry-run`: Только проверить наличие обновлений, не выполнять `git pull`.
- `--detailed_logs`: Включить детализированные логи.

---

## Логирование

- Скрипт использует централизованное логирование через `scheduler_runner.utils.logging`.
- Логи записываются в `logs/<user>/<task>/`.
- Флаг `--detailed_logs` включает детализированные логи (уровень DEBUG).

---

## Тесты

Юнит-тесты находятся в `tasks/system/tests/` и покрывают основные функции скрипта:
- `test_updater_script.py`: Проверка логики обновления.

### Запуск тестов
```bash
pytest scheduler_runner/tasks/system/tests/
```

---

## Автоматизация

Задача предназначена для запуска через ядро планировщика:
```bash
pythonw -m scheduler_runner.runner --user system
```
- Расписание задается в `config/scripts/updater_config.py` и собирается в `system_schedule.py`.

Для принудительного запуска задачи:
```bash
pythonw -m scheduler_runner.runner --user system --task UpdaterScript
```
