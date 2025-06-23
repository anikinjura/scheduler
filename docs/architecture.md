# Архитектура проекта

Этот документ описывает общую архитектуру приложения для централизованного запуска задач под разными системными пользователями посредством Windows Task Scheduler.

## 1. Дерево каталогов

```text
scheduler/                                # Корень проекта 
├── config/                               # Общие конфигурационные файлы
│   └── base_config.py                    # PVZ_ID, ENV_MODE, пути и ENV-переменные
│
├── scheduler_runner/                     # Движок запуска задач
│   ├── runner.py                         # Точка входа: парсит --user, фильтрует и запускает задачи
│   ├── schedule_config.py                # Описание SCHEDULE: скрипты, аргументы, расписания, пользователи
│   ├── utils/                            # Вспомогательные модули
│   │   ├── logging.py                    # configure_logger: настраивает логи в logs/{user}/{domain_name}
│   │   ├── timing.py                     # should_run_now: проверка «hourly»/«daily + время»
│   │   ├── subprocess.py                 # run_subprocess: запуск скриптов с прокидкой ENV
│   │   └── notifications.py              # Telegram-сервис для отправки уведомлений
│   └── tasks/                            # Доменные подпакеты задач
│       ├── cameras/                      # Задачи видеонаблюдения
│       │   ├── CleanupScript.py
│       │   ├── CopyScript.py
│       │   ├── CloudMonitorScript.py
│       │   ├── VideoMonitorScript.py
│       │   └── config/
│       │       └── cameras_config.py     # TASK_SCHEDULE или SCHEDULE: описание расписания обхода камер
│       ├── system_updates/               # Задачи обновления ОС
│       │   ├── UpdateOS.py
│       │   └── config/
│       │       └── system_updates_config.py
│       └── common/                       # Общие задачи/утилиты (при необходимости)
│
├── logs/                                 # Логи: logs/{user}/{TaskDomain}_{TaskName}/YYYY-MM-DD.log
├── tests/                                # Юнит- и интеграционные тесты
├── docs/                                 # архитектура, инструкция по настройке Task Scheduler
│   └── architecture.md
├── pvz_config.ini                        # INI-файл с PVZ_ID и ENV_MODE (production/test)
├── requirements.txt                      # зависимые пакеты
└── README.md                             # обзор проекта и инструкции по развёртыванию
```

## 2. Компоненты

### 2.1 config/
- **base_config.py** —
  - Читает `pvz_config.ini` для получения `PVZ_ID` и `ENV_MODE`.
  - Формирует `PATH_CONFIG` с путями к локальным и сетевым директориям, логам и Telegram-переменным.
- **cameras_config.py** —
  - Содержит словарь `CAMERAS_BY_PVZ` с описанием камер для каждого объекта.

### 2.2 scheduler_runner/
- **runner.py** —
  - Принимает аргумент `--user` (operator, camera, admin).
  - Загружает `schedule_config.SCHEDULE` и фильтрует задачи для данного пользователя.
  - С помощью `utils.should_run_now` проверяет, пора ли запускать.
  - Вызывает `utils.run_subprocess` для запуска соответствующих скриптов.
  - Логирует через `utils.configure_logger` в папку `logs/{user}/{TaskDomain}_{TaskName}`.

- **schedule_config.py** —
  - Определяет структуру `SCHEDULE`:
    ```python
    SCHEDULE = {
      "TaskName": {
          "script": "FileName.py",
          "args": "--flags",
          "schedule": "hourly"|"daily",
          "time": "HH:MM" (для daily),
          "user": "operator"|"camera"|"admin"
      },
      # …
    }
    ```

- **utils/** —
  - **logging.py** — настраивает `logging` с раздельными файлами по задачам и пользователям.
  - **timing.py** — содержит `should_run_now(schedule, time)`, возвращает `True` если текущий момент соответствует расписанию.
  - **subprocess.py** — реализует `run_subprocess(script, args, env)`, запускает Python-модуль и обрабатывает вывод.
  - **notifications.py** — обёртка для отправки Telegram-уведомлений (используется из любых задач).

- **tasks/** —
  - **cameras/** — пакет задач по видеозаписям (`CleanupScript.py`, `CopyScript.py`, и т. п.).
  - **cameras/config/cameras_config.py** — локальная копия общего конфига камер.
  - **system_updates/** — содержит `UpdateOS.py` для обновления ОС.
  - **common/** — может хранить переиспользуемые скрипты и утилиты.

## 3. Логи

Структура директории `logs/`:
```
logs/
└── operator/
    └── cameras_CleanupScript/2025-06-11.log
└── camera/
    └── ...
└── admin/
    └── system_updates_UpdateOS/2025-06-11.log
```

## 4. Развёртывание

В Windows Task Scheduler создаются три задачи:
- **Scheduler – Operator**: `python "…\runner.py" --user operator` (раз в час)
- **Scheduler – camera**: `… --user camera`
- **Scheduler – Administrator**: `… --user admin`

Настройки задач:
- Run whether user is logged on or not
- Run with highest privileges
- Триггер: повторять каждую 1 час; для daily можно задать конкретное время.

---