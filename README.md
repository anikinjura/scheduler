# Планировщик задач

Этот проект представляет собой модульный планировщик задач, предназначенный для автоматического выполнения различных скриптов по расписанию. Ядро системы динамически обнаруживает, загружает и выполняет задачи, определенные в отдельных "доменах" (например, `cameras`, `system`).

---

## Архитектура

Планировщик построен на модульной архитектуре, где ядро (`scheduler_runner`) отвечает за общую логику, а конкретные задачи инкапсулированы в своих директориях (`scheduler_runner/tasks/<domain>`).

1.  **Точка входа (`scheduler_runner/runner.py`)**:
    -   Принимает аргументы командной строки: `--user`, `--task`, `--detailed_logs`.
    -   Фильтрует задачи по пользователю и, опционально, по имени задачи.
    -   Последовательно выполняет отфильтрованные задачи в соответствии с их расписанием.

2.  **Сборка расписания (Дискавери)**:
    -   **Главный конфигуратор (`scheduler_runner/schedule_config.py`)**: Сканирует директорию `scheduler_runner/tasks/` на наличие доменов задач.
    -   **Конфигуратор домена (`tasks/<domain>/config/<domain>_schedule.py`)**: В каждом домене ищется файл конфигурации расписания. Его задача — собрать все расписания для этого домена.
    -   **Утилита сбора (`scheduler_runner/utils/schedule_utils.py`)**: Функция `collect_task_schedule` автоматически находит и импортирует все файлы `*_config.py` из папки `scripts` домена.
    -   **Конфигуратор скрипта (`tasks/<domain>/config/scripts/*_config.py`)**: В этих файлах определяется переменная `SCHEDULE` — список, содержащий один или несколько словарей с параметрами для запуска конкретного скрипта.
    -   Все найденные расписания объединяются в единый список `SCHEDULE`, который используется ядром.

3.  **Выполнение задачи (`scheduler_runner/utils/subprocess.py`)**:
    -   Каждая задача запускается в отдельном подпроцессе для изоляции и стабильности.
    -   Используются lock-файлы (`.lock`) для предотвращения одновременного запуска одной и той же задачи.
    -   Ведется проверка на повторный запуск в рамках одного "окна" времени (например, для ежечасных задач, чтобы они не запускались чаще раза в час).

---

## Структура проекта

```
/
├── scheduler_runner/           # Ядро планировщика
│   ├── runner.py               # Главный исполняемый файл
│   ├── schedule_config.py      # Логика сбора и валидации всех расписаний
│   ├── tasks/                  # Директория с задачами
│   │   ├── cameras/            # Пример домена задач "cameras"
│   │   │   ├── CopyScript.py
│   │   │   └── config/
│   │   │       ├── cameras_schedule.py
│   │   │       └── scripts/
│   │   │           └── copy_config.py
│   │   └── system/             # Пример домена задач "system"
│   └── utils/                  # Вспомогательные утилиты (логирование, ФС, и т.д.)
│
├── config/                     # Глобальная конфигурация
│   └── base_config.py          # Базовые пути и переменные
│
├── logs/                       # Директория для лог-файлов
├── pvz_config.ini              # Конфигурация объекта (ID и режим работы)
├── requirements.txt            # Зависимости проекта
└── README.md                   # Эта документация
```

---

## Конфигурация

### `pvz_config.ini`

Этот файл должен находиться в `C:\tools\pvz_config.ini` и содержать:

-   `PVZ_ID`: Уникальный идентификатор объекта (например, `340`).
-   `ENV_MODE`: Режим окружения (`production` или `test`). Влияет на выбор путей и параметров в задачах.

### Конфигурация задачи

Каждая задача в расписании представляет собой словарь. **Важно**: переменная `SCHEDULE` в файлах `*_config.py` всегда должна быть **списком**, даже если в нем всего одна задача.

**Пример из `scheduler_runner/tasks/cameras/config/scripts/copy_config.py`:**
```python
# scheduler_runner/tasks/cameras/config/scripts/copy_config.py

MODULE_PATH = "scheduler_runner.tasks.cameras.CopyScript"

SCRIPT_CONFIG = {
    "INPUT_DIR": "...",
    "OUTPUT_DIR": "...",
    "USER": "operator",
    "TASK_NAME": "CopyScript",
}

# SCHEDULE должен быть списком
SCHEDULE = [
    {
        "name": SCRIPT_CONFIG["TASK_NAME"],
        "module": MODULE_PATH,
        "args": ["--shutdown", "30"],
        "schedule": "daily",
        "time": "21:10",
        "user": SCRIPT_CONFIG["USER"],
        "no_timeout_control": True,
    }
]
```

**Ключи конфигурации:**

-   `name` (str): Уникальное имя задачи.
-   `user` (str): Имя пользователя, от имени которого запускается задача.
-   `module` (str): Путь к Python-модулю для запуска (например, `scheduler_runner.tasks.cameras.CopyScript`).
-   `schedule` (str): Тип расписания (`daily`, `hourly`, `interval`).
-   `time` (str, optional): Время запуска для `daily` расписания (формат `HH:MM`).
-   `interval` (int, optional): Интервал в секундах для `interval`.
-   `args` (list): Список аргументов командной строки для скрипта.
-   `env` (dict, optional): Словарь переменных окружения для подпроцесса.
-   `timeout` (int, optional): Таймаут выполнения в секундах.
-   `no_timeout_control` (bool, optional): Если `True`, задача запускается в режиме "fire-and-forget" без контроля времени выполнения. Полезно для долгих процессов. По умолчанию `False`.

---

## Использование

### Запуск планировщика

Планировщик запускается из командной строки. Рекомендуется использовать `pythonw.exe` для запуска в фоновом режиме без окна консоли.

```bash
# Запустить все задачи для пользователя 'operator' по расписанию
pythonw -m scheduler_runner.runner --user operator

# Принудительно запустить задачу 'CopyScript' для пользователя 'operator'
pythonw -m scheduler_runner.runner --user operator --task CopyScript

# Запустить задачи с подробным логированием (уровень DEBUG)
pythonw -m scheduler_runner.runner --user operator --detailed_logs
```

### Аргументы командной строки

-   `--user <имя>`: **(Обязательный)** Указывает, для какого пользователя фильтровать задачи.
-   `--task <имя>`: (Опциональный) Имя конкретной задачи для принудительного запуска, игнорируя расписание.
-   `--detailed_logs`: (Опциональный) Включает запись `DEBUG` логов в отдельный файл.

---

## Добавление новой задачи

1.  **Создать директорию домена**:
    -   В `scheduler_runner/tasks/` создайте новую папку, например, `my_new_task`.

2.  **Написать скрипт(ы)**:
    -   Внутри `my_new_task/` разместите один или несколько Python-скриптов (например, `MyScript.py`).

3.  **Создать конфигурацию скрипта**:
    -   Создайте структуру папок `my_new_task/config/scripts/`.
    -   Внутри создайте файл `myscript_config.py`.
    -   В этом файле определите переменную `SCHEDULE` — **список** словарей, описывающих задачи.

    ```python
    # my_new_task/config/scripts/myscript_config.py
    MODULE_PATH = "scheduler_runner.tasks.my_new_task.MyScript"
    SCHEDULE = [
        {
            "name": "MyFirstTask",
            "module": MODULE_PATH,
            "args": ["--mode", "fast"],
            "schedule": "hourly",
            "user": "operator"
        }
    ]
    ```

4.  **Создать конфигурацию расписания домена**:
    -   В `my_new_task/config/` создайте файл `my_new_task_schedule.py`.
    -   В нем используйте утилиту `collect_task_schedule` для автоматического сбора всех расписаний из папки `scripts/`:
        ```python
        # my_new_task/config/my_new_task_schedule.py
        from scheduler_runner.utils.schedule_utils import collect_task_schedule
        
        TASK_SCHEDULE = collect_task_schedule("my_new_task")
        ```

После этих шагов ядро планировщика автоматически обнаружит и подключит новую задачу.

---

## Логирование

-   Логи всех задач и ядра хранятся в директории `logs/`.
-   Структура логов: `logs/<user>/<task_name>/<YYYY-MM-DD>.log`.
-   При использовании флага `--detailed_logs` создается дополнительный файл `<YYYY-MM-DD>_detailed.log` с отладочной информацией.
-   Старые логи автоматически удаляются.