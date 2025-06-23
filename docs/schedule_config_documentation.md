# Документация модуля `schedule_config.py`

## Обзор

Модуль `schedule_config.py` отвечает за автоматическое обнаружение, загрузку и валидацию конфигураций задач из доменных подпакетов в рамках системы планирования задач. Он предоставляет централизованное управление расписанием выполнения задач и API для работы с конфигурациями, обеспечивая их корректность и согласованность.

---

## Архитектура

Модуль организован следующим образом:

```
schedule_config.py
├── Обнаружение конфигураций
│   ├── _discover_task_configs()    # Сканирование директорий
│   └── _extract_task_configs()     # Извлечение из модулей
├── Валидация
│   ├── _validate_schedule_config() # Общая валидация
│   ├── _validate_required_fields() # Проверка полей
│   ├── _validate_schedule_format() # Проверка расписания
│   └── _validate_time_format()     # Проверка времени
├── Управление окружением
│   └── get_task_env()             # Переменные окружения
└── API функции
    ├── reload_schedule()          # Перезагрузка
    ├── get_tasks_by_user()        # Фильтр по пользователю
    ├── get_task_by_name()         # Поиск по имени
    └── get_schedule_summary()     # Статистика
```

---

## Структура конфигурации задачи

Каждая задача представлена словарем с обязательными и опциональными полями. Пример конфигурации:

```python
task_config = {
    # Обязательные поля
    'name': 'backup_database',      # Уникальное имя задачи
    'user': 'admin',               # Системный пользователь
    'schedule': 'daily',           # Тип расписания (daily, interval, hourly)

    # Поля выполнения
    'module': 'backup_script',     # Модуль для запуска
    'script': 'legacy_script.py',  # Альтернативное имя скрипта
    'args': ['--full', '--compress'], # Аргументы командной строки

    # Параметры расписания
    'time': '02:30',              # Время для daily (HH:MM)
    'interval': 3600,             # Интервал для interval (секунды)

    # Параметры выполнения
    'timeout': 1800,              # Таймаут в секундах
    'working_dir': '/opt/scripts', # Рабочая директория

    # Переменные окружения
    'env': {
        'DB_HOST': 'localhost',
        'DB_PORT': 5432,
        'BACKUP_PATH': '/backup'
    },

    # Дополнительные параметры
    'description': 'Ежедневное резервное копирование БД',
    'enabled': True,              # Включена ли задача
    'max_retries': 3,             # Количество повторов при ошибке
}
```

---

## Глобальные переменные

### `DEFAULT_TASK_ENV`

Базовые переменные окружения для всех задач:

```python
DEFAULT_TASK_ENV = {
    'PYTHON_PATH': '/path/to/project',     # Путь к проекту
    'LOG_LEVEL': 'INFO',                   # Уровень логирования
    'TASK_RUNNER': 'scheduler_runner',     # Идентификатор системы
    'SCRIPTS_DIR': '/path/to/scripts',     # Директория скриптов
    'LOGS_ROOT': '/path/to/logs',          # Корень логов
    'TELEGRAM_TOKEN': 'bot_token',         # Токен Telegram бота
    'TELEGRAM_CHAT_ID': 'chat_id',         # ID чата для уведомлений
}
```

### `SCHEDULE`

Глобальный список всех загруженных задач:

```python
SCHEDULE: List[Dict[str, Any]] = [
    {
        'name': 'camera_cleanup',
        'user': 'operator',
        'schedule': 'daily',
        'time': '03:00'
    },
    # ... другие задачи
]
```

---

## Основные функции

### `_discover_task_configs()`

**Назначение**: Автоматическое обнаружение и загрузка конфигураций задач из структуры директорий.

**Возвращает**: `List[Dict[str, Any]]` — список всех найденных конфигураций.

**Структура поиска**:
```
scheduler_runner/tasks/
├── cameras/                    # Домен "cameras"
│   ├── CleanupScript.py
│   ├── MonitorScript.py
│   └── config/
│       └── cameras_config.py   # Ищем здесь TASK_SCHEDULE или SCHEDULE
├── system_updates/             # Домен "system_updates"  
│   ├── UpdateOS.py
│   └── config/
│       └── system_updates_config.py
└── backup/                     # Домен "backup"
    └── config/
        └── backup_config.py
```

**Алгоритм**:
1. Поиск директории `tasks`.
2. Сканирование поддиректорий, пропуск скрытых папок (`startswith('_')`).
3. Поиск файлов вида `<domain_name>_config.py` в папке `config`.
4. Динамический импорт модулей через `importlib.import_module`.
5. Извлечение конфигураций с помощью `_extract_task_configs()`.

**Пример лога**:
```
2024-01-15 12:00:00 INFO: Поиск конфигураций задач в директории: /app/tasks
2024-01-15 12:00:00 DEBUG: Попытка импорта модуля: tasks.cameras.config.cameras_config
2024-01-15 12:00:00 INFO: Загружено 4 задач из модуля cameras
2024-01-15 12:00:00 INFO: Всего обнаружено задач: 6
```

---

### `_extract_task_configs()`

**Назначение**: Извлечение конфигураций из загруженного модуля.

**Параметры**:
- `config_module` — импортированный модуль.
- `domain_name: str` — имя домена для логирования.

**Возвращает**: `Optional[List[Dict[str, Any]]]` — список конфигураций или `None`.

**Алгоритм**:
- Проверяет наличие `TASK_SCHEDULE` (приоритет 1).
- Если отсутствует, проверяет `SCHEDULE` (приоритет 2).
- Возвращает `None`, если конфигурации не найдены.

```python
if hasattr(config_module, 'TASK_SCHEDULE'):
    return config_module.TASK_SCHEDULE
elif hasattr(config_module, 'SCHEDULE'):
    return config_module.SCHEDULE
return None
```

---

### `_validate_schedule_config()`

**Назначение**: Комплексная валидация конфигурации расписания.

**Параметры**:
- `schedule: List[Dict[str, Any]]` — список задач.

**Raises**: `ValueError` — при ошибках в конфигурации.

**Проверки**:
1. Обязательные поля: `user`, `name`, `schedule`.
2. Формат расписания (например, наличие `time` для `daily`).
3. Формат времени (HH:MM).

**Пример ошибок**:
```
ValueError: "backup_task: отсутствует обязательное поле 'user'"
ValueError: "daily_cleanup: неверный формат времени '25:70' (ожидается HH:MM)"
```

---

### `_validate_time_format()`

**Назначение**: Проверка корректности формата времени HH:MM.

**Параметры**:
- `time_str: str` — строка времени.
- `task_name: str` — имя задачи для сообщений об ошибках.

**Raises**: `ValueError` — при некорректном формате.

**Алгоритм**:
```python
parts = time_str.split(':')
if len(parts) != 2:
    raise ValueError("Должно быть ровно 2 части")
hour, minute = map(int, parts)
if not (0 <= hour <= 23):
    raise ValueError(f"Час от 0 до 23, получено: {hour}")
if not (0 <= minute <= 59):
    raise ValueError(f"Минута от 0 до 59, получено: {minute}")
```

**Примеры**:
- `'14:30'` — корректно.
- `'25:70'` — ошибка.

---

### `get_task_env()`

**Назначение**: Формирование переменных окружения для задачи.

**Параметры**:
- `task: Dict[str, Any]` — конфигурация задачи.

**Возвращает**: `Dict[str, str]` — словарь переменных окружения.

**Алгоритм**:
1. Копирует `DEFAULT_TASK_ENV`.
2. Добавляет специфичные переменные из `task['env']`, преобразовав значения в строки.
3. Удаляет переменные с `None`.

**Пример**:
```python
task = {'env': {'DB_HOST': 'localhost', 'DB_PORT': 5432}}
env = get_task_env(task)
# {'PYTHON_PATH': '/path/to/project', ..., 'DB_HOST': 'localhost', 'DB_PORT': '5432'}
```

---

### `reload_schedule()`

**Назначение**: Перезагрузка конфигурации расписания.

**Возвращает**: `List[Dict[str, Any]]` — обновленный список задач.

**Raises**:
- `ValueError` — ошибки валидации.
- `ImportError` — ошибки импорта.

**Алгоритм**:
1. Вызов `_discover_task_configs()`.
2. Валидация через `_validate_schedule_config()`.
3. Логирование результатов.

**Пример**:
```python
new_schedule = reload_schedule()
print(f"Загружено задач: {len(new_schedule)}")
```

---

### `get_tasks_by_user()`

**Назначение**: Фильтрация задач по пользователю.

**Параметры**:
- `user: str` — имя пользователя.

**Возвращает**: `List[Dict[str, Any]]` — список задач пользователя.

**Пример**:
```python
tasks = get_tasks_by_user('operator')
print(f"Задачи: {len(tasks)}")
```

---

### `get_task_by_name()`

**Назначение**: Поиск задачи по имени с опциональной фильтрацией по пользователю.

**Параметры**:
- `name: str` — имя задачи.
- `user: Optional[str] = None` — фильтр по пользователю.

**Возвращает**: `Optional[Dict[str, Any]]` — задача или `None`.

**Пример**:
```python
task = get_task_by_name('backup_db', user='admin')
if task:
    print(task['schedule'])
```

---

### `get_schedule_summary()`

**Назначение**: Сводная статистика по расписанию.

**Возвращает**: `Dict[str, Any]` — статистика:
- `total_tasks` — общее количество задач.
- `users` — список пользователей.
- `schedule_types` — типы расписаний.
- `task_names` — имена задач.

**Пример**:
```python
summary = get_schedule_summary()
print(f"Всего задач: {summary['total_tasks']}")
```

**Вывод**:
```
Всего задач: 12
Пользователи: admin, operator
Типы расписаний: {'daily': 8, 'interval': 4}
```

---

## Обработка ошибок

- **Ошибки импорта**:
  ```python
  try:
      config_module = importlib.import_module(module_path)
  except ImportError as e:
      logger.error(f"Ошибка импорта: {e}")
  ```

- **Ошибки валидации**:
  ```python
  try:
      _validate_schedule_config(schedule)
  except ValueError as e:
      logger.error(f"Ошибка валидации: {e}")
      raise
  ```

---

## Логирование

Используется модуль `utils.logging`:

```python
logger = configure_logger(
    user='system',
    task_name='schedule_config',
    detailed=True,
    logs_dir=str(PATH_CONFIG['LOGS_ROOT'])
)
```

**Путь логов**: `logs/system/schedule_config/YYYY-MM-DD.log`.

**Пример**:
```
2024-01-15 12:00:00 INFO: Загружено 6 задач
```

---

## Интеграция

- **`runner.py`**: Использует `SCHEDULE` и `get_task_env`.
- **`utils.logging`**: Логирование.
- **`tasks/*/config/*_config.py`**: Источник конфигураций.

---

## Мониторинг и отладка

- **Метрики**: `len(SCHEDULE)`, `get_schedule_summary()`.
- **Логи**: Детали в `logs/system/schedule_config/`.

---

## Безопасность

- Валидация всех полей.
- Пропуск скрытых директорий.
- Преобразование значений в строки для `subprocess`.

---

Эта документация полностью описывает модуль `schedule_config.py`, включая его структуру, функции, обработку ошибок и интеграцию.