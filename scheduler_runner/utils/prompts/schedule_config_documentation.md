# Документация модуля schedule_config.py

## Обзор

Модуль `schedule_config.py` отвечает за автоматическое обнаружение, загрузку и валидацию конфигураций задач из доменных подпакетов. Он обеспечивает централизованное управление расписанием выполнения задач и предоставляет API для работы с конфигурациями.

## Архитектура

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

## Структура конфигурации задачи

```python
task_config = {
    # Обязательные поля
    'name': 'backup_database',      # Уникальное имя задачи
    'user': 'admin',               # Системный пользователь
    'schedule': 'daily',           # Тип расписания
    
    # Поля выполнения
    'module': 'backup_script',     # Модуль для запуска
    'script': 'legacy_script.py', # Альтернативное имя скрипта
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
    'max_retries': 3,            # Количество повторов при ошибке
}
```

## Глобальные переменные

### DEFAULT_TASK_ENV

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

### SCHEDULE

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

## Основные функции

### _discover_task_configs()

**Назначение**: Автоматическое обнаружение и загрузка конфигураций из структуры директорий.

**Возвращает**: `List[Dict[str, Any]]` - список всех найденных конфигураций

**Структура поиска**:
```
scheduler_runner/tasks/
├── cameras/                    # Домен "cameras"
│   ├── CleanupScript.py
│   ├── MonitorScript.py
│   └── config/
│       └── cameras_config.py   # ← Ищем здесь TASK_SCHEDULE или SCHEDULE
├── system_updates/             # Домен "system_updates"  
│   ├── UpdateOS.py
│   └── config/
│       └── system_updates_config.py # ← И здесь
└── backup/                     # Домен "backup"
    └── config/
        └── backup_config.py    # ← И здесь
```

**Ход выполнения**:

1. **Поиск директории tasks**
   ```python
   tasks_dir = Path(__file__).parent / 'tasks'
   if not tasks_dir.exists():
       logger.warning(f"Директория задач не найдена: {tasks_dir}")
       return []
   ```

2. **Сканирование поддиректорий**
   ```python
   for task_dir in tasks_dir.iterdir():
       if not task_dir.is_dir() or task_dir.name.startswith('_'):
           continue  # Пропускаем файлы и скрытые папки
   ```

3. **Поиск конфигурационных файлов**
   ```python
   config_file = task_dir / 'config' / f'{task_dir.name}_config.py'
   if not config_file.exists():
       continue  # Конфигурация не найдена
   ```

4. **Динамический импорт модулей**
   ```python
   module_path = f'tasks.{task_dir.name}.config.{task_dir.name}_config'
   config_module = importlib.import_module(module_path)
   ```

5. **Извлечение конфигураций**
   ```python
   task_configs = _extract_task_configs(config_module, task_dir.name)
   if task_configs and isinstance(task_configs, list):
       tasks_schedule.extend(task_configs)
   ```

**Пример лога выполнения**:
```
2024-01-15 12:00:00 INFO: Поиск конфигураций задач в директории: /app/tasks
2024-01-15 12:00:00 DEBUG: Попытка импорта модуля: tasks.cameras.config.cameras_config
2024-01-15 12:00:00 INFO: Загружено 4 задач из модуля cameras
2024-01-15 12:00:00 DEBUG: Попытка импорта модуля: tasks.backup.config.backup_config
2024-01-15 12:00:00 INFO: Загружено 2 задач из модуля backup
2024-01-15 12:00:00 INFO: Всего обнаружено задач: 6
```

### _extract_task_configs()

**Назначение**: Извлечение конфигураций из загруженного модуля.

**Параметры**:
- `config_module` - импортированный модуль
- `domain_name: str` - имя домена для логирования

**Возвращает**: `Optional[List[Dict[str, Any]]]` - список конфигураций или None

**Алгоритм поиска переменных**:
```python
# Приоритет 1: TASK_SCHEDULE
if hasattr(config_module, 'TASK_SCHEDULE'):
    return config_module.TASK_SCHEDULE

# Приоритет 2: SCHEDULE  
elif hasattr(config_module, 'SCHEDULE'):
    return config_module.SCHEDULE

# Не найдено
return None
```

### _validate_schedule_config()

**Назначение**: Комплексная валидация конфигурации расписания.

**Параметры**:
- `schedule: List[Dict[str, Any]]` - список задач для валидации

**Raises**: `ValueError` - при обнаружении ошибок в конфигурации

**Проверки**:

1. **Обязательные поля**: `user`, `name`, `schedule`
2. **Формат расписания**: корректность типов и параметров
3. **Формат времени**: валидация HH:MM для daily задач

**Ход валидации**:

```python
required_fields = ['user', 'name', 'schedule']

for i, task in enumerate(schedule):
    task_name = task.get('name', f'задача #{i}')
    
    # Проверка обязательных полей
    _validate_required_fields(task, required_fields, task_name)
    
    # Проверка формата расписания  
    _validate_schedule_format(task, task_name)
```

**Примеры ошибок валидации**:
```python
# Отсутствует обязательное поле
ValueError: "backup_task: отсутствует обязательное поле 'user'"

# Неверный формат времени
ValueError: "daily_cleanup: неверный формат времени '25:70' (ожидается HH:MM)"

# Отсутствует время для daily расписания
ValueError: "morning_backup: для daily расписания требуется поле 'time'"
```

### _validate_time_format()

**Назначение**: Проверка корректности формата времени HH:MM.

**Параметры**:
- `time_str: str` - строка времени
- `task_name: str` - имя задачи для ошибок

**Алгоритм валидации**:
```python
def _validate_time_format(time_str: str, task_name: str) -> None:
    parts = time_str.split(':')
    
    # Проверка количества частей
    if len(parts) != 2:
        raise ValueError("Должно быть ровно 2 части")
    
    hour, minute = map(int, parts)
    
    # Проверка диапазонов
    if not (0 <= hour <= 23):
        raise ValueError(f"Час от 0 до 23, получено: {hour}")
        
    if not (0 <= minute <= 59):
        raise ValueError(f"Минута от 0 до 59, получено: {minute}")
```

**Примеры валидации**:
```python
_validate_time_format('14:30', 'task1')  # ✓ Корректно
_validate_time_format('25:70', 'task2')  # ✗ ValueError
_validate_time_format('14:30:45', 'task3')  # ✗ ValueError (3 части)
_validate_time_format('14', 'task4')     # ✗ ValueError (1 часть)
```

### get_task_env()

**Назначение**: Формирование переменных окружения для задачи.

**Параметры**:
- `task: Dict[str, Any]` - конфигурация задачи

**Возвращает**: `Dict