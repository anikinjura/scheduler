"""
Модуль конфигурации расписания задач.

Этот модуль отвечает за автоматическое обнаружение, загрузку и валидацию
конфигураций задач из доменных подпакетов в директории tasks/.

Модуль выполняет следующие функции:
    - Автоматическое обнаружение конфигураций задач в подпапках
    - Динамическая загрузка модулей конфигурации
    - Валидация структуры и содержимого конфигураций
    - Объединение переменных окружения для задач
    - Предоставление централизованного доступа к расписанию

Структура конфигурации задачи:
    {
        'name': 'имя_задачи',
        'user': 'системный_пользователь',
        'module': 'модуль_для_запуска',
        'schedule': 'daily' | 'hourly' | 'interval',
        'time': 'HH:MM',  # для daily
        'args': ['arg1', 'arg2'],
        'timeout': 300,
        'env': {'KEY': 'value'}
    }

Author: anikinjura
"""
__version__ = '0.0.1'

from typing import List, Dict, Any, Optional
from pathlib import Path
import sys
import importlib

# Импортируем базовую конфигурацию
from config.base_config import PATH_CONFIG

# Импортируем централизованную систему логирования
from scheduler_runner.utils.logging import configure_logger

# Настройка логирования для schedule_config
logger = configure_logger(
    user='system',
    task_name='schedule_config',
    detailed=True,
    logs_dir=str(PATH_CONFIG['LOGS_ROOT'])
)

# Переменные окружения по умолчанию для всех задач. Пути к директориям преобразуются в строки для совместимости
# с запуском в subprocess.
DEFAULT_TASK_ENV = {
    'BASE_DIR': str(PATH_CONFIG['BASE_DIR']),
    'SCHEDULER_ROOT': str(PATH_CONFIG['SCHEDULER_ROOT']),
    'LOGS_ROOT': str(PATH_CONFIG['LOGS_ROOT']),
    'TASKS_ROOT': str(PATH_CONFIG['TASKS_ROOT']),

    'LOG_LEVEL': 'INFO',
    'TASK_RUNNER': 'scheduler_runner',
}


def _discover_task_configs() -> List[Dict[str, Any]]:
    """
    Обнаруживает и загружает конфигурации задач из всех подпапок tasks.
    
    Функция сканирует директорию tasks/ в поисках подпапок с конфигурациями.
    Для каждой подпапки ищет файл config/{папка}_schedule.py и пытается
    импортировать переменные TASK_SCHEDULE или SCHEDULE.
    
    Структура поиска:
        scheduler_runner/tasks/
        ├── domain1/
        │   └── config/
        │       └── domain1_schedule.py  # содержит TASK_SCHEDULE или SCHEDULE
        └── domain2/
            └── config/
                └── domain2_schedule.py
    
    Returns:
        List[Dict[str, Any]]: список всех найденных и загруженных конфигураций задач
        
    Raises:
        ImportError: если не удается импортировать модуль конфигурации
        AttributeError: если в модуле отсутствуют ожидаемые переменные
        
    Example:
        >>> configs = _discover_task_configs()
        >>> len(configs)
        5
        >>> print(configs[0]['name'])
        'camera_cleanup'
    """
    tasks_schedule: List[Dict[str, Any]] = []

    # Определяем путь к директории tasks
    tasks_dir = Path(DEFAULT_TASK_ENV['TASKS_ROOT'])
    
    if not tasks_dir.exists():
        logger.warning(f"Директория задач не найдена: {tasks_dir}")
        return tasks_schedule
    
    logger.info(f"Поиск конфигураций задач в директории: {tasks_dir}")
    
    # Проходим по всем подпапкам в tasks
    for task_dir in tasks_dir.iterdir():
        # Пропускаем файлы и скрытые директории
        if not task_dir.is_dir() or task_dir.name.startswith('_'):
            continue
        # Формируем путь к конфигурационному файлу    
        config_file = task_dir / 'config' / f'{task_dir.name}_schedule.py'
        
        if not config_file.exists():
            logger.debug(f"Конфигурация не найдена для задачи: {task_dir.name}")
            continue
            
        try:
            # Добавляем путь к tasks в sys.path для корректного импорта
            tasks_path = str(tasks_dir.parent)
            if tasks_path not in sys.path:
                sys.path.insert(0, tasks_path)            
            
             # Формируем путь для импорта модуля
            module_path = f'tasks.{task_dir.name}.config.{task_dir.name}_schedule'
            logger.debug(f"Попытка импорта модуля: {module_path}")
            
            # Импортируем модуль конфигурации
            config_module = importlib.import_module(module_path)
            
            # Ищем переменную TASK_SCHEDULE или SCHEDULE в модуле
            task_configs = _extract_task_configs(config_module, task_dir.name)
            
            if task_configs:
                if isinstance(task_configs, list):
                    # Добавляем информацию о домене к каждой задаче
                    for task_config in task_configs:
                        if 'domain' not in task_config:
                            task_config['domain'] = task_dir.name

                    tasks_schedule.extend(task_configs)
                    logger.info(f"Загружено {len(task_configs)} задач из модуля {task_dir.name}")
                else:
                    logger.warning(f"Конфигурация в {config_file} должна быть списком")
            else:
                logger.warning(f"Не найдена переменная TASK_SCHEDULE или SCHEDULE в {config_file}")
                
        except ImportError as e:
            logger.error(f"Ошибка импорта конфигурации {config_file}: {e}")
        except Exception as e:
            logger.error(f"Неожиданная ошибка при загрузке конфигурации {config_file}: {e}")
    
    logger.info(f"Всего обнаружено задач: {len(tasks_schedule)}")
    
    return tasks_schedule

def _extract_task_configs(config_module, domain_name: str) -> Optional[List[Dict[str, Any]]]:
    """
    Извлекает конфигурации задач из загруженного модуля.
    
    Функция ищет в модуле переменные TASK_SCHEDULE или SCHEDULE
    и возвращает их содержимое. Приоритет отдается TASK_SCHEDULE.
    
    Args:
        config_module: импортированный модуль конфигурации
        domain_name: имя домена для логирования
        
    Returns:
        Optional[List[Dict[str, Any]]]: список конфигураций задач или None
        
    Example:
        >>> module = importlib.import_module('tasks.cameras.config.cameras_config')
        >>> configs = _extract_task_configs(module, 'cameras')
        >>> len(configs)
        3
    """
    # Проверяем наличие переменной TASK_SCHEDULE (приоритет)
    if hasattr(config_module, 'TASK_SCHEDULE'):
        logger.debug(f"Найдена переменная TASK_SCHEDULE в домене {domain_name}")
        return config_module.TASK_SCHEDULE
    
    # Проверяем наличие переменной SCHEDULE (альтернатива)
    elif hasattr(config_module, 'SCHEDULE'):
        logger.debug(f"Найдена переменная SCHEDULE в домене {domain_name}")
        return config_module.SCHEDULE
    
    # Переменные не найдены
    return None

def _validate_schedule_config(schedule: List[Dict[str, Any]]):
    """
    Проверяет корректность конфигурации расписания задач.
    
    Функция выполняет комплексную валидацию каждой задачи:
    1. Проверяет наличие обязательных полей
    2. Валидирует формат расписания
    3. Проверяет корректность времени для daily задач
    4. Валидирует структуру данных
    
    Args:
        schedule: список конфигураций задач для валидации
        
    Raises:
        ValueError: если найдены ошибки в конфигурации
        
    Example:
        >>> schedule = [
        ...     {'name': 'backup', 'user': 'admin', 'schedule': 'daily', 'time': '02:00'},
        ...     {'name': 'cleanup', 'user': 'operator', 'schedule': 'hourly'}
        ... ]
        >>> _validate_schedule_config(schedule)  # Успешно
        
        >>> bad_schedule = [{'name': 'bad', 'schedule': 'daily'}]  # Отсутствует 'user'
        >>> _validate_schedule_config(bad_schedule)  # Raises ValueError
    """
    required_fields = ['user', 'name', 'schedule']
    
    logger.debug(f"Валидация конфигурации для {len(schedule)} задач")
    
    for i, task in enumerate(schedule):
        task_name = task.get('name', f'задача #{i}')
        
        # Проверяем обязательные поля
        _validate_required_fields(task, required_fields, task_name)
        
        # Проверяем корректность расписания (в том числе формат времени для daily задач)
        _validate_schedule_format(task, task_name)
        
        logger.debug(f"{task_name}: конфигурация валидна")
    
    logger.info(f"Валидация завершена успешно для {len(schedule)} задач")


def _validate_required_fields(
    task: Dict[str, Any], 
    required_fields: List[str], 
    task_name: str
) -> None:
    """
    Проверяет наличие обязательных полей в конфигурации задачи.
    
    Args:
        task: конфигурация задачи для проверки
        required_fields: список обязательных полей
        task_name: имя задачи для сообщений об ошибках
        
    Raises:
        ValueError: если отсутствует обязательное поле
        
    Example:
        >>> task = {'name': 'test', 'user': 'admin'}
        >>> _validate_required_fields(task, ['name', 'user'], 'test')  # OK
        >>> _validate_required_fields(task, ['name', 'schedule'], 'test')  # ValueError
    """
    for field in required_fields:
        if field not in task or not task[field]:
            error_msg = f"{task_name}: отсутствует обязательное поле '{field}'"
            logger.error(error_msg)
            raise ValueError(error_msg)


def _validate_schedule_format(task: Dict[str, Any], task_name: str) -> None:
    """
    Проверяет корректность формата расписания задачи.
    
    Валидирует следующие типы расписаний:
    - daily: требует поле 'time' в формате HH:MM
    - hourly: дополнительных полей не требует
    - interval: может требовать дополнительные параметры
    
    Args:
        task: конфигурация задачи
        task_name: имя задачи для сообщений об ошибках
        
    Raises:
        ValueError: если формат расписания некорректен
        
    Example:
        >>> task = {'schedule': 'daily', 'time': '14:30'}
        >>> _validate_schedule_format(task, 'test')  # OK
        
        >>> task = {'schedule': 'daily', 'time': '25:70'}
        >>> _validate_schedule_format(task, 'test')  # ValueError
    """
    schedule_type = task.get('schedule')
    
    # Проверяем расписание типа 'daily'
    if schedule_type == 'daily':
        if 'time' not in task:
            error_msg = f"{task_name}: для daily расписания требуется поле 'time'"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Проверяем формат времени
        _validate_time_format(task.get('time', ''), task_name)


def _validate_time_format(time_str: str, task_name: str) -> None:
    """
    Проверяет корректность формата времени HH:MM.
    
    Args:
        time_str: строка времени для проверки
        task_name: имя задачи для сообщений об ошибках
        
    Raises:
        ValueError: если формат времени некорректен
        
    Example:
        >>> _validate_time_format('14:30', 'test')  # OK
        >>> _validate_time_format('25:70', 'test')  # ValueError
        >>> _validate_time_format('14:30:45', 'test')  # ValueError
    """
    try:
        parts = time_str.split(':')
        if len(parts) != 2:
            raise ValueError("Неверное количество частей")
        
        hour, minute = map(int, parts)
        
        if not (0 <= hour <= 23):
            raise ValueError(f"Час должен быть от 0 до 23, получено: {hour}")
            
        if not (0 <= minute <= 59):
            raise ValueError(f"Минута должна быть от 0 до 59, получено: {minute}")
            
        logger.debug(f"{task_name}: время выполнения {time_str} корректно")
        
    except ValueError as e:
        error_msg = f"{task_name}: неверный формат времени '{time_str}' (ожидается HH:MM): {e}"
        logger.error(error_msg)
        raise ValueError(error_msg)


def get_task_env(task: Dict[str, Any]) -> Dict[str, str]:
    """
    Возвращает объединенные переменные окружения для задачи.
    
    Функция объединяет переменные окружения по приоритету:
    1. DEFAULT_TASK_ENV (базовые переменные для всех задач)
    2. task['env'] (специфичные переменные задачи)
    
    Все значения преобразуются в строки, так как subprocess требует
    строковые значения переменных окружения.
    
    Args:
        task: конфигурация задачи, может содержать поле 'env'
        
    Returns:
        Dict[str, str]: словарь переменных окружения со строковыми значениями
        
    Example:
        >>> task = {
        ...     'name': 'backup',
        ...     'env': {
        ...         'DB_HOST': 'localhost',
        ...         'DB_PORT': 5432,
        ...         'DEBUG': True
        ...     }
        ... }
        >>> env = get_task_env(task)
        >>> env['DB_HOST']
        'localhost'
        >>> env['DB_PORT']
        '5432'
        >>> env['DEBUG']
        'True'
    """
    # Начинаем с копии базовых переменных окружения
    env = DEFAULT_TASK_ENV.copy()
    
    # Получаем специфичные для задачи переменные окружения
    task_env = task.get('env', {})
    
    # Объединяем базовые и специфичные переменные окружения
    for key, value in task_env.items():
        # добавляем в env только если значение в task_env не None
        if value is not None:
            # Добавляем к env = DEFAULT_TASK_ENV преобразованные значение в строку (требование для subprocess) из task['env']
            env[key] = str(value)
    
    # Если в DEFAULT_TASK_ENV или задаче остались ключи с None, удаляем их тоже:
    filtered_env = {k: v for k, v in env.items() if v is not None}

    return filtered_env


def reload_schedule() -> List[Dict[str, Any]]:
    """
    Перезагружает конфигурацию расписания из всех задач.
    
    Функция полезна для динамического обновления конфигурации
    без перезапуска основного процесса планировщика.
    Выполняет полный цикл: обнаружение -> загрузка -> валидация.
    
    Returns:
        List[Dict[str, Any]]: обновленный список задач
        
    Raises:
        ValueError: если новая конфигурация содержит ошибки
        ImportError: если не удается загрузить модули конфигурации
        
    Example:
        >>> # После изменения конфигурационных файлов
        >>> new_schedule = reload_schedule()
        >>> len(new_schedule)
        7  # Обновленное количество задач
        
    Note:
        Функция не обновляет глобальную переменную SCHEDULE,
        возвращает новый список для ручного управления.
    """

    logger.info("Перезагрузка конфигурации расписания")
    
    try:
        # Обнаруживаем и загружаем конфигурации
        new_schedule = _discover_task_configs()
        
        # Валидируем новую конфигурацию
        _validate_schedule_config(new_schedule)
        
        logger.info(f"Конфигурация успешно перезагружена: {len(new_schedule)} задач")
        return new_schedule
        
    except Exception as e:
        logger.error(f"Ошибка при перезагрузке конфигурации: {e}")
        raise

def get_tasks_by_user(user: str) -> List[Dict[str, Any]]:
    """
    Возвращает все задачи для указанного пользователя.
    
    Args:
        user: имя пользователя для фильтрации задач
        
    Returns:
        List[Dict[str, Any]]: список задач пользователя
        
    Example:
        >>> operator_tasks = get_tasks_by_user('operator')
        >>> len(operator_tasks)
        3
        >>> all(task['user'] == 'operator' for task in operator_tasks)
        True
    """
    return [task for task in SCHEDULE if task.get('user') == user]


def get_task_by_name(name: str, user: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Возвращает задачу по имени, опционально фильтруя по пользователю.
    
    Args:
        name: имя задачи для поиска
        user: опциональный фильтр по пользователю
        
    Returns:
        Optional[Dict[str, Any]]: найденная задача или None
        
    Example:
        >>> task = get_task_by_name('backup_db')
        >>> task['name']
        'backup_db'
        
        >>> task = get_task_by_name('backup_db', user='admin')
        >>> task is not None
        True
    """
    for task in SCHEDULE:
        if task.get('name') == name:
            if user is None or task.get('user') == user:
                return task
    return None


def get_schedule_summary() -> Dict[str, Any]:
    """
    Возвращает сводную информацию о расписании задач.
    
    Returns:
        Dict[str, Any]: словарь со статистикой расписания
        
    Example:
        >>> summary = get_schedule_summary()
        >>> summary['total_tasks']
        12
        >>> summary['users']
        ['admin', 'operator', 'backup_user']
        >>> summary['schedule_types']
        {'daily': 8, 'hourly': 3, 'interval': 1}
    """
    users = set()
    schedule_types = {}
    
    for task in SCHEDULE:
        # Собираем уникальных пользователей
        if 'user' in task:
            users.add(task['user'])
        
        # Подсчитываем типы расписаний
        schedule_type = task.get('schedule', 'unknown')
        schedule_types[schedule_type] = schedule_types.get(schedule_type, 0) + 1
    
    return {
        'total_tasks': len(SCHEDULE),
        'users': sorted(list(users)),
        'schedule_types': schedule_types,
        'task_names': [task.get('name', 'Unknown') for task in SCHEDULE]
    }



def print_schedule(
    user: Optional[str] = None,
    domain: Optional[str] = None,
    show_env: bool = False,
    show_args: bool = True,
    show_time: bool = True,
    show_module: bool = True,
    sort_by: str = "time"
):
    """
    Красиво выводит задачи расписания с возможностью фильтрации по пользователю и домену.

    Args:
        user (str, optional): фильтр по пользователю
        domain (str, optional): фильтр по домену (например, 'cameras', 'system')
        show_env (bool): выводить ли переменные окружения задачи
        show_args (bool): выводить ли аргументы задачи
        show_time (bool): выводить ли время/тип расписания
        show_module (bool): выводить ли модуль задачи
        sort_by (str): поле для сортировки ('time', 'name', 'user', 'domain')
    Пример использования:
        from scheduler_runner.schedule_config import print_schedule
        
        # Вывести все задачи
        print_schedule()

        # Только для пользователя 'operator'
        print_schedule(user='operator')

        # Только для домена 'system'
        print_schedule(domain='system')

        # Для анализа пересечений по времени (например, только daily)
        print_schedule(sort_by='time')    
        
    """
    from pprint import pprint

    filtered = SCHEDULE
    if user:
        filtered = [t for t in filtered if t.get("user") == user]
    if domain:
        filtered = [t for t in filtered if t.get("domain") == domain]

    def sort_key(task):
        return task.get(sort_by) or ""

    filtered = sorted(filtered, key=sort_key)

    print(f"\n{'='*30} SCHEDULE (user={user or 'ANY'}, domain={domain or 'ANY'}) {'='*30}")
    for i, task in enumerate(filtered, 1):
        print(f"{i:2d}. {task.get('name', '???')}", end="")
        if show_time:
            print(f" | {task.get('schedule', '')}", end="")
            if task.get('schedule') == 'daily':
                print(f" {task.get('time', '')}", end="")
        print(f" | user: {task.get('user', '')}", end="")
        if 'domain' in task:
            print(f" | domain: {task['domain']}", end="")
        if show_module:
            print(f"\n    module: {task.get('module', '')}", end="")
        if show_args and task.get('args'):
            print(f"\n    args: {task['args']}", end="")
        if show_env and task.get('env'):
            print(f"\n    env: {task['env']}", end="")
        print("\n" + "-"*80)
    print(f"Всего задач: {len(filtered)}\n")



# Загружаем конфигурацию задач при импорте модуля
logger.info("Инициализация системы расписания задач")
SCHEDULE: List[Dict[str, Any]] = _discover_task_configs()

# print("[DEBUG] Итоговый SCHEDULE:")
# for task in SCHEDULE:
#   print(task)

# Выполняем валидацию при импорте модуля
try:
    _validate_schedule_config(SCHEDULE)
    logger.info(f"Система расписания инициализирована: загружено и проверено {len(SCHEDULE)} задач")

    # Выводим краткую сводку в лог
    summary = get_schedule_summary()
    logger.info(f"Пользователи: {', '.join(summary['users'])}")
    logger.info(f"Типы расписаний: {summary['schedule_types']}")    
except Exception as e:
    logger.error(f"Критическая ошибка при инициализации системы расписания: {e}")
    raise