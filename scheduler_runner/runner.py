"""
Основной модуль планировщика задач.

Этот модуль является точкой входа для системы планирования и выполнения задач.
Запускается Windows Task Scheduler с параметрами командной строки для фильтрации
и выполнения задач по расписанию или принудительно.

Пример запуска:
    pythonw runner.py --user operator
    pythonw runner.py --user admin --task BackupDB --detailed

Модуль выполняет следующие функции:
    - Парсинг аргументов командной строки
    - Загрузка и фильтрация конфигурации задач
    - Проверка расписания выполнения
    - Запуск задач в подпроцессах с контролем окружения
    - Логирование результатов выполнения

Author: anikinjura
"""
__version__ = '0.0.1'

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

try:
    from scheduler_runner.schedule_config import SCHEDULE, get_task_env
    from scheduler_runner.utils.logging import configure_logger
    from scheduler_runner.utils.timing import should_run_now
    from scheduler_runner.utils.subprocess import run_subprocess
    from config.base_config import PATH_CONFIG
except ImportError as e:
    print(f"Критическая ошибка импорта: {e}")
    print("Проверьте структуру проекта и наличие всех модулей")
    sys.exit(3)


def parse_arguments() -> argparse.Namespace:
    """
    Парсит аргументы командной строки для планировщика задач.
    
    Функция создает парсер аргументов командной строки с поддержкой следующих параметров:
    - --user: обязательный параметр для указания системного пользователя
    - --task: опциональный параметр для принудительного запуска конкретной задачи
    - --detailed: флаг включения детального логирования на уровне DEBUG
    
    Returns:
        argparse.Namespace: объект с распарсенными аргументами командной строки
        
    Raises:
        SystemExit: если переданы некорректные аргументы или отсутствует --user
        
    Example:
        >>> args = parse_arguments()
        >>> print(args.user)
        'operator'
        >>> print(args.task)
        None
        >>> print(args.detailed)
        False
    """
    parser = argparse.ArgumentParser(
        description="Планировщик выполнения задач по расписанию",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  %(prog)s --user operator                    # Запуск всех задач пользователя operator по расписанию
  %(prog)s --user admin --task BackupDB       # Принудительный запуск конкретной задачи BackupDB
  %(prog)s --user operator --detailed         # Запуск с детальным логированием (DEBUG)
        """
    )
    
    parser.add_argument(
        '--user', 
        required=True, 
        help='Имя системного пользователя для фильтрации задач'
    )
    
    parser.add_argument(
        '--task', 
        help='Имя конкретной задачи для принудительного запуска (игнорирует расписание)'
    )
    
    parser.add_argument(
        '--detailed', 
        action='store_true', 
        help='Включить детальное логирование на уровне DEBUG'
    )
    
    return parser.parse_args()


def filter_tasks(all_tasks: List[Dict[str, Any]], user: str, task_name: str | None = None) -> List[Dict[str, Any]]:
    """
    Фильтрует список задач по пользователю и опционально по имени задачи.
    
    Функция выполняет двухэтапную фильтрацию:
    1. Сначала отбирает все задачи для указанного пользователя
    2. Если указано имя задачи, дополнительно фильтрует по этому критерию
    
    Args:
        all_tasks: полный список задач из конфигурации планировщика
        user: имя пользователя для фильтрации задач
        task_name: опциональное имя конкретной задачи для дополнительной фильтрации
        
    Returns:
        List[Dict[str, Any]]: отфильтрованный список задач, соответствующих критериям
        
    Example:
        >>> tasks = [
        ...     {'name': 'backup', 'user': 'admin'},
        ...     {'name': 'cleanup', 'user': 'operator'},
        ...     {'name': 'monitor', 'user': 'operator'}
        ... ]
        >>> filtered = filter_tasks(tasks, 'operator')
        >>> len(filtered)
        2
        >>> filtered = filter_tasks(tasks, 'operator', 'cleanup')
        >>> len(filtered)
        1
    """
    # Приводим имя пользователя к нижнему регистру для унификации
    user_lower = user.lower()

    # Первый этап: фильтрация по пользователю (без учета регистра)
    user_tasks = [task for task in all_tasks if str(task.get('user', '')).lower() == user_lower]
    
    # Второй этап: дополнительная фильтрация по имени задачи (если задача указана, приводим её к нижнему регистру)
    if task_name:
        task_name_lower = task_name.lower()
        user_tasks = [task for task in user_tasks if str(task.get('name', '')).lower() == task_name_lower]
    
    return user_tasks

def sort_tasks_by_time(tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Сортирует задачи в порядке возрастания по полю 'time'.
    Если time отсутствует или некорректен, считается 0 минут.
    """    
    def _get_time(task: Dict[str, Any]) -> int:
        t = task.get('time')
        if isinstance(t, str) and ':' in t:
            try:
                h, m = map(int, t.split(':', 1))
                return h * 60 + m
            except ValueError:
                return 0
        return 0
    return sorted(tasks, key=_get_time)

def execute_task(task: Dict[str, Any], logger, force_run: bool = False) -> bool:
    """
    Выполняет отдельную задачу с проверкой расписания и контролем процесса.
    
    Функция выполняет следующие операции:
    1. Проверяет расписание выполнения (если не принудительный запуск)
    2. Определяет модуль/скрипт для запуска
    3. Подготавливает параметры запуска и переменные окружения
    4. Запускает задачу в подпроцессе с контролем времени выполнения
    5. Логирует результат выполнения
    
    Args:
        task: словарь с конфигурацией задачи, содержащий поля:
            - name: имя задачи
            - module/script: модуль или скрипт для запуска
            - args: список аргументов командной строки
            - timeout: максимальное время выполнения в секундах
            - schedule: конфигурация расписания
        logger: настроенный логгер для записи событий выполнения
        force_run: флаг принудительного запуска без проверки расписания
        
    Returns:
        bool: True если задача выполнена успешно, False в случае ошибки
        
    Raises:
        ValueError: если конфигурация расписания задачи некорректна
        
    Example:
        >>> task = {
        ...     'name': 'backup_db',
        ...     'module': 'backup_script',
        ...     'args': ['--full'],
        ...     'timeout': 300,
        ...     'schedule': 'daily',
        ...     'time': '02:00'
        ... }
        >>> success = execute_task(task, logger, force_run=True)
        >>> print(success)
        True
    """
    task_name = task.get('name', 'Unknown')
    schedule_type = task.get('schedule')

    # Определяем, нужно ли запускать задачу
    if force_run:
        should_run = True
        logger.info(f"Старт задачи '{task_name}' (force_run={force_run})")
    else:
        try:
            should_run = should_run_now(task, datetime.now())
        except ValueError as e:
            logger.error(f"Ошибка в конфиге расписания '{task_name}': {e}")
            return False
        except Exception as e:
            logger.error(f"Ошибка проверки расписания '{task_name}': {e}")
            return False
        if not should_run:
            logger.info(f"Задача '{task_name}' не должна запускаться сейчас по расписанию")
            return False # TODO: возможно, стоит вернуть True, если задача уже выполнена
        # логируем старт
        logger.info(f"Старт задачи '{task_name}' (force_run={force_run})")
    
    # Определяем модуль для запуска (приоритет: module > script > name)
    script_module = task.get('module') or task.get('script') or task_name
    
    # Подготавливаем параметры запуска
    args_list = task.get('args', [])
    env_vars = get_task_env(task)
    timeout_seconds = task.get('timeout', 60)

    # Определяем рабочую директорию
    working_directory = task.get('working_dir')
    if not working_directory and PATH_CONFIG.get('SCRIPTS_DIR'):
        working_directory = str(PATH_CONFIG['SCRIPTS_DIR'])
    else:
        PROJECT_ROOT = Path(__file__).parent.parent
        working_directory = str(PROJECT_ROOT / 'scheduler_runner' / 'tasks')

    # Логируем параметры запуска
    logger.info(f"Запуск модуля '{script_module}' с аргументами: {args_list}")
    if logger.isEnabledFor(10):  # DEBUG level
        logger.debug(f"Переменные окружения: {list(env_vars.keys())}")
        logger.debug(f"Рабочая директория: {working_directory}")
        logger.debug(f"Таймаут: {timeout_seconds} сек")
    
    # Выполняем задачу в подпроцессе
    try:
        now = datetime.now()
        if schedule_type in ('daily', 'hourly'):
            window = now.strftime('%Y-%m-%d_%H')
        else:
            window = now.strftime('%Y-%m-%d_%H-%M')

        success = run_subprocess(
            script_name=script_module,
            args=args_list,
            env=env_vars,
            logger=logger,
            timeout=timeout_seconds,
            working_dir=working_directory,
            schedule_type=schedule_type,
            window=window            
        )
    except Exception as e:
        logger.exception(f"Ошибка при запуске подпроцесса для '{task_name}': {e}")
        return False
    
    # Логируем результат
    if success:
        logger.info(f"Задача '{task_name}' успешно завершена")
        return True
    else:
        logger.error(f"Задача '{task_name}' завершилась с ошибкой")
        return False


def main() -> None:
    """
    Основная функция приложения - точка входа планировщика задач.
    
    Функция выполняет полный цикл работы планировщика:
    1. Парсит аргументы командной строки
    2. Фильтрует задачи по пользователю и имени
    3. Проверяет наличие задач для выполнения
    4. Запускает каждую задачу с индивидуальным логированием
    5. Подсчитывает статистику выполнения
    6. Устанавливает код завершения процесса
    
    Exit Codes:
        0: все задачи выполнены успешно
        1: одна или несколько задач завершились с ошибкой
        2: выполнение прервано пользователем (Ctrl+C)
        3: критическая ошибка приложения
        
    Raises:
        KeyboardInterrupt: при прерывании выполнения пользователем
        Exception: при критических ошибках приложения
        
    Example:
        При запуске с аргументами --user operator --detailed:
        1. Загружает все задачи для пользователя 'operator'
        2. Проверяет расписание каждой задачи
        3. Запускает задачи с детальным логированием
        4. Выводит итоговую статистику
    """
    try:
        # Парсим аргументы командной строки
        args = parse_arguments()

        # Проверяем доступность конфигурации
        if not SCHEDULE:
            print("Конфигурация расписания пуста или не загружена")
            sys.exit(3)

        # Фильтруем задачи по пользователю и имени задачи            
        tasks_to_run = filter_tasks(SCHEDULE, args.user, args.task)
        
        # Сортируем задачи по возрастанию времени выполнения
        tasks_to_run = sort_tasks_by_time(tasks_to_run)

        # Проверяем наличие задач для выполнения
        if not tasks_to_run:
            if args.task:
                print(f"Задача '{args.task}' для пользователя '{args.user}' не найдена")
            else:
                print(f"Нет задач для пользователя '{args.user}'")
            sys.exit(2)
        
        # Выводим информацию о найденных задачах
        task_count = len(tasks_to_run)
        print(f"Найдено {task_count} задач(и) для выполнения")

        # Инициализируем счетчики успешных и неудачных выполнений
        successful_tasks = 0
        failed_tasks = 0       
        
        # Выполняем каждую задачу
        for task in tasks_to_run:
            # Настраиваем индивидуальный логгер для каждой задачи
            try:
                logger = configure_logger(
                    user=args.user,
                    task_name=task.get('name'),
                    detailed=args.detailed
                )
            except Exception as e:
                print(f"Ошибка настройки логгера для задачи {task.get('name', 'Unknown')}: {e}")
                failed_tasks += 1
                continue
            
            try:
                # Выполняем задачу
                if execute_task(task, logger, force_run=bool(args.task)):
                    successful_tasks += 1
                else:
                    failed_tasks += 1
            except Exception as e:
                # Логируем неожиданные ошибки выполнения
                task_name = task.get('name', 'Unknown')
                logger.exception(f"Неожиданная ошибка при выполнении задачи '{task_name}': {e}")
                failed_tasks += 1
        
        # Выводим итоговую статистику
        print(f"Выполнение завершено: {successful_tasks} успешно, {failed_tasks} с ошибками")
        
        # Устанавливаем код завершения в зависимости от результатов
        if failed_tasks > 0:
            sys.exit(1)
        sys.exit(0)

    except KeyboardInterrupt:
        print("Выполнение прервано пользователем")
        sys.exit(2)
        
    except Exception as e:
        print(f"Критическая ошибка: {e}")
        sys.exit(3)


if __name__ == '__main__':
    main()