"""
CleanupScript.py

Скрипт автоматической очистки указанных директорий от устаревших файлов и пустых папок для задачи cameras.

Назначение:
    - Удаление файлов старше заданного количества дней.
    - Удаление пустых папок после очистки.
    - Гибкая настройка сценариев (local/network) через конфиги задачи cameras.
    - Централизованное логирование и обработка ошибок.

Архитектура:
    - Аргументы командной строки позволяют выбрать сценарий, порог возраста файлов и уровень логирования.
    - Все параметры по умолчанию берутся из cleanup_config.py.
    - Операции с файловой системой реализованы через scheduler_runner.utils.filesystem.
    - Логирование через scheduler_runner.utils.logging.

Пример запуска:
    python -m scheduler_runner.tasks.cameras.CleanupScript --input_dir_scenario local --max_age_days 7 --detailed_logs

Author: anikinjura
"""
__version__ = '0.0.2'

import argparse
import logging
import sys
from pathlib import Path

from scheduler_runner.tasks.cameras.config.scripts.cleanup_config import SCRIPT_CONFIG, SCHEDULE
from scheduler_runner.utils.filesystem import (
    remove_old_files,
    remove_empty_folders,
    ensure_directory_exists,
    FileSystemUtils,
)
from scheduler_runner.utils.logging import configure_logger

def parse_arguments() -> argparse.Namespace:
    """
    Парсит и возвращает аргументы командной строки.

    Аргументы:
      --input_dir_scenario: сценарий, по которому выбирается работа скрипта: локально ("local") или в облаке ("network").
      --max_age_days: Порог возраста файлов в днях, файлы старше которого будут удалены.
      --detailed_logs / --no-detailed_logs: Включить или отключить детализированное логирование.
      
    Returns:
        argparse.Namespace: Объект с распарсенными аргументами.
    """
    parser = argparse.ArgumentParser(
        description="Скрипт для удаления старых файлов и пустых папок",
        epilog="Пример: python CleanupScript.py --input_dir_scenario local --max_age_days 15 --detailed_logs"
    )
    parser.add_argument(
        "--input_dir_scenario",
        type=str,
        required=True,
        choices=["local", "network"],
        help=f"Сценарий, по которому выбирается работа скрипта: local или network"
    )
    parser.add_argument(
        "--max_age_days",
        type=int,
        help=f"Порог возраста файлов в днях (по умолчанию: из конфигурации для выбранного сценария)"
    )
    parser.add_argument(
        "--detailed_logs",
        dest="detailed_logs",
        action="store_true",
        help="Включить детализированные логи"
    )
    parser.add_argument(
        "--no-detailed_logs",
        dest="detailed_logs",
        action="store_false",
        help="Отключить детализированные логи"
    )
    # Значение по умолчанию для аргумента detailed_logs в None, если пользователь не указал ни --detailed_logs, ни --no-detailed_logs в командной строке
    parser.set_defaults(detailed_logs=None)

    return parser.parse_args()

def execute_cleanup_operations(logger: logging.Logger, input_dir: Path, max_age_days: int) -> None:
    """
    Выполняет операции очистки (удаление файлов и пустых папок) и логирует результаты.

    Эта функция вызывает методы удаления файлов и директорий из модуля utils.py,
    а результаты каждой операции записываются в основной лог-файл.

    Args:
        logger: Настроенный логгер для записи информации.
        input_dir (Path): Входная директория для очистки, выбранная в зависимости от сценария.
        max_age_days (int): Порог возраста файлов в днях; удаляются только файлы старше этого порога.

    Raises:
        RuntimeError: При возникновении критической ошибки выполнения операций.

    Используемые функции:
        - remove_old_files(...) -> dict: {'DeletedFiles': int, 'FileDeletionErrors': int}
        - remove_empty_folders(...) -> dict: {'DeletedFolders': int, 'FolderDeletionErrors': int}
        - write_operation_log(...): записывает сообщения в лог-файл
    """
    try:
        # Удаление файлов, которые старше указанного порога (в днях)
        file_result = remove_old_files(
            target_folder = input_dir,
            days_threshold = max_age_days,
            logger=logger
        )
    except Exception as e:
        logger.error(f"Ошибка при удалении старых файлов: {e}", exc_info=True)
        raise

    try:
        # Удаление пустых директорий
        folder_result = remove_empty_folders(
            target_folder = input_dir,
            logger=logger,
        )
    except Exception as e:
        logger.error(f"Ошибка при удалении пустых папок: {e}", exc_info=True)
        raise

    # Формирование сводной информации по количеству удаленных файло и директорий
    logger.info(
        "Сводка: Удалено файлов - %d, ошибок удаления файлов - %d, "
        "удалено папок - %d, ошибок удаления папок - %d",
        file_result.get("removed", 0),
        file_result.get("errors", 0),
        folder_result.get("removed", 0),
        folder_result.get("errors", 0)
    )

def get_scenario_config(scenario: str) -> dict:
    """
    Возвращает параметры конфигурации для выбранного сценария.

    Args:
        scenario (str): Имя сценария ('local' или 'network').

    Returns:
        dict: Конфиг для выбранного сценария.

    Raises:
        ValueError: Если сценарий не найден в SCRIPT_CONFIG.
    """    
    scenario = scenario.lower()
    if scenario not in SCRIPT_CONFIG:
        raise ValueError(f"Неизвестный сценарий: {scenario}. Допустимые значения: {', '.join(SCRIPT_CONFIG.keys())}")
    return SCRIPT_CONFIG[scenario]

def main() -> None:
    """
    Основная функция выполнения скрипта очистки.

    Этапы:
      1. Разбор аргументов командной строки, выбор конфигурации для указанного сценария и определение параметров очистки. 
      2. Настройка логера.
      3. Валидация директории для очистки.
      4. Выполнение операций очистки и логирование результатов.
      5. Обработка исключений с критическим завершением при ошибке и корректное завершение.

    Возвращаемое значение:
        None

    Пример использования:
        python CleanupScript.py --input_dir_scenario local --max_age_days 15 --detailed_logs
    """
    # Разбор аргументов командной строки, выбор конфигурации для указанного сценария и определение параметров очистки.
    args = parse_arguments()                                                                                # парсинг параметров, указаных в командной строке
    scenario_config = get_scenario_config(args.input_dir_scenario)

    # Устанавливаем параметры. Приоритет: аргументы командной строки > конфиг сценария.
    max_age_days = args.max_age_days if args.max_age_days is not None else scenario_config["MAX_AGE_DAYS"]           # значение по умолчанию берем из конфига в соответствии с выбранным сценарием
    if args.detailed_logs is not None:                      # detailed_logs может быть True, False или None
        detailed_logs = args.detailed_logs
    else:
        detailed_logs = scenario_config["DETAILED_LOGS"]    # если detailed_logs не указан в командной строке, то берем из конфига                                                 # значение по умолчанию берем из конфига в соответствии с выбранным сценарием
    input_dir = scenario_config["CLEANUP_DIR"]

    # Формируем параметры для логгера
    user = scenario_config.get("USER", "operator")
    task_name = scenario_config.get("TASK_NAME", f"CleanupScript_{args.input_dir_scenario}")

    logger = configure_logger(
        user=user,
        task_name=task_name,
        detailed=detailed_logs
    )

    # Валидация и подготовка директории для очистки
    try:
        if not FileSystemUtils.validate_writable_path(input_dir):
            logger.critical(f"Нет доступа на запись в директорию: {input_dir}")
            sys.exit(1)
        ensure_directory_exists(input_dir, logger=logger)
    except Exception as e:
        logger.critical(f"Ошибка подготовки директории: {e}", exc_info=True)
        sys.exit(1)

    logger.info("Старт очистки: %s (Порог: %d дней)", input_dir, max_age_days)

    # Выполнение операций очистки и логирование результатов.
    try:
        execute_cleanup_operations(logger, input_dir, max_age_days)
    # Обработка исключений с критическим завершением при ошибке.
    except Exception as e:
        logger.critical(f"Процедура очистки завершена с ошибкой: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Процедура очистки завершена.")

if __name__ == "__main__":
    main()