"""
CopyScript.py

Скрипт автоматического копирования файлов по возрастному критерию для задачи cameras.

- Копирует файлы из INPUT_DIR в OUTPUT_DIR, если им не больше MAX_AGE_DAYS.
- Обрабатывает конфликты имён файлов (skip/rename).
- Централизованное логирование.
- Поддержка детализированных логов и выключения ПК по завершении.

Author: anikinjura
"""
__version__ = '1.2.0'

import argparse
import sys
import time
from pathlib import Path

from scheduler_runner.tasks.cameras.config.scripts.copy_config import SCRIPT_CONFIG, SCHEDULE
from scheduler_runner.utils.filesystem import (
    copy_recent_files,
    ensure_directory_exists,
    FileSystemUtils,
)
from scheduler_runner.utils.logging import configure_logger
from scheduler_runner.utils.system import SystemUtils

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Система копирования файлов по возрасту",
        epilog="Пример: python CopyScript.py --max_age_days 3 --source_dir D:/input --dest_dir D:/output --shutdown 60"
    )
    parser.add_argument(
        "--max_age_days", "--days",
        type=int,
        help=f"Максимальный возраст файлов в днях (по умолчанию: {SCRIPT_CONFIG['MAX_AGE_DAYS']})"
    )
    parser.add_argument(
        "--conflict_mode",
        type=str,
        choices=["skip", "rename"],
        help=f"Стратегия разрешения конфликтов: skip или rename (по умолчанию: {SCRIPT_CONFIG['ON_CONFLICT']})"
    )
    parser.add_argument(
        "--source_dir",
        type=str,
        help=f"Исходная директория для копирования (по умолчанию: {SCRIPT_CONFIG['INPUT_DIR']})"
    )
    parser.add_argument(
        "--dest_dir",
        type=str,
        help=f"Целевая директория для копирования (по умолчанию: {SCRIPT_CONFIG['OUTPUT_DIR']})"
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
    parser.set_defaults(detailed_logs=None)
    parser.add_argument(
        "--shutdown",
        nargs='?',
        const=True,
        default=False,
        type=int,
        help="Выключить компьютер после копирования (можно указать паузу в минутах, например --shutdown 10)"
    )
    return parser.parse_args()

def main() -> None:
    """
    Главная функция управления workflow скрипта.

    Этапы выполнения:
      1. Парсинг аргументов командной строки.
      2. Настройка глобального логирования через configure_logger из utils.py.
      3. Валидация и подготовка необходимых директорий (исходной, целевой и логовой).
      4. Выполнение операций копирования и логирование результатов.
      5. Выключение компьютера (если указано).
      6. Обработка исключений с критическим завершением при ошибке.
    """    
    # 1. Парсинг аргументов командной строки.
    args = parse_arguments()
    
    # Устанавливаем параметры. Приоритет: аргументы командной строки > конфиг сценария.
    max_age_days = args.max_age_days if args.max_age_days is not None else SCRIPT_CONFIG["MAX_AGE_DAYS"]
    conflict_mode = args.conflict_mode if args.conflict_mode is not None else SCRIPT_CONFIG["ON_CONFLICT"]
    if args.detailed_logs is not None:
        detailed_logs = args.detailed_logs
    else:
        detailed_logs = SCRIPT_CONFIG["DETAILED_LOGS"]
    input_dir = args.source_dir if args.source_dir else SCRIPT_CONFIG["INPUT_DIR"]
    output_dir = args.dest_dir if args.dest_dir else SCRIPT_CONFIG["OUTPUT_DIR"]

    user = SCRIPT_CONFIG.get("USER", "operator")
    task_name = SCRIPT_CONFIG.get("TASK_NAME", "CopyScript")
    
    # 2. Настройка логирования
    logger = configure_logger(
        user=user,
        task_name=task_name,
        detailed=detailed_logs
    )

    try:
        # 3. Валидация и подготовка директорий
        source_dir = FileSystemUtils.validate_readable_path(Path(input_dir))
        dest_dir = FileSystemUtils.validate_writable_path(Path(output_dir))

        if not source_dir:
            logger.critical(f"Исходная директория недоступна для чтения: {input_dir}")
            sys.exit(1)
        if not dest_dir:
            logger.critical(f"Целевая директория недоступна для записи: {output_dir}")
            sys.exit(1)

        logger.info("Старт копирования: %s -> %s (Порог: %s дней)", 
                    source_dir, dest_dir, max_age_days)
        
        # 4. Выполнение операций копирования и логирование результатов.
        file_result = copy_recent_files(
            src=source_dir,
            dst=dest_dir,
            days_threshold=max_age_days,
            conflict_mode=conflict_mode,
            logger=logger
        )

        logger.info("Файлов скопировано: %d, Ошибок: %d",
                    file_result.get('CopiedFiles', 0),
                    file_result.get('FileCopyingErrors', 0))
        
        # 5. Выключение компьютера (если указано) после завершения копирования.
        if args.shutdown:
            if isinstance(args.shutdown, int):
                pause_seconds = args.shutdown * 60
                logger.info("Пауза перед выключением: %d минут", args.shutdown)
                time.sleep(pause_seconds)
            logger.warning("Инициируется выключение компьютера")
            SystemUtils.shutdown_computer(logger=logger, force=False)

    # 6. Обработка исключений с критическим завершением при ошибке.
    except Exception as e:
        logger.critical("Критическая ошибка: %s", str(e), exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Процедура копирования завершена")

if __name__ == "__main__":
    main()