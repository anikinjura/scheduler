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
        const=30,  # по умолчанию 30 минут
        default=False,
        type=int,
        help="Выключить компьютер после копирования (можно указать паузу в минутах, например --shutdown 10, по умолчанию 30)"
    )
    parser.add_argument(
        "--no-shutdown-delay",
        action="store_true",
        help="Отключить паузу перед выключением (для тестирования)"
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
        
        # Проверка: есть ли файлы для копирования
        def count_files_within_days(directory, days):
            """Вспомогательная функция для подсчета файлов в пределах заданного количества дней"""
            import os
            from pathlib import Path
            from datetime import datetime, timedelta
            count = 0
            now = datetime.now()
            threshold_time = now - timedelta(days=days)
            
            for root, dirs, files in os.walk(directory):
                for file in files:
                    file_path = Path(root) / file
                    try:
                        mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                        if mtime >= threshold_time:
                            count += 1
                    except Exception:
                        continue
            return count
        
        files_to_copy = count_files_within_days(source_dir, max_age_days)
        logger.info(f"Обнаружено {files_to_copy} файлов, подходящих под критерии копирования")
        
        # Проверяем, разрешено ли выключение в конфигурации
        SHUTDOWN_ENABLED = SCRIPT_CONFIG.get("SHUTDOWN_ENABLED", True)
        SHUTDOWN_IF_NO_FILES = SCRIPT_CONFIG.get("SHUTDOWN_IF_NO_FILES", False)  # по умолчанию False
        
        if files_to_copy == 0:
            logger.warning("Нет файлов для копирования по указанным критериям")
            # Определяем, нужно ли выключать компьютер при отсутствии файлов
            should_shutdown = SHUTDOWN_ENABLED and SHUTDOWN_IF_NO_FILES
        else:
            # 4. Выполнение операций копирования и логирование результатов.
            file_result = copy_recent_files(
                src=source_dir,
                dst=dest_dir,
                days_threshold=max_age_days,
                conflict_mode=conflict_mode,
                logger=logger
            )
            
            copied_files = file_result.get('CopiedFiles', 0)
            skipped_files = file_result.get('SkippedFiles', 0)
            errors = file_result.get('FileCopyingErrors', 0)
            
            logger.info("Файлов скопировано: %d, пропущено: %d, Ошибок: %d",
                        copied_files, skipped_files, errors)
            
            # Определяем, нужно ли выключать компьютер
            should_shutdown = SHUTDOWN_ENABLED and ((copied_files > 0 or skipped_files > 0) or SHUTDOWN_IF_NO_FILES)

        # 5. Выключение компьютера (если указано) после завершения копирования.
        if args.shutdown and should_shutdown:
            if isinstance(args.shutdown, int):
                if not args.no_shutdown_delay:  # Только если не отключена пауза
                    pause_seconds = args.shutdown * 60
                    logger.info("Пауза перед выключением: %d минут", args.shutdown)
                    time.sleep(pause_seconds)
                else:
                    logger.info("Пауза перед выключением отключена (--no-shutdown-delay)")
            logger.info("Проверка условий для выключения компьютера...")
            success = SystemUtils.shutdown_computer(logger=logger, force=False)
            if success:
                logger.info("Компьютер будет выключен через 60 секунд")
            else:
                logger.warning("Не удалось инициировать выключение компьютера")
        elif args.shutdown and not should_shutdown:
            if not SHUTDOWN_ENABLED:
                logger.info("Выключение компьютера отключено в конфигурации")
            else:
                logger.info("Выключение компьютера не требуется - нет файлов для копирования и SHUTDOWN_IF_NO_FILES = False")

    # 6. Обработка исключений с критическим завершением при ошибке.
    except Exception as e:
        logger.critical("Критическая ошибка: %s", str(e), exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Процедура копирования завершена")

if __name__ == "__main__":
    main()