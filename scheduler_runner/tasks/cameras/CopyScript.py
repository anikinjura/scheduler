"""CopyScript for cameras domain."""
__version__ = "1.3.0"

import argparse
import sys
import time
from pathlib import Path

from scheduler_runner.tasks.cameras.config.scripts.copy_config import SCRIPT_CONFIG, SCHEDULE
from scheduler_runner.utils.filesystem import FileSystemUtils, copy_recent_files
from scheduler_runner.utils.logging import configure_logger
from scheduler_runner.utils.system import SystemUtils


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Система копирования файлов по возрасту",
        epilog="Пример: python CopyScript.py --max_age_days 3 --source_dir D:/input --dest_dir D:/output --shutdown 60",
    )
    parser.add_argument("--max_age_days", "--days", type=int, help=f"Максимальный возраст файлов (по умолчанию: {SCRIPT_CONFIG['MAX_AGE_DAYS']})")
    parser.add_argument("--conflict_mode", type=str, choices=["skip", "rename"], help=f"Режим конфликта (по умолчанию: {SCRIPT_CONFIG['ON_CONFLICT']})")
    parser.add_argument("--source_dir", type=str, help="Явно задать один источник (перекрывает INPUT_DIRS/INPUT_DIR)")
    parser.add_argument("--dest_dir", type=str, help=f"Целевая директория (по умолчанию: {SCRIPT_CONFIG['OUTPUT_DIR']})")
    parser.add_argument("--detailed_logs", dest="detailed_logs", action="store_true", help="Включить детализированные логи")
    parser.add_argument("--no-detailed_logs", dest="detailed_logs", action="store_false", help="Отключить детализированные логи")
    parser.set_defaults(detailed_logs=None)
    parser.add_argument(
        "--shutdown",
        nargs="?",
        const=30,
        default=False,
        type=int,
        help="Выключить компьютер после копирования (минуты задержки перед выключением)",
    )
    parser.add_argument("--no-shutdown-delay", action="store_true", help="Отключить паузу перед выключением")
    return parser.parse_args()


def count_files_within_days(directory: Path, days: int) -> int:
    import os
    from datetime import datetime, timedelta

    count = 0
    threshold_time = datetime.now() - timedelta(days=days)
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = Path(root) / file
            try:
                if datetime.fromtimestamp(file_path.stat().st_mtime) >= threshold_time:
                    count += 1
            except Exception:
                continue
    return count


def main() -> None:
    args = parse_arguments()

    max_age_days = args.max_age_days if args.max_age_days is not None else SCRIPT_CONFIG["MAX_AGE_DAYS"]
    conflict_mode = args.conflict_mode if args.conflict_mode is not None else SCRIPT_CONFIG["ON_CONFLICT"]
    detailed_logs = args.detailed_logs if args.detailed_logs is not None else SCRIPT_CONFIG["DETAILED_LOGS"]

    if args.source_dir:
        input_dirs = [args.source_dir]
    else:
        input_dirs = SCRIPT_CONFIG.get("INPUT_DIRS") or [SCRIPT_CONFIG["INPUT_DIR"]]
    output_dir = args.dest_dir if args.dest_dir else SCRIPT_CONFIG["OUTPUT_DIR"]

    logger = configure_logger(
        user=SCRIPT_CONFIG.get("USER", "operator"),
        task_name=SCRIPT_CONFIG.get("TASK_NAME", "CopyScript"),
        detailed=detailed_logs,
    )

    try:
        source_dirs = []
        for input_dir in input_dirs:
            validated = FileSystemUtils.validate_readable_path(Path(input_dir))
            if not validated:
                logger.critical("Исходная директория недоступна для чтения: %s", input_dir)
                sys.exit(1)
            source_dirs.append(validated)

        dest_dir = FileSystemUtils.validate_writable_path(Path(output_dir))
        if not dest_dir:
            logger.critical("Целевая директория недоступна для записи: %s", output_dir)
            sys.exit(1)

        logger.info("Старт копирования: %s -> %s (порог: %s дней)", ", ".join([str(p) for p in source_dirs]), dest_dir, max_age_days)

        files_to_copy = sum(count_files_within_days(source_dir, max_age_days) for source_dir in source_dirs)
        logger.info("Обнаружено %s файлов, подходящих под критерии копирования", files_to_copy)

        shutdown_enabled = SCRIPT_CONFIG.get("SHUTDOWN_ENABLED", True)
        shutdown_if_no_files = SCRIPT_CONFIG.get("SHUTDOWN_IF_NO_FILES", False)

        copied_files = 0
        skipped_files = 0
        errors = 0

        if files_to_copy == 0:
            logger.warning("Нет файлов для копирования по указанным критериям")
            should_shutdown = shutdown_enabled and shutdown_if_no_files
        else:
            for source_dir in source_dirs:
                logger.info("Копирование из источника: %s", source_dir)
                file_result = copy_recent_files(
                    src=source_dir,
                    dst=dest_dir,
                    days_threshold=max_age_days,
                    conflict_mode=conflict_mode,
                    logger=logger,
                )
                copied_files += file_result.get("CopiedFiles", 0)
                skipped_files += file_result.get("SkippedFiles", 0)
                errors += file_result.get("FileCopyingErrors", 0)

            logger.info("Файлов скопировано: %d, пропущено: %d, ошибок: %d", copied_files, skipped_files, errors)
            should_shutdown = shutdown_enabled and ((copied_files > 0 or skipped_files > 0) or shutdown_if_no_files)

        if args.shutdown and should_shutdown:
            if isinstance(args.shutdown, int):
                if not args.no_shutdown_delay:
                    logger.info("Пауза перед выключением: %d минут", args.shutdown)
                    time.sleep(args.shutdown * 60)
                else:
                    logger.info("Пауза перед выключением отключена (--no-shutdown-delay)")
            logger.info("Проверка условий для выключения компьютера...")
            success = SystemUtils.shutdown_computer(logger=logger, force=False)
            if success:
                logger.info("Компьютер будет выключен через 60 секунд")
            else:
                logger.warning("Не удалось инициировать выключение компьютера")
        elif args.shutdown and not should_shutdown:
            if not shutdown_enabled:
                logger.info("Выключение компьютера отключено в конфигурации")
            else:
                logger.info("Выключение компьютера не требуется")

    except Exception as e:
        logger.critical("Критическая ошибка: %s", str(e), exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Процедура копирования завершена")


if __name__ == "__main__":
    main()
