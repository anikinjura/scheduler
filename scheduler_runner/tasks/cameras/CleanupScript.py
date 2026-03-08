"""
CleanupScript.py

Очистка архивов камер:
- сценарий `local` поддерживает несколько директорий (`CLEANUP_DIRS`);
- сценарий `network` очищает целевой архив/съемный носитель.
"""
__version__ = "0.1.0"

import argparse
import logging
import sys
from pathlib import Path

from scheduler_runner.tasks.cameras.config.scripts.cleanup_config import SCRIPT_CONFIG, SCHEDULE
from scheduler_runner.utils.filesystem import FileSystemUtils, ensure_directory_exists, remove_empty_folders, remove_old_files
from scheduler_runner.utils.logging import configure_logger


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Удаление старых файлов и пустых папок",
        epilog="Пример: python -m scheduler_runner.tasks.cameras.CleanupScript --input_dir_scenario local --max_age_days 15 --detailed_logs",
    )
    parser.add_argument("--input_dir_scenario", type=str, required=True, choices=["local", "network"], help="Сценарий: local/network")
    parser.add_argument("--max_age_days", type=int, help="Порог возраста файлов в днях")
    parser.add_argument("--detailed_logs", dest="detailed_logs", action="store_true", help="Включить детализированные логи")
    parser.add_argument("--no-detailed_logs", dest="detailed_logs", action="store_false", help="Отключить детализированные логи")
    parser.set_defaults(detailed_logs=None)
    return parser.parse_args()


def execute_cleanup_operations(logger: logging.Logger, input_dir: Path, max_age_days: int) -> None:
    file_result = remove_old_files(target_folder=input_dir, days_threshold=max_age_days, logger=logger)
    folder_result = remove_empty_folders(target_folder=input_dir, logger=logger)
    logger.info(
        "Сводка (%s): удалено файлов=%d, ошибок файлов=%d, удалено папок=%d, ошибок папок=%d",
        input_dir,
        file_result.get("removed", 0),
        file_result.get("errors", 0),
        folder_result.get("removed", 0),
        folder_result.get("errors", 0),
    )


def get_scenario_config(scenario: str) -> dict:
    scenario = scenario.lower()
    if scenario not in SCRIPT_CONFIG:
        raise ValueError(f"Неизвестный сценарий: {scenario}. Допустимые: {', '.join(SCRIPT_CONFIG.keys())}")
    return SCRIPT_CONFIG[scenario]


def main() -> None:
    args = parse_arguments()
    scenario_config = get_scenario_config(args.input_dir_scenario)

    max_age_days = args.max_age_days if args.max_age_days is not None else scenario_config["MAX_AGE_DAYS"]
    detailed_logs = args.detailed_logs if args.detailed_logs is not None else scenario_config["DETAILED_LOGS"]

    logger = configure_logger(
        user=scenario_config.get("USER", "operator"),
        task_name=scenario_config.get("TASK_NAME", f"CleanupScript_{args.input_dir_scenario}"),
        detailed=detailed_logs,
    )

    if args.input_dir_scenario == "local":
        input_dirs = scenario_config.get("CLEANUP_DIRS") or [scenario_config["CLEANUP_DIR"]]
    else:
        input_dirs = [scenario_config["CLEANUP_DIR"]]

    try:
        validated_dirs = []
        for input_dir in input_dirs:
            input_dir_path = Path(input_dir)
            if not FileSystemUtils.validate_writable_path(input_dir_path):
                logger.critical("Нет доступа на запись в директорию: %s", input_dir_path)
                sys.exit(1)
            ensure_directory_exists(input_dir_path, logger=logger)
            validated_dirs.append(input_dir_path)

        logger.info("Старт очистки для %d директорий (порог: %d дней)", len(validated_dirs), max_age_days)
        for input_dir_path in validated_dirs:
            execute_cleanup_operations(logger, input_dir_path, max_age_days)

    except Exception as e:
        logger.critical("Процедура очистки завершена с ошибкой: %s", str(e), exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Процедура очистки завершена.")


if __name__ == "__main__":
    main()
