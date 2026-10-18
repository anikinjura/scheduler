"""
CleanupScript.py

Очистка архивов камер:
- сценарий `local` поддерживает несколько директорий (`CLEANUP_DIRS`);
- сценарий `network` очищает целевой архив/съемный носитель: при `DATED_FOLDERS` удаляет целиком папки записей с
  датой в имени, самые старые первыми и в пределах `TIME_BUDGET_SECONDS` (обход всех файлов облака не укладывался в
  таймаут, и облако на 143/144 не очищалось — 10.2026); проверка доступа — не дольше `DIR_CHECK_TIMEOUT_SECONDS`.
"""
__version__ = "0.2.0"

import argparse
import logging
import sys
from pathlib import Path

from scheduler_runner.tasks.cameras.config.scripts.cleanup_config import SCRIPT_CONFIG, SCHEDULE
from scheduler_runner.utils.filesystem import (
    FileSystemUtils,
    call_with_timeout,
    ensure_directory_exists,
    remove_empty_folders,
    remove_old_dated_folders,
    remove_old_files,
)
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


def execute_dated_cleanup(logger: logging.Logger, input_dir: Path, max_age_days: int,
                          time_budget_seconds: float = None) -> dict:
    result = remove_old_dated_folders(
        target_folder=input_dir, days_threshold=max_age_days, logger=logger, time_budget_seconds=time_budget_seconds,
    )
    logger.info(
        "Сводка (%s): удалено папок с датой=%d, ошибок=%d, осталось старых=%d, самая старая оставшаяся дата=%s",
        input_dir,
        result["removed_folders"],
        result["errors"],
        result["remaining_old"],
        result["oldest_kept"] or "-",
    )
    if result["budget_exhausted"]:
        logger.warning(
            "Время очистки (%s с) исчерпано: осталось старых папок %d — продолжим при следующем запуске",
            time_budget_seconds, result["remaining_old"],
        )
    return result


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

    check_timeout = scenario_config.get("DIR_CHECK_TIMEOUT_SECONDS", 60)
    try:
        validated_dirs = []
        for input_dir in input_dirs:
            input_dir_path = Path(input_dir)
            finished, writable = call_with_timeout(FileSystemUtils.validate_writable_path, check_timeout, input_dir_path)
            if not finished:
                logger.critical("Директория не отвечает дольше %s с (зависла): %s", check_timeout, input_dir_path)
                sys.exit(1)
            if not writable:
                logger.critical("Нет доступа на запись в директорию: %s", input_dir_path)
                sys.exit(1)
            ensure_directory_exists(input_dir_path, logger=logger)
            validated_dirs.append(input_dir_path)

        dated = scenario_config.get("DATED_FOLDERS", False)
        logger.info("Старт очистки для %d директорий (порог: %d дней, режим: %s)", len(validated_dirs), max_age_days,
                    "папки с датой" if dated else "файлы")
        for input_dir_path in validated_dirs:
            if dated:
                execute_dated_cleanup(logger, input_dir_path, max_age_days, scenario_config.get("TIME_BUDGET_SECONDS"))
            else:
                execute_cleanup_operations(logger, input_dir_path, max_age_days)

    except Exception as e:
        logger.critical("Процедура очистки завершена с ошибкой: %s", str(e), exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Процедура очистки завершена.")


if __name__ == "__main__":
    main()
