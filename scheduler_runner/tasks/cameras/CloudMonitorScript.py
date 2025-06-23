"""
CloudMonitorScript.py

Скрипт для мониторинга доступности облачного хранилища.

- Проверяет возможность записи во внешнюю директорию (cloud).
- Повторяет попытки проверки, если неудачно.
- При ошибке отправляет уведомление через Telegram.

Author: anikinjura
"""
__version__ = '1.2.0'

import argparse
import logging
import sys
from pathlib import Path
import tempfile
import time

from scheduler_runner.tasks.cameras.config.scripts.cloudmonitor_config import SCRIPT_CONFIG
from scheduler_runner.utils.logging import configure_logger
from scheduler_runner.utils.notify import send_telegram_message

def parse_arguments() -> argparse.Namespace:
    """
    Парсит аргументы командной строки для скрипта мониторинга облачного хранилища.
    """
    parser = argparse.ArgumentParser(
        description="Мониторинг доступности облачного хранилища",
        epilog="Пример: python CloudMonitorScript.py --detailed_logs"
    )
    parser.add_argument(
        "--detailed_logs",
        action="store_true",
        default=False,
        help="Включить детализированные логи"
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=SCRIPT_CONFIG["RETRIES"],
        help="Количество попыток проверки облачного хранилища"
    )
    parser.add_argument(
        "--delay",
        type=int,
        default=SCRIPT_CONFIG["DELAY"],
        help="Задержка между попытками в секундах"
    )
    return parser.parse_args()

def test_cloud_accessibility(dest_dir: Path, logger: logging.Logger) -> tuple[bool, str]:
    """
    Тестирует доступность облачного хранилища путем создания временного файла.
    """
    try:
        if not dest_dir.exists():
            logger.warning(f"Целевая директория {dest_dir} не существует")
            return False, f"Директория {dest_dir} не существует"
        if not dest_dir.is_dir():
            logger.error(f"Путь {dest_dir} не является директорией")
            return False, f"{dest_dir} не является директорией"
        with tempfile.NamedTemporaryFile(dir=dest_dir, prefix="cloud_test_", suffix=".tmp", delete=True) as tmp:
            tmp.write(b"test")
            tmp.flush()
        logger.info("Проверка доступности облачного хранилища: Успешно")
        return True, "Успешно"
    except Exception as e:
        logger.error(f"Ошибка проверки доступности: {e}", exc_info=True)
        return False, str(e)

def send_notification(message: str, logger: logging.Logger) -> bool:
    """
    Отправляет уведомление через Telegram.
    """
    token = SCRIPT_CONFIG["TOKEN"]
    chat_id = SCRIPT_CONFIG["CHAT_ID"]
    if not token or not chat_id:
        logger.warning("Параметры Telegram не заданы, уведомление не отправлено")
        return False
    success, result = send_telegram_message(token, chat_id, message, logger)
    if success:
        logger.info("Уведомление успешно отправлено через Telegram")
    else:
        logger.error("Ошибка отправки уведомления через Telegram: %s", result)
    return success

def main() -> None:
    """
    Основная функция скрипта мониторинга облачного хранилища.
    """
    args = parse_arguments()
    logger = configure_logger(
        user=SCRIPT_CONFIG["USER"],
        task_name=SCRIPT_CONFIG["TASK_NAME"],
        detailed=args.detailed_logs if args.detailed_logs is not None else SCRIPT_CONFIG["DETAILED_LOGS"]
    )

    dest_dir = Path(SCRIPT_CONFIG["CHECK_DIR"])
    retries = args.retries
    delay = args.delay
    success = False
    last_error = "Неизвестная ошибка"

    logger.info("Начало мониторинга облачного хранилища %s", dest_dir)

    for attempt in range(1, retries + 1):
        logger.debug("Попытка %d из %d", attempt, retries)
        accessibility_result, error_message = test_cloud_accessibility(dest_dir, logger)
        if accessibility_result:
            logger.info("Облачное хранилище доступно после %d попытки(попыток)", attempt)
            success = True
            break
        else:
            last_error = error_message
            logger.debug("Попытка %d неудачна: %s", attempt, error_message)
            if attempt < retries:
                logger.debug("Ожидание %d секунд до следующей попытки...", delay)
                time.sleep(delay)

    if not success:
        failure_message = (
            f"КРИТИЧЕСКАЯ ОШИБКА: Облачное хранилище недоступно после {retries} попыток проверки. "
            f"ПВЗ: {SCRIPT_CONFIG['PVZ_ID']}, Путь: {dest_dir}. "
            f"Последняя ошибка: {last_error}. "
            f"Требуется срочное вмешательство!"
        )
        notification_message = (
            f"ПВЗ: {SCRIPT_CONFIG['PVZ_ID']}, облачное хранилище недоступно. Требуется срочное вмешательство!"
        )
        logger.critical(failure_message)
        send_notification(notification_message, logger)

    logger.info("Мониторинг облачного хранилища завершен. Статус: %s", "УСПЕШНО" if success else "ОШИБКА")
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()