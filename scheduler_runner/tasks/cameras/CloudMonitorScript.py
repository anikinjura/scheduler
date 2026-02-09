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
from scheduler_runner.utils.logging import configure_logger, TRACE_LEVEL
from scheduler_runner.utils.notifications import send_notification, test_connection as test_notification_connection

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

def create_notification_logger():
    """
    Создает и настраивает логгер для микросервиса уведомлений

    Returns:
        logging.Logger: Настроенный объект логгера для уведомлений
    """
    logger = configure_logger(
        user="cameras_domain",
        task_name="Notification",
        log_levels=[TRACE_LEVEL, logging.DEBUG],
        single_file_for_levels=False
    )

    return logger

def send_telegram_notification(message: str, main_logger=None) -> bool:
    """
    Отправляет уведомление через изолированный микросервис уведомлений.

    Аргументы:
        message: Текст уведомления.
        main_logger: Основной логгер для записи информации о попытке отправки (не используется в микросервисе уведомлений).
    Возвращает:
        True, если отправлено успешно, False в противном случае.
    """
    # Создаем изолированный логгер для работы с микросервисом уведомлений
    notification_logger = create_notification_logger()

    # Подготовим параметры подключения из конфигурации
    token = SCRIPT_CONFIG["TOKEN"]
    chat_id = SCRIPT_CONFIG["CHAT_ID"]

    if not token or not chat_id:
        notification_logger.warning("Параметры Telegram не заданы, уведомление не отправлено")
        return False

    # Подготовим параметры подключения
    connection_params = {
        "TELEGRAM_BOT_TOKEN": token,
        "TELEGRAM_CHAT_ID": chat_id
    }

    # Проверим подключение к Telegram
    notification_logger.info("Проверка подключения к Telegram...")
    connection_result = test_notification_connection(connection_params, logger=notification_logger)
    notification_logger.info(f"Результат проверки подключения к Telegram: {connection_result}")

    if not connection_result.get("success", False):
        notification_logger.error("Подключение к Telegram не удалось")
        return False

    # Отправим уведомление
    notification_logger.info(f"Отправка уведомления в Telegram: {len(message)} символов")
    notification_result = send_notification(
        message=message,
        connection_params=connection_params,
        logger=notification_logger
    )

    notification_logger.info(f"Результат отправки уведомления: {notification_result}")

    # Если передан основной логгер, сообщим ему об итогах
    if main_logger:
        if notification_result.get("success", False):
            main_logger.info("Уведомление успешно отправлено через микросервис уведомлений")
        else:
            main_logger.error(f"Ошибка при отправке уведомления: {notification_result.get('error', 'Неизвестная ошибка')}")

    return notification_result.get("success", False)

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
        send_telegram_notification(notification_message, logger)

    logger.info("Мониторинг облачного хранилища завершен. Статус: %s", "УСПЕШНО" if success else "ОШИБКА")
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()