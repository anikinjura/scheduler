"""
NotifyScript.py

Скрипт для отправки уведомлений в Telegram с данными отчетов ОЗОН для домена (задачи) reports.

Функции:
- Загрузка JSON-файла с отчетом из директории reports
- Извлечение ключевых данных из отчета
- Формирование сообщения для Telegram
- Отправка уведомления через утилиту scheduler_runner/utils/notify.py
- Обеспечение логирования процесса

Архитектура:
- Все параметры задаются в config/scripts/NotifyScript_config.py.
- Использует централизованную утилиту scheduler_runner/utils/notify.py для отправки уведомлений.
- Использует транслитерацию для кириллических имен ПВЗ при поиске файлов.
- Обеспечивает уникальность уведомлений и избегает дублирования.

Author: anikinjura
"""
__version__ = '1.0.0'

import argparse
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

import sys
from pathlib import Path

# Добавляем корень проекта в sys.path для корректного импорта
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from scheduler_runner.utils.notify import send_telegram_message
from scheduler_runner.tasks.reports.config.scripts.NotifyScript_config import SCRIPT_CONFIG
from scheduler_runner.utils.logging import configure_logger
from scheduler_runner.tasks.reports.config.reports_paths import REPORTS_PATHS


def parse_arguments() -> argparse.Namespace:
    """
    Парсит аргументы командной строки для скрипта уведомлений.

    --report_date              - дата отчета в формате YYYY-MM-DD (по умолчанию сегодняшняя дата)
    --detailed_logs            - включить детализированные логи
    --pvz_id                   - идентификатор ПВЗ для уведомления
    """
    parser = argparse.ArgumentParser(
        description="Скрипт для отправки уведомлений в Telegram с данными отчетов ОЗОН",
        epilog="Пример: python NotifyScript.py --report_date 2026-01-02 --detailed_logs"
    )
    parser.add_argument(
        "--report_date",
        type=str,
        help="Дата отчета в формате YYYY-MM-DD (по умолчанию сегодняшняя дата)"
    )
    parser.add_argument(
        "--detailed_logs",
        action="store_true",
        default=False,
        help="Включить детализированные логи"
    )
    parser.add_argument(
        "--pvz_id",
        type=str,
        help="Идентификатор ПВЗ для уведомления"
    )

    return parser.parse_args()


def load_report_data(report_date: str, pvz_id: str) -> Dict[str, Any]:
    """
    Загружает данные отчета из JSON-файла.

    Args:
        report_date: дата отчета в формате YYYY-MM-DD
        pvz_id: идентификатор ПВЗ

    Returns:
        Dict[str, Any]: данные отчета
    """
    # Формируем имя файла отчета
    if not report_date:
        report_date = datetime.now().strftime('%Y-%m-%d')

    # Используем транслитерацию для кириллических имен ПВЗ
    from scheduler_runner.utils.system import SystemUtils
    pvz_for_filename = SystemUtils.cyrillic_to_translit(pvz_id)

    # Ищем файлы с отчетами в директории REPORTS_JSON
    report_dir = REPORTS_PATHS["REPORTS_JSON"]
    
    # Сначала пробуем найти файл с именем ПВЗ в названии
    report_filename = f"ozon_giveout_report_{pvz_for_filename}_{report_date}.json"
    report_path = report_dir / report_filename

    if not report_path.exists():
        # Если файл с именем ПВЗ не найден, ищем файл без имени ПВЗ в названии
        # Это может быть файл, созданный в тестовой среде
        for file_path in report_dir.glob(f"ozon_giveout_report_*_{report_date.replace('-', '')}*.json"):
            if report_date.replace('-', '') in file_path.name:
                report_path = file_path
                break
        else:
            # Если не найден файл с датой, ищем самый последний файл с отчетом
            report_files = list(report_dir.glob("ozon_giveout_report_*.json"))
            if report_files:
                report_path = max(report_files, key=lambda x: x.stat().st_mtime)
            else:
                raise FileNotFoundError(f"Файл отчета не найден в директории {report_dir}")

    with open(report_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def format_notification_message(report_data: Dict[str, Any]) -> str:
    """
    Форматирует сообщение для уведомления в Telegram.

    Args:
        report_data: данные отчета из JSON

    Returns:
        str: отформатированное сообщение для Telegram
    """
    # Извлекаем основные данные из отчета
    date_str = report_data.get('date', datetime.now().strftime('%Y-%m-%d'))
    issued_packages = report_data.get('issued_packages', report_data.get('total_packages', 0))
    pvz_info = report_data.get('pvz_info', 'Неизвестный ПВЗ')
    marketplace = report_data.get('marketplace', 'ОЗОН')

    # Формируем сообщение для Telegram
    message = f"📊 Отчет {marketplace}\n"
    message += f"ПВЗ: {pvz_info}\n"
    message += f"Дата: {date_str}\n"
    message += f"Количество выдач: {issued_packages}"

    return message


def send_notification(token: str, chat_id: str, message: str, logger) -> bool:
    """
    Отправляет уведомление через утилиту ядра scheduler_runner/utils/notify.py.

    Args:
        token: Токен Telegram-бота
        chat_id: ID чата для отправки
        message: Текст уведомления
        logger: Логгер для записи информации
    Returns:
        True, если отправлено успешно, False в противном случае.
    """
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
    """Основная функция управления процессом отправки уведомлений"""
    args = parse_arguments()

    # Настройка логгера
    logger = configure_logger(
        user=SCRIPT_CONFIG["USER"],
        task_name=SCRIPT_CONFIG["TASK_NAME"],
        detailed=args.detailed_logs or SCRIPT_CONFIG["DETAILED_LOGS"]
    )

    try:
        # Загружаем данные отчета
        logger.info("Загрузка данных отчета...")

        # Получаем PVZ_ID из конфигурации, если не указан в аргументах
        from config.base_config import PVZ_ID
        pvz_id = args.pvz_id or PVZ_ID

        report_data = load_report_data(args.report_date, pvz_id)
        logger.info(f"Данные отчета загружены для ПВЗ {pvz_id}, дата: {report_data.get('date', 'N/A')}")

        # Форматируем сообщение для уведомления
        message = format_notification_message(report_data)
        logger.info("Сообщение для уведомления сформировано")

        # Получаем токены из конфигурации
        token = REPORTS_PATHS.get('TELEGRAM_TOKEN')
        chat_id = REPORTS_PATHS.get('TELEGRAM_CHAT_ID')

        # Отправляем уведомление
        logger.info("Отправка уведомления в Telegram...")
        success = send_notification(token, chat_id, message, logger)

        if success:
            logger.info("Уведомление успешно отправлено в Telegram")
        else:
            logger.error("Ошибка при отправке уведомления в Telegram")

    except FileNotFoundError as e:
        logger.error(f"Файл отчета не найден: {e}")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)


if __name__ == "__main__":
    main()