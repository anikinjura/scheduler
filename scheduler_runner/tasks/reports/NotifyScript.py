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
    Загружает данные отчетов из JSON-файлов обоих типов.

    Args:
        report_date: дата отчета в формате YYYY-MM-DD
        pvz_id: идентификатор ПВЗ

    Returns:
        Dict[str, Any]: объединенные данные отчетов
    """
    # Формируем имя файла отчета
    if not report_date:
        report_date = datetime.now().strftime('%Y-%m-%d')

    # Используем транслитерацию для кириллических имен ПВЗ
    from scheduler_runner.utils.system import SystemUtils
    pvz_for_filename = SystemUtils.cyrillic_to_translit(pvz_id)

    # Ищем файлы с отчетами в директории REPORTS_JSON
    report_dir = REPORTS_PATHS["REPORTS_JSON"]

    # Загружаем данные из отчета по выдаче (ozon_giveout_report)
    giveout_report_data = {}
    giveout_report_filename = f"ozon_giveout_report_{pvz_for_filename}_{report_date}.json"
    giveout_report_path = report_dir / giveout_report_filename

    if not giveout_report_path.exists():
        # Если файл с именем ПВЗ не найден, ищем файл без имени ПВЗ в названии
        for file_path in report_dir.glob(f"ozon_giveout_report_*_{report_date.replace('-', '')}*.json"):
            if report_date.replace('-', '') in file_path.name:
                giveout_report_path = file_path
                break
        else:
            # Если не найден файл с датой, ищем самый последний файл с отчетом
            giveout_report_files = list(report_dir.glob("ozon_giveout_report_*.json"))
            if giveout_report_files:
                giveout_report_path = max(giveout_report_files, key=lambda x: x.stat().st_mtime)
            else:
                print(f"Файл отчета по выдаче не найден: {giveout_report_filename}")

    if giveout_report_path.exists():
        with open(giveout_report_path, 'r', encoding='utf-8') as f:
            giveout_report_data = json.load(f)

    # Загружаем данные из отчета по селлерским отправлениям (ozon_direct_flow_report)
    direct_flow_report_data = {}
    direct_flow_report_filename = f"ozon_direct_flow_report_{pvz_for_filename}_{report_date}.json"
    direct_flow_report_path = report_dir / direct_flow_report_filename

    if not direct_flow_report_path.exists():
        # Если файл с именем ПВЗ не найден, ищем файл без имени ПВЗ в названии
        for file_path in report_dir.glob(f"ozon_direct_flow_report_*_{report_date.replace('-', '')}*.json"):
            if report_date.replace('-', '') in file_path.name:
                direct_flow_report_path = file_path
                break
        else:
            # Если не найден файл с датой, ищем самый последний файл с отчетом
            direct_flow_report_files = list(report_dir.glob("ozon_direct_flow_report_*.json"))
            if direct_flow_report_files:
                direct_flow_report_path = max(direct_flow_report_files, key=lambda x: x.stat().st_mtime)
            else:
                print(f"Файл отчета по селлерским отправлениям не найден: {direct_flow_report_filename}")

    if direct_flow_report_path.exists():
        with open(direct_flow_report_path, 'r', encoding='utf-8') as f:
            direct_flow_report_data = json.load(f)

    # Объединяем данные из обоих отчетов
    combined_data = {
        'giveout_report': giveout_report_data,
        'direct_flow_report': direct_flow_report_data,
        'date': report_date,
        'pvz_info': giveout_report_data.get('pvz_info') or direct_flow_report_data.get('pvz_info', pvz_id),
        'marketplace': giveout_report_data.get('marketplace') or direct_flow_report_data.get('marketplace', 'ОЗОН')
    }

    return combined_data


def format_notification_message(report_data: Dict[str, Any]) -> str:
    """
    Форматирует сообщение для уведомления в Telegram.

    Args:
        report_data: объединенные данные отчетов из JSON

    Returns:
        str: отформатированное сообщение для Telegram
    """
    # Извлекаем основные данные из объединенного отчета
    date_str = report_data.get('date', datetime.now().strftime('%Y-%m-%d'))
    pvz_info = report_data.get('pvz_info', 'Неизвестный ПВЗ')
    marketplace = report_data.get('marketplace', 'ОЗОН')

    # Извлекаем данные из отчета по выдаче
    giveout_report = report_data.get('giveout_report', {})
    issued_packages = giveout_report.get('issued_packages', giveout_report.get('total_packages', 0))

    # Извлекаем данные из отчета по селлерским отправлениям
    direct_flow_report = report_data.get('direct_flow_report', {})
    total_items_count = direct_flow_report.get('total_items_count', 0)
    total_carriages_found = direct_flow_report.get('total_carriages_found', 0)

    # Преобразуем формат даты из YYYY-MM-DD в DD.MM.YYYY
    try:
        # Парсим дату в формате YYYY-MM-DD
        parsed_date = datetime.strptime(date_str, '%Y-%m-%d')
        # Преобразуем в формат DD.MM.YYYY
        formatted_date = parsed_date.strftime('%d.%m.%Y')
    except ValueError:
        # Если формат даты не соответствует ожидаемому, используем как есть
        formatted_date = date_str

    # Формируем сообщение для Telegram
    message = f"📊 Отчет {marketplace}\n"
    message += f"ПВЗ: {pvz_info}\n"
    message += f"Дата: {formatted_date}\n"

    # Добавляем информацию из отчета по выдаче
    message += f"Количество выдач: {issued_packages}\n"

    # Добавляем информацию из отчета по селлерским отправлениям в новом формате
    message += f"Прямые перевозки: {total_items_count} ({total_carriages_found} перевозки)"

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
        # Загружаем данные отчетов
        logger.info("Загрузка данных отчетов...")

        # Получаем PVZ_ID из конфигурации, если не указан в аргументах
        from config.base_config import PVZ_ID
        pvz_id = args.pvz_id or PVZ_ID

        report_data = load_report_data(args.report_date, pvz_id)
        logger.info(f"Данные отчетов загружены для ПВЗ {pvz_id}, дата: {report_data.get('date', 'N/A')}")

        # Проверяем, есть ли данные в каком-либо из отчетов
        giveout_report = report_data.get('giveout_report', {})
        direct_flow_report = report_data.get('direct_flow_report', {})

        if not giveout_report and not direct_flow_report:
            logger.warning("Нет данных ни в одном из отчетов")
            return

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

    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)


if __name__ == "__main__":
    main()