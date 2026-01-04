"""
GoogleSheetsUploadScript.py

Скрипт для автоматической отправки данных отчетов ОЗОН в Google-таблицу для домена (задачи) reports.

Функции:
- Загрузка JSON-файла с отчетом из директории reports
- Форматирование данных отчета для соответствия структуре Google-таблицы
- Преобразование формата даты из YYYY-MM-DD в DD.MM.YYYY
- Использование транслитерации для кириллических имен ПВЗ при поиске файлов
- Подключение к Google-таблице через GoogleSheetsReporter
- Загрузка отформатированных данных в таблицу
- Обеспечение логирования процесса

Архитектура:
- Все параметры задаются в config/scripts/GoogleSheetsUploadScript_config.py.
- Использует универсальный модуль scheduler_runner/utils/google_sheets.py для работы с Google Sheets.
- Использует транслитерацию для кириллических имен ПВЗ при поиске файлов.
- Обеспечивает уникальность записей с помощью Id столбца с формулой.

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

from scheduler_runner.utils.google_sheets import GoogleSheetsReporter
from scheduler_runner.tasks.reports.config.scripts.GoogleSheetsUploadScript_config import SCRIPT_CONFIG
from scheduler_runner.utils.logging import configure_logger
from scheduler_runner.tasks.reports.config.reports_paths import REPORTS_PATHS


def parse_arguments() -> argparse.Namespace:
    """
    Парсит аргументы командной строки для скрипта загрузки в Google Sheets.

    --report_date              - дата отчета в формате YYYY-MM-DD (по умолчанию сегодняшняя дата)
    --detailed_logs            - включить детализированные логи
    --pvz_id                   - идентификатор ПВЗ для загрузки отчета
    """
    parser = argparse.ArgumentParser(
        description="Скрипт для отправки данных отчетов ОЗОН в Google-таблицу",
        epilog="Пример: python GoogleSheetsUploadScript.py --report_date 2026-01-02 --detailed_logs"
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
        help="Идентификатор ПВЗ для загрузки отчета"
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
    from scheduler_runner.tasks.reports.utils.reports_utils import load_combined_report_data
    return load_combined_report_data(report_date, pvz_id)


def format_report_data_for_sheets(report_data: Dict[str, Any], pvz_id: str) -> Dict[str, Any]:
    """
    Форматирует данные отчета для записи в Google-таблицу.

    Args:
        report_data: объединенные данные отчетов из JSON
        pvz_id: идентификатор ПВЗ

    Returns:
        Dict[str, Any]: отформатированные данные для Google-таблицы
    """
    # Извлекаем основные данные из объединенного отчета
    date_str = report_data.get('date', datetime.now().strftime('%Y-%m-%d'))
    pvz_info = report_data.get('pvz_info', pvz_id)  # используем pvz_info из отчета, если доступно

    # Извлекаем данные из отчета по выдаче
    giveout_report = report_data.get('giveout_report', {})
    issued_packages = giveout_report.get('issued_packages', giveout_report.get('total_packages', 0))

    # Извлекаем данные из отчета по селлерским отправлениям (старый формат)
    direct_flow_report = report_data.get('direct_flow_report', {})
    total_items_count = direct_flow_report.get('total_items_count', 0)  # общее количество отправлений

    # Извлекаем данные из нового отчета по перевозкам (новый формат)
    carriages_report = report_data.get('carriages_report', {})
    direct_flow_data = carriages_report.get('direct_flow', {})
    return_flow_data = carriages_report.get('return_flow', {})

    # Если есть данные в новом формате, используем их
    direct_flow_count = total_items_count  # по умолчанию используем старые данные
    return_flow_count = 0  # по умолчанию 0

    if direct_flow_data:
        direct_flow_count = direct_flow_data.get('total_items_count', total_items_count)

    if return_flow_data:
        return_flow_count = return_flow_data.get('total_items_count', 0)

    # Преобразуем формат даты из YYYY-MM-DD в DD.MM.YYYY для российского формата
    try:
        # Парсим дату в формате YYYY-MM-DD
        parsed_date = datetime.strptime(date_str, '%Y-%m-%d')
        # Преобразуем в формат DD.MM.YYYY
        formatted_date = parsed_date.strftime('%d.%m.%Y')
    except ValueError:
        # Если формат даты не соответствует ожидаемому, используем как есть
        formatted_date = date_str

    # Формируем структуру данных для Google-таблицы в соответствии с новой структурой
    # Id будет вычислен формулой в таблице, поэтому оставляем его пустым
    formatted_data = {
        "id": "",  # будет заполнен формулой в таблице
        "Дата": formatted_date,
        "ПВЗ": pvz_info,  # используем информацию из отчета
        "Количество выдач": issued_packages,  # используем количество выданных посылок из отчета по выдаче
        "Прямой поток": direct_flow_count,  # используем данные о прямых перевозках
        "Возвратный поток": return_flow_count  # используем данные о возвратных перевозках
    }

    return formatted_data


def main() -> None:
    """Основная функция управления процессом отправки данных в Google-таблицу"""
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
        carriages_report = report_data.get('carriages_report', {})

        # Проверяем, есть ли какие-либо данные в отчетах
        if not giveout_report and not direct_flow_report and not carriages_report:
            logger.warning("Нет данных ни в одном из отчетов")
            return

        # Форматируем данные для Google-таблицы
        formatted_data = format_report_data_for_sheets(report_data, pvz_id)
        logger.info("Данные отформатированы для записи в Google-таблицу")

        # Подключаемся к Google-таблице
        logger.info("Подключение к Google-таблице...")
        reporter = GoogleSheetsReporter(
            credentials_path=SCRIPT_CONFIG["CREDENTIALS_PATH"],
            spreadsheet_name=SCRIPT_CONFIG["SPREADSHEET_NAME"],
            worksheet_name=SCRIPT_CONFIG["WORKSHEET_NAME"]
        )

        # Проверяем структуру данных
        if not reporter.validate_data_structure(formatted_data, SCRIPT_CONFIG["REQUIRED_HEADERS"]):
            logger.error("Структура данных не соответствует требованиям таблицы")
            return

        # Отправляем данные в Google-таблицу
        logger.info("Отправка данных в Google-таблицу...")
        success = reporter.update_or_append_data(formatted_data, date_key="Дата", pvz_key="ПВЗ")

        if success:
            logger.info("Данные успешно отправлены в Google-таблицу")
        else:
            logger.error("Ошибка при отправке данных в Google-таблицу")

    except FileNotFoundError as e:
        logger.error(f"Файл отчета не найден: {e}")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)


if __name__ == "__main__":
    main()