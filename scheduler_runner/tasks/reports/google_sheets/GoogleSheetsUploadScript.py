"""
GoogleSheetsUploadScript.py

Скрипт для автоматической отправки данных отчетов ОЗОН в Google-таблицу.

- Загружает JSON-файл с отчетом
- Подключается к Google-таблице
- Добавляет данные в следующую строку после последней заполненной
- Обеспечивает логирование процесса

Author: anikinjura
"""
__version__ = '0.0.1'

import argparse
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

from scheduler_runner.tasks.reports.google_sheets.GoogleSheetsReporter import GoogleSheetsReporter
from scheduler_runner.tasks.reports.google_sheets.config.google_sheets_config import GOOGLE_SHEETS_CONFIG
from scheduler_runner.utils.logging import configure_logger
from scheduler_runner.tasks.reports.ozon.config.reports_paths import REPORTS_PATHS


def parse_arguments() -> argparse.Namespace:
    """Парсинг аргументов командной строки"""
    parser = argparse.ArgumentParser(
        description="Скрипт для отправки данных отчетов ОЗОН в Google-таблицу"
    )
    parser.add_argument(
        "--report_date",
        type=str,
        help="Дата отчета в формате YYYY-MM-DD (по умолчанию сегодняшняя дата)"
    )
    parser.add_argument(
        "--detailed_logs",
        action="store_true",
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
    
    report_filename = f"ozon_giveout_report_{pvz_id}_{report_date}.json"
    report_path = REPORTS_PATHS["REPORTS_DIR"] / report_filename
    
    if not report_path.exists():
        raise FileNotFoundError(f"Файл отчета не найден: {report_path}")
    
    with open(report_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def format_report_data_for_sheets(report_data: Dict[str, Any], pvz_id: str) -> Dict[str, Any]:
    """
    Форматирует данные отчета для записи в Google-таблицу.
    
    Args:
        report_data: данные отчета из JSON
        pvz_id: идентификатор ПВЗ
        
    Returns:
        Dict[str, Any]: отформатированные данные для Google-таблицы
    """
    # Извлекаем основные данные из отчета
    date_str = report_data.get('date', datetime.now().strftime('%Y-%m-%d'))
    giveout_count = report_data.get('giveout_count', 0)
    giveout_percentage = report_data.get('giveout_percentage', 0)
    
    # Формируем структуру данных для Google-таблицы
    formatted_data = {
        "Дата": date_str,
        "ПВЗ": pvz_id,
        "Количество выданных": giveout_count,
        "Процент выполнения": f"{giveout_percentage}%",
        "Комментарии": report_data.get('comments', '')
    }
    
    return formatted_data


def main() -> None:
    """Основная функция управления процессом отправки данных в Google-таблицу"""
    args = parse_arguments()
    
    # Настройка логгера
    logger = configure_logger(
        user=GOOGLE_SHEETS_CONFIG["USER"],
        task_name=GOOGLE_SHEETS_CONFIG["TASK_NAME"],
        detailed=args.detailed_logs or GOOGLE_SHEETS_CONFIG["DETAILED_LOGS"]
    )
    
    try:
        # Загружаем данные отчета
        logger.info("Загрузка данных отчета...")
        
        # Получаем PVZ_ID из конфигурации, если не указан в аргументах
        from config.base_config import PVZ_CONFIG
        pvz_id = args.pvz_id or PVZ_CONFIG.get('PVZ_ID', 'UNKNOWN')
        
        report_data = load_report_data(args.report_date, pvz_id)
        logger.info(f"Данные отчета загружены для ПВЗ {pvz_id}, дата: {report_data.get('date', 'N/A')}")
        
        # Форматируем данные для Google-таблицы
        formatted_data = format_report_data_for_sheets(report_data, pvz_id)
        logger.info("Данные отформатированы для записи в Google-таблицу")
        
        # Подключаемся к Google-таблице
        logger.info("Подключение к Google-таблице...")
        reporter = GoogleSheetsReporter(
            credentials_path=GOOGLE_SHEETS_CONFIG["CREDENTIALS_PATH"],
            spreadsheet_name=GOOGLE_SHEETS_CONFIG["SPREADSHEET_NAME"],
            worksheet_name=GOOGLE_SHEETS_CONFIG["WORKSHEET_NAME"]
        )
        
        # Проверяем структуру данных
        if not reporter.validate_data_structure(formatted_data, GOOGLE_SHEETS_CONFIG["REQUIRED_HEADERS"]):
            logger.error("Структура данных не соответствует требованиям таблицы")
            return
        
        # Отправляем данные в Google-таблицу
        logger.info("Отправка данных в Google-таблицу...")
        success = reporter.update_or_append_data(formatted_data, date_key="Дата")
        
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