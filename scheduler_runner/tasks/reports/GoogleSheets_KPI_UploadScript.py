"""
GoogleSheets_KPI_UploadScript.py

Скрипт для автоматической отправки KPI данных отчетов ОЗОН в Google-таблицу.
Использует новую архитектуру с универсальной загрузкой данных и трансформерами.

Функции:
- Загрузка данных отчетов через универсальную утилиту load_reports_data
- Преобразование данных через GoogleSheetsTransformer
- Запись данных в Google-таблицу через GoogleSheetsReporter
- Обеспечение логирования процесса

Архитектура:
- Использует конфигурацию из GoogleSheets_KPI_UploadScript_config.py
- Использует универсальный модуль scheduler_runner/utils/google_sheets.py для работы с Google Sheets
- Использует новую систему загрузки данных из scheduler_runner/tasks/reports/utils/load_reports_data.py
- Использует трансформеры данных из scheduler_runner/tasks/reports/utils/data_transformers.py

Author: anikinjura
Version: 3.0.0 (новая архитектура)
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# Добавляем корень проекта в sys.path для корректного импорта
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from scheduler_runner.utils.google_sheets import GoogleSheetsReporter
from scheduler_runner.tasks.reports.config.scripts.GoogleSheets_KPI_UploadScript_config import SCRIPT_CONFIG
from scheduler_runner.utils.logging import configure_logger
from scheduler_runner.tasks.reports.utils.load_reports_data import load_reports_data
from scheduler_runner.tasks.reports.utils.data_transformers import GoogleSheetsTransformer


def parse_arguments() -> argparse.Namespace:
    """
    Парсит аргументы командной строки для скрипта загрузки KPI в Google Sheets.

    --report_date              - дата отчета в формате YYYY-MM-DD (по умолчанию сегодняшняя дата)
    --detailed_logs            - включить детализированные логи
    --pvz_id                   - идентификатор ПВЗ для загрузки отчета
    """
    parser = argparse.ArgumentParser(
        description="Скрипт для отправки KPI данных отчетов ОЗОН в Google-таблицу",
        epilog="Пример: python GoogleSheets_KPI_UploadScript.py --report_date 2026-01-02 --detailed_logs"
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


def load_kpi_report_data(report_date: str, pvz_id: str) -> Dict[str, Any]:
    """
    Загружает KPI данные отчетов через универсальную утилиту.

    Args:
        report_date: дата отчета в формате YYYY-MM-DD
        pvz_id: идентификатор ПВЗ

    Returns:
        Dict[str, Any]: объединенные данные отчетов в универсальном формате
    """
    from scheduler_runner.tasks.reports.config.scripts.GoogleSheets_KPI_UploadScript_config import SCRIPT_CONFIG
    
    logger = configure_logger(
        user=SCRIPT_CONFIG["USER"],
        task_name=SCRIPT_CONFIG["TASK_NAME"],
        detailed=False  # логирование здесь не нужно, будет в основном скрипте
    )
    
    logger.info(f"Загрузка KPI данных отчетов за {report_date} для ПВЗ {pvz_id}")
    
    try:
        # Загружаем данные через универсальную утилиту
        raw_data = load_reports_data(
            report_date=report_date,
            pvz_id=pvz_id,
            config=SCRIPT_CONFIG["REPORT_CONFIGS"]
        )
        
        logger.info(f"Данные отчетов загружены: {bool(raw_data)}")
        if raw_data:
            logger.info(f"Ключи данных: {list(raw_data.keys())[:10]}...")  # первые 10 ключей
        
        return raw_data
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных отчетов: {e}")
        raise


def transform_kpi_data_for_sheets(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Преобразует универсальные данные в формат для Google Sheets.

    Args:
        raw_data: объединенные данные отчетов из универсальной утилиты

    Returns:
        Dict[str, Any]: отформатированные данные для Google-таблицы
    """
    logger = configure_logger(
        user=SCRIPT_CONFIG["USER"],
        task_name=SCRIPT_CONFIG["TASK_NAME"],
        detailed=False
    )
    
    logger.info("Преобразование данных для Google Sheets...")
    
    try:
        # Создаем трансформер
        transformer = GoogleSheetsTransformer()
        
        # Преобразуем данные
        sheets_data = transformer.transform(raw_data)
        
        logger.info(f"Данные преобразованы: {bool(sheets_data)}")
        if sheets_data:
            logger.info(f"Поля для Google Sheets: {list(sheets_data.keys())}")
        
        return sheets_data
        
    except Exception as e:
        logger.error(f"Ошибка при преобразовании данных: {e}")
        raise


def main() -> None:
    """Основная функция управления процессом отправки KPI данных в Google-таблицу"""
    args = parse_arguments()

    # Настройка логгера
    logger = configure_logger(
        user=SCRIPT_CONFIG["USER"],
        task_name=SCRIPT_CONFIG["TASK_NAME"],
        detailed=args.detailed_logs or SCRIPT_CONFIG["DETAILED_LOGS"]
    )

    try:
        # Получаем PVZ_ID из конфигурации, если не указан в аргументах
        from config.base_config import PVZ_ID
        pvz_id = args.pvz_id or PVZ_ID

        # Загружаем данные отчетов через новую архитектуру
        logger.info("Загрузка KPI данных отчетов...")
        raw_data = load_kpi_report_data(args.report_date, pvz_id)
        
        if not raw_data:
            logger.warning("Нет данных для загрузки в Google-таблицу")
            return

        # Преобразуем данные для Google Sheets
        logger.info("Преобразование данных для Google-таблицы...")
        sheets_data = transform_kpi_data_for_sheets(raw_data)
        
        if not sheets_data:
            logger.error("Не удалось преобразовать данные для Google Sheets")
            return

        # Подключаемся к Google-таблице через новую архитектуру
        logger.info("Подключение к Google-таблице...")
        reporter = GoogleSheetsReporter(
            credentials_path=SCRIPT_CONFIG["CREDENTIALS_PATH"],
            spreadsheet_name=SCRIPT_CONFIG["SPREADSHEET_NAME"],
            worksheet_name=SCRIPT_CONFIG["WORKSHEET_NAME"]
        )

        # Отправляем данные в Google-таблицу через новый API
        logger.info("Отправка данных в Google-таблицу...")
        result = reporter.update_or_append_data_with_config(
            data=sheets_data,
            config=SCRIPT_CONFIG["TABLE_CONFIG"]
        )

        if result.get('success'):
            logger.info(f"Данные успешно отправлены в Google-таблицу: {result}")
        else:
            logger.error(f"Ошибка при отправке данных в Google-таблицу: {result}")

    except FileNotFoundError as e:
        logger.error(f"Файл отчета не найден: {e}")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)


if __name__ == "__main__":
    main()