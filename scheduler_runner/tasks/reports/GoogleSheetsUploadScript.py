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
    issued_packages = report_data.get('issued_packages', 0)  # используем issued_packages из отчета ОЗОН
    pvz_info = report_data.get('pvz_info', pvz_id)  # используем pvz_info из отчета, если доступно

    # Преобразуем формат даты из YYYY-MM-DD в DD.MM.YYYY для российского формата
    try:
        # Парсим дату в формате YYYY-MM-DD
        parsed_date = datetime.strptime(date_str, '%Y-%m-%d')
        # Преобразуем в формат DD.MM.YYYY
        formatted_date = parsed_date.strftime('%d.%m.%Y')
    except ValueError:
        # Если формат даты не соответствует ожидаемому, используем как есть
        formatted_date = date_str

    # Формируем структуру данных для Google-таблицы в соответствии с реальной структурой листа KPI
    # Id будет вычислен формулой в таблице, поэтому оставляем его пустым
    formatted_data = {
        "id": "",  # будет заполнен формулой в таблице
        "Дата": formatted_date,
        "ПВЗ": pvz_info,  # используем информацию из отчета
        "Количество выдач": issued_packages,  # используем количество выданных посылок
        "Селлер (FBS)": "",  # может быть заполнен позже, если нужна дополнительная информация
        "Обработано возвратов": ""  # может быть заполнен позже, если нужна дополнительная информация
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
        # Загружаем данные отчета
        logger.info("Загрузка данных отчета...")

        # Получаем PVZ_ID из конфигурации, если не указан в аргументах
        from config.base_config import PVZ_ID
        pvz_id = args.pvz_id or PVZ_ID

        report_data = load_report_data(args.report_date, pvz_id)
        logger.info(f"Данные отчета загружены для ПВЗ {pvz_id}, дата: {report_data.get('date', 'N/A')}")

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