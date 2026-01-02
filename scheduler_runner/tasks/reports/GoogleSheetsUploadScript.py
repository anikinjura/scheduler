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

    # Извлекаем данные из отчета по селлерским отправлениям
    direct_flow_report = report_data.get('direct_flow_report', {})
    total_items_count = direct_flow_report.get('total_items_count', 0)  # общее количество отправлений

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
        "Количество выдач": issued_packages,  # используем количество выданных посылок из отчета по выдаче
        "Селлер (FBS)": total_items_count,  # используем общее количество отправлений из отчета по селлерским отправлениям
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