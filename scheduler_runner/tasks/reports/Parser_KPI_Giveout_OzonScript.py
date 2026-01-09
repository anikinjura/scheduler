"""
Parser_KPI_Giveout_OzonScript.py

Скрипт для автоматического парсинга данных о выдачах из маркетплейса ОЗОН.
Использует новую архитектуру с универсальной загрузкой данных и форматированием.

Функции:
- Парсинг веб-страницы ОЗОН для получения данных о выдачах
- Сохранение данных в JSON-файл
- Обеспечение логирования процесса
- Совместимость с системой уведомлений и загрузки в Google Sheets

Архитектура:
- Использует конфигурацию из Parser_KPI_Giveout_OzonScript_config.py
- Использует базовый класс BaseOzonParser для парсинга
- Использует универсальный модуль scheduler_runner/utils/google_sheets.py для загрузки данных
- Использует транслитерацию для кириллических имен ПВЗ при поиске файлов
- Обеспечивает уникальность записей с помощью Id столбца с формулой (для совместимости)

Author: anikinjura
Version: 3.0.0 (новая архитектура)
"""
__version__ = '1.0.0'

import argparse
import sys
import time
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# Импорты для обработки исключений Selenium
from selenium.common.exceptions import TimeoutException, NoSuchElementException


# Добавляем корень проекта в sys.path для корректного импорта
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from scheduler_runner.utils.logging import configure_logger
from scheduler_runner.tasks.reports.BaseOzonParser import BaseOzonParser
from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Giveout_OzonScript_config import SCRIPT_CONFIG


class OzonGiveoutReportParser(BaseOzonParser):
    """Парсер для получения данных о выдачах из ERP-системы ОЗОН"""

    def __init__(self, config, logger=None):
        super().__init__(config, logger)

    def navigate_to_reports(self):
        """Навигация к странице отчета о выдачах ОЗОН"""
        # В данном случае навигация уже выполнена в login(), так как мы переходим сразу к нужной странице
        if self.logger:
            self.logger.info("Навигация к отчету о выдачах выполнена")

    def get_report_type(self) -> str:
        """Возвращает тип отчета"""
        return 'giveout'


    def extract_data(self) -> Dict[str, Any]:
        """Извлечение данных о выдачах из ERP-системы ОЗОН"""
        # Извлекаем специфичные данные
        specific_data = self.extract_specific_data()

        # Обрабатываем общую структуру данных с учетом специфичных данных
        processed_data = self.process_report_data(specific_data)

        return processed_data

    def extract_specific_data(self) -> Dict[str, Any]:
        """Извлекает специфичные данные для отчета о выдачах"""
        from selenium.webdriver.common.by import By
        import re

        try:
            # Извлекаем количество выданных посылок
            issued_packages = self._extract_issued_packages()

            return {
                'issued_packages': issued_packages,
            }
        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при извлечении специфичных данных: {e}")
            return {
                'issued_packages': 0,
            }

    def get_report_schema(self) -> Dict[str, Any]:
        """Возвращает схему данных отчета о выдачах"""
        from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Giveout_OzonScript_config import REPORT_DATA_SCHEMA
        return REPORT_DATA_SCHEMA

    def _extract_pvz_info(self) -> str:
        """Извлечение информации о пункте выдачи с использованием ключевых слов из конфигурации"""
        from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Giveout_OzonScript_config import SCRIPT_CONFIG
        pvz_keywords = SCRIPT_CONFIG.get('PVZ_KEYWORDS')
        return super()._extract_pvz_info(pvz_keywords)

    def _extract_issued_packages(self) -> int:
        """Извлечение количества выданных посылок"""
        # Используем универсальный метод из базового класса
        return self.extract_number_by_selector('GIVEOUT_COUNT', wait_time=2)


def parse_arguments() -> argparse.Namespace:
    """
    Парсит аргументы командной строки для скрипта парсинга выдач ОЗОН.

    --detailed_logs              - включить детализированные логи
    --date                       - дата для парсинга в формате YYYY-MM-DD (по умолчанию - сегодня)
    """
    parser = argparse.ArgumentParser(
        description="Парсинг данных о выдачах из ERP-системы ОЗОН",
        epilog="Пример: python Parser_KPI_Giveout_OzonScript.py --detailed_logs --date 2026-01-01"
    )
    parser.add_argument(
        "--detailed_logs",
        action="store_true",
        default=False,
        help="Включить детализированные логи"
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Дата для парсинга в формате YYYY-MM-DD (по умолчанию - сегодня)"
    )
    return parser.parse_args()


def main():
    """Основная функция скрипта для запуска из командной строки"""
    from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Giveout_OzonScript_config import (
        ERP_URL_TEMPLATE,
        DATE_FORMAT,
        FILE_PATTERN
    )

    # Создаем экземпляр парсера
    parser = OzonGiveoutReportParser(SCRIPT_CONFIG)

    # Вызываем общий метод из базового класса
    parser.run_parser_with_params(
        ERP_URL_TEMPLATE=ERP_URL_TEMPLATE,
        DATE_FORMAT=DATE_FORMAT,
        FILE_PATTERN=FILE_PATTERN,
        USER=SCRIPT_CONFIG["USER"],
        TASK_NAME=SCRIPT_CONFIG["TASK_NAME"],
        OUTPUT_DIR=SCRIPT_CONFIG['OUTPUT_DIR'],
        PVZ_ID=SCRIPT_CONFIG['PVZ_ID'],
        DETAILED_LOGS=SCRIPT_CONFIG.get("DETAILED_LOGS", False)
    )


if __name__ == "__main__":
    main()