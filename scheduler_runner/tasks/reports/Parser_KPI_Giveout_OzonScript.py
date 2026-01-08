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

# Модульные константы для магических строк
LOGIN_INDICATORS = ['login', 'signin', 'auth']

# Добавляем корень проекта в sys.path для корректного импорта
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from scheduler_runner.utils.logging import configure_logger
from scheduler_runner.tasks.reports.BaseOzonParser import BaseOzonParser
from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Giveout_OzonScript_config import SCRIPT_CONFIG


class OzonGiveoutReportParser(BaseOzonParser):
    """Парсер для получения данных о выдачах из ERP-системы ОЗОН"""

    def __init__(self, config, logger=None):
        super().__init__(config, logger)

    def login(self):
        """Вход в ERP-систему ОЗОН"""
        # Заходим на страницу с базовым URL
        self.driver.get(self.config['ERP_URL'])
        if self.logger:
            self.logger.info(f"Переход на страницу: {self.config['ERP_URL']}")

    def navigate_to_reports(self):
        """Навигация к странице отчета о выдачах ОЗОН"""
        # В данном случае навигация уже выполнена в login(), так как мы переходим сразу к нужной странице
        if self.logger:
            self.logger.info("Навигация к отчету о выдачах выполнена")

    def get_report_type(self) -> str:
        """Возвращает тип отчета"""
        return 'giveout'

    def get_default_selectors(self) -> Dict[str, str]:
        """Возвращает селекторы по умолчанию для данного типа отчета"""
        return self.config.get('SELECTORS', {})

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


    def _extract_issued_packages(self) -> int:
        """Извлечение количества выданных посылок"""
        # Используем универсальный метод из базового класса
        return self.extract_number_by_selector('GIVEOUT_COUNT', wait_time=2)

    def logout(self):
        """Выход из системы (обычно не требуется при использовании существующей сессии)"""
        pass


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
    try:
        # 1. Парсинг аргументов командной строки
        args = parse_arguments()
        detailed_logs = args.detailed_logs or SCRIPT_CONFIG.get("DETAILED_LOGS", False)

        # Получаем дату из аргументов или используем текущую
        target_date = args.date
        if target_date is None:
            from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Giveout_OzonScript_config import DATE_FORMAT
            target_date = datetime.now().strftime(DATE_FORMAT)

        # 2. Настройка логирования
        logger = configure_logger(
            user=SCRIPT_CONFIG["USER"],
            task_name=SCRIPT_CONFIG["TASK_NAME"],
            detailed=detailed_logs
        )

        # 3. Логирование начала процесса
        logger.info(f"Запуск парсинга данных о выдачах ERP-системы ОЗОН за дату: {target_date}")

        # 4. Создание копии конфигурации с обновленным URL для указанной даты
        from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Giveout_OzonScript_config import ERP_URL_TEMPLATE

        # Формируем готовый URL, подставив дату в шаблон
        erp_url = ERP_URL_TEMPLATE.format(date=target_date)

        # Создаем копию конфигурации и обновляем только URL
        script_config = SCRIPT_CONFIG.copy()
        script_config["ERP_URL"] = erp_url

        # 5. Создание экземпляра парсера
        parser = OzonGiveoutReportParser(script_config, logger)

        # 6. Настройка драйвера
        try:
            parser.setup_driver() # setup_driver() определена в базовом классе BaseOzonParser, создает и настраивает экземпляр браузера, готовый к работе с ERP-системой Ozon.

            # 7. Выполнение основных операций
            parser.login()
            parser.navigate_to_reports()

            # Извлечение данных о выдачах
            data = parser.extract_data()

            parser.logout()

            # 8. Сохранение данных
            output_dir = Path(script_config['OUTPUT_DIR'])
            output_dir.mkdir(parents=True, exist_ok=True)

            # Формируем имя файла с использованием шаблона из конфигурации
            date_str = target_date.replace('-', '')  # Преобразуем формат даты для имени файла
            pvz_id = data.get('pvz_info', script_config['PVZ_ID'])
            # Транслитерируем ПВЗ для использования в имени файла
            from scheduler_runner.utils.system import SystemUtils
            translit_pvz = SystemUtils.cyrillic_to_translit(pvz_id) if pvz_id else 'unknown'

            # Используем шаблон из конфигурации для формирования имени файла
            from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Giveout_OzonScript_config import FILE_PATTERN
            filename_template = FILE_PATTERN.replace('{pvz_id}', translit_pvz).replace('{date}', date_str)
            filename = output_dir / filename_template

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)

            logger.info(f"Отчет о выдачах ОЗОН успешно сохранен в {filename}")
            logger.info(f"Извлеченные данные: {data}")
        finally:
            # 9. Завершение работы
            parser.close()

    except Exception as e:
        # 10. Обработка исключений
        import traceback
        logger.error(f"Ошибка при парсинге данных о выдачах ERP-системы ОЗОН: {e}")
        logger.error(f"Полный стек трейса: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()