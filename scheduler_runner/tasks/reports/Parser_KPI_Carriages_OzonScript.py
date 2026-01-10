"""
Parser_KPI_Carriages_OzonScript.py

Скрипт для автоматического парсинга данных о перевозках из маркетплейса ОЗОН.
Использует новую архитектуру с универсальной загрузкой данных и форматированием.

Функции:
- Парсинг веб-страницы ОЗОН для получения данных о перевозках
- Сохранение данных в JSON-файл
- Обеспечение логирования процесса
- Совместимость с системой уведомлений и загрузки в Google Sheets

Архитектура:
- Использует конфигурацию из Parser_KPI_Carriages_OzonScript_config.py
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
from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Carriages_OzonScript_config import SCRIPT_CONFIG


class OzonCarriagesReportParser(BaseOzonParser):
    """Парсер для получения данных о перевозках из ERP-системы ОЗОН"""

    def __init__(self, config, logger=None):
        super().__init__(config)
        self.logger = logger

    def get_report_type(self) -> str:
        """Возвращает тип отчета"""
        return 'carriages'

    def extract_specific_data(self) -> Dict[str, Any]:
        """Извлекает специфичные данные для отчета о перевозках"""
        # Этот метод не используется в данном классе, так как extract_data принимает параметр flow_type
        # Вместо этого, мы реализуем логику в extract_data с параметром flow_type
        # Возвращаем пустой словарь, так как основная логика обрабатывается в extract_data
        return {}

    def get_report_schema(self) -> Dict[str, Any]:
        """Возвращает схему данных отчета"""
        from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Carriages_OzonScript_config import REPORT_DATA_SCHEMA
        return REPORT_DATA_SCHEMA

    def _extract_pvz_info(self) -> str:
        """Извлечение информации о пункте выдачи с использованием ключевых слов из конфигурации"""
        from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Carriages_OzonScript_config import SCRIPT_CONFIG
        pvz_keywords = SCRIPT_CONFIG.get('PVZ_KEYWORDS')
        return super()._extract_pvz_info(pvz_keywords)

    def navigate_to_reports(self):
        """Навигация к странице отчета о выдачах ОЗОН"""
        # В данном случае навигация уже выполнена в login(), так как мы переходим сразу к нужной странице
        if self.logger:
            self.logger.info("Навигация к отчету о выдачах выполнена")

    def extract_data(self, flow_type: str = None) -> Dict[str, Any]:
        """Извлечение данных о перевозках из ERP-системы ОЗОН"""
        if flow_type is None:
            flow_type = self.FLOW_TYPE_UNKNOWN
        from selenium.webdriver.common.by import By
        import time
        from urllib.parse import unquote

        if self.logger:
            self.logger.info(f"Текущий URL: {self.driver.current_url}")
            self.logger.info(f"Заголовок страницы: {self.driver.title}")

        # Проверяем статус авторизации
        is_logged_in, error_response = self._check_authorization_status()
        if not is_logged_in:
            return error_response
        else:
            # Ждем полной загрузки страницы
            time.sleep(3)

            # Пытаемся установить правильный пункт выдачи
            self._try_set_pvz()

            # Извлечение базовой информации
            try:
                # Получаем дату из текущего URL
                current_url_encoded = self.driver.current_url  # закодированный URL
                current_url = unquote(current_url_encoded)  # декодированный URL
                import re
                # Ищем дату в формате startSentMoment или endSentMoment
                date_match = re.search(r'(?:startSentMoment|endSentMoment)%22:%22(\d{4}-\d{2}-\d{2})', current_url_encoded)
                report_date = date_match.group(1) if date_match else datetime.now().strftime('%Y-%m-%d')

                # Извлекаем информацию о ПВЗ с помощью вспомогательного метода
                pvz_info = self._extract_pvz_info()

                # Определяем тип перевозки из закодированного URL
                if flow_type == self.FLOW_TYPE_UNKNOWN:
                    if "flowType%22:%22Direct%22" in current_url_encoded:
                        flow_type = self.FLOW_TYPE_DIRECT
                    elif "flowType%22:%22Return%22" in current_url_encoded:
                        flow_type = self.FLOW_TYPE_RETURN

                # Обработка конкретного типа перевозок
                if self.logger:
                    self.logger.info(f"Начинаем обработку {flow_type.lower()} перевозок")
                flow_data = self.process_flow_type(flow_type, report_date)

                # Формируем итоговые данные
                # Формируем структуру данных с использованием REPORT_DATA_SCHEMA из конфигурации
                from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Carriages_OzonScript_config import REPORT_DATA_SCHEMA

                # Подготавливаем все возможные значения для подстановки
                all_values = {
                    'date': report_date,
                    'timestamp': datetime.now().isoformat(),
                    'page_title': self.driver.title,
                    'current_url': self.driver.current_url,
                    'direct_flow_type': self.FLOW_TYPE_DIRECT if flow_type == self.FLOW_TYPE_DIRECT else '',
                    'return_flow_type': self.FLOW_TYPE_RETURN if flow_type == self.FLOW_TYPE_RETURN else '',
                    'total_direct_carriages': flow_data['total_carriages_found'] if flow_type == self.FLOW_TYPE_DIRECT else 0,
                    'total_return_carriages': flow_data['total_carriages_found'] if flow_type == self.FLOW_TYPE_RETURN else 0,
                    'direct_carriage_numbers': flow_data['carriage_numbers'] if flow_type == self.FLOW_TYPE_DIRECT else [],
                    'return_carriage_numbers': flow_data['carriage_numbers'] if flow_type == self.FLOW_TYPE_RETURN else [],
                    'direct_carriage_details': flow_data['carriage_details'] if flow_type == self.FLOW_TYPE_DIRECT else [],
                    'return_carriage_details': flow_data['carriage_details'] if flow_type == self.FLOW_TYPE_RETURN else [],
                    'total_direct_items': flow_data['total_items_count'] if flow_type == self.FLOW_TYPE_DIRECT else 0,
                    'total_return_items': flow_data['total_items_count'] if flow_type == self.FLOW_TYPE_RETURN else 0,
                    'pvz_info': pvz_info,
                    'page_source_length': len(self.driver.page_source),
                    'page_text_length': len(self.driver.find_element(By.TAG_NAME, "body").text)
                }

                # Формируем структуру данных с подстановкой значений
                raw_data = self._substitute_values_in_schema(REPORT_DATA_SCHEMA, all_values)

                # Используем raw_data как итоговую структуру, так как она уже сформирована по шаблону
                data = raw_data

                if self.logger:
                    self.logger.info(f"Информация о ПВЗ: {pvz_info}")
                    self.logger.info(f"{flow_type} поток - найдено перевозок: {flow_data['total_carriages_found']}, всего отправлений: {flow_data['total_items_count']}")

                return data
            except NoSuchElementException as e:
                if self.logger:
                    self.logger.error(f"Не найден элемент на странице: {e}")
                # Получаем значения по умолчанию из REPORT_DATA_SCHEMA
                from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Carriages_OzonScript_config import REPORT_DATA_SCHEMA
                default_marketplace = REPORT_DATA_SCHEMA.get('marketplace', 'Ozon')
                default_report_type = REPORT_DATA_SCHEMA.get('report_type', 'carriages')

                return {
                    'marketplace': default_marketplace,
                    'report_type': default_report_type,
                    'date': datetime.now().strftime('%Y-%m-%d'),
                    'timestamp': datetime.now().isoformat(),
                    'error': f'Element not found: {str(e)}',
                    'current_url': self.driver.current_url,
                    'page_title': self.driver.title,
                    'direct_flow': {
                        'flow_type': self.FLOW_TYPE_UNKNOWN,
                        'total_carriages_found': 0,
                        'carriage_numbers': [],
                        'carriage_details': [],
                        'total_items_count': 0
                    },
                    'return_flow': {
                        'flow_type': self.FLOW_TYPE_UNKNOWN,
                        'total_carriages_found': 0,
                        'carriage_numbers': [],
                        'carriage_details': [],
                        'total_items_count': 0
                    },
                    'pvz_info': '',
                }
            except TimeoutException as e:
                if self.logger:
                    self.logger.error(f"Таймаут ожидания элемента: {e}")
                # Получаем значения по умолчанию из REPORT_DATA_SCHEMA
                from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Carriages_OzonScript_config import REPORT_DATA_SCHEMA
                default_marketplace = REPORT_DATA_SCHEMA.get('marketplace', 'Ozon')
                default_report_type = REPORT_DATA_SCHEMA.get('report_type', 'carriages')

                return {
                    'marketplace': default_marketplace,
                    'report_type': default_report_type,
                    'date': datetime.now().strftime('%Y-%m-%d'),
                    'timestamp': datetime.now().isoformat(),
                    'error': f'Timeout: {str(e)}',
                    'current_url': self.driver.current_url,
                    'page_title': self.driver.title,
                    'direct_flow': {
                        'flow_type': self.FLOW_TYPE_UNKNOWN,
                        'total_carriages_found': 0,
                        'carriage_numbers': [],
                        'carriage_details': [],
                        'total_items_count': 0
                    },
                    'return_flow': {
                        'flow_type': self.FLOW_TYPE_UNKNOWN,
                        'total_carriages_found': 0,
                        'carriage_numbers': [],
                        'carriage_details': [],
                        'total_items_count': 0
                    },
                    'pvz_info': '',
                }
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Неожиданная ошибка при извлечении данных: {e}")
                    import traceback
                    self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
                # Получаем значения по умолчанию из REPORT_DATA_SCHEMA
                from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Carriages_OzonScript_config import REPORT_DATA_SCHEMA
                default_marketplace = REPORT_DATA_SCHEMA.get('marketplace', 'Ozon')
                default_report_type = REPORT_DATA_SCHEMA.get('report_type', 'carriages')

                return {
                    'marketplace': default_marketplace,
                    'report_type': default_report_type,
                    'date': datetime.now().strftime('%Y-%m-%d'),
                    'timestamp': datetime.now().isoformat(),
                    'error': f'Error extracting data: {str(e)}',
                    'current_url': self.driver.current_url,
                    'page_title': self.driver.title,
                    'direct_flow': {
                        'flow_type': self.FLOW_TYPE_UNKNOWN,
                        'total_carriages_found': 0,
                        'carriage_numbers': [],
                        'carriage_details': [],
                        'total_items_count': 0
                    },
                    'return_flow': {
                        'flow_type': self.FLOW_TYPE_UNKNOWN,
                        'total_carriages_found': 0,
                        'carriage_numbers': [],
                        'carriage_details': [],
                        'total_items_count': 0
                    },
                    'pvz_info': '',
                }

    def _check_authorization_status(self) -> tuple:
        """Проверка статуса авторизации и возврат ошибки при необходимости

        Returns:
            tuple[bool, dict]: (успешно ли авторизован, словарь ошибки если нет)
        """
        current_url = self.driver.current_url.lower()
        login_indicators = self.config.get('LOGIN_INDICATORS', ['login', 'signin', 'auth'])
        is_logged_in = not any(indicator in current_url for indicator in login_indicators)

        if not is_logged_in:
            if self.logger:
                self.logger.warning("Все еще на странице логина - сессия не активна или недостаточно прав")
            # Получаем значения по умолчанию из REPORT_DATA_SCHEMA
            from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Carriages_OzonScript_config import REPORT_DATA_SCHEMA
            default_marketplace = REPORT_DATA_SCHEMA.get('marketplace', 'Ozon')
            default_report_type = REPORT_DATA_SCHEMA.get('report_type', 'carriages')

            error_response = {
                'marketplace': default_marketplace,
                'report_type': default_report_type,
                'date': datetime.now().strftime('%Y-%m-%d'),
                'timestamp': datetime.now().isoformat(),
                'error': 'Still on login page - session not active or insufficient permissions',
                'current_url': self.driver.current_url,
                'page_title': self.driver.title,
            }
            return False, error_response

        if self.logger:
            self.logger.info("Успешно вошли в систему")
        return True, None

    def _try_set_pvz(self):
        """Попытка установки правильного ПВЗ"""
        try:
            from selenium.webdriver.common.by import By
            pvz_input = self.driver.find_element(By.XPATH, self.config['SELECTORS']['PVZ_INPUT'])

            current_value = pvz_input.get_attribute("value")
            if self.logger:
                self.logger.info(f"Текущий пункт выдачи: {current_value}")

            expected_pvz = self.config.get('PVZ_ID', '')
            if self.logger:
                self.logger.info(f"Ожидаемый пункт выдачи: {expected_pvz}")

            if current_value != expected_pvz:
                if self.logger:
                    self.logger.info(f"Текущий пункт выдачи ({current_value}) не совпадает с ожидаемым ({expected_pvz}). Пытаемся изменить...")

                original_url = self.driver.current_url
                if self.logger:
                    self.logger.info(f"Сохраненный URL до изменения: {original_url}")

                success = self.select_pvz_dropdown_option(
                    expected_pvz=expected_pvz,
                    original_url=original_url
                )

                if not success:
                    if self.logger:
                        self.logger.error(f"Не удалось установить пункт выдачи {expected_pvz}")
                        self.logger.info("Продолжаем с текущим пунктом выдачи...")
            else:
                if self.logger:
                    self.logger.info(f"Пункт выдачи уже установлен правильно: {current_value}")

        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при установке пункта выдачи: {e}")

    def process_flow_type(self, flow_type: str, report_date: str) -> Dict[str, Any]:
        """Обработка одного типа перевозок (Direct или Return)"""
        from selenium.webdriver.common.by import By
        import time
        from urllib.parse import unquote

        # Используем текущий URL, на котором мы уже находимся
        if self.logger:
            self.logger.info(f"Обрабатываем {flow_type.lower()} перевозки по текущему URL: {self.driver.current_url}")

        # Ждем загрузки страницы
        time.sleep(3)

        # Первый этап: извлечение информации о перевозках
        if self.logger:
            self.logger.info(f"Начинаем первый этап: извлечение информации о {flow_type.lower()} перевозках")

        # Ищем элемент с информацией о количестве найденных перевозок
        total_carriages_text = self.extract_ozon_element_by_xpath(self.config['SELECTORS']['TOTAL_CARRIAGES'], "textContent")
        if total_carriages_text:
            # Извлекаем число из текста "Найдено: N"
            import re
            found_pattern = self.config['FOUND_PATTERN']
            found_count_match = re.search(found_pattern, total_carriages_text)
            if found_count_match:
                total_carriages = int(found_count_match.group(1))
                if self.logger:
                    self.logger.info(f"Найдено {flow_type.lower()} перевозок: {total_carriages}")
            else:
                total_carriages = 0
                if self.logger:
                    self.logger.warning(f"Не удалось извлечь количество {flow_type.lower()} перевозок из текста")
        else:
            if self.logger:
                self.logger.warning(f"Не найден элемент с информацией о количестве {flow_type.lower()} перевозок")
            total_carriages = 0

        # Извлечение номеров перевозок из таблицы
        carriage_numbers = []
        carriage_elements = self.driver.find_elements(By.XPATH, self.config['SELECTORS']['CARRIAGE_NUMBER'])

        for element in carriage_elements:
            carriage_number = element.text.strip()
            if carriage_number:
                carriage_numbers.append(carriage_number)

        if self.logger:
            self.logger.info(f"Извлеченные номера {flow_type.lower()} перевозок: {carriage_numbers}")

        # Второй этап: обработка каждой перевозки по отдельности
        if self.logger:
            self.logger.info(f"Начинаем второй этап: обработка {len(carriage_numbers)} {flow_type.lower()} перевозок")

        carriage_details = []
        for i, carriage_number in enumerate(carriage_numbers):
            if self.logger:
                self.logger.info(f"Обрабатываем {flow_type.lower()} перевозку {i+1}/{len(carriage_numbers)}: {carriage_number}")

            # Проверка формата номера перевозки
            import re
            if not re.match(r'^[A-Za-z0-9_-]+$', carriage_number):
                if self.logger:
                    self.logger.warning(f"Неверный формат номера перевозки: {carriage_number}")
                carriage_detail = {
                    'carriage_number': carriage_number,
                    'items_count': 0,
                    'error': 'Invalid carriage number format'
                }
                carriage_details.append(carriage_detail)
                continue

            # Сохраняем оригинальный URL для возврата
            original_url = self.driver.current_url

            try:
                # Переходим на страницу с деталями конкретной перевозки
                carriage_url = f"https://turbo-pvz.ozon.ru/outbound/carriages-archive/{carriage_number}?filter=%7B%22articleState%22:%22Took%22,%22articleType%22:%22ArticlePosting%22%7D"
                if self.logger:
                    self.logger.info(f"Переходим на страницу перевозки: {carriage_url}")
                self.driver.get(carriage_url)

                # Ждем загрузки страницы
                time.sleep(3)

                # Извлекаем количество отправлений из элемента на странице перевозки
                from selenium.webdriver.common.by import By

                # Ищем элемент с информацией о количестве найденных отправлений
                total_items_text = self.extract_ozon_element_by_xpath(self.config['SELECTORS']['TOTAL_ITEMS_ON_DETAIL_PAGE'], "textContent")

                items_count = 0
                if total_items_text:
                    # Извлекаем число из текста "Найдено: N"
                    import re
                    found_pattern = self.config['FOUND_PATTERN']
                    found_count_match = re.search(found_pattern, total_items_text)
                    if found_count_match:
                        items_count = int(found_count_match.group(1))
                        if self.logger:
                            self.logger.info(f"Найдено отправлений в {flow_type.lower()} перевозке {carriage_number}: {items_count}")
                    else:
                        if self.logger:
                            self.logger.warning(f"Не удалось извлечь количество отправлений из текста: {total_items_text}")
                else:
                    if self.logger:
                        self.logger.warning(f"Не найден элемент с информацией о количестве отправлений для {flow_type.lower()} перевозки {carriage_number}")

                # Возвращаемся на страницу с типом перевозок
                self.driver.get(original_url)
                time.sleep(1)  # Ждем возврата

                # Формируем детали для этой перевозки
                carriage_detail = {
                    'carriage_number': carriage_number,
                    'items_count': items_count,  # Количество отправлений в перевозке
                }

            except NoSuchElementException as e:
                if self.logger:
                    self.logger.error(f"Не найден элемент при обработке {flow_type.lower()} перевозки {carriage_number}: {e}")
                # Возвращаемся на страницу с типом перевозок в случае ошибки
                try:
                    self.driver.get(original_url)
                except:
                    pass  # Если не удалось вернуться, продолжаем с текущей страницы

                carriage_detail = {
                    'carriage_number': carriage_number,
                    'items_count': 0,
                    'error': f'Element not found: {str(e)}'
                }
            except TimeoutException as e:
                if self.logger:
                    self.logger.error(f"Таймаут при обработке {flow_type.lower()} перевозки {carriage_number}: {e}")
                # Возвращаемся на страницу с типом перевозок в случае ошибки
                try:
                    self.driver.get(original_url)
                except:
                    pass  # Если не удалось вернуться, продолжаем с текущей страницы

                carriage_detail = {
                    'carriage_number': carriage_number,
                    'items_count': 0,
                    'error': f'Timeout: {str(e)}'
                }
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Ошибка при обработке {flow_type.lower()} перевозки {carriage_number}: {e}")
                # Возвращаемся на страницу с типом перевозок в случае ошибки
                try:
                    self.driver.get(original_url)
                except:
                    pass  # Если не удалось вернуться, продолжаем с текущей страницы

                carriage_detail = {
                    'carriage_number': carriage_number,
                    'items_count': 0,
                    'error': str(e)
                }

            carriage_details.append(carriage_detail)

        # Вычисляем общее количество отправлений по всем перевозкам
        total_items_count = sum(detail.get('items_count', 0) for detail in carriage_details)

        flow_data = {
            'flow_type': flow_type,
            'total_carriages_found': total_carriages,  # Общее количество найденных перевозок
            'carriage_numbers': carriage_numbers,  # Список номеров перевозок
            'carriage_details': carriage_details,  # Детали по каждой перевозке
            'total_items_count': total_items_count,  # Общее количество отправлений по всем перевозкам
        }

        return flow_data





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
    from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Carriages_OzonScript_config import (
        ERP_URL_TEMPLATE,
        DIRECT_FLOW_URL_TEMPLATE,
        RETURN_FLOW_URL_TEMPLATE,
        DATE_FORMAT,
        FILE_PATTERN
    )

    # Создаем экземпляр парсера
    parser = OzonCarriagesReportParser(SCRIPT_CONFIG)

    # Вызываем специфичный метод для обработки двух типов перевозок
    parser.run_carriage_parser_with_params(
        ERP_URL_TEMPLATE=ERP_URL_TEMPLATE,
        DIRECT_FLOW_URL_TEMPLATE=DIRECT_FLOW_URL_TEMPLATE,
        RETURN_FLOW_URL_TEMPLATE=RETURN_FLOW_URL_TEMPLATE,
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
