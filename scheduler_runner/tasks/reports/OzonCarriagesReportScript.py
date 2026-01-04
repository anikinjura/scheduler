"""
OzonCarriagesReportScript.py

Скрипт формирования отчета по перевозкам (прямые и возвратные) для маркетплейса ОЗОН для домена (задачи) reports.
Собирает статистику из ERP-системы ОЗОН с использованием уже активной сессии пользователя,
с возможностью выбора нужного пункта выдачи и извлечения количества перевозок обоих типов (прямые и возвратные) за текущий день.

Архитектура:
- Все параметры задаются в config/scripts/OzonCarriagesReportScript_config.py.
- Использует Selenium для автоматизации браузера Edge.
- Завершает все процессы Edge перед запуском для избежания конфликтов.
- Использует существующую сессию пользователя.
- Сохраняет отчет по перевозкам в JSON-файл.

Author: anikinjura
"""
__version__ = '1.0.0'

import argparse
import sys
import time
from pathlib import Path
from datetime import datetime
import json
import re
from typing import Dict, Any

# Добавляем путь к корню проекта для импорта модулей
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from scheduler_runner.utils.logging import configure_logger
from scheduler_runner.tasks.reports.BaseOzonParser import BaseOzonParser
from scheduler_runner.tasks.reports.config.scripts.OzonCarriagesReportScript_config import SCRIPT_CONFIG


class OzonCarriagesReportParser(BaseOzonParser):
    """Парсер для получения отчета по перевозкам (прямые и возвратные) из ERP-системы ОЗОН"""

    def __init__(self, config, logger=None):
        super().__init__(config)
        self.logger = logger

    def login(self):
        """Вход в ERP-систему ОЗОН"""
        # Заходим на страницу с базовым URL (без типа перевозки)
        self.driver.get(self.config['ERP_URL'])
        # Реализация входа (если требуется, обычно сессия уже активна)

    def navigate_to_reports(self):
        """Навигация к странице отчета по перевозкам ОЗОН"""
        pass

    def extract_data(self, target_url: str = None) -> Dict[str, Any]:
        """Извлечение отчета по перевозкам из ERP-системы ОЗОН"""
        from selenium.webdriver.common.by import By
        import time
        from urllib.parse import unquote

        # Если передан целевой URL, переходим на него
        if target_url:
            self.driver.get(target_url)
            if self.logger:
                self.logger.info(f"Переходим на целевой URL: {target_url}")

        if self.logger:
            self.logger.info(f"Текущий URL: {self.driver.current_url}")
            self.logger.info(f"Заголовок страницы: {self.driver.title}")

        # Проверяем, остались ли мы на странице логина
        if "login" in self.driver.current_url.lower():
            if self.logger:
                self.logger.warning("Все еще на странице логина - сессия не активна или недостаточно прав")
            return {
                'marketplace': 'Ozon',
                'report_type': 'carriages',
                'date': datetime.now().strftime('%Y-%m-%d'),
                'timestamp': datetime.now().isoformat(),
                'error': 'Still on login page - session not active or insufficient permissions',
                'current_url': self.driver.current_url,
                'page_title': self.driver.title,
            }
        else:
            if self.logger:
                self.logger.info("Успешно вошли в систему")

            # Ждем полной загрузки страницы
            time.sleep(3)

            # Пытаемся установить правильный пункт выдачи
            try:
                # Находим элемент выпадающего списка по ID
                pvz_input = self.driver.find_element(By.XPATH, "//input[@id='input___v-0-0']")

                # Получаем текущее значение
                current_value = pvz_input.get_attribute("value")
                if self.logger:
                    self.logger.info(f"Текущий пункт выдачи: {current_value}")

                # Получаем ожидаемый ПВЗ из конфига (должен совпадать с PVZ_ID)
                expected_pvz = self.config.get('EXPECTED_PVZ_CODE', '')  # Используем ожидаемый ПВЗ из конфига
                if self.logger:
                    self.logger.info(f"Ожидаемый пункт выдачи: {expected_pvz}")

                # Если текущий пункт выдачи не соответствует ожидаемому, пытаемся изменить
                if current_value != expected_pvz:
                    if self.logger:
                        self.logger.info(f"Текущий пункт выдачи ({current_value}) не совпадает с ожидаемым ({expected_pvz}). Пытаемся изменить...")

                    # Сохраняем текущий URL до изменения
                    original_url = self.driver.current_url
                    if self.logger:
                        self.logger.info(f"Сохраненный URL до изменения: {original_url}")

                    # Используем специфичный метод из базового класса ОЗОН для выбора опции в выпадающем списке
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
                # Продолжаем выполнение, даже если не удалось установить правильный пункт выдачи

            # Извлечение базовой информации
            try:
                # Получаем дату из текущего URL
                current_url_encoded = self.driver.current_url  # закодированный URL
                current_url = unquote(current_url_encoded)  # декодированный URL
                import re
                # Ищем дату в формате startSentMoment или endSentMoment
                date_match = re.search(r'(?:startSentMoment|endSentMoment)%22:%22(\d{4}-\d{2}-\d{2})', current_url_encoded)
                report_date = date_match.group(1) if date_match else datetime.now().strftime('%Y-%m-%d')

                # Используем специфичные методы из базового класса ОЗОН для извлечения информации о ПВЗ
                pvz_info = ""

                # Ищем специфичный элемент с информацией о ПВЗ по точным классам и ID
                # Это input с ID "input___v-0-0" и значением названия ПВЗ
                pvz_value = self.extract_ozon_element_by_xpath("//input[@id='input___v-0-0' and @readonly]", "value")
                if pvz_value and ('ПВЗ' in pvz_value.upper() or 'PVZ' in pvz_value.upper() or '_' in pvz_value):
                    pvz_info = pvz_value

                # Если не нашли через специфичный XPath, ищем по классу и атрибуту readonly
                if not pvz_info:
                    pvz_value = self.extract_ozon_element_by_xpath("//input[contains(@class, 'ozi__input__input__ie7wU') and @readonly]", "value")
                    if pvz_value and ('ПВЗ' in pvz_value.upper() or 'PVZ' in pvz_value.upper() or '_' in pvz_value):
                        pvz_info = pvz_value

                # Если не нашли в элементах, ищем в общем тексте
                # Ищем все возможные ПВЗ в формате НАЗВАНИЕ_число
                if not pvz_info:
                    page_text = self.driver.find_element(By.TAG_NAME, "body").text
                    pvz_matches = re.findall(r'([А-Яа-яЁёA-Za-z_]+\d+)', page_text)
                    if pvz_matches:
                        # Фильтруем найденные совпадения, оставляя только те, что похожи на названия ПВЗ
                        for match in pvz_matches:
                            if '_' in match and any(keyword in match.upper() for keyword in ['ПВЗ', 'PVZ', 'СОС', 'ЧЕБ', 'КАЗ', 'РОС']):
                                pvz_info = match
                                break
                        # Если не нашли подходящий ПВЗ по ключевым словам, берем первый найденный
                        if not pvz_info and pvz_matches:
                            pvz_info = pvz_matches[0]

                # Определяем тип перевозки из закодированного URL
                flow_type = "Unknown"
                if "flowType%22:%22Direct%22" in current_url_encoded:
                    flow_type = "Direct"
                elif "flowType%22:%22Return%22" in current_url_encoded:
                    flow_type = "Return"

                # Обработка конкретного типа перевозок
                if self.logger:
                    self.logger.info(f"Начинаем обработку {flow_type.lower()} перевозок")
                flow_data = self.process_flow_type(flow_type, report_date)

                # Формируем итоговые данные
                data = {
                    'marketplace': 'Ozon',
                    'report_type': f'carriages_{flow_type.lower()}',
                    'date': report_date,
                    'timestamp': datetime.now().isoformat(),
                    'page_title': self.driver.title,
                    'current_url': self.driver.current_url,
                    'flow_type': flow_type,
                    f'{flow_type.lower()}_flow': flow_data,
                    'pvz_info': pvz_info,  # Информация о пункте выдачи
                }

                if self.logger:
                    self.logger.info(f"Информация о ПВЗ: {pvz_info}")
                    self.logger.info(f"{flow_type} поток - найдено перевозок: {flow_data['total_carriages_found']}, всего отправлений: {flow_data['total_items_count']}")

                return data
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Ошибка при извлечении данных: {e}")
                    import traceback
                    self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
                return {
                    'marketplace': 'Ozon',
                    'report_type': 'carriages',
                    'date': datetime.now().strftime('%Y-%m-%d'),
                    'timestamp': datetime.now().isoformat(),
                    'error': f'Error extracting data: {str(e)}',
                    'current_url': self.driver.current_url,
                    'page_title': self.driver.title,
                    'flow_type': 'Unknown',
                    'unknown_flow': {
                        'total_carriages_found': 0,
                        'carriage_numbers': [],
                        'carriage_details': [],
                        'total_items_count': 0
                    },
                    'pvz_info': '',
                }

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
            found_count_match = re.search(r'Найдено:\s*(\d+)', total_carriages_text)
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
                    found_count_match = re.search(r'Найдено:\s*(\d+)', total_items_text)
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

    def logout(self):
        """Выход из системы (обычно не требуется при использовании существующей сессии)"""
        pass


def parse_arguments() -> argparse.Namespace:
    """
    Парсит аргументы командной строки для скрипта формирования отчета ОЗОН.

    --detailed_logs              - включить детализированные логи
    """
    parser = argparse.ArgumentParser(
        description="Формирование отчета по перевозкам (прямые и возвратные) ERP-системы ОЗОН",
        epilog="Пример: python OzonCarriagesReportScript.py --detailed_logs"
    )
    parser.add_argument(
        "--detailed_logs",
        action="store_true",
        default=False,
        help="Включить детализированные логи"
    )
    return parser.parse_args()


def main_for_data_extraction():
    """Основная функция скрипта, возвращающая данные отчета"""
    pass


def main():
    """Основная функция скрипта для запуска из командной строки"""
    try:
        # 1. Парсинг аргументов командной строки
        args = parse_arguments()
        detailed_logs = args.detailed_logs or SCRIPT_CONFIG.get("DETAILED_LOGS", False)

        # 2. Настройка логирования
        logger = configure_logger(
            user=SCRIPT_CONFIG["USER"],
            task_name=SCRIPT_CONFIG["TASK_NAME"],
            detailed=detailed_logs
        )

        # 3. Логирование начала процесса
        logger.info("Запуск формирования отчета по перевозкам (прямые и возвратные) ERP-системы ОЗОН")

        # 4. Создание экземпляра парсера
        parser = OzonCarriagesReportParser(SCRIPT_CONFIG, logger)

        # 5. Настройка драйвера
        try:
            parser.setup_driver() # setup_driver() определена в базовом классе BaseOzonParser, создает и настраивает экземпляр браузера, готовый к работе с ERP-системой Ozon.

            # 6. Выполнение основных операций
            parser.login()
            parser.navigate_to_reports()

            # Извлечение данных для прямых перевозок
            direct_url = parser.config['DIRECT_FLOW_URL']
            direct_data = parser.extract_data(direct_url)

            # Возвращаемся на начальную страницу
            parser.login()

            # Извлечение данных для возвратных перевозок
            return_url = parser.config['RETURN_FLOW_URL']
            return_data = parser.extract_data(return_url)

            # Объединяем данные
            combined_data = {
                'marketplace': 'Ozon',
                'report_type': 'carriages_combined',
                'date': direct_data.get('date', return_data.get('date', datetime.now().strftime('%Y-%m-%d'))),
                'timestamp': datetime.now().isoformat(),
                'page_title': direct_data.get('page_title', return_data.get('page_title', '')),
                'current_url': parser.config['ERP_URL'],  # Базовый URL с фильтром по дате
                'direct_flow': direct_data.get('direct_flow', direct_data.get('unknown_flow', {})),
                'return_flow': return_data.get('return_flow', return_data.get('unknown_flow', {})),
                'pvz_info': direct_data.get('pvz_info', return_data.get('pvz_info', '')),
            }

            parser.logout()

            # 7. Сохранение данных
            output_dir = Path(SCRIPT_CONFIG['OUTPUT_DIR'])
            output_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = output_dir / f"ozon_carriages_report_{timestamp}.json"

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(combined_data, f, ensure_ascii=False, indent=2, default=str)

            # 8. Логирование завершения
            logger.info(f"Отчет по перевозкам ОЗОН успешно сохранен в {filename}")
            logger.info(f"Извлеченные данные: {combined_data}")
        finally:
            # 9. Завершение работы
            parser.close()

    except Exception as e:
        # 10. Обработка исключений
        logger.error(f"Ошибка при формировании отчета по перевозкам ERP-системы ОЗОН: {e}")
        sys.exit(1)