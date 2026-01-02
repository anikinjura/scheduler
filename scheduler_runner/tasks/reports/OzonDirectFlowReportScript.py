"""
OzonDirectFlowReportScript.py

Скрипт формирования отчета по селлерским отправлениям с типом "прямой поток" для маркетплейса ОЗОН для домена (задачи) reports.
Собирает статистику из ERP-системы ОЗОН с использованием уже активной сессии пользователя,
с возможностью выбора нужного пункта выдачи и извлечения количества перевозок с типом "прямой поток" за текущий день.

Архитектура:
- Все параметры задаются в config/scripts/OzonDirectFlowReportScript_config.py.
- Использует Selenium для автоматизации браузера Edge.
- Завершает все процессы Edge перед запуском для избежания конфликтов.
- Использует существующую сессию пользователя.
- Сохраняет отчет по селлерским отправлениям в JSON-файл.

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
from scheduler_runner.tasks.reports.config.scripts.OzonDirectFlowReportScript_config import SCRIPT_CONFIG

class OzonDirectFlowReportParser(BaseOzonParser):
    """Парсер для получения отчета по селлерским отправлениям с типом 'прямой поток' из ERP-системы ОЗОН"""

    def login(self):
        """Вход в ERP-систему ОЗОН"""
        self.driver.get(self.config['ERP_URL'])
        # Реализация входа (если требуется, обычно сессия уже активна)

    def navigate_to_reports(self):
        """Навигация к странице отчета по селлерским отправлениям ОЗОН"""
        # Для отчета по селлерским отправлениям мы сразу переходим на нужный URL
        # Навигация по элементам интерфейса ОЗОН
        # Конкретная реализация зависит от структуры ERP-системы
        pass

    def extract_data(self) -> Dict[str, Any]:
        """Извлечение отчета по селлерским отправлениям из ERP-системы ОЗОН"""
        from selenium.webdriver.common.by import By
        import time

        print(f"Текущий URL: {self.driver.current_url}")
        print(f"Заголовок страницы: {self.driver.title}")

        # Проверяем, остались ли мы на странице логина
        if "login" in self.driver.current_url.lower():
            print("Все еще на странице логина - сессия не активна или недостаточно прав")
            return {
                'marketplace': 'Ozon',
                'report_type': 'direct_flow',
                'date': datetime.now().strftime('%Y-%m-%d'),
                'timestamp': datetime.now().isoformat(),
                'error': 'Still on login page - session not active or insufficient permissions',
                'current_url': self.driver.current_url,
                'page_title': self.driver.title,
            }
        else:
            print("Успешно вошли в систему")

            # Ждем полной загрузки страницы
            time.sleep(3)

            # Пытаемся установить правильный пункт выдачи
            try:
                # Находим элемент выпадающего списка по ID
                pvz_input = self.driver.find_element(By.XPATH, "//input[@id='input___v-0-0']")

                # Получаем текущее значение
                current_value = pvz_input.get_attribute("value")
                print(f"Текущий пункт выдачи: {current_value}")

                # Получаем ожидаемый ПВЗ из конфига (должен совпадать с PVZ_ID)
                expected_pvz = self.config.get('EXPECTED_PVZ_CODE', '')  # Используем ожидаемый ПВЗ из конфига
                print(f"Ожидаемый пункт выдачи: {expected_pvz}")

                # Если текущий пункт выдачи не соответствует ожидаемому, пытаемся изменить
                if current_value != expected_pvz:
                    print(f"Текущий пункт выдачи ({current_value}) не совпадает с ожидаемым ({expected_pvz}). Пытаемся изменить...")

                    # Сохраняем текущий URL до изменения
                    original_url = self.driver.current_url
                    print(f"Сохраненный URL до изменения: {original_url}")

                    # Используем специфичный метод из базового класса ОЗОН для выбора опции в выпадающем списке
                    success = self.select_pvz_dropdown_option(
                        expected_pvz=expected_pvz,
                        original_url=original_url
                    )

                    if not success:
                        print(f"Не удалось установить пункт выдачи {expected_pvz}")
                        print("Продолжаем с текущим пунктом выдачи...")
                else:
                    print(f"Пункт выдачи уже установлен правильно: {current_value}")

            except Exception as e:
                print(f"Ошибка при установке пункта выдачи: {e}")
                # Продолжаем выполнение, даже если не удалось установить правильный пункт выдачи

            # Извлечение базовой информации
            try:
                # Первый этап: извлечение информации о перевозках
                print("Начинаем первый этап: извлечение информации о перевозках")

                # Ищем элемент с информацией о количестве найденных перевозок
                total_carriages_text = self.extract_ozon_element_by_xpath("//div[contains(@class, '_total_1n8st_15')]", "textContent")
                if total_carriages_text:
                    # Извлекаем число из текста "Найдено: N"
                    import re
                    found_count_match = re.search(r'Найдено:\s*(\d+)', total_carriages_text)
                    if found_count_match:
                        total_carriages = int(found_count_match.group(1))
                        print(f"Найдено перевозок: {total_carriages}")
                    else:
                        total_carriages = 0
                        print("Не удалось извлечь количество перевозок из текста")
                else:
                    print("Не найден элемент с информацией о количестве перевозок")
                    total_carriages = 0

                # Извлечение номеров перевозок из таблицы
                carriage_numbers = []
                carriage_elements = self.driver.find_elements(By.XPATH, "//div[contains(@class, '_carriageNumber_tu0l6_21')]")

                for element in carriage_elements:
                    carriage_number = element.text.strip()
                    if carriage_number:
                        carriage_numbers.append(carriage_number)

                print(f"Извлеченные номера перевозок: {carriage_numbers}")

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

                # Второй этап: обработка каждой перевозки по отдельности
                print(f"Начинаем второй этап: обработка {len(carriage_numbers)} перевозок")

                carriage_details = []
                for i, carriage_number in enumerate(carriage_numbers):
                    print(f"Обрабатываем перевозку {i+1}/{len(carriage_numbers)}: {carriage_number}")

                    # Сохраняем оригинальный URL для возврата
                    original_url = self.driver.current_url

                    try:
                        # Переходим на страницу с деталями конкретной перевозки
                        carriage_url = f"https://turbo-pvz.ozon.ru/outbound/carriages-archive/{carriage_number}?filter=%7B%22articleState%22:%22Took%22,%22articleType%22:%22ArticlePosting%22%7D"
                        print(f"Переходим на страницу перевозки: {carriage_url}")
                        self.driver.get(carriage_url)

                        # Ждем загрузки страницы
                        time.sleep(3)

                        # Извлекаем количество отправлений из элемента на странице перевозки
                        from selenium.webdriver.common.by import By

                        # Ищем элемент с информацией о количестве найденных отправлений
                        total_items_text = self.extract_ozon_element_by_xpath("//div[contains(@class, '_total_1n8st_15')]", "textContent")

                        items_count = 0
                        if total_items_text:
                            # Извлекаем число из текста "Найдено: N"
                            import re
                            found_count_match = re.search(r'Найдено:\s*(\d+)', total_items_text)
                            if found_count_match:
                                items_count = int(found_count_match.group(1))
                                print(f"Найдено отправлений в перевозке {carriage_number}: {items_count}")
                            else:
                                print(f"Не удалось извлечь количество отправлений из текста: {total_items_text}")
                        else:
                            print(f"Не найден элемент с информацией о количестве отправлений для перевозки {carriage_number}")

                        # Возвращаемся на основную страницу
                        self.driver.get(original_url)
                        time.sleep(1)  # Ждем возврата

                        # Формируем детали для этой перевозки
                        carriage_detail = {
                            'carriage_number': carriage_number,
                            'items_count': items_count,  # Количество отправлений в перевозке
                        }

                    except Exception as e:
                        print(f"Ошибка при обработке перевозки {carriage_number}: {e}")
                        # Возвращаемся на основную страницу в случае ошибки
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

                data = {
                    'marketplace': 'Ozon',
                    'report_type': 'direct_flow',
                    'date': datetime.now().strftime('%Y-%m-%d'),
                    'timestamp': datetime.now().isoformat(),
                    'page_title': self.driver.title,
                    'current_url': self.driver.current_url,
                    'total_carriages_found': total_carriages,  # Общее количество найденных перевозок
                    'carriage_numbers': carriage_numbers,  # Список номеров перевозок
                    'carriage_details': carriage_details,  # Детали по каждой перевозке
                    'pvz_info': pvz_info,  # Информация о пункте выдачи
                }

                print(f"Всего найдено перевозок: {total_carriages}")
                print(f"Извлечено номеров перевозок: {len(carriage_numbers)}")
                print(f"Информация о ПВЗ: {pvz_info}")

                return data
            except Exception as e:
                print(f"Ошибка при извлечении данных: {e}")
                import traceback
                print(f"Полный стек трейса: {traceback.format_exc()}")
                return {
                    'marketplace': 'Ozon',
                    'report_type': 'direct_flow',
                    'date': datetime.now().strftime('%Y-%m-%d'),
                    'timestamp': datetime.now().isoformat(),
                    'error': f'Error extracting data: {str(e)}',
                    'current_url': self.driver.current_url,
                    'page_title': self.driver.title,
                    'total_carriages_found': 0,
                    'carriage_numbers': [],
                    'carriage_details': [],
                    'pvz_info': '',
                }

    def logout(self):
        """Выход из системы (обычно не требуется при использовании существующей сессии)"""
        pass

def parse_arguments() -> argparse.Namespace:
    """
    Парсит аргументы командной строки для скрипта формирования отчета ОЗОН.

    --detailed_logs              - включить детализированные логи
    """
    parser = argparse.ArgumentParser(
        description="Формирование отчета по селлерским отправлениям с типом 'прямой поток' ERP-системы ОЗОН",
        epilog="Пример: python OzonDirectFlowReportScript.py --detailed_logs"
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
    try:
        args = parse_arguments()
        detailed_logs = args.detailed_logs or SCRIPT_CONFIG.get("DETAILED_LOGS", False)

        logger = configure_logger(
            user=SCRIPT_CONFIG["USER"],
            task_name=SCRIPT_CONFIG["TASK_NAME"],
            detailed=detailed_logs
        )

        logger.info("Запуск формирования отчета по селлерским отправлениям с типом 'прямой поток' ERP-системы ОЗОН")

        parser = OzonDirectFlowReportParser(SCRIPT_CONFIG)
        try:
            parser.setup_driver()
            parser.login()
            parser.navigate_to_reports()
            data = parser.extract_data()
            parser.logout()

            # Сохранение данных
            output_dir = Path(SCRIPT_CONFIG['OUTPUT_DIR'])
            output_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = output_dir / f"ozon_direct_flow_report_{timestamp}.json"

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)

            logger.info(f"Отчет по селлерским отправлениям ОЗОН успешно сохранен в {filename}")
            logger.info(f"Извлеченные данные: {data}")

            return data
        finally:
            parser.close()

    except Exception as e:
        import traceback
        print(f"Ошибка при формировании отчета по селлерским отправлениям ERP-системы ОЗОН: {e}")
        print(f"Полный стек трейса: {traceback.format_exc()}")
        return None

def main():
    """Основная функция скрипта для запуска из командной строки"""
    args = parse_arguments()
    detailed_logs = args.detailed_logs or SCRIPT_CONFIG.get("DETAILED_LOGS", False)

    logger = configure_logger(
        user=SCRIPT_CONFIG["USER"],
        task_name=SCRIPT_CONFIG["TASK_NAME"],
        detailed=detailed_logs
    )

    try:
        logger.info("Запуск формирования отчета по селлерским отправлениям с типом 'прямой поток' ERP-системы ОЗОН")

        parser = OzonDirectFlowReportParser(SCRIPT_CONFIG)
        try:
            parser.setup_driver()
            parser.login()
            parser.navigate_to_reports()
            data = parser.extract_data()
            parser.logout()

            # Сохранение данных
            output_dir = Path(SCRIPT_CONFIG['OUTPUT_DIR'])
            output_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = output_dir / f"ozon_direct_flow_report_{timestamp}.json"

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)

            logger.info(f"Отчет по селлерским отправлениям ОЗОН успешно сохранен в {filename}")
            logger.info(f"Извлеченные данные: {data}")
        finally:
            parser.close()

    except Exception as e:
        logger.error(f"Ошибка при формировании отчета по селлерским отправлениям ERP-системы ОЗОН: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()