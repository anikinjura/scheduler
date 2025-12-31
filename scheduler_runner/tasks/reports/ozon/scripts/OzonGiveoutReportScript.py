"""
OzonGiveoutReportScript.py

Скрипт для автоматического парсинга отчета по выдаче из ERP-системы ОЗОН.

- Использует Selenium для автоматизации браузера Edge
- Завершает все процессы Edge перед запуском для избежания конфликтов
- Использует существующую сессию пользователя
- Сохраняет отчет по выдаче в нужном формате

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

# Добавляем корень проекта в sys.path для корректного импорта утилит
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from scheduler_runner.utils.logging import configure_logger
from scheduler_runner.tasks.reports.ozon.BaseOzonParser import BaseOzonParser
from scheduler_runner.tasks.reports.ozon.config.scripts.ozon_giveout_report_config import SCRIPT_CONFIG

class OzonGiveoutReportParser(BaseOzonParser):
    """Парсер для получения отчета по выдаче из ERP-системы ОЗОН"""
    
    def login(self):
        """Вход в ERP-систему ОЗОН"""
        self.driver.get(self.config['ERP_URL'])
        # Реализация входа (если требуется, обычно сессия уже активна)
    
    def navigate_to_reports(self):
        """Навигация к странице отчета по выдаче ОЗОН"""
        # Для отчета по выдаче мы сразу переходим на нужный URL
        # Навигация по элементам интерфейса ОЗОН
        # Конкретная реализация зависит от структуры ERP-системы
        pass
    
    def extract_data(self) -> Dict[str, Any]:
        """Извлечение отчета по выдаче из ERP-системы ОЗОН"""
        from selenium.webdriver.common.by import By
        import time

        print(f"Текущий URL: {self.driver.current_url}")
        print(f"Заголовок страницы: {self.driver.title}")

        # Проверяем, остались ли мы на странице логина
        if "login" in self.driver.current_url.lower():
            print("Все еще на странице логина - сессия не активна или недостаточно прав")
            return {
                'marketplace': 'Ozon',
                'report_type': 'giveout',
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

                # Если текущий пункт выдачи не соответствует ожидаемому, пытаемся изменить
                expected_pvz = self.config.get('EXPECTED_PVZ_CODE', 'ЧЕБОКСАРЫ_144')  # Используем ожидаемый ПВЗ из конфига
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
                        # Проверяем, есть ли ожидаемый ПВЗ в списке доступных
                        try:
                            # Кликаем по выпадающему списку, чтобы открыть опции
                            pvz_container = self.driver.find_element(By.XPATH, "//div[contains(@class, 'ozi__input-select__inputSelect__UA4xr')]")
                            pvz_container.click()
                            time.sleep(2)

                            # Пытаемся найти все доступные опции в выпадающем списке
                            all_option_elements = self.driver.find_elements(By.XPATH, "//div[contains(@class, 'ozi__dropdown-item__dropdownItem__cDZcD')]")

                            available_options = []
                            for element in all_option_elements:
                                # Ищем название ПВЗ в элементе с классом ozi__data-content__label__TA_HC
                                label_elements = element.find_elements(By.XPATH, ".//div[contains(@class, 'ozi__data-content__label__TA_HC')]")
                                if label_elements:
                                    element_text = label_elements[0].text.strip()
                                    if element_text and len(element_text) > 3:  # Фильтруем короткие или пустые значения
                                        available_options.append(element_text)

                            if expected_pvz not in available_options:
                                print(f"Ожидаемый ПВЗ {expected_pvz} недоступен в списке. Доступные ПВЗ: {available_options}")
                                # В этом случае используем первый доступный ПВЗ из списка, если он начинается с того же региона
                                region_prefix = expected_pvz.split('_')[0] if '_' in expected_pvz else expected_pvz
                                suitable_pvz = None
                                for option in available_options:
                                    if option.startswith(region_prefix):
                                        suitable_pvz = option
                                        break

                                if suitable_pvz:
                                    print(f"Используем ближайший подходящий ПВЗ: {suitable_pvz}")
                                    # Повторяем попытку с подходящим ПВЗ
                                    success = self.select_pvz_dropdown_option(
                                        expected_pvz=suitable_pvz,
                                        original_url=original_url
                                    )
                                    if not success:
                                        print(f"Не удалось установить даже подходящий ПВЗ {suitable_pvz}")
                                else:
                                    print(f"Не найдено подходящих ПВЗ для региона {region_prefix}")
                                    # Если не найдено подходящих ПВЗ для региона, используем первый из доступных
                                    if available_options:
                                        suitable_pvz = available_options[0].split('\n')[0]  # Берем только название ПВЗ, без адреса
                                        print(f"Используем первый доступный ПВЗ: {suitable_pvz}")
                                        success = self.select_pvz_dropdown_option(
                                            expected_pvz=suitable_pvz,
                                            original_url=original_url
                                        )
                                        if not success:
                                            print(f"Не удалось установить даже первый доступный ПВЗ {suitable_pvz}")
                            else:
                                print(f"Ожидаемый ПВЗ {expected_pvz} доступен в списке, но не удалось его выбрать")
                        except Exception as e:
                            print(f"Ошибка при проверке доступных ПВЗ: {e}")

                        print("Продолжаем с текущим пунктом выдачи...")
                else:
                    print(f"Пункт выдачи уже установлен правильно: {current_value}")

            except Exception as e:
                print(f"Ошибка при установке пункта выдачи: {e}")
                # Продолжаем выполнение, даже если не удалось установить правильный пункт выдачи

            # Извлечение базовой информации
            try:
                page_text = self.driver.find_element(By.TAG_NAME, "body").text

                # Извлечение заголовков таблиц, если они есть
                tables = self.driver.find_elements(By.TAG_NAME, "table")
                table_count = len(tables)

                # Используем специфичный метод из базового класса ОЗОН для поиска "Всего: N"
                total_packages = 0
                total_text = self.extract_ozon_data_by_pattern(r'Всего:\s*(\d+)', page_text)
                if total_text:
                    total_packages = int(total_text)

                # Используем специфичные методы из базового класса ОЗОН для извлечения информации о ПВЗ
                pvz_info = ""

                # Ищем специфичный элемент с информацией о ПВЗ по точным классам и ID
                # Это input с ID "input___v-0-0" и значением названия ПВЗ
                pvz_value = self.extract_ozon_element_by_xpath("//input[@id='input___v-0-0' and @readonly]", "value")
                if pvz_value and ('ЧЕБОКСАР' in pvz_value.upper() or 'PVZ' in pvz_value.upper() or 'ПУНКТ' in pvz_value.upper()):
                    pvz_info = pvz_value

                # Если не нашли через специфичный XPath, ищем по классу и атрибуту readonly
                if not pvz_info:
                    pvz_value = self.extract_ozon_element_by_xpath("//input[contains(@class, 'ozi__input__input__ie7wU') and @readonly]", "value")
                    if pvz_value and ('ЧЕБОКСАР' in pvz_value.upper() or 'PVZ' in pvz_value.upper() or 'ПУНКТ' in pvz_value.upper()):
                        pvz_info = pvz_value

                # Если не нашли в элементах, ищем в общем тексте
                if not pvz_info:
                    pvz_matches = re.findall(r'(ЧЕБОКСАР\w+)', page_text)
                    if pvz_matches:
                        pvz_info = pvz_matches[0]

                data = {
                    'marketplace': 'Ozon',
                    'report_type': 'giveout',
                    'date': datetime.now().strftime('%Y-%m-%d'),
                    'timestamp': datetime.now().isoformat(),
                    'page_title': self.driver.title,
                    'current_url': self.driver.current_url,
                    'table_count': table_count,
                    'issued_packages': total_packages,  # Используем total_packages как количество выданных
                    'total_packages': total_packages,  # Общее количество выданных
                    'pvz_info': pvz_info,  # Информация о пункте выдачи
                    'raw_text_preview': page_text[:500] + "..." if len(page_text) > 500 else page_text,
                }

                print(f"Найдено таблиц: {table_count}")
                print(f"Всего выданных посылок: {total_packages}")
                print(f"Информация о ПВЗ: {pvz_info}")

                # Отправляем уведомление через Telegram
                try:
                    from scheduler_runner.utils.logging import configure_logger
                    logger = configure_logger(
                        user=self.config.get('USER', 'system'),
                        task_name=self.config.get('TASK_NAME', 'OzonGiveoutReportScript'),
                        detailed=self.config.get('DETAILED_LOGS', False)
                    )

                    # Формируем сообщение для уведомления
                    notification_message = f"📊 Отчет по выдаче ОЗОН\nПВЗ: {pvz_info}\nДата: {data['date']}\nВыдано посылок: {total_packages}"
                    self.send_ozon_notification(notification_message, logger)
                except Exception as e:
                    print(f"Ошибка при отправке уведомления: {e}")

                return data
            except Exception as e:
                print(f"Ошибка при извлечении данных: {e}")
                import traceback
                print(f"Полный стек трейса: {traceback.format_exc()}")
                return {
                    'marketplace': 'Ozon',
                    'report_type': 'giveout',
                    'date': datetime.now().strftime('%Y-%m-%d'),
                    'timestamp': datetime.now().isoformat(),
                    'error': f'Error extracting data: {str(e)}',
                    'current_url': self.driver.current_url,
                    'page_title': self.driver.title,
                    'issued_packages': 0,
                    'total_packages': 0,
                    'pvz_info': '',
                }
    
    def logout(self):
        """Выход из системы (обычно не требуется при использовании существующей сессии)"""
        pass

def main():
    """Основная функция скрипта"""
    parser = argparse.ArgumentParser(description="Парсинг отчета по выдаче из ERP-системы ОЗОН.")
    parser.add_argument("--detailed_logs", action="store_true", help="Включить детализированные логи.")
    args = parser.parse_args()

    detailed_logs = args.detailed_logs or SCRIPT_CONFIG.get("DETAILED_LOGS", False)

    logger = configure_logger(
        user=SCRIPT_CONFIG["USER"],
        task_name=SCRIPT_CONFIG["TASK_NAME"],
        detailed=detailed_logs
    )

    try:
        logger.info("Запуск парсинга отчета по выдаче ERP-системы ОЗОН")
        
        parser = OzonGiveoutReportParser(SCRIPT_CONFIG)
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
            filename = output_dir / f"ozon_giveout_report_{timestamp}.json"
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
            
            logger.info(f"Отчет по выдаче ОЗОН успешно сохранен в {filename}")
            logger.info(f"Извлеченные данные: {data}")
        finally:
            parser.close()
            
    except Exception as e:
        logger.error(f"Ошибка при парсинге отчета по выдаче ERP-системы ОЗОН: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()