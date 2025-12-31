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