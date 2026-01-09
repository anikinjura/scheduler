"""
BaseParser.py

Базовый класс для парсинга отчетов из маркетплейсов.

Определяет общий интерфейс для всех парсеров маркетплейсов.
"""
__version__ = '1.0.0'

import subprocess
import time
import re
import os
from abc import ABC, abstractmethod
from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from datetime import datetime
from typing import Dict, List, Any

class BaseParser(ABC):
    """Базовый класс для парсинга отчетов из маркетплейсов"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.driver = None

    def terminate_edge_processes(self):
        """Завершает все процессы Microsoft Edge"""
        print("Завершаем все процессы Microsoft Edge...")
        try:
            subprocess.run(["taskkill", "/f", "/im", "msedge.exe"],
                          stdout=subprocess.DEVNULL,
                          stderr=subprocess.DEVNULL)
            time.sleep(2)  # Ждем 2 секунды, чтобы процессы точно завершились
        except Exception as e:
            print(f"Ошибка при завершении процессов Edge: {e}")

    def setup_driver(self):
        """Настройка веб-драйвера"""
        # Завершаем все процессы Edge перед запуском
        self.terminate_edge_processes()

        # Получаем путь к пользовательским данным Edge
        # Если в конфиге не указан путь, используем путь для текущего пользователя
        edge_user_data_dir = self.config.get('EDGE_USER_DATA_DIR')
        if not edge_user_data_dir or edge_user_data_dir == "":
            edge_user_data_dir = self.get_edge_user_data_dir()

        options = Options()
        options.add_argument(f"--user-data-dir={edge_user_data_dir}")
        options.add_argument("--profile-directory=Default")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        # Используем headless режим, если указано в конфиге
        if self.config.get('HEADLESS', True):
            options.add_argument("--headless")

        self.driver = webdriver.Edge(options=options)
        return self.driver

    def select_dropdown_option(self, dropdown_selector: str, option_selector: str, expected_value: str,
                              original_url: str = None, report_url_pattern: str = None,
                              exact_match: bool = False, text_content_attr: str = None) -> bool:
        """
        Универсальный метод для выбора опции в выпадающем списке

        Args:
            dropdown_selector: XPath или CSS-селектор для выпадающего списка
            option_selector: XPath или CSS-селектор для элементов опций
            expected_value: Ожидаемое значение для выбора
            original_url: URL для возврата, если произошел переход на другую страницу
            report_url_pattern: Паттерн URL для проверки, остались ли на странице отчета
            exact_match: Флаг, указывающий на необходимость точного соответствия
            text_content_attr: Атрибут для получения текста (например, 'textContent')

        Returns:
            bool: True, если опция была успешно выбрана
        """
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.common.action_chains import ActionChains

        try:
            # Кликаем по выпадающему списку, чтобы открыть опции
            dropdown_element = self.driver.find_element(By.XPATH, dropdown_selector)
            dropdown_element.click()

            # Ждем появления опций
            time.sleep(2)

            # Пытаемся найти все доступные опции в выпадающем списке
            all_option_elements = self.driver.find_elements(By.XPATH, option_selector)

            if self.logger:
                self.logger.debug("Доступные опции в выпадающем списке:")
            available_options = []
            for element in all_option_elements:
                # Ищем текст опции в элементе
                if text_content_attr:
                    element_text = element.get_attribute(text_content_attr) or element.text
                else:
                    element_text = element.text.strip()

                element_text = element_text.strip()
                if element_text and len(element_text) > 3:  # Фильтруем короткие или пустые значения
                    available_options.append(element_text)
                    if self.logger:
                        self.logger.debug(f"  - {element_text}")

            # Ищем конкретно нужный пункт выдачи среди доступных опций
            target_option = None
            for option_element in all_option_elements:
                if text_content_attr:
                    option_text = option_element.get_attribute(text_content_attr) or option_element.text
                else:
                    option_text = option_element.text.strip()

                if exact_match:
                    if expected_value == option_text:
                        target_option = option_element
                        if self.logger:
                            self.logger.debug(f"Найден элемент для значения {expected_value}")
                        break
                else:
                    if expected_value in option_text:
                        target_option = option_element
                        if self.logger:
                            self.logger.debug(f"Найден элемент для значения {expected_value} (в виде подстроки в '{option_text}')")
                        break
                    else:
                        if self.logger:
                            self.logger.debug(f"Проверен элемент с текстом '{option_text}' (ожидалось '{expected_value}')")

            if target_option:
                # Используем ActionChains для более надежного клика
                actions = ActionChains(self.driver)
                actions.move_to_element(target_option).click().perform()

                if self.logger:
                    self.logger.info(f"Установлено значение: {expected_value}")
                time.sleep(2)  # Ждем обновления страницы

                # Если нужно проверить URL и вернуться при необходимости
                if original_url and report_url_pattern:
                    # Проверяем, остались ли мы на нужной странице
                    current_url = self.driver.current_url
                    if self.logger:
                        self.logger.debug(f"Текущий URL после смены значения: {current_url}")

                    # Если мы не на странице отчета, возвращаемся туда
                    if report_url_pattern not in current_url:
                        if self.logger:
                            self.logger.info("Мы покинули страницу отчета, возвращаемся...")
                        # Восстанавливаем URL с фильтрами
                        self.driver.get(original_url)
                        time.sleep(3)  # Ждем загрузки страницы
                    else:
                        if self.logger:
                            self.logger.debug("Мы остались на странице отчета")

                return True
            else:
                if self.logger:
                    self.logger.warning(f"Не найдена опция для значения {expected_value}")
                    self.logger.debug(f"Доступные значения: {available_options}")
                return False

        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при установке значения в выпадающем списке: {e}")
            return False

    def run_parser_with_params(self, config_module, date_format='%Y-%m-%d',
                              file_pattern='{report_type}_report_{pvz_id}_{date}.json',
                              erp_url_template: str = None, marketplace_name: str = ''):
        """
        Общий метод для запуска парсера с общей логикой

        Args:
            config_module: Модуль конфигурации
            date_format: Формат даты
            file_pattern: Шаблон имени файла
            erp_url_template: Шаблон URL для ERP системы
            marketplace_name: Название маркетплейса для логирования
        """
        import argparse
        import sys
        import time
        import json
        from pathlib import Path
        from datetime import datetime
        from scheduler_runner.utils.logging import configure_logger
        from scheduler_runner.utils.system import SystemUtils

        def parse_arguments():
            parser = argparse.ArgumentParser(
                description=f"Парсинг данных из ERP-системы",
                epilog="Пример: python script.py --detailed_logs --date 2026-01-01"
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

        # Инициализируем logger как None для избежания ошибок в блоке except
        logger = None

        try:
            # 1. Парсинг аргументов командной строки
            args = parse_arguments()
            detailed_logs = args.detailed_logs or getattr(config_module, 'SCRIPT_CONFIG', {}).get("DETAILED_LOGS", False)

            # Получаем дату из аргументов или используем текущую
            target_date = args.date
            if target_date is None:
                target_date = datetime.now().strftime(date_format)

            # 2. Настройка логирования
            logger = configure_logger(
                user=getattr(config_module, 'SCRIPT_CONFIG', {}).get("USER", "system"),
                task_name=getattr(config_module, 'SCRIPT_CONFIG', {}).get("TASK_NAME", "BaseParser"),
                detailed=detailed_logs
            )

            # 3. Логирование начала процесса
            marketplace_display = f" {marketplace_name}" if marketplace_name else ""
            logger.info(f"Запуск парсинга данных ERP-системы{marketplace_display} за дату: {target_date}")

            # 4. Создание копии конфигурации с обновленным URL для указанной даты
            # Формируем готовый URL, подставив дату в шаблон
            if erp_url_template:
                erp_url = erp_url_template.format(date=target_date)
            else:
                erp_url_template = getattr(config_module, 'ERP_URL_TEMPLATE', '')
                erp_url = erp_url_template.format(date=target_date)

            # Создаем копию конфигурации и обновляем только URL
            script_config = getattr(config_module, 'SCRIPT_CONFIG', {}).copy()
            script_config["ERP_URL"] = erp_url

            # 5. Обновляем конфигурацию в текущем экземпляре
            # Проверяем, является ли self.config словарем, и если да, то обновляем его
            if hasattr(self, 'config') and isinstance(self.config, dict):
                self.config.update(script_config)
            else:
                # Если self.config не существует или не является словарем, создаем новый словарь
                self.config = script_config
            self.logger = logger

            # 6. Настройка драйвера
            try:
                self.setup_driver()

                # 7. Выполнение основных операций
                self.login()
                self.navigate_to_reports()

                # Извлечение данных
                data = self.extract_data()

                self.logout()

                # 8. Сохранение данных
                output_dir = Path(script_config['OUTPUT_DIR'])
                output_dir.mkdir(parents=True, exist_ok=True)

                # Формируем имя файла с использованием шаблона из конфигурации
                date_str = target_date.replace('-', '')  # Преобразуем формат даты для имени файла
                pvz_id = data.get('pvz_info', script_config.get('PVZ_ID', ''))
                # Транслитерируем ПВЗ для использования в имени файла
                translit_pvz = SystemUtils.cyrillic_to_translit(pvz_id) if pvz_id else 'unknown'

                # Используем шаблон из конфигурации для формирования имени файла
                filename_template = file_pattern.replace('{pvz_id}', translit_pvz).replace('{date}', date_str).replace('{report_type}', getattr(self, 'get_report_type', lambda: 'unknown')())
                filename = output_dir / filename_template

                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2, default=str)

                logger.info(f"Отчет{marketplace_display} успешно сохранен в {filename}")
                logger.info(f"Извлеченные данные: {data}")
            finally:
                # 9. Завершение работы
                self.close()

        except Exception as e:
            # 10. Обработка исключений
            import traceback
            if logger:
                logger.error(f"Ошибка при парсинге данных ERP-системы{marketplace_display}: {e}")
                logger.error(f"Полный стек трейса: {traceback.format_exc()}")
            else:
                print(f"Ошибка при парсинге данных ERP-системы{marketplace_display}: {e}")
                print(f"Полный стек трейса: {traceback.format_exc()}")
            sys.exit(1)

    def extract_data_by_pattern(self, pattern: str, page_text: str = None) -> str:
        """
        Извлекает данные с использованием регулярного выражения

        Args:
            pattern: Регулярное выражение для поиска
            page_text: Текст страницы (если не указан, будет получен из текущей страницы)

        Returns:
            str: Найденное значение или пустая строка
        """
        if page_text is None:
            from selenium.webdriver.common.by import By
            page_text = self.driver.find_element(By.TAG_NAME, "body").text

        matches = re.findall(pattern, page_text)
        return matches[0] if matches else ""

    def extract_element_by_xpath(self, xpath: str, attribute: str = None) -> str:
        """
        Извлекает данные из элемента по XPath

        Args:
            xpath: XPath для поиска элемента
            attribute: Имя атрибута для извления (если None, извлекается текст)

        Returns:
            str: Значение атрибута или текст элемента
        """
        try:
            element = self.driver.find_element(By.XPATH, xpath)
            if attribute:
                return element.get_attribute(attribute) or ""
            else:
                return element.text.strip()
        except:
            return ""

    @staticmethod
    def get_current_user():
        """
        Возвращает имя текущего пользователя системы.

        Returns:
            str: Имя текущего пользователя
        """
        return os.getlogin()

    @staticmethod
    def get_edge_user_data_dir(username: str = None):
        """
        Возвращает путь к пользовательским данным Edge для указанного пользователя.

        Args:
            username: Имя пользователя (если None, используется текущий пользователь)

        Returns:
            str: Путь к пользовательским данным Edge
        """
        if username is None:
            username = BaseParser.get_current_user()

        return f"C:/Users/{username}/AppData/Local/Microsoft/Edge/User Data"

    def send_notification(self, message: str, logger=None) -> bool:
        """
        Отправляет уведомление через Telegram, если токены и чат-ID доступны

        Args:
            message: Текст уведомления
            logger: Логгер для записи информации

        Returns:
            bool: True, если уведомление отправлено успешно
        """
        try:
            from scheduler_runner.utils.notify import send_telegram_message

            token = self.config.get('TELEGRAM_TOKEN')
            chat_id = self.config.get('TELEGRAM_CHAT_ID')

            if not token or not chat_id:
                if logger:
                    logger.warning("Токены Telegram не заданы, уведомление не отправлено")
                return False

            success, result = send_telegram_message(token, chat_id, message, logger)
            if success:
                if logger:
                    logger.info("Уведомление успешно отправлено через Telegram")
            else:
                if logger:
                    logger.error(f"Ошибка отправки уведомления через Telegram: {result}")
            return success
        except Exception as e:
            if logger:
                logger.error(f"Ошибка при отправке уведомления: {e}")
            return False

    @abstractmethod
    def login(self):
        """Метод для входа в систему маркетплейса"""
        pass

    @abstractmethod
    def navigate_to_reports(self):
        """Метод для навигации к странице отчетов"""
        pass

    @abstractmethod
    def extract_data(self) -> Dict[str, Any]:
        """Метод для извлечения данных из системы"""
        pass

    @abstractmethod
    def logout(self):
        """Метод для выхода из системы"""
        pass

    def close(self):
        """Закрытие драйвера"""
        if self.driver:
            self.driver.quit()