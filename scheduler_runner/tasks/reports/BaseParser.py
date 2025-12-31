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
                              original_url: str = None, report_url_pattern: str = None) -> bool:
        """
        Универсальный метод для выбора опции в выпадающем списке

        Args:
            dropdown_selector: XPath или CSS-селектор для выпадающего списка
            option_selector: XPath или CSS-селектор для элементов опций
            expected_value: Ожидаемое значение для выбора
            original_url: URL для возврата, если произошел переход на другую страницу
            report_url_pattern: Паттерн URL для проверки, остались ли на странице отчета

        Returns:
            bool: True, если опция была успешно выбрана
        """
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC

        try:
            # Кликаем по выпадающему списку, чтобы открыть опции
            dropdown_element = self.driver.find_element(By.XPATH, dropdown_selector)
            dropdown_element.click()

            # Ждем появления опций
            time.sleep(2)

            # Пытаемся найти все доступные опции в выпадающем списке
            all_option_elements = self.driver.find_elements(By.XPATH, option_selector)

            print("Доступные опции в выпадающем списке:")
            available_options = []
            for element in all_option_elements:
                # Ищем текст опции в элементе
                element_text = element.text.strip()
                if element_text and len(element_text) > 3:  # Фильтруем короткие или пустые значения
                    available_options.append(element_text)
                    print(f"  - {element_text}")

            # Ищем конкретно нужный пункт выдачи среди доступных опций
            target_option = None
            for option_element in all_option_elements:
                if expected_value in option_element.text:
                    target_option = option_element
                    break

            if target_option:
                target_option.click()
                print(f"Установлено значение: {expected_value}")
                time.sleep(2)  # Ждем обновления страницы

                # Если нужно проверить URL и вернуться при необходимости
                if original_url and report_url_pattern:
                    # Проверяем, остались ли мы на нужной странице
                    current_url = self.driver.current_url
                    print(f"Текущий URL после смены значения: {current_url}")

                    # Если мы не на странице отчета, возвращаемся туда
                    if report_url_pattern not in current_url:
                        print("Мы покинули страницу отчета, возвращаемся...")
                        # Восстанавливаем URL с фильтрами
                        self.driver.get(original_url)
                        time.sleep(3)  # Ждем загрузки страницы
                    else:
                        print("Мы остались на странице отчета")

                return True
            else:
                print(f"Не найдена опция для значения {expected_value}")
                print(f"Доступные значения: {available_options}")
                return False

        except Exception as e:
            print(f"Ошибка при установке значения в выпадающем списке: {e}")
            return False

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