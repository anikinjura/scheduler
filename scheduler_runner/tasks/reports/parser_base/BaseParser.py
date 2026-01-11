"""
BaseParser.py

Минимальный базовый класс для парсинга отчетов из маркетплейсов.

Определяет минимально необходимый интерфейс для всех парсеров маркетплейсов.
"""
__version__ = '2.1.0'

import subprocess
import time
import re
import os
from pathlib import Path
from abc import ABC, abstractmethod
from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from datetime import datetime
from typing import Dict, Any


class BaseParser(ABC):
    """Минимальный базовый класс для парсинга отчетов из маркетплейсов"""

    def __init__(self, config: Dict[str, Any]):
        """Инициализация базового парсера"""
        self.config = config
        self.driver = None
        self.logger = None

    # === АБСТРАКТНЫЕ МЕТОДЫ (обязательны для реализации в дочерних классах) ===

    @abstractmethod
    def login(self):
        """Метод для входа в систему маркетплейса"""
        pass

    @abstractmethod
    def navigate_to_target_page(self):
        """Метод для навигации к целевой странице"""
        pass

    @abstractmethod
    def extract_data(self) -> Dict[str, Any]:
        """Метод для извлечения данных из системы"""
        pass

    @abstractmethod
    def logout(self):
        """Метод для выхода из системы"""
        pass

    # === ШАБЛОННЫЕ МЕТОДЫ (для переопределения в дочерних классах) ===

    def perform_login(self):
        """Шаг выполнения логина"""
        self.login()

    def perform_navigation(self):
        """Шаг выполнения навигации"""
        # Вызываем навигацию к целевой странице
        self.navigate_to_target_page()

    def perform_extraction(self) -> Dict[str, Any]:
        """Шаг выполнения извлечения данных"""
        return self.extract_data()

    def perform_logout(self):
        """Шаг выполнения логаута"""
        self.logout()

    def get_output_filename(self, data: Dict[str, Any], script_config: Dict[str, Any],
                           target_date: str, file_pattern: str) -> Path:
        """Получает имя выходного файла на основе данных и конфигурации"""
        # Базовая реализация - просто используем дату
        date_str = target_date.replace('-', '')
        filename_template = file_pattern.replace('{date}', date_str)
        output_dir = Path(script_config['OUTPUT_DIR'])
        return output_dir / filename_template

    # === ОСНОВНЫЕ МЕТОДЫ (необходимы для работы парсера) ===
    
    def setup_driver(self):
        """Настройка веб-драйвера"""
        # Завершаем все процессы Edge перед запуском
        self.terminate_edge_processes()

        # Получаем путь к пользовательским данным Edge
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

    def close(self):
        """Закрытие драйвера"""
        if self.driver:
            self.driver.quit()

    # === УНИВЕРСАЛЬНЫЕ МЕТОДЫ ИЗВЛЕЧЕНИЯ ДАННЫХ ===
    
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
            page_text = self.driver.find_element(By.TAG_NAME, "body").text

        matches = re.findall(pattern, page_text)
        return matches[0] if matches else ""

    def extract_element_by_xpath(self, xpath: str, attribute: str = None) -> str:
        """
        Извлекает данные из элемента по XPath

        Args:
            xpath: XPath для поиска элемента
            attribute: Имя атрибута для извлечения (если None, извлекается текст)

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

    # === МЕТОД ЗАПУСКА ПАРСЕРА ===
    
    def run_parser_with_params(self, config_module, date_format='%Y-%m-%d',
                              file_pattern='{report_type}_report_{pvz_id}_{date}.json',
                              target_url_template: str = None, source_name: str = ''):
        """
        Общий метод для запуска парсера с общей логикой

        Args:
            config_module: Модуль конфигурации
            date_format: Формат даты
            file_pattern: Шаблон имени файла
            target_url_template: Шаблон URL для целевой системы
            source_name: Название источника данных для логирования
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
                description=f"Парсинг данных из целевой системы",
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
            source_display = f" {source_name}" if source_name else ""
            logger.info(f"Запуск парсинга данных из целевой системы{source_display} за дату: {target_date}")

            # 4. Создание копии конфигурации с обновленным URL для указанной даты
            # Формируем готовый URL, подставив дату в шаблон
            if target_url_template:
                target_url = target_url_template.format(date=target_date)
            else:
                target_url_template = getattr(config_module, 'TARGET_URL_TEMPLATE', '')
                target_url = target_url_template.format(date=target_date)

            # Создаем копию конфигурации и обновляем только URL
            script_config = getattr(config_module, 'SCRIPT_CONFIG', {}).copy()
            script_config["TARGET_URL"] = target_url

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

                # 7. Выполнение основных операций через шаблонные методы
                self.perform_login()
                self.perform_navigation()
                data = self.perform_extraction()
                self.perform_logout()

                # 8. Сохранение данных
                output_dir = Path(script_config['OUTPUT_DIR'])
                output_dir.mkdir(parents=True, exist_ok=True)

                # Формируем имя файла с использованием шаблонного метода
                filename = self.get_output_filename(data, script_config, target_date, file_pattern)

                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2, default=str)

                logger.info(f"Отчет{source_display} успешно сохранен в {filename}")
                logger.info(f"Извлеченные данные: {data}")
            finally:
                # 9. Завершение работы
                self.close()

        except Exception as e:
            # 10. Обработка исключений
            import traceback
            if logger:
                logger.error(f"Ошибка при парсинге данных из целевой системы{source_display}: {e}")
                logger.error(f"Полный стек трейса: {traceback.format_exc()}")
            else:
                print(f"Ошибка при парсинге данных из целевой системы{source_display}: {e}")
                print(f"Полный стек трейса: {traceback.format_exc()}")
            sys.exit(1)

    # === ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ (минимально необходимые) ===
    
    def terminate_edge_processes(self):
        """Завершает все процессы Microsoft Edge"""
        try:
            subprocess.run(["taskkill", "/f", "/im", "msedge.exe"],
                          stdout=subprocess.DEVNULL,
                          stderr=subprocess.DEVNULL)
            time.sleep(2)  # Ждем 2 секунды, чтобы процессы точно завершились
        except Exception as e:
            print(f"Ошибка при завершении процессов Edge: {e}")

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

    def select_option_from_dropdown(self, dropdown_selector: str, option_selector: str,
                                   option_value: str,
                                   return_url: str = None, expected_url_pattern: str = None,
                                   exact_match: bool = False, text_attribute: str = None,
                                   value_attribute: str = 'value') -> bool:
        """
        Универсальный метод для выбора опции в выпадающем списке

        Args:
            dropdown_selector: XPath или CSS-селектор для выпадающего списка
            option_selector: XPath или CSS-селектор для элементов опций
            option_value: Целевое значение для выбора
            return_url: URL для возврата, если произошел переход на другую страницу
            expected_url_pattern: Паттерн URL для проверки, остались ли на целевой странице
            exact_match: Флаг, указывающий на необходимость точного соответствия
            text_attribute: Атрибут для получения текста (например, 'textContent')
            value_attribute: Атрибут для получения значения (например, 'value' или 'data-value')

        Returns:
            bool: True, если опция была успешно выбрана
        """
        try:
            # Кликаем по выпадающему списку, чтобы открыть опции
            dropdown_element = self.driver.find_element(By.XPATH, dropdown_selector)
            dropdown_element.click()

            # Ждем появление опций
            time.sleep(2)

            # Пытаемся найти все доступные опции в выпадающем списке
            all_option_elements = self.driver.find_elements(By.XPATH, option_selector)

            if self.logger:
                self.logger.debug("Доступные опции в выпадающем списке:")
            available_options = []
            for element in all_option_elements:
                # Ищем текст опции в элементе
                if text_attribute:
                    element_text = element.get_attribute(text_attribute) or element.text
                else:
                    element_text = element.text.strip()

                # Также проверяем значение атрибута, если указан
                element_value = element.get_attribute(value_attribute) if value_attribute else None

                element_text = element_text.strip()
                if element_text and len(element_text) > 3:  # Фильтруем короткие или пустые
                    available_options.append({
                        'text': element_text,
                        'value': element_value
                    })
                    if self.logger:
                        self.logger.debug(f"  - {element_text} (value: {element_value})")

            # Ищем конкретно нужный пункт среди доступных опций
            target_option = None
            for option_element in all_option_elements:
                # Проверяем текст элемента
                if text_attribute:
                    option_text = option_element.get_attribute(text_attribute) or option_element.text
                else:
                    option_text = option_element.text.strip()

                # Проверяем значение атрибута
                option_attr_value = option_element.get_attribute(value_attribute) if value_attribute else None

                if exact_match:
                    if option_value == option_text or (option_attr_value and option_value == option_attr_value):
                        target_option = option_element
                        if self.logger:
                            self.logger.debug(f"Найден элемент для значения {option_value}")
                        break
                else:
                    if option_value in option_text or (option_attr_value and option_value in option_attr_value):
                        target_option = option_element
                        if self.logger:
                            self.logger.debug(f"Найден элемент для значения {option_value} (в виде подстроки)")
                        break

            if target_option:
                # Используем ActionChains для более надежного клика
                actions = ActionChains(self.driver)
                actions.move_to_element(target_option).click().perform()

                if self.logger:
                    self.logger.info(f"Установлено значение: {option_value}")
                time.sleep(2)  # Ждем обновления страницы

                # Если нужно проверить URL и вернуться при необходимости
                if return_url and expected_url_pattern:
                    # Проверяем, остались ли мы на нужной странице
                    current_url = self.driver.current_url
                    if self.logger:
                        self.logger.debug(f"Текущий URL после смены значения: {current_url}")

                    # Если мы не на целевой странице, возвращаемся туда
                    if expected_url_pattern not in current_url:
                        if self.logger:
                            self.logger.info("Мы покинули целевую страницу, возвращаемся...")
                        # Восстанавливаем URL
                        self.driver.get(return_url)
                        time.sleep(3)  # Ждем загрузки страницы
                    else:
                        if self.logger:
                            self.logger.debug("Мы остались на целевой странице")

                return True
            else:
                if self.logger:
                    self.logger.warning(f"Не найдена опция для значения {option_value}")
                    # Исправленный код для получения текстов опций
                    available_texts = [opt['text'] for opt in available_options]
                    self.logger.debug(f"Доступные значения: {available_texts}")
                return False

        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при установке значения в выпадающем списке: {e}")
            return False