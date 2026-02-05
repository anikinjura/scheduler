"""
Базовый класс для парсинга данных из веб-систем

Архитектура:
- Универсальный класс для работы с веб-браузером Edge через Selenium
- Поддержка расширения для других браузеров в будущем
- Гибкая система конфигурации через словарь
- Поддержка различных методов аутентификации
- Расширяемая архитектура для специфичных типов парсеров
- Разделение на публичные и внутренние (с префиксом _) методы
- Использование традиционных имен методов login/logout для аутентификации
- Поддержка настраиваемых параметров через конфигурацию
- Поддержка использования существующей пользовательской сессии

Изменения в версии 0.0.1:
- Метод select_option_from_dropdown переименован в _select_option_from_dropdown
- Метод set_element_value теперь использует _select_option_from_dropdown для работы с выпадающими списками
- _select_option_from_dropdown модифицирован для поддержки работы с уже найденным элементом
"""
__version__ = '0.0.1'

import subprocess
import time
import re
import os
from pathlib import Path
from abc import ABC, abstractmethod
from selenium import webdriver
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from datetime import datetime
from typing import Dict, Any, Union, Optional


class BaseParser(ABC):
    """Базовый класс для парсинга данных из веб-систем"""

    def __init__(self, config: Dict[str, Any], logger=None):
        """
        Инициализация базового парсера

        Args:
            config: Конфигурационный словарь с параметрами для работы парсера
            logger: Объект логгера (если не передан, будет использован внутренний логгер из config)

        Поддерживаемые параметры конфигурации:
            - DEFAULT_TIMEOUT: Таймаут ожидания элементов (по умолчанию 60)
            - ELEMENT_CLICK_TIMEOUT: Таймаут ожидания кликабельности элемента (по умолчанию 10)
            - ELEMENT_WAIT_TIMEOUT: Таймаут ожидания появления элемента (по умолчанию 10)
            - BROWSER_EXECUTABLE: Имя исполняемого файла браузера (по умолчанию 'msedge.exe')
            - BROWSER_USER_DATA_PATH_TEMPLATE: Шаблон пути к данным браузера (по умолчанию 'C:/Users/{username}/AppData/Local/Microsoft/Edge/User Data')
            - PROCESS_TERMINATION_SLEEP: Время ожидания после завершения процессов (по умолчанию 2)
            - DROPDOWN_OPEN_DELAY: Задержка после открытия выпадающего списка (по умолчанию 2)
            - PAGE_UPDATE_DELAY: Задержка после обновления страницы (по умолчанию 2)
            - PAGE_LOAD_DELAY: Задержка после загрузки страницы (по умолчанию 3)
            - EDGE_USER_DATA_DIR: Путь к пользовательским данным Edge
            - HEADLESS: Режим headless для браузера
            - table_configs: Словарь с конфигурациями для извлечения данных из таблиц.
                             Структура: {
                               'table_identifier': {
                                 'table_selector': 'xpath_to_table',
                                 'table_type': 'standard|dynamic',
                                 'table_columns': [
                                   {'name': 'col_name', 'selector': 'xpath_to_cell', 'regex': 'optional_regex'}
                                 ]
                               }
                             }
        """
        # Сохраняем config до установки логгера
        self.config = config

        # Устанавливаем логгер: если передан извне, используем его, иначе создаем внутренний
        if logger is not None:
            self.logger = logger
        elif 'logger' in config and config['logger'] is not None:
            self.logger = config['logger']
        else:
            # Создаем внутренний логгер, если ни внешний, ни из конфига не переданы
            try:
                from scheduler_runner.utils.logging import configure_logger
                self.logger = configure_logger(
                    user=self.config.get("USER", "system"),
                    task_name=self.config.get("TASK_NAME", "BaseParser"),
                    detailed=self.config.get("DETAILED_LOGS", False)
                )
            except Exception as e:
                print(f"Ошибка при создании логгера в BaseParser: {e}")
                self.logger = None

        if self.logger:
            self.logger.trace("Попали в метод BaseParser.__init__")

        self.driver = None

    # === АБСТРАКТНЫЕ МЕТОДЫ (обязательны для реализации в дочерних классах) ===

    @abstractmethod
    def login(self) -> bool:
        """
        Метод для входа в систему

        Returns:
            bool: True, если вход прошел успешно
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseParser.login")
        pass

    @abstractmethod
    def navigate_to_target(self) -> bool:
        """
        Метод для навигации к целевой странице

        Returns:
            bool: True, если навигация прошла успешно
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseParser.navigate_to_target")
        pass

    def extract_data(self) -> Dict[str, Any]:
        """
        Метод для извлечения данных из системы

        Returns:
            Dict[str, Any]: Извлеченные данные
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseParser.extract_data")
        pass

    @abstractmethod
    def logout(self) -> bool:
        """
        Метод для выхода из системы

        Returns:
            bool: True, если выход прошел успешно
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseParser.logout")
        pass

    # === МЕТОДЫ УПРАВЛЕНИЯ БРАУЗЕРОМ (текущая реализация для Edge, с возможностью расширения) ===

    def setup_browser(self, browser_config: Optional[Dict[str, Any]] = None) -> bool:
        """
        Настройка веб-браузера (Edge)

        Метод настраивает браузер Edge с использованием параметров из конфигурации.
        Особое внимание уделяется использованию существующего профиля пользователя:
        - Используется параметр --user-data-dir для указания директории с пользовательскими данными
        - Это позволяет использовать сохраненную сессию, cookies, настройки и авторизацию пользователя
        - Если путь к пользовательским данным не указан, используется путь к данным текущего пользователя
        - Также устанавливаются другие параметры, такие как размер окна, режим headless и т.д.

        Args:
            browser_config: Конфигурация браузера

        Returns:
            bool: True, если браузер успешно настроен
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseParser.setup_browser")
        # Получаем конфигурацию браузера
        config = browser_config or self.config.get('browser_config', {})

        # Завершаем все процессы браузера перед запуском
        self._terminate_browser_processes()

        # Получаем путь к пользовательским данным браузера
        user_data_dir = config.get('user_data_dir', self.config.get('EDGE_USER_DATA_DIR'))
        if not user_data_dir or user_data_dir == "":
            user_data_dir = self._get_default_browser_user_data_dir()

        # Создаем опции для Edge
        options = EdgeOptions()
        options.add_argument(f"--user-data-dir={user_data_dir}")
        options.add_argument("--profile-directory=Default")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        # Устанавливаем headless режим, если указано
        if config.get('headless', self.config.get('HEADLESS', False)):
            options.add_argument("--headless")

        # Устанавливаем размер окна, если указано
        window_size = config.get('window_size', [1920, 1080])
        if window_size and len(window_size) >= 2:
            width, height = window_size[0], window_size[1]
            options.add_argument(f"--window-size={width},{height}")

        # Создаем экземпляр драйвера Edge
        try:
            self.driver = webdriver.Edge(options=options)

            # Устанавливаем таймауты
            timeout = config.get('timeout', self.config.get('DEFAULT_TIMEOUT', 60))
            self.driver.implicitly_wait(timeout)

            return True
        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при настройке браузера Edge: {e}")
            return False

    def close_browser(self):
        """Закрытие браузера и освобождение ресурсов"""
        if self.logger:
            self.logger.trace("Попали в метод BaseParser.close_browser")
        if self.driver:
            self.driver.quit()
            self.driver = None

    # === УНИВЕРСАЛЬНЫЕ МЕТОДЫ РАБОТЫ С ЭЛЕМЕНТАМИ ===

    def get_element_value(self,
                         selector: str,
                         element_type: str = 'input',
                         attribute: Optional[str] = None,
                         pattern: Optional[str] = None) -> str:
        """
        Получение значения элемента

        Args:
            selector: Селектор элемента (XPath, CSS)
            element_type: Тип элемента ('input', 'textarea', 'select', 'div', etc.)
            attribute: Имя атрибута для извлечения (если None, извлекается текст или значение)
            pattern: Регулярное выражение для извлечения части текста (опционально)

        Returns:
            str: Значение элемента или пустая строка
        """
        if self.logger:
            self.logger.trace(f"Попали в метод BaseParser.get_element_value с селектором: {selector}")
        try:
            # Добавляем задержку перед поиском элемента, чтобы дать странице время загрузиться
            import time
            search_delay = self.config.get('ELEMENT_SEARCH_DELAY', 1)
            if search_delay > 0:
                time.sleep(search_delay)

            if self.logger:
                self.logger.debug(f"Поиск элемента по селектору: {selector}")
            element = self.driver.find_element(By.XPATH, selector)
            if self.logger:
                self.logger.debug(f"Элемент найден: {element.tag_name}, текст: '{element.text[:50]}...'")

            if attribute:
                # Получаем значение атрибута
                value = element.get_attribute(attribute)
                if self.logger:
                    self.logger.debug(f"Значение атрибута '{attribute}': {value}")
            elif element_type in ['input', 'textarea', 'select']:
                # Для полей ввода получаем значение атрибута 'value'
                value = element.get_attribute('value') or element.text
                if self.logger:
                    self.logger.debug(f"Значение элемента типа {element_type}: {value}")
            elif element_type in ['button', 'div', 'span', 'label', 'p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                # Для других элементов получаем текст
                value = element.text
                if self.logger:
                    self.logger.debug(f"Текст элемента типа {element_type}: {value[:100]}...")
            elif element_type in ['checkbox', 'radio']:
                # Для чекбоксов и радио-кнопок делегируем получение состояния вспомогательному методу
                value = self._get_checkbox_state(element)
                if self.logger:
                    self.logger.debug(f"Состояние чекбокса/радио: {value}")
            else:
                # По умолчанию получаем текст элемента
                value = element.text
                if self.logger:
                    self.logger.debug(f"Текст элемента по умолчанию: {value[:100]}...")

            result = value.strip() if value else ""

            # Применяем регулярное выражение, если оно задано
            if pattern and result:
                import re
                if self.logger:
                    self.logger.debug(f"Применяем регулярное выражение: {pattern}")
                matches = re.findall(pattern, result)
                if self.logger:
                    self.logger.debug(f"Найденные совпадения: {matches}")
                if matches:
                    result = matches[0]  # Берем первое совпадение
                    if self.logger:
                        self.logger.debug(f"После применения регулярного выражения: '{result}'")

            if self.logger:
                self.logger.debug(f"Возвращаемое значение: '{result}'")
            return result
        except Exception as e:
            if self.logger:
                self.logger.debug(f"Ошибка при получении значения элемента {selector}: {e}")
            if self.logger:
                self.logger.warning(f"Не удалось получить значение элемента {selector}: {e}")
            return ""

    def set_element_value(self,
                         selector: str,
                         value: str,
                         element_type: str = 'input',
                         clear_before_set: bool = True,
                         **kwargs) -> bool:
        """
        Установка значения элемента

        Args:
            selector: Селектор элемента (XPath, CSS)
            value: Значение для установки
            element_type: Тип элемента ('input', 'textarea', 'dropdown', 'checkbox', etc.)
            clear_before_set: Очищать ли поле перед установкой значения
            **kwargs: Дополнительные аргументы для работы с выпадающими списками и другими элементами

        Returns:
            bool: True, если значение успешно установлено

        Note:
            Для элементов типа 'dropdown' вызывает метод _select_option_from_dropdown(),
            передавая ему найденный элемент и значение для установки.
            Для элементов типа 'checkbox' и 'radio' вызывает метод _set_checkbox_state().
        """
        if self.logger:
            self.logger.trace(f"Попали в метод BaseParser.set_element_value с селектором: {selector} и значением: {value}")
        try:
            element = self.driver.find_element(By.XPATH, selector)

            if element_type == 'dropdown':
                # Для выпадающих списков делегируем работу вспомогательному методу
                # Если передан option_selector, используем режим с селекторами, иначе режим с элементом
                if 'option_selector' in kwargs:
                    # Используем режим с селекторами, так как нужен специфичный option_selector
                    dropdown_selector = selector
                    option_selector = kwargs.pop('option_selector')
                    return self._select_option_from_dropdown(
                        dropdown_selector=dropdown_selector,
                        option_selector=option_selector,
                        option_value=value,
                        **kwargs
                    )
                else:
                    # Используем режим с элементом
                    return self._select_option_from_dropdown(element=element, option_value=value, **kwargs)
            elif element_type in ['checkbox', 'radio']:
                # Для чекбоксов и радио-кнопок делегируем работу вспомогательному методу
                target_state = value.lower() == 'true'
                return self._set_checkbox_state(element=element, target_state=target_state)
            else:
                # Для обычных полей ввода
                if clear_before_set:
                    element.clear()
                element.send_keys(value)

                # Проверяем, что значение действительно установлено
                current_value = element.get_attribute('value') or element.text
                return str(current_value).strip() == str(value).strip()

        except Exception as e:
            if self.logger:
                self.logger.warning(f"Не удалось установить значение элемента {selector}: {e}")
            return False

    def _click_element(self, selector: str, wait_for_clickable: bool = True, timeout: Optional[int] = None) -> bool:
        """
        Клик по элементу

        Args:
            selector: Селектор элемента (XPath, CSS)
            wait_for_clickable: Ждать ли, пока элемент станет кликабельным
            timeout: Время ожидания (если не указано, используется значение из конфига)

        Returns:
            bool: True, если клик прошел успешно
        """
        if self.logger:
            self.logger.trace(f"Попали в метод BaseParser._click_element с селектором: {selector}")
        # Получаем таймаут из параметра, конфига или используем значение по умолчанию
        wait_timeout = timeout or self.config.get('ELEMENT_CLICK_TIMEOUT', 10)

        try:
            if wait_for_clickable:
                element = WebDriverWait(self.driver, wait_timeout).until(
                    EC.element_to_be_clickable((By.XPATH, selector))
                )
            else:
                element = self.driver.find_element(By.XPATH, selector)

            element.click()
            return True
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Не удалось кликнуть по элементу {selector}: {e}")
            return False


    # === ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ===

    def _terminate_browser_processes(self):
        """Завершает все процессы Microsoft Edge"""
        if self.logger:
            self.logger.trace("Попали в метод BaseParser._terminate_browser_processes")
        try:
            browser_executable = self.config.get('BROWSER_EXECUTABLE', 'msedge.exe')
            subprocess.run(["taskkill", "/f", "/im", browser_executable],
                          stdout=subprocess.DEVNULL,
                          stderr=subprocess.DEVNULL)
            # Ждем, чтобы процессы точно завершились
            sleep_time = self.config.get('PROCESS_TERMINATION_SLEEP', 2)
            time.sleep(sleep_time)
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Ошибка при завершении процессов {self.config.get('BROWSER_EXECUTABLE', 'msedge.exe')}: {e}")

    def _get_default_browser_user_data_dir(self, username: Optional[str] = None) -> str:
        """
        Возвращает путь к пользовательским данным браузера для указанного пользователя.

        Args:
            username: Имя пользователя (если None, используется текущий пользователь)

        Returns:
            str: Путь к пользовательским данным браузера
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseParser._get_default_browser_user_data_dir")
        if username is None:
            username = self._get_current_user()

        # Получаем путь к данным браузера из конфига или используем значение по умолчанию
        default_path_template = self.config.get('BROWSER_USER_DATA_PATH_TEMPLATE',
                                               "C:/Users/{username}/AppData/Local/Microsoft/Edge/User Data")
        return default_path_template.format(username=username)

    def _get_current_user(self) -> str:
        """
        Возвращает имя текущего пользователя системы.

        Returns:
            str: Имя текущего пользователя
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseParser._get_current_user")
        return os.getlogin()



    def _get_checkbox_state(self, element) -> str:
        """
        Получает состояние чекбокса или радио-кнопки

        Args:
            element: WebElement чекбокса или радио-кнопки

        Returns:
            str: Состояние элемента ('True' - отмечен, 'False' - не отмечен)
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseParser._get_checkbox_state")
        try:
            # Получаем состояние элемента
            state = element.is_selected()
            return str(state)

        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при получении состояния чекбокса: {e}")
            return "False"

    def _set_checkbox_state(self, element, target_state: bool) -> bool:
        """
        Устанавливает состояние чекбокса или радио-кнопки

        Args:
            element: WebElement чекбокса или радио-кнопки
            target_state: Целевое состояние (True - отмечен, False - не отмечен)

        Returns:
            bool: True, если состояние успешно установлено
        """
        if self.logger:
            self.logger.trace(f"Попали в метод BaseParser._set_checkbox_state с целевым состоянием: {target_state}")
        try:
            # Получаем текущее состояние элемента
            current_state = element.is_selected()

            # Если текущее состояние не соответствует целевому, кликаем по элементу
            if current_state != target_state:
                element.click()

            # Проверяем, что состояние действительно стало целевым
            final_state = element.is_selected()
            return final_state == target_state

        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при установке состояния чекбокса: {e}")
            return False

    def _select_option_from_dropdown(self,
                                   dropdown_selector: str = None,
                                   option_selector: str = None,
                                   option_value: str = None,
                                   element = None,
                                   return_url: Optional[str] = None,
                                   expected_url_pattern: Optional[str] = None,
                                   exact_match: bool = False,
                                   text_attribute: Optional[str] = None,
                                   value_attribute: str = 'value') -> bool:
        """
        Универсальный метод для выбора опции в выпадающем списке

        Args:
            dropdown_selector: XPath или CSS-селектор для выпадающего списка (опционально, если передан element)
            option_selector: XPath или CSS-селектор для элементов опций (опционально, если передан element)
            option_value: Целевое значение для выбора
            element: Готовый элемент выпадающего списка (опционально, если переданы селекторы)
            return_url: URL для возврата, если произошел переход на другую странице
            expected_url_pattern: Паттерн URL для проверки, остались ли на целевой странице
            exact_match: Флаг, указывающий на необходимость точного соответствия
            text_attribute: Атрибут для получения текста (например, 'textContent')
            value_attribute: Атрибут для получения значения (например, 'value' или 'data-value')

        Returns:
            bool: True, если опция была успешно выбрана

        Note:
            Метод поддерживает два режима работы:
            1. Поиск элементов по селекторам (старый режим) - используется, когда element=None
            2. Работа с уже найденным элементом (новый режим) - используется, когда передан element
               В этом случае применяется стандартный подход через selenium.webdriver.support.ui.Select
        """
        if self.logger:
            self.logger.trace(f"Попали в метод BaseParser._select_option_from_dropdown с значением: {option_value}")
        try:
            # Если передан готовый элемент, используем его, иначе находим по селектору
            if element is not None:
                dropdown_element = element
            else:
                # Кликаем по выпадающему списку, чтобы открыть опции
                click_timeout = self.config.get('ELEMENT_CLICK_TIMEOUT', 10)
                self._click_element(dropdown_selector, timeout=click_timeout)

                # Ждем появление опций
                time.sleep(self.config.get('DROPDOWN_OPEN_DELAY', 2))

                # Пытаемся найти все доступные опции в выпадающем списке
                all_option_elements = self.driver.find_elements(By.XPATH, option_selector)

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
                            break
                    else:
                        if option_value in option_text or (option_attr_value and option_value in option_attr_value):
                            target_option = option_element
                            break

                if target_option:
                    # Используем ActionChains для более надежного клика
                    actions = ActionChains(self.driver)
                    actions.move_to_element(target_option).click().perform()

                    # Ждем обновления страницы
                    time.sleep(self.config.get('PAGE_UPDATE_DELAY', 2))

                    # Если нужно проверить URL и вернуться при необходимости
                    if return_url and expected_url_pattern:
                        # Проверяем, остались ли мы на нужной странице
                        current_url = self.driver.current_url

                        # Если мы не на целевой странице, возвращаемся туда
                        if expected_url_pattern not in current_url:
                            self.driver.get(return_url)
                            # Ждем загрузки страницы
                            time.sleep(self.config.get('PAGE_LOAD_DELAY', 3))

                    return True
                else:
                    if self.logger:
                        self.logger.warning(f"Не найдена опция для значения {option_value}")
                    return False

            # Если был передан готовый элемент, используем стандартный способ выбора опции
            if element is not None:
                from selenium.webdriver.support.ui import Select
                select = Select(dropdown_element)

                # Пытаемся выбрать по значению, тексту или индексу
                try:
                    select.select_by_value(option_value)
                except:
                    try:
                        select.select_by_visible_text(option_value)
                    except:
                        try:
                            select.select_by_index(int(option_value))
                        except:
                            return False
                return True

        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при установке значения в выпадающем списке: {e}")
            return False

    def extract_table_data(self, table_config_key: str = None, table_config: dict = None) -> list:
        """
        Извлечение данных из HTML-таблицы.

        Args:
            table_config_key: Ключ в конфигурации (self.config['table_configs']), по которому находится конфигурация таблицы.
                              Если указан, используется конфигурация из self.config['table_configs'][table_config_key].
            table_config: Явно переданная конфигурация таблицы. Если указан, используется вместо поиска по ключу.
                          Приоритет выше, чем у table_config_key.

        Returns:
            list: Список словарей, где каждый словарь представляет строку таблицы с ключами, соответствующими
                  именам колонок из конфигурации. Возвращает пустой список в случае ошибки или отсутствия данных.
        """
        if self.logger:
            self.logger.trace(f"Попали в метод BaseParser.extract_table_data с ключом: {table_config_key}")
        # Определяем, какую конфигурацию использовать
        config_to_use = None
        if table_config:
            config_to_use = table_config
        elif table_config_key and isinstance(self.config, dict):
            # Ищем table_config_key внутри self.config['table_configs']
            table_configs = self.config.get('table_configs', {})
            if table_config_key in table_configs:
                config_to_use = table_configs[table_config_key]
            else:
                if self.logger:
                    self.logger.error(f"Конфигурация таблицы не найдена для ключа '{table_config_key}' в 'table_configs'.")
                return []
        else:
            if self.logger:
                self.logger.error(f"Конфигурация таблицы не найдена. "
                                  f"Проверьте table_config_key='{table_config_key}' или передайте table_config.")
            return []

        # Извлекаем параметры из конфигурации
        table_selector = config_to_use.get("table_selector", "")
        columns_config = config_to_use.get("table_columns", [])
        table_type = config_to_use.get("table_type", "standard") # По умолчанию стандартная таблица

        if not table_selector or not isinstance(columns_config, list):
            if self.logger:
                self.logger.error(f"Некорректная конфигурация таблицы по ключу '{table_config_key}'. "
                                  f"Проверьте 'table_selector' и 'table_columns'.")
            return []

        try:
            # Находим таблицу
            table_element = self.driver.find_element(By.XPATH, table_selector)
            if not table_element:
                if self.logger:
                    self.logger.warning(f"Таблица не найдена по селектору: {table_selector}")
                return []

            # Определяем тип таблицы и применяем соответствующую логику
            if table_type == 'standard':
                return self._extract_standard_table_data(table_element, columns_config)
            elif table_type == 'dynamic':
                # Здесь может быть логика для динамических таблиц (прокрутка, ожидание загрузки)
                # Пока используем стандартную как fallback
                if self.logger:
                     self.logger.info(f"Обработка динамической таблицы как стандартной. "
                                      f"Расширьте логику для полной поддержки динамических таблиц.")
                return self._extract_standard_table_data(table_element, columns_config)
            else:
                # Неизвестный тип, используем стандартную как fallback
                if self.logger:
                    self.logger.warning(f"Неизвестный тип таблицы '{table_type}', используем стандартную логику.")
                return self._extract_standard_table_data(table_element, columns_config)

        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при извлечении данных из таблицы (ключ: '{table_config_key}'): {e}")
            return [] # Возвращаем пустой список в случае ошибки

    def _extract_standard_table_data(self, table_element, columns_config: list) -> list:
        """
        Внутренний метод для извлечения данных из стандартной HTML-таблицы (thead/tbody).
        Использует Selenium для поиска элементов.

        Args:
            table_element: WebElement объект таблицы.
            columns_config: Список словарей с конфигурацией колонок.

        Returns:
            list: Список словарей с данными строк.
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseParser._extract_standard_table_data")
        rows_data = []

        # Находим все строки tbody
        try:
            rows = table_element.find_elements(By.XPATH, ".//tbody/tr")
        except Exception:
            # Если tbody нет, пробуем найти все tr внутри table
            try:
                rows = table_element.find_elements(By.XPATH, ".//tr[not(./th)]") # Исключаем строки с th, если они есть
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"Не удалось найти строки таблицы (tbody/tr).")
                return [] # Возвращаем пустой список, если строки не найдены

        for i, row in enumerate(rows):
            row_data = {}
            # Проходим по каждой колонке в конфигурации
            for col_config in columns_config:
                col_name = col_config.get('name', f'column_{len(row_data)}') # Генерируем имя, если не задано
                cell_selector = col_config.get('selector')
                regex_pattern = col_config.get('regex')

                if not cell_selector:
                    if self.logger:
                        self.logger.warning(f"Для колонки '{col_name}' не задан 'selector'. Пропускаем.")
                    row_data[col_name] = ""
                    continue

                try:
                    # Находим ячейку в строке
                    cell_element = row.find_element(By.XPATH, cell_selector)

                    # Извлекаем текст
                    cell_text = cell_element.text.strip()

                    # Если текст пуст, пробуем получить его из атрибута textContent
                    if not cell_text:
                        cell_text_content = cell_element.get_attribute("textContent")
                        if cell_text_content:
                            cell_text = cell_text_content.strip()

                    # Применяем регулярное выражение, если задано
                    if regex_pattern:
                        import re
                        matches = re.findall(regex_pattern, cell_text)
                        if matches:
                            cell_text = matches[0]  # Берем первое совпадение
                        else:
                            cell_text = "" # Или оставить исходный текст, если совпадений нет

                    row_data[col_name] = cell_text
                except Exception as e:
                    if self.logger:
                        self.logger.warning(f"Не удалось извлечь данные для колонки '{col_name}' в строке {i}: {e}")
                    row_data[col_name] = "" # Устанавливаем пустую строку в случае ошибки

            rows_data.append(row_data)

        return rows_data

    # === МЕТОД ЗАПУСКА ПАРСЕРА ===

    def run_parser(self) -> Dict[str, Any]:
        """
        Метод запуска парсера, определяющий последовательность вызова абстрактных методов

        Returns:
            Dict[str, Any]: Извлеченные данные
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseParser.run_parser")
        try:
            # 1. Настройка браузера
            if not self.setup_browser():
                raise Exception("Не удалось настроить браузер")

            # 2. Вход в систему
            if not self.login():
                raise Exception("Не удалось выполнить вход в систему")

            # 3. Навигация к целевой странице
            if not self.navigate_to_target():
                raise Exception("Не удалось выполнить навигацию к целевой странице")

            # 4. Извлечение данных
            data = self.extract_data()

            # 5. Выход из системы
            if not self.logout():
                if self.logger:
                    self.logger.warning("Не удалось корректно выйти из системы")

            return data

        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при выполнении парсинга: {e}")
            raise
        finally:
            # 6. Закрытие браузера
            self.close_browser()
