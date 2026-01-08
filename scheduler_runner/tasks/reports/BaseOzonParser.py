"""
BaseOzonParser.py

Базовый класс для парсинга отчетов из маркетплейса ОЗОН.

Наследуется от BaseParser и добавляет специфичные методы для ОЗОН.
"""
__version__ = '2.0.0'

from abc import ABC, abstractmethod
from scheduler_runner.tasks.reports.BaseParser import BaseParser
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import re
from typing import Dict, Any, List, Tuple
from urllib.parse import unquote
from datetime import datetime

class BaseOzonParser(BaseParser, ABC):
    """Базовый класс для парсинга отчетов из маркетплейса ОЗОН"""

    # Модульные константы для магических строк
    LOGIN_INDICATORS = ['login', 'signin', 'auth']
    MARKETPLACE_NAME = 'Ozon'
    FLOW_TYPE_DIRECT = 'Direct'
    FLOW_TYPE_RETURN = 'Return'
    FLOW_TYPE_UNKNOWN = 'Unknown'
    FOUND_PATTERN = r'Найдено:\s*(\d+)'
    PVZ_KEYWORDS = ['ПВЗ', 'PVZ', 'СОС', 'ЧЕБ', 'КАЗ', 'РОС']

    def __init__(self, config, logger=None):
        super().__init__(config)
        self.logger = logger

    def select_pvz_dropdown_option(self, expected_pvz: str, original_url: str = None) -> bool:
        """
        Специфичный метод для выбора пункта выдачи ОЗОН

        Args:
            expected_pvz: Ожидаемое значение пункта выдачи
            original_url: URL для возврата, если произошел переход на другую страницу

        Returns:
            bool: True, если опция была успешно выбрана
        """
        from selenium.webdriver.common.by import By
        import time

        try:
            # Кликаем по выпадающему списку, чтобы открыть опции
            dropdown_element = self.driver.find_element(By.XPATH, "//div[contains(@class, 'ozi__input-select__inputSelect__UA4xr')]")
            dropdown_element.click()

            # Ждем появления опций
            time.sleep(2)

            # Пытаемся найти все доступные опции в выпадающем списке
            all_option_elements = self.driver.find_elements(By.XPATH, "//div[contains(@class, 'ozi__dropdown-item__dropdownItem__cDZcD')]")

            if self.logger:
                self.logger.debug("Доступные опции в выпадающем списке:")
            available_options = []
            for element in all_option_elements:
                # Ищем название ПВЗ в элементе с классом ozi__data-content__label__TA_HC
                label_elements = element.find_elements(By.XPATH, ".//div[contains(@class, 'ozi__data-content__label__TA_HC')]")
                if label_elements:
                    # Используем textContent атрибут для более точного извлечения текста
                    element_text = label_elements[0].get_attribute("textContent") or label_elements[0].text
                    element_text = element_text.strip()
                    if element_text and len(element_text) > 3:  # Фильтруем короткие или пустые значения
                        available_options.append(element_text)
                        if self.logger:
                            self.logger.debug(f"  - {element_text}")

            # Ищем конкретно нужный пункт выдачи среди доступных опций
            # Сначала проверим, есть ли элемент с ожидаемым текстом в DOM
            target_option = None
            for option_element in all_option_elements:
                label_elements = option_element.find_elements(By.XPATH, ".//div[contains(@class, 'ozi__data-content__label__TA_HC')]")
                if label_elements:
                    label_text = label_elements[0].text.strip()
                    # Проверяем точное совпадение, но также проверим на наличие ожидаемого значения в тексте
                    if expected_pvz == label_text:
                        target_option = option_element
                        if self.logger:
                            self.logger.debug(f"Найден элемент для ПВЗ {expected_pvz}")
                        break
                    elif expected_pvz in label_text:
                        # Если точное совпадение не найдено, но ожидаемое значение содержится в тексте
                        target_option = option_element
                        if self.logger:
                            self.logger.debug(f"Найден элемент для ПВЗ {expected_pvz} (в виде подстроки в '{label_text}')")
                        break
                    else:
                        if self.logger:
                            self.logger.debug(f"Проверен элемент с текстом '{label_text}' (ожидалось '{expected_pvz}')")
                        # Печатаем длину строк для отладки
                        if self.logger:
                            self.logger.debug(f"  Длина ожидаемого текста: {len(expected_pvz)}, длина фактического текста: {len(label_text)}")
                        # Печатаем байты для отладки
                        if self.logger:
                            self.logger.debug(f"  Байты ожидаемого текста: {expected_pvz.encode('utf-8')}")
                            self.logger.debug(f"  Байты фактического текста: {label_text.encode('utf-8')}")

            if target_option:
                # Используем ActionChains для более надежного клика
                from selenium.webdriver.common.action_chains import ActionChains
                actions = ActionChains(self.driver)
                actions.move_to_element(target_option).click().perform()

                if self.logger:
                    self.logger.info(f"Установлен пункт выдачи: {expected_pvz}")
                time.sleep(2)  # Ждем обновления страницы

                # Проверяем, остались ли мы на нужной странице
                current_url = self.driver.current_url
                if self.logger:
                    self.logger.debug(f"Текущий URL после смены ПВЗ: {current_url}")

                # Если мы не на странице отчета, возвращаемся туда
                if "reports/" not in current_url:
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
                    self.logger.warning(f"Не найдена опция для пункта выдачи {expected_pvz}")
                    self.logger.debug(f"Доступные пункты выдачи: {available_options}")
                return False

        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при установке пункта выдачи: {e}")
            return False

    def extract_ozon_data_by_pattern(self, pattern: str, page_text: str = None) -> str:
        """
        Извлекает данные с использованием регулярного выражения, специфичного для ОЗОН

        Args:
            pattern: Регулярное выражение для поиска
            page_text: Текст страницы (если не указан, будет получен из текущей страницы)

        Returns:
            str: Найденное значение или пустая строка
        """
        return self.extract_data_by_pattern(pattern, page_text)

    def extract_ozon_element_by_xpath(self, xpath: str, attribute: str = None) -> str:
        """
        Извлекает данные из элемента по XPath, специфичного для ОЗОН

        Args:
            xpath: XPath для поиска элемента
            attribute: Имя атрибута для извлечения (если None, извлекается текст)

        Returns:
            str: Значение атрибута или текст элемента
        """
        return self.extract_element_by_xpath(xpath, attribute)

    def extract_number_by_selector(self, selector_key: str, wait_time: int = 0) -> int:
        """
        Извлекает числовое значение из элемента по ключу селектора

        Args:
            selector_key: Ключ селектора в конфигурации
            wait_time: Время ожидания перед извлечением (в секундах)

        Returns:
            int: Извлеченное числовое значение или 0 в случае ошибки
        """
        import re
        import time

        try:
            if wait_time > 0:
                time.sleep(wait_time)

            xpath = self.config['SELECTORS'][selector_key]
            element_text = self.extract_ozon_element_by_xpath(xpath, "textContent")

            if element_text:
                numbers = re.findall(r'\d+', element_text)
                if numbers:
                    return int(numbers[0])
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Не удалось извлечь числовое значение по селектору {selector_key}: {e}")

        return 0

    def send_ozon_notification(self, message: str, logger=None) -> bool:
        """
        Отправляет уведомление через Telegram с использованием настроек ОЗОН

        Args:
            message: Текст уведомления
            logger: Логгер для записи информации

        Returns:
            bool: True, если уведомление отправлено успешно
        """
        # Получаем токены из конфигурации
        from scheduler_runner.tasks.reports.config.reports_paths import REPORTS_PATHS

        # Обновляем конфиг с токенами из REPORTS_PATHS
        self.config['TELEGRAM_TOKEN'] = REPORTS_PATHS.get('TELEGRAM_TOKEN')
        self.config['TELEGRAM_CHAT_ID'] = REPORTS_PATHS.get('TELEGRAM_CHAT_ID')

        return self.send_notification(message, logger)

    def _check_authorization_status(self) -> Tuple[bool, Dict[str, Any]]:
        """Проверка статуса авторизации и возврат ошибки при необходимости

        Returns:
            tuple[bool, dict]: (успешно ли авторизован, словарь ошибки если нет)
        """
        current_url = self.driver.current_url.lower()
        is_logged_in = not any(indicator in current_url for indicator in self.LOGIN_INDICATORS)

        if not is_logged_in:
            if self.logger:
                self.logger.warning("Все еще на странице логина - сессия не активна или недостаточно прав")
            error_response = {
                'marketplace': self.MARKETPLACE_NAME,
                'report_type': self.get_report_type(),
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

    def _extract_pvz_info(self) -> str:
        """Извлечение информации о пункте выдачи"""
        from selenium.webdriver.common.by import By
        import re

        # Используем специфичные методы из базового класса ОЗОН для извлечения информации о ПВЗ
        pvz_info = ""

        # Ищем специфичный элемент с информацией о ПВЗ по точным классам и ID
        pvz_value = self.extract_ozon_element_by_xpath(self.config['SELECTORS']['PVZ_INPUT_READONLY'], "value")
        if pvz_value and (any(keyword in pvz_value.upper() for keyword in self.PVZ_KEYWORDS) or '_' in pvz_value):
            pvz_info = pvz_value

        # Если не нашли через специфичный XPath, ищем по классу и атрибуту readonly
        if not pvz_info:
            pvz_value = self.extract_ozon_element_by_xpath(self.config['SELECTORS']['PVZ_INPUT_CLASS_READONLY'], "value")
            if pvz_value and (any(keyword in pvz_value.upper() for keyword in self.PVZ_KEYWORDS) or '_' in pvz_value):
                pvz_info = pvz_value

        # Если не нашли в элементах, ищем в общем тексте
        if not pvz_info:
            page_text = self.driver.find_element(By.TAG_NAME, "body").text
            pvz_matches = re.findall(r'([А-Яа-яЁёA-Za-z_]+\d+)', page_text)
            if pvz_matches:
                # Фильтруем найденные совпадения, оставляя только те, что похожи на названия ПВЗ
                for match in pvz_matches:
                    if '_' in match and any(keyword in match.upper() for keyword in self.PVZ_KEYWORDS):
                        pvz_info = match
                        break
                # Если не нашли подходящий ПВЗ по ключевым словам, берем первый найденный
                if not pvz_info and pvz_matches:
                    pvz_info = pvz_matches[0]

        return pvz_info

    def _substitute_values_in_schema(self, schema: dict, values: dict) -> dict:
        """
        Рекурсивно подставляет значения в шаблон структуры данных.

        Args:
            schema: Шаблон структуры данных с плейсхолдерами
            values: Значения для подстановки

        Returns:
            dict: Структура данных с подставленными значениями
        """
        result = {}

        for key, value in schema.items():
            if isinstance(value, dict):
                # Если значение - словарь, рекурсивно обрабатываем его
                result[key] = self._substitute_values_in_schema(value, values)
            elif isinstance(value, str) and value.startswith('{') and value.endswith('}'):
                # Если значение - плейсхолдер вида {key}, подставляем соответствующее значение
                placeholder = value[1:-1]  # Убираем фигурные скобки
                if placeholder in values:
                    result[key] = values[placeholder]
                else:
                    # Если значение не найдено, используем пустое значение или оставляем плейсхолдер
                    # Вместо оставления плейсхолдера, используем None для лучшей совместимости
                    result[key] = None
            else:
                # В противном случае используем значение как есть
                result[key] = value

        return result

    @abstractmethod
    def get_report_type(self) -> str:
        """Возвращает тип отчета"""
        pass

    @abstractmethod
    def get_default_selectors(self) -> Dict[str, str]:
        """Возвращает селекторы по умолчанию для данного типа отчета"""
        pass

    @abstractmethod
    def extract_specific_data(self) -> Dict[str, Any]:
        """Извлекает специфичные данные для конкретного типа отчета"""
        pass

    @abstractmethod
    def get_report_schema(self) -> Dict[str, Any]:
        """Возвращает схему данных отчета"""
        pass

    def extract_common_data(self) -> Dict[str, Any]:
        """Извлекает общие данные для всех типов отчетов"""
        from selenium.webdriver.common.by import By
        import time

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
                date_match = re.search(r'(?:startSentMoment|endSentMoment|startDate|endDate)%22:%22(\d{4}-\d{2}-\d{2})', current_url_encoded)
                report_date = date_match.group(1) if date_match else datetime.now().strftime('%Y-%m-%d')

                # Извлекаем информацию о ПВЗ с помощью вспомогательного метода
                pvz_info = self._extract_pvz_info()

                # Формируем общие данные
                common_data = {
                    'marketplace': self.MARKETPLACE_NAME,
                    'date': report_date,
                    'timestamp': datetime.now().isoformat(),
                    'page_title': self.driver.title,
                    'current_url': self.driver.current_url,
                    'pvz_info': pvz_info,
                    'raw_data': {
                        'page_source_length': len(self.driver.page_source),
                        'page_text_length': len(self.driver.find_element(By.TAG_NAME, "body").text)
                    }
                }

                if self.logger:
                    self.logger.info(f"Информация о ПВЗ: {pvz_info}")

                return common_data
            except NoSuchElementException as e:
                if self.logger:
                    self.logger.error(f"Не найден элемент на странице: {e}")
                return self.create_error_response(str(e), 'Element not found')
            except TimeoutException as e:
                if self.logger:
                    self.logger.error(f"Таймаут ожидания элемента: {e}")
                return self.create_error_response(str(e), 'Timeout')
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Неожиданная ошибка при извлечении данных: {e}")
                    import traceback
                    self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
                return self.create_error_response(str(e), 'Error extracting data')

    def create_error_response(self, error_message: str, error_type: str) -> Dict[str, Any]:
        """Создает ответ с информацией об ошибке"""
        return {
            'marketplace': self.MARKETPLACE_NAME,
            'report_type': self.get_report_type(),
            'date': datetime.now().strftime('%Y-%m-%d'),
            'timestamp': datetime.now().isoformat(),
            'error': f'{error_type}: {error_message}',
            'current_url': self.driver.current_url,
            'page_title': self.driver.title,
            'pvz_info': '',
        }

    def process_report_data(self, specific_data: Dict[str, Any]) -> Dict[str, Any]:
        """Обрабатывает общую структуру данных с учетом специфичных данных"""
        # Извлекаем общие данные
        common_data = self.extract_common_data()

        if 'error' in common_data:
            return common_data

        # Объединяем общие и специфичные данные
        report_schema = self.get_report_schema()
        all_values = {**common_data, **specific_data}

        # Формируем итоговые данные с подстановкой значений
        processed_data = self._substitute_values_in_schema(report_schema, all_values)

        return processed_data

    def build_url_from_template(self, template: str, **kwargs) -> str:
        """Формирует URL из шаблона с подстановкой параметров"""
        try:
            return template.format(**kwargs)
        except KeyError as e:
            if self.logger:
                self.logger.error(f"Отсутствует обязательный параметр для формирования URL: {e}")
            raise

    def extract_data(self) -> Dict[str, Any]:
        """Извлечение данных из ERP-системы"""
        # Извлекаем специфичные данные
        specific_data = self.extract_specific_data()

        # Обрабатываем общую структуру данных с учетом специфичных данных
        processed_data = self.process_report_data(specific_data)

        return processed_data