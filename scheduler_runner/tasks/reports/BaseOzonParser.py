"""
BaseOzonParser.py

Базовый класс для парсинга отчетов из маркетплейса ОЗОН.

Наследуется от BaseParser и добавляет специфичные методы для ОЗОН.
"""
__version__ = '1.0.0'

from scheduler_runner.tasks.reports.BaseParser import BaseParser
from selenium.webdriver.common.by import By
import time
import re

class BaseOzonParser(BaseParser):
    """Базовый класс для парсинга отчетов из маркетплейса ОЗОН"""

    def __init__(self, config):
        super().__init__(config)

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

            print("Доступные опции в выпадающем списке:")
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
                        print(f"  - {element_text}")

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
                        print(f"Найден элемент для ПВЗ {expected_pvz}")
                        break
                    elif expected_pvz in label_text:
                        # Если точное совпадение не найдено, но ожидаемое значение содержится в тексте
                        target_option = option_element
                        print(f"Найден элемент для ПВЗ {expected_pvz} (в виде подстроки в '{label_text}')")
                        break
                    else:
                        print(f"Проверен элемент с текстом '{label_text}' (ожидалось '{expected_pvz}')")
                        # Печатаем длину строк для отладки
                        print(f"  Длина ожидаемого текста: {len(expected_pvz)}, длина фактического текста: {len(label_text)}")
                        # Печатаем байты для отладки
                        print(f"  Байты ожидаемого текста: {expected_pvz.encode('utf-8')}")
                        print(f"  Байты фактического текста: {label_text.encode('utf-8')}")

            if target_option:
                # Используем ActionChains для более надежного клика
                from selenium.webdriver.common.action_chains import ActionChains
                actions = ActionChains(self.driver)
                actions.move_to_element(target_option).click().perform()

                print(f"Установлен пункт выдачи: {expected_pvz}")
                time.sleep(2)  # Ждем обновления страницы

                # Проверяем, остались ли мы на нужной странице
                current_url = self.driver.current_url
                print(f"Текущий URL после смены ПВЗ: {current_url}")

                # Если мы не на странице отчета, возвращаемся туда
                if "reports/" not in current_url:
                    print("Мы покинули страницу отчета, возвращаемся...")
                    # Восстанавливаем URL с фильтрами
                    self.driver.get(original_url)
                    time.sleep(3)  # Ждем загрузки страницы
                else:
                    print("Мы остались на странице отчета")

                return True
            else:
                print(f"Не найдена опция для пункта выдачи {expected_pvz}")
                print(f"Доступные пункты выдачи: {available_options}")
                return False

        except Exception as e:
            print(f"Ошибка при установке пункта выдачи: {e}")
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