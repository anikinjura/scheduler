"""
BaseReportParser.py

Базовый класс для парсинга отчетов из маркетплейсов.

Промежуточный базовый класс между BaseParser и специфичными парсерами отчетов.
"""
__version__ = '1.0.0'

from pathlib import Path
from abc import ABC, abstractmethod
from scheduler_runner.tasks.reports.parser_base.BaseParser import BaseParser
from selenium.webdriver.common.by import By
from datetime import datetime
from typing import Dict, Any


class BaseReportParser(BaseParser, ABC):
    """Базовый класс для парсинга отчетов из маркетплейсов"""

    def __init__(self, config, logger=None):
        super().__init__(config)
        self.logger = logger

    # === АБСТРАКТНЫЕ МЕТОДЫ (обязательны для реализации в конечных скриптах) ===
    
    @abstractmethod
    def get_report_type(self) -> str:
        """Возвращает тип отчета"""
        pass

    @abstractmethod
    def get_report_schema(self) -> Dict[str, Any]:
        """Возвращает схему данных отчета"""
        pass

    @abstractmethod
    def extract_specific_data(self) -> Dict[str, Any]:
        """Извлекает специфичные данные для конкретного типа отчета"""
        pass

    # === РЕАЛИЗОВАННЫЕ МЕТОДЫ ===

    def navigate_to_target_page(self):
        """Навигация к целевой странице"""
        # Используем TARGET_URL, с fallback на ERP_URL для обратной совместимости
        url = self.config.get('TARGET_URL') or self.config.get('ERP_URL')
        self.driver.get(url)
        if self.logger:
            self.logger.info(f"Переход на страницу: {url}")

    def perform_navigation(self):
        """Переопределение шага навигации для отчетных парсеров"""
        self.navigate_to_target_page()

    def login(self):
        """Вход в систему (реализация в дочерних классах)"""
        # В базовом классе отчетов логин просто переходит на URL
        # Используем TARGET_URL, с fallback на ERP_URL для обратной совместимости
        url = self.config.get('TARGET_URL') or self.config.get('ERP_URL')
        self.driver.get(url)
        if self.logger:
            self.logger.info(f"Переход на страницу: {url}")

    def logout(self):
        """Выход из системы (обычно не требуется при использовании существующей сессии)"""
        pass

    def get_output_filename(self, data: Dict[str, Any], script_config: Dict[str, Any],
                           target_date: str, file_pattern: str) -> Path:
        """Получает имя выходного файла для отчетов с учетом ПВЗ и типа отчета"""
        from pathlib import Path
        from scheduler_runner.utils.system import SystemUtils

        date_str = target_date.replace('-', '')  # Преобразуем формат даты для имени файла
        pvz_id = data.get('pvz_info', script_config.get('PVZ_ID', ''))
        # Транслитерируем ПВЗ для использования в имени файла
        translit_pvz = SystemUtils.cyrillic_to_translit(pvz_id) if pvz_id else 'unknown'

        # Используем шаблон из конфигурации для формирования имени файла
        filename_template = file_pattern.replace('{pvz_id}', translit_pvz)\
                                       .replace('{date}', date_str)\
                                       .replace('{report_type}', self.get_report_type())
        output_dir = Path(script_config['OUTPUT_DIR'])
        return output_dir / filename_template

    def extract_data(self) -> Dict[str, Any]:
        """Извлекает данные, объединяя общую и специфичную логику"""
        # Извлекаем общие данные
        common_data = self.extract_common_data()

        # Проверяем, не произошла ли ошибка при извлечении общих данных
        if 'error' in common_data:
            return common_data

        # Извлекаем специфичные данные
        specific_data = self.extract_specific_data()

        # Объединяем общие и специфичные данные
        report_data = {**common_data, **specific_data}

        return report_data

    # === ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ (для расширения в дочерних классах) ===
    
    def extract_common_data(self) -> Dict[str, Any]:
        """Извлекает общие данные для всех типов отчетов"""
        if self.logger:
            self.logger.info(f"Текущий URL: {self.driver.current_url}")
            self.logger.info(f"Заголовок страницы: {self.driver.title}")

        # Извлекаем базовую информацию
        common_data = {
            'data_source': getattr(self, 'DATA_SOURCE_NAME', getattr(self, 'MARKETPLACE_NAME', 'Unknown')),
            'report_type': self.get_report_type(),
            'date': datetime.now().strftime('%Y-%m-%d'),
            'timestamp': datetime.now().isoformat(),
            'page_title': self.driver.title,
            'current_url': self.driver.current_url,
            'raw_data': {
                'page_source_length': len(self.driver.page_source),
                'page_text_length': len(self.driver.find_element(By.TAG_NAME, "body").text)
            }
        }

        return common_data

    def extract_data_from_element_by_pattern(self, element_selector: str, regex_pattern: str, attribute: str = None) -> str:
        """
        Извлекает данные из элемента страницы по селектору, затем применяет регулярное выражение к содержимому элемента

        Args:
            element_selector (str): XPath селектор для поиска элемента на странице
            regex_pattern (str): Регулярное выражение для поиска в тексте элемента
            attribute (str, optional): Имя атрибута для извлечения (если None, извлекается текст)

        Returns:
            str: Найденное значение или пустая строка
        """
        # Сначала извлекаем элемент по селектору
        element_text = self.extract_element_by_xpath(element_selector, attribute)

        # Затем применяем регулярное выражение к извлеченному тексту
        extracted_data = self.extract_data_by_pattern(regex_pattern, element_text)

        return extracted_data

    # === МЕТОДЫ ДЛЯ РАБОТЫ С ИСТОЧНИКАМИ ДАННЫХ ===

    def ensure_data_source(self, required_source: str,
                          source_type: str = 'pvz',
                          selectors: Dict[str, str] = None,
                          config_key: str = 'SELECTORS') -> Dict[str, Any]:
        """
        Проверяет текущий источник данных и устанавливает нужный, если необходимо

        Args:
            required_source: Требуемый источник данных (например, ПВЗ ID)
            source_type: Тип источника данных (по умолчанию 'pvz')
            selectors: Селекторы для работы с источником (если None, использовать из self.config)
            config_key: Ключ для получения селекторов из конфигурации

        Returns:
            Dict[str, Any]: Результат операции с полями:
                - success: bool - успешность операции
                - previous_source: str - предыдущий источник данных
                - current_source: str - текущий источник данных
                - changed: bool - был ли изменен источник данных
                - message: str - сообщение о результате
        """
        # Получаем селекторы из параметра или из конфигурации
        if selectors is None:
            selectors = self.config.get(config_key, {})

        # Извлекаем текущий источник данных
        current_source = self.extract_current_data_source(selectors)
        previous_source = current_source

        result = {
            'success': True,
            'previous_source': previous_source,
            'current_source': current_source,
            'changed': False,
            'message': f'Текущий {source_type} уже установлен правильно: {current_source}'
        }

        # Сравниваем текущий и требуемый источники данных
        if current_source != required_source:
            if self.logger:
                self.logger.info(f"Текущий {source_type} '{current_source}' не совпадает с требуемым '{required_source}'. Устанавливаем нужный {source_type}...")

            # Пытаемся установить нужный источник данных
            success = self.set_data_source(required_source, selectors)

            if success:
                # Обновляем информацию о текущем источнике данных
                current_source = self.extract_current_data_source(selectors)
                result.update({
                    'success': True,
                    'current_source': current_source,
                    'changed': True,
                    'message': f'Успешно установлен {source_type}: {required_source}'
                })

                if self.logger:
                    self.logger.info(f"Успешно установлен {source_type}: {required_source}")
            else:
                result.update({
                    'success': False,
                    'changed': False,
                    'message': f'Не удалось установить {source_type}: {required_source}'
                })

                if self.logger:
                    self.logger.warning(f"Не удалось установить {source_type}: {required_source}")
        else:
            if self.logger:
                self.logger.info(f"{source_type} уже установлен правильно: {current_source}")

        return result

    def extract_current_data_source(self, selectors: Dict[str, str]) -> str:
        """
        Извлекает текущий выбранный источник данных

        Args:
            selectors: Словарь селекторов для извлечения источника данных

        Returns:
            str: Текущий источник данных или 'Unknown' если не найден
        """
        # Определяем возможные селекторы для извлечения текущего источника данных
        source_selectors = [
            selectors.get('PVZ_INPUT_READONLY', ''),
            selectors.get('PVZ_INPUT_CLASS_READONLY', ''),
            selectors.get('PVZ_INPUT', ''),
            selectors.get('DATA_SOURCE_INPUT', ''),
            selectors.get('CURRENT_SOURCE', '')
        ]

        for selector in source_selectors:
            if selector:
                source_text = self.extract_element_by_xpath(selector)
                if source_text and len(source_text.strip()) > 0:
                    return source_text.strip()

        return "Unknown"

    def set_data_source(self, required_source: str,
                       selectors: Dict[str, str]) -> bool:
        """
        Устанавливает нужный источник данных

        Args:
            required_source: Требуемый источник данных
            selectors: Словарь селекторов для установки источника данных

        Returns:
            bool: Успешность операции
        """
        # Получаем селекторы для выпадающего списка и опций
        dropdown_selector = selectors.get('PVZ_DROPDOWN', selectors.get('DATA_SOURCE_DROPDOWN', ''))
        option_selector = selectors.get('PVZ_OPTION', selectors.get('DATA_SOURCE_OPTION', ''))

        if dropdown_selector and option_selector:
            # Используем универсальный метод из базового класса для выбора опции
            success = self.select_option_from_dropdown(
                dropdown_selector=dropdown_selector,
                option_selector=option_selector,
                option_value=required_source,
                exact_match=True
            )

            return success
        else:
            if self.logger:
                self.logger.warning("Не найдены селекторы для установки источника данных")
            return False

    def get_data_source_config(self, source_type: str) -> Dict[str, str]:
        """
        Получает конфигурацию для работы с источником данных

        Args:
            source_type: Тип источника данных (например, 'pvz', 'warehouse', etc.)

        Returns:
            Dict[str, str]: Словарь селекторов для работы с источником данных
        """
        # Возвращаем селекторы из конфигурации в зависимости от типа источника
        selectors = self.config.get('SELECTORS', {})

        # Возвращаем все селекторы, так как они могут содержать нужные для конкретного типа
        return selectors