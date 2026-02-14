"""
Базовый класс для парсинга отчетов из системы управления ПВЗ Ozon

Архитектура:
- BaseParser (абстрактный класс) → BaseReportParser (абстрактный класс) → OzonReportParser (абстрактный класс) → конкретные реализации
- Добавляет специфичную логику для работы с Ozon ПВЗ
- Включает методы для проверки и установки нужного ПВЗ
- Переопределяет метод navigate_to_target для проверки ПВЗ
- Обеспечивает единообразную работу с ПВЗ для всех дочерних классов

=== ОПИСАНИЕ МЕТОДОВ ===

Метод ensure_correct_pvz() - проверяет, что на странице выбран правильный ПВЗ и при необходимости устанавливает нужный
Метод get_current_pvz() - извлекает текущий выбранный ПВЗ с помощью селектора из конфигурации
Метод set_pvz() - устанавливает нужный ПВЗ в выпадающем списке
Метод navigate_to_target() - переопределен для проверки ПВЗ после перехода на целевую страницу
Метод build_url_filter() - вспомогательный метод, собирающий общий фильтр в URL по шаблону из конфига
Метод extract_report_data() - переопределен для добавления информации о ПВЗ в результат

=== АБСТРАКТНЫЕ МЕТОДЫ (должны быть реализованы в дочернем классе) ===

Метод get_report_type() - возвращает тип отчета (например, 'sales_report', 'inventory_report', 'giveout_report')
Метод get_report_schema() - возвращает схему данных отчета
Метод extract_report_data() - извлекает специфичные данные отчета
Метод login() - реализует логику аутентификации в системе
Метод logout() - реализует логику завершения сессии

=== ПОСЛЕДОВАТЕЛЬНОСТЬ ВЫПОЛНЕНИЯ МЕТОДОВ В ДОЧЕРНЕМ КЛАССЕ ===

1. __init__() - инициализация парсера с конфигурацией
2. run_parser() (наследуется из BaseReportParser) - запускает последовательность выполнения:
   a. setup_browser() - настройка браузера
   b. login() - вход в систему (реализуется в дочернем классе)
   c. navigate_to_target() - навигация к целевой странице (реализация в этом классе)
      - build_url_filter() - формирование фильтра для URL
      - ensure_correct_pvz() - проверка и установка правильного ПВЗ
        - get_current_pvz() - получение текущего ПВЗ
        - set_pvz() - установка нужного ПВЗ при необходимости
   d. extract_data() (наследуется из BaseReportParser) - извлечение данных
      - collect_report_data() (наследуется из BaseReportParser)
        - extract_report_data() - извлечение специфичных данных (реализуется в дочернем классе)
      - validate_report_data() (наследуется из BaseReportParser) - валидация данных
      - extract_report_data() - добавление информации о ПВЗ (переопределен в этом классе)
   e. save_report() (наследуется из BaseReportParser) - сохранение отчета
   f. logout() - выход из системы (реализуется в дочернем классе)
   g. close_browser() - закрытие браузера
"""
__version__ = '0.0.1'

from .base_report_parser import BaseReportParser
from ..configs.base_configs.ozon_report_config import OZON_BASE_CONFIG
from typing import Dict, Any
import time


class OzonReportParser(BaseReportParser):
    """Базовый класс для парсинга отчетов из системы управления ПВЗ Ozon"""

    def __init__(self, config: Dict[str, Any], args=None, logger=None):
        """
        Инициализация базового парсера отчетов Ozon

        Args:
            config: Конфигурационный словарь с параметрами для работы парсера
            args: Аргументы командной строки (если не переданы, будут разобраны из sys.argv)
            logger: Объект логгера (если не передан, будет использован внутренний логгер)
        """
        # Обновляем конфиг, если не передан специфический
        if config == {} or config is None:
            config = OZON_BASE_CONFIG.copy()
        super().__init__(config, args, logger)

        if self.logger:
            self.logger.trace("Попали в метод OzonReportParser.__init__")

    def get_current_pvz(self) -> str:
        """
        Извлекает текущий выбранный ПВЗ с помощью селектора из конфигурации

        Returns:
            str: Текущий выбранный ПВЗ или 'Unknown', если не удалось извлечь
        """
        if self.logger:
            self.logger.trace("Попали в метод OzonReportParser.get_current_pvz")
        try:
            # Получаем селектор для ПВЗ из конфигурации
            pvz_selectors = self.config.get("selectors", {}).get("pvz_selectors", {})

            if self.logger:
                self.logger.debug(f"Селекторы ПВЗ: {pvz_selectors}")

            pvz_input_selector = pvz_selectors.get("input_class_readonly",
                                                 pvz_selectors.get("input_readonly",
                                                                 pvz_selectors.get("input")))

            if not pvz_input_selector:
                if self.logger:
                    self.logger.error("Не найден селектор для получения текущего ПВЗ")
                return "Unknown"

            if self.logger:
                self.logger.debug(f"Используемый селектор для получения ПВЗ: {pvz_input_selector}")

            # Используем метод из базового класса для извлечения значения
            current_pvz = self.get_element_value(
                selector=pvz_input_selector,
                element_type="input"
            )

            if self.logger:
                self.logger.info(f"Текущий ПВЗ: {current_pvz}")

            return current_pvz if current_pvz else "Unknown"
        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при извлечении текущего ПВЗ: {e}")
                import traceback
                self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
            return "Unknown"

    def set_pvz(self, target_pvz: str) -> bool:
        """
        Устанавливает нужный ПВЗ в выпадающем списке

        Args:
            target_pvz: Целевой ПВЗ для установки

        Returns:
            bool: True, если ПВЗ успешно установлен
        """
        if self.logger:
            self.logger.trace("Попали в метод OzonReportParser.set_pvz")
        try:
            # Получаем селекторы для ПВЗ из конфигурации
            selectors = self.config.get("selectors", {}).get("pvz_selectors", {})

            if self.logger:
                self.logger.debug(f"Селекторы для установки ПВЗ: {selectors}")

            dropdown_selector = selectors.get("dropdown")
            option_selector = selectors.get("option")

            if not dropdown_selector or not option_selector:
                if self.logger:
                    self.logger.error("Не найдены селекторы для установки ПВЗ")
                return False

            if self.logger:
                self.logger.debug(f"Селектор выпадающего списка: {dropdown_selector}")
                self.logger.debug(f"Селектор опции: {option_selector}")
                self.logger.debug(f"Целевой ПВЗ для установки: {target_pvz}")

            # Используем метод из базового класса для установки значения в выпадающем списке
            success = self.set_element_value(
                selector=dropdown_selector,
                value=target_pvz,
                element_type="dropdown",
                option_selector=option_selector
            )

            if success:
                if self.logger:
                    self.logger.info(f"ПВЗ успешно установлен: {target_pvz}")
                # Ждем немного, чтобы изменения вступили в силу
                time.sleep(2)
            else:
                if self.logger:
                    self.logger.error(f"Не удалось установить ПВЗ: {target_pvz}")

            return success
        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при установке ПВЗ: {e}")
                import traceback
                self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
            return False

    def ensure_correct_pvz(self) -> bool:
        """
        Проверяет, что на странице выбран правильный ПВЗ и при необходимости устанавливает нужный

        Returns:
            bool: True, если правильный ПВЗ выбран или успешно установлен
        """
        if self.logger:
            self.logger.trace("Попали в метод OzonReportParser.ensure_correct_pvz")
        try:
            # Получаем требуемый ПВЗ из конфигурации
            required_pvz = self.config.get("additional_params", {}).get("location_id", "")

            if not required_pvz or required_pvz == "":
                if self.logger:
                    self.logger.error("Не указан требуемый ПВЗ в конфигурации")
                return False

            if self.logger:
                self.logger.debug(f"Требуемый ПВЗ: {required_pvz}")

            # Получаем текущий ПВЗ
            current_pvz = self.get_current_pvz()

            if self.logger:
                self.logger.debug(f"Текущий ПВЗ: {current_pvz}")

            if current_pvz == required_pvz:
                if self.logger:
                    self.logger.info(f"Правильный ПВЗ уже установлен: {required_pvz}")
                return True
            else:
                if self.logger:
                    self.logger.info(f"Текущий ПВЗ '{current_pvz}' отличается от требуемого '{required_pvz}'. Устанавливаем нужный...")

                # Устанавливаем нужный ПВЗ
                success = self.set_pvz(required_pvz)

                if success:
                    # Проверяем, что ПВЗ действительно изменился
                    new_pvz = self.get_current_pvz()

                    if self.logger:
                        self.logger.debug(f"ПВЗ после установки: {new_pvz}")

                    if new_pvz == required_pvz:
                        if self.logger:
                            self.logger.info(f"Успешно установлен правильный ПВЗ: {required_pvz}")

                        # После установки правильного ПВЗ может произойти переход на другую страницу
                        # Поэтому нужно снова перейти на целевую страницу
                        if self.logger:
                            self.logger.info("После установки ПВЗ возвращаемся на целевую страницу")

                        # Сохраняем текущий URL для восстановления
                        original_target_url = self.config.get('target_url', '')

                        # Повторно выполняем навигацию к целевой странице
                        nav_success = super().navigate_to_target()

                        if nav_success:
                            # Проверяем, что ПВЗ все еще правильный после навигации
                            final_pvz = self.get_current_pvz()
                            if self.logger:
                                self.logger.debug(f"ПВЗ после повторной навигации: {final_pvz}")
                            
                            if final_pvz == required_pvz:
                                if self.logger:
                                    self.logger.info(f"После повторной навигации ПВЗ по-прежнему правильный: {required_pvz}")
                                return True
                            else:
                                # Если ПВЗ изменился после навигации, снова устанавливаем правильный
                                if self.logger:
                                    self.logger.info(f"ПВЗ изменился после навигации ({final_pvz}), устанавливаем правильный снова...")
                                return self.set_pvz(required_pvz) and self.get_current_pvz() == required_pvz
                        else:
                            if self.logger:
                                self.logger.error("Не удалось вернуться на целевую страницу после установки ПВЗ")
                            return False
                    else:
                        if self.logger:
                            self.logger.error(f"ПВЗ не изменился после установки. Текущий: {new_pvz}, требуемый: {required_pvz}")
                        return False
                else:
                    if self.logger:
                        self.logger.error(f"Не удалось установить требуемый ПВЗ: {required_pvz}")
                    return False
        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при проверке/установке ПВЗ: {e}")
                import traceback
                self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
            return False


    def navigate_to_target(self) -> bool:
        """
        Переопределенный метод навигации к целевой странице.
        Сначала выполняет логику родительского класса (вычисление target_url и навигация),
        затем добавляет специфичную логику проверки/установки правильного ПВЗ.

        Returns:
            bool: True, если навигация и проверка ПВЗ прошли успешно
        """
        if self.logger:
            self.logger.trace("Попали в метод OzonReportParser.navigate_to_target")
        # Сначала выполнить логику родительского метода (вычисление target_url и навигация)
        if self.logger:
            self.logger.debug("Выполнение базовой навигации к целевой странице")
        parent_result = super().navigate_to_target()

        if not parent_result:
            if self.logger:
                self.logger.error("Не удалось выполнить базовую навигацию к целевой странице")
            return False

        # После навигации проверяем и при необходимости устанавливаем правильный ПВЗ
        if self.logger:
            self.logger.debug("Проверка и установка правильного ПВЗ")
        pvz_success = self.ensure_correct_pvz()

        if not pvz_success:
            if self.logger:
                self.logger.error("Не удалось установить правильный ПВЗ после навигации")
            return False

        # После установки правильного ПВЗ добавляем задержку, чтобы дать странице время обновиться
        import time
        page_load_delay = self.config.get('PAGE_LOAD_DELAY', 3)
        if page_load_delay > 0:
            if self.logger:
                self.logger.debug(f"Ожидание загрузки страницы: {page_load_delay} секунд")
            time.sleep(page_load_delay)

        if self.logger:
            self.logger.debug("Навигация и проверка ПВЗ завершены успешно")
        return True

    def extract_report_data(self) -> Dict[str, Any]:
        """
        Переопределенный метод извлечения данных отчета.
        Добавляет информацию о ПВЗ в результат.

        Returns:
            Dict[str, Any]: Словарь с данными отчета
        """
        if self.logger:
            self.logger.trace("Попали в метод OzonReportParser.extract_report_data")
        try:
            if self.logger:
                self.logger.debug("Начало извлечения данных отчета")
            # Получаем базовые данные отчета
            report_data = super().extract_report_data()
            if self.logger:
                self.logger.debug(f"Базовые данные отчета получены: {list(report_data.keys())}")

            # Добавляем информацию о ПВЗ
            current_pvz = self.get_current_pvz()
            if self.logger:
                self.logger.debug(f"Текущий ПВЗ: {current_pvz}")
            report_data["location_info"] = current_pvz

            if self.logger:
                self.logger.debug(f"Данные отчета с информацией о ПВЗ: {report_data}")
            return report_data
        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при извлечении данных отчета: {e}")
                import traceback
                self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
            # Возвращаем минимально необходимые данные
            current_pvz = self.get_current_pvz()
            # Используем target_url из конфига, если он есть, иначе текущий URL
            source_url = self.config.get('target_url', self.driver.current_url if self.driver else "Unknown")
            result = {
                "location_info": current_pvz,
                "extraction_timestamp": self._get_current_timestamp(),
                "source_url": source_url
            }
            if self.logger:
                self.logger.debug(f"Возвращаем минимальные данные отчета: {result}")
            return result

    def _get_current_timestamp(self) -> str:
        """
        Вспомогательный метод для получения текущей даты и времени

        Returns:
            str: Текущая даты и время в формате, заданном в конфигурации
        """
        if self.logger:
            self.logger.trace("Попали в метод OzonReportParser._get_current_timestamp")
        from datetime import datetime
        datetime_format = self.config.get('datetime_format', '%Y-%m-%d %H:%M:%S')
        timestamp = datetime.now().strftime(datetime_format)
        if self.logger:
            self.logger.debug(f"Получена временная метка: {timestamp}")
        return timestamp