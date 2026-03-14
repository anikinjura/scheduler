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
        self._last_known_pvz = None

        if self.logger:
            self.logger.trace("Попали в метод OzonReportParser.__init__")

    def _remember_current_pvz(self, pvz_value: str) -> str:
        normalized_pvz = (pvz_value or "").strip()
        if normalized_pvz and normalized_pvz != "Unknown":
            self._last_known_pvz = normalized_pvz
        return normalized_pvz

    def _get_cached_pvz(self) -> str:
        return self._last_known_pvz or "Unknown"

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

            selector_candidates = pvz_selectors.get("input_candidates") or [
                pvz_selectors.get("input_readonly"),
                pvz_selectors.get("input"),
                pvz_selectors.get("input_class_readonly"),
            ]
            selector_candidates = [selector for selector in selector_candidates if selector]

            if not selector_candidates:
                if self.logger:
                    self.logger.error("Не найден селектор для получения текущего ПВЗ")
                return "Unknown"

            current_pvz = ""
            for pvz_input_selector in selector_candidates:
                if self.logger:
                    self.logger.debug(f"Пробуем селектор для получения ПВЗ: {pvz_input_selector}")

                current_pvz = self.get_element_value(
                    selector=pvz_input_selector,
                    element_type="input"
                )
                if current_pvz:
                    break

            remembered_pvz = self._remember_current_pvz(current_pvz)

            if self.logger:
                self.logger.info(f"Текущий ПВЗ: {remembered_pvz}")

            if remembered_pvz:
                return remembered_pvz

            cached_pvz = self._get_cached_pvz()
            if self.logger and cached_pvz != "Unknown":
                self.logger.warning(f"Не удалось прочитать текущий ПВЗ из UI, используем кешированное значение: {cached_pvz}")
            return cached_pvz
        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при извлечении текущего ПВЗ: {e}")
                import traceback
                self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
            cached_pvz = self._get_cached_pvz()
            if self.logger and cached_pvz != "Unknown":
                self.logger.warning(f"Используем кешированное значение ПВЗ после ошибки чтения UI: {cached_pvz}")
            return cached_pvz

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

            dropdown_candidates = selectors.get("dropdown_candidates") or [
                selectors.get("dropdown"),
            ]
            option_templates = selectors.get("option_candidates") or [
                selectors.get("option"),
            ]
            option_candidates = [
                option_selector.format(target_pvz=target_pvz)
                for option_selector in option_templates
                if option_selector
            ]
            dropdown_candidates = [selector for selector in dropdown_candidates if selector]
            option_candidates = [selector for selector in option_candidates if selector]

            if not dropdown_candidates or not option_candidates:
                if self.logger:
                    self.logger.error("Не найдены селекторы для установки ПВЗ")
                return False

            # === ПРОВЕРКА И ЗАКРЫТИЕ ОВЕРЛЕЯ ПЕРЕД ОТКРЫТИЕМ DROPDOWN ===
            if self.logger:
                self.logger.debug("Проверка оверлея перед открытием dropdown")
            if not self._check_and_close_overlay():
                if self.logger:
                    self.logger.warning("Не удалось закрыть оверлей перед открытием dropdown")

            success = False
            for dropdown_selector in dropdown_candidates:
                for option_selector in option_candidates:
                    if self.logger:
                        self.logger.debug(f"Пробуем dropdown selector: {dropdown_selector}")
                        self.logger.debug(f"Пробуем option selector: {option_selector}")
                        self.logger.debug(f"Целевой ПВЗ для установки: {target_pvz}")

                    success = self.set_element_value(
                        selector=dropdown_selector,
                        value=target_pvz,
                        element_type="dropdown",
                        option_selector=option_selector
                    )
                    if success:
                        break
                if success:
                    break

            if success:
                self._remember_current_pvz(target_pvz)
                if self.logger:
                    self.logger.info(f"ПВЗ успешно установлен: {target_pvz}")
                # Ждем немного, чтобы изменения вступили в силу
                time.sleep(2)
                
                # === ПРОВЕРКА И ЗАКРЫТИЕ ОВЕРЛЕЯ ПОСЛЕ ВЫБОРА ПВЗ ===
                if self.logger:
                    self.logger.debug("Проверка оверлея после выбора ПВЗ")
                if not self._check_and_close_overlay():
                    if self.logger:
                        self.logger.warning("Не удалось закрыть оверлей после выбора ПВЗ")
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
        
        # ПРОВЕРКА И ЗАКРЫТИЕ ОВЕРЛЕЯ ПЕРЕД РАБОТОЙ СО СТРАНИЦЕЙ
        if self.logger:
            self.logger.debug("Проверка наличия оверлея перед получением ПВЗ")
        if not self._check_and_close_overlay():
            if self.logger:
                self.logger.warning("Не удалось закрыть оверлей, продолжаем работу")

        # === ДИАГНОСТИКА СТРАНИЦЫ ПЕРЕД ПРОВЕРКОЙ ПВЗ ===
        if self.logger:
            try:
                self.logger.debug("=== ДИАГНОСТИКА СТРАНИЦЫ ===")
                self.logger.debug(f"Текущий URL: {self.driver.current_url}")
                self.logger.debug(f"Заголовок страницы: {self.driver.title}")
                
                # Проверка на страницу логина
                if 'login' in self.driver.current_url.lower():
                    self.logger.error("❌ СТРАНИЦА ЛОГИНА ОБНАРУЖЕНА при проверке ПВЗ!")
                    self.logger.error("Сессия невалидна — возможна причина: выход пользователя или истек таймаут")
                
                # Краткая информация о DOM
                page_source_len = len(self.driver.page_source)
                self.logger.debug(f"Размер HTML страницы: {page_source_len} байт")
                
            except Exception as diag_err:
                self.logger.error(f"Ошибка диагностики страницы: {diag_err}")

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
                
                # Проверка: если current_pvz = 'Unknown', это может означать проблему с сессией
                cached_pvz = self._get_cached_pvz()
                if current_pvz == 'Unknown':
                    if cached_pvz == required_pvz:
                        if self.logger:
                            self.logger.warning(
                                f"Текущий ПВЗ не читается из UI, но кешированное значение совпадает с требуемым: {cached_pvz}"
                            )
                        return True
                    self.logger.warning("⚠️ ПВЗ не определен (Unknown) — возможна проблема с сессией или структурой страницы")
                    self.logger.warning("Проверьте, что браузер находится на правильной странице, а не на странице логина")
                    self.dump_debug_artifacts("pvz_unknown_before_set")

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
                                self._remember_current_pvz(final_pvz)
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
                    self.dump_debug_artifacts("pvz_set_failed")
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

    def _check_and_close_overlay(self) -> bool:
        """
        Проверка наличия оверлея (модального окна) на странице и его закрытие

        Метод проверяет наличие оверлея по селектору из конфигурации и пытается
        закрыть его, кликнув по кнопке закрытия (крестику).

        Returns:
            bool: True, если оверлея нет или он успешно закрыт, False в случае ошибки
        """
        if self.logger:
            self.logger.trace("Попали в метод OzonReportParser._check_and_close_overlay")

        # Получаем конфигурацию оверлея
        overlay_config = self.config.get("overlay_config", {})

        # Проверяем, включена ли проверка оверлея
        if not overlay_config.get("enabled", False):
            if self.logger:
                self.logger.debug("Проверка оверлея отключена в конфигурации")
            return True

        # Получаем селекторы и параметры из конфигурации
        overlay_selector = overlay_config.get("overlay_selector")
        close_button_selector = overlay_config.get("close_button_selector")
        wait_timeout = overlay_config.get("wait_timeout", 5)
        retry_count = overlay_config.get("retry_count", 3)
        retry_delay = overlay_config.get("retry_delay", 1)

        if not overlay_selector or not close_button_selector:
            if self.logger:
                self.logger.warning("Не указаны селекторы для проверки оверлея в конфигурации")
            return True

        if self.logger:
            self.logger.debug(f"Проверка оверлея: selector={overlay_selector}, timeout={wait_timeout}s")

        # Проверяем наличие оверлея на странице
        if self._is_overlay_present(overlay_selector, wait_timeout):
            if self.logger:
                self.logger.info("Обнаружен оверлей на странице, пытаемся закрыть...")

            # Пытаемся закрыть оверлей с retry_count попытками
            for attempt in range(1, retry_count + 1):
                if self.logger:
                    self.logger.debug(f"Попытка {attempt}/{retry_count}: клик по кнопке закрытия оверлея")

                if self._click_close_button(close_button_selector):
                    if self.logger:
                        self.logger.info(f"Оверлей успешно закрыт с попытки {attempt}")

                    # Проверяем, что оверлей действительно исчез
                    if not self._is_overlay_present(overlay_selector, timeout=2):
                        if self.logger:
                            self.logger.debug("Оверлей успешно закрыт и не обнаружен после проверки")
                        return True
                    else:
                        if self.logger:
                            self.logger.warning("Оверлей всё ещё присутствует после клика, повторяем попытку")
                else:
                    if self.logger:
                        self.logger.warning(f"Не удалось кликнуть по кнопке закрытия (попытка {attempt})")

                # Задержка перед следующей попыткой
                if attempt < retry_count:
                    if self.logger:
                        self.logger.debug(f"Задержка перед следующей попыткой: {retry_delay}s")
                    time.sleep(retry_delay)

            # Если все попытки исчерпаны
            if self.logger:
                self.logger.error(f"Не удалось закрыть оверлей после {retry_count} попыток")
            return False
        else:
            if self.logger:
                self.logger.debug("Оверлей не обнаружен на странице")
            return True

    def _is_overlay_present(self, selector: str, timeout: int = 5) -> bool:
        """
        Проверка наличия оверлея на странице

        Метод ищет элемент по заданному селектору и определяет, виден ли он.
        Также проверяет наличие активного backdrop (полупрозрачного фона).

        Args:
            selector: XPath или CSS селектор для поиска оверлея
            timeout: Время ожидания появления элемента (секунды)

        Returns:
            bool: True, если оверлей найден и виден, или backdrop активен; False otherwise
        """
        if self.logger:
            self.logger.trace(f"Попали в метод OzonReportParser._is_overlay_present с селектором: {selector}")

        try:
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC

            if self.logger:
                self.logger.debug(f"Ожидание появления оверлея в течение {timeout} секунд")

            # === ОПТИМИЗАЦИЯ ПРОИЗВОДИТЕЛЬНОСТИ ===
            # Сохраняем текущий implicit_wait для последующего восстановления
            old_implicit_wait = self.driver.timeouts.implicit_wait
            
            try:
                # Устанавливаем короткий implicit_wait для быстрой проверки
                # Это критично для производительности: без этого WebDriverWait ждёт до implicit_wait
                # timeout по умолчанию = 5 сек, но implicit_wait(20) заставляет ждать дольше
                self.driver.implicitly_wait(min(timeout, 1))  # 1 секунда для быстрой проверки
                
                # Ждём появления элемента (но не обязательно видимости)
                elements = WebDriverWait(self.driver, timeout).until(
                    EC.presence_of_all_elements_located((By.XPATH, selector))
                )
            finally:
                # Восстанавливаем исходный implicit_wait
                self.driver.implicitly_wait(old_implicit_wait)
                if self.logger:
                    self.logger.debug(f"Восстановлен implicit_wait: {old_implicit_wait} сек")

            if not elements or len(elements) == 0:
                if self.logger:
                    self.logger.debug("Оверлей не найден на странице")
                # Проверяем наличие активного backdrop
                return self._is_backdrop_active()

            # === ДЕТАЛЬНАЯ ДИАГНОСТИКА НАЙДЕННЫХ ЭЛЕМЕНТОВ ===
            if self.logger:
                self.logger.debug(f"=== ДИАГНОСТИКА ОВЕРЛЕЯ ===")
                self.logger.debug(f"Найдено элементов по селектору '{selector}': {len(elements)}")
                for i, elem in enumerate(elements):
                    try:
                        elem_id = elem.get_attribute('id')
                        elem_class = elem.get_attribute('class')
                        elem_style = elem.get_attribute('style')
                        is_displayed = elem.is_displayed()
                        self.logger.debug(f"  Элемент #{i+1}: id='{elem_id}', class='{elem_class[:50] if elem_class else None}...', displayed={is_displayed}")
                        if elem_style:
                            self.logger.debug(f"    style='{elem_style[:100]}...'")
                    except Exception as elem_err:
                        self.logger.debug(f"  Элемент #{i+1}: ошибка диагностики - {elem_err}")

            # Проверяем, виден ли хотя бы один элемент
            for element in elements:
                if element.is_displayed():
                    if self.logger:
                        self.logger.info(f"Оверлей найден и виден (количество: {len(elements)})")
                    return True

            # Оверлей найден, но скрыт (displayed=False)
            # Это может означать, что идет анимация закрытия
            if self.logger:
                self.logger.debug("Оверлей найден, но не виден (скрыт) - возможна анимация закрытия")

            # Проверяем наличие активного backdrop
            if self._is_backdrop_active():
                if self.logger:
                    self.logger.info("Backdrop активен, ожидаем завершения анимации")
                return True

            return False

        except Exception as e:
            if self.logger:
                self.logger.debug(f"Оверлей не обнаружен: {e}")
            # Даже если оверлей не найден, проверяем backdrop
            return self._is_backdrop_active()

    def _is_backdrop_active(self) -> bool:
        """
        Проверка наличия активного backdrop (полупрозрачного фона оверлея)

        Backdrop может оставаться активным даже после того, как оверлей скрыт,
        что блокирует клики по элементам страницы.

        Селекторы backdrop извлекаются из конфигурации overlay_config.backdrop_selectors.

        Returns:
            bool: True, если backdrop активен
        """
        if self.logger:
            self.logger.trace("Попали в метод OzonReportParser._is_backdrop_active")

        try:
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC

            # Получаем селекторы backdrop из конфигурации
            overlay_config = self.config.get("overlay_config", {})
            backdrop_selectors = overlay_config.get("backdrop_selectors", [])

            if not backdrop_selectors:
                if self.logger:
                    self.logger.warning("Не указаны селекторы backdrop в конфигурации overlay_config.backdrop_selectors")
                return False

            if self.logger:
                self.logger.debug(f"Проверка backdrop: {len(backdrop_selectors)} селекторов из конфигурации")

            # === ОПТИМИЗАЦИЯ ПРОИЗВОДИТЕЛЬНОСТИ ===
            # Сохраняем текущий implicit_wait для последующего восстановления
            old_implicit_wait = self.driver.timeouts.implicit_wait
            
            try:
                # Устанавливаем короткий implicit_wait для быстрой проверки
                # Это критично для производительности: без этого WebDriverWait ждёт до implicit_wait
                self.driver.implicitly_wait(0.5)  # 0.5 секунды вместо 20
                
                # Используем короткий таймаут для проверки backdrop (1 сек)
                # 4 селектора × 1 сек = 4 секунды на проверку (вместо 80 сек с implicit_wait(20))
                backdrop_timeout = 1  # секунды на каждый селектор

                for selector in backdrop_selectors:
                    try:
                        # Быстрая проверка наличия элементов с коротким таймаутом
                        backdrop_elements = WebDriverWait(self.driver, backdrop_timeout).until(
                            EC.presence_of_all_elements_located((By.XPATH, selector)),
                            message=f"Backdrop check timeout for selector: {selector[:50]}..."
                        )

                        for elem in backdrop_elements:
                            if elem.is_displayed():
                                elem_class = elem.get_attribute('class')
                                elem_style = elem.get_attribute('style')
                                if self.logger:
                                    self.logger.debug(f"Backdrop найден: selector='{selector[:60]}...', class='{elem_class[:80] if elem_class else None}...'")
                                    if elem_style:
                                        self.logger.debug(f"  style='{elem_style[:100]}...'")
                                return True
                    except Exception:
                        # Элементы не найдены или таймаут - переходим к следующему селектору
                        pass
            finally:
                # Восстанавливаем исходный implicit_wait
                self.driver.implicitly_wait(old_implicit_wait)
                if self.logger:
                    self.logger.debug(f"Восстановлен implicit_wait: {old_implicit_wait} сек")

            if self.logger:
                self.logger.debug("Backdrop не активен")
            return False

        except Exception as e:
            if self.logger:
                self.logger.debug(f"Ошибка при проверке backdrop: {e}")
            return False

    def _click_close_button(self, selector: str) -> bool:
        """
        Клик по кнопке закрытия оверлея

        Метод находит кнопку закрытия по селектору и выполняет клик по ней.

        Args:
            selector: XPath или CSS селектор для поиска кнопки закрытия

        Returns:
            bool: True, если клик успешно выполнен, False в случае ошибки
        """
        if self.logger:
            self.logger.trace(f"Попали в метод OzonReportParser._click_close_button с селектором: {selector}")

        try:
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC

            if self.logger:
                self.logger.debug(f"Поиск кнопки закрытия по селектору: {selector}")

            # Ждём кликабельности элемента
            close_button = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, selector))
            )

            if self.logger:
                self.logger.debug("Кнопка закрытия найдена и кликабельна, выполняем клик")

            # Выполняем клик
            close_button.click()

            if self.logger:
                self.logger.info("Клик по кнопке закрытия успешно выполнен")

            # Небольшая задержка после клика
            time.sleep(0.5)

            return True

        except Exception as e:
            if self.logger:
                self.logger.warning(f"Не удалось кликнуть по кнопке закрытия: {e}")
            return False
