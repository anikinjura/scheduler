"""
Parser_KPI_Giveout_OzonScript.py

Скрипт для автоматического парсинга данных о выдачах из маркетплейса ОЗОН.
Использует новую архитектуру с универсальной загрузкой данных и форматированием.

Функции:
- Парсинг веб-страницы ОЗОН для получения данных о выдачах
- Сохранение данных в JSON-файл
- Обеспечение логирования процесса
- Совместимость с системой уведомлений и загрузки в Google Sheets

Архитектура:
- Использует конфигурацию из Parser_KPI_Giveout_OzonScript_config.py
- Использует базовый класс BaseOzonParser для парсинга
- Использует универсальный модуль scheduler_runner/utils/google_sheets.py для загрузки данных
- Использует транслитерацию для кириллических имен ПВЗ при поиске файлов
- Обеспечивает уникальность записей с помощью Id столбца с формулой (для совместимости)

Author: anikinjura
Version: 3.0.0 (новая архитектура)
"""
__version__ = '1.0.0'

import argparse
import sys
import time
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# Импорты для обработки исключений Selenium
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Модульные константы для магических строк
LOGIN_INDICATORS = ['login', 'signin', 'auth']

# Добавляем корень проекта в sys.path для корректного импорта
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from scheduler_runner.utils.logging import configure_logger
from scheduler_runner.tasks.reports.BaseOzonParser import BaseOzonParser
from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Giveout_OzonScript_config import SCRIPT_CONFIG


class OzonGiveoutReportParser(BaseOzonParser):
    """Парсер для получения данных о выдачах из ERP-системы ОЗОН"""

    def __init__(self, config, logger=None):
        super().__init__(config)
        self.logger = logger

    def login(self):
        """Вход в ERP-систему ОЗОН"""
        # Заходим на страницу с базовым URL
        self.driver.get(self.config['ERP_URL'])
        if self.logger:
            self.logger.info(f"Переход на страницу: {self.config['ERP_URL']}")

    def navigate_to_reports(self):
        """Навигация к странице отчета о выдачах ОЗОН"""
        # В данном случае навигация уже выполнена в login(), так как мы переходим сразу к нужной странице
        if self.logger:
            self.logger.info("Навигация к отчету о выдачах выполнена")

    def extract_data(self) -> Dict[str, Any]:
        """Извлечение данных о выдачах из ERP-системы ОЗОН"""
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
                # Получаем дату из текущего URL или используем текущую дату
                import re
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', self.driver.current_url)
                report_date = date_match.group(1) if date_match else datetime.now().strftime('%Y-%m-%d')

                # Извлекаем информацию о ПВЗ с помощью вспомогательного метода
                pvz_info = self._extract_pvz_info()

                # Извлекаем количество выдач
                total_packages = self._extract_total_giveout()

                # Извлекаем количество выданных посылок
                issued_packages = self._extract_issued_packages()

                # Формируем итоговые данные
                # Формируем структуру данных с использованием REPORT_DATA_SCHEMA из конфигурации
                from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Giveout_OzonScript_config import REPORT_DATA_SCHEMA

                # Подготавливаем все возможные значения для подстановки
                all_values = {
                    'date': report_date,
                    'timestamp': datetime.now().isoformat(),
                    'page_title': self.driver.title,
                    'current_url': self.driver.current_url,
                    'issued_packages': issued_packages,
                    'total_packages': total_packages,
                    'pvz_info': pvz_info,
                    'page_source_length': len(self.driver.page_source),
                    'page_text_length': len(self.driver.find_element(By.TAG_NAME, "body").text)
                }

                # Формируем структуру данных с подстановкой значений
                raw_data = self._substitute_values_in_schema(REPORT_DATA_SCHEMA, all_values)

                # Используем raw_data как итоговую структуру, так как она уже сформирована по шаблону
                data = raw_data

                if self.logger:
                    self.logger.info(f"Информация о ПВЗ: {pvz_info}")
                    self.logger.info(f"Количество выдач: {issued_packages}")
                    self.logger.info(f"Всего пакетов: {total_packages}")

                return data
            except NoSuchElementException as e:
                if self.logger:
                    self.logger.error(f"Не найден элемент на странице: {e}")
                # Получаем значения по умолчанию из REPORT_DATA_SCHEMA
                from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Giveout_OzonScript_config import REPORT_DATA_SCHEMA
                default_marketplace = REPORT_DATA_SCHEMA.get('marketplace', 'Ozon')
                default_report_type = REPORT_DATA_SCHEMA.get('report_type', 'giveout')

                return {
                    'marketplace': default_marketplace,
                    'report_type': default_report_type,
                    'date': datetime.now().strftime('%Y-%m-%d'),
                    'timestamp': datetime.now().isoformat(),
                    'error': f'Element not found: {str(e)}',
                    'current_url': self.driver.current_url,
                    'page_title': self.driver.title,
                    'issued_packages': 0,
                    'total_packages': 0,
                    'pvz_info': '',
                }
            except TimeoutException as e:
                if self.logger:
                    self.logger.error(f"Таймаут ожидания элемента: {e}")
                # Получаем значения по умолчанию из REPORT_DATA_SCHEMA
                from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Giveout_OzonScript_config import REPORT_DATA_SCHEMA
                default_marketplace = REPORT_DATA_SCHEMA.get('marketplace', 'Ozon')
                default_report_type = REPORT_DATA_SCHEMA.get('report_type', 'giveout')

                return {
                    'marketplace': default_marketplace,
                    'report_type': default_report_type,
                    'date': datetime.now().strftime('%Y-%m-%d'),
                    'timestamp': datetime.now().isoformat(),
                    'error': f'Timeout: {str(e)}',
                    'current_url': self.driver.current_url,
                    'page_title': self.driver.title,
                    'issued_packages': 0,
                    'total_packages': 0,
                    'pvz_info': '',
                }
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Неожиданная ошибка при извлечении данных: {e}")
                    import traceback
                    self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
                # Получаем значения по умолчанию из REPORT_DATA_SCHEMA
                from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Giveout_OzonScript_config import REPORT_DATA_SCHEMA
                default_marketplace = REPORT_DATA_SCHEMA.get('marketplace', 'Ozon')
                default_report_type = REPORT_DATA_SCHEMA.get('report_type', 'giveout')

                return {
                    'marketplace': default_marketplace,
                    'report_type': default_report_type,
                    'date': datetime.now().strftime('%Y-%m-%d'),
                    'timestamp': datetime.now().isoformat(),
                    'error': f'Error extracting data: {str(e)}',
                    'current_url': self.driver.current_url,
                    'page_title': self.driver.title,
                    'issued_packages': 0,
                    'total_packages': 0,
                    'pvz_info': '',
                }

    def _check_authorization_status(self) -> tuple:
        """Проверка статуса авторизации и возврат ошибки при необходимости

        Returns:
            tuple[bool, dict]: (успешно ли авторизован, словарь ошибки если нет)
        """
        current_url = self.driver.current_url.lower()
        is_logged_in = not any(indicator in current_url for indicator in LOGIN_INDICATORS)

        if not is_logged_in:
            if self.logger:
                self.logger.warning("Все еще на странице логина - сессия не активна или недостаточно прав")
            # Получаем значения по умолчанию из REPORT_DATA_SCHEMA
            from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Giveout_OzonScript_config import REPORT_DATA_SCHEMA
            default_marketplace = REPORT_DATA_SCHEMA.get('marketplace', 'Ozon')
            default_report_type = REPORT_DATA_SCHEMA.get('report_type', 'giveout')

            error_response = {
                'marketplace': default_marketplace,
                'report_type': default_report_type,
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
        if pvz_value:
            pvz_info = pvz_value

        # Если не нашли через специфичный XPath, ищем по классу и атрибуту readonly
        if not pvz_info:
            pvz_value = self.extract_ozon_element_by_xpath(self.config['SELECTORS']['PVZ_INPUT_CLASS_READONLY'], "value")
            if pvz_value:
                pvz_info = pvz_value

        # Если не нашли в элементах, ищем в общем тексте
        if not pvz_info:
            page_text = self.driver.find_element(By.TAG_NAME, "body").text
            # Ищем возможные названия ПВЗ в тексте страницы
            pvz_keywords = ['ПВЗ', 'PVZ', 'СОС', 'ЧЕБ', 'КАЗ', 'РОС']
            pvz_matches = re.findall(r'([А-Яа-яЁёA-Za-z_]+\d+)', page_text)
            if pvz_matches:
                # Фильтруем найденные совпадения, оставляя только те, что похожи на названия ПВЗ
                for match in pvz_matches:
                    if '_' in match and any(keyword in match.upper() for keyword in pvz_keywords):
                        pvz_info = match
                        break
                # Если не нашли подходящий ПВЗ по ключевым словам, берем первый найденный
                if not pvz_info and pvz_matches:
                    pvz_info = pvz_matches[0]

        return pvz_info

    def _extract_total_giveout(self) -> int:
        """Извлечение общего количества выдач"""
        from selenium.webdriver.common.by import By
        import re

        try:
            # Ищем элемент с общей информацией о выдачах
            total_giveout_text = self.extract_ozon_element_by_xpath(self.config['SELECTORS']['TOTAL_GIVEOUT'], "textContent")
            if total_giveout_text:
                # Извлекаем число из текста
                numbers = re.findall(r'\d+', total_giveout_text)
                if numbers:
                    return int(numbers[0])
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Не удалось извлечь общее количество выдач: {e}")

        return 0

    def _extract_issued_packages(self) -> int:
        """Извлечение количества выданных посылок"""
        from selenium.webdriver.common.by import By
        import re

        try:
            # Используем специфичный метод из базового класса ОЗОН для поиска "Всего: N"
            page_text = self.driver.find_element(By.TAG_NAME, "body").text
            total_text = self.extract_ozon_data_by_pattern(r'Всего:\s*(\d+)', page_text)
            if total_text:
                return int(total_text)
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Не удалось извлечь количество выданных посылок по паттерну 'Всего: N': {e}")

        # Если не нашли по основному паттерну, пробуем искать в элементах по селектору
        try:
            issued_packages_text = self.extract_ozon_element_by_xpath(self.config['SELECTORS']['GIVEOUT_COUNT'], "textContent")
            if issued_packages_text:
                # Извлекаем число из текста
                numbers = re.findall(r'\d+', issued_packages_text)
                if numbers:
                    return int(numbers[0])
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Не удалось извлечь количество выданных посылок: {e}")

        # Если не нашли по основному селектору, пробуем искать в общем тексте страницы
        try:
            page_text = self.driver.find_element(By.TAG_NAME, "body").text
            # Ищем возможные упоминания количества выдач
            patterns = [
                r'выдано\s*(\d+)',
                r'выдач[иа]\s*(\d+)',
                r'(\d+)\s*выдано',
                r'(\d+)\s*выдач'
            ]
            for pattern in patterns:
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    return int(match.group(1))
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Не удалось извлечь количество выдач из текста страницы: {e}")

        return 0

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

    def logout(self):
        """Выход из системы (обычно не требуется при использовании существующей сессии)"""
        pass


def parse_arguments() -> argparse.Namespace:
    """
    Парсит аргументы командной строки для скрипта парсинга выдач ОЗОН.

    --detailed_logs              - включить детализированные логи
    --date                       - дата для парсинга в формате YYYY-MM-DD (по умолчанию - сегодня)
    """
    parser = argparse.ArgumentParser(
        description="Парсинг данных о выдачах из ERP-системы ОЗОН",
        epilog="Пример: python Parser_KPI_Giveout_OzonScript.py --detailed_logs --date 2026-01-01"
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


def main():
    """Основная функция скрипта для запуска из командной строки"""
    try:
        # 1. Парсинг аргументов командной строки
        args = parse_arguments()
        detailed_logs = args.detailed_logs or SCRIPT_CONFIG.get("DETAILED_LOGS", False)

        # Получаем дату из аргументов или используем текущую
        target_date = args.date
        if target_date is None:
            from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Giveout_OzonScript_config import DATE_FORMAT
            target_date = datetime.now().strftime(DATE_FORMAT)

        # 2. Настройка логирования
        logger = configure_logger(
            user=SCRIPT_CONFIG["USER"],
            task_name=SCRIPT_CONFIG["TASK_NAME"],
            detailed=detailed_logs
        )

        # 3. Логирование начала процесса
        logger.info(f"Запуск парсинга данных о выдачах ERP-системы ОЗОН за дату: {target_date}")

        # 4. Создание копии конфигурации с обновленным URL для указанной даты
        from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Giveout_OzonScript_config import ERP_URL_TEMPLATE, DATE_FORMAT

        # Формируем готовый URL, подставив дату в шаблон
        erp_url = ERP_URL_TEMPLATE.format(date=target_date)

        # Создаем копию конфигурации и обновляем только URL
        script_config = SCRIPT_CONFIG.copy()
        script_config["ERP_URL"] = erp_url

        # 5. Создание экземпляра парсера
        parser = OzonGiveoutReportParser(script_config, logger)

        # 6. Настройка драйвера
        try:
            parser.setup_driver() # setup_driver() определена в базовом классе BaseOzonParser, создает и настраивает экземпляр браузера, готовый к работе с ERP-системой Ozon.

            # 7. Выполнение основных операций
            parser.login()
            parser.navigate_to_reports()

            # Извлечение данных о выдачах
            data = parser.extract_data()

            parser.logout()

            # 8. Сохранение данных
            output_dir = Path(script_config['OUTPUT_DIR'])
            output_dir.mkdir(parents=True, exist_ok=True)

            # Формируем имя файла с использованием шаблона из конфигурации
            date_str = target_date.replace('-', '')  # Преобразуем формат даты для имени файла
            pvz_id = data.get('pvz_info', script_config['PVZ_ID'])
            # Транслитерируем ПВЗ для использования в имени файла
            from scheduler_runner.utils.system import SystemUtils
            translit_pvz = SystemUtils.cyrillic_to_translit(pvz_id) if pvz_id else 'unknown'

            # Используем шаблон из конфигурации для формирования имени файла
            from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Giveout_OzonScript_config import FILE_PATTERN
            filename_template = FILE_PATTERN.replace('{pvz_id}', translit_pvz).replace('{date}', date_str)
            filename = output_dir / filename_template

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)

            logger.info(f"Отчет о выдачах ОЗОН успешно сохранен в {filename}")
            logger.info(f"Извлеченные данные: {data}")
        finally:
            # 9. Завершение работы
            parser.close()

    except Exception as e:
        # 10. Обработка исключений
        import traceback
        logger.error(f"Ошибка при парсинге данных о выдачах ERP-системы ОЗОН: {e}")
        logger.error(f"Полный стек трейса: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()