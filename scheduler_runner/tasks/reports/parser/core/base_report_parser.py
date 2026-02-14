"""
Базовый класс для парсинга отчетов из веб-систем

Архитектура:
- Наследуется от BaseParser
- Добавляет специфичную логику для работы с отчетами
- Включает методы валидации, форматирования и обработки данных отчетов
- Поддерживает различные форматы отчетов (JSON, CSV, XML и др.)
- Переопределяет метод run_parser для добавления специфичной логики обработки отчетов
- Включает методы сохранения отчетов в различных форматах
- Поддерживает передачу аргументов командной строки, включая дату отчета
- Включает методы для формирования URL с фильтрами (base_url, filter_template и др.)
- Поддерживает мульти-шаговую обработку с различными типами обработки (simple, table, table_nested)
- Включает вложенную обработку и агрегацию результатов

Изменения в версии 0.0.1:
- Обновлены ссылки на методы BaseParser с учетом переименования select_option_from_dropdown в _select_option_from_dropdown
- Добавлены методы _build_url_filter и обновлен navigate_to_target для формирования URL с фильтрами
- Добавлена поддержка мульти-шаговой обработки с методами:
  - _execute_multi_step_processing: основной метод для выполнения мульти-шаговой обработки
  - _execute_single_step_processing: метод для одностадийной обработки (обратная совместимость)
  - _execute_single_step: метод для выполнения одного шага в мульти-шаговом процессе
  - _handle_simple_extraction, _handle_table_extraction, _handle_table_nested_extraction: обработчики разных типов извлечения данных
  - _handle_nested_processing: обработка вложенных сценариев
  - _combine_step_results: объединение результатов всех шагов
  - _update_config_for_step: временное обновление конфигурации для шага
  - _extract_value_by_config: универсальный метод извлечения значений
  - _apply_post_processing: постобработка извлеченных значений
  - _aggregate_values: агрегация значений по указанному методу
"""
__version__ = '0.0.1'

import argparse
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from .base_parser import BaseParser
from datetime import datetime


class BaseReportParser(BaseParser, ABC):
    """Базовый класс для парсинга отчетов из веб-систем"""

    def __init__(self, config: Dict[str, Any], args=None, logger=None):
        """
        Инициализация базового парсера отчетов

        Args:
            config: Конфигурационный словарь с параметрами для работы парсера
            args: Аргументы командной строки (если не переданы, будут разобраны из sys.argv)
            logger: Объект логгера (если не передан, будет использован внутренний логгер)

        Поддерживаемые параметры конфигурации:
            - Параметры из BaseParser (см. документацию BaseParser)
            - xml_root_element: Имя корневого элемента XML (по умолчанию 'report')
            - xml_item_prefix: Префикс для элементов списка в XML (по умолчанию 'item_')
            - default_output_dir: Путь по умолчанию для сохранения отчетов (по умолчанию './output')
            - filename_template: Шаблон имени файла (по умолчанию '{report_type}_report_{timestamp}.{output_format}')
            - schema_keys: Ключи для получения информации из схемы
                - required_fields: Ключ для обязательных полей (по умолчанию 'required_fields')
                - field_types: Ключ для типов полей (по умолчанию 'field_types')
            - supported_field_types: Поддерживаемые типы данных
                - string: str
                - integer: int
                - float: float
                - boolean: bool
                - list: list
                - dict: dict
            - date_format: Формат даты (по умолчанию '%Y-%m-%d')
            - datetime_format: Формат даты и времени (по умолчанию '%Y-%m-%d %H:%M:%S')
            - output_config: Конфигурация вывода данных
                - dir: Директория для сохранения отчетов
                - format: Формат вывода ('json', 'csv', 'xml', etc.)
                - encoding: Кодировка файлов (по умолчанию 'utf-8')
            - validation_config: Конфигурация валидации
                - strict_mode: Строгий режим валидации (по умолчанию False)
                - log_errors: Логировать ошибки валидации (по умолчанию True)
                - skip_invalid_fields: Пропускать невалидные поля (по умолчанию True)
            - last_collected_data: Последние собранные данные отчета
            - execution_date: Дата выполнения (в формате, установленном в date_format)
            - base_url: Базовый URL для запросов (по умолчанию '')
            - filter_template: Шаблон формирования общего фильтра в URL (по умолчанию '')
            - date_filter_template: Шаблон фильтра по дате (по умолчанию '')
            - data_type_filter_template: Шаблон фильтра по типу данных (по умолчанию '')
            - target_url: Целевой URL, формируется в логике парсера (по умолчанию '')

        Note:
            В BaseParser были внесены изменения:
            - Метод select_option_from_dropdown переименован в _select_option_from_dropdown
            - Метод set_element_value теперь использует _select_option_from_dropdown для работы с выпадающими списками
        """
        # Вызов родительского конструктора с передачей логгера
        super().__init__(config, logger=logger)

        if self.logger:
            self.logger.trace("Попали в метод BaseReportParser.__init__")

        # Обработка аргументов командной строки
        self.args = self._parse_arguments(args)

        # Обновление даты выполнения на основе аргументов или текущего времени
        self._update_execution_date()

    def _parse_arguments(self, args=None):
        """
        Разбор аргументов командной строки

        Args:
            args: Список аргументов (если None, будут использованы sys.argv)

        Returns:
            argparse.Namespace: Разобранные аргументы
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseReportParser._parse_arguments")
        import sys

        parser = argparse.ArgumentParser(description='Парсер отчетов')
        parser.add_argument('--report_date', '--date', '-d',
                          dest='report_date',
                          help='Дата отчета в формате YYYY-MM-DD (например, 2023-12-25)')

        if args is None:
            # Исключаем имя скрипта из аргументов, если разбираем sys.argv
            args = sys.argv[1:] if len(sys.argv) > 1 else []

        # Разбираем только известные аргументы, игнорируя неизвестные
        parsed_args, _ = parser.parse_known_args(args)
        return parsed_args

    def _update_execution_date(self):
        """
        Обновление даты выполнения на основе аргументов командной строки,
        конфигурации или текущего времени
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseReportParser._update_execution_date")
        import datetime

        # Получаем формат даты из конфига
        date_format = self.config.get('date_format', '%Y-%m-%d')

        # Приоритет: 1. Аргумент командной строки, 2. Значение из конфига, 3. Текущая дата
        if self.args and self.args.report_date:
            # Если передана дата в аргументе, используем её
            execution_date = self.args.report_date
        elif 'execution_date' in self.config:
            # Если дата уже есть в конфиге, используем её
            execution_date = self.config['execution_date']
        else:
            # Если ничего не передано, используем текущую дату
            execution_date = datetime.datetime.now().strftime(date_format)

        # Обновляем дату выполнения в конфиге
        self.config['execution_date'] = execution_date

    # === АБСТРАКТНЫЕ МЕТОДЫ (обязательны для реализации в дочерних классах) ===

    @abstractmethod
    def get_report_type(self) -> str:
        """
        Возвращает тип отчета

        Returns:
            str: Тип отчета (например, 'sales_report', 'inventory_report', 'giveout_report')
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseReportParser.get_report_type")
        pass


    @abstractmethod
    def extract_report_data(self) -> Dict[str, Any]:
        """
        Извлекает специфичные данные отчета

        Returns:
            Dict[str, Any]: Словарь с данными отчета
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseReportParser.extract_report_data")
        pass


    # === МЕТОДЫ РАБОТЫ С ОТЧЕТАМИ ===

    def run_parser(self, save_to_file: bool = True, output_format: str = 'json') -> Dict[str, Any]:
        """
        Метод запуска парсера отчетов, определяющий последовательность вызова методов
        Переопределяет метод родительского класса для добавления специфичной логики обработки отчетов
        Поддерживает только мульти-шаговую обработку

        Args:
            save_to_file: Сохранять ли результат в файл
            output_format: Формат вывода ('json', 'csv', 'xml', etc.)

        Returns:
            Dict[str, Any]: Извлеченные данные отчета
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseReportParser.run_parser")
        try:
            if self.logger:
                self.logger.debug("Начало выполнения парсера отчетов")
            
            # 1. Настройка браузера
            if self.logger:
                self.logger.debug("Шаг 1: Настройка браузера")
            if not self.setup_browser():
                raise Exception("Не удалось настроить браузер")

            # 2. Вход в систему
            if self.logger:
                self.logger.debug("Шаг 2: Вход в систему")
            if not self.login():
                raise Exception("Не удалось выполнить вход в систему")

            # 3. Выполнение мульти-шаговой обработки (теперь единственный способ обработки)
            if self.logger:
                self.logger.debug("Шаг 3: Выполнение мульти-шаговой обработки")
            multi_step_config = self.config.get("multi_step_config", {})
            if not multi_step_config or not multi_step_config.get("steps"):
                raise Exception("Для обработки обязательно должен быть указан multi_step_config со списком шагов")

            if self.logger:
                self.logger.debug(f"Конфигурация мульти-шаговой обработки: {list(multi_step_config.keys())}")
                self.logger.debug(f"Количество шагов: {len(multi_step_config.get('steps', []))}")

            data = self._execute_multi_step_processing(multi_step_config)

            # 4. Сохранение отчета в файл, если требуется
            if save_to_file:
                if self.logger:
                    self.logger.debug("Шаг 4: Сохранение отчета в файл")
                self.save_report(data=data, output_format=output_format)

            # 5. Выход из системы
            if self.logger:
                self.logger.debug("Шаг 5: Выход из системы")
            if not self.logout():
                if self.logger:
                    self.logger.warning("Не удалось корректно выйти из системы")

            if self.logger:
                self.logger.debug("Парсинг отчетов завершен успешно")
            return data

        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при выполнении парсинга отчета: {e}")
                self.logger.error(f"Тип ошибки: {type(e).__name__}")
                import traceback
                self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
            raise
        finally:
            # 6. Закрытие браузера
            # Добавляем задержку перед закрытием браузера, если указано в конфигурации
            import time
            close_delay = self.config.get("BROWSER_CLOSE_DELAY", 0)
            if close_delay > 0:
                if self.logger:
                    self.logger.info(f"Задержка перед закрытием браузера: {close_delay} секунд")
                time.sleep(close_delay)
            if self.logger:
                self.logger.debug("Закрытие браузера")
            self.close_browser()



    def format_report_output(self,
                           data: Optional[Dict[str, Any]] = None,
                           output_format: str = 'json') -> str:
        """
        Форматирование данных отчета для вывода

        Args:
            data: Данные отчета {'field1': value1, 'field2': value2, etc.}
            output_format: Формат вывода ('json', 'csv', 'xml', etc.)

        Returns:
            str: Отформатированные данные в заданном формате
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseReportParser.format_report_output")
        # Если параметры не переданы, берем из конфига
        report_data = data or self.config.get('last_collected_data', {})
        format_type = output_format or self.config.get('output_config', {}).get('format', 'json')

        if self.logger:
            self.logger.debug(f"Форматирование данных отчета: тип={format_type}, количество ключей={len(report_data) if isinstance(report_data, dict) else 'unknown'}")

        try:
            if format_type.lower() == 'json':
                if self.logger:
                    self.logger.debug("Форматирование в JSON")
                import json
                result = json.dumps(report_data, ensure_ascii=False, indent=2, default=str)
                if self.logger:
                    self.logger.debug(f"JSON форматирование завершено, размер: {len(result) if result else 0} символов")
                return result
            elif format_type.lower() == 'csv':
                if self.logger:
                    self.logger.debug("Форматирование в CSV")
                import csv
                import io
                output = io.StringIO()
                if report_data:
                    # Если данные представляют собой список словарей
                    if isinstance(list(report_data.values())[0], list) if report_data else False:
                        fieldnames = set()
                        for row in report_data.values():
                            if isinstance(row, list):
                                for item in row:
                                    if isinstance(item, dict):
                                        fieldnames.update(item.keys())
                        fieldnames = list(fieldnames) if fieldnames else list(report_data.keys())

                        writer = csv.DictWriter(output, fieldnames=fieldnames)
                        writer.writeheader()
                        for row_list in report_data.values():
                            if isinstance(row_list, list):
                                for item in row_list:
                                    if isinstance(item, dict):
                                        writer.writerow(item)
                    else:
                        # Если данные представляют собой один словарь
                        writer = csv.DictWriter(output, fieldnames=list(report_data.keys()))
                        writer.writeheader()
                        writer.writerow(report_data)
                result = output.getvalue()
                if self.logger:
                    self.logger.debug(f"CSV форматирование завершено, размер: {len(result) if result else 0} символов")
                return result
            elif format_type.lower() == 'xml':
                if self.logger:
                    self.logger.debug("Форматирование в XML")
                import xml.etree.ElementTree as ET
                # Используем имя корневого элемента из конфига
                root_element_name = self.config.get('xml_root_element', 'report')
                root = ET.Element(root_element_name)

                def add_data_to_xml(parent, data):
                    if isinstance(data, dict):
                        for key, value in data.items():
                            child = ET.SubElement(parent, key.replace(' ', '_'))
                            add_data_to_xml(child, value)
                    elif isinstance(data, list):
                        # Используем префикс для элементов списка из конфига
                        item_prefix = self.config.get('xml_item_prefix', 'item_')
                        for i, item in enumerate(data):
                            child = ET.SubElement(parent, f"{item_prefix}{i}")
                            add_data_to_xml(child, item)
                    else:
                        parent.text = str(data)

                add_data_to_xml(root, report_data)
                result = ET.tostring(root, encoding='unicode')
                if self.logger:
                    self.logger.debug(f"XML форматирование завершено, размер: {len(result) if result else 0} символов")
                return result
            else:
                # По умолчанию используем JSON
                if self.logger:
                    self.logger.debug("Форматирование в JSON по умолчанию")
                import json
                result = json.dumps(report_data, ensure_ascii=False, indent=2, default=str)
                if self.logger:
                    self.logger.debug(f"JSON форматирование по умолчанию завершено, размер: {len(result) if result else 0} символов")
                return result

        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при форматировании данных отчета: {e}")
                import traceback
                self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
            # В случае ошибки возвращаем JSON по умолчанию
            import json
            result = json.dumps(report_data, ensure_ascii=False, indent=2, default=str)
            if self.logger:
                self.logger.debug(f"Возвращаем JSON по умолчанию, размер: {len(result) if result else 0} символов")
            return result

    def save_report(self,
                   data: Optional[Dict[str, Any]] = None,
                   output_path: Optional[str] = None,
                   output_format: str = 'json') -> bool:
        """
        Сохранение отчета в файл

        Args:
            data: Данные отчета для сохранения
            output_path: Путь для сохранения файла
            output_format: Формат файла ('json', 'csv', 'xml', etc.)

        Returns:
            bool: True, если отчет успешно сохранен
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseReportParser.save_report")
        try:
            # Получаем данные для сохранения
            report_data = data or self.config.get('last_collected_data', {})
            if self.logger:
                self.logger.debug(f"Данные для сохранения: {list(report_data.keys()) if isinstance(report_data, dict) else type(report_data)}")

            # Если путь не указан, используем путь из конфига
            if not output_path:
                output_dir = self.config.get('output_config', {}).get('dir',
                                                                    self.config.get('default_output_dir', './output'))
                report_type = self.get_report_type()

                # Получаем дату выполнения и формат для имени файла
                execution_date = self.config.get('execution_date', '')

                # Форматируем дату для использования в имени файла (удаляем дефисы или преобразуем по формату)
                if execution_date:
                    # Пытаемся распознать формат даты из конфига и преобразовать в формат для имени файла
                    date_format = self.config.get('date_format', '%Y-%m-%d')
                    try:
                        # Преобразуем строку даты в объект datetime, затем в нужный формат для имени файла
                        dt_obj = datetime.strptime(execution_date, date_format)
                        # Для имени файла обычно используются даты без дефисов
                        timestamp = dt_obj.strftime('%Y%m%d')
                    except ValueError:
                        # Если формат не совпадает, просто удаляем дефисы
                        timestamp = execution_date.replace('-', '')
                else:
                    timestamp = 'latest'

                # Используем шаблон имени файла из конфига
                filename_template = self.config.get('filename_template',
                                                  "{report_type}_report_{timestamp}.{output_format}")
                filename = filename_template.format(
                    report_type=report_type,
                    timestamp=timestamp,
                    output_format=output_format
                )
                output_path = f"{output_dir}/{filename}"
                
            if self.logger:
                self.logger.debug(f"Путь для сохранения: {output_path}")

            # Создаем директорию, если она не существует
            import os
            output_dir = os.path.dirname(output_path)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
                if self.logger:
                    self.logger.debug(f"Директория создана или уже существует: {output_dir}")

            # Форматируем и сохраняем данные
            formatted_data = self.format_report_output(report_data, output_format)
            if self.logger:
                self.logger.debug(f"Данные отформатированы, размер: {len(formatted_data) if formatted_data else 0} символов")

            # Используем кодировку из конфига
            encoding = self.config.get('output_config', {}).get('encoding', 'utf-8')
            if self.logger:
                self.logger.debug(f"Кодировка файла: {encoding}")
            with open(output_path, 'w', encoding=encoding) as f:
                f.write(formatted_data)

            if self.logger:
                self.logger.info(f"Отчет успешно сохранен в {output_path}")

            return True

        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при сохранении отчета: {e}")
                import traceback
                self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
            return False

    # === ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ===


    def _build_url_filter(self) -> str:
        """
        Вспомогательный метод, собирающий общий фильтр в URL по шаблону из конфига

        Returns:
            str: Готовый фильтр для добавления к URL
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseReportParser._build_url_filter")

        try:
            # Получаем шаблоны из конфигурации
            filter_template = self.config.get("filter_template", "")
            date_filter_template = self.config.get("date_filter_template", "")
            data_type_filter_template = self.config.get("data_type_filter_template", "")

            if self.logger:
                self.logger.debug(f"Шаблоны фильтров: filter_template='{filter_template}', date_filter_template='{date_filter_template}', data_type_filter_template='{data_type_filter_template}'")

            # Получаем дату выполнения
            execution_date = self.config.get('execution_date', None)
            if self.logger:
                self.logger.debug(f"Дата выполнения: {execution_date}")

            if execution_date and filter_template:
                if self.logger:
                    self.logger.debug("Формируем фильтр с датой и типом данных")
                # Формируем фильтр по дате, подставляя дату в шаблон
                date_filter = date_filter_template.replace("{date}", execution_date)
                if self.logger:
                    self.logger.debug(f"Фильтр по дате: {date_filter}")

                # Формируем общий фильтр, подставляя составные части в основной шаблон
                # Заменяем плейсхолдеры в основном шаблоне на актуальные значения
                final_filter = filter_template.replace("{date_filter_template}", date_filter)
                final_filter = final_filter.replace("{data_type_filter_template}", data_type_filter_template)
                if self.logger:
                    self.logger.debug(f"Промежуточный фильтр: {final_filter}")

                # Исправляем двойные фигурные скобки, которые могут появиться из-за форматирования
                # Заменяем {{ на { и }} на }, если они образовались при подстановке
                final_filter = final_filter.replace('{{', '{').replace('}}', '}')
                if self.logger:
                    self.logger.debug(f"Финальный фильтр: {final_filter}")

                return final_filter
            else:
                if self.logger:
                    self.logger.debug("Формируем фильтр только с типом данных (нет даты или шаблона)")
                # Если даты нет или шаблона нет, формируем фильтр только с типом данных
                # Заменяем плейсхолдеры в основном шаблоне, учитывая, что фильтр по дате отсутствует
                # Убираем плейсхолдер фильтра по дате и лишние запятые
                final_filter = filter_template.replace("{date_filter_template},", "")
                final_filter = final_filter.replace(",{date_filter_template}", "")
                final_filter = final_filter.replace("{date_filter_template}", "")
                if self.logger:
                    self.logger.debug(f"Фильтр после удаления плейсхолдера даты: {final_filter}")

                # Заменяем плейсхолдер на реальное значение
                final_filter = final_filter.replace("{data_type_filter_template}", data_type_filter_template)
                if self.logger:
                    self.logger.debug(f"Фильтр после подстановки типа данных: {final_filter}")

                # Исправляем двойные фигурные скобки, которые могут появиться из-за форматирования
                final_filter = final_filter.replace('{{', '{').replace('}}', '}')
                if self.logger:
                    self.logger.debug(f"Финальный фильтр: {final_filter}")

                return final_filter
        except Exception as e:
            if self.logger:
                self.logger.debug(f"Ошибка при формировании фильтра URL: {e}")
            if self.logger:
                self.logger.error(f"Ошибка при формировании фильтра URL: {e}")
                import traceback
                self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
            return ""

    def _execute_multi_step_processing(self, multi_step_config):
        """
        Основной метод для выполнения мульти-шаговой обработки.
        Выполняет последовательную обработку всех шагов, определенных в конфигурации,
        и объединяет результаты в соответствии с логикой агрегации.

        Args:
            multi_step_config: Конфигурация мульти-шаговой обработки, содержащая:
                - steps: список имен шагов для выполнения
                - step_configurations: конфигурации для каждого шага
                - aggregation_logic: логика объединения результатов
                - nested_processing: общие параметры для вложенной обработки

        Returns:
            Dict[str, Any]: Объединенные результаты всех шагов в соответствии с логикой агрегации
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseReportParser._execute_multi_step_processing")

        all_step_results = {}

        # Получаем список шагов из конфигурации
        steps = multi_step_config.get("steps", [])
        step_configurations = multi_step_config.get("step_configurations", {})

        if self.logger:
            self.logger.debug(f"Количество шагов: {len(steps)}, шаги: {steps}")

        for step_name in steps:
            if self.logger:
                self.logger.debug(f"Обрабатываем шаг: {step_name}")
            # Получаем конфигурацию для текущего шага
            step_config = step_configurations.get(step_name, {})
            if self.logger:
                self.logger.debug(f"Конфигурация шага {step_name}: {step_config}")

            # Выполняем обработку текущего шага
            try:
                step_result = self._execute_single_step(step_config)
                if self.logger:
                    self.logger.debug(f"Результат шага {step_name}: {step_result}")
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Ошибка при выполнении шага {step_name}: {e}")
                    import traceback
                    self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
                # Продолжаем выполнение других шагов, даже если один из них не удался
                step_result = {"error": str(e)}
                
            # Сохраняем результат шага
            result_key = step_config.get("result_key", step_name)
            all_step_results[result_key] = step_result
            if self.logger:
                self.logger.debug(f"Результат сохранен под ключом {result_key}")

        if self.logger:
            self.logger.debug(f"Все результаты шагов: {all_step_results}")

        # Объединяем результаты всех шагов
        aggregation_config = multi_step_config.get("aggregation_logic", {})
        if self.logger:
            self.logger.debug(f"Конфигурация агрегации: {aggregation_config}")
        combined_results = self._combine_step_results(all_step_results, aggregation_config)
        if self.logger:
            self.logger.debug(f"Объединенные результаты: {combined_results}")

        return combined_results


    def _execute_single_step(self, step_config):
        """
        Метод для выполнения одного шага в мульти-шаговом процессе.
        Временно обновляет конфигурацию для выполнения конкретного шага,
        выполняет навигацию и извлекает данные в соответствии с типом обработки.

        Args:
            step_config: Конфигурация конкретного шага, содержащая:
                - параметры навигации (base_url, filter_template и т.д.)
                - processing_type: тип обработки (simple, table, table_nested)
                - data_extraction: параметры извлечения данных (для simple)
                - table_processing: параметры табличной обработки (для table, table_nested)
                - nested_processing: параметры вложенной обработки (для table_nested)
                - result_key: ключ для сохранения результата

        Returns:
            Результаты обработки шага в зависимости от типа обработки
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseReportParser._execute_single_step")

        # Временно обновляем конфигурацию для текущего шага
        if self.logger:
            self.logger.debug(f"Обновление конфигурации для шага: {step_config.get('result_key', 'unknown')}")
        original_config = self._update_config_for_step(step_config)
        if self.logger:
            self.logger.debug("Конфигурация обновлена, оригинальная сохранена")

        try:
            # Выполняем навигацию к целевой странице
            if self.logger:
                self.logger.debug("Выполняем навигацию к целевой странице")
            if not self.navigate_to_target():
                raise Exception("Не удалось выполнить навигацию к целевой странице для шага")

            # Сохраняем URL, с которого были извлечены данные для этого шага
            step_source_url = self.config.get('target_url', getattr(self.driver, 'current_url', 'Unknown') if getattr(self, 'driver', None) else 'Unknown')
            if self.logger:
                self.logger.debug(f"URL источника данных для шага: {step_source_url}")

            # Определяем тип обработки
            processing_type = step_config.get("processing_type", "simple")
            if self.logger:
                self.logger.debug(f"Тип обработки: {processing_type}")

            if processing_type == "simple":
                if self.logger:
                    self.logger.debug("Выполняем простое извлечение")
                result = self._handle_simple_extraction(step_config)
            elif processing_type == "table":
                if self.logger:
                    self.logger.debug("Выполняем табличное извлечение")
                result = self._handle_table_extraction(step_config)
            elif processing_type == "table_nested":
                if self.logger:
                    self.logger.debug("Выполняем вложенное табличное извлечение")
                result = self._handle_table_nested_extraction(step_config)
            else:
                raise Exception(f"Неизвестный тип обработки: {processing_type}")

            # Добавляем информацию об источнике данных к результату
            if isinstance(result, dict):
                result['__STEP_SOURCE_URL__'] = step_source_url
            else:
                result = {
                    'value': result,
                    '__STEP_SOURCE_URL__': step_source_url
                }

            if self.logger:
                self.logger.debug(f"Результат обработки шага: {result}")
            return result
        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при выполнении одного шага: {e}")
                import traceback
                self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
            raise
        finally:
            # Восстанавливаем исходную конфигурацию
            if self.logger:
                self.logger.debug("Восстанавливаем исходную конфигурацию")
            self.config.update(original_config)
            if self.logger:
                self.logger.debug("Конфигурация восстановлена")

    def _update_config_for_step(self, step_config):
        """
        Метод для временного обновления конфигурации для выполнения конкретного шага.
        Сохраняет текущую конфигурацию, применяет параметры из конфигурации шага
        и возвращает копию оригинальной конфигурации для последующего восстановления.

        Args:
            step_config: Конфигурация шага, содержащая параметры, которые нужно
                        временно применить к основной конфигурации, включая:
                - параметры навигации (base_url, filter_template и т.д.)
                - параметры обработки данных
                - другие специфичные для шага параметры
                - исключая служебные ключи (processing_type, data_extraction и т.д.)

        Returns:
            Dict: Копия оригинальной конфигурации для восстановления после обработки шага
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseReportParser._update_config_for_step")

        # Сохраняем текущую конфигурацию
        original_config = self.config.copy()
        if self.logger:
            self.logger.debug(f"Оригинальная конфигурация сохранена, содержит {len(original_config)} элементов")

        # Применяем параметры из step_config к основной конфигурации
        for key, value in step_config.items():
            if key not in ["processing_type", "data_extraction", "table_processing", "nested_processing", "result_key"]:
                if self.logger:
                    self.logger.debug(f"Обновляем параметр {key}: {value}")
                self.config[key] = value

        if self.logger:
            self.logger.debug(f"Конфигурация обновлена, теперь содержит {len(self.config)} элементов")
        return original_config

    def _handle_simple_extraction(self, step_config):
        """
        Метод для извлечения данных при simple processing_type.
        Использует конфигурацию data_extraction для извлечения значения из элемента страницы
        с применением селектора, регулярного выражения и постобработки.

        Args:
            step_config: Конфигурация шага, содержащая:
                - data_extraction: словарь с параметрами извлечения, включающий:
                    - selector: XPath или CSS селектор для поиска элемента
                    - pattern: регулярное выражение для извлечения части текста
                    - element_type: тип элемента (div, input и т.д.)
                    - post_processing: параметры постобработки значения

        Returns:
            Извлеченные данные после применения всех преобразований
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseReportParser._handle_simple_extraction")

        extraction_config = step_config.get("data_extraction", {})
        if self.logger:
            self.logger.debug(f"Используемая конфигурация извлечения: {extraction_config}")

        # Извлекаем значение с помощью универсального метода
        try:
            result = self._extract_value_by_config(extraction_config)
            if self.logger:
                self.logger.debug(f"Результат извлечения: {result}")
        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при простом извлечении данных: {e}")
                import traceback
                self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
            result = {"error": str(e)}
            
        return result

    def _handle_table_extraction(self, step_config):
        """
        Метод для извлечения данных при table processing_type.
        Использует конфигурацию table_processing для извлечения данных из HTML-таблицы
        с помощью существующего метода extract_table_data.

        Args:
            step_config: Конфигурация шага, содержащая:
                - table_processing: словарь с параметрами табличной обработки, включающий:
                    - table_config_key: ключ для получения конфигурации таблицы из config
                    - id_column: имя колонки, содержащей идентификаторы (для table_nested)
                    - result_mapping: карта сопоставления результатов

        Returns:
            Извлеченные табличные данные в виде списка словарей
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseReportParser._handle_table_extraction")

        table_processing_config = step_config.get("table_processing", {})
        if self.logger:
            self.logger.debug(f"Конфигурация табличной обработки: {table_processing_config}")

        table_config_key = table_processing_config.get("table_config_key")

        if not table_config_key:
            raise Exception("Не указан table_config_key для табличной обработки")

        if self.logger:
            self.logger.debug(f"Извлечение табличных данных по ключу: {table_config_key}")

        # Извлекаем табличные данные
        try:
            table_data = self.extract_table_data(table_config_key=table_config_key)
            if self.logger:
                self.logger.debug(f"Извлечено {len(table_data) if isinstance(table_data, list) else 'unknown'} строк табличных данных")
        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при табличной обработке данных: {e}")
                import traceback
                self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
            table_data = [{"error": str(e)}]
            
        return table_data

    def _handle_table_nested_extraction(self, step_config):
        """
        Метод для извлечения данных при table_nested processing_type.
        Сначала извлекает табличные данные, затем для каждой строки выполняет
        вложенную обработку с использованием параметров nested_processing,
        и агрегирует результаты в соответствии с конфигурацией агрегации.

        Args:
            step_config: Конфигурация шага, содержащая:
                - table_processing: параметры табличной обработки
                - nested_processing: параметры вложенной обработки, включающие:
                    - enabled: флаг включения вложенной обработки
                    - base_url_template: шаблон URL для вложенных вызовов
                    - filter_template: шаблон фильтра для вложенных вызовов
                    - data_extraction: параметры извлечения данных из вложенных вызовов
                    - aggregation: параметры агрегации результатов
                    - data_type_filter_template: шаблон фильтра по типу данных

        Returns:
            Агрегированные результаты вложенной обработки
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseReportParser._handle_table_nested_extraction")

        # Сначала извлекаем табличные данные
        if self.logger:
            self.logger.debug("Начало извлечения табличных данных для вложенной обработки")
        table_data = self._handle_table_extraction(step_config)

        # Получаем конфигурацию вложенной обработки
        nested_config = step_config.get("nested_processing", {})
        if self.logger:
            self.logger.debug(f"Конфигурация вложенной обработки: {nested_config}")

        if not nested_config.get("enabled", False):
            if self.logger:
                self.logger.debug("Вложенная обработка отключена, возвращаем табличные данные")
            return table_data

        # Извлекаем идентификаторы из указанной колонки
        table_processing_config = step_config.get("table_processing", {})
        id_column = table_processing_config.get("id_column")

        if not id_column:
            raise Exception("Не указана id_column для вложенной обработки")

        if self.logger:
            self.logger.debug(f"Извлечение идентификаторов из колонки: {id_column}")

        identifiers = []
        for row in table_data:
            if id_column in row:
                identifiers.append(row[id_column])

        if self.logger:
            self.logger.debug(f"Найдено {len(identifiers)} идентификаторов для вложенной обработки")

        # Выполняем вложенную обработку для каждого идентификатора
        nested_results = self._handle_nested_processing(nested_config, identifiers)

        # Агрегируем результаты вложенной обработки
        aggregation_config = nested_config.get("aggregation", {})
        aggregated_result = self._aggregate_nested_results(nested_results, aggregation_config)

        if self.logger:
            self.logger.debug(f"Результаты вложенной обработки агрегированы: {aggregated_result}")

        return aggregated_result

    def _handle_nested_processing(self, nested_config, identifiers):
        """
        Метод для обработки вложенной логики.
        Для каждого идентификатора из списка формирует URL на основе шаблона,
        временно обновляет конфигурацию, выполняет навигацию и извлекает данные
        в соответствии с конфигурацией извлечения.

        Args:
            nested_config: Конфигурация вложенной обработки, содержащая:
                - enabled: флаг включения вложенной обработки
                - base_url_template: шаблон URL с плейсхолдерами для подстановки идентификаторов
                - filter_template: шаблон фильтра для вложенных вызовов
                - data_type_filter_template: шаблон фильтра по типу данных
                - data_extraction: параметры извлечения данных из вложенных вызовов
            identifiers: Список идентификаторов для обработки, каждый из которых
                        будет подставлен в шаблон URL

        Returns:
            Результаты вложенной обработки в виде списка словарей с информацией
            об идентификаторе, извлеченном значении и URL
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseReportParser._handle_nested_processing")
        results = []

        if self.logger:
            self.logger.debug(f"Начало вложенной обработки для {len(identifiers)} идентификаторов")
            self.logger.debug(f"Конфигурация вложенной обработки: {nested_config}")

        for i, identifier in enumerate(identifiers):
            if self.logger:
                self.logger.debug(f"Обработка идентификатора {i+1}/{len(identifiers)}: {identifier}")
            
            # Обновляем URL с использованием шаблона
            base_url_template = nested_config.get("base_url_template", "")

            # Заменяем плейсхолдеры в шаблоне URL
            target_url = base_url_template.replace("{carriage_id}", str(identifier))

            if self.logger:
                self.logger.debug(f"Целевой URL для идентификатора {identifier}: {target_url}")

            # Временно сохраняем оригинальную конфигурацию
            original_url = self.config.get("base_url", "")
            original_filter = self.config.get("filter_template", "")
            original_data_filter = self.config.get("data_type_filter_template", "")

            try:
                # Обновляем конфигурацию для вложенной обработки
                self.config["base_url"] = target_url
                self.config["filter_template"] = nested_config.get("filter_template", "")
                self.config["data_type_filter_template"] = nested_config.get("data_type_filter_template", "")

                # Выполняем навигацию к новому URL
                if not self.navigate_to_target():
                    if self.logger:
                        self.logger.warning(f"Не удалось выполнить навигацию к URL для идентификатора {identifier}")
                    continue

                # Извлекаем данные с использованием конфигурации извлечения
                extraction_config = nested_config.get("data_extraction", {})
                if self.logger:
                    self.logger.debug(f"Конфигурация извлечения: {extraction_config}")
                extracted_value = self._extract_value_by_config(extraction_config)

                # Используем финальный URL, который был сформирован в navigate_to_target
                final_url = self.config.get("target_url", target_url)

                if self.logger:
                    self.logger.debug(f"Извлеченное значение для {identifier}: {extracted_value}")

                # Добавляем результат в список
                results.append({
                    "identifier": identifier,
                    "value": extracted_value,
                    "url": final_url
                })

            except Exception as e:
                if self.logger:
                    self.logger.error(f"Ошибка при обработке идентификатора {identifier}: {e}")
                    import traceback
                    self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
                continue
            finally:
                # Восстанавливаем оригинальную конфигурацию
                self.config["base_url"] = original_url
                self.config["filter_template"] = original_filter
                self.config["data_type_filter_template"] = original_data_filter

        if self.logger:
            self.logger.debug(f"Вложенная обработка завершена, получено {len(results)} результатов")
        return results

    def _aggregate_nested_results(self, nested_results, aggregation_config):
        """
        Метод для агрегации результатов вложенной обработки

        Args:
            nested_results: Результаты вложенной обработки
            aggregation_config: Конфигурация агрегации

        Returns:
            Агрегированные результаты
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseReportParser._aggregate_nested_results")
        if self.logger:
            self.logger.debug(f"Агрегация вложенных результатов: количество={len(nested_results) if nested_results else 0}, конфигурация={aggregation_config}")
            
        method = aggregation_config.get("method", "sum")
        target_field = aggregation_config.get("target_field", "aggregated_value")

        values = []
        for result in nested_results:
            if isinstance(result.get("value"), (int, float)):
                values.append(result["value"])

        if self.logger:
            self.logger.debug(f"Найдено {len(values)} числовых значений для агрегации")
            
        aggregated_value = self._aggregate_values(values, method, target_field)

        result = {
            target_field: aggregated_value,
            "details": nested_results
        }
        
        if self.logger:
            self.logger.debug(f"Результат агрегации: {result}")
            
        return result

    def __filter_structure_by_available_keys(self, data, available_keys):
        """
        Рекурсивно фильтрует структуру данных, оставляя только те плейсхолдеры,
        для которых есть соответствующие ключи в available_keys

        Args:
            data: Данные любого типа (строка, словарь, список и т.д.)
            available_keys: Список или множество доступных ключей для замены плейсхолдеров

        Returns:
            Отфильтрованные данные, содержащие только допустимые плейсхолдеры
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseReportParser.__filter_structure_by_available_keys")
        if self.logger:
            self.logger.debug(f"Фильтрация структуры по доступным ключам: {list(available_keys)[:10]}{'...' if len(available_keys) > 10 else ''}")
            
        if isinstance(data, str):
            if self.logger:
                self.logger.debug(f"Обработка строки: {data[:50]}{'...' if len(data) > 50 else ''}")
            # Для строки проверяем, содержит ли она хотя бы один доступный плейсхолдер
            has_available_placeholder = False
            for key in available_keys:
                placeholder = '{' + key + '}'
                if placeholder in data:
                    if self.logger:
                        self.logger.debug(f"Найден доступный плейсхолдер '{placeholder}' в строке")
                    has_available_placeholder = True
                    break

            # Если строка содержит хотя бы один доступный плейсхолдер, включая специальные (__LOCATION_INFO__, и т.д.), возвращаем её
            # Иначе возвращаем None, чтобы пометить для удаления
            if has_available_placeholder:
                if self.logger:
                    self.logger.debug("Строка содержит доступные плейсхолдеры, возвращаем как есть")
                return data
            else:
                # Проверяем, содержит ли строка специальные плейсхолдеры (__LOCATION_INFO__, __EXTRACTION_TIMESTAMP__, и т.д.)
                special_placeholders = ['__LOCATION_INFO__', '__EXTRACTION_TIMESTAMP__', '__SOURCE_URL__', '__EXECUTION_DATE__']
                for special_placeholder in special_placeholders:
                    if f'{{{special_placeholder}}}' in data:
                        if self.logger:
                            self.logger.debug(f"Найден специальный плейсхолдер '{special_placeholder}', возвращаем строку")
                        return data  # Возвращаем строку, если она содержит специальный плейсхолдер
                if self.logger:
                    self.logger.debug("Строка не содержит доступных плейсхолдеров, возвращаем None")
                return None
        elif isinstance(data, dict):
            if self.logger:
                self.logger.debug(f"Обработка словаря с {len(data)} элементами")
            # Обработка словаря - рекурсивно фильтруем значения
            result = {}
            for key, value in data.items():
                if self.logger:
                    self.logger.debug(f"Обработка ключа '{key}' в словаре")
                filtered_value = self.__filter_structure_by_available_keys(value, available_keys)
                # Добавляем в результат только если значение не None
                if filtered_value is not None:
                    result[key] = filtered_value
                else:
                    if self.logger:
                        self.logger.debug(f"Значение для ключа '{key}' отфильтровано (None)")

            # Если словарь пустой после фильтрации, возвращаем None
            if result:
                if self.logger:
                    self.logger.debug(f"Словарь после фильтрации содержит {len(result)} элементов")
                return result
            else:
                if self.logger:
                    self.logger.debug("Словарь после фильтрации пуст, возвращаем None")
                return None
        elif isinstance(data, list):
            if self.logger:
                self.logger.debug(f"Обработка списка с {len(data)} элементами")
            # Обработка списка - рекурсивно фильтруем элементы
            result = []
            for i, item in enumerate(data):
                if self.logger:
                    self.logger.debug(f"Обработка элемента {i} в списке")
                filtered_item = self.__filter_structure_by_available_keys(item, available_keys)
                # Добавляем в результат только если элемент не None
                if filtered_item is not None:
                    result.append(filtered_item)
                else:
                    if self.logger:
                        self.logger.debug(f"Элемент {i} отфильтрован (None)")

            # Если список пустой после фильтрации, возвращаем None
            if result:
                if self.logger:
                    self.logger.debug(f"Список после фильтрации содержит {len(result)} элементов")
                return result
            else:
                if self.logger:
                    self.logger.debug("Список после фильтрации пуст, возвращаем None")
                return None
        else:
            # Для остальных типов данных возвращаем как есть
            if self.logger:
                self.logger.debug(f"Возвращаем данные без изменений (тип: {type(data)}): {data}")
            return data

    def _replace_placeholders_recursive(self, data, replacements):
        """
        Рекурсивно заменяет плейсхолдеры в структуре данных на значения из replacements

        Args:
            data: Данные любого типа (строка, словарь, список и т.д.)
            replacements: Словарь с парами ключ-значение для замены плейсхолдеров

        Returns:
            Обработанные данные с замененными плейсхолдерами
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseReportParser._replace_placeholders_recursive")
        if self.logger:
            self.logger.debug(f"Замена плейсхолдеров в данных: {type(data)}, replacements: {list(replacements.keys())}")
            
        if isinstance(data, str):
            # Обработка строки - заменяем плейсхолдеры
            result = data
            # Check if the string is a single placeholder (e.g., "{giveout_count}")
            # If so, return the replacement value directly instead of converting to string
            for key, value in replacements.items():
                placeholder = '{' + key + '}'
                if result.strip() == placeholder:
                    # This is a single placeholder, return the value directly
                    if self.logger:
                        self.logger.debug(f"Найден одиночный плейсхолдер '{placeholder}', возвращаем значение: {value}")
                    return value
                elif placeholder in result:
                    # This is a string with embedded placeholders, convert value to string
                    if self.logger:
                        self.logger.debug(f"Заменяем плейсхолдер '{placeholder}' на значение: {value}")
                    result = result.replace(placeholder, str(value))
            if self.logger:
                self.logger.debug(f"Результат обработки строки: {result}")
            return result
        elif isinstance(data, dict):
            # Обработка словаря - рекурсивно обрабатываем значения
            if self.logger:
                self.logger.debug(f"Обработка словаря с {len(data)} элементами")
            result = {}
            for key, value in data.items():
                if self.logger:
                    self.logger.debug(f"Обработка ключа '{key}' в словаре")
                result[key] = self._replace_placeholders_recursive(value, replacements)
            return result
        elif isinstance(data, list):
            # Обработка списка - рекурсивно обрабатываем элементы
            if self.logger:
                self.logger.debug(f"Обработка списка с {len(data)} элементами")
            result = []
            for i, item in enumerate(data):
                if self.logger:
                    self.logger.debug(f"Обработка элемента {i} в списке")
                result.append(self._replace_placeholders_recursive(item, replacements))
            return result
        else:
            # Для остальных типов данных возвращаем как есть
            if self.logger:
                self.logger.debug(f"Возвращаем данные без изменений (тип: {type(data)}): {data}")
            return data

    def _combine_step_results(self, all_step_results, aggregation_config):
        """
        Метод для объединения результатов всех шагов.
        Применяет логику агрегации из конфигурации к результатам всех шагов
        и формирует итоговую структуру данных в соответствии с заданной структурой.

        Args:
            all_step_results: Словарь с результатами всех шагов, где ключи - это
                            имена шагов или result_key из конфигурации шагов
            aggregation_config: Конфигурация агрегации, содержащая:
                - combine_nested_results: флаг объединения вложенных результатов
                - sum_nested_values: список полей, значения которых нужно суммировать
                - result_structure: структура итогового результата с шаблонами подстановки
                - aggregation_logic: дополнительная логика агрегации

        Returns:
            Объединенные результаты в формате, определенном в result_structure
            или в виде словаря с результатами всех шагов, если структура не задана
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseReportParser._combine_step_results")
        if self.logger:
            self.logger.debug(f"Результаты всех шагов: {all_step_results}")
        if self.logger:
            self.logger.debug(f"Конфигурация агрегации: {aggregation_config}")

        # Если нет специфичной логики агрегации, возвращаем все результаты как есть
        if not aggregation_config:
            if self.logger:
                self.logger.debug("Нет конфигурации агрегации, возвращаем результаты как есть")
            return all_step_results

        # Применяем логику агрегации
        combine_nested = aggregation_config.get("combine_nested_results", True)
        sum_nested_values = aggregation_config.get("sum_nested_values", [])
        result_structure = aggregation_config.get("result_structure", {})

        if self.logger:
            self.logger.debug(f"combine_nested: {combine_nested}, sum_nested_values: {sum_nested_values}, result_structure: {result_structure}")

        # Если нужно объединить вложенные результаты
        if combine_nested:
            for field_name in sum_nested_values:
                if field_name in all_step_results:
                    # Суммируем значения для указанного поля
                    pass  # Реализация зависит от структуры данных

        # Применяем структуру результата
        if result_structure:
            if self.logger:
                self.logger.debug("Применяем структуру результата")
            # Извлекаем доступные ключи из результатов шагов
            available_keys = set(all_step_results.keys())
            if self.logger:
                self.logger.debug(f"Доступные ключи для фильтрации: {available_keys}")

            # Извлекаем все __STEP_SOURCE_URL__ из результатов шагов до подстановки плейсхолдеров
            step_urls = []
            for key, value in all_step_results.items():
                if isinstance(value, dict) and '__STEP_SOURCE_URL__' in value:
                    step_urls.append(value['__STEP_SOURCE_URL__'])
                elif isinstance(value, str) and '__STEP_SOURCE_URL__' in value:
                    # Если значение - строка, содержащая __STEP_SOURCE_URL__,
                    # нужно извлечь URL из этой строки
                    import re
                    match = re.search(r"'__STEP_SOURCE_URL__': '([^']+)'", value)
                    if match:
                        step_urls.append(match.group(1))
                elif isinstance(value, dict):
                    # Если значение - словарь, проверим вложенные элементы
                    for sub_key, sub_value in value.items():
                        if isinstance(sub_value, dict) and '__STEP_SOURCE_URL__' in sub_value:
                            step_urls.append(sub_value['__STEP_SOURCE_URL__'])

            # Получаем общую информацию отчета, передавая результаты всех шагов для определения __SOURCE_URL__
            common_info = self.get_common_report_info(all_step_results)
            if self.logger:
                self.logger.debug(f"Общая информация отчета: {common_info}")

            # Объединяем ключи из результатов шагов и общей информации
            all_available_keys = available_keys.union(set(common_info.keys()))
            if self.logger:
                self.logger.debug(f"Все доступные ключи (шаги + общая информация): {all_available_keys}")

            # Фильтруем структуру по всем доступным ключам (включая специальные плейсхолдеры)
            filtered_structure = self.__filter_structure_by_available_keys(result_structure, all_available_keys)
            if self.logger:
                self.logger.debug(f"Отфильтрованная структура: {filtered_structure}")

            # Если после фильтрации структура пуста, возвращаем результаты шагов как есть
            if filtered_structure is None:
                if self.logger:
                    self.logger.debug("Отфильтрованная структура пуста, возвращаем результаты шагов как есть")
                return all_step_results

            # Объединяем все замены (результаты шагов + общая информация)
            all_replacements = {**all_step_results, **common_info}

            # Используем рекурсивный метод для замены всех плейсхолдеров в отфильтрованной структуре
            try:
                final_result = self._replace_placeholders_recursive(filtered_structure, all_replacements)
                if self.logger:
                    self.logger.debug(f"Финальный результат после подстановки: {final_result}")
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Ошибка при замене плейсхолдеров: {e}")
                    import traceback
                    self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
                # В случае ошибки возвращаем результаты шагов как есть
                return all_step_results
                
            return final_result
        else:
            if self.logger:
                self.logger.debug("Нет структуры результата, возвращаем результаты как есть")

            # Добавляем общую информацию к результатам, если структура не задана
            # Передаем результаты всех шагов для определения __SOURCE_URL__
            common_info = self.get_common_report_info(all_step_results)
            # Преобразуем специальные ключи в нормальные
            normalized_common_info = {
                k.replace('__', '').replace('__', '').lower(): v
                for k, v in common_info.items()
            }
            all_step_results.update(normalized_common_info)

            return all_step_results

    def _apply_post_processing(self, value, post_processing_config):
        """
        Метод для применения постобработки к извлеченным значениям.
        Выполняет преобразование типа, установку значений по умолчанию
        и другие операции с извлеченным значением в соответствии с конфигурацией.

        Args:
            value: Значение для постобработки (обычно строка или число)
            post_processing_config: Конфигурация постобработки, содержащая:
                - convert_to: тип для преобразования ('int', 'float', 'str')
                - default_value: значение по умолчанию, если преобразование не удалось
                - другие параметры постобработки (в будущем)

        Returns:
            Обработанное значение с примененными преобразованиями
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseReportParser._apply_post_processing")
        if not post_processing_config:
            if self.logger:
                self.logger.debug("Конфигурация постобработки пуста, возвращаем исходное значение")
            return value

        if self.logger:
            self.logger.debug(f"Применение постобработки: значение='{value}', конфигурация={post_processing_config}")

        # Применяем преобразование типа
        convert_to = post_processing_config.get("convert_to")
        if convert_to == "int":
            try:
                value = int(float(str(value)))  # Сначала в float, затем в int, чтобы обработать строки с дробями
                if self.logger:
                    self.logger.debug(f"Значение преобразовано в int: {value}")
            except (ValueError, TypeError):
                default_value = post_processing_config.get("default_value", 0)
                value = default_value
                if self.logger:
                    self.logger.debug(f"Ошибка преобразования в int, установлено значение по умолчанию: {value}")
        elif convert_to == "float":
            try:
                value = float(value)
                if self.logger:
                    self.logger.debug(f"Значение преобразовано в float: {value}")
            except (ValueError, TypeError):
                default_value = post_processing_config.get("default_value", 0.0)
                value = default_value
                if self.logger:
                    self.logger.debug(f"Ошибка преобразования в float, установлено значение по умолчанию: {value}")
        elif convert_to == "str":
            value = str(value) if value is not None else ""
            if self.logger:
                self.logger.debug(f"Значение преобразовано в str: {value}")

        return value

    def _extract_value_by_config(self, extraction_config):
        """
        Универсальный метод для извлечения значения по конфигурации.
        Использует селектор для поиска элемента на странице, применяет регулярное
        выражение для извлечения части текста и выполняет постобработку значения.

        Args:
            extraction_config: Конфигурация извлечения данных, содержащая:
                - selector: XPath или CSS селектор для поиска элемента
                - pattern: регулярное выражение для извлечения части текста (опционально)
                - element_type: тип элемента (div, input и т.д.) для корректной обработки
                - post_processing: параметры постобработки значения (опционально)

        Returns:
            Извлеченное и обработанное значение, которое может быть подвергнуто
            преобразованию типа и другим операциям постобработки
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseReportParser._extract_value_by_config")

        if not extraction_config:
            if self.logger:
                self.logger.debug("Конфигурация извлечения пуста, возвращаем None")
            return None

        selector = extraction_config.get("selector")
        pattern = extraction_config.get("pattern")
        element_type = extraction_config.get("element_type", "div")

        if self.logger:
            self.logger.debug(f"Извлечение: selector='{selector}', pattern='{pattern}', element_type='{element_type}'")

        if not selector:
            if self.logger:
                self.logger.debug("Селектор не задан, возвращаем None")
            return None

        # Извлекаем значение с помощью улучшенного get_element_value, который теперь поддерживает pattern
        try:
            raw_value = self.get_element_value(selector=selector, element_type=element_type, pattern=pattern)
            if self.logger:
                self.logger.debug(f"Извлеченное значение (с примененным паттерном): '{raw_value}'")

            # Применяем постобработку
            post_processing_config = extraction_config.get("post_processing", {})
            if self.logger:
                self.logger.debug(f"Применяем постобработку: {post_processing_config}")
            processed_value = self._apply_post_processing(raw_value, post_processing_config)
            if self.logger:
                self.logger.debug(f"Значение после постобработки: '{processed_value}'")

            return processed_value
        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при извлечении значения по конфигурации: {e}")
                import traceback
                self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
            return None

    def get_common_report_info(self, all_step_results=None):
        """
        Метод для получения общей информации отчета

        Args:
            all_step_results: Результаты всех шагов мульти-шаговой обработки (опционально)
                              Если переданы, используется для определения __SOURCE_URL__ как общего префикса
                              всех __STEP_SOURCE_URL__ из результатов шагов

        Returns:
            Dict[str, Any]: Словарь с общей информацией отчета, включающий:
                - location_info: информация о местоположении/ПВЗ
                - extraction_timestamp: время извлечения данных
                - source_url: URL источника данных
                - execution_date: дата выполнения
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseReportParser.get_common_report_info")
        # Получаем информацию о местоположении (переопределяется в дочерних классах)
        try:
            location_info = getattr(self, 'get_current_pvz', lambda: 'Unknown')()
            if self.logger:
                self.logger.debug(f"Информация о местоположении: {location_info}")
        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при получении информации о местоположении: {e}")
                location_info = 'Unknown'

        # Получаем время извлечения данных
        try:
            extraction_timestamp = self._get_current_timestamp()
            if self.logger:
                self.logger.debug(f"Время извлечения данных: {extraction_timestamp}")
        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при получении времени извлечения данных: {e}")
                extraction_timestamp = 'Unknown'

        # Получаем URL источника
        # Если переданы результаты всех шагов, формируем общий префикс из __STEP_SOURCE_URL__
        if all_step_results:
            if self.logger:
                self.logger.debug(f"all_step_results: {all_step_results}")
            step_urls = []
            for key, value in all_step_results.items():
                if self.logger:
                    self.logger.debug(f"Обрабатываем ключ: {key}, значение: {value}, тип: {type(value)}")

                # Проверяем, является ли значение словарем с __STEP_SOURCE_URL__
                if isinstance(value, dict):
                    if '__STEP_SOURCE_URL__' in value:
                        step_urls.append(value['__STEP_SOURCE_URL__'])
                        if self.logger:
                            self.logger.debug(f"Найден __STEP_SOURCE_URL__ в словаре: {value['__STEP_SOURCE_URL__']}")
                    else:
                        # Если в значении есть вложенные словари, проверим их тоже
                        for sub_key, sub_value in value.items():
                            if isinstance(sub_value, dict) and '__STEP_SOURCE_URL__' in sub_value:
                                step_urls.append(sub_value['__STEP_SOURCE_URL__'])
                                if self.logger:
                                    self.logger.debug(f"Найден __STEP_SOURCE_URL__ во вложенном словаре: {sub_value['__STEP_SOURCE_URL__']}")
                elif isinstance(value, str) and '__STEP_SOURCE_URL__' in value:
                    # Если значение - строка, содержащая __STEP_SOURCE_URL__,
                    # нужно извлечь URL из этой строки
                    import re
                    # Используем более надежное регулярное выражение для извлечения URL
                    matches = re.findall(r"'__STEP_SOURCE_URL__': '([^']+)'", value)
                    for match in matches:
                        step_urls.append(match)
                        if self.logger:
                            self.logger.debug(f"Найден __STEP_SOURCE_URL__ в строке: {match}")
                elif isinstance(value, str) and key.startswith('__STEP_SOURCE_URL__'):
                    step_urls.append(value)
                    if self.logger:
                        self.logger.debug(f"Найден __STEP_SOURCE_URL__ в ключе: {value}")

            if step_urls:
                source_url = self._get_common_url_prefix(step_urls)
                if self.logger:
                    self.logger.debug(f"Найденные STEP_SOURCE_URL: {step_urls}")
                    self.logger.debug(f"Общий префикс URL: {source_url}")
            else:
                # Если __STEP_SOURCE_URL__ не найдены, используем старую логику
                if self.logger:
                    self.logger.debug("Не найдены STEP_SOURCE_URL, используем старую логику")
                source_url = self.config.get('target_url', getattr(self.driver, 'current_url', 'Unknown') if getattr(self, 'driver', None) else 'Unknown')
        else:
            # Используем старую логику, если результаты шагов не переданы
            if self.logger:
                self.logger.debug("all_step_results пустой, используем старую логику")
            source_url = self.config.get('target_url', getattr(self.driver, 'current_url', 'Unknown') if getattr(self, 'driver', None) else 'Unknown')

        # Получаем дату выполнения
        execution_date = self.config.get('execution_date', '')
        if self.logger:
            self.logger.debug(f"Дата выполнения: {execution_date}")

        result = {
            '__LOCATION_INFO__': location_info,
            '__EXTRACTION_TIMESTAMP__': extraction_timestamp,
            '__SOURCE_URL__': source_url,
            '__EXECUTION_DATE__': execution_date
        }
        
        if self.logger:
            self.logger.debug(f"Общая информация отчета: {result}")
            
        return result

    def _get_common_url_prefix(self, urls):
        """
        Находит общий префикс для списка URL

        Args:
            urls: Список URL-адресов

        Returns:
            str: Общий префикс URL
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseReportParser._get_common_url_prefix")
            self.logger.debug(f"_get_common_url_prefix: входные URL: {urls}")

        if not urls:
            if self.logger:
                self.logger.debug("_get_common_url_prefix: список URL пуст, возвращаем пустую строку")
            return ""

        # Преобразуем все URL в строки и удаляем 'Unknown' значения
        valid_urls = [url for url in urls if url != 'Unknown' and url is not None]

        if not valid_urls:
            if self.logger:
                self.logger.debug("_get_common_url_prefix: нет допустимых URL, возвращаем пустую строку")
            return ""

        if self.logger:
            self.logger.debug(f"_get_common_url_prefix: допустимые URL: {valid_urls}")

        # Используем первый URL как основу для сравнения
        from urllib.parse import urlparse
        try:
            parsed_urls = [urlparse(url) for url in valid_urls]
        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при парсинге URL: {e}")
                import traceback
                self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
            return ""

        # Найдем общий префикс для scheme, netloc и path
        base_parsed = parsed_urls[0]
        common_scheme = base_parsed.scheme
        common_netloc = base_parsed.netloc
        common_path = base_parsed.path

        if self.logger:
            self.logger.debug(f"_get_common_url_prefix: начальный common_scheme: {common_scheme}, common_netloc: {common_netloc}, common_path: {common_path}")

        # Проверяем остальные URL
        for parsed_url in parsed_urls[1:]:
            # Проверяем схему
            if parsed_url.scheme != common_scheme:
                # Если схемы отличаются, возвращаем пустую строку или базовую
                # В данном случае, оставляем первую схему
                if self.logger:
                    self.logger.debug(f"_get_common_url_prefix: схемы отличаются: {common_scheme} != {parsed_url.scheme}")

            # Находим общий префикс для netloc (домена) вручную
            # Не используем os.path.commonprefix, так как он работает неправильно для доменов
            min_len = min(len(common_netloc), len(parsed_url.netloc))
            common_netloc_chars = []
            for i in range(min_len):
                if common_netloc[i] == parsed_url.netloc[i]:
                    common_netloc_chars.append(common_netloc[i])
                else:
                    break
            common_netloc = ''.join(common_netloc_chars)

            # Находим общий префикс для пути вручную
            min_path_len = min(len(common_path), len(parsed_url.path))
            common_path_chars = []
            for i in range(min_path_len):
                if common_path[i] == parsed_url.path[i]:
                    common_path_chars.append(common_path[i])
                else:
                    break
            common_path = ''.join(common_path_chars)

            if self.logger:
                self.logger.debug(f"_get_common_url_prefix: после обработки {parsed_url.geturl()}, common_netloc: {common_netloc}, common_path: {common_path}")

        # Убедимся, что мы не разрезаем домен посередине
        # Если общий префикс содержит только часть домена, расширим его до полного домена
        if common_netloc and '.' in common_netloc:
            # Разобьем на части и убедимся, что получили хотя бы домен второго уровня
            domain_parts = common_netloc.split('.')
            if len(domain_parts) >= 2:
                # Проверим, является ли найденный префикс полным доменом или его частью
                # Если все исходные URL имели одинаковый домен, то common_netloc уже содержит полный домен
                # В противном случае, найдем домен второго уровня
                base_domain_parts = base_parsed.netloc.split('.')
                # Если длина найденного префикса совпадает с длиной базового домена,
                # и префикс совпадает с началом базового домена, значит это полный домен
                if len(domain_parts) == len(base_domain_parts) and base_parsed.netloc.startswith(common_netloc):
                    # Оставляем найденный префикс как есть, так как это полный домен
                    if self.logger:
                        self.logger.debug(f"_get_common_url_prefix: найден полный домен: {common_netloc}")
                else:
                    # Найдем домен второго уровня
                    sld_parts = domain_parts[-2:]  # последние 2 части (домен второго уровня)
                    common_netloc = '.'.join(sld_parts)
                    if self.logger:
                        self.logger.debug(f"_get_common_url_prefix: найден домен второго уровня: {common_netloc}")
            else:
                # Если не можем определить домен второго уровня, используем как есть
                common_netloc = base_parsed.netloc
                if self.logger:
                    self.logger.debug(f"_get_common_url_prefix: используем базовый домен: {common_netloc}")
        else:
            # Если общий префикс слишком короткий, используем домен первого URL
            common_netloc = base_parsed.netloc
            if self.logger:
                self.logger.debug(f"_get_common_url_prefix: используем базовый домен из-за короткого префикса: {common_netloc}")

        # Воссоздаем общий префикс URL
        # Удалим конечный слэш из пути, если он есть, чтобы избежать дублирования
        if common_path and common_path != '/':
            if common_path.endswith('/'):
                common_path = common_path.rstrip('/')
            # Если путь пустой или равен '/', не добавляем его к URL
            common_prefix = f"{common_scheme}://{common_netloc}{common_path}"
            if self.logger:
                self.logger.debug(f"_get_common_url_prefix: общий префикс с путем: {common_prefix}")
        else:
            common_prefix = f"{common_scheme}://{common_netloc}"
            if self.logger:
                self.logger.debug(f"_get_common_url_prefix: общий префикс без пути: {common_prefix}")

        if self.logger:
            self.logger.debug(f"_get_common_url_prefix: возвращаем общий префикс: {common_prefix}")

        return common_prefix

    def _aggregate_values(self, values, aggregation_method, target_field):
        """
        Метод для агрегации значений по указанному методу.
        Применяет указанный метод агрегации к списку значений и возвращает результат.

        Args:
            values: Список значений для агрегации (обычно числовые значения)
            aggregation_method: Метод агрегации:
                - 'sum': суммирование значений
                - 'average': вычисление среднего значения
                - 'count': подсчет количества значений
                - 'max': нахождение максимального значения
                - 'min': нахождение минимального значения
            target_field: Имя целевого поля (используется для логирования или отладки,
                         но не влияет на результат агрегации в текущей реализации)

        Returns:
            Агрегированное значение в соответствии с указанным методом
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseReportParser._aggregate_values")
        if self.logger:
            self.logger.debug(f"Агрегация значений: количество={len(values) if values else 0}, метод={aggregation_method}, поле={target_field}")
            
        if not values:
            if self.logger:
                self.logger.debug("Список значений пуст, возвращаем 0")
            return 0

        if self.logger:
            self.logger.debug(f"Входные значения: {values}")

        if aggregation_method == "sum":
            result = sum(v for v in values if isinstance(v, (int, float)))
            if self.logger:
                self.logger.debug(f"Результат суммирования: {result}")
            return result
        elif aggregation_method == "average":
            numeric_values = [v for v in values if isinstance(v, (int, float))]
            result = sum(numeric_values) / len(numeric_values) if numeric_values else 0
            if self.logger:
                self.logger.debug(f"Результат усреднения: {result} (из {len(numeric_values)} числовых значений)")
            return result
        elif aggregation_method == "count":
            result = len([v for v in values if v is not None])
            if self.logger:
                self.logger.debug(f"Результат подсчета: {result}")
            return result
        elif aggregation_method == "max":
            numeric_values = [v for v in values if isinstance(v, (int, float))]
            result = max(numeric_values) if numeric_values else 0
            if self.logger:
                self.logger.debug(f"Результат поиска максимума: {result}")
            return result
        elif aggregation_method == "min":
            numeric_values = [v for v in values if isinstance(v, (int, float))]
            result = min(numeric_values) if numeric_values else 0
            if self.logger:
                self.logger.debug(f"Результат поиска минимума: {result}")
            return result
        else:
            # По умолчанию суммируем
            result = sum(v for v in values if isinstance(v, (int, float)))
            if self.logger:
                self.logger.debug(f"Результат суммирования по умолчанию: {result}")
            return result

    def navigate_to_target(self) -> bool:
        """
        Переопределенный метод навигации к целевой странице из BaseParser.
        Выполняет вычисление target_url и навигацию к target_url.

        Returns:
            bool: True, если навигация прошла успешно
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseReportParser.navigate_to_target")

        # Получаем базовый URL из конфигурации
        base_url = self.config.get("base_url", "")
        if self.logger:
            self.logger.debug(f"Базовый URL: {base_url}")

        # Получаем дату выполнения
        execution_date = self.config.get('execution_date', None)
        if self.logger:
            self.logger.debug(f"Дата выполнения: {execution_date}")

        # Если есть дата и базовый URL, применяем фильтр к base_url
        if execution_date and base_url:
            if self.logger:
                self.logger.debug("Есть дата и базовый URL, формируем фильтр")
            # Получаем общий фильтр в URL (из вспомогательного метода _build_url_filter)
            url_filter = self._build_url_filter()
            if self.logger:
                self.logger.debug(f"Сформированный фильтр: {url_filter}")
            if url_filter:
                # Формируем "target_url" с фильтром, применяя объединение
                target_url = base_url + url_filter
            else:
                # Если фильтр не удалось сформировать, используем базовый URL
                target_url = base_url
        else:
            if self.logger:
                self.logger.debug("Нет даты или базового URL, используем базовый URL как есть")
            # Если даты нет или базового URL нет, используем базовый URL как есть
            target_url = base_url

        if self.logger:
            self.logger.debug(f"Итоговый target_url: {target_url}")

        # Если target_url пустой, возвращаем ошибку
        if not target_url:
            if self.logger:
                self.logger.debug("Target URL пустой, невозможно выполнить навигацию")
            if self.logger:
                self.logger.error("Не удалось сформировать целевой URL для навигации")
            return False

        try:
            if self.logger:
                self.logger.debug(f"Переходим на URL: {target_url}")
            # Переходим на целевую страницу, используя готовый URL
            self.driver.get(target_url)

            # Сохраняем правильный URL в конфиг для дальнейшего использования
            self.config['target_url'] = target_url
            if self.logger:
                self.logger.debug("Навигация выполнена успешно, URL сохранен в конфиг")

            return True
        except Exception as e:
            if self.logger:
                self.logger.debug(f"Ошибка при навигации: {e}")
            if self.logger:
                self.logger.error(f"Ошибка при навигации: {e}")
                import traceback
                self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
                
                # Дополнительно проверим, доступен ли драйвер и сессия
                if hasattr(self, 'driver') and self.driver:
                    try:
                        if self.driver.session_id:
                            self.logger.debug(f"Сессия драйвера активна: {self.driver.session_id[:10]}...")
                        else:
                            self.logger.error("Сессия драйвера неактивна")
                            
                        self.logger.debug(f"Текущий URL: {self.driver.current_url}")
                        self.logger.debug(f"Заголовок страницы: {self.driver.title}")
                    except Exception as driver_check_error:
                        self.logger.error(f"Ошибка при проверке состояния драйвера: {driver_check_error}")
                else:
                    self.logger.error("Драйвер не инициализирован или недоступен")
                    
            return False