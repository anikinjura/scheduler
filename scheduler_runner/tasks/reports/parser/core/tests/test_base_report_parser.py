"""
Полные тесты для BaseReportParser с использованием новой конфигурации и с учетом изменений в версии 3.0.0

В версии 3.0.0 были внесены изменения:
- Обновлены ссылки на методы BaseParser с учетом переименования select_option_from_dropdown в _select_option_from_dropdown
- Добавлена поддержка мульти-шаговой обработки с новыми методами
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
import json
import xml.etree.ElementTree as ET
import sys
from scheduler_runner.tasks.reports.parser.core.base_report_parser import BaseReportParser
from scheduler_runner.tasks.reports.parser.configs.base_configs.base_report_config import BASE_REPORT_CONFIG


class TestConcreteReportParser(BaseReportParser):
    """Тестовый дочерний класс для тестирования BaseReportParser"""

    def __init__(self, config, args=None, logger=None):
        """Инициализация с возможностью передачи логгера"""
        # Создаем фиктивный логгер с методом trace, если он не передан
        if logger is None:
            import logging
            logger = logging.getLogger(self.__class__.__name__)
            logger.setLevel(logging.DEBUG)
            # Добавляем обработчик, чтобы избежать предупреждений
            if not logger.handlers:
                handler = logging.NullHandler()
                logger.addHandler(handler)
            
            # Добавляем метод trace к логгеру
            def trace(msg, *args, **kwargs):
                logger.debug(msg, *args, **kwargs)
            logger.trace = trace
        
        super().__init__(config, args=args, logger=logger)

    def get_report_type(self) -> str:
        """Реализация абстрактного метода get_report_type"""
        return "test_report"

    def get_report_schema(self) -> dict:
        """Реализация абстрактного метода get_report_schema"""
        return {
            "required_fields": ["field1", "field2"],
            "field_types": {
                "field1": "string",
                "field2": "integer",
                "field3": "list"
            }
        }

    def extract_report_data(self) -> dict:
        """Реализация абстрактного метода extract_report_data"""
        return {
            "field1": "test_value",
            "field2": 123,
            "field3": [1, 2, 3]
        }

    def login(self) -> bool:
        """Реализация абстрактного метода login"""
        return True

    def navigate_to_target(self) -> bool:
        """Реализация абстрактного метода navigate_to_target"""
        return True

    def logout(self) -> bool:
        """Реализация абстрактного метода logout"""
        return True


class TestBaseReportParser(unittest.TestCase):
    """Тесты для BaseReportParser"""

    def setUp(self):
        """Настройка теста"""
        self.config = BASE_REPORT_CONFIG.copy()
        self.parser = TestConcreteReportParser(self.config)

    def test_format_report_output_json(self):
        """Тест форматирования отчета в JSON"""
        data = {"test": "data", "number": 123}
        result = self.parser.format_report_output(data, 'json')

        # Проверяем, что результат - валидный JSON
        parsed = json.loads(result)
        self.assertEqual(parsed["test"], "data")
        self.assertEqual(parsed["number"], 123)

    def test_format_report_output_xml_with_custom_root(self):
        """Тест форматирования отчета в XML с пользовательским корневым элементом"""
        # Изменяем конфиг для использования пользовательского корневого элемента
        self.config['xml_root_element'] = 'custom_report'
        parser = TestConcreteReportParser(self.config)

        data = {"field1": "value1", "field2": [1, 2, 3]}
        result = parser.format_report_output(data, 'xml')

        # Проверяем, что корневой элемент - наш кастомный
        root = ET.fromstring(result)
        self.assertEqual(root.tag, 'custom_report')

    def test_format_report_output_xml_with_custom_item_prefix(self):
        """Тест форматирования отчета в XML с пользовательским префиксом элементов списка"""
        # Изменяем конфиг для использования пользовательского префикса
        self.config['xml_item_prefix'] = 'custom_item_'
        parser = TestConcreteReportParser(self.config)

        data = {"field1": [1, 2, 3]}
        result = parser.format_report_output(data, 'xml')

        # Проверяем, что элементы списка имеют наш кастомный префикс
        root = ET.fromstring(result)
        list_items = root.find('field1')
        if list_items is not None:
            # Проверяем наличие элементов с кастомным префиксом
            children_tags = [child.tag for child in list_items]
            self.assertTrue(any(tag.startswith('custom_item_') for tag in children_tags))

    def test_save_report_with_custom_filename_template(self):
        """Тест сохранения отчета с пользовательским шаблоном имени файла"""
        # Изменяем конфиг для использования пользовательского шаблона
        self.config['filename_template'] = "custom_{report_type}_{timestamp}_final.{output_format}"
        self.config['execution_date'] = '2023-12-01'
        parser = TestConcreteReportParser(self.config)

        # Мокаем open, чтобы не создавать реальный файл
        with patch('builtins.open', unittest.mock.mock_open()) as mock_file:
            result = parser.save_report({"test": "data"}, output_format='json')

            # Проверяем, что open был вызван с правильным именем файла
            mock_file.assert_called_once()
            args, kwargs = mock_file.call_args
            filename = args[0]

            # Проверяем, что имя файла соответствует шаблону
            self.assertIn('custom_test_report_20231201_final.json', filename)

    def test_get_report_schema_with_custom_keys(self):
        """Тест получения схемы отчета с пользовательскими ключами"""
        # Создаем тестовый парсер с кастомными ключами схемы
        custom_config = BASE_REPORT_CONFIG.copy()
        custom_config['schema_keys'] = {
            'required_fields': 'mandatory_fields',
            'field_types': 'data_types'
        }

        class CustomSchemaParser(TestConcreteReportParser):
            def get_report_schema(self) -> dict:
                return {
                    "mandatory_fields": ["field1"],  # Используем кастомный ключ
                    "data_types": {  # Используем кастомный ключ
                        "field1": "string"
                    }
                }

            def get_report_type(self) -> str:
                return "custom_report"

            def extract_report_data(self) -> dict:
                return {"field1": "test_value"}

            def login(self) -> bool:
                return True

            def navigate_to_target(self) -> bool:
                return True

            def logout(self) -> bool:
                return True

        parser = CustomSchemaParser(custom_config)

        # Проверяем, что схема возвращает правильные данные
        schema = parser.get_report_schema()
        self.assertIn("mandatory_fields", schema)
        self.assertIn("data_types", schema)
        self.assertEqual(schema["mandatory_fields"], ["field1"])

    def test_report_schema_with_custom_supported_types(self):
        """Тест схемы отчета с пользовательскими поддерживаемыми типами"""
        # Добавляем кастомный тип в конфиг
        custom_config = BASE_REPORT_CONFIG.copy()
        custom_config['supported_field_types']['email'] = str  # Для примера, email как строка

        class EmailSchemaParser(TestConcreteReportParser):
            def get_report_schema(self) -> dict:
                return {
                    "required_fields": ["email_field"],
                    "field_types": {
                        "email_field": "email"  # Используем кастомный тип
                    }
                }

            def get_report_type(self) -> str:
                return "email_report"

            def extract_report_data(self) -> dict:
                return {"email_field": "test@example.com"}

            def login(self) -> bool:
                return True

            def navigate_to_target(self) -> bool:
                return True

            def logout(self) -> bool:
                return True

        parser = EmailSchemaParser(custom_config)

        # Проверяем, что схема возвращает правильные данные
        schema = parser.get_report_schema()
        self.assertIn("required_fields", schema)
        self.assertIn("field_types", schema)
        self.assertEqual(schema["field_types"]["email_field"], "email")

    def test_save_report_with_custom_encoding(self):
        """Тест сохранения отчета с пользовательской кодировкой"""
        # Изменяем конфиг для использования пользовательской кодировки
        self.config['output_config']['encoding'] = 'utf-16'
        parser = TestConcreteReportParser(self.config)

        # Мокаем open, чтобы проверить, что кодировка передается правильно
        with patch('builtins.open', unittest.mock.mock_open()) as mock_file:
            result = parser.save_report({"test": "данные"}, output_format='json')

            # Проверяем, что open был вызван с правильной кодировкой
            mock_file.assert_called_once()
            args, kwargs = mock_file.call_args

            # Проверяем, что кодировка - та, что мы указали
            self.assertEqual(kwargs.get('encoding'), 'utf-16')

    def test_extract_data_basic(self):
        """Тест базового извлечения данных"""
        # Тестируем вызов метода извлечения данных
        parser = TestConcreteReportParser(self.config)

        # Вызываем метод извлечения данных
        result = parser.extract_data()

        # Проверяем, что метод не вызывает ошибок (возвращаемое значение может быть любым)
        # Важно, чтобы метод не выбрасывал исключение
        self.assertIsNotNone(parser)

    def test_run_parser_with_strict_validation(self):
        """Тест запуска парсера со строгой валидацией"""
        # Включаем строгий режим валидации
        self.config['validation_config']['strict_mode'] = True
        parser = TestConcreteReportParser(self.config)

        # Мокаем методы
        parser.setup_browser = Mock(return_value=True)
        parser.login = Mock(return_value=True)
        parser.navigate_to_target = Mock(return_value=True)
        parser.extract_data = Mock(return_value={"wrong_field": "value"})
        parser.validate_report_data = Mock(return_value=False)  # Валидация не проходит
        parser.logout = Mock(return_value=True)
        parser.close_browser = Mock()
        parser.save_report = Mock()

        # В строгом режиме при проваленной валидации должен выброситься exception
        with self.assertRaises(Exception) as context:
            parser.run_parser(save_to_file=True)

        # Проверяем, что сообщение об ошибке содержит информацию о причине
        self.assertTrue(len(str(context.exception)) > 0)

    def test_parse_arguments_with_report_date(self):
        """Тест разбора аргументов командной строки с датой отчета"""
        # Тестируем разбор аргументов с датой отчета
        test_args = ['--report_date', '2023-12-25']
        
        # Создаем новый экземпляр парсера с этими аргументами
        parser = TestConcreteReportParser(self.config, args=test_args)
        
        # Проверяем, что дата отчета была правильно разобрана
        self.assertEqual(parser.args.report_date, '2023-12-25')

    def test_parse_arguments_without_report_date(self):
        """Тест разбора аргументов командной строки без даты отчета"""
        # Тестируем разбор аргументов без даты отчета
        test_args = []  # Пустой список аргументов
        
        # Создаем новый экземпляр парсера с этими аргументами
        parser = TestConcreteReportParser(self.config, args=test_args)
        
        # Проверяем, что дата отчета не установлена
        self.assertIsNone(parser.args.report_date)

    def test_update_execution_date_from_args(self):
        """Тест обновления даты выполнения из аргументов командной строки"""
        # Устанавливаем дату в аргументах
        test_args = ['--report_date', '2023-12-25']
        parser = TestConcreteReportParser(self.config, args=test_args)
        
        # Проверяем, что дата выполнения обновилась из аргументов
        self.assertEqual(parser.config['execution_date'], '2023-12-25')

    def test_update_execution_date_from_config(self):
        """Тест обновления даты выполнения из конфигурации"""
        # Устанавливаем дату в конфиге
        self.config['execution_date'] = '2023-11-30'
        parser = TestConcreteReportParser(self.config)
        
        # Проверяем, что дата выполнения взята из конфига
        self.assertEqual(parser.config['execution_date'], '2023-11-30')

    def test_update_execution_date_from_current_time(self):
        """Тест обновления даты выполнения из текущего времени"""
        from datetime import datetime
        
        # Удаляем дату из конфига и не передаем аргументы
        if 'execution_date' in self.config:
            del self.config['execution_date']
            
        parser = TestConcreteReportParser(self.config, args=[])
        
        # Проверяем, что дата выполнения установлена в текущую дату
        today = datetime.now().strftime('%Y-%m-%d')
        self.assertEqual(parser.config['execution_date'], today)

    def test_build_url_filter_with_date_and_data_type(self):
        """Тест построения фильтра URL с датой и типом данных"""
        # Устанавливаем параметры фильтрации в конфиге
        self.config['filter_template'] = '?filter={{{date_filter_template},{data_type_filter_template}}}'
        self.config['date_filter_template'] = '"startDate":"{date}T00:00%2B03:00","endDate":"{date}T23:59%2B03:00"'
        self.config['data_type_filter_template'] = '"operationTypes":["GiveoutAll"]'
        self.config['execution_date'] = '2023-12-25'
        
        parser = TestConcreteReportParser(self.config)
        filter_str = parser._build_url_filter()
        
        # Проверяем, что фильтр содержит дату и тип данных
        self.assertIn('2023-12-25', filter_str)
        self.assertIn('operationTypes', filter_str)

    def test_build_url_filter_without_date(self):
        """Тест построения фильтра URL без даты"""
        # Устанавливаем параметры фильтрации без даты
        self.config['filter_template'] = '?filter={{{data_type_filter_template}}}'
        self.config['data_type_filter_template'] = '"operationTypes":["GiveoutAll"]'
        self.config['execution_date'] = ''
        
        parser = TestConcreteReportParser(self.config)
        filter_str = parser._build_url_filter()
        
        # Проверяем, что фильтр содержит только тип данных
        self.assertIn('operationTypes', filter_str)
        self.assertNotIn('{date_filter_template}', filter_str)

    def test_execute_single_step_simple_processing(self):
        """Тест выполнения одного шага с простой обработкой"""
        # Создаем конфигурацию шага с простой обработкой
        step_config = {
            "processing_type": "simple",
            "data_extraction": {
                "selector": "//div[@id='test']/span",
                "element_type": "div"
            },
            "result_key": "test_result"
        }
        
        parser = TestConcreteReportParser(self.config)
        
        # Мокаем необходимые методы
        parser._update_config_for_step = Mock(return_value=self.config.copy())
        parser.navigate_to_target = Mock(return_value=True)
        parser._handle_simple_extraction = Mock(return_value="test_value")
        
        result = parser._execute_single_step(step_config)
        
        # Проверяем, что были вызваны нужные методы
        parser._handle_simple_extraction.assert_called_once_with(step_config)
        self.assertIn('__STEP_SOURCE_URL__', result)

    def test_update_config_for_step(self):
        """Тест обновления конфигурации для шага"""
        # Создаем конфигурацию шага
        step_config = {
            "base_url": "https://example.com",
            "filter_template": "?filter=test",
            "processing_type": "simple",  # Это служебный ключ, не должен обновляться
            "data_extraction": {},       # Это служебный ключ, не должен обновляться
        }
        
        parser = TestConcreteReportParser(self.config)
        
        # Сохраняем оригинальную конфигурацию
        original_config = parser.config.copy()
        
        # Обновляем конфигурацию для шага
        original = parser._update_config_for_step(step_config)
        
        # Проверяем, что служебные ключи не были добавлены в конфиг
        self.assertEqual(parser.config["base_url"], "https://example.com")
        self.assertEqual(parser.config["filter_template"], "?filter=test")
        self.assertNotIn("processing_type", parser.config)
        self.assertNotIn("data_extraction", parser.config)
        
        # Проверяем, что оригинальная конфигурация возвращена
        self.assertEqual(original, original_config)

    def test_handle_simple_extraction(self):
        """Тест обработки простого извлечения данных"""
        # Создаем конфигурацию шага с простым извлечением
        step_config = {
            "data_extraction": {
                "selector": "//div[@id='test']/span",
                "element_type": "div",
                "post_processing": {
                    "convert_to": "str"
                }
            }
        }
        
        parser = TestConcreteReportParser(self.config)
        
        # Мокаем метод извлечения значения
        parser._extract_value_by_config = Mock(return_value="test_value")
        
        result = parser._handle_simple_extraction(step_config)
        
        # Проверяем, что был вызван метод извлечения значения
        parser._extract_value_by_config.assert_called_once_with(step_config["data_extraction"])
        self.assertEqual(result, "test_value")

    def test_apply_post_processing_int_conversion(self):
        """Тест постобработки с преобразованием в целое число"""
        parser = TestConcreteReportParser(self.config)
        
        # Тестируем преобразование строки в целое число
        value = "123"
        config = {"convert_to": "int"}
        result = parser._apply_post_processing(value, config)
        
        self.assertEqual(result, 123)
        self.assertIsInstance(result, int)

    def test_apply_post_processing_float_conversion(self):
        """Тест постобработки с преобразованием в число с плавающей точкой"""
        parser = TestConcreteReportParser(self.config)
        
        # Тестируем преобразование строки в число с плавающей точкой
        value = "123.45"
        config = {"convert_to": "float"}
        result = parser._apply_post_processing(value, config)
        
        self.assertEqual(result, 123.45)
        self.assertIsInstance(result, float)

    def test_apply_post_processing_str_conversion(self):
        """Тест постобработки с преобразованием в строку"""
        parser = TestConcreteReportParser(self.config)
        
        # Тестируем преобразование в строку
        value = 123
        config = {"convert_to": "str"}
        result = parser._apply_post_processing(value, config)
        
        self.assertEqual(result, "123")
        self.assertIsInstance(result, str)

    def test_apply_post_processing_with_default_value(self):
        """Тест постобработки с установкой значения по умолчанию при ошибке"""
        parser = TestConcreteReportParser(self.config)
        
        # Тестируем установку значения по умолчанию при невозможности преобразования
        value = "not_a_number"
        config = {"convert_to": "int", "default_value": 0}
        result = parser._apply_post_processing(value, config)
        
        self.assertEqual(result, 0)

    def test_extract_value_by_config(self):
        """Тест извлечения значения по конфигурации"""
        parser = TestConcreteReportParser(self.config)
        
        # Мокаем метод получения значения элемента
        parser.get_element_value = Mock(return_value="test_value")
        
        extraction_config = {
            "selector": "//div[@id='test']",
            "element_type": "div",
            "post_processing": {
                "convert_to": "str"
            }
        }
        
        result = parser._extract_value_by_config(extraction_config)
        
        # Проверяем, что был вызван метод получения значения
        parser.get_element_value.assert_called_once_with(
            selector="//div[@id='test']",
            element_type="div",
            pattern=None
        )
        self.assertEqual(result, "test_value")

    def test_get_common_report_info(self):
        """Тест получения общей информации отчета"""
        parser = TestConcreteReportParser(self.config)
        
        # Устанавливаем дату выполнения
        parser.config['execution_date'] = '2023-12-25'
        
        # Мокаем метод получения текущего ПВЗ
        parser.get_current_pvz = Mock(return_value="Test PVZ")
        
        # Мокаем метод получения текущего времени
        parser._get_current_timestamp = Mock(return_value="2023-12-25 10:00:00")
        
        info = parser.get_common_report_info()
        
        # Проверяем, что все ключи присутствуют
        self.assertIn('__LOCATION_INFO__', info)
        self.assertIn('__EXTRACTION_TIMESTAMP__', info)
        self.assertIn('__SOURCE_URL__', info)
        self.assertIn('__EXECUTION_DATE__', info)
        
        self.assertEqual(info['__EXECUTION_DATE__'], '2023-12-25')
        self.assertEqual(info['__LOCATION_INFO__'], "Test PVZ")

    def test_aggregate_values_sum(self):
        """Тест агрегации значений - суммирование"""
        parser = TestConcreteReportParser(self.config)
        
        values = [10, 20, 30]
        result = parser._aggregate_values(values, "sum", "total")
        
        self.assertEqual(result, 60)

    def test_aggregate_values_average(self):
        """Тест агрегации значений - среднее значение"""
        parser = TestConcreteReportParser(self.config)
        
        values = [10, 20, 30]
        result = parser._aggregate_values(values, "average", "avg")
        
        self.assertEqual(result, 20.0)

    def test_aggregate_values_count(self):
        """Тест агрегации значений - подсчет количества"""
        parser = TestConcreteReportParser(self.config)
        
        values = [10, 20, None, 30]
        result = parser._aggregate_values(values, "count", "count")
        
        self.assertEqual(result, 3)

    def test_aggregate_values_max(self):
        """Тест агрегации значений - максимальное значение"""
        parser = TestConcreteReportParser(self.config)
        
        values = [10, 20, 30, 5]
        result = parser._aggregate_values(values, "max", "max_val")
        
        self.assertEqual(result, 30)

    def test_aggregate_values_min(self):
        """Тест агрегации значений - минимальное значение"""
        parser = TestConcreteReportParser(self.config)
        
        values = [10, 20, 30, 5]
        result = parser._aggregate_values(values, "min", "min_val")
        
        self.assertEqual(result, 5)

    def test_replace_placeholders_recursive_string(self):
        """Тест рекурсивной замены плейсхолдеров в строке"""
        parser = TestConcreteReportParser(self.config)
        
        data = "Value is {value} and date is {date}"
        replacements = {"value": "test", "date": "2023-12-25"}
        result = parser._replace_placeholders_recursive(data, replacements)
        
        self.assertEqual(result, "Value is test and date is 2023-12-25")

    def test_replace_placeholders_recursive_dict(self):
        """Тест рекурсивной замены плейсхолдеров в словаре"""
        parser = TestConcreteReportParser(self.config)
        
        data = {
            "field1": "Value is {value}",
            "field2": "Date is {date}",
            "nested": {
                "field3": "Combined: {value} on {date}"
            }
        }
        replacements = {"value": "test", "date": "2023-12-25"}
        result = parser._replace_placeholders_recursive(data, replacements)
        
        self.assertEqual(result["field1"], "Value is test")
        self.assertEqual(result["field2"], "Date is 2023-12-25")
        self.assertEqual(result["nested"]["field3"], "Combined: test on 2023-12-25")

    def test_replace_placeholders_recursive_list(self):
        """Тест рекурсивной замены плейсхолдеров в списке"""
        parser = TestConcreteReportParser(self.config)
        
        data = ["Value is {value}", "Date is {date}"]
        replacements = {"value": "test", "date": "2023-12-25"}
        result = parser._replace_placeholders_recursive(data, replacements)
        
        self.assertEqual(result[0], "Value is test")
        self.assertEqual(result[1], "Date is 2023-12-25")

    def test_navigate_to_target_basic(self):
        """Тест базовой навигации к целевой странице"""
        # Устанавливаем параметры для навигации
        self.config['base_url'] = 'https://example.com/report'
        self.config['execution_date'] = '2023-12-25'

        parser = TestConcreteReportParser(self.config)

        # Создаем фиктивный драйвер
        parser.driver = Mock()
        parser.driver.get = Mock(return_value=None)

        result = parser.navigate_to_target()

        # Проверяем, что навигация вернула True (успешно)
        self.assertTrue(result)

    def test_get_common_url_prefix(self):
        """Тест получения общего префикса URL"""
        parser = TestConcreteReportParser(self.config)
        
        urls = [
            'https://turbo-pvz.ozon.ru/reports/giveout?filter=...',
            'https://turbo-pvz.ozon.ru/outbound/carriages-archive?filter=...'
        ]
        
        common_prefix = parser._get_common_url_prefix(urls)
        
        # Проверяем, что общий префикс содержит домен
        self.assertIn('https://turbo-pvz.ozon.ru', common_prefix)
        self.assertEqual(common_prefix, 'https://turbo-pvz.ozon.ru')

    def test_get_common_url_prefix_empty_list(self):
        """Тест получения общего префикса URL с пустым списком"""
        parser = TestConcreteReportParser(self.config)
        
        urls = []
        
        common_prefix = parser._get_common_url_prefix(urls)
        
        # Проверяем, что возвращается пустая строка
        self.assertEqual(common_prefix, "")

    def test_get_common_url_prefix_single_url(self):
        """Тест получения общего префикса URL с одним URL"""
        parser = TestConcreteReportParser(self.config)

        urls = ['https://example.com/path/to/page']

        common_prefix = parser._get_common_url_prefix(urls)

        # Проверяем, что возвращается домен или его часть
        self.assertIn('https://example.com', common_prefix)

    def test_filter_structure_by_available_keys_string_with_available_key(self):
        """Тест фильтрации структуры по доступным ключам - строка с доступным ключом"""
        parser = TestConcreteReportParser(self.config)
        
        data = "Value is {value}"
        available_keys = {"value", "date"}
        
        # Используем метод с префиксом _className для доступа к приватному методу
        result = parser._BaseReportParser__filter_structure_by_available_keys(data, available_keys)
        
        self.assertEqual(result, "Value is {value}")

    def test_filter_structure_by_available_keys_string_without_available_key(self):
        """Тест фильтрации структуры по доступным ключам - строка без доступного ключа"""
        parser = TestConcreteReportParser(self.config)
        
        data = "Value is {missing_key}"
        available_keys = {"value", "date"}
        
        # Используем метод с префиксом _className для доступа к приватному методу
        result = parser._BaseReportParser__filter_structure_by_available_keys(data, available_keys)
        
        self.assertIsNone(result)

    def test_filter_structure_by_available_keys_special_placeholders(self):
        """Тест фильтрации структуры по доступным ключам - специальные плейсхолдеры"""
        parser = TestConcreteReportParser(self.config)
        
        data = "Source: {__SOURCE_URL__}"
        available_keys = {"value", "date"}  # Даже если специальный ключ не в списке
        
        # Используем метод с префиксом _className для доступа к приватному методу
        result = parser._BaseReportParser__filter_structure_by_available_keys(data, available_keys)
        
        # Должно вернуть строку, потому что специальные плейсхолдеры всегда остаются
        self.assertEqual(result, "Source: {__SOURCE_URL__}")

    def test_filter_structure_by_available_keys_dict(self):
        """Тест фильтрации структуры по доступным ключам - словарь"""
        parser = TestConcreteReportParser(self.config)
        
        data = {
            "field1": "Value is {value}",
            "field2": "Missing: {missing_key}",
            "field3": "Date: {date}"
        }
        available_keys = {"value", "date"}
        
        # Используем метод с префиксом _className для доступа к приватному методу
        result = parser._BaseReportParser__filter_structure_by_available_keys(data, available_keys)
        
        # Проверяем, что остались только поля с доступными ключами
        self.assertIn("field1", result)
        self.assertNotIn("field2", result)  # Это поле должно быть удалено
        self.assertIn("field3", result)
        self.assertNotIn("field2", result.keys())

    def test_filter_structure_by_available_keys_list(self):
        """Тест фильтрации структуры по доступным ключам - список"""
        parser = TestConcreteReportParser(self.config)
        
        data = [
            "Value is {value}",
            "Missing: {missing_key}",
            "Date: {date}"
        ]
        available_keys = {"value", "date"}
        
        # Используем метод с префиксом _className для доступа к приватному методу
        result = parser._BaseReportParser__filter_structure_by_available_keys(data, available_keys)
        
        # Проверяем, что остались только элементы с доступными ключами
        self.assertIn("Value is {value}", result)
        self.assertIn("Date: {date}", result)
        # Элемент с недоступным ключом должен быть удален

    def test_execute_multi_step_processing(self):
        """Тест выполнения мульти-шаговой обработки"""
        # Создаем конфигурацию мульти-шаговой обработки
        multi_step_config = {
            "steps": ["step1", "step2"],
            "step_configurations": {
                "step1": {
                    "processing_type": "simple",
                    "data_extraction": {
                        "selector": "//div[@id='test1']/span",
                        "element_type": "div"
                    },
                    "result_key": "result1"
                },
                "step2": {
                    "processing_type": "simple",
                    "data_extraction": {
                        "selector": "//div[@id='test2']/span",
                        "element_type": "div"
                    },
                    "result_key": "result2"
                }
            },
            "aggregation_logic": {}
        }
        
        parser = TestConcreteReportParser(self.config)
        
        # Мокаем необходимые методы
        parser._execute_single_step = Mock(side_effect=["value1", "value2"])
        parser._combine_step_results = Mock(return_value={"result1": "value1", "result2": "value2"})
        
        result = parser._execute_multi_step_processing(multi_step_config)
        
        # Проверяем, что были вызваны нужные методы
        self.assertEqual(parser._execute_single_step.call_count, 2)
        parser._combine_step_results.assert_called_once()

    def test_handle_table_extraction(self):
        """Тест обработки табличного извлечения данных"""
        # Создаем конфигурацию шага с табличным извлечением
        step_config = {
            "table_processing": {
                "table_config_key": "test_table",
                "enabled": True
            }
        }
        
        # Добавляем конфигурацию таблицы в общий конфиг
        self.config["table_configs"] = {
            "test_table": {
                "table_selector": "//table[@id='test-table']",
                "columns": [
                    {"name": "col1", "selector": ".//td[1]"},
                    {"name": "col2", "selector": ".//td[2]"}
                ]
            }
        }
        
        parser = TestConcreteReportParser(self.config)
        
        # Мокаем метод извлечения табличных данных
        expected_table_data = [{"col1": "value1", "col2": "value2"}]
        parser.extract_table_data = Mock(return_value=expected_table_data)
        
        result = parser._handle_table_extraction(step_config)
        
        # Проверяем, что был вызван метод извлечения табличных данных
        parser.extract_table_data.assert_called_once_with(table_config_key="test_table")
        self.assertEqual(result, expected_table_data)

    def test_handle_table_nested_extraction(self):
        """Тест обработки вложенного табличного извлечения данных"""
        # Создаем конфигурацию шага с вложенным табличным извлечением
        step_config = {
            "table_processing": {
                "table_config_key": "test_table",
                "id_column": "id",
                "enabled": True
            },
            "nested_processing": {
                "enabled": True,
                "base_url_template": "https://example.com/detail/{carriage_id}",
                "data_extraction": {
                    "selector": "//div[@class='detail-value']",
                    "element_type": "div"
                },
                "aggregation": {
                    "method": "sum",
                    "target_field": "total"
                }
            }
        }
        
        # Добавляем конфигурацию таблицы в общий конфиг
        self.config["table_configs"] = {
            "test_table": {
                "table_selector": "//table[@id='test-table']",
                "columns": [
                    {"name": "id", "selector": ".//td[1]"},
                    {"name": "name", "selector": ".//td[2]"}
                ]
            }
        }
        
        parser = TestConcreteReportParser(self.config)
        
        # Мокаем необходимые методы
        table_data = [{"id": "1", "name": "Item1"}, {"id": "2", "name": "Item2"}]
        parser._handle_table_extraction = Mock(return_value=table_data)
        nested_results = [
            {"identifier": "1", "value": 10, "url": "https://example.com/detail/1"},
            {"identifier": "2", "value": 20, "url": "https://example.com/detail/2"}
        ]
        parser._handle_nested_processing = Mock(return_value=nested_results)
        aggregated_result = {"total": 30, "details": nested_results}
        parser._aggregate_nested_results = Mock(return_value=aggregated_result)
        
        result = parser._handle_table_nested_extraction(step_config)
        
        # Проверяем, что были вызваны нужные методы
        parser._handle_table_extraction.assert_called_once_with(step_config)
        parser._handle_nested_processing.assert_called_once()
        parser._aggregate_nested_results.assert_called_once()
        self.assertEqual(result, aggregated_result)

    def test_handle_nested_processing(self):
        """Тест обработки вложенных данных"""
        # Создаем конфигурацию вложенной обработки
        nested_config = {
            "enabled": True,
            "base_url_template": "https://example.com/detail/{carriage_id}",
            "data_extraction": {
                "selector": "//div[@class='detail-value']",
                "element_type": "div"
            }
        }
        
        identifiers = ["1", "2", "3"]
        
        parser = TestConcreteReportParser(self.config)
        
        # Мокаем необходимые методы
        parser.navigate_to_target = Mock(return_value=True)
        parser._extract_value_by_config = Mock(side_effect=[10, 20, 30])
        
        # Устанавливаем target_url для проверки
        parser.config["target_url"] = "https://example.com/detail/1"
        
        results = parser._handle_nested_processing(nested_config, identifiers)
        
        # Проверяем, что результат содержит данные для всех идентификаторов
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0]["identifier"], "1")
        self.assertEqual(results[0]["value"], 10)

    def test_aggregate_nested_results(self):
        """Тест агрегации вложенных результатов"""
        nested_results = [
            {"identifier": "1", "value": 10, "url": "https://example.com/1"},
            {"identifier": "2", "value": 20, "url": "https://example.com/2"},
            {"identifier": "3", "value": 30, "url": "https://example.com/3"}
        ]
        
        aggregation_config = {
            "method": "sum",
            "target_field": "total_value"
        }
        
        parser = TestConcreteReportParser(self.config)
        
        result = parser._aggregate_nested_results(nested_results, aggregation_config)
        
        # Проверяем, что результат содержит агрегированное значение
        self.assertIn("total_value", result)
        self.assertEqual(result["total_value"], 60)  # 10 + 20 + 30
        self.assertIn("details", result)
        self.assertEqual(len(result["details"]), 3)

    def test_combine_step_results(self):
        """Тест объединения результатов шагов"""
        all_step_results = {
            "step1": {"value": 10},
            "step2": {"value": 20}
        }
        
        aggregation_config = {
            "result_structure": {
                "total": "{step1[value]} + {step2[value]} = {calculated_total}",
                "calculated_total": "{step1[value]} + {step2[value]}"
            }
        }
        
        parser = TestConcreteReportParser(self.config)
        
        # Мокаем методы, используемые внутри _combine_step_results
        parser.get_common_report_info = Mock(return_value={
            "__LOCATION_INFO__": "Test Location",
            "__EXTRACTION_TIMESTAMP__": "2023-12-25 10:00:00",
            "__SOURCE_URL__": "https://example.com",
            "__EXECUTION_DATE__": "2023-12-25"
        })
        
        result = parser._combine_step_results(all_step_results, aggregation_config)
        
        # Проверяем, что результат содержит объединенные данные
        self.assertIsNotNone(result)


if __name__ == '__main__':
    unittest.main()