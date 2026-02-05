"""
Модуль тестов для OzonReportParser

Этот модуль содержит юнит-тесты для класса OzonReportParser,
проверяющие основную функциональность:
- Формирование URL фильтра
- Навигацию к целевой странице
- Работу с ПВЗ
- Извлечение данных отчета
"""
import unittest
from unittest.mock import Mock, MagicMock, patch, ANY
from scheduler_runner.tasks.reports.parser.core.ozon_report_parser import OzonReportParser
from scheduler_runner.tasks.reports.parser.configs.base_configs.ozon_report_config import OZON_BASE_CONFIG


class TestOzonReportParserImpl(OzonReportParser):
    """Тестовая реализация абстрактного класса OzonReportParser"""

    def __init__(self, config, args=None, logger=None):
        # Устанавливаем logger до вызова super().__init__
        if logger is not None:
            self.logger = logger
        else:
            self.logger = None
        super().__init__(config, args, logger)

    def get_report_type(self):
        return "test_report"

    def get_report_schema(self):
        return {}

    def extract_report_data(self):
        # Вызываем родительскую реализацию
        return super().extract_report_data()

    def login(self):
        return True

    def logout(self):
        return True


class TestOzonReportParser(unittest.TestCase):
    """Тесты для класса OzonReportParser"""

    def setUp(self):
        """Подготовка тестовой среды"""
        self.config = OZON_BASE_CONFIG.copy()
        self.config['execution_date'] = '2026-01-15'  # Устанавливаем фиксированную дату для тестов
        self.parser = TestOzonReportParserImpl(self.config)

    # Тесты для метода _build_url_filter (унаследованного от BaseReportParser, но используемого в OzonReportParser)
    def test_build_url_filter_with_date(self):
        """Тест формирования URL фильтра с датой"""
        # Устанавливаем дату выполнения
        self.parser.config['execution_date'] = '2026-01-15'
        # Устанавливаем необходимые шаблоны для формирования фильтра
        self.parser.config['filter_template'] = '?filter={"dateFilter": "{date_filter_template}", "dataType": "{data_type_filter_template}"}'
        self.parser.config['date_filter_template'] = '{"startDate": "{date}", "endDate": "{date}"}'
        self.parser.config['data_type_filter_template'] = '"SALES"'

        # Вызываем метод формирования фильтра
        result = self.parser._build_url_filter()

        # Проверяем, что фильтр содержит дату
        self.assertIn('2026-01-15', result)
        self.assertIn('startDate', result)
        self.assertIn('endDate', result)
        self.assertIn('SALES', result)
        self.assertTrue(result.startswith('?filter={'))

    def test_build_url_filter_without_date(self):
        """Тест формирования URL фильтра без даты"""
        # Удаляем дату выполнения
        if 'execution_date' in self.parser.config:
            del self.parser.config['execution_date']

        # Устанавливаем шаблоны для формирования фильтра
        self.parser.config['filter_template'] = '?filter={"dataType": "{data_type_filter_template}"}'
        self.parser.config['data_type_filter_template'] = '"SALES"'

        # Вызываем метод формирования фильтра
        result = self.parser._build_url_filter()

        # Проверяем, что фильтр все равно формируется (хотя и без даты)
        # При отсутствии даты должен формироваться фильтр только с типом данных
        self.assertIn('SALES', result)
        # Также проверим, что в фильтре нет плейсхолдеров
        self.assertNotIn('{date_filter_template}', result)
        self.assertNotIn('{data_type_filter_template}', result)

    def test_build_url_filter_with_different_date_formats(self):
        """Тест формирования URL фильтра с разными форматами даты"""
        test_dates = [
            '2026-01-01',
            '2026-12-31',
            '2025-02-28'
        ]

        for test_date in test_dates:
            with self.subTest(test_date=test_date):
                self.parser.config['execution_date'] = test_date
                self.parser.config['filter_template'] = '?filter={"dateFilter": "{date_filter_template}", "dataType": "{data_type_filter_template}"}'
                self.parser.config['date_filter_template'] = '{"startDate": "{date}", "endDate": "{date}"}'
                self.parser.config['data_type_filter_template'] = '"SALES"'
                result = self.parser._build_url_filter()

                self.assertIn(test_date, result)

    # Тесты для метода get_current_pvz
    def test_get_current_pvz_success(self):
        """Тест успешного получения текущего ПВЗ"""
        # Мокаем метод get_element_value
        self.parser.get_element_value = Mock(return_value='TEST_PVZ_123')

        # Мокаем селекторы
        self.parser.config['selectors'] = {
            'pvz_selectors': {
                'input_class_readonly': '//test-input-class',
                'input_readonly': '//test-input-readonly',
                'input': '//test-input'
            }
        }

        # Вызываем метод
        result = self.parser.get_current_pvz()

        # Проверяем, что get_element_value был вызван с правильным селектором
        self.parser.get_element_value.assert_called_once_with(
            selector='//test-input-class',
            element_type="input"
        )
        self.assertEqual(result, 'TEST_PVZ_123')

    def test_get_current_pvz_fallback_selectors(self):
        """Тест получения ПВЗ с использованием fallback селекторов"""
        # Мокаем селекторы
        self.parser.config['selectors'] = {
            'pvz_selectors': {
                'input_class_readonly': '//first-selector',  # Этот селектор будет использован первым
                'input_readonly': '//second-selector',
                'input': '//third-selector'
            }
        }

        # Мокаем get_element_value, чтобы он возвращал значение для первого селектора
        self.parser.get_element_value = Mock(return_value='TEST_PVZ_FALLBACK')

        # Вызываем метод
        result = self.parser.get_current_pvz()

        # Проверяем, что get_element_value был вызван один раз с первым селектором
        self.parser.get_element_value.assert_called_once_with(
            selector='//first-selector',
            element_type="input"
        )
        self.assertEqual(result, 'TEST_PVZ_FALLBACK')

    def test_get_current_pvz_no_selectors(self):
        """Тест получения ПВЗ при отсутствии селекторов"""
        # Мокаем селекторы как пустой словарь
        self.parser.config['selectors'] = {
            'pvz_selectors': {}
        }

        # Вызываем метод
        result = self.parser.get_current_pvz()

        # Проверяем, что результат 'Unknown'
        self.assertEqual(result, 'Unknown')

    def test_get_current_pvz_exception_handling(self):
        """Тест обработки исключений в get_current_pvz"""
        # Мокаем метод get_element_value, чтобы он выбрасывал исключение
        self.parser.get_element_value = Mock(side_effect=Exception("Test exception"))

        # Мокаем селекторы
        self.parser.config['selectors'] = {
            'pvz_selectors': {
                'input_class_readonly': '//test-input-class'
            }
        }

        # Вызываем метод
        result = self.parser.get_current_pvz()

        # Проверяем, что результат 'Unknown'
        self.assertEqual(result, 'Unknown')

    # Тесты для метода set_pvz
    def test_set_pvz_success(self):
        """Тест успешной установки ПВЗ"""
        # Мокаем метод set_element_value
        self.parser.set_element_value = Mock(return_value=True)

        # Мокаем селекторы
        self.parser.config['selectors'] = {
            'pvz_selectors': {
                'dropdown': '//test-dropdown',
                'option': '//test-option'
            }
        }

        # Вызываем метод
        result = self.parser.set_pvz('TEST_PVZ_NEW')

        # Проверяем, что set_element_value был вызван с правильными параметрами
        self.parser.set_element_value.assert_called_once_with(
            selector='//test-dropdown',
            value='TEST_PVZ_NEW',
            element_type="dropdown",
            option_selector='//test-option'
        )
        self.assertTrue(result)

    def test_set_pvz_missing_selectors(self):
        """Тест установки ПВЗ при отсутствии селекторов"""
        # Мокаем селекторы как пустой словарь
        self.parser.config['selectors'] = {
            'pvz_selectors': {}
        }

        # Вызываем метод
        result = self.parser.set_pvz('TEST_PVZ_NEW')

        # Проверяем, что результат False
        self.assertFalse(result)

    def test_set_pvz_exception_handling(self):
        """Тест обработки исключений в set_pvz"""
        # Мокаем метод set_element_value, чтобы он выбрасывал исключение
        self.parser.set_element_value = Mock(side_effect=Exception("Test exception"))

        # Мокаем селекторы
        self.parser.config['selectors'] = {
            'pvz_selectors': {
                'dropdown': '//test-dropdown',
                'option': '//test-option'
            }
        }

        # Вызываем метод
        result = self.parser.set_pvz('TEST_PVZ_NEW')

        # Проверяем, что результат False
        self.assertFalse(result)

    # Тесты для метода ensure_correct_pvz
    def test_ensure_correct_pvz_with_matching_pvz(self):
        """Тест ensure_correct_pvz когда требуемый ПВЗ уже установлен"""
        # Мокаем методы
        self.parser.get_current_pvz = Mock(return_value='TEST_PVZ')
        self.parser.config['additional_params'] = {'location_id': 'TEST_PVZ'}

        # Вызываем метод
        result = self.parser.ensure_correct_pvz()

        # Проверяем, что результат True
        self.assertTrue(result)

    def test_ensure_correct_pvz_with_different_pvz_success(self):
        """Тест ensure_correct_pvz когда требуемый ПВЗ отличается и установка проходит успешно"""
        # Мокаем методы
        call_sequence = ['CURRENT_PVZ', 'REQUIRED_PVZ', 'REQUIRED_PVZ']  # Текущий -> После установки -> После навигации
        call_index = 0

        def mock_get_current_pvz():
            nonlocal call_index
            value = call_sequence[call_index]
            call_index += 1
            return value

        self.parser.get_current_pvz = Mock(side_effect=mock_get_current_pvz)
        self.parser.set_pvz = Mock(return_value=True)
        self.parser.config['additional_params'] = {'location_id': 'REQUIRED_PVZ'}

        # Мокаем метод navigate_to_target, чтобы он возвращал True
        from scheduler_runner.tasks.reports.parser.core.base_report_parser import BaseReportParser
        BaseReportParser.navigate_to_target = Mock(return_value=True)

        # Вызываем метод
        result = self.parser.ensure_correct_pvz()

        # Проверяем, что результат True и set_pvz вызывается
        self.assertTrue(result)
        self.parser.set_pvz.assert_called_once_with('REQUIRED_PVZ')

        # Восстанавливаем оригинальный метод
        BaseReportParser.navigate_to_target = BaseReportParser.navigate_to_target

    def test_ensure_correct_pvz_with_different_pvz_failure(self):
        """Тест ensure_correct_pvz когда требуемый ПВЗ отличается и установка не проходит успешно"""
        # Мокаем методы
        self.parser.get_current_pvz = Mock(return_value='CURRENT_PVZ')
        self.parser.set_pvz = Mock(return_value=False)
        self.parser.config['additional_params'] = {'location_id': 'REQUIRED_PVZ'}

        # Вызываем метод
        result = self.parser.ensure_correct_pvz()

        # Проверяем, что результат False
        self.assertFalse(result)
        self.parser.set_pvz.assert_called_once_with('REQUIRED_PVZ')

    def test_ensure_correct_pvz_no_required_pvz_in_config(self):
        """Тест ensure_correct_pvz при отсутствии требуемого ПВЗ в конфигурации"""
        # Мокаем методы
        self.parser.config['additional_params'] = {'location_id': ''}

        # Вызываем метод
        result = self.parser.ensure_correct_pvz()

        # Проверяем, что результат False
        self.assertFalse(result)

    def test_ensure_correct_pvz_exception_handling(self):
        """Тест обработки исключений в ensure_correct_pvz"""
        # Мокаем методы, чтобы они выбрасывали исключение
        self.parser.get_current_pvz = Mock(side_effect=Exception("Test exception"))
        self.parser.config['additional_params'] = {'location_id': 'TEST_PVZ'}

        # Вызываем метод
        result = self.parser.ensure_correct_pvz()

        # Проверяем, что результат False
        self.assertFalse(result)

    # Тесты для метода navigate_to_target
    @patch('selenium.webdriver.Edge')
    def test_navigate_to_target_success(self, mock_driver):
        """Тест успешной навигации к целевой странице"""
        # Мокаем драйвер
        self.parser.driver = mock_driver.return_value
        self.parser.driver.current_url = 'https://turbo-pvz.ozon.ru/reports/giveout'

        # Мокаем методы
        self.parser.ensure_correct_pvz = Mock(return_value=True)
        self.parser.config['execution_date'] = '2026-01-15'
        self.parser.config['base_url'] = 'https://turbo-pvz.ozon.ru/reports/giveout'

        # Мокаем метод driver.get, чтобы он не пытался перейти на реальный URL
        self.parser.driver.get = Mock()

        # Мокаем базовый метод navigate_to_target, чтобы он устанавливал target_url
        original_navigate_to_target = self.parser.__class__.__bases__[0].navigate_to_target
        def mock_super_navigate_to_target(self):
            # Имитируем логику базового метода: формируем target_url и сохраняем в конфиг
            base_url = self.config.get("base_url", "")
            execution_date = self.config.get('execution_date', None)

            if execution_date and base_url:
                # Получаем общий фильтр в URL (из вспомогательного метода _build_url_filter)
                url_filter = self._build_url_filter()
                if url_filter:
                    # Формируем "target_url" с фильтром, применяя объединение
                    target_url = base_url + url_filter
                else:
                    # Если фильтр не удалось сформировать, используем базовый URL
                    target_url = base_url
            else:
                # Если даты нет или базового URL нет, используем базовый URL как есть
                target_url = base_url

            # Сохраняем правильный URL в конфиг для дальнейшего использования
            self.config['target_url'] = target_url

            return True  # Имитируем успешную навигацию

        # Заменяем базовый метод
        from scheduler_runner.tasks.reports.parser.core.base_report_parser import BaseReportParser
        BaseReportParser.navigate_to_target = mock_super_navigate_to_target

        # Вызываем метод
        result = self.parser.navigate_to_target()

        # Восстанавливаем оригинальный метод
        BaseReportParser.navigate_to_target = original_navigate_to_target

        # Проверяем, что результат True
        self.assertTrue(result)

        # Проверяем, что ensure_correct_pvz был вызван
        self.parser.ensure_correct_pvz.assert_called_once()

        # Проверяем, что target_url был установлен в конфиг
        self.assertIn('target_url', self.parser.config)

    @patch('selenium.webdriver.Edge')
    def test_navigate_to_target_ensure_pvz_failure(self, mock_driver):
        """Тест навигации к целевой странице при неудачной проверке ПВЗ"""
        # Мокаем драйвер
        self.parser.driver = mock_driver.return_value
        self.parser.driver.current_url = 'https://turbo-pvz.ozon.ru/reports/giveout'

        # Мокаем методы
        self.parser.ensure_correct_pvz = Mock(return_value=False)
        self.parser.config['execution_date'] = '2026-01-15'
        self.parser.config['base_url'] = 'https://turbo-pvz.ozon.ru/reports/giveout'

        # Вызываем метод
        result = self.parser.navigate_to_target()

        # Проверяем, что результат False
        self.assertFalse(result)

    # Тесты для метода extract_report_data
    def test_extract_report_data_includes_location_info(self):
        """Тест, что extract_report_data включает информацию о ПВЗ"""
        # Мокаем методы
        self.parser.get_current_pvz = Mock(return_value='TEST_PVZ_LOCATION')
        self.parser._get_current_timestamp = Mock(return_value='2026-01-15 12:00:00')

        # Мокаем базовый метод, чтобы он возвращал базовую структуру данных
        original_extract = self.parser.__class__.__bases__[0].extract_report_data
        from scheduler_runner.tasks.reports.parser.core.base_report_parser import BaseReportParser
        original_method = BaseReportParser.extract_report_data
        BaseReportParser.extract_report_data = lambda self: {"base_field": "test_value"}

        try:
            # Вызываем метод
            result = self.parser.extract_report_data()

            # Проверяем, что результат содержит location_info
            self.assertIn('location_info', result)
            self.assertEqual(result['location_info'], 'TEST_PVZ_LOCATION')
            self.assertIn('base_field', result)
            self.assertEqual(result['base_field'], 'test_value')
        finally:
            # Восстанавливаем оригинальный метод
            BaseReportParser.extract_report_data = original_method

    def test_extract_report_data_exception_handling(self):
        """Тест обработки исключений в extract_report_data"""
        # Мокаем все необходимые методы, чтобы избежать исключений в цепочке вызовов
        from scheduler_runner.tasks.reports.parser.core.base_report_parser import BaseReportParser
        original_method = BaseReportParser.extract_report_data

        # Мокаем базовый метод, чтобы он выбрасывал исключение
        def mock_base_extract_report_data(self):
            raise Exception("Test exception")

        BaseReportParser.extract_report_data = mock_base_extract_report_data

        try:
            # Вызываем метод
            result = self.parser.extract_report_data()

            # Проверяем, что результат содержит минимально необходимые данные
            self.assertIn('location_info', result)
            self.assertIn('extraction_timestamp', result)
            self.assertIn('source_url', result)
        finally:
            # Восстанавливаем оригинальный метод
            BaseReportParser.extract_report_data = original_method

    # Тесты для метода _get_current_timestamp
    def test_get_current_timestamp_format(self):
        """Тест формата времени в _get_current_timestamp"""
        import re
        from datetime import datetime
        
        # Вызываем метод
        result = self.parser._get_current_timestamp()

        # Проверяем формат времени (YYYY-MM-DD HH:MM:SS)
        pattern = r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
        self.assertRegex(result, pattern)

        # Проверяем, что время близко к текущему (в пределах 1 минуты)
        current_time = datetime.now()
        parsed_time = datetime.strptime(result, '%Y-%m-%d %H:%M:%S')
        
        time_diff = abs((current_time - parsed_time).total_seconds())
        self.assertLessEqual(time_diff, 60)  # Разница должна быть менее 1 минуты

    def test_get_current_timestamp_custom_format(self):
        """Тест _get_current_timestamp с пользовательским форматом"""
        # Устанавливаем кастомный формат даты-времени
        self.parser.config['datetime_format'] = '%Y/%m/%d %H.%M.%S'

        # Вызываем метод
        result = self.parser._get_current_timestamp()

        # Проверяем формат времени (YYYY/MM/DD HH.MM.SS)
        import re
        pattern = r'^\d{4}/\d{2}/\d{2} \d{2}\.\d{2}\.\d{2}$'
        self.assertRegex(result, pattern)


if __name__ == '__main__':
    unittest.main()