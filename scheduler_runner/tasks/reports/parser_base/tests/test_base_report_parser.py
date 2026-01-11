"""
Тесты для BaseReportParser
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
from scheduler_runner.tasks.reports.parser_base.BaseReportParser import BaseReportParser
from selenium.webdriver.common.by import By
from datetime import datetime


class TestConcreteReportParser(BaseReportParser):
    """Тестовый дочерний класс для тестирования BaseReportParser"""
    
    def get_report_type(self) -> str:
        """Реализация абстрактного метода get_report_type"""
        return 'test_report'
    
    def get_report_schema(self) -> dict:
        """Реализация абстрактного метода get_report_schema"""
        return {'test_field': 'test_value'}
    
    def extract_specific_data(self) -> dict:
        """Реализация абстрактного метода extract_specific_data"""
        return {'specific_data': 'test_value'}


class TestBaseReportParser(unittest.TestCase):
    """Тесты для BaseReportParser"""
    
    def setUp(self):
        """Настройка теста"""
        self.config = {
            'EDGE_USER_DATA_DIR': 'test_data_dir',
            'HEADLESS': True,
            'OUTPUT_DIR': 'test_output_dir',
            'PVZ_ID': 'TEST_PVZ'
        }
        self.parser = TestConcreteReportParser(self.config)
    
    def test_init(self):
        """Тест инициализации"""
        self.assertEqual(self.parser.config, self.config)
        self.assertIsNone(self.parser.driver)
        self.assertIsNone(self.parser.logger)
    
    def test_extract_common_data(self):
        """Тест извлечения общих данных"""
        # Настройка mock-объектов
        self.parser.driver = Mock()
        self.parser.driver.title = 'Test Page Title'
        self.parser.driver.current_url = 'https://test.example.com'
        self.parser.driver.page_source = '<html><body>Test content</body></html>'

        mock_element = Mock()
        mock_element.text = 'Test body content'
        self.parser.driver.find_element.return_value = mock_element

        # Вызов тестируемого метода
        result = self.parser.extract_common_data()

        # Проверки
        self.assertIn('data_source', result)  # в новой версии используется data_source вместо marketplace
        self.assertIn('report_type', result)
        self.assertIn('date', result)
        self.assertIn('timestamp', result)
        self.assertIn('page_title', result)
        self.assertIn('current_url', result)
        self.assertIn('raw_data', result)

        self.assertEqual(result['page_title'], 'Test Page Title')
        self.assertEqual(result['current_url'], 'https://test.example.com')
        self.assertEqual(result['report_type'], 'test_report')  # из get_report_type()
    
    def test_extract_data(self):
        """Тест объединения общей и специфичной логики извлечения"""
        # Настройка mock-объектов
        self.parser.extract_common_data = Mock(return_value={'common': 'data'})
        self.parser.extract_specific_data = Mock(return_value={'specific': 'data'})
        
        # Вызов тестируемого метода
        result = self.parser.extract_data()
        
        # Проверки
        self.parser.extract_common_data.assert_called_once()
        self.parser.extract_specific_data.assert_called_once()
        self.assertIn('common', result)
        self.assertIn('specific', result)
        self.assertEqual(result['common'], 'data')
        self.assertEqual(result['specific'], 'data')
    
    @patch('scheduler_runner.tasks.reports.parser_base.BaseReportParser.By')
    def test_extract_data_from_element_by_pattern(self, mock_by):
        """Тест извлечения данных из элемента по паттерну"""
        # Настройка mock-объектов
        mock_element = Mock()
        mock_element.text = 'Найдено: 12345 товаров'
        mock_element.get_attribute.return_value = None
        
        self.parser.driver = Mock()
        self.parser.driver.find_element.return_value = mock_element
        
        # Вызов тестируемого метода
        result = self.parser.extract_data_from_element_by_pattern(
            element_selector="//div[@id='test']",
            regex_pattern=r"Найдено: (\d+) товаров"
        )
        
        # Проверки
        self.parser.driver.find_element.assert_called_once()
        self.assertEqual(result, '12345')
    
    @patch('scheduler_runner.tasks.reports.parser_base.BaseReportParser.By')
    def test_extract_data_from_element_by_pattern_with_attribute(self, mock_by):
        """Тест извлечения данных из элемента по паттерну с атрибутом"""
        # Настройка mock-объектов
        mock_element = Mock()
        mock_element.get_attribute.return_value = 'Значение атрибута: 67890'
        mock_element.text = 'Текст элемента'
        
        self.parser.driver = Mock()
        self.parser.driver.find_element.return_value = mock_element
        
        # Вызов тестируемого метода
        result = self.parser.extract_data_from_element_by_pattern(
            element_selector="//div[@id='test']",
            regex_pattern=r"Значение атрибута: (\d+)",
            attribute='data-value'
        )
        
        # Проверки
        self.parser.driver.find_element.assert_called_once()
        self.assertEqual(result, '67890')
    
    def test_navigate_to_target_page(self):
        """Тест навигации к целевой странице"""
        # Настройка mock-объектов
        self.parser.driver = Mock()
        self.parser.config = {'ERP_URL': 'https://test.example.com/report'}
        
        # Вызов тестируемого метода
        self.parser.navigate_to_target_page()
        
        # Проверки
        self.parser.driver.get.assert_called_once_with('https://test.example.com/report')
    
    def test_perform_navigation(self):
        """Тест переопределенной навигации"""
        # Настройка mock-объектов
        self.parser.navigate_to_target_page = Mock()
        
        # Вызов тестируемого метода
        self.parser.perform_navigation()
        
        # Проверки
        self.parser.navigate_to_target_page.assert_called_once()
    
    def test_login(self):
        """Тест логики входа"""
        # Настройка mock-объектов
        self.parser.driver = Mock()
        self.parser.config = {'ERP_URL': 'https://test.example.com/login'}
        
        # Вызов тестируемого метода
        self.parser.login()
        
        # Проверки
        self.parser.driver.get.assert_called_once_with('https://test.example.com/login')
    
    def test_logout(self):
        """Тест логики выхода"""
        # Настройка mock-объектов
        self.parser.driver = Mock()
        
        # Вызов тестируемого метода
        self.parser.logout()
        
        # Проверки - метод logout в базовом классе пустой, поэтому не должно быть вызовов
        # Проверим, что метод не вызывает никаких методов драйвера
        self.parser.driver.assert_not_called()
    
    def test_get_output_filename(self):
        """Тест формирования имени файла для отчетов"""
        from pathlib import Path

        # Подготовка данных
        data = {'pvz_info': 'TEST_PVZ'}
        script_config = {'OUTPUT_DIR': '/test/output'}
        target_date = '2023-12-01'
        file_pattern = '{report_type}_report_{pvz_id}_{date}.json'

        # Вызов тестируемого метода
        result = self.parser.get_output_filename(data, script_config, target_date, file_pattern)

        # Проверки
        self.assertIsInstance(result, Path)
        self.assertIn('test_report', str(result).lower())
        self.assertIn('test_pvz', str(result).lower())  # транслитерация
        self.assertIn('20231201', str(result))
        self.assertIn('output', str(result))
    
    def test_ensure_data_source_success(self):
        """Тест проверки и установки источника данных (успех)"""
        # Подготовка данных
        selectors = {
            'PVZ_INPUT_READONLY': '//input[@id="pvz_input"]',
            'PVZ_INPUT_CLASS_READONLY': '//input[contains(@class, "pvz-input")]',
            'PVZ_INPUT': '//input[@name="pvz"]'
        }

        # Настройка mock-объектов
        # Первый вызов возвращает старое значение, второй - новое после установки
        self.parser.extract_current_data_source = Mock(side_effect=['CURRENT_PVZ', 'REQUIRED_PVZ'])
        self.parser.set_data_source = Mock(return_value=True)

        # Вызов тестируемого метода
        result = self.parser.ensure_data_source(
            required_source='REQUIRED_PVZ',
            source_type='pvz',
            selectors=selectors
        )

        # Проверки
        # extract_current_data_source должен быть вызван дважды - один раз для получения текущего значения,
        # второй раз для получения нового значения после установки
        self.assertEqual(self.parser.extract_current_data_source.call_count, 2)
        self.parser.set_data_source.assert_called_once_with('REQUIRED_PVZ', selectors)
        self.assertTrue(result['success'])
        self.assertEqual(result['previous_source'], 'CURRENT_PVZ')
        self.assertEqual(result['current_source'], 'REQUIRED_PVZ')
        self.assertTrue(result['changed'])
    
    def test_ensure_data_source_already_correct(self):
        """Тест проверки и установки источника данных (уже правильный)"""
        # Подготовка данных
        selectors = {
            'PVZ_INPUT_READONLY': '//input[@id="pvz_input"]',
            'PVZ_INPUT_CLASS_READONLY': '//input[contains(@class, "pvz-input")]',
            'PVZ_INPUT': '//input[@name="pvz"]'
        }
        
        # Настройка mock-объектов
        self.parser.extract_current_data_source = Mock(return_value='REQUIRED_PVZ')
        
        # Вызов тестируемого метода
        result = self.parser.ensure_data_source(
            required_source='REQUIRED_PVZ',
            source_type='pvz',
            selectors=selectors
        )
        
        # Проверки
        self.parser.extract_current_data_source.assert_called_once_with(selectors)
        # set_data_source не должен быть вызван, так как уже правильный
        self.assertTrue(result['success'])
        self.assertEqual(result['previous_source'], 'REQUIRED_PVZ')
        self.assertEqual(result['current_source'], 'REQUIRED_PVZ')
        self.assertFalse(result['changed'])
        self.assertIn('уже установлен правильно', result['message'])
    
    def test_extract_current_data_source(self):
        """Тест извлечения текущего источника данных"""
        # Подготовка данных
        selectors = {
            'PVZ_INPUT_READONLY': '//input[@id="pvz_input"]',
            'PVZ_INPUT_CLASS_READONLY': '//input[contains(@class, "pvz-input")]',
            'PVZ_INPUT': '//input[@name="pvz"]'
        }
        
        # Настройка mock-объектов
        self.parser.extract_element_by_xpath = Mock(return_value='CURRENT_PVZ')
        
        # Вызов тестируемого метода
        result = self.parser.extract_current_data_source(selectors)
        
        # Проверки
        self.parser.extract_element_by_xpath.assert_any_call('//input[@id="pvz_input"]')
        self.assertEqual(result, 'CURRENT_PVZ')
    
    def test_set_data_source_success(self):
        """Тест установки источника данных (успех)"""
        # Подготовка данных
        selectors = {
            'PVZ_DROPDOWN': '//select[@id="pvz_dropdown"]',
            'PVZ_OPTION': '//option[@value="test_pvz"]'
        }
        
        # Настройка mock-объектов
        self.parser.select_option_from_dropdown = Mock(return_value=True)
        
        # Вызов тестируемого метода
        result = self.parser.set_data_source('NEW_PVZ', selectors)
        
        # Проверки
        self.parser.select_option_from_dropdown.assert_called_once_with(
            dropdown_selector='//select[@id="pvz_dropdown"]',
            option_selector='//option[@value="test_pvz"]',
            option_value='NEW_PVZ',
            exact_match=True
        )
        self.assertTrue(result)
    
    def test_set_data_source_failure(self):
        """Тест установки источника данных (неудача)"""
        # Подготовка данных
        selectors = {
            'PVZ_DROPDOWN': '//select[@id="pvz_dropdown"]',
            'PVZ_OPTION': '//option[@value="test_pvz"]'
        }
        
        # Настройка mock-объектов
        self.parser.select_option_from_dropdown = Mock(return_value=False)
        
        # Вызов тестируемого метода
        result = self.parser.set_data_source('NEW_PVZ', selectors)
        
        # Проверки
        self.parser.select_option_from_dropdown.assert_called_once_with(
            dropdown_selector='//select[@id="pvz_dropdown"]',
            option_selector='//option[@value="test_pvz"]',
            option_value='NEW_PVZ',
            exact_match=True
        )
        self.assertFalse(result)
    
    def test_get_data_source_config(self):
        """Тест получения конфигурации источника данных"""
        # Подготовка данных
        self.parser.config = {
            'SELECTORS': {
                'PVZ_INPUT': '//input[@id="pvz"]',
                'PVZ_DROPDOWN': '//select[@id="pvz_dropdown"]'
            }
        }
        
        # Вызов тестируемого метода
        result = self.parser.get_data_source_config('pvz')
        
        # Проверки
        expected = {
            'PVZ_INPUT': '//input[@id="pvz"]',
            'PVZ_DROPDOWN': '//select[@id="pvz_dropdown"]'
        }
        self.assertEqual(result, expected)


if __name__ == '__main__':
    unittest.main()