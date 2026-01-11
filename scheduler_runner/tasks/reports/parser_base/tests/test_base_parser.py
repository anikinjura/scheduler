"""
Тесты для BaseParser
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
import subprocess
import os
from scheduler_runner.tasks.reports.parser_base.BaseParser import BaseParser
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains


class TestConcreteParser(BaseParser):
    """Тестовый дочерний класс для тестирования BaseParser"""

    def login(self):
        """Реализация абстрактного метода login"""
        pass

    def navigate_to_target_page(self):
        """Реализация абстрактного метода navigate_to_target_page"""
        pass

    def extract_data(self):
        """Реализация абстрактного метода extract_data"""
        return {}

    def logout(self):
        """Реализация абстрактного метода logout"""
        pass


class TestBaseParser(unittest.TestCase):
    """Тесты для BaseParser"""

    def setUp(self):
        """Настройка теста"""
        self.config = {
            'EDGE_USER_DATA_DIR': 'test_data_dir',
            'HEADLESS': True,
            'OUTPUT_DIR': 'test_output_dir'
        }
        self.parser = TestConcreteParser(self.config)
    
    def test_init(self):
        """Тест инициализации"""
        self.assertEqual(self.parser.config, self.config)
        self.assertIsNone(self.parser.driver)
        self.assertIsNone(self.parser.logger)
    
    @patch('os.getlogin')
    def test_get_current_user(self, mock_getlogin):
        """Тест получения текущего пользователя"""
        mock_getlogin.return_value = 'test_user'
        result = BaseParser.get_current_user()
        self.assertEqual(result, 'test_user')
        mock_getlogin.assert_called_once()
    
    def test_get_edge_user_data_dir(self):
        """Тест получения пути к данным Edge"""
        # Проверим, что метод возвращает правильный формат пути
        result = BaseParser.get_edge_user_data_dir('test_user')
        expected = "C:/Users/test_user/AppData/Local/Microsoft/Edge/User Data"
        self.assertEqual(result, expected)

        # Тест с текущим пользователем (mock-им для этого)
        with patch.object(BaseParser, 'get_current_user', return_value='current_user'):
            result_with_current = BaseParser.get_edge_user_data_dir()
            expected_with_current = "C:/Users/current_user/AppData/Local/Microsoft/Edge/User Data"
            self.assertEqual(result_with_current, expected_with_current)
    
    @patch('scheduler_runner.tasks.reports.parser_base.BaseParser.webdriver.Edge')
    def test_setup_driver(self, mock_webdriver):
        """Тест настройки драйвера"""
        mock_driver = Mock()
        mock_webdriver.return_value = mock_driver

        # Mock-им метод terminate_edge_processes у экземпляра
        self.parser.terminate_edge_processes = Mock()

        driver = self.parser.setup_driver()

        self.parser.terminate_edge_processes.assert_called_once()
        mock_webdriver.assert_called_once()
        self.assertEqual(driver, mock_driver)
        self.assertEqual(self.parser.driver, mock_driver)
    
    @patch('scheduler_runner.tasks.reports.parser_base.BaseParser.webdriver.Edge')
    def test_setup_driver_with_custom_user_data_dir(self, mock_webdriver):
        """Тест настройки драйвера с пользовательским каталогом данных"""
        mock_driver = Mock()
        mock_webdriver.return_value = mock_driver

        custom_config = {'EDGE_USER_DATA_DIR': '/custom/path', 'HEADLESS': True}
        parser = TestConcreteParser(custom_config)

        # Mock-им метод terminate_edge_processes у экземпляра
        parser.terminate_edge_processes = Mock()

        driver = parser.setup_driver()

        parser.terminate_edge_processes.assert_called_once()
        mock_webdriver.assert_called_once()
        self.assertEqual(driver, mock_driver)
    
    def test_close(self):
        """Тест закрытия драйвера"""
        mock_driver = Mock()
        self.parser.driver = mock_driver
        
        self.parser.close()
        
        mock_driver.quit.assert_called_once()
    
    def test_close_without_driver(self):
        """Тест закрытия без драйвера"""
        # Не должно вызвать ошибку
        self.parser.close()
        self.assertIsNone(self.parser.driver)
    
    @patch('scheduler_runner.tasks.reports.parser_base.BaseParser.subprocess.run')
    @patch('scheduler_runner.tasks.reports.parser_base.BaseParser.time.sleep')
    def test_terminate_edge_processes(self, mock_sleep, mock_subprocess_run):
        """Тест завершения процессов Edge"""
        mock_subprocess_run.return_value = Mock()
        
        self.parser.terminate_edge_processes()
        
        mock_subprocess_run.assert_called_once_with(
            ["taskkill", "/f", "/im", "msedge.exe"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        mock_sleep.assert_called_once_with(2)
    
    @patch('scheduler_runner.tasks.reports.parser_base.BaseParser.time.sleep')
    @patch('scheduler_runner.tasks.reports.parser_base.BaseParser.subprocess.run')
    def test_terminate_edge_processes_exception(self, mock_subprocess_run, mock_sleep):
        """Тест завершения процессов Edge с исключением"""
        mock_subprocess_run.side_effect = Exception("Test exception")

        # Не должно вызвать ошибку
        self.parser.terminate_edge_processes()

        mock_subprocess_run.assert_called_once()
        # sleep не должен быть вызван, если subprocess.run бросает исключение
        mock_sleep.assert_not_called()
    
    def test_extract_data_by_pattern_with_page_text(self):
        """Тест извлечения данных по паттерну с указанным текстом страницы"""
        page_text = "Найдено: 12345 товаров"
        pattern = r"Найдено: (\d+) товаров"
        
        result = self.parser.extract_data_by_pattern(pattern, page_text)
        self.assertEqual(result, "12345")
    
    def test_extract_data_by_pattern_with_no_matches(self):
        """Тест извлечения данных по паттерну без совпадений"""
        page_text = "Нет данных"
        pattern = r"Найдено: (\d+) товаров"
        
        result = self.parser.extract_data_by_pattern(pattern, page_text)
        self.assertEqual(result, "")
    
    def test_extract_data_by_pattern_with_multiple_matches(self):
        """Тест извлечения данных по паттерну с несколькими совпадениями"""
        page_text = "Найдено: 12345 товаров и 67890 штук"
        pattern = r"(\d+)"
        
        result = self.parser.extract_data_by_pattern(pattern, page_text)
        self.assertEqual(result, "12345")  # Должно вернуть первое совпадение
    
    @patch('scheduler_runner.tasks.reports.parser_base.BaseParser.By')
    def test_extract_element_by_xpath_with_text(self, mock_by):
        """Тест извлечения элемента по XPath с текстом"""
        mock_element = Mock()
        mock_element.text = "Тестовый текст"
        mock_element.get_attribute.return_value = None
        
        self.parser.driver = Mock()
        self.parser.driver.find_element.return_value = mock_element
        
        result = self.parser.extract_element_by_xpath("//div[@id='test']")
        
        self.parser.driver.find_element.assert_called_once()
        self.assertEqual(result, "Тестовый текст")
    
    @patch('scheduler_runner.tasks.reports.parser_base.BaseParser.By')
    def test_extract_element_by_xpath_with_attribute(self, mock_by):
        """Тест извлечения элемента по XPath с атрибутом"""
        mock_element = Mock()
        mock_element.get_attribute.return_value = "Тестовое значение"
        
        self.parser.driver = Mock()
        self.parser.driver.find_element.return_value = mock_element
        
        result = self.parser.extract_element_by_xpath("//div[@id='test']", attribute='value')
        
        self.parser.driver.find_element.assert_called_once()
        self.assertEqual(result, "Тестовое значение")
    
    @patch('scheduler_runner.tasks.reports.parser_base.BaseParser.By')
    def test_extract_element_by_xpath_exception(self, mock_by):
        """Тест извлечения элемента по XPath с исключением"""
        self.parser.driver = Mock()
        self.parser.driver.find_element.side_effect = Exception("Element not found")
        
        result = self.parser.extract_element_by_xpath("//div[@id='test']")
        
        self.assertEqual(result, "")
    
    def test_perform_login_calls_login(self):
        """Тест шаблонного метода perform_login"""
        # Создаем mock для абстрактного метода login
        self.parser.login = Mock()
        
        self.parser.perform_login()
        
        self.parser.login.assert_called_once()
    
    def test_perform_navigation_calls_navigate_to_target_page(self):
        """Тест шаблонного метода perform_navigation"""
        # Создаем mock для абстрактного метода navigate_to_target_page
        self.parser.navigate_to_target_page = Mock()
        
        self.parser.perform_navigation()
        
        self.parser.navigate_to_target_page.assert_called_once()
    
    def test_perform_extraction_calls_extract_data(self):
        """Тест шаблонного метода perform_extraction"""
        # Создаем mock для абстрактного метода extract_data
        mock_data = {'test': 'data'}
        self.parser.extract_data = Mock(return_value=mock_data)
        
        result = self.parser.perform_extraction()
        
        self.parser.extract_data.assert_called_once()
        self.assertEqual(result, mock_data)
    
    def test_perform_logout_calls_logout(self):
        """Тест шаблонного метода perform_logout"""
        # Создаем mock для абстрактного метода logout
        self.parser.logout = Mock()
        
        self.parser.perform_logout()
        
        self.parser.logout.assert_called_once()
    
    def test_get_output_filename_basic(self):
        """Тест базового метода формирования имени файла"""
        from pathlib import Path

        data = {}
        script_config = {'OUTPUT_DIR': '/test/output'}
        target_date = '2023-12-01'
        file_pattern = '{report_type}_report_{date}.json'

        filename = self.parser.get_output_filename(data, script_config, target_date, file_pattern)

        # Проверяем, что имя файла содержит правильную дату
        self.assertIn('20231201', str(filename))
        self.assertIn('output', str(filename))
    
    @patch('scheduler_runner.tasks.reports.parser_base.BaseParser.ActionChains')
    @patch('scheduler_runner.tasks.reports.parser_base.BaseParser.WebDriverWait')
    @patch('scheduler_runner.tasks.reports.parser_base.BaseParser.By')
    def test_select_option_from_dropdown_exact_match(self, mock_by, mock_wait, mock_actions_class):
        """Тест метода выбора опции из выпадающего списка с точным совпадением"""
        # Настройка mock-объектов
        mock_dropdown = Mock()
        mock_option = Mock()
        mock_option.text = "Требуемый ПВЗ"
        mock_option.get_attribute.return_value = None

        self.parser.driver = Mock()
        self.parser.driver.find_element.return_value = mock_dropdown
        self.parser.driver.find_elements.return_value = [mock_option]

        mock_action_chains_instance = Mock()
        # Настройка цепочки вызовов: move_to_element().click().perform()
        mock_action_chains_instance.move_to_element.return_value = mock_action_chains_instance
        mock_action_chains_instance.click.return_value = mock_action_chains_instance
        mock_actions_class.return_value = mock_action_chains_instance  # Правильное имя параметра

        # Вызов тестируемого метода
        result = self.parser.select_option_from_dropdown(
            dropdown_selector="//select[@id='pvz']",
            option_selector="//option",
            option_value="Требуемый ПВЗ",
            exact_match=True
        )

        # Проверки
        self.assertTrue(result)
        self.parser.driver.find_element.assert_called_once()
        self.parser.driver.find_elements.assert_called_once()
        mock_actions_class.assert_called_once_with(self.parser.driver)  # Проверяем, что ActionChains вызван с driver
        mock_action_chains_instance.move_to_element.assert_called_once_with(mock_option)
        mock_action_chains_instance.click.assert_called_once()
        mock_action_chains_instance.perform.assert_called_once()
    
    @patch('scheduler_runner.tasks.reports.parser_base.BaseParser.ActionChains')
    @patch('scheduler_runner.tasks.reports.parser_base.BaseParser.By')
    def test_select_option_from_dropdown_no_match(self, mock_by, mock_actions):
        """Тест метода выбора опции из выпадающего списка без совпадений"""
        # Настройка mock-объектов
        mock_dropdown = Mock()
        mock_option = Mock()
        mock_option.text = "Другой ПВЗ"
        mock_option.get_attribute.return_value = None
        
        self.parser.driver = Mock()
        self.parser.driver.find_element.return_value = mock_dropdown
        self.parser.driver.find_elements.return_value = [mock_option]
        
        # Вызов тестируемого метода
        result = self.parser.select_option_from_dropdown(
            dropdown_selector="//select[@id='pvz']",
            option_selector="//option",
            option_value="Требуемый ПВЗ",
            exact_match=True
        )
        
        # Проверки
        self.assertFalse(result)
        self.parser.driver.find_element.assert_called_once()
        self.parser.driver.find_elements.assert_called_once()
        # ActionChains не должен быть вызван, так как опция не найдена
        mock_actions.assert_not_called()
    
    @patch('scheduler_runner.tasks.reports.parser_base.BaseParser.ActionChains')
    @patch('scheduler_runner.tasks.reports.parser_base.BaseParser.By')
    def test_select_option_from_dropdown_partial_match(self, mock_by, mock_actions_class):
        """Тест метода выбора опции из выпадающего списка с частичным совпадением"""
        # Настройка mock-объектов
        mock_dropdown = Mock()
        mock_option = Mock()
        mock_option.text = "Требуемый ПВЗ - Москва"
        mock_option.get_attribute.return_value = None

        self.parser.driver = Mock()
        self.parser.driver.find_element.return_value = mock_dropdown
        self.parser.driver.find_elements.return_value = [mock_option]

        mock_action_chains_instance = Mock()
        # Настройка цепочки вызовов: move_to_element().click().perform()
        mock_action_chains_instance.move_to_element.return_value = mock_action_chains_instance
        mock_action_chains_instance.click.return_value = mock_action_chains_instance
        mock_actions_class.return_value = mock_action_chains_instance

        # Вызов тестируемого метода
        result = self.parser.select_option_from_dropdown(
            dropdown_selector="//select[@id='pvz']",
            option_selector="//option",
            option_value="Требуемый ПВЗ",
            exact_match=False
        )

        # Проверки
        self.assertTrue(result)
        self.parser.driver.find_element.assert_called_once()
        self.parser.driver.find_elements.assert_called_once()
        mock_actions_class.assert_called_once_with(self.parser.driver)  # Проверяем, что ActionChains вызван с driver
        mock_action_chains_instance.move_to_element.assert_called_once_with(mock_option)
        mock_action_chains_instance.click.assert_called_once()
        mock_action_chains_instance.perform.assert_called_once()


if __name__ == '__main__':
    unittest.main()