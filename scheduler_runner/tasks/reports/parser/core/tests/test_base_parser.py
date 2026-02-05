"""
Тесты для BaseParser с учетом изменений в версии 3.0.0

В версии 3.0.0 были внесены изменения:
- Метод select_option_from_dropdown переименован в _select_option_from_dropdown
- Метод set_element_value теперь использует _select_option_from_dropdown для работы с выпадающими списками
- Обновлены тесты для использования нового метода _select_option_from_dropdown
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
import subprocess
import os
from scheduler_runner.tasks.reports.parser.core.base_parser import BaseParser
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains


class TestConcreteParser(BaseParser):
    """Тестовый дочерний класс для тестирования BaseParser"""

    def login(self):
        """Реализация абстрактного метода login"""
        pass

    def navigate_to_target(self):
        """Реализация абстрактного метода navigate_to_target"""
        pass

    def extract_data(self):
        """Реализация абстрактного метода extract_data"""
        return {}

    def logout(self):
        """Реализация абстрактного метода logout"""
        pass


class TestBaseParser(unittest.TestCase):
    """Тесты для нового BaseParser"""

    def setUp(self):
        """Настройка теста"""
        self.config = {
            'EDGE_USER_DATA_DIR': 'test_data_dir',
            'HEADLESS': True,
            'OUTPUT_DIR': 'test_output_dir',
            'DEFAULT_TIMEOUT': 60,
            'ELEMENT_CLICK_TIMEOUT': 10,
            'ELEMENT_WAIT_TIMEOUT': 10,
            'BROWSER_EXECUTABLE': 'msedge.exe',
            'BROWSER_USER_DATA_PATH_TEMPLATE': 'C:/Users/{username}/AppData/Local/Microsoft/Edge/User Data',
            'PROCESS_TERMINATION_SLEEP': 2,
            'DROPDOWN_OPEN_DELAY': 2,
            'PAGE_UPDATE_DELAY': 2,
            'PAGE_LOAD_DELAY': 3,
            'browser_config': {
                'user_data_dir': '',
                'headless': False,
                'window_size': [1920, 1080],
                'timeout': 60
            }
        }
        self.parser = TestConcreteParser(self.config)

    def test_init(self):
        """Тест инициализации"""
        self.assertEqual(self.parser.config, self.config)
        self.assertIsNone(self.parser.driver)
        self.assertIsNone(self.parser.logger)

    def test_get_current_user(self):
        """Тест получения текущего пользователя"""
        with patch('os.getlogin', return_value='test_user'):
            result = self.parser._get_current_user()
            self.assertEqual(result, 'test_user')

    @patch('scheduler_runner.tasks.reports.parser.core.base_parser.os.getlogin')
    def test_get_default_browser_user_data_dir(self, mock_getlogin):
        """Тест получения пути к данным браузера"""
        mock_getlogin.return_value = 'test_user'

        # Тест с указанным пользователем
        result = self.parser._get_default_browser_user_data_dir('custom_user')
        expected = "C:/Users/custom_user/AppData/Local/Microsoft/Edge/User Data"
        self.assertEqual(result, expected)

        # Тест с текущим пользователем
        result_with_current = self.parser._get_default_browser_user_data_dir()
        expected_with_current = "C:/Users/test_user/AppData/Local/Microsoft/Edge/User Data"
        self.assertEqual(result_with_current, expected_with_current)

    @patch('scheduler_runner.tasks.reports.parser.core.base_parser.webdriver.Edge')
    def test_setup_browser(self, mock_webdriver):
        """Тест настройки браузера"""
        mock_driver = Mock()
        mock_webdriver.return_value = mock_driver

        # Mock-им метод _terminate_browser_processes у экземпляра
        self.parser._terminate_browser_processes = Mock()

        result = self.parser.setup_browser()

        self.parser._terminate_browser_processes.assert_called_once()
        mock_webdriver.assert_called_once()
        self.assertTrue(result)
        self.assertEqual(self.parser.driver, mock_driver)

    def test_close_browser(self):
        """Тест закрытия браузера"""
        mock_driver = Mock()
        self.parser.driver = mock_driver

        self.parser.close_browser()

        mock_driver.quit.assert_called_once()
        self.assertIsNone(self.parser.driver)

    def test_close_browser_without_driver(self):
        """Тест закрытия браузера без драйвера"""
        # Не должно вызвать ошибку
        self.parser.close_browser()
        self.assertIsNone(self.parser.driver)

    @patch('scheduler_runner.tasks.reports.parser.core.base_parser.subprocess.run')
    @patch('scheduler_runner.tasks.reports.parser.core.base_parser.time.sleep')
    def test_terminate_browser_processes(self, mock_sleep, mock_subprocess_run):
        """Тест завершения процессов браузера"""
        mock_subprocess_run.return_value = Mock()

        self.parser._terminate_browser_processes()

        mock_subprocess_run.assert_called_once_with(
            ["taskkill", "/f", "/im", "msedge.exe"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        mock_sleep.assert_called_once_with(2)

    @patch('scheduler_runner.tasks.reports.parser.core.base_parser.time.sleep')
    @patch('scheduler_runner.tasks.reports.parser.core.base_parser.subprocess.run')
    def test_terminate_browser_processes_with_custom_executable(self, mock_subprocess_run, mock_sleep):
        """Тест завершения процессов браузера с пользовательским исполняемым файлом"""
        mock_subprocess_run.return_value = Mock()

        # Изменим конфиг для тестирования другого исполняемого файла
        self.parser.config['BROWSER_EXECUTABLE'] = 'chrome.exe'

        self.parser._terminate_browser_processes()

        mock_subprocess_run.assert_called_once_with(
            ["taskkill", "/f", "/im", "chrome.exe"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        mock_sleep.assert_called_once_with(2)

    @patch('scheduler_runner.tasks.reports.parser.core.base_parser.time.sleep')
    @patch('scheduler_runner.tasks.reports.parser.core.base_parser.subprocess.run')
    def test_terminate_browser_processes_exception(self, mock_subprocess_run, mock_sleep):
        """Тест завершения процессов браузера с исключением"""
        mock_subprocess_run.side_effect = Exception("Test exception")

        # Не должно вызвать ошибку
        self.parser._terminate_browser_processes()

        mock_subprocess_run.assert_called_once()
        # sleep не должен быть вызван, если subprocess.run бросает исключение
        mock_sleep.assert_not_called()

    @patch('scheduler_runner.tasks.reports.parser.core.base_parser.WebDriverWait')
    @patch('scheduler_runner.tasks.reports.parser.core.base_parser.By')
    def test_click_element_with_wait(self, mock_by, mock_wait):
        """Тест клика по элементу с ожиданием"""
        mock_element = Mock()
        mock_wait_instance = Mock()
        mock_wait_instance.until.return_value = mock_element

        self.parser.driver = Mock()
        mock_wait.return_value = mock_wait_instance

        result = self.parser._click_element("//div[@id='test']")

        mock_wait.assert_called_once()
        mock_wait_instance.until.assert_called_once()
        mock_element.click.assert_called_once()
        self.assertTrue(result)

    @patch('scheduler_runner.tasks.reports.parser.core.base_parser.By')
    def test_click_element_without_wait(self, mock_by):
        """Тест клика по элементу без ожидания"""
        mock_element = Mock()

        self.parser.driver = Mock()
        self.parser.driver.find_element.return_value = mock_element

        result = self.parser._click_element("//div[@id='test']", wait_for_clickable=False)

        self.parser.driver.find_element.assert_called_once()
        mock_element.click.assert_called_once()
        self.assertTrue(result)

    def test_get_element_value_input(self):
        """Тест получения значения элемента типа input"""
        mock_element = Mock()
        mock_element.get_attribute.return_value = "Тестовое значение"
        mock_element.text = ""

        self.parser.driver = Mock()
        self.parser.driver.find_element.return_value = mock_element

        result = self.parser.get_element_value("//input[@id='test']", element_type='input')

        self.parser.driver.find_element.assert_called_once()
        mock_element.get_attribute.assert_called_once_with('value')
        self.assertEqual(result, "Тестовое значение")

    def test_get_element_value_div(self):
        """Тест получения значения элемента типа div"""
        mock_element = Mock()
        mock_element.text = "Текст элемента"

        self.parser.driver = Mock()
        self.parser.driver.find_element.return_value = mock_element

        result = self.parser.get_element_value("//div[@id='test']", element_type='div')

        self.parser.driver.find_element.assert_called_once()
        self.assertEqual(result, "Текст элемента")

    def test_set_element_value_input(self):
        """Тест установки значения элемента типа input"""
        mock_element = Mock()
        # Возвращаемое значение для get_attribute('value') должно совпадать с устанавливаемым значением
        mock_element.get_attribute.return_value = "Новое значение"
        mock_element.text = ""

        self.parser.driver = Mock()
        self.parser.driver.find_element.return_value = mock_element

        result = self.parser.set_element_value("//input[@id='test']", "Новое значение", element_type='input')

        self.parser.driver.find_element.assert_called_once()
        mock_element.clear.assert_called_once()
        mock_element.send_keys.assert_called_once_with("Новое значение")
        self.assertTrue(result)

    @patch('selenium.webdriver.support.ui.Select')
    def test_set_element_value_dropdown(self, mock_select_class):
        """Тест установки значения элемента типа dropdown"""
        mock_element = Mock()
        mock_select = Mock()

        mock_select_class.return_value = mock_select

        self.parser.driver = Mock()
        self.parser.driver.find_element.return_value = mock_element

        result = self.parser.set_element_value("//select[@id='test']", "option_value", element_type='dropdown')

        mock_select_class.assert_called_once_with(mock_element)
        mock_select.select_by_value.assert_called_once_with("option_value")
        self.assertTrue(result)

    @patch('scheduler_runner.tasks.reports.parser.core.base_parser.ActionChains')
    @patch('scheduler_runner.tasks.reports.parser.core.base_parser.By')
    def test_select_option_from_dropdown_exact_match(self, mock_by, mock_actions_class):
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
        mock_actions_class.return_value = mock_action_chains_instance

        # Вызов тестируемого метода
        result = self.parser._select_option_from_dropdown(
            dropdown_selector="//select[@id='pvz']",
            option_selector="//option",
            option_value="Требуемый ПВЗ",
            exact_match=True
        )

        # Проверки
        self.assertTrue(result)
        # Метод _select_option_from_dropdown вызывает find_element несколько раз,
        # поэтому проверим, что он был вызван хотя бы один раз
        self.assertGreaterEqual(self.parser.driver.find_element.call_count, 1)
        self.parser.driver.find_elements.assert_called_once()
        mock_actions_class.assert_called_once_with(self.parser.driver)
        mock_action_chains_instance.move_to_element.assert_called_once_with(mock_option)
        mock_action_chains_instance.click.assert_called_once()
        mock_action_chains_instance.perform.assert_called_once()

    @patch('scheduler_runner.tasks.reports.parser.core.base_parser.ActionChains')
    @patch('scheduler_runner.tasks.reports.parser.core.base_parser.By')
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
        result = self.parser._select_option_from_dropdown(
            dropdown_selector="//select[@id='pvz']",
            option_selector="//option",
            option_value="Требуемый ПВЗ",
            exact_match=True
        )

        # Проверки
        self.assertFalse(result)
        # Метод _select_option_from_dropdown вызывает find_element несколько раз,
        # поэтому проверим, что он был вызван хотя бы один раз
        self.assertGreaterEqual(self.parser.driver.find_element.call_count, 1)
        self.parser.driver.find_elements.assert_called_once()
        # ActionChains не должен быть вызван, так как опция не найдена
        mock_actions.assert_not_called()

    @patch('scheduler_runner.tasks.reports.parser.core.base_parser.ActionChains')
    @patch('scheduler_runner.tasks.reports.parser.core.base_parser.By')
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
        result = self.parser._select_option_from_dropdown(
            dropdown_selector="//select[@id='pvz']",
            option_selector="//option",
            option_value="Требуемый ПВЗ",
            exact_match=False
        )

        # Проверки
        self.assertTrue(result)
        # Метод _select_option_from_dropdown вызывает find_element несколько раз,
        # поэтому проверим, что он был вызван хотя бы один раз
        self.assertGreaterEqual(self.parser.driver.find_element.call_count, 1)
        self.parser.driver.find_elements.assert_called_once()
        mock_actions_class.assert_called_once_with(self.parser.driver)
        mock_action_chains_instance.move_to_element.assert_called_once_with(mock_option)
        mock_action_chains_instance.click.assert_called_once()
        mock_action_chains_instance.perform.assert_called_once()

    def test_run_parser_success(self):
        """Тест метода запуска парсера с успешным выполнением"""
        # Mock-им все абстрактные методы
        self.parser.setup_browser = Mock(return_value=True)
        self.parser.login = Mock(return_value=True)
        self.parser.navigate_to_target = Mock(return_value=True)
        self.parser.extract_data = Mock(return_value={'test': 'data'})
        self.parser.logout = Mock(return_value=True)
        self.parser.close_browser = Mock()

        result = self.parser.run_parser()

        self.parser.setup_browser.assert_called_once()
        self.parser.login.assert_called_once()
        self.parser.navigate_to_target.assert_called_once()
        self.parser.extract_data.assert_called_once()
        self.parser.logout.assert_called_once()
        self.parser.close_browser.assert_called_once()
        self.assertEqual(result, {'test': 'data'})

    def test_run_parser_setup_failure(self):
        """Тест метода запуска парсера с ошибкой на этапе настройки браузера"""
        self.parser.setup_browser = Mock(return_value=False)

        with self.assertRaises(Exception) as context:
            self.parser.run_parser()

        self.parser.setup_browser.assert_called_once()
        self.assertIn("Не удалось настроить браузер", str(context.exception))


class TestNewFunctionality(unittest.TestCase):
    """Тесты для новых функций, добавленных в версии 3.0.0"""

    def setUp(self):
        """Настройка теста"""
        from scheduler_runner.tasks.reports.parser.configs.base_configs.base_config import BASE_CONFIG
        self.config = BASE_CONFIG.copy()
        self.parser = TestConcreteParser(self.config)

    def test_set_element_value_with_dropdown(self):
        """Тестирование метода set_element_value с выпадающим списком"""
        # Создаем мок-объекты для тестирования
        mock_driver = Mock()
        mock_element = Mock()

        # Мокаем драйвер
        self.parser.driver = mock_driver

        # Настройка возвращаемого значения для find_element
        mock_driver.find_element.return_value = mock_element

        # Мокируем _select_option_from_dropdown, чтобы проверить, что он вызывается
        with patch.object(self.parser, '_select_option_from_dropdown', return_value=True) as mock_method:
            # Вызываем метод set_element_value с типом dropdown
            result = self.parser.set_element_value("//select[@id='test']", "option_value", element_type="dropdown")

            # Проверяем, что find_element был вызван
            mock_driver.find_element.assert_called_once_with(By.XPATH, "//select[@id='test']")

            # Проверяем, что _select_option_from_dropdown был вызван с правильными параметрами
            mock_method.assert_called_once_with(element=mock_element, option_value="option_value")

            self.assertTrue(result)

    def test_select_option_from_dropdown_with_element(self):
        """Тестирование метода _select_option_from_dropdown с переданным элементом"""
        # Создаем мок-объекты для тестирования
        mock_element = Mock()
        mock_select_instance = Mock()

        # Мокируем Select, чтобы проверить, что он используется правильно
        with patch('selenium.webdriver.support.ui.Select', return_value=mock_select_instance):
            # Вызываем метод _select_option_from_dropdown с переданным элементом
            result = self.parser._select_option_from_dropdown(element=mock_element, option_value="option_value")

            # Проверяем, что Select был вызван с правильным элементом
            from selenium.webdriver.support.ui import Select
            Select.assert_called_once_with(mock_element)

            # Проверяем, что select_by_value был вызван с правильным значением
            mock_select_instance.select_by_value.assert_called_once_with("option_value")

            self.assertTrue(result)

    def test_set_checkbox_state_checked(self):
        """Тестирование метода _set_checkbox_state для установки состояния 'отмечен'"""
        # Создаем мок-объект для элемента
        mock_element = Mock()
        # Настройка возвращаемого значения для is_selected
        # Сначала возвращает False (не отмечен), потом True (отмечен после клика)
        mock_element.is_selected.side_effect = [False, True]

        # Вызываем метод _set_checkbox_state для установки состояния 'отмечен'
        result = self.parser._set_checkbox_state(mock_element, True)

        # Проверяем, что элемент был кликнут (изменилось состояние)
        mock_element.click.assert_called_once()
        # Проверяем, что результат равен True (успешно установлено состояние)
        self.assertTrue(result)

    def test_set_checkbox_state_unchecked(self):
        """Тестирование метода _set_checkbox_state для установки состояния 'не отмечен'"""
        # Создаем мок-объект для элемента
        mock_element = Mock()
        # Настройка возвращаемого значения для is_selected
        # Сначала возвращает True (отмечен), потом False (не отмечен после клика)
        mock_element.is_selected.side_effect = [True, False]

        # Вызываем метод _set_checkbox_state для установки состояния 'не отмечен'
        result = self.parser._set_checkbox_state(mock_element, False)

        # Проверяем, что элемент был кликнут (изменилось состояние)
        mock_element.click.assert_called_once()
        # Проверяем, что результат равен True (успешно установлено состояние)
        self.assertTrue(result)

    def test_set_checkbox_state_no_change_needed(self):
        """Тестирование метода _set_checkbox_state когда изменение не требуется"""
        # Создаем мок-объект для элемента
        mock_element = Mock()
        mock_element.is_selected.return_value = True  # Изначально отмечен

        # Вызываем метод _set_checkbox_state для установки состояния 'отмечен' (уже так установлено)
        result = self.parser._set_checkbox_state(mock_element, True)

        # Проверяем, что элемент не был кликнут (состояние уже правильное)
        mock_element.click.assert_not_called()
        # Проверяем, что результат равен True (состояние уже было правильное)
        self.assertTrue(result)

    def test_extract_standard_table_data(self):
        """Тестирование метода _extract_standard_table_data"""
        # Создаем мок-объекты для тестирования
        mock_table = Mock()
        mock_row1 = Mock()
        mock_row2 = Mock()
        mock_cell1 = Mock()
        mock_cell2 = Mock()
        mock_cell3 = Mock()
        mock_cell4 = Mock()

        # Настройка возвращаемых значений
        mock_cell1.text = "Значение1"
        mock_cell2.text = "Значение2"
        mock_cell3.text = "Значение3"
        mock_cell4.text = "Значение4"

        mock_row1.find_element.side_effect = lambda by, value: mock_cell1 if "1" in value else mock_cell2
        mock_row2.find_element.side_effect = lambda by, value: mock_cell3 if "1" in value else mock_cell4
        
        mock_table.find_elements.return_value = [mock_row1, mock_row2]

        columns_config = [
            {"name": "col1", "selector": ".//td[1]"},
            {"name": "col2", "selector": ".//td[2]"}
        ]

        result = self.parser._extract_standard_table_data(mock_table, columns_config)

        # Проверяем, что результат содержит правильные данные
        expected_result = [
            {"col1": "Значение1", "col2": "Значение2"},
            {"col1": "Значение3", "col2": "Значение4"}
        ]
        
        self.assertEqual(result, expected_result)
        # Проверяем, что были вызваны методы для поиска строк
        mock_table.find_elements.assert_called_once()

    def test_extract_standard_table_data_with_regex(self):
        """Тестирование метода _extract_standard_table_data с использованием регулярных выражений"""
        # Создаем мок-объекты для тестирования
        mock_table = Mock()
        mock_row = Mock()
        mock_cell = Mock()

        # Настройка возвращаемого значения с текстом, содержащим цифры
        mock_cell.text = "Количество: 123 шт."

        mock_row.find_element.return_value = mock_cell
        mock_table.find_elements.return_value = [mock_row]

        columns_config = [
            {"name": "count", "selector": ".//td[1]", "regex": r"\d+"}
        ]

        result = self.parser._extract_standard_table_data(mock_table, columns_config)

        # Проверяем, что регулярное выражение правильно извлекло число
        expected_result = [
            {"count": "123"}
        ]
        
        self.assertEqual(result, expected_result)

    def test_extract_table_data_with_valid_config(self):
        """Тестирование метода extract_table_data с корректной конфигурацией"""
        # Мокаем _extract_standard_table_data, чтобы проверить, что он вызывается
        with patch.object(self.parser, '_extract_standard_table_data', return_value=[{"test": "data"}]) as mock_method:
            # Создаем мок-таблицу
            mock_table_element = Mock()
            self.parser.driver = Mock()
            self.parser.driver.find_element.return_value = mock_table_element

            table_config = {
                "table_selector": "//table[@id='test-table']",
                "table_type": "standard",
                "table_columns": [
                    {"name": "col1", "selector": ".//td[1]"}
                ]
            }

            result = self.parser.extract_table_data(table_config=table_config)

            # Проверяем, что _extract_standard_table_data был вызван
            mock_method.assert_called_once()
            self.assertEqual(result, [{"test": "data"}])

    def test_extract_table_data_with_config_key(self):
        """Тестирование метода extract_table_data с использованием ключа конфигурации"""
        # Обновляем конфигурацию для теста
        table_configs = {
            "test_table": {
                "table_selector": "//table[@id='test-table']",
                "table_type": "standard",
                "table_columns": [
                    {"name": "col1", "selector": ".//td[1]"}
                ]
            }
        }
        self.parser.config['table_configs'] = table_configs

        # Мокаем _extract_standard_table_data
        with patch.object(self.parser, '_extract_standard_table_data', return_value=[{"col1": "value1"}]) as mock_method:
            # Создаем мок-таблицу
            mock_table_element = Mock()
            self.parser.driver = Mock()
            self.parser.driver.find_element.return_value = mock_table_element

            result = self.parser.extract_table_data(table_config_key='test_table')

            # Проверяем, что _extract_standard_table_data был вызван
            mock_method.assert_called_once()
            self.assertEqual(result, [{"col1": "value1"}])

    def test_extract_table_data_with_invalid_config(self):
        """Тестирование метода extract_table_data с некорректной конфигурацией"""
        # Тест с отсутствующим ключом
        result = self.parser.extract_table_data(table_config_key='nonexistent_table')
        self.assertEqual(result, [])

        # Тест с некорректной конфигурацией (без обязательных полей)
        invalid_config = {
            "table_selector": "",  # Пустой селектор
            "table_columns": []
        }
        result = self.parser.extract_table_data(table_config=invalid_config)
        self.assertEqual(result, [])

    def test_extract_table_data_exception_handling(self):
        """Тестирование метода extract_table_data с обработкой исключений"""
        # Мокаем find_element, чтобы выбросить исключение
        self.parser.driver = Mock()
        self.parser.driver.find_element.side_effect = Exception("Element not found")

        table_config = {
            "table_selector": "//table[@id='test-table']",
            "table_type": "standard",
            "table_columns": [
                {"name": "col1", "selector": ".//td[1]"}
            ]
        }

        result = self.parser.extract_table_data(table_config=table_config)
        self.assertEqual(result, [])


if __name__ == '__main__':
    unittest.main()