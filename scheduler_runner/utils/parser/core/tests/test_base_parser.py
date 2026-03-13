"""
РўРµСЃС‚С‹ РґР»СЏ BaseParser СЃ СѓС‡РµС‚РѕРј РёР·РјРµРЅРµРЅРёР№ РІ РІРµСЂСЃРёРё 3.0.0

Р’ РІРµСЂСЃРёРё 3.0.0 Р±С‹Р»Рё РІРЅРµСЃРµРЅС‹ РёР·РјРµРЅРµРЅРёСЏ:
- РњРµС‚РѕРґ select_option_from_dropdown РїРµСЂРµРёРјРµРЅРѕРІР°РЅ РІ _select_option_from_dropdown
- РњРµС‚РѕРґ set_element_value С‚РµРїРµСЂСЊ РёСЃРїРѕР»СЊР·СѓРµС‚ _select_option_from_dropdown РґР»СЏ СЂР°Р±РѕС‚С‹ СЃ РІС‹РїР°РґР°СЋС‰РёРјРё СЃРїРёСЃРєР°РјРё
- РћР±РЅРѕРІР»РµРЅС‹ С‚РµСЃС‚С‹ РґР»СЏ РёСЃРїРѕР»СЊР·РѕРІР°РЅРёСЏ РЅРѕРІРѕРіРѕ РјРµС‚РѕРґР° _select_option_from_dropdown
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
import subprocess
import os
from scheduler_runner.utils.parser.core.base_parser import BaseParser
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains


class TestConcreteParser(BaseParser):
    """РўРµСЃС‚РѕРІС‹Р№ РґРѕС‡РµСЂРЅРёР№ РєР»Р°СЃСЃ РґР»СЏ С‚РµСЃС‚РёСЂРѕРІР°РЅРёСЏ BaseParser"""

    def login(self):
        """Р РµР°Р»РёР·Р°С†РёСЏ Р°Р±СЃС‚СЂР°РєС‚РЅРѕРіРѕ РјРµС‚РѕРґР° login"""
        pass

    def navigate_to_target(self):
        """Р РµР°Р»РёР·Р°С†РёСЏ Р°Р±СЃС‚СЂР°РєС‚РЅРѕРіРѕ РјРµС‚РѕРґР° navigate_to_target"""
        pass

    def extract_data(self):
        """Р РµР°Р»РёР·Р°С†РёСЏ Р°Р±СЃС‚СЂР°РєС‚РЅРѕРіРѕ РјРµС‚РѕРґР° extract_data"""
        return {}

    def logout(self):
        """Р РµР°Р»РёР·Р°С†РёСЏ Р°Р±СЃС‚СЂР°РєС‚РЅРѕРіРѕ РјРµС‚РѕРґР° logout"""
        pass


class TestBaseParser(unittest.TestCase):
    """РўРµСЃС‚С‹ РґР»СЏ РЅРѕРІРѕРіРѕ BaseParser"""

    def setUp(self):
        """РќР°СЃС‚СЂРѕР№РєР° С‚РµСЃС‚Р°"""
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
        """РўРµСЃС‚ РёРЅРёС†РёР°Р»РёР·Р°С†РёРё"""
        self.assertEqual(self.parser.config, self.config)
        self.assertIsNone(self.parser.driver)
        self.assertIsNotNone(self.parser.logger)

    def test_get_current_user(self):
        """РўРµСЃС‚ РїРѕР»СѓС‡РµРЅРёСЏ С‚РµРєСѓС‰РµРіРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ"""
        with patch('os.getlogin', return_value='test_user'):
            result = self.parser._get_current_user()
            self.assertEqual(result, 'test_user')

    @patch('scheduler_runner.utils.parser.core.base_parser.os.getlogin')
    def test_get_default_browser_user_data_dir(self, mock_getlogin):
        """РўРµСЃС‚ РїРѕР»СѓС‡РµРЅРёСЏ РїСѓС‚Рё Рє РґР°РЅРЅС‹Рј Р±СЂР°СѓР·РµСЂР°"""
        mock_getlogin.return_value = 'test_user'

        # РўРµСЃС‚ СЃ СѓРєР°Р·Р°РЅРЅС‹Рј РїРѕР»СЊР·РѕРІР°С‚РµР»РµРј
        result = self.parser._get_default_browser_user_data_dir('custom_user')
        expected = "C:/Users/custom_user/AppData/Local/Microsoft/Edge/User Data"
        self.assertEqual(result, expected)

        # РўРµСЃС‚ СЃ С‚РµРєСѓС‰РёРј РїРѕР»СЊР·РѕРІР°С‚РµР»РµРј
        result_with_current = self.parser._get_default_browser_user_data_dir()
        expected_with_current = "C:/Users/test_user/AppData/Local/Microsoft/Edge/User Data"
        self.assertEqual(result_with_current, expected_with_current)

    @patch('scheduler_runner.utils.parser.core.base_parser.webdriver.Edge')
    @patch('scheduler_runner.utils.parser.core.base_parser.os.path.exists', return_value=True)
    @patch('scheduler_runner.utils.parser.core.base_parser.BaseParser._get_default_browser_user_data_dir', return_value='C:/tmp/edge-profile')
    def test_setup_browser(self, mock_get_default_path, mock_exists, mock_webdriver):
        """РўРµСЃС‚ РЅР°СЃС‚СЂРѕР№РєРё Р±СЂР°СѓР·РµСЂР°"""
        mock_driver = Mock()
        mock_driver.session_id = "test_session_id_12345"
        mock_webdriver.return_value = mock_driver

        # Mock-РёРј РјРµС‚РѕРґ _terminate_browser_processes Сѓ СЌРєР·РµРјРїР»СЏСЂР°
        self.parser._terminate_browser_processes = Mock()

        result = self.parser.setup_browser()

        self.parser._terminate_browser_processes.assert_called_once()
        mock_webdriver.assert_called_once()
        self.assertTrue(result)
        self.assertEqual(self.parser.driver, mock_driver)

    @patch('scheduler_runner.utils.parser.core.base_parser.webdriver.Edge')
    @patch('scheduler_runner.utils.parser.core.base_parser.time.sleep')
    @patch('scheduler_runner.utils.parser.core.base_parser.os.path.exists', return_value=True)
    @patch('scheduler_runner.utils.parser.core.base_parser.BaseParser._get_default_browser_user_data_dir', return_value='C:/tmp/edge-profile')
    def test_setup_browser_fallback_to_non_headless_on_startup_crash(
        self, mock_get_default_path, mock_exists, mock_sleep, mock_webdriver
    ):
        """РўРµСЃС‚ Р°РІР°СЂРёР№РЅРѕРіРѕ РѕР±С…РѕРґР°: РїРѕСЃР»Рµ РєСЂР°С€Р° РІ headless РІС‹РїРѕР»РЅСЏРµС‚СЃСЏ fallback headless=False."""
        crash_error = Exception("Microsoft Edge failed to start: crashed. DevToolsActivePort file doesn't exist")
        fallback_driver = Mock()
        fallback_driver.session_id = "fallback_session_id_12345"

        # 3 С„РµР№Р»Р° primary + 1 СѓСЃРїРµС€РЅС‹Р№ Р·Р°РїСѓСЃРє fallback.
        mock_webdriver.side_effect = [crash_error, crash_error, crash_error, fallback_driver]

        self.parser._terminate_browser_processes = Mock()
        self.parser._cleanup_lock_files = Mock()
        self.parser._log_startup_environment = Mock()
        self.parser.config['browser_config']['headless'] = True

        result = self.parser.setup_browser()

        self.assertTrue(result)
        self.assertEqual(self.parser.driver, fallback_driver)
        # РћРґРёРЅ РІС‹Р·РѕРІ РґРѕ primary + РѕРґРёРЅ РїРµСЂРµРґ fallback.
        self.assertEqual(self.parser._terminate_browser_processes.call_count, 2)
        self.assertEqual(mock_webdriver.call_count, 4)

    @patch('scheduler_runner.utils.parser.core.base_parser.webdriver.Edge')
    @patch('scheduler_runner.utils.parser.core.base_parser.time.sleep')
    @patch('scheduler_runner.utils.parser.core.base_parser.os.path.exists', return_value=True)
    @patch('scheduler_runner.utils.parser.core.base_parser.BaseParser._get_default_browser_user_data_dir', return_value='C:/tmp/edge-profile')
    def test_setup_browser_no_fallback_for_non_signature_error(
        self, mock_get_default_path, mock_exists, mock_sleep, mock_webdriver
    ):
        """РўРµСЃС‚: fallback РЅРµ СЃСЂР°Р±Р°С‚С‹РІР°РµС‚, РµСЃР»Рё РѕС€РёР±РєР° РЅРµ СЃРѕРґРµСЂР¶РёС‚ СЃРёРіРЅР°С‚СѓСЂСѓ startup crash."""
        generic_error = Exception("Random webdriver error")
        mock_webdriver.side_effect = [generic_error, generic_error, generic_error]

        self.parser._terminate_browser_processes = Mock()
        self.parser._cleanup_lock_files = Mock()
        self.parser._log_startup_environment = Mock()

        result = self.parser.setup_browser()

        self.assertFalse(result)
        # РўРѕР»СЊРєРѕ РїРµСЂРІРёС‡РЅС‹Р№ РІС‹Р·РѕРІ, fallback Р·Р°РїСѓСЃРєР°С‚СЊСЃСЏ РЅРµ РґРѕР»Р¶РµРЅ.
        self.assertEqual(self.parser._terminate_browser_processes.call_count, 1)
        self.assertEqual(mock_webdriver.call_count, 3)

    def test_close_browser(self):
        """РўРµСЃС‚ Р·Р°РєСЂС‹С‚РёСЏ Р±СЂР°СѓР·РµСЂР°"""
        mock_driver = Mock()
        self.parser.driver = mock_driver

        self.parser.close_browser()

        mock_driver.quit.assert_called_once()
        self.assertIsNone(self.parser.driver)

    def test_close_browser_without_driver(self):
        """РўРµСЃС‚ Р·Р°РєСЂС‹С‚РёСЏ Р±СЂР°СѓР·РµСЂР° Р±РµР· РґСЂР°Р№РІРµСЂР°"""
        # РќРµ РґРѕР»Р¶РЅРѕ РІС‹Р·РІР°С‚СЊ РѕС€РёР±РєСѓ
        self.parser.close_browser()
        self.assertIsNone(self.parser.driver)

    @patch('scheduler_runner.utils.parser.core.base_parser.subprocess.run')
    @patch('scheduler_runner.utils.parser.core.base_parser.time.sleep')
    def test_terminate_browser_processes(self, mock_sleep, mock_subprocess_run):
        """РўРµСЃС‚ Р·Р°РІРµСЂС€РµРЅРёСЏ РїСЂРѕС†РµСЃСЃРѕРІ Р±СЂР°СѓР·РµСЂР°"""
        mock_subprocess_run.return_value = Mock()

        self.parser._terminate_browser_processes()

        mock_subprocess_run.assert_called_once_with(
            ["taskkill", "/f", "/im", "msedge.exe"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        mock_sleep.assert_called_once_with(2)

    @patch('scheduler_runner.utils.parser.core.base_parser.time.sleep')
    @patch('scheduler_runner.utils.parser.core.base_parser.subprocess.run')
    def test_terminate_browser_processes_with_custom_executable(self, mock_subprocess_run, mock_sleep):
        """РўРµСЃС‚ Р·Р°РІРµСЂС€РµРЅРёСЏ РїСЂРѕС†РµСЃСЃРѕРІ Р±СЂР°СѓР·РµСЂР° СЃ РїРѕР»СЊР·РѕРІР°С‚РµР»СЊСЃРєРёРј РёСЃРїРѕР»РЅСЏРµРјС‹Рј С„Р°Р№Р»РѕРј"""
        mock_subprocess_run.return_value = Mock()

        # РР·РјРµРЅРёРј РєРѕРЅС„РёРі РґР»СЏ С‚РµСЃС‚РёСЂРѕРІР°РЅРёСЏ РґСЂСѓРіРѕРіРѕ РёСЃРїРѕР»РЅСЏРµРјРѕРіРѕ С„Р°Р№Р»Р°
        self.parser.config['BROWSER_EXECUTABLE'] = 'chrome.exe'

        self.parser._terminate_browser_processes()

        mock_subprocess_run.assert_called_once_with(
            ["taskkill", "/f", "/im", "chrome.exe"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        mock_sleep.assert_called_once_with(2)

    @patch('scheduler_runner.utils.parser.core.base_parser.time.sleep')
    @patch('scheduler_runner.utils.parser.core.base_parser.subprocess.run')
    def test_terminate_browser_processes_exception(self, mock_subprocess_run, mock_sleep):
        """РўРµСЃС‚ Р·Р°РІРµСЂС€РµРЅРёСЏ РїСЂРѕС†РµСЃСЃРѕРІ Р±СЂР°СѓР·РµСЂР° СЃ РёСЃРєР»СЋС‡РµРЅРёРµРј"""
        mock_subprocess_run.side_effect = Exception("Test exception")

        # РќРµ РґРѕР»Р¶РЅРѕ РІС‹Р·РІР°С‚СЊ РѕС€РёР±РєСѓ
        self.parser._terminate_browser_processes()

        mock_subprocess_run.assert_called_once()
        # sleep РЅРµ РґРѕР»Р¶РµРЅ Р±С‹С‚СЊ РІС‹Р·РІР°РЅ, РµСЃР»Рё subprocess.run Р±СЂРѕСЃР°РµС‚ РёСЃРєР»СЋС‡РµРЅРёРµ
        mock_sleep.assert_not_called()

    @patch('scheduler_runner.utils.parser.core.base_parser.WebDriverWait')
    @patch('scheduler_runner.utils.parser.core.base_parser.By')
    def test_click_element_with_wait(self, mock_by, mock_wait):
        """РўРµСЃС‚ РєР»РёРєР° РїРѕ СЌР»РµРјРµРЅС‚Сѓ СЃ РѕР¶РёРґР°РЅРёРµРј"""
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

    @patch('scheduler_runner.utils.parser.core.base_parser.By')
    def test_click_element_without_wait(self, mock_by):
        """РўРµСЃС‚ РєР»РёРєР° РїРѕ СЌР»РµРјРµРЅС‚Сѓ Р±РµР· РѕР¶РёРґР°РЅРёСЏ"""
        mock_element = Mock()

        self.parser.driver = Mock()
        self.parser.driver.find_element.return_value = mock_element

        result = self.parser._click_element("//div[@id='test']", wait_for_clickable=False)

        self.parser.driver.find_element.assert_called_once()
        mock_element.click.assert_called_once()
        self.assertTrue(result)

    def test_get_element_value_input(self):
        """РўРµСЃС‚ РїРѕР»СѓС‡РµРЅРёСЏ Р·РЅР°С‡РµРЅРёСЏ СЌР»РµРјРµРЅС‚Р° С‚РёРїР° input"""
        mock_element = Mock()
        mock_element.get_attribute.return_value = "РўРµСЃС‚РѕРІРѕРµ Р·РЅР°С‡РµРЅРёРµ"
        mock_element.text = ""

        self.parser.driver = Mock()
        self.parser.driver.find_element.return_value = mock_element

        result = self.parser.get_element_value("//input[@id='test']", element_type='input')

        self.parser.driver.find_element.assert_called_once()
        mock_element.get_attribute.assert_called_once_with('value')
        self.assertEqual(result, "РўРµСЃС‚РѕРІРѕРµ Р·РЅР°С‡РµРЅРёРµ")

    def test_get_element_value_div(self):
        """РўРµСЃС‚ РїРѕР»СѓС‡РµРЅРёСЏ Р·РЅР°С‡РµРЅРёСЏ СЌР»РµРјРµРЅС‚Р° С‚РёРїР° div"""
        mock_element = Mock()
        mock_element.text = "РўРµРєСЃС‚ СЌР»РµРјРµРЅС‚Р°"

        self.parser.driver = Mock()
        self.parser.driver.find_element.return_value = mock_element

        result = self.parser.get_element_value("//div[@id='test']", element_type='div')

        self.parser.driver.find_element.assert_called_once()
        self.assertEqual(result, "РўРµРєСЃС‚ СЌР»РµРјРµРЅС‚Р°")

    def test_set_element_value_input(self):
        """РўРµСЃС‚ СѓСЃС‚Р°РЅРѕРІРєРё Р·РЅР°С‡РµРЅРёСЏ СЌР»РµРјРµРЅС‚Р° С‚РёРїР° input"""
        mock_element = Mock()
        # Р’РѕР·РІСЂР°С‰Р°РµРјРѕРµ Р·РЅР°С‡РµРЅРёРµ РґР»СЏ get_attribute('value') РґРѕР»Р¶РЅРѕ СЃРѕРІРїР°РґР°С‚СЊ СЃ СѓСЃС‚Р°РЅР°РІР»РёРІР°РµРјС‹Рј Р·РЅР°С‡РµРЅРёРµРј
        mock_element.get_attribute.return_value = "РќРѕРІРѕРµ Р·РЅР°С‡РµРЅРёРµ"
        mock_element.text = ""

        self.parser.driver = Mock()
        self.parser.driver.find_element.return_value = mock_element

        result = self.parser.set_element_value("//input[@id='test']", "РќРѕРІРѕРµ Р·РЅР°С‡РµРЅРёРµ", element_type='input')

        self.parser.driver.find_element.assert_called_once()
        mock_element.clear.assert_called_once()
        mock_element.send_keys.assert_called_once_with("РќРѕРІРѕРµ Р·РЅР°С‡РµРЅРёРµ")
        self.assertTrue(result)

    @patch('selenium.webdriver.support.ui.Select')
    def test_set_element_value_dropdown(self, mock_select_class):
        """РўРµСЃС‚ СѓСЃС‚Р°РЅРѕРІРєРё Р·РЅР°С‡РµРЅРёСЏ СЌР»РµРјРµРЅС‚Р° С‚РёРїР° dropdown"""
        mock_element = Mock()
        mock_select = Mock()

        mock_select_class.return_value = mock_select

        self.parser.driver = Mock()
        self.parser.driver.find_element.return_value = mock_element

        result = self.parser.set_element_value("//select[@id='test']", "option_value", element_type='dropdown')

        mock_select_class.assert_called_once_with(mock_element)
        mock_select.select_by_value.assert_called_once_with("option_value")
        self.assertTrue(result)

    @patch('scheduler_runner.utils.parser.core.base_parser.ActionChains')
    @patch('scheduler_runner.utils.parser.core.base_parser.By')
    def test_select_option_from_dropdown_exact_match(self, mock_by, mock_actions_class):
        """РўРµСЃС‚ РјРµС‚РѕРґР° РІС‹Р±РѕСЂР° РѕРїС†РёРё РёР· РІС‹РїР°РґР°СЋС‰РµРіРѕ СЃРїРёСЃРєР° СЃ С‚РѕС‡РЅС‹Рј СЃРѕРІРїР°РґРµРЅРёРµРј"""
        # РќР°СЃС‚СЂРѕР№РєР° mock-РѕР±СЉРµРєС‚РѕРІ
        mock_dropdown = Mock()
        mock_option = Mock()
        mock_option.text = "РўСЂРµР±СѓРµРјС‹Р№ РџР’Р—"
        mock_option.get_attribute.return_value = None

        self.parser.driver = Mock()
        self.parser.driver.find_element.return_value = mock_dropdown
        self.parser.driver.find_elements.return_value = [mock_option]

        mock_action_chains_instance = Mock()
        # РќР°СЃС‚СЂРѕР№РєР° С†РµРїРѕС‡РєРё РІС‹Р·РѕРІРѕРІ: move_to_element().click().perform()
        mock_action_chains_instance.move_to_element.return_value = mock_action_chains_instance
        mock_action_chains_instance.click.return_value = mock_action_chains_instance
        mock_actions_class.return_value = mock_action_chains_instance

        # Р’С‹Р·РѕРІ С‚РµСЃС‚РёСЂСѓРµРјРѕРіРѕ РјРµС‚РѕРґР°
        result = self.parser._select_option_from_dropdown(
            dropdown_selector="//select[@id='pvz']",
            option_selector="//option",
            option_value="РўСЂРµР±СѓРµРјС‹Р№ РџР’Р—",
            exact_match=True
        )

        # РџСЂРѕРІРµСЂРєРё
        self.assertTrue(result)
        # РњРµС‚РѕРґ _select_option_from_dropdown РІС‹Р·С‹РІР°РµС‚ find_element РЅРµСЃРєРѕР»СЊРєРѕ СЂР°Р·,
        # РїРѕСЌС‚РѕРјСѓ РїСЂРѕРІРµСЂРёРј, С‡С‚Рѕ РѕРЅ Р±С‹Р» РІС‹Р·РІР°РЅ С…РѕС‚СЏ Р±С‹ РѕРґРёРЅ СЂР°Р·
        self.assertGreaterEqual(self.parser.driver.find_element.call_count, 1)
        self.parser.driver.find_elements.assert_called_once()
        mock_actions_class.assert_called_once_with(self.parser.driver)
        mock_action_chains_instance.move_to_element.assert_called_once_with(mock_option)
        mock_action_chains_instance.click.assert_called_once()
        mock_action_chains_instance.perform.assert_called_once()

    @patch('scheduler_runner.utils.parser.core.base_parser.ActionChains')
    @patch('scheduler_runner.utils.parser.core.base_parser.By')
    def test_select_option_from_dropdown_no_match(self, mock_by, mock_actions):
        """РўРµСЃС‚ РјРµС‚РѕРґР° РІС‹Р±РѕСЂР° РѕРїС†РёРё РёР· РІС‹РїР°РґР°СЋС‰РµРіРѕ СЃРїРёСЃРєР° Р±РµР· СЃРѕРІРїР°РґРµРЅРёР№"""
        # РќР°СЃС‚СЂРѕР№РєР° mock-РѕР±СЉРµРєС‚РѕРІ
        mock_dropdown = Mock()
        mock_option = Mock()
        mock_option.text = "Р”СЂСѓРіРѕР№ РџР’Р—"
        mock_option.get_attribute.return_value = None

        self.parser.driver = Mock()
        self.parser.driver.find_element.return_value = mock_dropdown
        self.parser.driver.find_elements.return_value = [mock_option]

        # Р’С‹Р·РѕРІ С‚РµСЃС‚РёСЂСѓРµРјРѕРіРѕ РјРµС‚РѕРґР°
        result = self.parser._select_option_from_dropdown(
            dropdown_selector="//select[@id='pvz']",
            option_selector="//option",
            option_value="РўСЂРµР±СѓРµРјС‹Р№ РџР’Р—",
            exact_match=True
        )

        # РџСЂРѕРІРµСЂРєРё
        self.assertFalse(result)
        # РњРµС‚РѕРґ _select_option_from_dropdown РІС‹Р·С‹РІР°РµС‚ find_element РЅРµСЃРєРѕР»СЊРєРѕ СЂР°Р·,
        # РїРѕСЌС‚РѕРјСѓ РїСЂРѕРІРµСЂРёРј, С‡С‚Рѕ РѕРЅ Р±С‹Р» РІС‹Р·РІР°РЅ С…РѕС‚СЏ Р±С‹ РѕРґРёРЅ СЂР°Р·
        self.assertGreaterEqual(self.parser.driver.find_element.call_count, 1)
        self.parser.driver.find_elements.assert_called_once()
        # ActionChains РЅРµ РґРѕР»Р¶РµРЅ Р±С‹С‚СЊ РІС‹Р·РІР°РЅ, С‚Р°Рє РєР°Рє РѕРїС†РёСЏ РЅРµ РЅР°Р№РґРµРЅР°
        mock_actions.assert_not_called()

    @patch('scheduler_runner.utils.parser.core.base_parser.ActionChains')
    @patch('scheduler_runner.utils.parser.core.base_parser.By')
    def test_select_option_from_dropdown_partial_match(self, mock_by, mock_actions_class):
        """РўРµСЃС‚ РјРµС‚РѕРґР° РІС‹Р±РѕСЂР° РѕРїС†РёРё РёР· РІС‹РїР°РґР°СЋС‰РµРіРѕ СЃРїРёСЃРєР° СЃ С‡Р°СЃС‚РёС‡РЅС‹Рј СЃРѕРІРїР°РґРµРЅРёРµРј"""
        # РќР°СЃС‚СЂРѕР№РєР° mock-РѕР±СЉРµРєС‚РѕРІ
        mock_dropdown = Mock()
        mock_option = Mock()
        mock_option.text = "РўСЂРµР±СѓРµРјС‹Р№ РџР’Р— - РњРѕСЃРєРІР°"
        mock_option.get_attribute.return_value = None

        self.parser.driver = Mock()
        self.parser.driver.find_element.return_value = mock_dropdown
        self.parser.driver.find_elements.return_value = [mock_option]

        mock_action_chains_instance = Mock()
        # РќР°СЃС‚СЂРѕР№РєР° С†РµРїРѕС‡РєРё РІС‹Р·РѕРІРѕРІ: move_to_element().click().perform()
        mock_action_chains_instance.move_to_element.return_value = mock_action_chains_instance
        mock_action_chains_instance.click.return_value = mock_action_chains_instance
        mock_actions_class.return_value = mock_action_chains_instance

        # Р’С‹Р·РѕРІ С‚РµСЃС‚РёСЂСѓРµРјРѕРіРѕ РјРµС‚РѕРґР°
        result = self.parser._select_option_from_dropdown(
            dropdown_selector="//select[@id='pvz']",
            option_selector="//option",
            option_value="РўСЂРµР±СѓРµРјС‹Р№ РџР’Р—",
            exact_match=False
        )

        # РџСЂРѕРІРµСЂРєРё
        self.assertTrue(result)
        # РњРµС‚РѕРґ _select_option_from_dropdown РІС‹Р·С‹РІР°РµС‚ find_element РЅРµСЃРєРѕР»СЊРєРѕ СЂР°Р·,
        # РїРѕСЌС‚РѕРјСѓ РїСЂРѕРІРµСЂРёРј, С‡С‚Рѕ РѕРЅ Р±С‹Р» РІС‹Р·РІР°РЅ С…РѕС‚СЏ Р±С‹ РѕРґРёРЅ СЂР°Р·
        self.assertGreaterEqual(self.parser.driver.find_element.call_count, 1)
        self.parser.driver.find_elements.assert_called_once()
        mock_actions_class.assert_called_once_with(self.parser.driver)
        mock_action_chains_instance.move_to_element.assert_called_once_with(mock_option)
        mock_action_chains_instance.click.assert_called_once()
        mock_action_chains_instance.perform.assert_called_once()

    def test_run_parser_success(self):
        """РўРµСЃС‚ РјРµС‚РѕРґР° Р·Р°РїСѓСЃРєР° РїР°СЂСЃРµСЂР° СЃ СѓСЃРїРµС€РЅС‹Рј РІС‹РїРѕР»РЅРµРЅРёРµРј"""
        # Mock-РёРј РІСЃРµ Р°Р±СЃС‚СЂР°РєС‚РЅС‹Рµ РјРµС‚РѕРґС‹
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
        """РўРµСЃС‚ РјРµС‚РѕРґР° Р·Р°РїСѓСЃРєР° РїР°СЂСЃРµСЂР° СЃ РѕС€РёР±РєРѕР№ РЅР° СЌС‚Р°РїРµ РЅР°СЃС‚СЂРѕР№РєРё Р±СЂР°СѓР·РµСЂР°"""
        self.parser.setup_browser = Mock(return_value=False)

        with self.assertRaises(Exception) as context:
            self.parser.run_parser()

        self.parser.setup_browser.assert_called_once()
        self.assertIn("\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043d\u0430\u0441\u0442\u0440\u043e\u0438\u0442\u044c \u0431\u0440\u0430\u0443\u0437\u0435\u0440", str(context.exception))


class TestNewFunctionality(unittest.TestCase):
    """РўРµСЃС‚С‹ РґР»СЏ РЅРѕРІС‹С… С„СѓРЅРєС†РёР№, РґРѕР±Р°РІР»РµРЅРЅС‹С… РІ РІРµСЂСЃРёРё 3.0.0"""

    def setUp(self):
        """РќР°СЃС‚СЂРѕР№РєР° С‚РµСЃС‚Р°"""
        from scheduler_runner.utils.parser.configs.base_configs.base_config import BASE_CONFIG
        self.config = BASE_CONFIG.copy()
        self.parser = TestConcreteParser(self.config)

    def test_set_element_value_with_dropdown(self):
        """РўРµСЃС‚РёСЂРѕРІР°РЅРёРµ РјРµС‚РѕРґР° set_element_value СЃ РІС‹РїР°РґР°СЋС‰РёРј СЃРїРёСЃРєРѕРј"""
        # РЎРѕР·РґР°РµРј РјРѕРє-РѕР±СЉРµРєС‚С‹ РґР»СЏ С‚РµСЃС‚РёСЂРѕРІР°РЅРёСЏ
        mock_driver = Mock()
        mock_element = Mock()

        # РњРѕРєР°РµРј РґСЂР°Р№РІРµСЂ
        self.parser.driver = mock_driver

        # РќР°СЃС‚СЂРѕР№РєР° РІРѕР·РІСЂР°С‰Р°РµРјРѕРіРѕ Р·РЅР°С‡РµРЅРёСЏ РґР»СЏ find_element
        mock_driver.find_element.return_value = mock_element

        # РњРѕРєРёСЂСѓРµРј _select_option_from_dropdown, С‡С‚РѕР±С‹ РїСЂРѕРІРµСЂРёС‚СЊ, С‡С‚Рѕ РѕРЅ РІС‹Р·С‹РІР°РµС‚СЃСЏ
        with patch.object(self.parser, '_select_option_from_dropdown', return_value=True) as mock_method:
            # Р’С‹Р·С‹РІР°РµРј РјРµС‚РѕРґ set_element_value СЃ С‚РёРїРѕРј dropdown
            result = self.parser.set_element_value("//select[@id='test']", "option_value", element_type="dropdown")

            # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ find_element Р±С‹Р» РІС‹Р·РІР°РЅ
            mock_driver.find_element.assert_called_once_with(By.XPATH, "//select[@id='test']")

            # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ _select_option_from_dropdown Р±С‹Р» РІС‹Р·РІР°РЅ СЃ РїСЂР°РІРёР»СЊРЅС‹РјРё РїР°СЂР°РјРµС‚СЂР°РјРё
            mock_method.assert_called_once_with(element=mock_element, option_value="option_value")

            self.assertTrue(result)

    def test_select_option_from_dropdown_with_element(self):
        """РўРµСЃС‚РёСЂРѕРІР°РЅРёРµ РјРµС‚РѕРґР° _select_option_from_dropdown СЃ РїРµСЂРµРґР°РЅРЅС‹Рј СЌР»РµРјРµРЅС‚РѕРј"""
        # РЎРѕР·РґР°РµРј РјРѕРє-РѕР±СЉРµРєС‚С‹ РґР»СЏ С‚РµСЃС‚РёСЂРѕРІР°РЅРёСЏ
        mock_element = Mock()
        mock_select_instance = Mock()

        # РњРѕРєРёСЂСѓРµРј Select, С‡С‚РѕР±С‹ РїСЂРѕРІРµСЂРёС‚СЊ, С‡С‚Рѕ РѕРЅ РёСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ РїСЂР°РІРёР»СЊРЅРѕ
        with patch('selenium.webdriver.support.ui.Select', return_value=mock_select_instance):
            # Р’С‹Р·С‹РІР°РµРј РјРµС‚РѕРґ _select_option_from_dropdown СЃ РїРµСЂРµРґР°РЅРЅС‹Рј СЌР»РµРјРµРЅС‚РѕРј
            result = self.parser._select_option_from_dropdown(element=mock_element, option_value="option_value")

            # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ Select Р±С‹Р» РІС‹Р·РІР°РЅ СЃ РїСЂР°РІРёР»СЊРЅС‹Рј СЌР»РµРјРµРЅС‚РѕРј
            from selenium.webdriver.support.ui import Select
            Select.assert_called_once_with(mock_element)

            # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ select_by_value Р±С‹Р» РІС‹Р·РІР°РЅ СЃ РїСЂР°РІРёР»СЊРЅС‹Рј Р·РЅР°С‡РµРЅРёРµРј
            mock_select_instance.select_by_value.assert_called_once_with("option_value")

            self.assertTrue(result)

    def test_set_checkbox_state_checked(self):
        """РўРµСЃС‚РёСЂРѕРІР°РЅРёРµ РјРµС‚РѕРґР° _set_checkbox_state РґР»СЏ СѓСЃС‚Р°РЅРѕРІРєРё СЃРѕСЃС‚РѕСЏРЅРёСЏ 'РѕС‚РјРµС‡РµРЅ'"""
        # РЎРѕР·РґР°РµРј РјРѕРє-РѕР±СЉРµРєС‚ РґР»СЏ СЌР»РµРјРµРЅС‚Р°
        mock_element = Mock()
        # РќР°СЃС‚СЂРѕР№РєР° РІРѕР·РІСЂР°С‰Р°РµРјРѕРіРѕ Р·РЅР°С‡РµРЅРёСЏ РґР»СЏ is_selected
        # РЎРЅР°С‡Р°Р»Р° РІРѕР·РІСЂР°С‰Р°РµС‚ False (РЅРµ РѕС‚РјРµС‡РµРЅ), РїРѕС‚РѕРј True (РѕС‚РјРµС‡РµРЅ РїРѕСЃР»Рµ РєР»РёРєР°)
        mock_element.is_selected.side_effect = [False, True]

        # Р’С‹Р·С‹РІР°РµРј РјРµС‚РѕРґ _set_checkbox_state РґР»СЏ СѓСЃС‚Р°РЅРѕРІРєРё СЃРѕСЃС‚РѕСЏРЅРёСЏ 'РѕС‚РјРµС‡РµРЅ'
        result = self.parser._set_checkbox_state(mock_element, True)

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЌР»РµРјРµРЅС‚ Р±С‹Р» РєР»РёРєРЅСѓС‚ (РёР·РјРµРЅРёР»РѕСЃСЊ СЃРѕСЃС‚РѕСЏРЅРёРµ)
        mock_element.click.assert_called_once()
        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЂРµР·СѓР»СЊС‚Р°С‚ СЂР°РІРµРЅ True (СѓСЃРїРµС€РЅРѕ СѓСЃС‚Р°РЅРѕРІР»РµРЅРѕ СЃРѕСЃС‚РѕСЏРЅРёРµ)
        self.assertTrue(result)

    def test_set_checkbox_state_unchecked(self):
        """РўРµСЃС‚РёСЂРѕРІР°РЅРёРµ РјРµС‚РѕРґР° _set_checkbox_state РґР»СЏ СѓСЃС‚Р°РЅРѕРІРєРё СЃРѕСЃС‚РѕСЏРЅРёСЏ 'РЅРµ РѕС‚РјРµС‡РµРЅ'"""
        # РЎРѕР·РґР°РµРј РјРѕРє-РѕР±СЉРµРєС‚ РґР»СЏ СЌР»РµРјРµРЅС‚Р°
        mock_element = Mock()
        # РќР°СЃС‚СЂРѕР№РєР° РІРѕР·РІСЂР°С‰Р°РµРјРѕРіРѕ Р·РЅР°С‡РµРЅРёСЏ РґР»СЏ is_selected
        # РЎРЅР°С‡Р°Р»Р° РІРѕР·РІСЂР°С‰Р°РµС‚ True (РѕС‚РјРµС‡РµРЅ), РїРѕС‚РѕРј False (РЅРµ РѕС‚РјРµС‡РµРЅ РїРѕСЃР»Рµ РєР»РёРєР°)
        mock_element.is_selected.side_effect = [True, False]

        # Р’С‹Р·С‹РІР°РµРј РјРµС‚РѕРґ _set_checkbox_state РґР»СЏ СѓСЃС‚Р°РЅРѕРІРєРё СЃРѕСЃС‚РѕСЏРЅРёСЏ 'РЅРµ РѕС‚РјРµС‡РµРЅ'
        result = self.parser._set_checkbox_state(mock_element, False)

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЌР»РµРјРµРЅС‚ Р±С‹Р» РєР»РёРєРЅСѓС‚ (РёР·РјРµРЅРёР»РѕСЃСЊ СЃРѕСЃС‚РѕСЏРЅРёРµ)
        mock_element.click.assert_called_once()
        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЂРµР·СѓР»СЊС‚Р°С‚ СЂР°РІРµРЅ True (СѓСЃРїРµС€РЅРѕ СѓСЃС‚Р°РЅРѕРІР»РµРЅРѕ СЃРѕСЃС‚РѕСЏРЅРёРµ)
        self.assertTrue(result)

    def test_set_checkbox_state_no_change_needed(self):
        """РўРµСЃС‚РёСЂРѕРІР°РЅРёРµ РјРµС‚РѕРґР° _set_checkbox_state РєРѕРіРґР° РёР·РјРµРЅРµРЅРёРµ РЅРµ С‚СЂРµР±СѓРµС‚СЃСЏ"""
        # РЎРѕР·РґР°РµРј РјРѕРє-РѕР±СЉРµРєС‚ РґР»СЏ СЌР»РµРјРµРЅС‚Р°
        mock_element = Mock()
        mock_element.is_selected.return_value = True  # РР·РЅР°С‡Р°Р»СЊРЅРѕ РѕС‚РјРµС‡РµРЅ

        # Р’С‹Р·С‹РІР°РµРј РјРµС‚РѕРґ _set_checkbox_state РґР»СЏ СѓСЃС‚Р°РЅРѕРІРєРё СЃРѕСЃС‚РѕСЏРЅРёСЏ 'РѕС‚РјРµС‡РµРЅ' (СѓР¶Рµ С‚Р°Рє СѓСЃС‚Р°РЅРѕРІР»РµРЅРѕ)
        result = self.parser._set_checkbox_state(mock_element, True)

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЌР»РµРјРµРЅС‚ РЅРµ Р±С‹Р» РєР»РёРєРЅСѓС‚ (СЃРѕСЃС‚РѕСЏРЅРёРµ СѓР¶Рµ РїСЂР°РІРёР»СЊРЅРѕРµ)
        mock_element.click.assert_not_called()
        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЂРµР·СѓР»СЊС‚Р°С‚ СЂР°РІРµРЅ True (СЃРѕСЃС‚РѕСЏРЅРёРµ СѓР¶Рµ Р±С‹Р»Рѕ РїСЂР°РІРёР»СЊРЅРѕРµ)
        self.assertTrue(result)

    def test_extract_standard_table_data(self):
        """РўРµСЃС‚РёСЂРѕРІР°РЅРёРµ РјРµС‚РѕРґР° _extract_standard_table_data"""
        # РЎРѕР·РґР°РµРј РјРѕРє-РѕР±СЉРµРєС‚С‹ РґР»СЏ С‚РµСЃС‚РёСЂРѕРІР°РЅРёСЏ
        mock_table = Mock()
        mock_row1 = Mock()
        mock_row2 = Mock()
        mock_cell1 = Mock()
        mock_cell2 = Mock()
        mock_cell3 = Mock()
        mock_cell4 = Mock()

        # РќР°СЃС‚СЂРѕР№РєР° РІРѕР·РІСЂР°С‰Р°РµРјС‹С… Р·РЅР°С‡РµРЅРёР№
        mock_cell1.text = "Р—РЅР°С‡РµРЅРёРµ1"
        mock_cell2.text = "Р—РЅР°С‡РµРЅРёРµ2"
        mock_cell3.text = "Р—РЅР°С‡РµРЅРёРµ3"
        mock_cell4.text = "Р—РЅР°С‡РµРЅРёРµ4"

        mock_row1.find_element.side_effect = lambda by, value: mock_cell1 if "1" in value else mock_cell2
        mock_row2.find_element.side_effect = lambda by, value: mock_cell3 if "1" in value else mock_cell4
        
        mock_table.find_elements.return_value = [mock_row1, mock_row2]

        columns_config = [
            {"name": "col1", "selector": ".//td[1]"},
            {"name": "col2", "selector": ".//td[2]"}
        ]

        result = self.parser._extract_standard_table_data(mock_table, columns_config)

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЂРµР·СѓР»СЊС‚Р°С‚ СЃРѕРґРµСЂР¶РёС‚ РїСЂР°РІРёР»СЊРЅС‹Рµ РґР°РЅРЅС‹Рµ
        expected_result = [
            {"col1": "Р—РЅР°С‡РµРЅРёРµ1", "col2": "Р—РЅР°С‡РµРЅРёРµ2"},
            {"col1": "Р—РЅР°С‡РµРЅРёРµ3", "col2": "Р—РЅР°С‡РµРЅРёРµ4"}
        ]
        
        self.assertEqual(result, expected_result)
        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ Р±С‹Р»Рё РІС‹Р·РІР°РЅС‹ РјРµС‚РѕРґС‹ РґР»СЏ РїРѕРёСЃРєР° СЃС‚СЂРѕРє
        mock_table.find_elements.assert_called_once()

    def test_extract_standard_table_data_with_regex(self):
        """РўРµСЃС‚РёСЂРѕРІР°РЅРёРµ РјРµС‚РѕРґР° _extract_standard_table_data СЃ РёСЃРїРѕР»СЊР·РѕРІР°РЅРёРµРј СЂРµРіСѓР»СЏСЂРЅС‹С… РІС‹СЂР°Р¶РµРЅРёР№"""
        # РЎРѕР·РґР°РµРј РјРѕРє-РѕР±СЉРµРєС‚С‹ РґР»СЏ С‚РµСЃС‚РёСЂРѕРІР°РЅРёСЏ
        mock_table = Mock()
        mock_row = Mock()
        mock_cell = Mock()

        # РќР°СЃС‚СЂРѕР№РєР° РІРѕР·РІСЂР°С‰Р°РµРјРѕРіРѕ Р·РЅР°С‡РµРЅРёСЏ СЃ С‚РµРєСЃС‚РѕРј, СЃРѕРґРµСЂР¶Р°С‰РёРј С†РёС„СЂС‹
        mock_cell.text = "РљРѕР»РёС‡РµСЃС‚РІРѕ: 123 С€С‚."

        mock_row.find_element.return_value = mock_cell
        mock_table.find_elements.return_value = [mock_row]

        columns_config = [
            {"name": "count", "selector": ".//td[1]", "regex": r"\d+"}
        ]

        result = self.parser._extract_standard_table_data(mock_table, columns_config)

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЂРµРіСѓР»СЏСЂРЅРѕРµ РІС‹СЂР°Р¶РµРЅРёРµ РїСЂР°РІРёР»СЊРЅРѕ РёР·РІР»РµРєР»Рѕ С‡РёСЃР»Рѕ
        expected_result = [
            {"count": "123"}
        ]
        
        self.assertEqual(result, expected_result)

    def test_extract_table_data_with_valid_config(self):
        """РўРµСЃС‚РёСЂРѕРІР°РЅРёРµ РјРµС‚РѕРґР° extract_table_data СЃ РєРѕСЂСЂРµРєС‚РЅРѕР№ РєРѕРЅС„РёРіСѓСЂР°С†РёРµР№"""
        # РњРѕРєР°РµРј _extract_standard_table_data, С‡С‚РѕР±С‹ РїСЂРѕРІРµСЂРёС‚СЊ, С‡С‚Рѕ РѕРЅ РІС‹Р·С‹РІР°РµС‚СЃСЏ
        with patch.object(self.parser, '_extract_standard_table_data', return_value=[{"test": "data"}]) as mock_method:
            # РЎРѕР·РґР°РµРј РјРѕРє-С‚Р°Р±Р»РёС†Сѓ
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

            # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ _extract_standard_table_data Р±С‹Р» РІС‹Р·РІР°РЅ
            mock_method.assert_called_once()
            self.assertEqual(result, [{"test": "data"}])

    def test_extract_table_data_with_config_key(self):
        """РўРµСЃС‚РёСЂРѕРІР°РЅРёРµ РјРµС‚РѕРґР° extract_table_data СЃ РёСЃРїРѕР»СЊР·РѕРІР°РЅРёРµРј РєР»СЋС‡Р° РєРѕРЅС„РёРіСѓСЂР°С†РёРё"""
        # РћР±РЅРѕРІР»СЏРµРј РєРѕРЅС„РёРіСѓСЂР°С†РёСЋ РґР»СЏ С‚РµСЃС‚Р°
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

        # РњРѕРєР°РµРј _extract_standard_table_data
        with patch.object(self.parser, '_extract_standard_table_data', return_value=[{"col1": "value1"}]) as mock_method:
            # РЎРѕР·РґР°РµРј РјРѕРє-С‚Р°Р±Р»РёС†Сѓ
            mock_table_element = Mock()
            self.parser.driver = Mock()
            self.parser.driver.find_element.return_value = mock_table_element

            result = self.parser.extract_table_data(table_config_key='test_table')

            # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ _extract_standard_table_data Р±С‹Р» РІС‹Р·РІР°РЅ
            mock_method.assert_called_once()
            self.assertEqual(result, [{"col1": "value1"}])

    def test_extract_table_data_with_invalid_config(self):
        """РўРµСЃС‚РёСЂРѕРІР°РЅРёРµ РјРµС‚РѕРґР° extract_table_data СЃ РЅРµРєРѕСЂСЂРµРєС‚РЅРѕР№ РєРѕРЅС„РёРіСѓСЂР°С†РёРµР№"""
        # РўРµСЃС‚ СЃ РѕС‚СЃСѓС‚СЃС‚РІСѓСЋС‰РёРј РєР»СЋС‡РѕРј
        result = self.parser.extract_table_data(table_config_key='nonexistent_table')
        self.assertEqual(result, [])

        # РўРµСЃС‚ СЃ РЅРµРєРѕСЂСЂРµРєС‚РЅРѕР№ РєРѕРЅС„РёРіСѓСЂР°С†РёРµР№ (Р±РµР· РѕР±СЏР·Р°С‚РµР»СЊРЅС‹С… РїРѕР»РµР№)
        invalid_config = {
            "table_selector": "",  # РџСѓСЃС‚РѕР№ СЃРµР»РµРєС‚РѕСЂ
            "table_columns": []
        }
        result = self.parser.extract_table_data(table_config=invalid_config)
        self.assertEqual(result, [])

    def test_extract_table_data_exception_handling(self):
        """РўРµСЃС‚РёСЂРѕРІР°РЅРёРµ РјРµС‚РѕРґР° extract_table_data СЃ РѕР±СЂР°Р±РѕС‚РєРѕР№ РёСЃРєР»СЋС‡РµРЅРёР№"""
        # РњРѕРєР°РµРј find_element, С‡С‚РѕР±С‹ РІС‹Р±СЂРѕСЃРёС‚СЊ РёСЃРєР»СЋС‡РµРЅРёРµ
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


