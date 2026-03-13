"""
РњРѕРґСѓР»СЊ С‚РµСЃС‚РѕРІ РґР»СЏ OzonReportParser

Р­С‚РѕС‚ РјРѕРґСѓР»СЊ СЃРѕРґРµСЂР¶РёС‚ СЋРЅРёС‚-С‚РµСЃС‚С‹ РґР»СЏ РєР»Р°СЃСЃР° OzonReportParser,
РїСЂРѕРІРµСЂСЏСЋС‰РёРµ РѕСЃРЅРѕРІРЅСѓСЋ С„СѓРЅРєС†РёРѕРЅР°Р»СЊРЅРѕСЃС‚СЊ:
- Р¤РѕСЂРјРёСЂРѕРІР°РЅРёРµ URL С„РёР»СЊС‚СЂР°
- РќР°РІРёРіР°С†РёСЋ Рє С†РµР»РµРІРѕР№ СЃС‚СЂР°РЅРёС†Рµ
- Р Р°Р±РѕС‚Сѓ СЃ РџР’Р—
- РР·РІР»РµС‡РµРЅРёРµ РґР°РЅРЅС‹С… РѕС‚С‡РµС‚Р°
"""
import unittest
from unittest.mock import Mock, MagicMock, patch, ANY
from scheduler_runner.utils.parser.core.ozon_report_parser import OzonReportParser
from scheduler_runner.utils.parser.configs.base_configs.ozon_report_config import OZON_BASE_CONFIG


class TestOzonReportParserImpl(OzonReportParser):
    """РўРµСЃС‚РѕРІР°СЏ СЂРµР°Р»РёР·Р°С†РёСЏ Р°Р±СЃС‚СЂР°РєС‚РЅРѕРіРѕ РєР»Р°СЃСЃР° OzonReportParser"""

    def __init__(self, config, args=None, logger=None):
        # РЈСЃС‚Р°РЅР°РІР»РёРІР°РµРј logger РґРѕ РІС‹Р·РѕРІР° super().__init__
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
        # Р’С‹Р·С‹РІР°РµРј СЂРѕРґРёС‚РµР»СЊСЃРєСѓСЋ СЂРµР°Р»РёР·Р°С†РёСЋ
        return super().extract_report_data()

    def login(self):
        return True

    def logout(self):
        return True


class TestOzonReportParser(unittest.TestCase):
    """РўРµСЃС‚С‹ РґР»СЏ РєР»Р°СЃСЃР° OzonReportParser"""

    def setUp(self):
        """РџРѕРґРіРѕС‚РѕРІРєР° С‚РµСЃС‚РѕРІРѕР№ СЃСЂРµРґС‹"""
        self.config = OZON_BASE_CONFIG.copy()
        self.config['execution_date'] = '2026-01-15'  # РЈСЃС‚Р°РЅР°РІР»РёРІР°РµРј С„РёРєСЃРёСЂРѕРІР°РЅРЅСѓСЋ РґР°С‚Сѓ РґР»СЏ С‚РµСЃС‚РѕРІ
        self.parser = TestOzonReportParserImpl(self.config)

    # РўРµСЃС‚С‹ РґР»СЏ РјРµС‚РѕРґР° _build_url_filter (СѓРЅР°СЃР»РµРґРѕРІР°РЅРЅРѕРіРѕ РѕС‚ BaseReportParser, РЅРѕ РёСЃРїРѕР»СЊР·СѓРµРјРѕРіРѕ РІ OzonReportParser)
    def test_build_url_filter_with_date(self):
        """РўРµСЃС‚ С„РѕСЂРјРёСЂРѕРІР°РЅРёСЏ URL С„РёР»СЊС‚СЂР° СЃ РґР°С‚РѕР№"""
        # РЈСЃС‚Р°РЅР°РІР»РёРІР°РµРј РґР°С‚Сѓ РІС‹РїРѕР»РЅРµРЅРёСЏ
        self.parser.config['execution_date'] = '2026-01-15'
        # РЈСЃС‚Р°РЅР°РІР»РёРІР°РµРј РЅРµРѕР±С…РѕРґРёРјС‹Рµ С€Р°Р±Р»РѕРЅС‹ РґР»СЏ С„РѕСЂРјРёСЂРѕРІР°РЅРёСЏ С„РёР»СЊС‚СЂР°
        self.parser.config['filter_template'] = '?filter={"dateFilter": "{date_filter_template}", "dataType": "{data_type_filter_template}"}'
        self.parser.config['date_filter_template'] = '{"startDate": "{date}", "endDate": "{date}"}'
        self.parser.config['data_type_filter_template'] = '"SALES"'

        # Р’С‹Р·С‹РІР°РµРј РјРµС‚РѕРґ С„РѕСЂРјРёСЂРѕРІР°РЅРёСЏ С„РёР»СЊС‚СЂР°
        result = self.parser._build_url_filter()

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ С„РёР»СЊС‚СЂ СЃРѕРґРµСЂР¶РёС‚ РґР°С‚Сѓ
        self.assertIn('2026-01-15', result)
        self.assertIn('startDate', result)
        self.assertIn('endDate', result)
        self.assertIn('SALES', result)
        self.assertTrue(result.startswith('?filter={'))

    def test_build_url_filter_without_date(self):
        """РўРµСЃС‚ С„РѕСЂРјРёСЂРѕРІР°РЅРёСЏ URL С„РёР»СЊС‚СЂР° Р±РµР· РґР°С‚С‹"""
        # РЈРґР°Р»СЏРµРј РґР°С‚Сѓ РІС‹РїРѕР»РЅРµРЅРёСЏ
        if 'execution_date' in self.parser.config:
            del self.parser.config['execution_date']

        # РЈСЃС‚Р°РЅР°РІР»РёРІР°РµРј С€Р°Р±Р»РѕРЅС‹ РґР»СЏ С„РѕСЂРјРёСЂРѕРІР°РЅРёСЏ С„РёР»СЊС‚СЂР°
        self.parser.config['filter_template'] = '?filter={"dataType": "{data_type_filter_template}"}'
        self.parser.config['data_type_filter_template'] = '"SALES"'

        # Р’С‹Р·С‹РІР°РµРј РјРµС‚РѕРґ С„РѕСЂРјРёСЂРѕРІР°РЅРёСЏ С„РёР»СЊС‚СЂР°
        result = self.parser._build_url_filter()

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ С„РёР»СЊС‚СЂ РІСЃРµ СЂР°РІРЅРѕ С„РѕСЂРјРёСЂСѓРµС‚СЃСЏ (С…РѕС‚СЏ Рё Р±РµР· РґР°С‚С‹)
        # РџСЂРё РѕС‚СЃСѓС‚СЃС‚РІРёРё РґР°С‚С‹ РґРѕР»Р¶РµРЅ С„РѕСЂРјРёСЂРѕРІР°С‚СЊСЃСЏ С„РёР»СЊС‚СЂ С‚РѕР»СЊРєРѕ СЃ С‚РёРїРѕРј РґР°РЅРЅС‹С…
        self.assertIn('SALES', result)
        # РўР°РєР¶Рµ РїСЂРѕРІРµСЂРёРј, С‡С‚Рѕ РІ С„РёР»СЊС‚СЂРµ РЅРµС‚ РїР»РµР№СЃС…РѕР»РґРµСЂРѕРІ
        self.assertNotIn('{date_filter_template}', result)
        self.assertNotIn('{data_type_filter_template}', result)

    def test_build_url_filter_with_different_date_formats(self):
        """РўРµСЃС‚ С„РѕСЂРјРёСЂРѕРІР°РЅРёСЏ URL С„РёР»СЊС‚СЂР° СЃ СЂР°Р·РЅС‹РјРё С„РѕСЂРјР°С‚Р°РјРё РґР°С‚С‹"""
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

    # РўРµСЃС‚С‹ РґР»СЏ РјРµС‚РѕРґР° get_current_pvz
    def test_get_current_pvz_success(self):
        """РўРµСЃС‚ СѓСЃРїРµС€РЅРѕРіРѕ РїРѕР»СѓС‡РµРЅРёСЏ С‚РµРєСѓС‰РµРіРѕ РџР’Р—"""
        # РњРѕРєР°РµРј РјРµС‚РѕРґ get_element_value
        self.parser.get_element_value = Mock(return_value='TEST_PVZ_123')

        # РњРѕРєР°РµРј СЃРµР»РµРєС‚РѕСЂС‹
        self.parser.config['selectors'] = {
            'pvz_selectors': {
                'input_class_readonly': '//test-input-class',
                'input_readonly': '//test-input-readonly',
                'input': '//test-input'
            }
        }

        # Р’С‹Р·С‹РІР°РµРј РјРµС‚РѕРґ
        result = self.parser.get_current_pvz()

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ get_element_value Р±С‹Р» РІС‹Р·РІР°РЅ СЃ РїСЂР°РІРёР»СЊРЅС‹Рј СЃРµР»РµРєС‚РѕСЂРѕРј
        self.parser.get_element_value.assert_called_once_with(
            selector='//test-input-class',
            element_type="input"
        )
        self.assertEqual(result, 'TEST_PVZ_123')

    def test_get_current_pvz_fallback_selectors(self):
        """РўРµСЃС‚ РїРѕР»СѓС‡РµРЅРёСЏ РџР’Р— СЃ РёСЃРїРѕР»СЊР·РѕРІР°РЅРёРµРј fallback СЃРµР»РµРєС‚РѕСЂРѕРІ"""
        # РњРѕРєР°РµРј СЃРµР»РµРєС‚РѕСЂС‹
        self.parser.config['selectors'] = {
            'pvz_selectors': {
                'input_class_readonly': '//first-selector',  # Р­С‚РѕС‚ СЃРµР»РµРєС‚РѕСЂ Р±СѓРґРµС‚ РёСЃРїРѕР»СЊР·РѕРІР°РЅ РїРµСЂРІС‹Рј
                'input_readonly': '//second-selector',
                'input': '//third-selector'
            }
        }

        # РњРѕРєР°РµРј get_element_value, С‡С‚РѕР±С‹ РѕРЅ РІРѕР·РІСЂР°С‰Р°Р» Р·РЅР°С‡РµРЅРёРµ РґР»СЏ РїРµСЂРІРѕРіРѕ СЃРµР»РµРєС‚РѕСЂР°
        self.parser.get_element_value = Mock(return_value='TEST_PVZ_FALLBACK')

        # Р’С‹Р·С‹РІР°РµРј РјРµС‚РѕРґ
        result = self.parser.get_current_pvz()

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ get_element_value Р±С‹Р» РІС‹Р·РІР°РЅ РѕРґРёРЅ СЂР°Р· СЃ РїРµСЂРІС‹Рј СЃРµР»РµРєС‚РѕСЂРѕРј
        self.parser.get_element_value.assert_called_once_with(
            selector='//first-selector',
            element_type="input"
        )
        self.assertEqual(result, 'TEST_PVZ_FALLBACK')

    def test_get_current_pvz_no_selectors(self):
        """РўРµСЃС‚ РїРѕР»СѓС‡РµРЅРёСЏ РџР’Р— РїСЂРё РѕС‚СЃСѓС‚СЃС‚РІРёРё СЃРµР»РµРєС‚РѕСЂРѕРІ"""
        # РњРѕРєР°РµРј СЃРµР»РµРєС‚РѕСЂС‹ РєР°Рє РїСѓСЃС‚РѕР№ СЃР»РѕРІР°СЂСЊ
        self.parser.config['selectors'] = {
            'pvz_selectors': {}
        }

        # Р’С‹Р·С‹РІР°РµРј РјРµС‚РѕРґ
        result = self.parser.get_current_pvz()

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЂРµР·СѓР»СЊС‚Р°С‚ 'Unknown'
        self.assertEqual(result, 'Unknown')

    def test_get_current_pvz_exception_handling(self):
        """РўРµСЃС‚ РѕР±СЂР°Р±РѕС‚РєРё РёСЃРєР»СЋС‡РµРЅРёР№ РІ get_current_pvz"""
        # РњРѕРєР°РµРј РјРµС‚РѕРґ get_element_value, С‡С‚РѕР±С‹ РѕРЅ РІС‹Р±СЂР°СЃС‹РІР°Р» РёСЃРєР»СЋС‡РµРЅРёРµ
        self.parser.get_element_value = Mock(side_effect=Exception("Test exception"))

        # РњРѕРєР°РµРј СЃРµР»РµРєС‚РѕСЂС‹
        self.parser.config['selectors'] = {
            'pvz_selectors': {
                'input_class_readonly': '//test-input-class'
            }
        }

        # Р’С‹Р·С‹РІР°РµРј РјРµС‚РѕРґ
        result = self.parser.get_current_pvz()

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЂРµР·СѓР»СЊС‚Р°С‚ 'Unknown'
        self.assertEqual(result, 'Unknown')

    # РўРµСЃС‚С‹ РґР»СЏ РјРµС‚РѕРґР° set_pvz
    def test_set_pvz_success(self):
        """РўРµСЃС‚ СѓСЃРїРµС€РЅРѕР№ СѓСЃС‚Р°РЅРѕРІРєРё РџР’Р—"""
        # РњРѕРєР°РµРј РјРµС‚РѕРґ set_element_value
        self.parser.set_element_value = Mock(return_value=True)

        # РњРѕРєР°РµРј СЃРµР»РµРєС‚РѕСЂС‹
        self.parser.config['selectors'] = {
            'pvz_selectors': {
                'dropdown': '//test-dropdown',
                'option': '//test-option'
            }
        }

        # Р’С‹Р·С‹РІР°РµРј РјРµС‚РѕРґ
        result = self.parser.set_pvz('TEST_PVZ_NEW')

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ set_element_value Р±С‹Р» РІС‹Р·РІР°РЅ СЃ РїСЂР°РІРёР»СЊРЅС‹РјРё РїР°СЂР°РјРµС‚СЂР°РјРё
        self.parser.set_element_value.assert_called_once_with(
            selector='//test-dropdown',
            value='TEST_PVZ_NEW',
            element_type="dropdown",
            option_selector='//test-option'
        )
        self.assertTrue(result)

    def test_set_pvz_missing_selectors(self):
        """РўРµСЃС‚ СѓСЃС‚Р°РЅРѕРІРєРё РџР’Р— РїСЂРё РѕС‚СЃСѓС‚СЃС‚РІРёРё СЃРµР»РµРєС‚РѕСЂРѕРІ"""
        # РњРѕРєР°РµРј СЃРµР»РµРєС‚РѕСЂС‹ РєР°Рє РїСѓСЃС‚РѕР№ СЃР»РѕРІР°СЂСЊ
        self.parser.config['selectors'] = {
            'pvz_selectors': {}
        }

        # Р’С‹Р·С‹РІР°РµРј РјРµС‚РѕРґ
        result = self.parser.set_pvz('TEST_PVZ_NEW')

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЂРµР·СѓР»СЊС‚Р°С‚ False
        self.assertFalse(result)

    def test_set_pvz_exception_handling(self):
        """РўРµСЃС‚ РѕР±СЂР°Р±РѕС‚РєРё РёСЃРєР»СЋС‡РµРЅРёР№ РІ set_pvz"""
        # РњРѕРєР°РµРј РјРµС‚РѕРґ set_element_value, С‡С‚РѕР±С‹ РѕРЅ РІС‹Р±СЂР°СЃС‹РІР°Р» РёСЃРєР»СЋС‡РµРЅРёРµ
        self.parser.set_element_value = Mock(side_effect=Exception("Test exception"))

        # РњРѕРєР°РµРј СЃРµР»РµРєС‚РѕСЂС‹
        self.parser.config['selectors'] = {
            'pvz_selectors': {
                'dropdown': '//test-dropdown',
                'option': '//test-option'
            }
        }

        # Р’С‹Р·С‹РІР°РµРј РјРµС‚РѕРґ
        result = self.parser.set_pvz('TEST_PVZ_NEW')

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЂРµР·СѓР»СЊС‚Р°С‚ False
        self.assertFalse(result)

    # РўРµСЃС‚С‹ РґР»СЏ РјРµС‚РѕРґР° ensure_correct_pvz
    def test_ensure_correct_pvz_with_matching_pvz(self):
        """РўРµСЃС‚ ensure_correct_pvz РєРѕРіРґР° С‚СЂРµР±СѓРµРјС‹Р№ РџР’Р— СѓР¶Рµ СѓСЃС‚Р°РЅРѕРІР»РµРЅ"""
        # РњРѕРєР°РµРј РјРµС‚РѕРґС‹
        self.parser.get_current_pvz = Mock(return_value='TEST_PVZ')
        self.parser.config['additional_params'] = {'location_id': 'TEST_PVZ'}

        # Р’С‹Р·С‹РІР°РµРј РјРµС‚РѕРґ
        result = self.parser.ensure_correct_pvz()

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЂРµР·СѓР»СЊС‚Р°С‚ True
        self.assertTrue(result)

    def test_ensure_correct_pvz_with_different_pvz_success(self):
        """РўРµСЃС‚ ensure_correct_pvz РєРѕРіРґР° С‚СЂРµР±СѓРµРјС‹Р№ РџР’Р— РѕС‚Р»РёС‡Р°РµС‚СЃСЏ Рё СѓСЃС‚Р°РЅРѕРІРєР° РїСЂРѕС…РѕРґРёС‚ СѓСЃРїРµС€РЅРѕ"""
        # РњРѕРєР°РµРј РјРµС‚РѕРґС‹
        call_sequence = ['CURRENT_PVZ', 'REQUIRED_PVZ', 'REQUIRED_PVZ']  # РўРµРєСѓС‰РёР№ -> РџРѕСЃР»Рµ СѓСЃС‚Р°РЅРѕРІРєРё -> РџРѕСЃР»Рµ РЅР°РІРёРіР°С†РёРё
        call_index = 0

        def mock_get_current_pvz():
            nonlocal call_index
            value = call_sequence[call_index]
            call_index += 1
            return value

        self.parser.get_current_pvz = Mock(side_effect=mock_get_current_pvz)
        self.parser.set_pvz = Mock(return_value=True)
        self.parser.config['additional_params'] = {'location_id': 'REQUIRED_PVZ'}

        # РњРѕРєР°РµРј РјРµС‚РѕРґ navigate_to_target, С‡С‚РѕР±С‹ РѕРЅ РІРѕР·РІСЂР°С‰Р°Р» True
        from scheduler_runner.utils.parser.core.base_report_parser import BaseReportParser
        BaseReportParser.navigate_to_target = Mock(return_value=True)

        # Р’С‹Р·С‹РІР°РµРј РјРµС‚РѕРґ
        result = self.parser.ensure_correct_pvz()

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЂРµР·СѓР»СЊС‚Р°С‚ True Рё set_pvz РІС‹Р·С‹РІР°РµС‚СЃСЏ
        self.assertTrue(result)
        self.parser.set_pvz.assert_called_once_with('REQUIRED_PVZ')

        # Р’РѕСЃСЃС‚Р°РЅР°РІР»РёРІР°РµРј РѕСЂРёРіРёРЅР°Р»СЊРЅС‹Р№ РјРµС‚РѕРґ
        BaseReportParser.navigate_to_target = BaseReportParser.navigate_to_target

    def test_ensure_correct_pvz_with_different_pvz_failure(self):
        """РўРµСЃС‚ ensure_correct_pvz РєРѕРіРґР° С‚СЂРµР±СѓРµРјС‹Р№ РџР’Р— РѕС‚Р»РёС‡Р°РµС‚СЃСЏ Рё СѓСЃС‚Р°РЅРѕРІРєР° РЅРµ РїСЂРѕС…РѕРґРёС‚ СѓСЃРїРµС€РЅРѕ"""
        # РњРѕРєР°РµРј РјРµС‚РѕРґС‹
        self.parser.get_current_pvz = Mock(return_value='CURRENT_PVZ')
        self.parser.set_pvz = Mock(return_value=False)
        self.parser.config['additional_params'] = {'location_id': 'REQUIRED_PVZ'}

        # Р’С‹Р·С‹РІР°РµРј РјРµС‚РѕРґ
        result = self.parser.ensure_correct_pvz()

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЂРµР·СѓР»СЊС‚Р°С‚ False
        self.assertFalse(result)
        self.parser.set_pvz.assert_called_once_with('REQUIRED_PVZ')

    def test_ensure_correct_pvz_no_required_pvz_in_config(self):
        """РўРµСЃС‚ ensure_correct_pvz РїСЂРё РѕС‚СЃСѓС‚СЃС‚РІРёРё С‚СЂРµР±СѓРµРјРѕРіРѕ РџР’Р— РІ РєРѕРЅС„РёРіСѓСЂР°С†РёРё"""
        # РњРѕРєР°РµРј РјРµС‚РѕРґС‹
        self.parser.config['additional_params'] = {'location_id': ''}

        # Р’С‹Р·С‹РІР°РµРј РјРµС‚РѕРґ
        result = self.parser.ensure_correct_pvz()

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЂРµР·СѓР»СЊС‚Р°С‚ False
        self.assertFalse(result)

    def test_ensure_correct_pvz_exception_handling(self):
        """РўРµСЃС‚ РѕР±СЂР°Р±РѕС‚РєРё РёСЃРєР»СЋС‡РµРЅРёР№ РІ ensure_correct_pvz"""
        # РњРѕРєР°РµРј РјРµС‚РѕРґС‹, С‡С‚РѕР±С‹ РѕРЅРё РІС‹Р±СЂР°СЃС‹РІР°Р»Рё РёСЃРєР»СЋС‡РµРЅРёРµ
        self.parser.get_current_pvz = Mock(side_effect=Exception("Test exception"))
        self.parser.config['additional_params'] = {'location_id': 'TEST_PVZ'}

        # Р’С‹Р·С‹РІР°РµРј РјРµС‚РѕРґ
        result = self.parser.ensure_correct_pvz()

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЂРµР·СѓР»СЊС‚Р°С‚ False
        self.assertFalse(result)

    # РўРµСЃС‚С‹ РґР»СЏ РјРµС‚РѕРґР° navigate_to_target
    @patch('selenium.webdriver.Edge')
    def test_navigate_to_target_success(self, mock_driver):
        """РўРµСЃС‚ СѓСЃРїРµС€РЅРѕР№ РЅР°РІРёРіР°С†РёРё Рє С†РµР»РµРІРѕР№ СЃС‚СЂР°РЅРёС†Рµ"""
        # РњРѕРєР°РµРј РґСЂР°Р№РІРµСЂ
        self.parser.driver = mock_driver.return_value
        self.parser.driver.current_url = 'https://turbo-pvz.ozon.ru/reports/giveout'

        # РњРѕРєР°РµРј РјРµС‚РѕРґС‹
        self.parser.ensure_correct_pvz = Mock(return_value=True)
        self.parser.config['execution_date'] = '2026-01-15'
        self.parser.config['base_url'] = 'https://turbo-pvz.ozon.ru/reports/giveout'

        # РњРѕРєР°РµРј РјРµС‚РѕРґ driver.get, С‡С‚РѕР±С‹ РѕРЅ РЅРµ РїС‹С‚Р°Р»СЃСЏ РїРµСЂРµР№С‚Рё РЅР° СЂРµР°Р»СЊРЅС‹Р№ URL
        self.parser.driver.get = Mock()

        # РњРѕРєР°РµРј Р±Р°Р·РѕРІС‹Р№ РјРµС‚РѕРґ navigate_to_target, С‡С‚РѕР±С‹ РѕРЅ СѓСЃС‚Р°РЅР°РІР»РёРІР°Р» target_url
        original_navigate_to_target = self.parser.__class__.__bases__[0].navigate_to_target
        def mock_super_navigate_to_target(self):
            # РРјРёС‚РёСЂСѓРµРј Р»РѕРіРёРєСѓ Р±Р°Р·РѕРІРѕРіРѕ РјРµС‚РѕРґР°: С„РѕСЂРјРёСЂСѓРµРј target_url Рё СЃРѕС…СЂР°РЅСЏРµРј РІ РєРѕРЅС„РёРі
            base_url = self.config.get("base_url", "")
            execution_date = self.config.get('execution_date', None)

            if execution_date and base_url:
                # РџРѕР»СѓС‡Р°РµРј РѕР±С‰РёР№ С„РёР»СЊС‚СЂ РІ URL (РёР· РІСЃРїРѕРјРѕРіР°С‚РµР»СЊРЅРѕРіРѕ РјРµС‚РѕРґР° _build_url_filter)
                url_filter = self._build_url_filter()
                if url_filter:
                    # Р¤РѕСЂРјРёСЂСѓРµРј "target_url" СЃ С„РёР»СЊС‚СЂРѕРј, РїСЂРёРјРµРЅСЏСЏ РѕР±СЉРµРґРёРЅРµРЅРёРµ
                    target_url = base_url + url_filter
                else:
                    # Р•СЃР»Рё С„РёР»СЊС‚СЂ РЅРµ СѓРґР°Р»РѕСЃСЊ СЃС„РѕСЂРјРёСЂРѕРІР°С‚СЊ, РёСЃРїРѕР»СЊР·СѓРµРј Р±Р°Р·РѕРІС‹Р№ URL
                    target_url = base_url
            else:
                # Р•СЃР»Рё РґР°С‚С‹ РЅРµС‚ РёР»Рё Р±Р°Р·РѕРІРѕРіРѕ URL РЅРµС‚, РёСЃРїРѕР»СЊР·СѓРµРј Р±Р°Р·РѕРІС‹Р№ URL РєР°Рє РµСЃС‚СЊ
                target_url = base_url

            # РЎРѕС…СЂР°РЅСЏРµРј РїСЂР°РІРёР»СЊРЅС‹Р№ URL РІ РєРѕРЅС„РёРі РґР»СЏ РґР°Р»СЊРЅРµР№С€РµРіРѕ РёСЃРїРѕР»СЊР·РѕРІР°РЅРёСЏ
            self.config['target_url'] = target_url

            return True  # РРјРёС‚РёСЂСѓРµРј СѓСЃРїРµС€РЅСѓСЋ РЅР°РІРёРіР°С†РёСЋ

        # Р—Р°РјРµРЅСЏРµРј Р±Р°Р·РѕРІС‹Р№ РјРµС‚РѕРґ
        from scheduler_runner.utils.parser.core.base_report_parser import BaseReportParser
        BaseReportParser.navigate_to_target = mock_super_navigate_to_target

        # Р’С‹Р·С‹РІР°РµРј РјРµС‚РѕРґ
        result = self.parser.navigate_to_target()

        # Р’РѕСЃСЃС‚Р°РЅР°РІР»РёРІР°РµРј РѕСЂРёРіРёРЅР°Р»СЊРЅС‹Р№ РјРµС‚РѕРґ
        BaseReportParser.navigate_to_target = original_navigate_to_target

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЂРµР·СѓР»СЊС‚Р°С‚ True
        self.assertTrue(result)

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ ensure_correct_pvz Р±С‹Р» РІС‹Р·РІР°РЅ
        self.parser.ensure_correct_pvz.assert_called_once()

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ target_url Р±С‹Р» СѓСЃС‚Р°РЅРѕРІР»РµРЅ РІ РєРѕРЅС„РёРі
        self.assertIn('target_url', self.parser.config)

    @patch('selenium.webdriver.Edge')
    def test_navigate_to_target_ensure_pvz_failure(self, mock_driver):
        """РўРµСЃС‚ РЅР°РІРёРіР°С†РёРё Рє С†РµР»РµРІРѕР№ СЃС‚СЂР°РЅРёС†Рµ РїСЂРё РЅРµСѓРґР°С‡РЅРѕР№ РїСЂРѕРІРµСЂРєРµ РџР’Р—"""
        # РњРѕРєР°РµРј РґСЂР°Р№РІРµСЂ
        self.parser.driver = mock_driver.return_value
        self.parser.driver.current_url = 'https://turbo-pvz.ozon.ru/reports/giveout'

        # РњРѕРєР°РµРј РјРµС‚РѕРґС‹
        self.parser.ensure_correct_pvz = Mock(return_value=False)
        self.parser.config['execution_date'] = '2026-01-15'
        self.parser.config['base_url'] = 'https://turbo-pvz.ozon.ru/reports/giveout'

        # Р’С‹Р·С‹РІР°РµРј РјРµС‚РѕРґ
        result = self.parser.navigate_to_target()

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЂРµР·СѓР»СЊС‚Р°С‚ False
        self.assertFalse(result)

    # РўРµСЃС‚С‹ РґР»СЏ РјРµС‚РѕРґР° extract_report_data
    def test_extract_report_data_includes_location_info(self):
        """РўРµСЃС‚, С‡С‚Рѕ extract_report_data РІРєР»СЋС‡Р°РµС‚ РёРЅС„РѕСЂРјР°С†РёСЋ Рѕ РџР’Р—"""
        # РњРѕРєР°РµРј РјРµС‚РѕРґС‹
        self.parser.get_current_pvz = Mock(return_value='TEST_PVZ_LOCATION')
        self.parser._get_current_timestamp = Mock(return_value='2026-01-15 12:00:00')

        # РњРѕРєР°РµРј Р±Р°Р·РѕРІС‹Р№ РјРµС‚РѕРґ, С‡С‚РѕР±С‹ РѕРЅ РІРѕР·РІСЂР°С‰Р°Р» Р±Р°Р·РѕРІСѓСЋ СЃС‚СЂСѓРєС‚СѓСЂСѓ РґР°РЅРЅС‹С…
        original_extract = self.parser.__class__.__bases__[0].extract_report_data
        from scheduler_runner.utils.parser.core.base_report_parser import BaseReportParser
        original_method = BaseReportParser.extract_report_data
        BaseReportParser.extract_report_data = lambda self: {"base_field": "test_value"}

        try:
            # Р’С‹Р·С‹РІР°РµРј РјРµС‚РѕРґ
            result = self.parser.extract_report_data()

            # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЂРµР·СѓР»СЊС‚Р°С‚ СЃРѕРґРµСЂР¶РёС‚ location_info
            self.assertIn('location_info', result)
            self.assertEqual(result['location_info'], 'TEST_PVZ_LOCATION')
            self.assertIn('base_field', result)
            self.assertEqual(result['base_field'], 'test_value')
        finally:
            # Р’РѕСЃСЃС‚Р°РЅР°РІР»РёРІР°РµРј РѕСЂРёРіРёРЅР°Р»СЊРЅС‹Р№ РјРµС‚РѕРґ
            BaseReportParser.extract_report_data = original_method

    def test_extract_report_data_exception_handling(self):
        """РўРµСЃС‚ РѕР±СЂР°Р±РѕС‚РєРё РёСЃРєР»СЋС‡РµРЅРёР№ РІ extract_report_data"""
        # РњРѕРєР°РµРј РІСЃРµ РЅРµРѕР±С…РѕРґРёРјС‹Рµ РјРµС‚РѕРґС‹, С‡С‚РѕР±С‹ РёР·Р±РµР¶Р°С‚СЊ РёСЃРєР»СЋС‡РµРЅРёР№ РІ С†РµРїРѕС‡РєРµ РІС‹Р·РѕРІРѕРІ
        from scheduler_runner.utils.parser.core.base_report_parser import BaseReportParser
        original_method = BaseReportParser.extract_report_data

        # РњРѕРєР°РµРј Р±Р°Р·РѕРІС‹Р№ РјРµС‚РѕРґ, С‡С‚РѕР±С‹ РѕРЅ РІС‹Р±СЂР°СЃС‹РІР°Р» РёСЃРєР»СЋС‡РµРЅРёРµ
        def mock_base_extract_report_data(self):
            raise Exception("Test exception")

        BaseReportParser.extract_report_data = mock_base_extract_report_data

        try:
            # Р’С‹Р·С‹РІР°РµРј РјРµС‚РѕРґ
            result = self.parser.extract_report_data()

            # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЂРµР·СѓР»СЊС‚Р°С‚ СЃРѕРґРµСЂР¶РёС‚ РјРёРЅРёРјР°Р»СЊРЅРѕ РЅРµРѕР±С…РѕРґРёРјС‹Рµ РґР°РЅРЅС‹Рµ
            self.assertIn('location_info', result)
            self.assertIn('extraction_timestamp', result)
            self.assertIn('source_url', result)
        finally:
            # Р’РѕСЃСЃС‚Р°РЅР°РІР»РёРІР°РµРј РѕСЂРёРіРёРЅР°Р»СЊРЅС‹Р№ РјРµС‚РѕРґ
            BaseReportParser.extract_report_data = original_method

    # РўРµСЃС‚С‹ РґР»СЏ РјРµС‚РѕРґР° _get_current_timestamp
    def test_get_current_timestamp_format(self):
        """РўРµСЃС‚ С„РѕСЂРјР°С‚Р° РІСЂРµРјРµРЅРё РІ _get_current_timestamp"""
        import re
        from datetime import datetime
        
        # Р’С‹Р·С‹РІР°РµРј РјРµС‚РѕРґ
        result = self.parser._get_current_timestamp()

        # РџСЂРѕРІРµСЂСЏРµРј С„РѕСЂРјР°С‚ РІСЂРµРјРµРЅРё (YYYY-MM-DD HH:MM:SS)
        pattern = r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
        self.assertRegex(result, pattern)

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ РІСЂРµРјСЏ Р±Р»РёР·РєРѕ Рє С‚РµРєСѓС‰РµРјСѓ (РІ РїСЂРµРґРµР»Р°С… 1 РјРёРЅСѓС‚С‹)
        current_time = datetime.now()
        parsed_time = datetime.strptime(result, '%Y-%m-%d %H:%M:%S')
        
        time_diff = abs((current_time - parsed_time).total_seconds())
        self.assertLessEqual(time_diff, 60)  # Р Р°Р·РЅРёС†Р° РґРѕР»Р¶РЅР° Р±С‹С‚СЊ РјРµРЅРµРµ 1 РјРёРЅСѓС‚С‹

    def test_get_current_timestamp_custom_format(self):
        """РўРµСЃС‚ _get_current_timestamp СЃ РїРѕР»СЊР·РѕРІР°С‚РµР»СЊСЃРєРёРј С„РѕСЂРјР°С‚РѕРј"""
        # РЈСЃС‚Р°РЅР°РІР»РёРІР°РµРј РєР°СЃС‚РѕРјРЅС‹Р№ С„РѕСЂРјР°С‚ РґР°С‚С‹-РІСЂРµРјРµРЅРё
        self.parser.config['datetime_format'] = '%Y/%m/%d %H.%M.%S'

        # Р’С‹Р·С‹РІР°РµРј РјРµС‚РѕРґ
        result = self.parser._get_current_timestamp()

        # РџСЂРѕРІРµСЂСЏРµРј С„РѕСЂРјР°С‚ РІСЂРµРјРµРЅРё (YYYY/MM/DD HH.MM.SS)
        import re
        pattern = r'^\d{4}/\d{2}/\d{2} \d{2}\.\d{2}\.\d{2}$'
        self.assertRegex(result, pattern)


if __name__ == '__main__':
    unittest.main()
