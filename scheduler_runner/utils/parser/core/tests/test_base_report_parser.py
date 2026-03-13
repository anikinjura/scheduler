"""
РџРѕР»РЅС‹Рµ С‚РµСЃС‚С‹ РґР»СЏ BaseReportParser СЃ РёСЃРїРѕР»СЊР·РѕРІР°РЅРёРµРј РЅРѕРІРѕР№ РєРѕРЅС„РёРіСѓСЂР°С†РёРё Рё СЃ СѓС‡РµС‚РѕРј РёР·РјРµРЅРµРЅРёР№ РІ РІРµСЂСЃРёРё 3.0.0

Р’ РІРµСЂСЃРёРё 3.0.0 Р±С‹Р»Рё РІРЅРµСЃРµРЅС‹ РёР·РјРµРЅРµРЅРёСЏ:
- РћР±РЅРѕРІР»РµРЅС‹ СЃСЃС‹Р»РєРё РЅР° РјРµС‚РѕРґС‹ BaseParser СЃ СѓС‡РµС‚РѕРј РїРµСЂРµРёРјРµРЅРѕРІР°РЅРёСЏ select_option_from_dropdown РІ _select_option_from_dropdown
- Р”РѕР±Р°РІР»РµРЅР° РїРѕРґРґРµСЂР¶РєР° РјСѓР»СЊС‚Рё-С€Р°РіРѕРІРѕР№ РѕР±СЂР°Р±РѕС‚РєРё СЃ РЅРѕРІС‹РјРё РјРµС‚РѕРґР°РјРё
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
import json
import xml.etree.ElementTree as ET
import sys
from scheduler_runner.utils.parser.core.base_report_parser import BaseReportParser
from scheduler_runner.utils.parser.core.contracts import ParserJob, ParserJobResult, ParserRuntimeContext, ReportDefinition
from scheduler_runner.utils.parser.configs.base_configs.base_report_config import BASE_REPORT_CONFIG


class TestConcreteReportParser(BaseReportParser):
    """РўРµСЃС‚РѕРІС‹Р№ РґРѕС‡РµСЂРЅРёР№ РєР»Р°СЃСЃ РґР»СЏ С‚РµСЃС‚РёСЂРѕРІР°РЅРёСЏ BaseReportParser"""

    def __init__(self, config, args=None, logger=None):
        """РРЅРёС†РёР°Р»РёР·Р°С†РёСЏ СЃ РІРѕР·РјРѕР¶РЅРѕСЃС‚СЊСЋ РїРµСЂРµРґР°С‡Рё Р»РѕРіРіРµСЂР°"""
        # РЎРѕР·РґР°РµРј С„РёРєС‚РёРІРЅС‹Р№ Р»РѕРіРіРµСЂ СЃ РјРµС‚РѕРґРѕРј trace, РµСЃР»Рё РѕРЅ РЅРµ РїРµСЂРµРґР°РЅ
        if logger is None:
            import logging
            logger = logging.getLogger(self.__class__.__name__)
            logger.setLevel(logging.DEBUG)
            # Р”РѕР±Р°РІР»СЏРµРј РѕР±СЂР°Р±РѕС‚С‡РёРє, С‡С‚РѕР±С‹ РёР·Р±РµР¶Р°С‚СЊ РїСЂРµРґСѓРїСЂРµР¶РґРµРЅРёР№
            if not logger.handlers:
                handler = logging.NullHandler()
                logger.addHandler(handler)
            
            # Р”РѕР±Р°РІР»СЏРµРј РјРµС‚РѕРґ trace Рє Р»РѕРіРіРµСЂСѓ
            def trace(msg, *args, **kwargs):
                logger.debug(msg, *args, **kwargs)
            logger.trace = trace
        
        super().__init__(config, args=args, logger=logger)

    def get_report_type(self) -> str:
        """Р РµР°Р»РёР·Р°С†РёСЏ Р°Р±СЃС‚СЂР°РєС‚РЅРѕРіРѕ РјРµС‚РѕРґР° get_report_type"""
        return "test_report"

    def get_report_schema(self) -> dict:
        """Р РµР°Р»РёР·Р°С†РёСЏ Р°Р±СЃС‚СЂР°РєС‚РЅРѕРіРѕ РјРµС‚РѕРґР° get_report_schema"""
        return {
            "required_fields": ["field1", "field2"],
            "field_types": {
                "field1": "string",
                "field2": "integer",
                "field3": "list"
            }
        }

    def extract_report_data(self) -> dict:
        """Р РµР°Р»РёР·Р°С†РёСЏ Р°Р±СЃС‚СЂР°РєС‚РЅРѕРіРѕ РјРµС‚РѕРґР° extract_report_data"""
        return {
            "field1": "test_value",
            "field2": 123,
            "field3": [1, 2, 3]
        }

    def login(self) -> bool:
        """Р РµР°Р»РёР·Р°С†РёСЏ Р°Р±СЃС‚СЂР°РєС‚РЅРѕРіРѕ РјРµС‚РѕРґР° login"""
        return True

    def navigate_to_target(self) -> bool:
        """Р РµР°Р»РёР·Р°С†РёСЏ Р°Р±СЃС‚СЂР°РєС‚РЅРѕРіРѕ РјРµС‚РѕРґР° navigate_to_target"""
        return True

    def logout(self) -> bool:
        """Р РµР°Р»РёР·Р°С†РёСЏ Р°Р±СЃС‚СЂР°РєС‚РЅРѕРіРѕ РјРµС‚РѕРґР° logout"""
        return True


class TestBaseReportParser(unittest.TestCase):
    """РўРµСЃС‚С‹ РґР»СЏ BaseReportParser"""

    def setUp(self):
        """РќР°СЃС‚СЂРѕР№РєР° С‚РµСЃС‚Р°"""
        self.config = BASE_REPORT_CONFIG.copy()
        self.parser = TestConcreteReportParser(self.config)

    def test_format_report_output_json(self):
        """РўРµСЃС‚ С„РѕСЂРјР°С‚РёСЂРѕРІР°РЅРёСЏ РѕС‚С‡РµС‚Р° РІ JSON"""
        data = {"test": "data", "number": 123}
        result = self.parser.format_report_output(data, 'json')

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЂРµР·СѓР»СЊС‚Р°С‚ - РІР°Р»РёРґРЅС‹Р№ JSON
        parsed = json.loads(result)
        self.assertEqual(parsed["test"], "data")
        self.assertEqual(parsed["number"], 123)

    def test_format_report_output_xml_with_custom_root(self):
        """РўРµСЃС‚ С„РѕСЂРјР°С‚РёСЂРѕРІР°РЅРёСЏ РѕС‚С‡РµС‚Р° РІ XML СЃ РїРѕР»СЊР·РѕРІР°С‚РµР»СЊСЃРєРёРј РєРѕСЂРЅРµРІС‹Рј СЌР»РµРјРµРЅС‚РѕРј"""
        # РР·РјРµРЅСЏРµРј РєРѕРЅС„РёРі РґР»СЏ РёСЃРїРѕР»СЊР·РѕРІР°РЅРёСЏ РїРѕР»СЊР·РѕРІР°С‚РµР»СЊСЃРєРѕРіРѕ РєРѕСЂРЅРµРІРѕРіРѕ СЌР»РµРјРµРЅС‚Р°
        self.config['xml_root_element'] = 'custom_report'
        parser = TestConcreteReportParser(self.config)

        data = {"field1": "value1", "field2": [1, 2, 3]}
        result = parser.format_report_output(data, 'xml')

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ РєРѕСЂРЅРµРІРѕР№ СЌР»РµРјРµРЅС‚ - РЅР°С€ РєР°СЃС‚РѕРјРЅС‹Р№
        root = ET.fromstring(result)
        self.assertEqual(root.tag, 'custom_report')

    def test_format_report_output_xml_with_custom_item_prefix(self):
        """РўРµСЃС‚ С„РѕСЂРјР°С‚РёСЂРѕРІР°РЅРёСЏ РѕС‚С‡РµС‚Р° РІ XML СЃ РїРѕР»СЊР·РѕРІР°С‚РµР»СЊСЃРєРёРј РїСЂРµС„РёРєСЃРѕРј СЌР»РµРјРµРЅС‚РѕРІ СЃРїРёСЃРєР°"""
        # РР·РјРµРЅСЏРµРј РєРѕРЅС„РёРі РґР»СЏ РёСЃРїРѕР»СЊР·РѕРІР°РЅРёСЏ РїРѕР»СЊР·РѕРІР°С‚РµР»СЊСЃРєРѕРіРѕ РїСЂРµС„РёРєСЃР°
        self.config['xml_item_prefix'] = 'custom_item_'
        parser = TestConcreteReportParser(self.config)

        data = {"field1": [1, 2, 3]}
        result = parser.format_report_output(data, 'xml')

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЌР»РµРјРµРЅС‚С‹ СЃРїРёСЃРєР° РёРјРµСЋС‚ РЅР°С€ РєР°СЃС‚РѕРјРЅС‹Р№ РїСЂРµС„РёРєСЃ
        root = ET.fromstring(result)
        list_items = root.find('field1')
        if list_items is not None:
            # РџСЂРѕРІРµСЂСЏРµРј РЅР°Р»РёС‡РёРµ СЌР»РµРјРµРЅС‚РѕРІ СЃ РєР°СЃС‚РѕРјРЅС‹Рј РїСЂРµС„РёРєСЃРѕРј
            children_tags = [child.tag for child in list_items]
            self.assertTrue(any(tag.startswith('custom_item_') for tag in children_tags))

    def test_save_report_with_custom_filename_template(self):
        """РўРµСЃС‚ СЃРѕС…СЂР°РЅРµРЅРёСЏ РѕС‚С‡РµС‚Р° СЃ РїРѕР»СЊР·РѕРІР°С‚РµР»СЊСЃРєРёРј С€Р°Р±Р»РѕРЅРѕРј РёРјРµРЅРё С„Р°Р№Р»Р°"""
        # РР·РјРµРЅСЏРµРј РєРѕРЅС„РёРі РґР»СЏ РёСЃРїРѕР»СЊР·РѕРІР°РЅРёСЏ РїРѕР»СЊР·РѕРІР°С‚РµР»СЊСЃРєРѕРіРѕ С€Р°Р±Р»РѕРЅР°
        self.config['filename_template'] = "custom_{report_type}_{timestamp}_final.{output_format}"
        self.config['execution_date'] = '2023-12-01'
        parser = TestConcreteReportParser(self.config)

        # РњРѕРєР°РµРј open, С‡С‚РѕР±С‹ РЅРµ СЃРѕР·РґР°РІР°С‚СЊ СЂРµР°Р»СЊРЅС‹Р№ С„Р°Р№Р»
        with patch('builtins.open', unittest.mock.mock_open()) as mock_file:
            result = parser.save_report({"test": "data"}, output_format='json')

            # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ open Р±С‹Р» РІС‹Р·РІР°РЅ СЃ РїСЂР°РІРёР»СЊРЅС‹Рј РёРјРµРЅРµРј С„Р°Р№Р»Р°
            mock_file.assert_called_once()
            args, kwargs = mock_file.call_args
            filename = args[0]

            # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ РёРјСЏ С„Р°Р№Р»Р° СЃРѕРѕС‚РІРµС‚СЃС‚РІСѓРµС‚ С€Р°Р±Р»РѕРЅСѓ
            self.assertIn('custom_test_report_20231201_final.json', filename)

    def test_get_report_schema_with_custom_keys(self):
        """РўРµСЃС‚ РїРѕР»СѓС‡РµРЅРёСЏ СЃС…РµРјС‹ РѕС‚С‡РµС‚Р° СЃ РїРѕР»СЊР·РѕРІР°С‚РµР»СЊСЃРєРёРјРё РєР»СЋС‡Р°РјРё"""
        # РЎРѕР·РґР°РµРј С‚РµСЃС‚РѕРІС‹Р№ РїР°СЂСЃРµСЂ СЃ РєР°СЃС‚РѕРјРЅС‹РјРё РєР»СЋС‡Р°РјРё СЃС…РµРјС‹
        custom_config = BASE_REPORT_CONFIG.copy()
        custom_config['schema_keys'] = {
            'required_fields': 'mandatory_fields',
            'field_types': 'data_types'
        }

        class CustomSchemaParser(TestConcreteReportParser):
            def get_report_schema(self) -> dict:
                return {
                    "mandatory_fields": ["field1"],  # РСЃРїРѕР»СЊР·СѓРµРј РєР°СЃС‚РѕРјРЅС‹Р№ РєР»СЋС‡
                    "data_types": {  # РСЃРїРѕР»СЊР·СѓРµРј РєР°СЃС‚РѕРјРЅС‹Р№ РєР»СЋС‡
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

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЃС…РµРјР° РІРѕР·РІСЂР°С‰Р°РµС‚ РїСЂР°РІРёР»СЊРЅС‹Рµ РґР°РЅРЅС‹Рµ
        schema = parser.get_report_schema()
        self.assertIn("mandatory_fields", schema)
        self.assertIn("data_types", schema)
        self.assertEqual(schema["mandatory_fields"], ["field1"])

    def test_report_schema_with_custom_supported_types(self):
        """РўРµСЃС‚ СЃС…РµРјС‹ РѕС‚С‡РµС‚Р° СЃ РїРѕР»СЊР·РѕРІР°С‚РµР»СЊСЃРєРёРјРё РїРѕРґРґРµСЂР¶РёРІР°РµРјС‹РјРё С‚РёРїР°РјРё"""
        # Р”РѕР±Р°РІР»СЏРµРј РєР°СЃС‚РѕРјРЅС‹Р№ С‚РёРї РІ РєРѕРЅС„РёРі
        custom_config = BASE_REPORT_CONFIG.copy()
        custom_config['supported_field_types']['email'] = str  # Р”Р»СЏ РїСЂРёРјРµСЂР°, email РєР°Рє СЃС‚СЂРѕРєР°

        class EmailSchemaParser(TestConcreteReportParser):
            def get_report_schema(self) -> dict:
                return {
                    "required_fields": ["email_field"],
                    "field_types": {
                        "email_field": "email"  # РСЃРїРѕР»СЊР·СѓРµРј РєР°СЃС‚РѕРјРЅС‹Р№ С‚РёРї
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

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЃС…РµРјР° РІРѕР·РІСЂР°С‰Р°РµС‚ РїСЂР°РІРёР»СЊРЅС‹Рµ РґР°РЅРЅС‹Рµ
        schema = parser.get_report_schema()
        self.assertIn("required_fields", schema)
        self.assertIn("field_types", schema)
        self.assertEqual(schema["field_types"]["email_field"], "email")

    def test_save_report_with_custom_encoding(self):
        """РўРµСЃС‚ СЃРѕС…СЂР°РЅРµРЅРёСЏ РѕС‚С‡РµС‚Р° СЃ РїРѕР»СЊР·РѕРІР°С‚РµР»СЊСЃРєРѕР№ РєРѕРґРёСЂРѕРІРєРѕР№"""
        # РР·РјРµРЅСЏРµРј РєРѕРЅС„РёРі РґР»СЏ РёСЃРїРѕР»СЊР·РѕРІР°РЅРёСЏ РїРѕР»СЊР·РѕРІР°С‚РµР»СЊСЃРєРѕР№ РєРѕРґРёСЂРѕРІРєРё
        self.config['output_config']['encoding'] = 'utf-16'
        parser = TestConcreteReportParser(self.config)

        # РњРѕРєР°РµРј open, С‡С‚РѕР±С‹ РїСЂРѕРІРµСЂРёС‚СЊ, С‡С‚Рѕ РєРѕРґРёСЂРѕРІРєР° РїРµСЂРµРґР°РµС‚СЃСЏ РїСЂР°РІРёР»СЊРЅРѕ
        with patch('builtins.open', unittest.mock.mock_open()) as mock_file:
            result = parser.save_report({"test": "РґР°РЅРЅС‹Рµ"}, output_format='json')

            # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ open Р±С‹Р» РІС‹Р·РІР°РЅ СЃ РїСЂР°РІРёР»СЊРЅРѕР№ РєРѕРґРёСЂРѕРІРєРѕР№
            mock_file.assert_called_once()
            args, kwargs = mock_file.call_args

            # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ РєРѕРґРёСЂРѕРІРєР° - С‚Р°, С‡С‚Рѕ РјС‹ СѓРєР°Р·Р°Р»Рё
            self.assertEqual(kwargs.get('encoding'), 'utf-16')

    def test_extract_data_basic(self):
        """РўРµСЃС‚ Р±Р°Р·РѕРІРѕРіРѕ РёР·РІР»РµС‡РµРЅРёСЏ РґР°РЅРЅС‹С…"""
        # РўРµСЃС‚РёСЂСѓРµРј РІС‹Р·РѕРІ РјРµС‚РѕРґР° РёР·РІР»РµС‡РµРЅРёСЏ РґР°РЅРЅС‹С…
        parser = TestConcreteReportParser(self.config)

        # Р’С‹Р·С‹РІР°РµРј РјРµС‚РѕРґ РёР·РІР»РµС‡РµРЅРёСЏ РґР°РЅРЅС‹С…
        result = parser.extract_data()

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ РјРµС‚РѕРґ РЅРµ РІС‹Р·С‹РІР°РµС‚ РѕС€РёР±РѕРє (РІРѕР·РІСЂР°С‰Р°РµРјРѕРµ Р·РЅР°С‡РµРЅРёРµ РјРѕР¶РµС‚ Р±С‹С‚СЊ Р»СЋР±С‹Рј)
        # Р’Р°Р¶РЅРѕ, С‡С‚РѕР±С‹ РјРµС‚РѕРґ РЅРµ РІС‹Р±СЂР°СЃС‹РІР°Р» РёСЃРєР»СЋС‡РµРЅРёРµ
        self.assertIsNotNone(parser)

    def test_run_parser_with_strict_validation(self):
        """РўРµСЃС‚ Р·Р°РїСѓСЃРєР° РїР°СЂСЃРµСЂР° СЃРѕ СЃС‚СЂРѕРіРѕР№ РІР°Р»РёРґР°С†РёРµР№"""
        # Р’РєР»СЋС‡Р°РµРј СЃС‚СЂРѕРіРёР№ СЂРµР¶РёРј РІР°Р»РёРґР°С†РёРё
        self.config['validation_config']['strict_mode'] = True
        parser = TestConcreteReportParser(self.config)

        # РњРѕРєР°РµРј РјРµС‚РѕРґС‹
        parser.setup_browser = Mock(return_value=True)
        parser.login = Mock(return_value=True)
        parser.navigate_to_target = Mock(return_value=True)
        parser.extract_data = Mock(return_value={"wrong_field": "value"})
        parser.validate_report_data = Mock(return_value=False)  # Р’Р°Р»РёРґР°С†РёСЏ РЅРµ РїСЂРѕС…РѕРґРёС‚
        parser.logout = Mock(return_value=True)
        parser.close_browser = Mock()
        parser.save_report = Mock()

        # Р’ СЃС‚СЂРѕРіРѕРј СЂРµР¶РёРјРµ РїСЂРё РїСЂРѕРІР°Р»РµРЅРЅРѕР№ РІР°Р»РёРґР°С†РёРё РґРѕР»Р¶РµРЅ РІС‹Р±СЂРѕСЃРёС‚СЊСЃСЏ exception
        with self.assertRaises(Exception) as context:
            parser.run_parser(save_to_file=True)

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЃРѕРѕР±С‰РµРЅРёРµ РѕР± РѕС€РёР±РєРµ СЃРѕРґРµСЂР¶РёС‚ РёРЅС„РѕСЂРјР°С†РёСЋ Рѕ РїСЂРёС‡РёРЅРµ
        self.assertTrue(len(str(context.exception)) > 0)

    def test_parse_arguments_with_report_date(self):
        """РўРµСЃС‚ СЂР°Р·Р±РѕСЂР° Р°СЂРіСѓРјРµРЅС‚РѕРІ РєРѕРјР°РЅРґРЅРѕР№ СЃС‚СЂРѕРєРё СЃ РґР°С‚РѕР№ РѕС‚С‡РµС‚Р°"""
        # РўРµСЃС‚РёСЂСѓРµРј СЂР°Р·Р±РѕСЂ Р°СЂРіСѓРјРµРЅС‚РѕРІ СЃ РґР°С‚РѕР№ РѕС‚С‡РµС‚Р°
        test_args = ['--report_date', '2023-12-25']
        
        # РЎРѕР·РґР°РµРј РЅРѕРІС‹Р№ СЌРєР·РµРјРїР»СЏСЂ РїР°СЂСЃРµСЂР° СЃ СЌС‚РёРјРё Р°СЂРіСѓРјРµРЅС‚Р°РјРё
        parser = TestConcreteReportParser(self.config, args=test_args)
        
        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ РґР°С‚Р° РѕС‚С‡РµС‚Р° Р±С‹Р»Р° РїСЂР°РІРёР»СЊРЅРѕ СЂР°Р·РѕР±СЂР°РЅР°
        self.assertEqual(parser.args.report_date, '2023-12-25')

    def test_parse_arguments_without_report_date(self):
        """РўРµСЃС‚ СЂР°Р·Р±РѕСЂР° Р°СЂРіСѓРјРµРЅС‚РѕРІ РєРѕРјР°РЅРґРЅРѕР№ СЃС‚СЂРѕРєРё Р±РµР· РґР°С‚С‹ РѕС‚С‡РµС‚Р°"""
        # РўРµСЃС‚РёСЂСѓРµРј СЂР°Р·Р±РѕСЂ Р°СЂРіСѓРјРµРЅС‚РѕРІ Р±РµР· РґР°С‚С‹ РѕС‚С‡РµС‚Р°
        test_args = []  # РџСѓСЃС‚РѕР№ СЃРїРёСЃРѕРє Р°СЂРіСѓРјРµРЅС‚РѕРІ
        
        # РЎРѕР·РґР°РµРј РЅРѕРІС‹Р№ СЌРєР·РµРјРїР»СЏСЂ РїР°СЂСЃРµСЂР° СЃ СЌС‚РёРјРё Р°СЂРіСѓРјРµРЅС‚Р°РјРё
        parser = TestConcreteReportParser(self.config, args=test_args)
        
        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ РґР°С‚Р° РѕС‚С‡РµС‚Р° РЅРµ СѓСЃС‚Р°РЅРѕРІР»РµРЅР°
        self.assertIsNone(parser.args.report_date)

    def test_update_execution_date_from_args(self):
        """РўРµСЃС‚ РѕР±РЅРѕРІР»РµРЅРёСЏ РґР°С‚С‹ РІС‹РїРѕР»РЅРµРЅРёСЏ РёР· Р°СЂРіСѓРјРµРЅС‚РѕРІ РєРѕРјР°РЅРґРЅРѕР№ СЃС‚СЂРѕРєРё"""
        # РЈСЃС‚Р°РЅР°РІР»РёРІР°РµРј РґР°С‚Сѓ РІ Р°СЂРіСѓРјРµРЅС‚Р°С…
        test_args = ['--report_date', '2023-12-25']
        parser = TestConcreteReportParser(self.config, args=test_args)
        
        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ РґР°С‚Р° РІС‹РїРѕР»РЅРµРЅРёСЏ РѕР±РЅРѕРІРёР»Р°СЃСЊ РёР· Р°СЂРіСѓРјРµРЅС‚РѕРІ
        self.assertEqual(parser.config['execution_date'], '2023-12-25')

    def test_update_execution_date_from_config(self):
        """РўРµСЃС‚ РѕР±РЅРѕРІР»РµРЅРёСЏ РґР°С‚С‹ РІС‹РїРѕР»РЅРµРЅРёСЏ РёР· РєРѕРЅС„РёРіСѓСЂР°С†РёРё"""
        # РЈСЃС‚Р°РЅР°РІР»РёРІР°РµРј РґР°С‚Сѓ РІ РєРѕРЅС„РёРіРµ
        self.config['execution_date'] = '2023-11-30'
        parser = TestConcreteReportParser(self.config)
        
        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ РґР°С‚Р° РІС‹РїРѕР»РЅРµРЅРёСЏ РІР·СЏС‚Р° РёР· РєРѕРЅС„РёРіР°
        self.assertEqual(parser.config['execution_date'], '2023-11-30')

    def test_update_execution_date_from_current_time(self):
        """РўРµСЃС‚ РѕР±РЅРѕРІР»РµРЅРёСЏ РґР°С‚С‹ РІС‹РїРѕР»РЅРµРЅРёСЏ РёР· С‚РµРєСѓС‰РµРіРѕ РІСЂРµРјРµРЅРё"""
        from datetime import datetime
        
        # РЈРґР°Р»СЏРµРј РґР°С‚Сѓ РёР· РєРѕРЅС„РёРіР° Рё РЅРµ РїРµСЂРµРґР°РµРј Р°СЂРіСѓРјРµРЅС‚С‹
        if 'execution_date' in self.config:
            del self.config['execution_date']
            
        parser = TestConcreteReportParser(self.config, args=[])
        
        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ РґР°С‚Р° РІС‹РїРѕР»РЅРµРЅРёСЏ СѓСЃС‚Р°РЅРѕРІР»РµРЅР° РІ С‚РµРєСѓС‰СѓСЋ РґР°С‚Сѓ
        today = datetime.now().strftime('%Y-%m-%d')
        self.assertEqual(parser.config['execution_date'], today)

    def test_build_url_filter_with_date_and_data_type(self):
        """РўРµСЃС‚ РїРѕСЃС‚СЂРѕРµРЅРёСЏ С„РёР»СЊС‚СЂР° URL СЃ РґР°С‚РѕР№ Рё С‚РёРїРѕРј РґР°РЅРЅС‹С…"""
        # РЈСЃС‚Р°РЅР°РІР»РёРІР°РµРј РїР°СЂР°РјРµС‚СЂС‹ С„РёР»СЊС‚СЂР°С†РёРё РІ РєРѕРЅС„РёРіРµ
        self.config['filter_template'] = '?filter={{{date_filter_template},{data_type_filter_template}}}'
        self.config['date_filter_template'] = '"startDate":"{date}T00:00%2B03:00","endDate":"{date}T23:59%2B03:00"'
        self.config['data_type_filter_template'] = '"operationTypes":["GiveoutAll"]'
        self.config['execution_date'] = '2023-12-25'
        
        parser = TestConcreteReportParser(self.config)
        filter_str = parser._build_url_filter()
        
        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ С„РёР»СЊС‚СЂ СЃРѕРґРµСЂР¶РёС‚ РґР°С‚Сѓ Рё С‚РёРї РґР°РЅРЅС‹С…
        self.assertIn('2023-12-25', filter_str)
        self.assertIn('operationTypes', filter_str)

    def test_build_url_filter_without_date(self):
        """РўРµСЃС‚ РїРѕСЃС‚СЂРѕРµРЅРёСЏ С„РёР»СЊС‚СЂР° URL Р±РµР· РґР°С‚С‹"""
        # РЈСЃС‚Р°РЅР°РІР»РёРІР°РµРј РїР°СЂР°РјРµС‚СЂС‹ С„РёР»СЊС‚СЂР°С†РёРё Р±РµР· РґР°С‚С‹
        self.config['filter_template'] = '?filter={{{data_type_filter_template}}}'
        self.config['data_type_filter_template'] = '"operationTypes":["GiveoutAll"]'
        self.config['execution_date'] = ''
        
        parser = TestConcreteReportParser(self.config)
        filter_str = parser._build_url_filter()
        
        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ С„РёР»СЊС‚СЂ СЃРѕРґРµСЂР¶РёС‚ С‚РѕР»СЊРєРѕ С‚РёРї РґР°РЅРЅС‹С…
        self.assertIn('operationTypes', filter_str)
        self.assertNotIn('{date_filter_template}', filter_str)

    def test_execute_single_step_simple_processing(self):
        """РўРµСЃС‚ РІС‹РїРѕР»РЅРµРЅРёСЏ РѕРґРЅРѕРіРѕ С€Р°РіР° СЃ РїСЂРѕСЃС‚РѕР№ РѕР±СЂР°Р±РѕС‚РєРѕР№"""
        # РЎРѕР·РґР°РµРј РєРѕРЅС„РёРіСѓСЂР°С†РёСЋ С€Р°РіР° СЃ РїСЂРѕСЃС‚РѕР№ РѕР±СЂР°Р±РѕС‚РєРѕР№
        step_config = {
            "processing_type": "simple",
            "data_extraction": {
                "selector": "//div[@id='test']/span",
                "element_type": "div"
            },
            "result_key": "test_result"
        }
        
        parser = TestConcreteReportParser(self.config)
        
        # РњРѕРєР°РµРј РЅРµРѕР±С…РѕРґРёРјС‹Рµ РјРµС‚РѕРґС‹
        parser._update_config_for_step = Mock(return_value=self.config.copy())
        parser.navigate_to_target = Mock(return_value=True)
        parser._handle_simple_extraction = Mock(return_value="test_value")
        
        result = parser._execute_single_step(step_config)
        
        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ Р±С‹Р»Рё РІС‹Р·РІР°РЅС‹ РЅСѓР¶РЅС‹Рµ РјРµС‚РѕРґС‹
        parser._handle_simple_extraction.assert_called_once_with(step_config)
        self.assertIn('__STEP_SOURCE_URL__', result)

    def test_update_config_for_step(self):
        """РўРµСЃС‚ РѕР±РЅРѕРІР»РµРЅРёСЏ РєРѕРЅС„РёРіСѓСЂР°С†РёРё РґР»СЏ С€Р°РіР°"""
        # РЎРѕР·РґР°РµРј РєРѕРЅС„РёРіСѓСЂР°С†РёСЋ С€Р°РіР°
        step_config = {
            "base_url": "https://example.com",
            "filter_template": "?filter=test",
            "processing_type": "simple",  # Р­С‚Рѕ СЃР»СѓР¶РµР±РЅС‹Р№ РєР»СЋС‡, РЅРµ РґРѕР»Р¶РµРЅ РѕР±РЅРѕРІР»СЏС‚СЊСЃСЏ
            "data_extraction": {},       # Р­С‚Рѕ СЃР»СѓР¶РµР±РЅС‹Р№ РєР»СЋС‡, РЅРµ РґРѕР»Р¶РµРЅ РѕР±РЅРѕРІР»СЏС‚СЊСЃСЏ
        }
        
        parser = TestConcreteReportParser(self.config)
        
        # РЎРѕС…СЂР°РЅСЏРµРј РѕСЂРёРіРёРЅР°Р»СЊРЅСѓСЋ РєРѕРЅС„РёРіСѓСЂР°С†РёСЋ
        original_config = parser.config.copy()
        
        # РћР±РЅРѕРІР»СЏРµРј РєРѕРЅС„РёРіСѓСЂР°С†РёСЋ РґР»СЏ С€Р°РіР°
        original = parser._update_config_for_step(step_config)
        
        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЃР»СѓР¶РµР±РЅС‹Рµ РєР»СЋС‡Рё РЅРµ Р±С‹Р»Рё РґРѕР±Р°РІР»РµРЅС‹ РІ РєРѕРЅС„РёРі
        self.assertEqual(parser.config["base_url"], "https://example.com")
        self.assertEqual(parser.config["filter_template"], "?filter=test")
        self.assertNotIn("processing_type", parser.config)
        self.assertNotIn("data_extraction", parser.config)
        
        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ РѕСЂРёРіРёРЅР°Р»СЊРЅР°СЏ РєРѕРЅС„РёРіСѓСЂР°С†РёСЏ РІРѕР·РІСЂР°С‰РµРЅР°
        self.assertEqual(original, original_config)

    def test_handle_simple_extraction(self):
        """РўРµСЃС‚ РѕР±СЂР°Р±РѕС‚РєРё РїСЂРѕСЃС‚РѕРіРѕ РёР·РІР»РµС‡РµРЅРёСЏ РґР°РЅРЅС‹С…"""
        # РЎРѕР·РґР°РµРј РєРѕРЅС„РёРіСѓСЂР°С†РёСЋ С€Р°РіР° СЃ РїСЂРѕСЃС‚С‹Рј РёР·РІР»РµС‡РµРЅРёРµРј
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
        
        # РњРѕРєР°РµРј РјРµС‚РѕРґ РёР·РІР»РµС‡РµРЅРёСЏ Р·РЅР°С‡РµРЅРёСЏ
        parser._extract_value_by_config = Mock(return_value="test_value")
        
        result = parser._handle_simple_extraction(step_config)
        
        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ Р±С‹Р» РІС‹Р·РІР°РЅ РјРµС‚РѕРґ РёР·РІР»РµС‡РµРЅРёСЏ Р·РЅР°С‡РµРЅРёСЏ
        parser._extract_value_by_config.assert_called_once_with(step_config["data_extraction"])
        self.assertEqual(result, "test_value")

    def test_apply_post_processing_int_conversion(self):
        """РўРµСЃС‚ РїРѕСЃС‚РѕР±СЂР°Р±РѕС‚РєРё СЃ РїСЂРµРѕР±СЂР°Р·РѕРІР°РЅРёРµРј РІ С†РµР»РѕРµ С‡РёСЃР»Рѕ"""
        parser = TestConcreteReportParser(self.config)
        
        # РўРµСЃС‚РёСЂСѓРµРј РїСЂРµРѕР±СЂР°Р·РѕРІР°РЅРёРµ СЃС‚СЂРѕРєРё РІ С†РµР»РѕРµ С‡РёСЃР»Рѕ
        value = "123"
        config = {"convert_to": "int"}
        result = parser._apply_post_processing(value, config)
        
        self.assertEqual(result, 123)
        self.assertIsInstance(result, int)

    def test_apply_post_processing_float_conversion(self):
        """РўРµСЃС‚ РїРѕСЃС‚РѕР±СЂР°Р±РѕС‚РєРё СЃ РїСЂРµРѕР±СЂР°Р·РѕРІР°РЅРёРµРј РІ С‡РёСЃР»Рѕ СЃ РїР»Р°РІР°СЋС‰РµР№ С‚РѕС‡РєРѕР№"""
        parser = TestConcreteReportParser(self.config)
        
        # РўРµСЃС‚РёСЂСѓРµРј РїСЂРµРѕР±СЂР°Р·РѕРІР°РЅРёРµ СЃС‚СЂРѕРєРё РІ С‡РёСЃР»Рѕ СЃ РїР»Р°РІР°СЋС‰РµР№ С‚РѕС‡РєРѕР№
        value = "123.45"
        config = {"convert_to": "float"}
        result = parser._apply_post_processing(value, config)
        
        self.assertEqual(result, 123.45)
        self.assertIsInstance(result, float)

    def test_apply_post_processing_str_conversion(self):
        """РўРµСЃС‚ РїРѕСЃС‚РѕР±СЂР°Р±РѕС‚РєРё СЃ РїСЂРµРѕР±СЂР°Р·РѕРІР°РЅРёРµРј РІ СЃС‚СЂРѕРєСѓ"""
        parser = TestConcreteReportParser(self.config)
        
        # РўРµСЃС‚РёСЂСѓРµРј РїСЂРµРѕР±СЂР°Р·РѕРІР°РЅРёРµ РІ СЃС‚СЂРѕРєСѓ
        value = 123
        config = {"convert_to": "str"}
        result = parser._apply_post_processing(value, config)
        
        self.assertEqual(result, "123")
        self.assertIsInstance(result, str)

    def test_apply_post_processing_with_default_value(self):
        """РўРµСЃС‚ РїРѕСЃС‚РѕР±СЂР°Р±РѕС‚РєРё СЃ СѓСЃС‚Р°РЅРѕРІРєРѕР№ Р·РЅР°С‡РµРЅРёСЏ РїРѕ СѓРјРѕР»С‡Р°РЅРёСЋ РїСЂРё РѕС€РёР±РєРµ"""
        parser = TestConcreteReportParser(self.config)
        
        # РўРµСЃС‚РёСЂСѓРµРј СѓСЃС‚Р°РЅРѕРІРєСѓ Р·РЅР°С‡РµРЅРёСЏ РїРѕ СѓРјРѕР»С‡Р°РЅРёСЋ РїСЂРё РЅРµРІРѕР·РјРѕР¶РЅРѕСЃС‚Рё РїСЂРµРѕР±СЂР°Р·РѕРІР°РЅРёСЏ
        value = "not_a_number"
        config = {"convert_to": "int", "default_value": 0}
        result = parser._apply_post_processing(value, config)
        
        self.assertEqual(result, 0)

    def test_extract_value_by_config(self):
        """РўРµСЃС‚ РёР·РІР»РµС‡РµРЅРёСЏ Р·РЅР°С‡РµРЅРёСЏ РїРѕ РєРѕРЅС„РёРіСѓСЂР°С†РёРё"""
        parser = TestConcreteReportParser(self.config)
        
        # РњРѕРєР°РµРј РјРµС‚РѕРґ РїРѕР»СѓС‡РµРЅРёСЏ Р·РЅР°С‡РµРЅРёСЏ СЌР»РµРјРµРЅС‚Р°
        parser.get_element_value = Mock(return_value="test_value")
        
        extraction_config = {
            "selector": "//div[@id='test']",
            "element_type": "div",
            "post_processing": {
                "convert_to": "str"
            }
        }
        
        result = parser._extract_value_by_config(extraction_config)
        
        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ Р±С‹Р» РІС‹Р·РІР°РЅ РјРµС‚РѕРґ РїРѕР»СѓС‡РµРЅРёСЏ Р·РЅР°С‡РµРЅРёСЏ
        parser.get_element_value.assert_called_once_with(
            selector="//div[@id='test']",
            element_type="div",
            pattern=None
        )
        self.assertEqual(result, "test_value")

    def test_get_common_report_info(self):
        """РўРµСЃС‚ РїРѕР»СѓС‡РµРЅРёСЏ РѕР±С‰РµР№ РёРЅС„РѕСЂРјР°С†РёРё РѕС‚С‡РµС‚Р°"""
        parser = TestConcreteReportParser(self.config)
        
        # РЈСЃС‚Р°РЅР°РІР»РёРІР°РµРј РґР°С‚Сѓ РІС‹РїРѕР»РЅРµРЅРёСЏ
        parser.config['execution_date'] = '2023-12-25'
        
        # РњРѕРєР°РµРј РјРµС‚РѕРґ РїРѕР»СѓС‡РµРЅРёСЏ С‚РµРєСѓС‰РµРіРѕ РџР’Р—
        parser.get_current_pvz = Mock(return_value="Test PVZ")
        
        # РњРѕРєР°РµРј РјРµС‚РѕРґ РїРѕР»СѓС‡РµРЅРёСЏ С‚РµРєСѓС‰РµРіРѕ РІСЂРµРјРµРЅРё
        parser._get_current_timestamp = Mock(return_value="2023-12-25 10:00:00")
        
        info = parser.get_common_report_info()
        
        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ РІСЃРµ РєР»СЋС‡Рё РїСЂРёСЃСѓС‚СЃС‚РІСѓСЋС‚
        self.assertIn('__LOCATION_INFO__', info)
        self.assertIn('__EXTRACTION_TIMESTAMP__', info)
        self.assertIn('__SOURCE_URL__', info)
        self.assertIn('__EXECUTION_DATE__', info)
        
        self.assertEqual(info['__EXECUTION_DATE__'], '2023-12-25')
        self.assertEqual(info['__LOCATION_INFO__'], "Test PVZ")

    def test_aggregate_values_sum(self):
        """РўРµСЃС‚ Р°РіСЂРµРіР°С†РёРё Р·РЅР°С‡РµРЅРёР№ - СЃСѓРјРјРёСЂРѕРІР°РЅРёРµ"""
        parser = TestConcreteReportParser(self.config)
        
        values = [10, 20, 30]
        result = parser._aggregate_values(values, "sum", "total")
        
        self.assertEqual(result, 60)

    def test_aggregate_values_average(self):
        """РўРµСЃС‚ Р°РіСЂРµРіР°С†РёРё Р·РЅР°С‡РµРЅРёР№ - СЃСЂРµРґРЅРµРµ Р·РЅР°С‡РµРЅРёРµ"""
        parser = TestConcreteReportParser(self.config)
        
        values = [10, 20, 30]
        result = parser._aggregate_values(values, "average", "avg")
        
        self.assertEqual(result, 20.0)

    def test_aggregate_values_count(self):
        """РўРµСЃС‚ Р°РіСЂРµРіР°С†РёРё Р·РЅР°С‡РµРЅРёР№ - РїРѕРґСЃС‡РµС‚ РєРѕР»РёС‡РµСЃС‚РІР°"""
        parser = TestConcreteReportParser(self.config)
        
        values = [10, 20, None, 30]
        result = parser._aggregate_values(values, "count", "count")
        
        self.assertEqual(result, 3)

    def test_aggregate_values_max(self):
        """РўРµСЃС‚ Р°РіСЂРµРіР°С†РёРё Р·РЅР°С‡РµРЅРёР№ - РјР°РєСЃРёРјР°Р»СЊРЅРѕРµ Р·РЅР°С‡РµРЅРёРµ"""
        parser = TestConcreteReportParser(self.config)
        
        values = [10, 20, 30, 5]
        result = parser._aggregate_values(values, "max", "max_val")
        
        self.assertEqual(result, 30)

    def test_aggregate_values_min(self):
        """РўРµСЃС‚ Р°РіСЂРµРіР°С†РёРё Р·РЅР°С‡РµРЅРёР№ - РјРёРЅРёРјР°Р»СЊРЅРѕРµ Р·РЅР°С‡РµРЅРёРµ"""
        parser = TestConcreteReportParser(self.config)
        
        values = [10, 20, 30, 5]
        result = parser._aggregate_values(values, "min", "min_val")
        
        self.assertEqual(result, 5)

    def test_replace_placeholders_recursive_string(self):
        """РўРµСЃС‚ СЂРµРєСѓСЂСЃРёРІРЅРѕР№ Р·Р°РјРµРЅС‹ РїР»РµР№СЃС…РѕР»РґРµСЂРѕРІ РІ СЃС‚СЂРѕРєРµ"""
        parser = TestConcreteReportParser(self.config)
        
        data = "Value is {value} and date is {date}"
        replacements = {"value": "test", "date": "2023-12-25"}
        result = parser._replace_placeholders_recursive(data, replacements)
        
        self.assertEqual(result, "Value is test and date is 2023-12-25")

    def test_replace_placeholders_recursive_dict(self):
        """РўРµСЃС‚ СЂРµРєСѓСЂСЃРёРІРЅРѕР№ Р·Р°РјРµРЅС‹ РїР»РµР№СЃС…РѕР»РґРµСЂРѕРІ РІ СЃР»РѕРІР°СЂРµ"""
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
        """РўРµСЃС‚ СЂРµРєСѓСЂСЃРёРІРЅРѕР№ Р·Р°РјРµРЅС‹ РїР»РµР№СЃС…РѕР»РґРµСЂРѕРІ РІ СЃРїРёСЃРєРµ"""
        parser = TestConcreteReportParser(self.config)
        
        data = ["Value is {value}", "Date is {date}"]
        replacements = {"value": "test", "date": "2023-12-25"}
        result = parser._replace_placeholders_recursive(data, replacements)
        
        self.assertEqual(result[0], "Value is test")
        self.assertEqual(result[1], "Date is 2023-12-25")

    def test_navigate_to_target_basic(self):
        """РўРµСЃС‚ Р±Р°Р·РѕРІРѕР№ РЅР°РІРёРіР°С†РёРё Рє С†РµР»РµРІРѕР№ СЃС‚СЂР°РЅРёС†Рµ"""
        # РЈСЃС‚Р°РЅР°РІР»РёРІР°РµРј РїР°СЂР°РјРµС‚СЂС‹ РґР»СЏ РЅР°РІРёРіР°С†РёРё
        self.config['base_url'] = 'https://example.com/report'
        self.config['execution_date'] = '2023-12-25'

        parser = TestConcreteReportParser(self.config)

        # РЎРѕР·РґР°РµРј С„РёРєС‚РёРІРЅС‹Р№ РґСЂР°Р№РІРµСЂ
        parser.driver = Mock()
        parser.driver.get = Mock(return_value=None)

        result = parser.navigate_to_target()

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ РЅР°РІРёРіР°С†РёСЏ РІРµСЂРЅСѓР»Р° True (СѓСЃРїРµС€РЅРѕ)
        self.assertTrue(result)

    def test_get_common_url_prefix(self):
        """РўРµСЃС‚ РїРѕР»СѓС‡РµРЅРёСЏ РѕР±С‰РµРіРѕ РїСЂРµС„РёРєСЃР° URL"""
        parser = TestConcreteReportParser(self.config)
        
        urls = [
            'https://turbo-pvz.ozon.ru/reports/giveout?filter=...',
            'https://turbo-pvz.ozon.ru/outbound/carriages-archive?filter=...'
        ]
        
        common_prefix = parser._get_common_url_prefix(urls)
        
        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ РѕР±С‰РёР№ РїСЂРµС„РёРєСЃ СЃРѕРґРµСЂР¶РёС‚ РґРѕРјРµРЅ
        self.assertIn('https://turbo-pvz.ozon.ru', common_prefix)
        self.assertEqual(common_prefix, 'https://turbo-pvz.ozon.ru')

    def test_get_common_url_prefix_empty_list(self):
        """РўРµСЃС‚ РїРѕР»СѓС‡РµРЅРёСЏ РѕР±С‰РµРіРѕ РїСЂРµС„РёРєСЃР° URL СЃ РїСѓСЃС‚С‹Рј СЃРїРёСЃРєРѕРј"""
        parser = TestConcreteReportParser(self.config)
        
        urls = []
        
        common_prefix = parser._get_common_url_prefix(urls)
        
        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ РІРѕР·РІСЂР°С‰Р°РµС‚СЃСЏ РїСѓСЃС‚Р°СЏ СЃС‚СЂРѕРєР°
        self.assertEqual(common_prefix, "")

    def test_get_common_url_prefix_single_url(self):
        """РўРµСЃС‚ РїРѕР»СѓС‡РµРЅРёСЏ РѕР±С‰РµРіРѕ РїСЂРµС„РёРєСЃР° URL СЃ РѕРґРЅРёРј URL"""
        parser = TestConcreteReportParser(self.config)

        urls = ['https://example.com/path/to/page']

        common_prefix = parser._get_common_url_prefix(urls)

        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ РІРѕР·РІСЂР°С‰Р°РµС‚СЃСЏ РґРѕРјРµРЅ РёР»Рё РµРіРѕ С‡Р°СЃС‚СЊ
        self.assertIn('https://example.com', common_prefix)

    def test_filter_structure_by_available_keys_string_with_available_key(self):
        """РўРµСЃС‚ С„РёР»СЊС‚СЂР°С†РёРё СЃС‚СЂСѓРєС‚СѓСЂС‹ РїРѕ РґРѕСЃС‚СѓРїРЅС‹Рј РєР»СЋС‡Р°Рј - СЃС‚СЂРѕРєР° СЃ РґРѕСЃС‚СѓРїРЅС‹Рј РєР»СЋС‡РѕРј"""
        parser = TestConcreteReportParser(self.config)
        
        data = "Value is {value}"
        available_keys = {"value", "date"}
        
        # РСЃРїРѕР»СЊР·СѓРµРј РјРµС‚РѕРґ СЃ РїСЂРµС„РёРєСЃРѕРј _className РґР»СЏ РґРѕСЃС‚СѓРїР° Рє РїСЂРёРІР°С‚РЅРѕРјСѓ РјРµС‚РѕРґСѓ
        result = parser._BaseReportParser__filter_structure_by_available_keys(data, available_keys)
        
        self.assertEqual(result, "Value is {value}")

    def test_filter_structure_by_available_keys_string_without_available_key(self):
        """РўРµСЃС‚ С„РёР»СЊС‚СЂР°С†РёРё СЃС‚СЂСѓРєС‚СѓСЂС‹ РїРѕ РґРѕСЃС‚СѓРїРЅС‹Рј РєР»СЋС‡Р°Рј - СЃС‚СЂРѕРєР° Р±РµР· РґРѕСЃС‚СѓРїРЅРѕРіРѕ РєР»СЋС‡Р°"""
        parser = TestConcreteReportParser(self.config)
        
        data = "Value is {missing_key}"
        available_keys = {"value", "date"}
        
        # РСЃРїРѕР»СЊР·СѓРµРј РјРµС‚РѕРґ СЃ РїСЂРµС„РёРєСЃРѕРј _className РґР»СЏ РґРѕСЃС‚СѓРїР° Рє РїСЂРёРІР°С‚РЅРѕРјСѓ РјРµС‚РѕРґСѓ
        result = parser._BaseReportParser__filter_structure_by_available_keys(data, available_keys)
        
        self.assertIsNone(result)

    def test_filter_structure_by_available_keys_special_placeholders(self):
        """РўРµСЃС‚ С„РёР»СЊС‚СЂР°С†РёРё СЃС‚СЂСѓРєС‚СѓСЂС‹ РїРѕ РґРѕСЃС‚СѓРїРЅС‹Рј РєР»СЋС‡Р°Рј - СЃРїРµС†РёР°Р»СЊРЅС‹Рµ РїР»РµР№СЃС…РѕР»РґРµСЂС‹"""
        parser = TestConcreteReportParser(self.config)
        
        data = "Source: {__SOURCE_URL__}"
        available_keys = {"value", "date"}  # Р”Р°Р¶Рµ РµСЃР»Рё СЃРїРµС†РёР°Р»СЊРЅС‹Р№ РєР»СЋС‡ РЅРµ РІ СЃРїРёСЃРєРµ
        
        # РСЃРїРѕР»СЊР·СѓРµРј РјРµС‚РѕРґ СЃ РїСЂРµС„РёРєСЃРѕРј _className РґР»СЏ РґРѕСЃС‚СѓРїР° Рє РїСЂРёРІР°С‚РЅРѕРјСѓ РјРµС‚РѕРґСѓ
        result = parser._BaseReportParser__filter_structure_by_available_keys(data, available_keys)
        
        # Р”РѕР»Р¶РЅРѕ РІРµСЂРЅСѓС‚СЊ СЃС‚СЂРѕРєСѓ, РїРѕС‚РѕРјСѓ С‡С‚Рѕ СЃРїРµС†РёР°Р»СЊРЅС‹Рµ РїР»РµР№СЃС…РѕР»РґРµСЂС‹ РІСЃРµРіРґР° РѕСЃС‚Р°СЋС‚СЃСЏ
        self.assertEqual(result, "Source: {__SOURCE_URL__}")

    def test_filter_structure_by_available_keys_dict(self):
        """РўРµСЃС‚ С„РёР»СЊС‚СЂР°С†РёРё СЃС‚СЂСѓРєС‚СѓСЂС‹ РїРѕ РґРѕСЃС‚СѓРїРЅС‹Рј РєР»СЋС‡Р°Рј - СЃР»РѕРІР°СЂСЊ"""
        parser = TestConcreteReportParser(self.config)
        
        data = {
            "field1": "Value is {value}",
            "field2": "Missing: {missing_key}",
            "field3": "Date: {date}"
        }
        available_keys = {"value", "date"}
        
        # РСЃРїРѕР»СЊР·СѓРµРј РјРµС‚РѕРґ СЃ РїСЂРµС„РёРєСЃРѕРј _className РґР»СЏ РґРѕСЃС‚СѓРїР° Рє РїСЂРёРІР°С‚РЅРѕРјСѓ РјРµС‚РѕРґСѓ
        result = parser._BaseReportParser__filter_structure_by_available_keys(data, available_keys)
        
        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ РѕСЃС‚Р°Р»РёСЃСЊ С‚РѕР»СЊРєРѕ РїРѕР»СЏ СЃ РґРѕСЃС‚СѓРїРЅС‹РјРё РєР»СЋС‡Р°РјРё
        self.assertIn("field1", result)
        self.assertNotIn("field2", result)  # Р­С‚Рѕ РїРѕР»Рµ РґРѕР»Р¶РЅРѕ Р±С‹С‚СЊ СѓРґР°Р»РµРЅРѕ
        self.assertIn("field3", result)
        self.assertNotIn("field2", result.keys())

    def test_filter_structure_by_available_keys_list(self):
        """РўРµСЃС‚ С„РёР»СЊС‚СЂР°С†РёРё СЃС‚СЂСѓРєС‚СѓСЂС‹ РїРѕ РґРѕСЃС‚СѓРїРЅС‹Рј РєР»СЋС‡Р°Рј - СЃРїРёСЃРѕРє"""
        parser = TestConcreteReportParser(self.config)
        
        data = [
            "Value is {value}",
            "Missing: {missing_key}",
            "Date: {date}"
        ]
        available_keys = {"value", "date"}
        
        # РСЃРїРѕР»СЊР·СѓРµРј РјРµС‚РѕРґ СЃ РїСЂРµС„РёРєСЃРѕРј _className РґР»СЏ РґРѕСЃС‚СѓРїР° Рє РїСЂРёРІР°С‚РЅРѕРјСѓ РјРµС‚РѕРґСѓ
        result = parser._BaseReportParser__filter_structure_by_available_keys(data, available_keys)
        
        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ РѕСЃС‚Р°Р»РёСЃСЊ С‚РѕР»СЊРєРѕ СЌР»РµРјРµРЅС‚С‹ СЃ РґРѕСЃС‚СѓРїРЅС‹РјРё РєР»СЋС‡Р°РјРё
        self.assertIn("Value is {value}", result)
        self.assertIn("Date: {date}", result)
        # Р­Р»РµРјРµРЅС‚ СЃ РЅРµРґРѕСЃС‚СѓРїРЅС‹Рј РєР»СЋС‡РѕРј РґРѕР»Р¶РµРЅ Р±С‹С‚СЊ СѓРґР°Р»РµРЅ

    def test_execute_multi_step_processing(self):
        """РўРµСЃС‚ РІС‹РїРѕР»РЅРµРЅРёСЏ РјСѓР»СЊС‚Рё-С€Р°РіРѕРІРѕР№ РѕР±СЂР°Р±РѕС‚РєРё"""
        # РЎРѕР·РґР°РµРј РєРѕРЅС„РёРіСѓСЂР°С†РёСЋ РјСѓР»СЊС‚Рё-С€Р°РіРѕРІРѕР№ РѕР±СЂР°Р±РѕС‚РєРё
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
        
        # РњРѕРєР°РµРј РЅРµРѕР±С…РѕРґРёРјС‹Рµ РјРµС‚РѕРґС‹
        parser._execute_single_step = Mock(side_effect=["value1", "value2"])
        parser._combine_step_results = Mock(return_value={"result1": "value1", "result2": "value2"})
        
        result = parser._execute_multi_step_processing(multi_step_config)
        
        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ Р±С‹Р»Рё РІС‹Р·РІР°РЅС‹ РЅСѓР¶РЅС‹Рµ РјРµС‚РѕРґС‹
        self.assertEqual(parser._execute_single_step.call_count, 2)
        parser._combine_step_results.assert_called_once()
        self.assertEqual(result.get('__RUN_STATUS__'), 'success')

    def test_handle_table_extraction(self):
        """РўРµСЃС‚ РѕР±СЂР°Р±РѕС‚РєРё С‚Р°Р±Р»РёС‡РЅРѕРіРѕ РёР·РІР»РµС‡РµРЅРёСЏ РґР°РЅРЅС‹С…"""
        # РЎРѕР·РґР°РµРј РєРѕРЅС„РёРіСѓСЂР°С†РёСЋ С€Р°РіР° СЃ С‚Р°Р±Р»РёС‡РЅС‹Рј РёР·РІР»РµС‡РµРЅРёРµРј
        step_config = {
            "table_processing": {
                "table_config_key": "test_table",
                "enabled": True
            }
        }
        
        # Р”РѕР±Р°РІР»СЏРµРј РєРѕРЅС„РёРіСѓСЂР°С†РёСЋ С‚Р°Р±Р»РёС†С‹ РІ РѕР±С‰РёР№ РєРѕРЅС„РёРі
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
        
        # РњРѕРєР°РµРј РјРµС‚РѕРґ РёР·РІР»РµС‡РµРЅРёСЏ С‚Р°Р±Р»РёС‡РЅС‹С… РґР°РЅРЅС‹С…
        expected_table_data = [{"col1": "value1", "col2": "value2"}]
        parser.extract_table_data = Mock(return_value=expected_table_data)
        
        result = parser._handle_table_extraction(step_config)
        
        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ Р±С‹Р» РІС‹Р·РІР°РЅ РјРµС‚РѕРґ РёР·РІР»РµС‡РµРЅРёСЏ С‚Р°Р±Р»РёС‡РЅС‹С… РґР°РЅРЅС‹С…
        parser.extract_table_data.assert_called_once_with(table_config_key="test_table")
        self.assertEqual(result, expected_table_data)

    def test_handle_table_nested_extraction(self):
        """РўРµСЃС‚ РѕР±СЂР°Р±РѕС‚РєРё РІР»РѕР¶РµРЅРЅРѕРіРѕ С‚Р°Р±Р»РёС‡РЅРѕРіРѕ РёР·РІР»РµС‡РµРЅРёСЏ РґР°РЅРЅС‹С…"""
        # РЎРѕР·РґР°РµРј РєРѕРЅС„РёРіСѓСЂР°С†РёСЋ С€Р°РіР° СЃ РІР»РѕР¶РµРЅРЅС‹Рј С‚Р°Р±Р»РёС‡РЅС‹Рј РёР·РІР»РµС‡РµРЅРёРµРј
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
        
        # Р”РѕР±Р°РІР»СЏРµРј РєРѕРЅС„РёРіСѓСЂР°С†РёСЋ С‚Р°Р±Р»РёС†С‹ РІ РѕР±С‰РёР№ РєРѕРЅС„РёРі
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
        
        # РњРѕРєР°РµРј РЅРµРѕР±С…РѕРґРёРјС‹Рµ РјРµС‚РѕРґС‹
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
        
        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ Р±С‹Р»Рё РІС‹Р·РІР°РЅС‹ РЅСѓР¶РЅС‹Рµ РјРµС‚РѕРґС‹
        parser._handle_table_extraction.assert_called_once_with(step_config)
        parser._handle_nested_processing.assert_called_once()
        parser._aggregate_nested_results.assert_called_once()
        self.assertEqual(result, aggregated_result)

    def test_handle_nested_processing(self):
        """РўРµСЃС‚ РѕР±СЂР°Р±РѕС‚РєРё РІР»РѕР¶РµРЅРЅС‹С… РґР°РЅРЅС‹С…"""
        # РЎРѕР·РґР°РµРј РєРѕРЅС„РёРіСѓСЂР°С†РёСЋ РІР»РѕР¶РµРЅРЅРѕР№ РѕР±СЂР°Р±РѕС‚РєРё
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
        
        # РњРѕРєР°РµРј РЅРµРѕР±С…РѕРґРёРјС‹Рµ РјРµС‚РѕРґС‹
        parser.navigate_to_target = Mock(return_value=True)
        parser._extract_value_by_config = Mock(side_effect=[10, 20, 30])
        
        # РЈСЃС‚Р°РЅР°РІР»РёРІР°РµРј target_url РґР»СЏ РїСЂРѕРІРµСЂРєРё
        parser.config["target_url"] = "https://example.com/detail/1"
        
        results = parser._handle_nested_processing(nested_config, identifiers)
        
        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЂРµР·СѓР»СЊС‚Р°С‚ СЃРѕРґРµСЂР¶РёС‚ РґР°РЅРЅС‹Рµ РґР»СЏ РІСЃРµС… РёРґРµРЅС‚РёС„РёРєР°С‚РѕСЂРѕРІ
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0]["identifier"], "1")
        self.assertEqual(results[0]["value"], 10)

    def test_aggregate_nested_results(self):
        """РўРµСЃС‚ Р°РіСЂРµРіР°С†РёРё РІР»РѕР¶РµРЅРЅС‹С… СЂРµР·СѓР»СЊС‚Р°С‚РѕРІ"""
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
        
        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЂРµР·СѓР»СЊС‚Р°С‚ СЃРѕРґРµСЂР¶РёС‚ Р°РіСЂРµРіРёСЂРѕРІР°РЅРЅРѕРµ Р·РЅР°С‡РµРЅРёРµ
        self.assertIn("total_value", result)
        self.assertEqual(result["total_value"], 60)  # 10 + 20 + 30
        self.assertIn("details", result)
        self.assertEqual(len(result["details"]), 3)

    def test_combine_step_results(self):
        """РўРµСЃС‚ РѕР±СЉРµРґРёРЅРµРЅРёСЏ СЂРµР·СѓР»СЊС‚Р°С‚РѕРІ С€Р°РіРѕРІ"""
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
        
        # РњРѕРєР°РµРј РјРµС‚РѕРґС‹, РёСЃРїРѕР»СЊР·СѓРµРјС‹Рµ РІРЅСѓС‚СЂРё _combine_step_results
        parser.get_common_report_info = Mock(return_value={
            "__LOCATION_INFO__": "Test Location",
            "__EXTRACTION_TIMESTAMP__": "2023-12-25 10:00:00",
            "__SOURCE_URL__": "https://example.com",
            "__EXECUTION_DATE__": "2023-12-25"
        })
        
        result = parser._combine_step_results(all_step_results, aggregation_config)
        
        # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЂРµР·СѓР»СЊС‚Р°С‚ СЃРѕРґРµСЂР¶РёС‚ РѕР±СЉРµРґРёРЅРµРЅРЅС‹Рµ РґР°РЅРЅС‹Рµ
        self.assertIsNotNone(result)


    def test_calculate_run_status(self):
        """Run status is derived from per-step error presence."""
        parser = TestConcreteReportParser(self.config)

        self.assertEqual(
            parser._calculate_run_status({"s1": {"value": 1}, "s2": {"value": 2}}),
            "success"
        )
        self.assertEqual(
            parser._calculate_run_status({"s1": {"error": "x"}, "s2": {"value": 2}}),
            "partial"
        )
        self.assertEqual(
            parser._calculate_run_status({"s1": {"error": "x"}, "s2": {"error": "y"}}),
            "failed"
        )

    def test_execute_single_step_auth_required_when_login_redirect(self):
        """Login redirect reason must be escalated to AUTH_REQUIRED."""
        parser = TestConcreteReportParser(self.config)
        parser.navigate_to_target = Mock(return_value=False)
        parser.config['_last_navigation_failure_reason'] = 'login_redirect'

        with self.assertRaises(Exception) as exc_info:
            parser._execute_single_step({"result_key": "test_step"})

        self.assertIn("AUTH_REQUIRED", str(exc_info.exception))

    def test_execute_multi_step_processing_stops_on_auth_required(self):
        """Multi-step processing should stop immediately on AUTH_REQUIRED."""
        parser = TestConcreteReportParser(self.config)
        parser._execute_single_step = Mock(side_effect=Exception("AUTH_REQUIRED: redirected_to_login"))

        multi_step_config = {
            "steps": ["step1", "step2"],
            "step_configurations": {
                "step1": {"result_key": "result1"},
                "step2": {"result_key": "result2"}
            },
            "aggregation_logic": {}
        }

        with self.assertRaises(Exception) as exc_info:
            parser._execute_multi_step_processing(multi_step_config)

        self.assertIn("\u0410\u0432\u0442\u043e\u0440\u0438\u0437\u0430\u0446\u0438\u044f \u043d\u0435\u0434\u0435\u0439\u0441\u0442\u0432\u0438\u0442\u0435\u043b\u044c\u043d\u0430", str(exc_info.exception))
        self.assertEqual(parser._execute_single_step.call_count, 1)

    def test_run_parser_raises_on_failed_run_status(self):
        """run_parser should fail when all steps are failed."""
        parser = TestConcreteReportParser(self.config)
        parser.setup_browser = Mock(return_value=True)
        parser.login = Mock(return_value=True)
        parser.logout = Mock(return_value=True)
        parser.close_browser = Mock()
        parser.config['multi_step_config'] = {"steps": ["step1"], "step_configurations": {"step1": {}}}
        parser._execute_multi_step_processing = Mock(return_value={"__RUN_STATUS__": "failed"})

        with self.assertRaises(Exception) as exc_info:
            parser.run_parser(save_to_file=False)

        self.assertIn("\u043d\u0435\u0443\u0441\u043f\u0435\u0448\u043d\u043e", str(exc_info.exception))

    def test_run_parser_batch_uses_single_browser_session(self):
        """Batch flow should open and close browser only once."""
        parser = TestConcreteReportParser(self.config)
        parser.setup_browser = Mock(return_value=True)
        parser.login = Mock(return_value=True)
        parser.logout = Mock(return_value=True)
        parser.close_browser = Mock()
        parser._run_single_date_in_current_session = Mock(
            side_effect=[
                {"execution_date": "2026-03-01", "__RUN_STATUS__": "success"},
                {"execution_date": "2026-03-02", "__RUN_STATUS__": "success"},
            ]
        )

        result = parser.run_parser_batch(["2026-03-01", "2026-03-02"], save_to_file=False)

        self.assertTrue(result["success"])
        self.assertEqual(result["successful_dates"], 2)
        parser.setup_browser.assert_called_once()
        parser.login.assert_called_once()
        parser.logout.assert_called_once()
        parser.close_browser.assert_called_once()

    def test_run_parser_batch_continues_after_single_date_failure(self):
        """Batch flow should continue when one date fails with non-auth error."""
        parser = TestConcreteReportParser(self.config)
        parser.setup_browser = Mock(return_value=True)
        parser.login = Mock(return_value=True)
        parser.logout = Mock(return_value=True)
        parser.close_browser = Mock()
        parser._run_single_date_in_current_session = Mock(
            side_effect=[
                {"execution_date": "2026-03-01", "__RUN_STATUS__": "success"},
                Exception("temporary parse failure"),
                {"execution_date": "2026-03-03", "__RUN_STATUS__": "success"},
            ]
        )

        result = parser.run_parser_batch(
            ["2026-03-01", "2026-03-02", "2026-03-03"],
            save_to_file=False
        )

        self.assertFalse(result["success"])
        self.assertEqual(result["successful_dates"], 2)
        self.assertEqual(result["failed_dates"], 1)
        self.assertFalse(result["results_by_date"]["2026-03-02"]["success"])
        parser.close_browser.assert_called_once()

    def test_run_job_reuses_existing_single_date_flow(self):
        """Job adapter should delegate to current single-date execution flow."""
        parser = TestConcreteReportParser(self.config)
        parser.setup_browser = Mock(return_value=True)
        parser.login = Mock(return_value=True)
        parser.logout = Mock(return_value=True)
        parser.close_browser = Mock()
        parser._run_single_date_in_current_session = Mock(
            return_value={"execution_date": "2026-03-01", "__RUN_STATUS__": "success"}
        )

        definition = ReportDefinition(report_type="report", config=self.config.copy())
        runtime = ParserRuntimeContext(save_to_file=False)
        job = ParserJob(report_type="report", pvz_id="PVZ1", execution_date="2026-03-01")

        result = parser.run_job(job, definition, runtime)

        self.assertTrue(result.success)
        self.assertEqual(result.pvz_id, "PVZ1")
        self.assertEqual(parser.config["additional_params"]["location_id"], "PVZ1")
        parser.setup_browser.assert_called_once()
        parser.login.assert_called_once()
        parser.logout.assert_called_once()
        parser.close_browser.assert_called_once()

    def test_run_jobs_for_pvz_uses_single_browser_session(self):
        """Job batch for one PVZ should reuse one browser session."""
        parser = TestConcreteReportParser(self.config)
        parser.setup_browser = Mock(return_value=True)
        parser.login = Mock(return_value=True)
        parser.logout = Mock(return_value=True)
        parser.close_browser = Mock()
        parser._run_single_date_in_current_session = Mock(
            side_effect=[
                {"execution_date": "2026-03-01", "__RUN_STATUS__": "success"},
                {"execution_date": "2026-03-02", "__RUN_STATUS__": "success"},
            ]
        )

        definition = ReportDefinition(report_type="report", config=self.config.copy())
        runtime = ParserRuntimeContext(save_to_file=False)
        jobs = [
            ParserJob(report_type="report", pvz_id="PVZ1", execution_date="2026-03-01"),
            ParserJob(report_type="report", pvz_id="PVZ1", execution_date="2026-03-02"),
        ]

        result = parser.run_jobs_for_pvz(jobs=jobs, definition=definition, runtime=runtime)

        self.assertEqual(len(result), 2)
        self.assertTrue(all(item.success for item in result))
        parser.setup_browser.assert_called_once()
        parser.login.assert_called_once()
        parser.logout.assert_called_once()
        parser.close_browser.assert_called_once()

    def test_run_jobs_batch_groups_by_pvz(self):
        """Job adapter batch should group jobs by PVZ."""
        parser = TestConcreteReportParser(self.config)
        parser.run_jobs_for_pvz = Mock(side_effect=[
            [ParserJobResult.from_success(report_type="report", pvz_id="PVZ1", execution_date="2026-03-01", data={})],
            [ParserJobResult.from_success(report_type="report", pvz_id="PVZ2", execution_date="2026-03-01", data={})],
        ])

        definition = ReportDefinition(report_type="report", config=self.config.copy())
        runtime = ParserRuntimeContext()
        jobs = [
            ParserJob(report_type="report", pvz_id="PVZ1", execution_date="2026-03-01"),
            ParserJob(report_type="report", pvz_id="PVZ2", execution_date="2026-03-01"),
        ]

        result = parser.run_jobs_batch(jobs=jobs, definition=definition, runtime=runtime)

        self.assertEqual(len(result), 2)
        self.assertEqual(parser.run_jobs_for_pvz.call_count, 2)

    def test_run_jobs_for_pvz_stops_on_auth_required(self):
        """AUTH_REQUIRED should stop batch for current PVZ."""
        parser = TestConcreteReportParser(self.config)
        parser.setup_browser = Mock(return_value=True)
        parser.login = Mock(return_value=True)
        parser.logout = Mock(return_value=True)
        parser.close_browser = Mock()
        parser.run_job = Mock(side_effect=[
            ParserJobResult.from_error(
                report_type="report",
                pvz_id="PVZ1",
                execution_date="2026-03-01",
                error_code="AUTH_REQUIRED",
                error_message="AUTH_REQUIRED: redirected_to_login",
            )
        ])

        definition = ReportDefinition(report_type="report", config=self.config.copy())
        runtime = ParserRuntimeContext()
        jobs = [
            ParserJob(report_type="report", pvz_id="PVZ1", execution_date="2026-03-01"),
            ParserJob(report_type="report", pvz_id="PVZ1", execution_date="2026-03-02"),
        ]

        result = parser.run_jobs_for_pvz(jobs=jobs, definition=definition, runtime=runtime)

        self.assertEqual(len(result), 1)
        parser.run_job.assert_called_once()

if __name__ == '__main__':
    unittest.main()


