"""
Тесты для BaseReportUploader

В версии 1.0.0 были внесены изменения:
- Созданы базовые тесты для BaseReportUploader
- Добавлены тесты для загрузки отчетов из файлов
- Добавлены тесты для валидации структуры отчетов
"""
import unittest
from unittest.mock import Mock, patch, MagicMock, mock_open
import tempfile
import json
import csv
import os
from datetime import datetime
from pathlib import Path

from scheduler_runner.utils.uploader.core.base_report_uploader import BaseReportUploader


class TestBaseReportUploader(unittest.TestCase):
    """Тесты для BaseReportUploader"""

    def setUp(self):
        """Настройка тестов"""
        self.config = {
            "CREDENTIALS_PATH": "./test_credentials.json",
            "TARGET_SYSTEM": "test_system",
            "BATCH_SIZE": 100,
            "MAX_RETRIES": 3,
            "DELAY_BETWEEN_RETRIES": 1,
            "LOG_LEVEL": "INFO",
            "DETAILED_LOGS": False,
            "REPORT_DATE": "2026-01-01"
        }
        self.uploader = BaseReportUploader(config=self.config)

    def test_initialization(self):
        """Тест инициализации BaseReportUploader"""
        self.assertIsNotNone(self.uploader.config)
        self.assertEqual(self.uploader.config["TARGET_SYSTEM"], "test_system")
        self.assertIsNone(self.uploader.args)
        # report_date устанавливается в _update_report_date(), а не в __init__()
        self.assertIsNone(self.uploader.report_date)
        self.assertIsNone(self.uploader.pvz_id)

    def test_add_report_metadata(self):
        """Тест добавления метаданных отчета"""
        report_data = {"field1": "value1", "field2": "value2"}
        self.uploader.report_date = "2026-01-01"
        self.uploader.pvz_id = "test_pvz"
        
        result = self.uploader._add_report_metadata(report_data)
        
        self.assertIn("_report_date", result)
        self.assertIn("_pvz_id", result)
        self.assertIn("_upload_timestamp", result)
        self.assertIn("field1", result)
        self.assertIn("field2", result)
        self.assertEqual(result["_report_date"], "2026-01-01")
        self.assertEqual(result["_pvz_id"], "test_pvz")

    @patch('builtins.open', new_callable=mock_open, read_data='{"test": "data"}')
    @patch('os.path.exists', return_value=True)
    def test_load_json_report(self, mock_exists, mock_file):
        """Тест загрузки JSON отчета"""
        result = self.uploader._load_json_report(Path("test.json"))
        
        self.assertEqual(result, {"test": "data"})
        mock_file.assert_called_once_with(Path("test.json"), 'r', encoding='utf-8')

    @patch('builtins.open', new_callable=mock_open)
    @patch('csv.DictReader')
    @patch('os.path.exists', return_value=True)
    def test_load_csv_report(self, mock_exists, mock_csv_reader, mock_file):
        """Тест загрузки CSV отчета"""
        mock_csv_reader.return_value = [{'field1': 'value1', 'field2': 'value2'}]
        mock_file.return_value.__enter__.return_value = mock_file
        
        result = self.uploader._load_csv_report(Path("test.csv"))
        
        self.assertIn("data", result)
        self.assertEqual(len(result["data"]), 1)
        self.assertEqual(result["data"][0]["field1"], "value1")
        self.assertEqual(result["data"][0]["field2"], "value2")

    def test_validate_report_structure_no_schema(self):
        """Тест валидации структуры отчета без схемы"""
        result = self.uploader.validate_report_structure({"field": "value"})
        
        self.assertTrue(result["success"])
        self.assertIn("warnings", result)
        self.assertIn("Схема валидации не указана", result["warnings"][0])

    def test_validate_report_structure_missing_required_fields(self):
        """Тест валидации структуры отчета с отсутствующими обязательными полями"""
        schema = {"required": ["field1", "field2"]}
        report_data = {"field1": "value1"}  # field2 отсутствует
        
        result = self.uploader.validate_report_structure(report_data, schema)
        
        self.assertFalse(result["success"])
        self.assertIn("field2", result["errors"][0])

    def test_validate_report_structure_wrong_types(self):
        """Тест валидации структуры отчета с неправильными типами данных"""
        schema = {"types": {"field1": "str", "field2": "int"}}
        report_data = {"field1": 123, "field2": "not_int"}  # неправильные типы
        
        result = self.uploader.validate_report_structure(report_data, schema)
        
        self.assertTrue(result["success"])  # Только предупреждения, не ошибка
        self.assertGreater(len(result["warnings"]), 0)

    def test_validate_report_structure_valid(self):
        """Тест валидации корректной структуры отчета"""
        schema = {"required": ["field1"], "types": {"field1": "str"}}
        report_data = {"field1": "value1", "field2": "value2"}
        
        result = self.uploader.validate_report_structure(report_data, schema)
        
        self.assertTrue(result["success"])

    def test_format_report_output_json(self):
        """Тест форматирования отчета в JSON"""
        data = {"field1": "value1", "field2": 123}
        
        result = self.uploader.format_report_output(data, "json")
        
        self.assertEqual(result, data)

    def test_format_report_output_csv(self):
        """Тест форматирования отчета в CSV"""
        data = {"field1": "value1", "field2": "value2"}
        
        result = self.uploader.format_report_output(data, "csv")
        
        self.assertIsInstance(result, str)
        self.assertIn("field1", result)
        self.assertIn("field2", result)

    def test_format_report_output_xml(self):
        """Тест форматирования отчета в XML"""
        data = {"field1": "value1", "field2": "value2"}
        
        result = self.uploader.format_report_output(data, "xml")
        
        self.assertIsInstance(result, str)
        self.assertIn("field1", result)
        self.assertIn("field2", result)

    def test_get_report_statistics(self):
        """Тест получения статистики по отчету"""
        report_data = {
            "field1": "value1",
            "field2": 123,
            "nested": {"subfield": "subvalue"},
            "list_field": [1, 2, 3],
            "null_field": None
        }
        
        stats = self.uploader.get_report_statistics(report_data)
        
        self.assertGreater(stats["total_records"], 0)
        self.assertGreater(stats["fields_count"], 0)
        self.assertGreater(stats["size_bytes"], 0)
        self.assertTrue(isinstance(stats["data_types"], dict))
        self.assertTrue(isinstance(stats["null_fields"], list))
        self.assertIn("null_field", stats["null_fields"])

    def test_upload_report_when_not_connected(self):
        """Тест загрузки отчета когда нет подключения"""
        result = self.uploader.upload_report({"field": "value"})
        
        self.assertFalse(result["success"])
        self.assertIn("подключения", result["error"])

    @patch('scheduler_runner.utils.uploader.core.base_report_uploader.datetime')
    def test_update_report_date_from_args(self, mock_datetime):
        """Тест обновления даты отчета из аргументов"""
        # Создаем uploader с аргументами
        uploader = BaseReportUploader(config=self.config)
        uploader.args = Mock()
        uploader.args.report_date = "2026-02-02"
        
        uploader._update_report_date()
        
        self.assertEqual(uploader.report_date, "2026-02-02")

    @patch('scheduler_runner.utils.uploader.core.base_report_uploader.datetime')
    def test_update_report_date_from_config(self, mock_datetime):
        """Тест обновления даты отчета из конфигурации"""
        mock_datetime.now.return_value.strftime.return_value = "2026-03-03"
        
        uploader = BaseReportUploader(config=self.config)
        uploader.args = Mock()
        uploader.args.report_date = None  # Нет в аргументах
        
        uploader._update_report_date()
        
        self.assertEqual(uploader.report_date, "2026-01-01")  # Из конфигурации

    @patch('scheduler_runner.utils.uploader.core.base_report_uploader.datetime')
    def test_update_report_date_from_current_time(self, mock_datetime):
        """Тест обновления даты отчета из текущего времени"""
        mock_datetime.now.return_value.strftime.return_value = "2026-04-04"
        
        config_without_date = self.config.copy()
        del config_without_date["REPORT_DATE"]
        
        uploader = BaseReportUploader(config=config_without_date)
        uploader.args = Mock()
        uploader.args.report_date = None  # Нет в аргументах
        
        uploader._update_report_date()
        
        self.assertEqual(uploader.report_date, "2026-04-04")


if __name__ == '__main__':
    unittest.main()