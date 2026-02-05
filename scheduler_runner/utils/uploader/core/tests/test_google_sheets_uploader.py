"""
Тесты для GoogleSheetsUploader

В версии 1.0.0 были внесены изменения:
- Созданы базовые тесты для GoogleSheetsUploader
- Добавлены тесты для подключения к Google Sheets
- Добавлены тесты для загрузки данных в Google Sheets
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from scheduler_runner.utils.uploader.implementations.google_sheets_uploader import GoogleSheetsUploader


class TestGoogleSheetsUploader(unittest.TestCase):
    """Тесты для GoogleSheetsUploader"""

    def setUp(self):
        """Настройка тестов"""
        self.config = {
            "CREDENTIALS_PATH": "./test_credentials.json",
            "SPREADSHEET_ID": "test_spreadsheet_id",
            "WORKSHEET_NAME": "Test Sheet",
            "TARGET_SYSTEM": "Google Sheets",
            "BATCH_SIZE": 100,
            "MAX_RETRIES": 3,
            "DELAY_BETWEEN_RETRIES": 1,
            "LOG_LEVEL": "INFO",
            "DETAILED_LOGS": False,
            "TABLE_CONFIG": None
        }
        self.uploader = GoogleSheetsUploader(config=self.config)

    def test_initialization(self):
        """Тест инициализации GoogleSheetsUploader"""
        self.assertIsNotNone(self.uploader.config)
        self.assertEqual(self.uploader.spreadsheet_id, "test_spreadsheet_id")
        self.assertEqual(self.uploader.worksheet_name, "Test Sheet")
        self.assertEqual(self.uploader.credentials_path, "./test_credentials.json")
        self.assertIsNone(self.uploader.table_config)

    @patch('pathlib.Path.exists', return_value=True)
    @patch('scheduler_runner.utils.uploader.core.providers.google_sheets.google_sheets_core.GoogleSheetsReporter', spec=False)
    def test_establish_connection_success(self, mock_reporter_class, mock_path_exists):
        """Тест успешного установления подключения к Google Sheets"""
        mock_reporter_instance = Mock()
        # Мокаем методы внутри экземпляра, чтобы избежать исключений
        mock_reporter_instance._sync_table_structure = Mock()
        # Устанавливаем мок как возвращаемое значение
        mock_reporter_class.return_value = mock_reporter_instance

        # Убедимся, что метод _establish_connection возвращает True
        result = self.uploader._establish_connection()

        self.assertTrue(result)
        # Проверяем, что GoogleSheetsReporter был вызван с правильными параметрами
        mock_reporter_class.assert_called_once_with(
            credentials_path="./test_credentials.json",
            spreadsheet_name="test_spreadsheet_id",
            worksheet_name="Test Sheet",
            table_config=None
        )
        self.assertEqual(self.uploader.sheets_reporter, mock_reporter_instance)

    def test_establish_connection_credentials_not_found(self):
        """Тест установления подключения когда файл учетных данных не найден"""
        # Используем реальный путь к несуществующему файлу
        self.uploader.credentials_path = "./nonexistent_credentials.json"

        result = self.uploader._establish_connection()

        self.assertFalse(result)

    @patch('scheduler_runner.utils.uploader.core.providers.google_sheets.google_sheets_core.GoogleSheetsReporter')
    def test_close_connection(self, mock_reporter_class):
        """Тест закрытия подключения к Google Sheets"""
        mock_reporter_instance = Mock()
        self.uploader.sheets_reporter = mock_reporter_instance
        
        result = self.uploader._close_connection()
        
        self.assertTrue(result)
        self.assertIsNone(self.uploader.sheets_reporter)

    def test_perform_upload_when_not_connected(self):
        """Тест загрузки данных когда нет подключения к Google Sheets"""
        result = self.uploader._perform_upload({"field": "value"})
        
        self.assertFalse(result["success"])
        self.assertIn("подключения", result["error"])

    @patch('scheduler_runner.utils.uploader.core.providers.google_sheets.google_sheets_core.GoogleSheetsReporter')
    def test_perform_upload_success(self, mock_reporter_class):
        """Тест успешной загрузки данных в Google Sheets"""
        mock_reporter_instance = Mock()
        mock_reporter_instance.update_or_append_data_with_config.return_value = {
            "success": True,
            "row_number": 5
        }
        
        self.uploader.sheets_reporter = mock_reporter_instance
        self.uploader.connected = True
        self.uploader.table_config = Mock()
        
        result = self.uploader._perform_upload({"field": "value"})
        
        self.assertTrue(result["success"])
        mock_reporter_instance.update_or_append_data_with_config.assert_called_once_with(
            data={"field": "value"},
            config=self.uploader.table_config,
            strategy="update_or_append"
        )

    @patch('scheduler_runner.utils.uploader.core.providers.google_sheets.google_sheets_core.GoogleSheetsReporter')
    def test_perform_upload_with_custom_strategy(self, mock_reporter_class):
        """Тест загрузки данных с пользовательской стратегией"""
        mock_reporter_instance = Mock()
        mock_reporter_instance.update_or_append_data_with_config.return_value = {
            "success": True,
            "row_number": 5
        }
        
        self.uploader.sheets_reporter = mock_reporter_instance
        self.uploader.connected = True
        self.uploader.table_config = Mock()
        
        result = self.uploader._perform_upload({"field": "value"}, strategy="append_only")
        
        self.assertTrue(result["success"])
        mock_reporter_instance.update_or_append_data_with_config.assert_called_once_with(
            data={"field": "value"},
            config=self.uploader.table_config,
            strategy="append_only"
        )

    @patch('scheduler_runner.utils.uploader.core.providers.google_sheets.google_sheets_core.GoogleSheetsReporter')
    def test_get_sheet_info_when_connected(self, mock_reporter_class):
        """Тест получения информации о листе когда подключение установлено"""
        mock_reporter_instance = Mock()
        mock_reporter_instance.get_table_headers.return_value = ["Header1", "Header2"]
        mock_reporter_instance.get_last_row_with_data.return_value = 10
        
        self.uploader.sheets_reporter = mock_reporter_instance
        self.uploader.connected = True
        self.uploader.spreadsheet_id = "test_spreadsheet_id"
        self.uploader.worksheet_name = "Test Sheet"
        
        result = self.uploader.get_sheet_info()
        
        self.assertTrue(result["success"])
        self.assertEqual(result["spreadsheet_id"], "test_spreadsheet_id")
        self.assertEqual(result["worksheet_name"], "Test Sheet")
        self.assertEqual(result["headers"], ["Header1", "Header2"])
        self.assertEqual(result["last_data_row"], 10)
        self.assertEqual(result["total_rows_with_data"], 9)

    def test_get_sheet_info_when_not_connected(self):
        """Тест получения информации о листе когда нет подключения"""
        result = self.uploader.get_sheet_info()

        self.assertFalse(result["success"])
        self.assertIn("подключения", result["error"])

    @patch('scheduler_runner.utils.uploader.core.providers.google_sheets.google_sheets_core.GoogleSheetsReporter')
    def test_upload_multiple_reports(self, mock_reporter_class):
        """Тест загрузки нескольких отчетов"""
        mock_reporter_instance = Mock()
        mock_reporter_instance.update_or_append_data_with_config.side_effect = [
            {"success": True, "row_number": 5},
            {"success": False, "error": "Test error"}
        ]
        
        self.uploader.sheets_reporter = mock_reporter_instance
        self.uploader.connected = True
        self.uploader.table_config = Mock()
        
        reports_data = [{"field1": "value1"}, {"field2": "value2"}]
        result = self.uploader.upload_multiple_reports(reports_data)
        
        self.assertTrue(result["success"])
        self.assertEqual(result["uploaded"], 1)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(len(result["details"]), 2)

    def test_perform_upload_process_implemented(self):
        """Тест метода выполнения процесса загрузки (реализован в GoogleSheetsUploader)"""
        # Метод реализован и не должен выбрасывать NotImplementedError
        self.uploader.connected = True
        # Заглушаем внутренние вызовы, чтобы избежать реальных операций
        with patch.object(self.uploader, 'config', {}):
            with patch.object(self.uploader, '_add_report_metadata', return_value={}):
                with patch.object(self.uploader, 'upload_data', return_value={"success": True}):
                    # Метод должен выполниться без ошибки
                    result = self.uploader._perform_upload_process()
                    # Результат должен быть словарем
                    self.assertIsInstance(result, dict)


if __name__ == '__main__':
    unittest.main()