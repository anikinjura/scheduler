"""
Тесты для BaseUploader

В версии 1.0.0 были внесены изменения:
- Созданы базовые тесты для BaseUploader
- Добавлены тесты для методов подключения и загрузки
- Добавлены тесты для валидации данных
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
import os
from scheduler_runner.utils.uploader.core.base_uploader import BaseUploader


class TestBaseUploader(unittest.TestCase):
    """Тесты для BaseUploader"""

    def setUp(self):
        """Настройка тестов"""
        self.config = {
            "CREDENTIALS_PATH": "./test_credentials.json",
            "TARGET_SYSTEM": "test_system",
            "BATCH_SIZE": 100,
            "MAX_RETRIES": 3,
            "DELAY_BETWEEN_RETRIES": 1,
            "LOG_LEVEL": "INFO",
            "DETAILED_LOGS": False
        }
        self.uploader = BaseUploader(config=self.config)

    def test_initialization(self):
        """Тест инициализации BaseUploader"""
        self.assertIsNotNone(self.uploader.config)
        self.assertEqual(self.uploader.config["TARGET_SYSTEM"], "test_system")
        self.assertFalse(self.uploader.connected)
        self.assertEqual(self.uploader.uploaded_count, 0)
        self.assertEqual(self.uploader.failed_count, 0)

    @patch('scheduler_runner.utils.uploader.core.base_uploader.BaseUploader._validate_connection_params')
    @patch('scheduler_runner.utils.uploader.core.base_uploader.BaseUploader._establish_connection')
    def test_connect_success(self, mock_establish_conn, mock_validate_params):
        """Тест успешного подключения"""
        mock_validate_params.return_value = True
        mock_establish_conn.return_value = True

        result = self.uploader.connect()

        self.assertTrue(result)
        self.assertTrue(self.uploader.connected)
        mock_validate_params.assert_called_once()
        mock_establish_conn.assert_called_once()

    @patch('scheduler_runner.utils.uploader.core.base_uploader.BaseUploader._validate_connection_params')
    def test_connect_validation_failure(self, mock_validate_params):
        """Тест неудачного подключения из-за провальной валидации"""
        mock_validate_params.return_value = False

        result = self.uploader.connect()

        self.assertFalse(result)
        self.assertFalse(self.uploader.connected)

    @patch('scheduler_runner.utils.uploader.core.base_uploader.BaseUploader._validate_connection_params')
    @patch('scheduler_runner.utils.uploader.core.base_uploader.BaseUploader._establish_connection')
    def test_connect_establishment_failure(self, mock_establish_conn, mock_validate_params):
        """Тест неудачного подключения из-за ошибки установления соединения"""
        mock_validate_params.return_value = True
        mock_establish_conn.return_value = False

        result = self.uploader.connect()

        self.assertFalse(result)
        self.assertFalse(self.uploader.connected)

    @patch('scheduler_runner.utils.uploader.core.base_uploader.BaseUploader._close_connection')
    def test_disconnect_when_connected(self, mock_close_conn):
        """Тест отключения когда соединение установлено"""
        self.uploader.connected = True
        mock_close_conn.return_value = True

        result = self.uploader.disconnect()

        self.assertTrue(result)
        self.assertFalse(self.uploader.connected)
        self.assertIsNone(self.uploader.connection_handle)
        mock_close_conn.assert_called_once()

    @patch('scheduler_runner.utils.uploader.core.base_uploader.BaseUploader._close_connection')
    def test_disconnect_when_not_connected(self, mock_close_conn):
        """Тест отключения когда соединение не установлено"""
        self.uploader.connected = False

        result = self.uploader.disconnect()

        self.assertTrue(result)  # Отключение от несуществующего соединения считается успешным
        self.assertFalse(self.uploader.connected)
        mock_close_conn.assert_not_called()

    def test_validate_data_empty(self):
        """Тест валидации пустых данных"""
        result = self.uploader._validate_data({})
        self.assertFalse(result["success"])
        self.assertIn("пусты", result["error"])

    def test_validate_data_wrong_type(self):
        """Тест валидации данных неправильного типа"""
        result = self.uploader._validate_data("not_a_dict")
        self.assertFalse(result["success"])
        self.assertIn("словаря", result["error"])

    def test_validate_data_missing_required_fields(self):
        """Тест валидации данных с отсутствующими обязательными полями"""
        # Обновляем конфигурацию с обязательными полями
        self.uploader.config["REQUIRED_FIELDS"] = ["field1", "field2"]
        
        result = self.uploader._validate_data({"field1": "value1"})  # field2 отсутствует
        self.assertFalse(result["success"])
        self.assertIn("field2", result["error"])

    def test_validate_data_valid(self):
        """Тест валидации корректных данных"""
        # Обновляем конфигурацию с обязательными полями
        self.uploader.config["REQUIRED_FIELDS"] = ["field1", "field2"]
        
        result = self.uploader._validate_data({"field1": "value1", "field2": "value2"})
        self.assertTrue(result["success"])

    def test_transform_data_if_needed_with_transformer(self):
        """Тест трансформации данных когда трансформер указан"""
        # Трансформер должен вызываться внешними средствами, а не внутри _transform_data_if_needed
        # Метод просто возвращает данные как есть, если TRANSFORMER_CLASS указан
        self.uploader.config["TRANSFORMER_CLASS"] = "GoogleSheetsTransformer"

        result = self.uploader._transform_data_if_needed({"original": "data"})

        # Метод должен вернуть данные как есть, так как трансформация происходит вне изолированного микросервиса
        self.assertEqual(result, {"original": "data"})

    def test_transform_data_if_needed_without_transformer(self):
        """Тест трансформации данных когда трансформер не указан"""
        result = self.uploader._transform_data_if_needed({"original": "data"})
        self.assertEqual(result, {"original": "data"})

    def test_get_status(self):
        """Тест получения статуса загрузчика"""
        self.uploader.connected = True
        self.uploader.uploaded_count = 5
        self.uploader.failed_count = 2

        status = self.uploader.get_status()

        self.assertTrue(status["connected"])
        self.assertEqual(status["uploaded_count"], 5)
        self.assertEqual(status["failed_count"], 2)
        self.assertEqual(status["config_summary"]["target_system"], "test_system")

    def test_upload_data_when_not_connected(self):
        """Тест загрузки данных когда нет подключения"""
        result = self.uploader.upload_data({"field": "value"})
        
        self.assertFalse(result["success"])
        self.assertIn("подключения", result["error"])

    @patch('scheduler_runner.utils.uploader.core.base_uploader.time.sleep')
    def test_retry_operation_success_first_attempt(self, mock_sleep):
        """Тест повторной операции с успешным выполнением с первого раза"""
        def successful_operation():
            return "success"
        
        result = self.uploader.retry_operation(successful_operation, max_retries=2, delay=0.1)
        
        self.assertEqual(result, "success")
        mock_sleep.assert_not_called()

    @patch('scheduler_runner.utils.uploader.core.base_uploader.time.sleep')
    def test_retry_operation_success_second_attempt(self, mock_sleep):
        """Тест повторной операции с успешным выполнением со второго раза"""
        attempts = 0
        def operation_that_succeeds_second_time():
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise Exception("First attempt fails")
            return "success on second attempt"
        
        result = self.uploader.retry_operation(operation_that_succeeds_second_time, max_retries=2, delay=0.01)
        
        self.assertEqual(result, "success on second attempt")
        self.assertEqual(mock_sleep.call_count, 1)  # Задержка после первой неудачной попытки

    @patch('scheduler_runner.utils.uploader.core.base_uploader.time.sleep')
    def test_retry_operation_fails_all_attempts(self, mock_sleep):
        """Тест повторной операции с неудачей на всех попытках"""
        def operation_that_always_fails():
            raise Exception("Always fails")
        
        with self.assertRaises(Exception) as context:
            self.uploader.retry_operation(operation_that_always_fails, max_retries=2, delay=0.01)
        
        self.assertIn("Always fails", str(context.exception))
        self.assertEqual(mock_sleep.call_count, 2)  # Задержки после 1-й и 2-й неудачных попыток


if __name__ == '__main__':
    unittest.main()