"""
Юнит-тесты для функциональности check_missing_items (coverage-check)

Тестируется новый API для поиска отсутствующих комбинаций ключей unique_key_columns
в Google Sheets с использованием batch_get.

Author: anikinjura
"""
import unittest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime
from itertools import product

from scheduler_runner.utils.uploader.core.providers.google_sheets.google_sheets_core import GoogleSheetsReporter
from scheduler_runner.utils.uploader.core.providers.google_sheets.google_sheets_data_models import (
    TableConfig, ColumnDefinition, ColumnType
)
from scheduler_runner.utils.uploader.implementations.google_sheets_uploader import GoogleSheetsUploader
from scheduler_runner.utils.uploader.interface import check_missing_items as interface_check_missing_items


class TestGoogleSheetsReporterCoverageCheck(unittest.TestCase):
    """Тесты для GoogleSheetsReporter.check_missing_items()"""

    def setUp(self):
        """Настройка тестов"""
        # Создаем конфигурацию с coverage_filter метаданными
        self.table_config = TableConfig(
            worksheet_name="KPI",
            id_column="id",
            columns=[
                ColumnDefinition(name="id", column_type=ColumnType.FORMULA, formula_template="=B{row}&C{row}"),
                ColumnDefinition(
                    name="Дата",
                    column_type=ColumnType.DATA,
                    required=True,
                    unique_key=True,
                    coverage_filter=True,
                    coverage_filter_type="date_range",
                    date_input_format="YYYY-MM-DD",
                    date_output_format="DD.MM.YYYY"
                ),
                ColumnDefinition(
                    name="ПВЗ",
                    column_type=ColumnType.DATA,
                    required=True,
                    unique_key=True,
                    coverage_filter=True,
                    coverage_filter_type="list",
                    normalization="strip_lower_str"
                ),
                ColumnDefinition(name="Количество выдач", column_type=ColumnType.DATA),
            ],
            unique_key_columns=["Дата", "ПВЗ"]
        )
        
        # Создаем экземпляр GoogleSheetsReporter через __new__ (без реального подключения)
        self.reporter = GoogleSheetsReporter.__new__(GoogleSheetsReporter)
        self.reporter.table_config = self.table_config
        self.reporter.logger = MagicMock()
        self.reporter.worksheet = MagicMock()
        self.reporter.worksheet.row_count = 100

    def _setup_mock_headers(self, headers):
        """Установка мока для заголовков"""
        self.reporter.worksheet.row_values.return_value = headers

    def _setup_mock_batch_get(self, date_values, pvz_values):
        """Установка мока для batch_get"""
        # Формат: list of ValueRange объектов [[val1], [val2], ...]
        self.reporter.worksheet.batch_get.return_value = [
            [[d] for d in date_values],
            [[p] for p in pvz_values]
        ]

    def test_check_missing_items_success(self):
        """Тест успешной проверки missing_items"""
        # Setup
        self._setup_mock_headers(["id", "Дата", "ПВЗ", "Количество выдач"])
        # Присутствуют даты 01.03.2026 и 02.03.2026 для msk-01
        self._setup_mock_batch_get(
            date_values=["01.03.2026", "02.03.2026", ""],
            pvz_values=["msk-01", "msk-01", ""]
        )
        
        filters = {
            "Дата_from": "2026-03-01",
            "Дата_to": "2026-03-03",
            "ПВЗ": ["msk-01"]
        }
        
        # Execute
        result = self.reporter.check_missing_items(filters=filters, config=self.table_config)
        
        # Assert
        self.assertTrue(result["success"])
        self.assertEqual(result["action"], "coverage_check")
        self.assertIsNotNone(result["data"])
        
        data = result["data"]
        self.assertEqual(len(data["missing_items"]), 1)
        self.assertEqual(data["missing_items"][0]["Дата"], "03.03.2026")
        self.assertEqual(data["missing_items"][0]["ПВЗ"], "msk-01")
        
        # Проверка missing_by_key (группировка по unique_key_columns[0] = "Дата")
        self.assertIn("03.03.2026", data["missing_by_key"])
        
        # Проверка stats
        stats = data["stats"]
        self.assertEqual(stats["expected_keys"], 3)  # 3 даты × 1 ПВЗ
        self.assertEqual(stats["present_keys"], 2)  # 2 присутствуют
        self.assertEqual(stats["missing_keys"], 1)
        self.assertIn("batch_get_ms", stats)
        self.assertIn("read_cells_est", stats)
        
        # Проверка diagnostics
        diagnostics = data["diagnostics"]
        self.assertIn("ranges", diagnostics)
        self.assertEqual(len(diagnostics["ranges"]), 2)  # 2 колонки

    def test_check_missing_items_multiple_pvz(self):
        """Тест с несколькими значениями ПВЗ в фильтре"""
        # Setup
        self._setup_mock_headers(["id", "Дата", "ПВЗ", "Количество выдач"])
        # Присутствует только 01.03.2026 для msk-01
        self._setup_mock_batch_get(
            date_values=["01.03.2026", "", ""],
            pvz_values=["msk-01", "", ""]
        )
        
        filters = {
            "Дата_from": "2026-03-01",
            "Дата_to": "2026-03-02",
            "ПВЗ": ["msk-01", "msk-02"]
        }
        
        # Execute
        result = self.reporter.check_missing_items(filters=filters, config=self.table_config)
        
        # Assert
        self.assertTrue(result["success"])
        
        data = result["data"]
        # Ожидаем 4 комбинации (2 даты × 2 ПВЗ), присутствует 1, missing = 3
        self.assertEqual(len(data["missing_items"]), 3)
        self.assertEqual(data["stats"]["expected_keys"], 4)
        self.assertEqual(data["stats"]["missing_keys"], 3)

    def test_check_missing_items_normalization_strip_lower_str(self):
        """Тест нормализации strip_lower_str для ПВЗ"""
        # Setup
        self._setup_mock_headers(["id", "Дата", "ПВЗ", "Количество выдач"])
        # В таблице значения в верхнем регистре
        self._setup_mock_batch_get(
            date_values=["01.03.2026", ""],
            pvz_values=["MSK-01", ""]
        )
        
        filters = {
            "Дата_from": "2026-03-01",
            "Дата_to": "2026-03-01",
            "ПВЗ": ["msk-01"]  # Фильтр в нижнем регистре
        }
        
        # Execute
        result = self.reporter.check_missing_items(filters=filters, config=self.table_config)
        
        # Assert
        self.assertTrue(result["success"])
        # Нормализация должна совпасть: MSK-01 → msk-01
        self.assertEqual(len(result["data"]["missing_items"]), 0)
        self.assertEqual(result["data"]["stats"]["present_keys"], 1)

    def test_check_missing_items_date_range_invalid_format(self):
        """Тест с некорректным форматом даты"""
        # Setup
        self._setup_mock_headers(["id", "Дата", "ПВЗ", "Количество выдач"])
        
        filters = {
            "Дата_from": "01-03-2026",  # Неверный формат (должен быть YYYY-MM-DD)
            "Дата_to": "2026-03-03",
            "ПВЗ": ["msk-01"]
        }
        
        # Execute
        result = self.reporter.check_missing_items(filters=filters, config=self.table_config)
        
        # Assert
        self.assertFalse(result["success"])
        self.assertIn("Некорректный формат даты", result["error"])

    def test_check_missing_items_date_from_greater_than_to(self):
        """Тест когда date_from > date_to"""
        # Setup
        self._setup_mock_headers(["id", "Дата", "ПВЗ", "Количество выдач"])
        
        filters = {
            "Дата_from": "2026-03-05",
            "Дата_to": "2026-03-01",
            "ПВЗ": ["msk-01"]
        }
        
        # Execute
        result = self.reporter.check_missing_items(filters=filters, config=self.table_config)
        
        # Assert
        self.assertFalse(result["success"])
        self.assertIn("больше даты", result["error"])

    def test_check_missing_items_missing_columns_strict_headers(self):
        """Тест отсутствия колонок в таблице (strict_headers=True)"""
        # Setup - заголовки без колонки "ПВЗ"
        self._setup_mock_headers(["id", "Дата", "Количество выдач"])
        
        filters = {
            "Дата_from": "2026-03-01",
            "Дата_to": "2026-03-03",
            "ПВЗ": ["msk-01"]
        }
        
        # Execute
        result = self.reporter.check_missing_items(
            filters=filters,
            config=self.table_config,
            strict_headers=True
        )
        
        # Assert
        self.assertFalse(result["success"])
        self.assertIn("Missing columns", result["error"])
        self.assertNotIn("Available columns", result["error"])

    def test_check_missing_items_missing_columns_non_strict_headers(self):
        """Тест отсутствия колонок в таблице (strict_headers=False)"""
        # Setup - заголовки без колонки "ПВЗ"
        self._setup_mock_headers(["id", "Дата", "Количество выдач"])
        
        filters = {
            "Дата_from": "2026-03-01",
            "Дата_to": "2026-03-03",
            "ПВЗ": ["msk-01"]
        }
        
        # Execute
        result = self.reporter.check_missing_items(
            filters=filters,
            config=self.table_config,
            strict_headers=False
        )
        
        # Assert
        self.assertFalse(result["success"])
        self.assertIn("Missing columns", result["error"])
        self.assertIn("Available columns", result["error"])

    def test_check_missing_items_unique_key_columns_not_covered(self):
        """Тест когда unique_key_columns не полностью покрыты фильтрами"""
        # Setup - конфигурация где только "Дата" имеет coverage_filter=True
        config_partial_coverage = TableConfig(
            worksheet_name="KPI",
            id_column="id",
            columns=[
                ColumnDefinition(name="id", column_type=ColumnType.FORMULA, formula_template="=B{row}&C{row}"),
                ColumnDefinition(
                    name="Дата",
                    column_type=ColumnType.DATA,
                    required=True,
                    unique_key=True,
                    coverage_filter=True,
                    coverage_filter_type="date_range",
                    date_input_format="YYYY-MM-DD",
                    date_output_format="DD.MM.YYYY"
                ),
                ColumnDefinition(name="ПВЗ", column_type=ColumnType.DATA, required=True, unique_key=True),
                # ПВЗ не имеет coverage_filter=True
            ],
            unique_key_columns=["Дата", "ПВЗ"]
        )
        
        self._setup_mock_headers(["id", "Дата", "ПВЗ"])
        
        # Передаем фильтр только для Даты (ПВЗ не покрыт)
        filters = {
            "Дата_from": "2026-03-01",
            "Дата_to": "2026-03-03",
            # ПВЗ отсутствует
        }
        
        # Execute
        result = self.reporter.check_missing_items(filters=filters, config=config_partial_coverage)
        
        # Assert
        self.assertFalse(result["success"])
        self.assertIn("unique_key_columns не полностью покрыты", result["error"])

    def test_check_missing_items_fallback_to_unique_keys_without_config_mutation(self):
        """Fallback на unique_key_columns не должен мутировать исходный TableConfig"""
        fallback_config = TableConfig(
            worksheet_name="KPI",
            id_column="id",
            columns=[
                ColumnDefinition(name="id", column_type=ColumnType.FORMULA, formula_template="=B{row}&C{row}"),
                ColumnDefinition(
                    name="Дата",
                    column_type=ColumnType.DATA,
                    required=True,
                    unique_key=True,
                    date_input_format="YYYY-MM-DD",
                    date_output_format="DD.MM.YYYY"
                ),
                ColumnDefinition(
                    name="ПВЗ",
                    column_type=ColumnType.DATA,
                    required=True,
                    unique_key=True
                ),
            ],
            unique_key_columns=["Дата", "ПВЗ"]
        )

        self._setup_mock_headers(["id", "Дата", "ПВЗ"])
        self._setup_mock_batch_get(
            date_values=["01.03.2026", ""],
            pvz_values=["MSK-01", ""]
        )

        filters = {
            "Дата_from": "2026-03-01",
            "Дата_to": "2026-03-01",
            "ПВЗ": "msk-01"
        }

        result = self.reporter.check_missing_items(filters=filters, config=fallback_config)

        self.assertTrue(result["success"])
        self.assertEqual(result["data"]["stats"]["present_keys"], 1)

        date_col = fallback_config.get_column("Дата")
        pvz_col = fallback_config.get_column("ПВЗ")
        self.assertFalse(date_col.coverage_filter)
        self.assertIsNone(date_col.coverage_filter_type)
        self.assertFalse(pvz_col.coverage_filter)
        self.assertIsNone(pvz_col.coverage_filter_type)
        self.assertIsNone(pvz_col.normalization)

    def test_check_missing_items_max_expected_keys_exceeded(self):
        """Тест превышения лимита MAX_EXPECTED_KEYS"""
        # Setup
        self._setup_mock_headers(["id", "Дата", "ПВЗ", "Количество выдач"])
        
        filters = {
            "Дата_from": "2026-01-01",
            "Дата_to": "2026-12-31",  # 365 дней
            "ПВЗ": [f"pvz-{i}" for i in range(500)]  # 500 ПВЗ
        }
        
        # Execute с маленьким лимитом
        result = self.reporter.check_missing_items(
            filters=filters,
            config=self.table_config,
            max_expected_keys=1000
        )
        
        # Assert
        self.assertFalse(result["success"])
        self.assertIn("Превышен лимит ожидаемых ключей", result["error"])

    def test_check_missing_items_duplicates_detection(self):
        """Тест обнаружения дубликатов"""
        # Setup
        self._setup_mock_headers(["id", "Дата", "ПВЗ", "Количество выдач"])
        # Дубликат: 01.03.2026 + msk-01 встречается дважды
        self._setup_mock_batch_get(
            date_values=["01.03.2026", "01.03.2026", ""],
            pvz_values=["msk-01", "msk-01", ""]
        )
        
        filters = {
            "Дата_from": "2026-03-01",
            "Дата_to": "2026-03-01",
            "ПВЗ": ["msk-01"]
        }
        
        # Execute
        result = self.reporter.check_missing_items(filters=filters, config=self.table_config)
        
        # Assert
        self.assertTrue(result["success"])
        stats = result["data"]["stats"]
        self.assertEqual(stats["duplicates_keys_count"], 1)
        
        diagnostics = result["data"]["diagnostics"]
        self.assertEqual(len(diagnostics["duplicates_samples"]), 1)
        self.assertEqual(diagnostics["duplicates_samples"][0]["count"], 2)

    def test_check_missing_items_anomalies_detection(self):
        """Тест обнаружения аномалий (пустые значения в required колонках)"""
        # Setup
        self._setup_mock_headers(["id", "Дата", "ПВЗ", "Количество выдач"])
        # Пустая дата во второй строке
        self._setup_mock_batch_get(
            date_values=["01.03.2026", "", ""],
            pvz_values=["msk-01", "msk-01", ""]
        )
        
        filters = {
            "Дата_from": "2026-03-01",
            "Дата_to": "2026-03-01",
            "ПВЗ": ["msk-01"]
        }
        
        # Execute
        result = self.reporter.check_missing_items(filters=filters, config=self.table_config)
        
        # Assert
        self.assertTrue(result["success"])
        stats = result["data"]["stats"]
        self.assertGreater(stats["anomalies_count"], 0)
        
        diagnostics = result["data"]["diagnostics"]
        self.assertGreater(len(diagnostics["anomalies_samples"]), 0)
        self.assertIn("empty_value_in_required_columns", diagnostics["anomalies_samples"][0]["reason"])

    def test_check_missing_items_batch_get_called_once(self):
        """Тест что batch_get вызывается только один раз"""
        # Setup
        self._setup_mock_headers(["id", "Дата", "ПВЗ", "Количество выдач"])
        self._setup_mock_batch_get(
            date_values=["01.03.2026", ""],
            pvz_values=["msk-01", ""]
        )
        
        filters = {
            "Дата_from": "2026-03-01",
            "Дата_to": "2026-03-01",
            "ПВЗ": ["msk-01"]
        }
        
        # Execute
        result = self.reporter.check_missing_items(filters=filters, config=self.table_config)
        
        # Assert
        self.assertTrue(result["success"])
        self.assertEqual(self.reporter.worksheet.batch_get.call_count, 1)

    def test_check_missing_items_read_cells_est_formula(self):
        """Тест формулы расчёта read_cells_est"""
        # Setup
        self._setup_mock_headers(["id", "Дата", "ПВЗ", "Количество выдач"])
        self._setup_mock_batch_get(
            date_values=["01.03.2026"] * 49,  # 49 строк
            pvz_values=["msk-01"] * 49
        )
        # Мокаем get_last_row_with_data чтобы вернуть 50 (header_row=1 + 49 строк данных)
        self.reporter.get_last_row_with_data = Mock(return_value=50)
        
        filters = {
            "Дата_from": "2026-03-01",
            "Дата_to": "2026-03-01",
            "ПВЗ": ["msk-01"]
        }
        
        # Execute
        result = self.reporter.check_missing_items(filters=filters, config=self.table_config)
        
        # Assert
        self.assertTrue(result["success"])
        stats = result["data"]["stats"]
        # read_cells_est = scanned_rows * len(coverage_columns)
        # scanned_rows = max_row - header_row = (50 + 10 buffer) - 1 = 59, но ограничено row_count=100
        # Фактически: scanned_rows = 59, read_cells_est = 59 * 2 = 118
        # Но если last_data_row=50, то max_row = min(100, 50+10) = 60
        # scanned_rows = 60 - 1 = 59
        self.assertEqual(stats["read_cells_est"], 118)  # 59 * 2


class TestGoogleSheetsUploaderCoverageCheck(unittest.TestCase):
    """Тесты для GoogleSheetsUploader.check_missing_items()"""

    def setUp(self):
        """Настройка тестов"""
        self.config = {
            "CREDENTIALS_PATH": "./test_credentials.json",
            "SPREADSHEET_ID": "test_spreadsheet_id",
            "WORKSHEET_NAME": "Test Sheet",
            "TABLE_CONFIG": None
        }
        self.uploader = GoogleSheetsUploader(config=self.config)

    @patch('pathlib.Path.exists', return_value=True)
    @patch('scheduler_runner.utils.uploader.core.providers.google_sheets.google_sheets_core.GoogleSheetsReporter')
    def test_check_missing_items_delegation(self, mock_reporter_class, mock_path_exists):
        """Тест делегирования в sheets_reporter"""
        # Setup
        mock_reporter_instance = Mock()
        mock_reporter_instance.check_missing_items.return_value = {
            "success": True,
            "action": "coverage_check",
            "data": {"missing_items": []},
            "error": None
        }
        mock_reporter_class.return_value = mock_reporter_instance
        
        self.uploader.connected = True
        self.uploader.sheets_reporter = mock_reporter_instance
        
        filters = {
            "Дата_from": "2026-03-01",
            "Дата_to": "2026-03-03",
            "ПВЗ": ["msk-01"]
        }
        
        # Execute
        result = self.uploader.check_missing_items(filters=filters)
        
        # Assert
        self.assertTrue(result["success"])
        mock_reporter_instance.check_missing_items.assert_called_once()

    def test_check_missing_items_not_connected(self):
        """Тест когда нет подключения"""
        self.uploader.connected = False
        
        filters = {
            "Дата_from": "2026-03-01",
            "Дата_to": "2026-03-03",
            "ПВЗ": ["msk-01"]
        }
        
        # Execute
        result = self.uploader.check_missing_items(filters=filters)
        
        # Assert
        self.assertFalse(result["success"])
        self.assertEqual(result["error"], "Нет подключения к Google Sheets")


class TestInterfaceCoverageCheck(unittest.TestCase):
    """Тесты для interface.check_missing_items()"""

    @patch('scheduler_runner.utils.uploader.interface.GoogleSheetsUploader')
    def test_check_missing_items_forwards_kwargs(self, mock_uploader_class):
        mock_uploader = Mock()
        mock_uploader.connect.return_value = True
        mock_uploader.check_missing_items.return_value = {
            "success": True,
            "action": "coverage_check",
            "data": {"missing_items": []},
            "error": None
        }
        mock_uploader.disconnect.return_value = True
        mock_uploader_class.return_value = mock_uploader

        filters = {
            "Дата_from": "2026-03-01",
            "Дата_to": "2026-03-03",
            "ПВЗ": ["msk-01"]
        }
        connection_params = {
            "CREDENTIALS_PATH": "./test_credentials.json",
            "SPREADSHEET_ID": "test_spreadsheet_id",
            "WORKSHEET_NAME": "Test Sheet",
            "TABLE_CONFIG": Mock()
        }

        result = interface_check_missing_items(
            filters=filters,
            connection_params=connection_params,
            strict_headers=False,
            max_scan_rows=250,
            max_expected_keys=1234
        )

        self.assertTrue(result["success"])
        mock_uploader.connect.assert_called_once()
        mock_uploader.check_missing_items.assert_called_once_with(
            filters=filters,
            strict_headers=False,
            max_scan_rows=250,
            max_expected_keys=1234
        )
        mock_uploader.disconnect.assert_called_once()


class TestNormalizeValue(unittest.TestCase):
    """Тесты для хелпера _normalize_value"""

    def setUp(self):
        """Настройка тестов"""
        self.reporter = GoogleSheetsReporter.__new__(GoogleSheetsReporter)

    def test_normalize_strip_lower_str(self):
        """Тест нормализации strip_lower_str"""
        self.assertEqual(self.reporter._normalize_value("MSK-01", "strip_lower_str"), "msk-01")
        self.assertEqual(self.reporter._normalize_value("  MSK-01  ", "strip_lower_str"), "msk-01")
        self.assertEqual(self.reporter._normalize_value("", "strip_lower_str"), "")
        self.assertEqual(self.reporter._normalize_value(None, "strip_lower_str"), "")

    def test_normalize_int(self):
        """Тест нормализации int"""
        self.assertEqual(self.reporter._normalize_value("123", "int"), 123)
        self.assertEqual(self.reporter._normalize_value(123, "int"), 123)
        self.assertEqual(self.reporter._normalize_value("", "int"), 0)
        self.assertEqual(self.reporter._normalize_value(None, "int"), 0)

    def test_normalize_none(self):
        """Тест нормализации none"""
        self.assertEqual(self.reporter._normalize_value("MSK-01", "none"), "MSK-01")
        self.assertEqual(self.reporter._normalize_value("  MSK-01  ", "none"), "MSK-01")
        self.assertEqual(self.reporter._normalize_value("", "none"), "")
        self.assertEqual(self.reporter._normalize_value(None, "none"), "")

    def test_normalize_default(self):
        """Тест нормализации по умолчанию (None normalization)"""
        self.assertEqual(self.reporter._normalize_value("MSK-01", None), "MSK-01")
        self.assertEqual(self.reporter._normalize_value("  test  ", None), "test")


if __name__ == '__main__':
    unittest.main()
