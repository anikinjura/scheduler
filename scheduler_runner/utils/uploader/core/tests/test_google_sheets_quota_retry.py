"""
Тесты устойчивости к квоте Google Sheets API (429) и временным сбоям.

- QuotaBackoffHTTPClient повторяет 429 с паузой не меньше минуты, 408/5xx и сетевые ошибки — с экспоненциальной;
- ошибка API при поиске строки по ключам не превращается в «строка не найдена» (иначе upsert добавит дубликат).
"""
import unittest
from unittest.mock import MagicMock, Mock, patch

import gspread
import requests

from scheduler_runner.utils.uploader.core.providers.google_sheets.google_sheets_core import (
    GoogleSheetsReporter,
    QuotaBackoffHTTPClient,
    diff_google_sheets_request_stats,
    get_google_sheets_request_stats,
    retry_on_api_error,
)
from scheduler_runner.utils.uploader.core.providers.google_sheets.google_sheets_data_models import (
    ColumnDefinition,
    ColumnType,
    TableConfig,
)

BASE_REQUEST = "gspread.http_client.HTTPClient.request"
SLEEP = "scheduler_runner.utils.uploader.core.providers.google_sheets.google_sheets_core.time.sleep"


def api_error(code, message="error"):
    response = Mock()
    response.json.return_value = {"error": {"code": code, "message": message, "status": "X"}}
    return gspread.exceptions.APIError(response)


def make_client():
    client = QuotaBackoffHTTPClient(auth=Mock(), session=Mock())
    client.logger = MagicMock()
    return client


class TestQuotaBackoffHTTPClient(unittest.TestCase):
    @patch(SLEEP)
    @patch(BASE_REQUEST)
    def test_429_retried_after_at_least_a_minute(self, mock_request, mock_sleep):
        ok = Mock()
        mock_request.side_effect = [api_error(429, "Quota exceeded"), ok]

        result = make_client().request("get", "https://sheets.googleapis.com/v4/x")

        self.assertIs(result, ok)
        self.assertEqual(mock_request.call_count, 2)
        self.assertGreaterEqual(mock_sleep.call_args[0][0], 65)

    @patch(SLEEP)
    @patch(BASE_REQUEST)
    def test_server_error_retried_with_short_backoff(self, mock_request, mock_sleep):
        mock_request.side_effect = [api_error(503), Mock()]

        make_client().request("get", "url")

        self.assertLess(mock_sleep.call_args[0][0], 65)

    @patch(SLEEP)
    @patch(BASE_REQUEST)
    def test_connection_error_retried(self, mock_request, mock_sleep):
        mock_request.side_effect = [requests.exceptions.ConnectionError("reset"), Mock()]

        make_client().request("get", "url")

        self.assertEqual(mock_request.call_count, 2)

    @patch(SLEEP)
    @patch(BASE_REQUEST)
    def test_client_error_not_retried(self, mock_request, mock_sleep):
        mock_request.side_effect = api_error(400)

        with self.assertRaises(gspread.exceptions.APIError):
            make_client().request("get", "url")

        self.assertEqual(mock_request.call_count, 1)
        mock_sleep.assert_not_called()

    @patch(SLEEP)
    @patch(BASE_REQUEST)
    def test_gives_up_after_max_attempts(self, mock_request, mock_sleep):
        mock_request.side_effect = api_error(429)
        client = make_client()

        with self.assertRaises(gspread.exceptions.APIError):
            client.request("get", "url")

        self.assertEqual(mock_request.call_count, client.max_attempts)
        self.assertEqual(mock_sleep.call_count, client.max_attempts - 1)


class TestRequestStats(unittest.TestCase):
    @patch(SLEEP)
    @patch(BASE_REQUEST)
    def test_counts_reads_writes_and_retries(self, mock_request, mock_sleep):
        mock_request.side_effect = [Mock(), api_error(429), Mock(), Mock()]
        before = get_google_sheets_request_stats()
        client = make_client()

        client.request("get", "values:batchGet")        # 1 чтение
        client.request("post", "values:append")         # 429 + повтор = 2 записи
        client.request("put", "values/A1")              # 1 запись

        self.assertEqual(client.stats, {"reads": 1, "writes": 3, "retries_429": 1, "retries_other": 0})
        self.assertEqual(diff_google_sheets_request_stats(before),
                         {"reads": 1, "writes": 3, "retries_429": 1, "retries_other": 0})


class TestHeaderCache(unittest.TestCase):
    def test_header_read_once_per_connection(self):
        reporter = GoogleSheetsReporter.__new__(GoogleSheetsReporter)
        reporter.logger = MagicMock()
        reporter.worksheet = MagicMock()
        reporter.worksheet.row_values.return_value = ["id", "work_date"]

        first = reporter._get_headers()
        first.append("mutated")  # вызывающий код не портит кэш
        second = reporter._get_headers()

        self.assertEqual(second, ["id", "work_date"])
        reporter.worksheet.row_values.assert_called_once_with(1)


class TestRetryDecoratorSkipsTransportCodes(unittest.TestCase):
    @patch(SLEEP)
    def test_429_not_retried_again_by_decorator(self, mock_sleep):
        calls = []

        @retry_on_api_error(max_retries=3)
        def operation():
            calls.append(1)
            raise api_error(429)

        with self.assertRaises(gspread.exceptions.APIError):
            operation()
        self.assertEqual(len(calls), 1)
        mock_sleep.assert_not_called()


class TestLookupDoesNotHideApiErrors(unittest.TestCase):
    def setUp(self):
        self.config = TableConfig(
            worksheet_name="KPI",
            id_column="id",
            columns=[
                ColumnDefinition(name="id", column_type=ColumnType.FORMULA, formula_template="=B{row}&C{row}"),
                ColumnDefinition(name="work_date", column_type=ColumnType.DATA, unique_key=True, required=True),
                ColumnDefinition(name="object_name", column_type=ColumnType.DATA, unique_key=True, required=True),
                ColumnDefinition(name="issued_packages", column_type=ColumnType.DATA),
            ],
            unique_key_columns=["work_date", "object_name"],
        )
        self.reporter = GoogleSheetsReporter.__new__(GoogleSheetsReporter)
        self.reporter.table_config = self.config
        self.reporter.logger = MagicMock()
        self.reporter.worksheet = MagicMock()
        self.reporter.worksheet.row_values.return_value = ["id", "work_date", "object_name", "issued_packages"]
        self.reporter.worksheet.col_values.return_value = ["work_date", "30.09.2026"]
        self.reporter.worksheet.batch_get.side_effect = api_error(429, "Quota exceeded")

    def test_get_rows_by_unique_keys_raises_instead_of_not_found(self):
        with self.assertRaises(gspread.exceptions.APIError):
            self.reporter.get_rows_by_unique_keys(
                {"work_date": "30.09.2026", "object_name": "ЧЕБОКСАРЫ_144"}, config=self.config, first_only=True
            )

    def test_upsert_returns_error_and_does_not_append_duplicate(self):
        with patch.object(self.reporter, "_append_new_row") as mock_append, \
                patch.object(self.reporter, "_update_existing_row") as mock_update:
            result = self.reporter.update_or_append_data_with_config(
                data={"work_date": "30.09.2026", "object_name": "ЧЕБОКСАРЫ_144", "issued_packages": 425},
                config=self.config,
                strategy="update_or_append",
            )

        self.assertFalse(result["success"])
        mock_append.assert_not_called()
        mock_update.assert_not_called()


if __name__ == "__main__":
    unittest.main()
