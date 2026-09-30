"""
Тесты пакетного upsert KPI (GoogleSheetsReporter.upsert_rows_batch) без сети.

Главное свойство — константное число вызовов листа на пакет: 1 batch_get, 1 append_rows, 1 batch_update,
без row_values / col_values / acell.
"""
import unittest
from unittest.mock import MagicMock

import gspread

from scheduler_runner.utils.uploader.core.providers.google_sheets.google_sheets_core import GoogleSheetsReporter
from scheduler_runner.utils.uploader.core.providers.google_sheets.google_sheets_data_models import (
    ColumnDefinition,
    ColumnType,
    TableConfig,
)

HEADERS = ["id", "work_date", "object_name", "issued_packages", "direct_flow", "return_flow",
           "reward_issued_packages", "reward_direct_flow", "reward_return_flow", "total_reward", "timestamp"]


def kpi_config():
    return TableConfig(
        worksheet_name="KPI",
        id_column="id",
        columns=[
            ColumnDefinition(name="id", column_type=ColumnType.FORMULA, formula_template="=B{row}&C{row}"),
            ColumnDefinition(name="work_date", column_type=ColumnType.DATA, required=True, unique_key=True),
            ColumnDefinition(name="object_name", column_type=ColumnType.DATA, required=True, unique_key=True),
            ColumnDefinition(name="issued_packages", column_type=ColumnType.DATA),
            ColumnDefinition(name="direct_flow", column_type=ColumnType.DATA),
            ColumnDefinition(name="return_flow", column_type=ColumnType.DATA),
            ColumnDefinition(name="reward_issued_packages", column_type=ColumnType.FORMULA,
                             formula_template='=GET_REWARD("issued_packages";D{row};$B{row};KPI_REWARD_RULES_RANGE)'),
            ColumnDefinition(name="reward_direct_flow", column_type=ColumnType.FORMULA,
                             formula_template='=GET_REWARD("direct_flow";E{row};$B{row};KPI_REWARD_RULES_RANGE)'),
            ColumnDefinition(name="reward_return_flow", column_type=ColumnType.FORMULA,
                             formula_template='=GET_REWARD("return_flow";F{row};$B{row};KPI_REWARD_RULES_RANGE)'),
            ColumnDefinition(name="total_reward", column_type=ColumnType.FORMULA, formula_template="=SUM(G{row}:I{row})"),
            ColumnDefinition(name="timestamp", column_type=ColumnType.DATA),
        ],
        unique_key_columns=["work_date", "object_name"],
    )


def record(day, pvz="ЧЕБОКСАРЫ_144", issued=100, ts="2026-09-30 21:40:00"):
    return {"work_date": f"{day:02d}.09.2026", "object_name": pvz, "issued_packages": issued,
            "direct_flow": 10, "return_flow": 5, "timestamp": ts}


def column(values):
    """ValueRange одной колонки в формате batch_get: список строк по одному значению."""
    return [[v] if v != "" else [] for v in values]


def api_error(code):
    response = MagicMock()
    response.json.return_value = {"error": {"code": code, "message": "error", "status": "X"}}
    return gspread.exceptions.APIError(response)


class TestUpsertRowsBatch(unittest.TestCase):
    def setUp(self):
        self.config = kpi_config()
        self.config.build_column_indexes(HEADERS)
        self.reporter = GoogleSheetsReporter.__new__(GoogleSheetsReporter)
        self.reporter.table_config = self.config
        self.reporter.logger = MagicMock()
        self.reporter._headers = list(HEADERS)
        self.ws = MagicMock()
        self.reporter.worksheet = self.ws
        # В листе: строки 2-3 с данными 144 за 20 и 21 сентября
        self.ws.batch_get.return_value = [
            column(["20.09.2026", "21.09.2026"]),
            column(["ЧЕБОКСАРЫ_144", "ЧЕБОКСАРЫ_144"]),
            column(["2026-09-20 21:40:00", "2026-09-21 21:40:00"]),
        ]

    def assert_no_row_level_reads(self):
        self.ws.row_values.assert_not_called()
        self.ws.col_values.assert_not_called()
        self.ws.acell.assert_not_called()

    def test_new_rows_one_append_and_one_batch_update_with_formulas(self):
        self.ws.append_rows.return_value = {"updates": {"updatedRange": "'KPI'!A4:K10"}}
        data = [record(d) for d in range(23, 30)]  # 7 новых дат

        result = self.reporter.upsert_rows_batch(data, config=self.config)

        self.assertTrue(result["success"])
        self.assertEqual((result["uploaded"], result["failed"]), (7, 0))
        self.assertEqual(result["diagnostics"]["appended"], 7)
        self.assertEqual(self.ws.batch_get.call_count, 1)
        self.assertEqual(self.ws.append_rows.call_count, 1)
        self.assertEqual(self.ws.batch_update.call_count, 1)
        self.assert_no_row_level_reads()

        appended_values = self.ws.append_rows.call_args.kwargs["values"]
        self.assertEqual(len(appended_values), 7)
        self.assertEqual(appended_values[0][0], "")            # id — формула пишется вторым запросом
        self.assertEqual(appended_values[0][1], "23.09.2026")
        self.assertEqual(appended_values[0][10], "2026-09-30 21:40:00")

        cells = {c["range"]: c["values"][0][0] for c in self.ws.batch_update.call_args.args[0]}
        self.assertEqual(cells["A4"], "=B4&C4")
        self.assertEqual(cells["J10"], "=SUM(G10:I10)")
        self.assertIn("D7", cells["G7"])
        self.assertEqual(len(cells), 7 * 5)
        self.assertEqual(self.ws.batch_update.call_args.kwargs["value_input_option"], "USER_ENTERED")

    def test_existing_rows_updated_in_one_request_timestamp_preserved(self):
        data = [record(20, issued=111, ts="2026-09-30 22:00:00"), record(21, issued=222)]

        result = self.reporter.upsert_rows_batch(data, config=self.config)

        self.assertTrue(result["success"])
        self.assertEqual(result["diagnostics"]["updated"], 2)
        self.ws.append_rows.assert_not_called()
        self.assertEqual(self.ws.batch_update.call_count, 1)
        self.assert_no_row_level_reads()
        rows = {c["range"]: c["values"][0] for c in self.ws.batch_update.call_args.args[0]}
        self.assertEqual(rows["A2:K2"][0], "=B2&C2")
        self.assertEqual(rows["A2:K2"][3], 111)
        self.assertEqual(rows["A2:K2"][10], "2026-09-20 21:40:00")  # timestamp из листа, не из данных
        self.assertEqual(rows["A3:K3"][3], 222)

    def test_mixed_batch(self):
        self.ws.append_rows.return_value = {"updates": {"updatedRange": "'KPI'!A4:K4"}}

        result = self.reporter.upsert_rows_batch([record(21, issued=5), record(22)], config=self.config)

        actions = [d["result"]["action"] for d in result["details"]]
        self.assertEqual(actions, ["updated", "appended"])
        self.assertEqual(self.ws.batch_update.call_count, 1)
        ranges = [c["range"] for c in self.ws.batch_update.call_args.args[0]]
        self.assertIn("A3:K3", ranges)
        self.assertIn("A4", ranges)

    def test_duplicate_input_last_wins(self):
        self.ws.append_rows.return_value = {"updates": {"updatedRange": "'KPI'!A4:K4"}}

        result = self.reporter.upsert_rows_batch([record(25, issued=1), record(25, issued=2)], config=self.config)

        self.assertTrue(result["success"])
        self.assertEqual(result["details"][0]["result"]["action"], "superseded")
        self.assertEqual(result["details"][1]["result"]["action"], "appended")
        self.assertEqual(self.ws.append_rows.call_args.kwargs["values"][0][3], 2)

    def test_key_normalization_matches_sheet_values(self):
        # Нормализация ключей та же, что в построчном режиме (_normalize_for_comparison): пробелы по краям
        # не создают новую строку (регистр учитывается — как и раньше)
        self.ws.batch_get.return_value = [column([" 20.09.2026"]), column([" ЧЕБОКСАРЫ_144 "]), column([""])]

        result = self.reporter.upsert_rows_batch([record(20)], config=self.config)

        self.assertEqual(result["details"][0]["result"]["action"], "updated")
        self.ws.append_rows.assert_not_called()

    def test_duplicate_rows_in_sheet_use_first(self):
        self.ws.batch_get.return_value = [
            column(["20.09.2026", "20.09.2026"]), column(["ЧЕБОКСАРЫ_144", "ЧЕБОКСАРЫ_144"]), column(["a", "b"])]

        result = self.reporter.upsert_rows_batch([record(20)], config=self.config)

        self.assertEqual(result["diagnostics"]["duplicate_sheet_rows"], 1)
        self.assertEqual(result["details"][0]["result"]["row_number"], 2)

    def test_unparsed_append_range_falls_back_to_key_lookup(self):
        self.ws.append_rows.return_value = {"updates": {}}
        after_append = [column(["20.09.2026", "21.09.2026", "22.09.2026"]),
                        column(["ЧЕБОКСАРЫ_144"] * 3), column(["x", "y", ""])]
        self.ws.batch_get.side_effect = [self.ws.batch_get.return_value, after_append]

        result = self.reporter.upsert_rows_batch([record(22)], config=self.config)

        self.assertTrue(result["success"])
        self.assertTrue(result["diagnostics"]["append_range_fallback"])
        self.assertEqual(result["details"][0]["result"]["row_number"], 4)
        self.assertEqual(self.ws.batch_get.call_count, 2)

    def test_append_error_fails_all_new_rows(self):
        self.ws.append_rows.side_effect = api_error(429)

        result = self.reporter.upsert_rows_batch([record(22), record(23)], config=self.config)

        self.assertFalse(result["success"])
        self.assertEqual(result["failed"], 2)
        self.assertIn("429", result["error"])
        self.ws.batch_update.assert_not_called()

    def test_formula_write_error_reported_for_retry(self):
        self.ws.append_rows.return_value = {"updates": {"updatedRange": "'KPI'!A4:K4"}}
        self.ws.batch_update.side_effect = api_error(503)

        result = self.reporter.upsert_rows_batch([record(22)], config=self.config)

        self.assertFalse(result["success"])
        self.assertIn("повтор загрузки допишет", result["details"][0]["result"]["message"])

    def test_retry_after_formula_failure_updates_row_with_formulas(self):
        # Строка 4 добавлена прошлой попыткой без формул — повтор находит ее и пишет полную строку
        self.ws.batch_get.return_value = [
            column(["20.09.2026", "21.09.2026", "22.09.2026"]), column(["ЧЕБОКСАРЫ_144"] * 3), column(["a", "b", "c"])]

        result = self.reporter.upsert_rows_batch([record(22)], config=self.config)

        self.assertEqual(result["details"][0]["result"]["action"], "updated")
        row = self.ws.batch_update.call_args.args[0][0]
        self.assertEqual(row["range"], "A4:K4")
        self.assertEqual(row["values"][0][0], "=B4&C4")
        self.ws.append_rows.assert_not_called()

    def test_key_read_error_fails_batch_without_writes(self):
        self.ws.batch_get.side_effect = api_error(429)

        result = self.reporter.upsert_rows_batch([record(22)], config=self.config)

        self.assertFalse(result["success"])
        self.ws.append_rows.assert_not_called()
        self.ws.batch_update.assert_not_called()

    def test_invalid_record_rejected_others_uploaded(self):
        self.ws.append_rows.return_value = {"updates": {"updatedRange": "'KPI'!A4:K4"}}
        bad = {"work_date": "", "object_name": "ЧЕБОКСАРЫ_144"}

        result = self.reporter.upsert_rows_batch([bad, record(22)], config=self.config)

        self.assertEqual(result["failed"], 1)
        self.assertFalse(result["details"][0]["result"]["success"])
        self.assertTrue(result["details"][1]["result"]["success"])

    def test_update_only_skips_new_rows(self):
        result = self.reporter.upsert_rows_batch([record(25)], config=self.config, strategy="update_only")

        self.assertEqual(result["details"][0]["result"]["action"], "skipped")
        self.ws.append_rows.assert_not_called()


if __name__ == "__main__":
    unittest.main()
