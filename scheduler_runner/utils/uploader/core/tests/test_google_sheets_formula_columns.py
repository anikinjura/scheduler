import unittest
from unittest.mock import Mock

from scheduler_runner.tasks.reports.config.scripts.kpi_google_sheets_config import TABLE_CONFIG
from scheduler_runner.utils.uploader.core.providers.google_sheets.google_sheets_core import GoogleSheetsReporter


class TestGoogleSheetsRewardFormulaColumns(unittest.TestCase):
    def test_prepare_row_values_includes_reward_formulas_for_kpi_sheet(self):
        reporter = GoogleSheetsReporter.__new__(GoogleSheetsReporter)
        reporter.worksheet = Mock()
        reporter.worksheet.row_values.return_value = [
            "id",
            "Дата",
            "ПВЗ",
            "Количество выдач",
            "Прямой поток",
            "Возвратный поток",
            "Сумма за Количество выдач",
            "Сумма за Прямой поток",
            "Сумма за Возвратный поток",
            "Итого вознаграждение",
            "timestamp",
        ]

        values = reporter._prepare_row_values(
            data={
                "Дата": "04.04.2026",
                "ПВЗ": "ЧЕБОКСАРЫ_144",
                "Количество выдач": 317,
                "Прямой поток": 20,
                "Возвратный поток": 0,
                "timestamp": "2026-04-04 21:33:00",
            },
            config=TABLE_CONFIG,
            row_number=7,
            formula_row_placeholder="{row}",
        )

        self.assertEqual(values[0], "=B7&C7")
        self.assertEqual(values[1], "04.04.2026")
        self.assertEqual(values[2], "ЧЕБОКСАРЫ_144")
        self.assertEqual(values[3], 317)
        self.assertEqual(values[4], 20)
        self.assertEqual(values[5], 0)
        self.assertEqual(
            values[6],
            '=GET_REWARD("Количество выдач";D7;$B7;KPI_REWARD_RULES_RANGE)',
        )
        self.assertEqual(
            values[7],
            '=GET_REWARD("Прямой поток";E7;$B7;KPI_REWARD_RULES_RANGE)',
        )
        self.assertEqual(
            values[8],
            '=GET_REWARD("Возвратный поток";F7;$B7;KPI_REWARD_RULES_RANGE)',
        )
        self.assertEqual(values[9], "=SUM(G7:I7)")
        self.assertEqual(values[10], "2026-04-04 21:33:00")


if __name__ == "__main__":
    unittest.main()
