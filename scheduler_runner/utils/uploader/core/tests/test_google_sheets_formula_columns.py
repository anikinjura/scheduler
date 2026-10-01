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
            "work_date",
            "object_name",
            "issued_packages",
            "direct_flow",
            "return_flow",
            "reward_issued_packages",
            "reward_direct_flow",
            "reward_return_flow",
            "total_reward",
            "timestamp",
            "owner_id",  # колонка другой системы (VK_shift), в конфигурации ее нет
        ]

        values = reporter._prepare_row_values(
            data={
                "work_date": "04.04.2026",
                "object_name": "ЧЕБОКСАРЫ_144",
                "issued_packages": 317,
                "direct_flow": 20,
                "return_flow": 0,
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
        self.assertEqual(values[6], '=GET_REWARD("issued_packages";D7;$B7;KPI_REWARD_RULES_RANGE)')
        self.assertEqual(values[7], '=GET_REWARD("direct_flow";E7;$B7;KPI_REWARD_RULES_RANGE)')
        self.assertEqual(values[8], '=GET_REWARD("return_flow";F7;$B7;KPI_REWARD_RULES_RANGE)')
        self.assertEqual(values[9], "=SUM(G7:I7)")
        self.assertEqual(values[10], "2026-04-04 21:33:00")
        self.assertIsNone(values[11])  # чужая колонка не записывается


if __name__ == "__main__":
    unittest.main()
