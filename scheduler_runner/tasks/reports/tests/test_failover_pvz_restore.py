"""
Этап D failover: после восстановления соседа учетная запись возвращается на свой ПВЗ.

Проверяют:
- run_claimed_failover_backfill передает restore_pvz=claimer_pvz и собирает неудачи возврата
- неудача возврата попадает в сводку и в уведомление
"""
import unittest
from unittest.mock import MagicMock, patch

from .. import failover_orchestration
from ..reports_notifications import format_reports_run_notification_message
from ..reports_summary import ReportsRunSummary, build_failover_run_summary


@patch.object(failover_orchestration, "mark_failover_state")
@patch.object(failover_orchestration, "run_upload_batch_microservice", return_value={"success": True, "uploaded_records": 1})
@patch.object(failover_orchestration, "detect_missing_report_dates",
              return_value={"success": True, "missing_dates": ["2026-09-30"]})
@patch.object(failover_orchestration, "create_uploader_logger", return_value=MagicMock())
@patch.object(failover_orchestration, "invoke_parser_for_pvz")
class TestRestoreInFailoverBackfill(unittest.TestCase):
    ROWS = [{"work_date": "2026-09-30", "target_object_name": "ЧЕБОКСАРЫ_143", "owner_object_name": "ЧЕБОКСАРЫ_143"}]

    def run_backfill(self):
        return failover_orchestration.run_claimed_failover_backfill(
            claimed_rows=self.ROWS, parser_api="legacy", parser_logger=MagicMock(), failover_logger=MagicMock(),
            claimer_pvz="ЧЕБОКСАРЫ_144", source_run_id="run")

    def test_restore_pvz_passed_and_success(self, mock_parser, *_mocks):
        mock_parser.return_value = {"results_by_date": {"2026-09-30": {"success": True, "data": {}}},
                                    "pvz_restore": {"pvz": "ЧЕБОКСАРЫ_144", "success": True}}
        result = self.run_backfill()
        self.assertEqual(mock_parser.call_args.kwargs["restore_pvz"], "ЧЕБОКСАРЫ_144")
        self.assertEqual(result["pvz_restore_failures"], [])

    def test_restore_failure_collected(self, mock_parser, *_mocks):
        mock_parser.return_value = {"results_by_date": {"2026-09-30": {"success": True, "data": {}}},
                                    "pvz_restore": {"pvz": "ЧЕБОКСАРЫ_144", "success": False}}
        result = self.run_backfill()
        self.assertEqual(result["pvz_restore_failures"], ["ЧЕБОКСАРЫ_143"])


class TestRestoreFailureNotification(unittest.TestCase):
    def test_notification_warns_operator(self):
        failover = build_failover_run_summary(enabled=True, failover_result={
            "attempted": True, "claimed_rows_count": 1, "recovered_pvz_count": 1, "recovered_dates_count": 1,
            "pvz_restore_failures": ["ЧЕБОКСАРЫ_143"]})
        summary = ReportsRunSummary(mode="backfill_single_pvz", configured_pvz_id="ЧЕБОКСАРЫ_144",
                                    date_from="2026-09-24", date_to="2026-09-30", final_status="partial",
                                    owner=None, multi_pvz=None, failover=failover)

        message = format_reports_run_notification_message(summary)

        self.assertIn("осталась в чужом ПВЗ", message)
        self.assertIn("ЧЕБОКСАРЫ_143", message)

    def test_default_no_failures(self):
        self.assertEqual(build_failover_run_summary(enabled=True, failover_result={}).pvz_restore_failures, [])


if __name__ == "__main__":
    unittest.main()
