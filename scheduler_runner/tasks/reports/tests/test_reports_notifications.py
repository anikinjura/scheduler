"""
Unit tests for reports_notifications (Phase 1.2).

Проверяют:
- format_notification_message форматирует правильно
- format_batch_notification_message
- format_reports_run_notification_message
- build_aggregated_backfill_summary
- format_aggregated_backfill_notification_message
"""
import unittest

from scheduler_runner.tasks.reports.reports_summary import (
    OwnerRunSummary,
    FailoverRunSummary,
    ReportsRunSummary,
    ReportsBackfillExecutionResult,
    PVZExecutionResult,
)
from scheduler_runner.tasks.reports.reports_notifications import (
    prepare_notification_data,
    format_notification_message,
    prepare_batch_notification_data,
    format_batch_notification_message,
    build_aggregated_backfill_summary,
    format_aggregated_backfill_notification_message,
    format_reports_run_notification_message,
)


class TestSingleNotification(unittest.TestCase):
    def test_prepare_notification_data(self):
        result = prepare_notification_data({
            "execution_date": "2026-04-01",
            "location_info": "ЧЕБОКСАРЫ_144",
            "summary": {
                "giveout": {"value": 150},
                "direct_flow_total": {"total_carriages": 10},
                "return_flow_total": {"total_carriages": 3},
            },
        })
        self.assertEqual(result["date"], "01.04.2026")
        self.assertEqual(result["pvz"], "ЧЕБОКСАРЫ_144")
        self.assertEqual(result["issued_packages"], 150)
        self.assertEqual(result["direct_flow"], 10)
        self.assertEqual(result["return_flow"], 3)

    def test_format_notification_message(self):
        msg = format_notification_message({
            "date": "01.04.2026",
            "pvz": "ЧЕБОКСАРЫ_144",
            "issued_packages": 150,
            "direct_flow": 10,
            "return_flow": 3,
        })
        self.assertIn("01.04.2026", msg)
        self.assertIn("ЧЕБОКСАРЫ_144", msg)
        self.assertIn("150", msg)


class TestBatchNotification(unittest.TestCase):
    def test_prepare_batch_notification_data(self):
        data = prepare_batch_notification_data(
            batch_result={
                "results_by_date": {
                    "2026-04-01": {"success": True},
                    "2026-04-02": {"success": False, "error": "parse_failed"},
                },
                "successful_dates": ["2026-04-01"],
            },
            upload_result={"success": True, "uploaded_records": 5},
            coverage_result={"missing_dates": ["2026-04-01", "2026-04-02"]},
            pvz_id="ЧЕБОКСАРЫ_144",
        )
        self.assertEqual(data["pvz"], "ЧЕБОКСАРЫ_144")
        self.assertEqual(data["missing_dates_count"], 2)
        self.assertEqual(len(data["failed_dates"]), 1)
        self.assertEqual(data["uploaded_records"], 5)

    def test_format_batch_notification_message(self):
        msg = format_batch_notification_message({
            "pvz": "ЧЕБОКСАРЫ_144",
            "date_from": "2026-04-01",
            "date_to": "2026-04-07",
            "missing_dates_count": 3,
            "successful_dates": 2,
            "failed_dates": ["2026-04-03"],
            "uploaded_records": 10,
        })
        self.assertIn("ЧЕБОКСАРЫ_144", msg)
        self.assertIn("Отсутствовало дат: 3", msg)
        self.assertIn("2026-04-03", msg)


class TestAggregatedBackfill(unittest.TestCase):
    def test_build_aggregated_backfill_summary(self):
        summary = build_aggregated_backfill_summary(
            pvz_results={
                "PVZ1": PVZExecutionResult(
                    pvz_id="PVZ1",
                    coverage_result={"missing_dates": ["2026-04-01"]},
                    batch_result={"successful_dates": ["2026-04-01"]},
                    upload_result={"uploaded_records": 5},
                    notification_data={},
                ),
                "PVZ2": PVZExecutionResult(
                    pvz_id="PVZ2",
                    coverage_result={"missing_dates": ["2026-04-02"]},
                    batch_result={"successful_dates": ["2026-04-02"]},
                    upload_result={"uploaded_records": 3},
                    notification_data={},
                ),
            },
            date_from="2026-04-01",
            date_to="2026-04-07",
        )
        self.assertEqual(summary.processed_pvz_count, 2)
        self.assertEqual(summary.missing_dates_count, 2)
        self.assertEqual(summary.successful_jobs_count, 2)
        self.assertEqual(summary.uploaded_records, 8)

    def test_format_aggregated_backfill_notification_message(self):
        summary = build_aggregated_backfill_summary(
            pvz_results={
                "PVZ1": PVZExecutionResult(
                    pvz_id="PVZ1",
                    coverage_result={"missing_dates": ["2026-04-01"]},
                    batch_result={"successful_dates": ["2026-04-01"]},
                    upload_result={"uploaded_records": 5},
                    notification_data={},
                ),
            },
            date_from="2026-04-01",
            date_to="2026-04-07",
        )
        msg = format_aggregated_backfill_notification_message(summary)
        self.assertIn("multi-PVZ", msg)
        self.assertIn("PVZ1", msg)


class TestFullRunNotification(unittest.TestCase):
    def test_format_reports_run_notification_message_success(self):
        owner = OwnerRunSummary(
            pvz_id="ЧЕБОКСАРЫ_144",
            coverage_success=True,
            missing_dates=["2026-04-01"],
            missing_dates_count=1,
            truncated=False,
            parse_success=True,
            successful_dates=["2026-04-01"],
            successful_dates_count=1,
            failed_dates=[],
            failed_dates_count=0,
            uploaded_records=5,
            upload_success=True,
            errors=[],
        )
        summary = ReportsRunSummary(
            mode="backfill_single_pvz",
            configured_pvz_id="ЧЕБОКСАРЫ_144",
            date_from="2026-04-01",
            date_to="2026-04-07",
            final_status="success",
            owner=owner,
            multi_pvz=None,
            failover=FailoverRunSummary(
                enabled=False,
                attempted=False,
                discovery_success=None,
                owner_state_sync_attempted=False,
                owner_state_sync_success=None,
                owner_state_sync_error="",
                candidate_scan_attempted=False,
                candidate_scan_success=None,
                candidate_scan_error="",
                available_pvz=[],
                candidate_rows_count=0,
                claimed_rows_count=0,
                recovered_pvz_count=0,
                recovered_dates_count=0,
                failed_recovery_dates_count=0,
                uploaded_records=0,
                results_by_pvz={},
            ),
        )
        msg = format_reports_run_notification_message(summary)
        self.assertIn("success", msg)
        self.assertIn("ЧЕБОКСАРЫ_144", msg)

    def test_format_reports_run_notification_message_failover(self):
        summary = ReportsRunSummary(
            mode="backfill_single_pvz",
            configured_pvz_id="ЧЕБОКСАРЫ_144",
            date_from="2026-04-01",
            date_to="2026-04-07",
            final_status="partial",
            owner=None,
            multi_pvz=None,
            failover=FailoverRunSummary(
                enabled=True,
                attempted=True,
                discovery_success=True,
                owner_state_sync_attempted=True,
                owner_state_sync_success=True,
                owner_state_sync_error="",
                candidate_scan_attempted=True,
                candidate_scan_success=False,
                candidate_scan_error="test_error",
                available_pvz=["ЧЕБОКСАРЫ_143"],
                candidate_rows_count=5,
                claimed_rows_count=2,
                recovered_pvz_count=1,
                recovered_dates_count=3,
                failed_recovery_dates_count=1,
                uploaded_records=10,
                results_by_pvz={
                    "ЧЕБОКСАРЫ_143": {
                        "recoverable_dates": ["2026-04-01", "2026-04-02", "2026-04-03"],
                        "batch_result": {},
                        "upload_result": {"uploaded_records": 10},
                    }
                },
            ),
        )
        msg = format_reports_run_notification_message(summary)
        self.assertIn("Помощь коллегам:", msg)
        self.assertIn("- candidate rows: 5", msg)
        self.assertIn("- claimed rows: 2", msg)


if __name__ == "__main__":
    unittest.main()

