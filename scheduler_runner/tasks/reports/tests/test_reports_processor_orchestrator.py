"""
Orchestrator tests for refactored reports_processor.py.

These tests verify that the thin orchestrator (reports_processor.py) correctly
delegates to the extracted modules. They mock the refactored module calls
and verify orchestration flow, argument passing, and branching logic.
"""
import unittest
from argparse import Namespace
from unittest.mock import patch, MagicMock, call, ANY

from .. import reports_processor


class TestOrchestratorMainSingle(unittest.TestCase):
    """main() in single-date mode."""

    @patch("scheduler_runner.tasks.reports.reports_processor.send_notification_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.format_notification_message")
    @patch("scheduler_runner.tasks.reports.reports_processor.prepare_notification_data")
    @patch("scheduler_runner.tasks.reports.reports_processor.run_upload_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.invoke_parser_for_single_date")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_single_uses_single_date_facade(
        self,
        mock_parse_args,
        mock_invoke_parser_for_single_date,
        mock_run_upload_microservice,
        mock_prepare_notification_data,
        mock_format_notification_message,
        mock_send_notification_microservice,
    ):
        mock_parse_args.return_value = Namespace(
            execution_date="2026-03-01",
            date_from=None,
            date_to=None,
            backfill_days=7,
            mode="single",
            max_missing_dates=7,
            parser_api="new",
            pvz=None,
            detailed_logs=False,
            enable_failover_coordination=False,
        )
        mock_invoke_parser_for_single_date.return_value = {"summary": {"total": 1}}
        mock_run_upload_microservice.return_value = {"success": True}
        mock_prepare_notification_data.return_value = {"date": "2026-03-01"}
        mock_format_notification_message.return_value = "ok"
        mock_send_notification_microservice.return_value = {"success": True}

        reports_processor.main()

        mock_invoke_parser_for_single_date.assert_called_once_with(
            execution_date="2026-03-01",
            parser_api="new",
            pvz_id=reports_processor.PVZ_ID,
            logger=ANY,
        )
        mock_run_upload_microservice.assert_called_once()
        mock_send_notification_microservice.assert_called_once()

    @patch("scheduler_runner.tasks.reports.reports_processor.send_notification_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.run_upload_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.invoke_parser_for_single_date")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_single_skips_upload_on_parse_failure(
        self,
        mock_parse_args,
        mock_invoke_parser_for_single_date,
        mock_run_upload_microservice,
        mock_send_notification_microservice,
    ):
        mock_parse_args.return_value = Namespace(
            execution_date="2026-03-01",
            date_from=None,
            date_to=None,
            backfill_days=7,
            mode="single",
            max_missing_dates=7,
            parser_api="legacy",
            pvz=None,
            detailed_logs=False,
            enable_failover_coordination=False,
        )
        mock_invoke_parser_for_single_date.return_value = None

        reports_processor.main()

        mock_run_upload_microservice.assert_not_called()
        mock_send_notification_microservice.assert_not_called()

    @patch("scheduler_runner.tasks.reports.reports_processor.send_notification_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.format_notification_message")
    @patch("scheduler_runner.tasks.reports.reports_processor.prepare_notification_data")
    @patch("scheduler_runner.tasks.reports.reports_processor.run_upload_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.invoke_parser_for_single_date")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_single_skips_notification_on_upload_failure(
        self,
        mock_parse_args,
        mock_invoke_parser_for_single_date,
        mock_run_upload_microservice,
        mock_prepare_notification_data,
        mock_format_notification_message,
        mock_send_notification_microservice,
    ):
        mock_parse_args.return_value = Namespace(
            execution_date="2026-03-01",
            date_from=None,
            date_to=None,
            backfill_days=7,
            mode="single",
            max_missing_dates=7,
            parser_api="legacy",
            pvz=None,
            detailed_logs=False,
            enable_failover_coordination=False,
        )
        mock_invoke_parser_for_single_date.return_value = {"summary": {"total": 1}}
        mock_run_upload_microservice.return_value = {"success": False}

        reports_processor.main()

        mock_run_upload_microservice.assert_called_once()
        mock_send_notification_microservice.assert_not_called()


class TestOrchestratorMainBackfillSinglePvz(unittest.TestCase):
    """main() in backfill mode with single PVZ path."""

    @patch("scheduler_runner.tasks.reports.reports_processor.send_notification_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.format_reports_run_notification_message")
    @patch("scheduler_runner.tasks.reports.reports_processor.build_reports_run_summary")
    @patch("scheduler_runner.tasks.reports.reports_processor.build_owner_run_summary")
    @patch("scheduler_runner.tasks.reports.reports_processor.build_failover_run_summary")
    @patch("scheduler_runner.tasks.reports.reports_processor.run_failover_coordination_pass")
    @patch("scheduler_runner.tasks.reports.reports_processor.sync_owner_failover_state_from_batch_result")
    @patch("scheduler_runner.tasks.reports.reports_processor.run_upload_batch_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.invoke_parser_for_pvz")
    @patch("scheduler_runner.tasks.reports.reports_processor.build_jobs_for_pvz")
    @patch("scheduler_runner.tasks.reports.reports_processor.detect_missing_report_dates")
    @patch("scheduler_runner.tasks.reports.reports_processor.should_run_automatic_failover_coordination")
    @patch("scheduler_runner.tasks.reports.reports_processor.resolve_accessible_pvz_ids")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_backfill_single_pvz_full_flow(
        self,
        mock_parse_args,
        mock_resolve_accessible_pvz_ids,
        mock_should_run_coord,
        mock_detect_missing,
        mock_build_jobs,
        mock_invoke_parser,
        mock_run_upload,
        mock_sync_owner,
        mock_run_failover,
        mock_build_failover_summary,
        mock_build_owner_summary,
        mock_build_reports_summary,
        mock_format_notification,
        mock_send_notification,
    ):
        mock_parse_args.return_value = Namespace(
            execution_date=None,
            date_from="2026-04-01",
            date_to="2026-04-07",
            backfill_days=7,
            mode="backfill",
            max_missing_dates=7,
            parser_api="legacy",
            pvz=None,
            detailed_logs=False,
            enable_failover_coordination=True,
        )
        mock_resolve_accessible_pvz_ids.return_value = {
            "accessible_pvz_ids": [reports_processor.PVZ_ID],
            "skipped_pvz_ids": [],
            "discovery_scope": None,
        }
        mock_should_run_coord.return_value = True
        mock_detect_missing.return_value = {
            "success": True,
            "missing_dates": ["2026-04-01", "2026-04-02"],
        }
        mock_build_jobs.return_value = [MagicMock(execution_date="2026-04-01", pvz_id=reports_processor.PVZ_ID)]
        mock_invoke_parser.return_value = {
            "success": True,
            "successful_dates": ["2026-04-01", "2026-04-02"],
            "results_by_date": {},
        }
        mock_run_upload.return_value = {"success": True, "uploaded_records": 2}
        mock_sync_owner.return_value = {
            "successful_dates": ["2026-04-01", "2026-04-02"],
            "failed_dates": [],
            "suppressed_success_dates": [],
            "persisted_rows_count": 2,
            "existing_state_prefetch_keys_count": 2,
            "existing_state_prefetch_rows_found": 0,
            "upsert_diagnostics": {"updated_count": 2, "appended_count": 0, "prefetch_matches_count": 0},
        }
        mock_run_failover.return_value = {
            "attempted": True,
            "candidate_rows": [],
            "candidate_rows_count": 0,
            "claimed_rows": [],
            "claimed_rows_count": 0,
            "recovered_pvz_count": 0,
            "recovered_dates_count": 0,
            "failed_recovery_dates_count": 0,
            "uploaded_records": 0,
        }
        mock_build_owner_summary.return_value = MagicMock()
        mock_build_failover_summary.return_value = MagicMock()
        mock_build_reports_summary.return_value = MagicMock(final_status="success")
        mock_format_notification.return_value = "test notification"
        mock_send_notification.return_value = {"success": True}

        reports_processor.main()

        # Verify orchestration sequence
        mock_resolve_accessible_pvz_ids.assert_called_once()
        mock_detect_missing.assert_called_once()
        mock_invoke_parser.assert_called_once()
        mock_run_upload.assert_called_once()
        mock_sync_owner.assert_called_once()
        mock_run_failover.assert_called_once()
        mock_build_reports_summary.assert_called_once()
        mock_format_notification.assert_called_once()
        mock_send_notification.assert_called_once()

    @patch("scheduler_runner.tasks.reports.reports_processor.send_notification_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.format_reports_run_notification_message")
    @patch("scheduler_runner.tasks.reports.reports_processor.build_reports_run_summary")
    @patch("scheduler_runner.tasks.reports.reports_processor.build_owner_run_summary")
    @patch("scheduler_runner.tasks.reports.reports_processor.build_failover_run_summary")
    @patch("scheduler_runner.tasks.reports.reports_processor.run_failover_coordination_pass")
    @patch("scheduler_runner.tasks.reports.reports_processor.sync_owner_failover_state_from_batch_result")
    @patch("scheduler_runner.tasks.reports.reports_processor.run_upload_batch_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.invoke_parser_for_pvz")
    @patch("scheduler_runner.tasks.reports.reports_processor.build_jobs_for_pvz")
    @patch("scheduler_runner.tasks.reports.reports_processor.detect_missing_report_dates")
    @patch("scheduler_runner.tasks.reports.reports_processor.should_run_automatic_failover_coordination")
    @patch("scheduler_runner.tasks.reports.reports_processor.resolve_accessible_pvz_ids")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_backfill_upload_failure_syncs_owner_state_and_skips_failover(
        self,
        mock_parse_args,
        mock_resolve_accessible_pvz_ids,
        mock_should_run_coord,
        mock_detect_missing,
        mock_build_jobs,
        mock_invoke_parser,
        mock_run_upload,
        mock_sync_owner,
        mock_run_failover,
        mock_build_failover_summary,
        mock_build_owner_summary,
        mock_build_reports_summary,
        mock_format_notification,
        mock_send_notification,
    ):
        mock_parse_args.return_value = Namespace(
            execution_date=None,
            date_from="2026-04-01",
            date_to="2026-04-07",
            backfill_days=7,
            mode="backfill",
            max_missing_dates=7,
            parser_api="legacy",
            pvz=None,
            detailed_logs=False,
            enable_failover_coordination=True,
        )
        mock_resolve_accessible_pvz_ids.return_value = {
            "accessible_pvz_ids": [reports_processor.PVZ_ID],
            "skipped_pvz_ids": [],
            "discovery_scope": None,
        }
        mock_should_run_coord.return_value = True
        mock_detect_missing.return_value = {
            "success": True,
            "missing_dates": ["2026-04-01"],
        }
        mock_build_jobs.return_value = [MagicMock(execution_date="2026-04-01", pvz_id=reports_processor.PVZ_ID)]
        mock_invoke_parser.return_value = {"success": True, "successful_dates": ["2026-04-01"], "results_by_date": {}}
        mock_run_upload.return_value = {"success": False, "error": "upload_failed"}
        mock_build_owner_summary.return_value = MagicMock()
        mock_build_failover_summary.return_value = MagicMock()
        mock_build_reports_summary.return_value = MagicMock(final_status="partial")
        mock_format_notification.return_value = "test notification"
        mock_send_notification.return_value = {"success": True}

        reports_processor.main()

        # Upload happened
        mock_run_upload.assert_called_once()
        # Owner state is synced anyway (owner_failed), failover pass is skipped
        mock_sync_owner.assert_called_once()
        self.assertEqual(mock_sync_owner.call_args.kwargs["upload_result"], {"success": False, "error": "upload_failed"})
        mock_run_failover.assert_not_called()
        # Notification still sent with partial status
        mock_send_notification.assert_called_once()

    @patch("scheduler_runner.tasks.reports.reports_processor.send_notification_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.format_reports_run_notification_message")
    @patch("scheduler_runner.tasks.reports.reports_processor.build_reports_run_summary")
    @patch("scheduler_runner.tasks.reports.reports_processor.build_owner_run_summary")
    @patch("scheduler_runner.tasks.reports.reports_processor.build_failover_run_summary")
    @patch("scheduler_runner.tasks.reports.reports_processor.run_failover_coordination_pass")
    @patch("scheduler_runner.tasks.reports.reports_processor.sync_owner_failover_state_from_batch_result")
    @patch("scheduler_runner.tasks.reports.reports_processor.run_upload_batch_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.invoke_parser_for_pvz")
    @patch("scheduler_runner.tasks.reports.reports_processor.build_jobs_for_pvz")
    @patch("scheduler_runner.tasks.reports.reports_processor.detect_missing_report_dates")
    @patch("scheduler_runner.tasks.reports.reports_processor.should_run_automatic_failover_coordination")
    @patch("scheduler_runner.tasks.reports.reports_processor.resolve_accessible_pvz_ids")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_backfill_owner_sync_failure_skips_failover_pass(
        self,
        mock_parse_args,
        mock_resolve_accessible_pvz_ids,
        mock_should_run_coord,
        mock_detect_missing,
        mock_build_jobs,
        mock_invoke_parser,
        mock_run_upload,
        mock_sync_owner,
        mock_run_failover,
        mock_build_failover_summary,
        mock_build_owner_summary,
        mock_build_reports_summary,
        mock_format_notification,
        mock_send_notification,
    ):
        mock_parse_args.return_value = Namespace(
            execution_date=None,
            date_from="2026-04-01",
            date_to="2026-04-07",
            backfill_days=7,
            mode="backfill",
            max_missing_dates=7,
            parser_api="legacy",
            pvz=None,
            detailed_logs=False,
            enable_failover_coordination=True,
        )
        mock_resolve_accessible_pvz_ids.return_value = {
            "accessible_pvz_ids": [reports_processor.PVZ_ID],
            "skipped_pvz_ids": [],
            "discovery_scope": None,
        }
        mock_should_run_coord.return_value = True
        mock_detect_missing.return_value = {
            "success": True,
            "missing_dates": ["2026-04-01"],
        }
        mock_build_jobs.return_value = [MagicMock(execution_date="2026-04-01", pvz_id=reports_processor.PVZ_ID)]
        mock_invoke_parser.return_value = {"success": True, "successful_dates": ["2026-04-01"], "results_by_date": {}}
        mock_run_upload.return_value = {"success": True, "uploaded_records": 1}
        mock_sync_owner.side_effect = RuntimeError("upsert failed")
        mock_build_owner_summary.return_value = MagicMock()
        mock_build_failover_summary.return_value = MagicMock()
        mock_build_reports_summary.return_value = MagicMock(final_status="partial")
        mock_format_notification.return_value = "test notification"
        mock_send_notification.return_value = {"success": True}

        reports_processor.main()

        mock_sync_owner.assert_called_once()
        mock_run_failover.assert_not_called()
        mock_send_notification.assert_called_once()


class TestOrchestratorOwnerFailureReporting(unittest.TestCase):
    """Этап A failover: владелец сообщает о сбое при любом исходе."""

    def run_main(self, *, parser_result=None, parser_error=None, upload_result=None):
        target = "scheduler_runner.tasks.reports.reports_processor"
        args = Namespace(
            execution_date=None, date_from="2026-09-24", date_to="2026-09-30", backfill_days=7, mode="backfill",
            max_missing_dates=7, parser_api="legacy", pvz=None, detailed_logs=False, enable_failover_coordination=True,
        )
        names = ["resolve_accessible_pvz_ids", "should_run_automatic_failover_coordination", "detect_missing_report_dates",
                 "build_jobs_for_pvz", "invoke_parser_for_pvz", "run_upload_batch_microservice",
                 "sync_owner_failover_state_from_batch_result", "run_failover_coordination_pass",
                 "build_failover_run_summary", "build_owner_run_summary", "build_reports_run_summary",
                 "format_reports_run_notification_message", "send_notification_microservice"]
        patchers = [patch("argparse.ArgumentParser.parse_args", return_value=args)]
        patchers += [patch(f"{target}.{name}") for name in names]
        mocks = {}
        for name, patcher in zip(["parse_args"] + names, patchers):
            mocks[name] = patcher.start()
            self.addCleanup(patcher.stop)
        mocks["resolve_accessible_pvz_ids"].return_value = {
            "accessible_pvz_ids": [reports_processor.PVZ_ID], "skipped_pvz_ids": [], "discovery_scope": None}
        mocks["should_run_automatic_failover_coordination"].return_value = True
        mocks["detect_missing_report_dates"].return_value = {"success": True, "missing_dates": ["2026-09-29", "2026-09-30"]}
        if parser_error:
            mocks["invoke_parser_for_pvz"].side_effect = parser_error
        else:
            mocks["invoke_parser_for_pvz"].return_value = parser_result
        mocks["run_upload_batch_microservice"].return_value = upload_result
        mocks["sync_owner_failover_state_from_batch_result"].return_value = {}
        mocks["build_reports_run_summary"].return_value = MagicMock(final_status="failed")
        reports_processor.main()
        return mocks

    def test_parser_crash_writes_owner_failed_for_all_dates_and_notifies(self):
        mocks = self.run_main(parser_error=RuntimeError("AUTH_REQUIRED: session revoked"),
                              upload_result={"success": False, "no_data": True, "error": "Нет данных для загрузки"})

        batch_result = mocks["sync_owner_failover_state_from_batch_result"].call_args.kwargs["batch_result"]
        self.assertEqual(batch_result["failed_dates"], ["2026-09-29", "2026-09-30"])
        self.assertIn("AUTH_REQUIRED", batch_result["results_by_date"]["2026-09-30"]["error"])
        mocks["run_failover_coordination_pass"].assert_not_called()
        mocks["send_notification_microservice"].assert_called_once()

    def test_all_dates_rejected_writes_owner_state_and_skips_failover(self):
        """Случай 30.09.2026: все даты отклонены, загружать нечего — состояние пишется, помощь пропускается."""
        mocks = self.run_main(
            parser_result={"success": False, "successful_dates": 0, "failed_dates": 2, "results_by_date": {
                "2026-09-29": {"success": False, "error": "PARTIAL_DATE_REJECTED"},
                "2026-09-30": {"success": False, "error": "PARTIAL_DATE_REJECTED"}}},
            upload_result={"success": False, "no_data": True, "error": "Нет данных для загрузки"},
        )
        mocks["sync_owner_failover_state_from_batch_result"].assert_called_once()
        mocks["run_failover_coordination_pass"].assert_not_called()

    def test_partial_success_runs_failover_pass(self):
        mocks = self.run_main(
            parser_result={"success": False, "successful_dates": 1, "failed_dates": 1, "results_by_date": {
                "2026-09-29": {"success": True, "data": {}},
                "2026-09-30": {"success": False, "error": "PARTIAL_DATE_REJECTED"}}},
            upload_result={"success": True, "uploaded_records": 1},
        )
        mocks["sync_owner_failover_state_from_batch_result"].assert_called_once()
        mocks["run_failover_coordination_pass"].assert_called_once()


class TestResolveFailoverPassSkipReason(unittest.TestCase):
    def reason(self, *, missing=("2026-09-30",), batch=None, upload=None, sync=None):
        return reports_processor.resolve_failover_pass_skip_reason(
            missing_dates=list(missing), batch_result=batch or {}, upload_result=upload or {},
            owner_state_sync_result=sync or {})

    def test_no_own_missing_dates_allows(self):
        self.assertEqual(self.reason(missing=()), "")

    def test_all_own_dates_failed(self):
        batch = {"results_by_date": {"2026-09-30": {"success": False}}}
        self.assertEqual(self.reason(batch=batch, upload={"success": False, "no_data": True}), "own_dates_all_failed")

    def test_upload_failed(self):
        batch = {"results_by_date": {"2026-09-30": {"success": True}}}
        self.assertEqual(self.reason(batch=batch, upload={"success": False}), "owner_upload_failed")

    def test_state_sync_failed(self):
        batch = {"results_by_date": {"2026-09-30": {"success": True}}}
        self.assertEqual(self.reason(batch=batch, upload={"success": True}, sync={"success": False}),
                         "owner_state_sync_failed")

    def test_ok(self):
        batch = {"results_by_date": {"2026-09-30": {"success": True}}}
        self.assertEqual(self.reason(batch=batch, upload={"success": True}, sync={"success": True}), "")


class TestOrchestratorMainBackfillMultiPvz(unittest.TestCase):
    """main() in backfill mode with multiple PVZ path."""

    @patch("scheduler_runner.tasks.reports.reports_processor.send_notification_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.format_reports_run_notification_message")
    @patch("scheduler_runner.tasks.reports.reports_processor.build_reports_run_summary")
    @patch("scheduler_runner.tasks.reports.reports_processor.build_failover_run_summary")
    @patch("scheduler_runner.tasks.reports.reports_processor.build_aggregated_backfill_summary")
    @patch("scheduler_runner.tasks.reports.reports_processor.run_upload_batch_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.invoke_parser_for_grouped_jobs")
    @patch("scheduler_runner.tasks.reports.reports_processor.group_jobs_by_pvz")
    @patch("scheduler_runner.tasks.reports.reports_processor.build_jobs_from_missing_dates_by_pvz")
    @patch("scheduler_runner.tasks.reports.reports_processor.detect_missing_report_dates_by_pvz")
    @patch("scheduler_runner.tasks.reports.reports_processor.should_run_automatic_failover_coordination")
    @patch("scheduler_runner.tasks.reports.reports_processor.resolve_accessible_pvz_ids")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_backfill_multi_pvz_uses_loop_per_pvz(
        self,
        mock_parse_args,
        mock_resolve_accessible_pvz_ids,
        mock_should_run_coord,
        mock_detect_by_pvz,
        mock_build_jobs,
        mock_group_jobs,
        mock_invoke_grouped,
        mock_run_upload,
        mock_build_aggregated,
        mock_build_failover_summary,
        mock_build_reports_summary,
        mock_format_notification,
        mock_send_notification,
    ):
        mock_parse_args.return_value = Namespace(
            execution_date=None,
            date_from="2026-04-01",
            date_to="2026-04-07",
            backfill_days=7,
            mode="backfill",
            max_missing_dates=7,
            parser_api="legacy",
            pvz=["PVZ1", "PVZ2"],
            detailed_logs=False,
            enable_failover_coordination=False,
        )
        mock_resolve_accessible_pvz_ids.return_value = {
            "accessible_pvz_ids": ["PVZ1", "PVZ2"],
            "skipped_pvz_ids": [],
            "discovery_scope": None,
        }
        mock_should_run_coord.return_value = False
        mock_detect_by_pvz.return_value = {
            "success": True,
            "missing_dates_by_pvz": {"PVZ1": ["2026-04-01"], "PVZ2": ["2026-04-02"]},
            "coverage_results_by_pvz": {
                "PVZ1": {"success": True, "missing_dates": ["2026-04-01"]},
                "PVZ2": {"success": True, "missing_dates": ["2026-04-02"]},
            },
        }
        mock_build_jobs.return_value = [
            MagicMock(execution_date="2026-04-01", pvz_id="PVZ1"),
            MagicMock(execution_date="2026-04-02", pvz_id="PVZ2"),
        ]
        mock_group_jobs.return_value = {
            "PVZ1": [MagicMock(execution_date="2026-04-01", pvz_id="PVZ1")],
            "PVZ2": [MagicMock(execution_date="2026-04-02", pvz_id="PVZ2")],
        }
        mock_invoke_grouped.return_value = {
            "PVZ1": {"success": True, "successful_dates": ["2026-04-01"], "results_by_date": {}},
            "PVZ2": {"success": True, "successful_dates": ["2026-04-02"], "results_by_date": {}},
        }
        mock_run_upload.return_value = {"success": True, "uploaded_records": 1}
        mock_build_aggregated.return_value = MagicMock()
        mock_build_failover_summary.return_value = MagicMock()
        mock_build_reports_summary.return_value = MagicMock(final_status="success")
        mock_format_notification.return_value = "test notification"
        mock_send_notification.return_value = {"success": True}

        reports_processor.main()

        # Multi-PVZ flow: detect_by_pv, build_jobs, group, invoke_grouped, upload per pvz
        mock_detect_by_pvz.assert_called_once()
        mock_build_jobs.assert_called_once()
        mock_group_jobs.assert_called_once()
        mock_invoke_grouped.assert_called_once()
        # Upload called once per PVZ (2 times)
        self.assertEqual(mock_run_upload.call_count, 2)
        mock_send_notification.assert_called_once()

    @patch("scheduler_runner.tasks.reports.reports_processor.resolve_accessible_pvz_ids")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_backfill_stops_when_no_accessible_pvz(
        self,
        mock_parse_args,
        mock_resolve_accessible_pvz_ids,
    ):
        mock_parse_args.return_value = Namespace(
            execution_date=None,
            date_from="2026-04-01",
            date_to="2026-04-07",
            backfill_days=7,
            mode="backfill",
            max_missing_dates=7,
            parser_api="legacy",
            pvz=["PVZ3"],
            detailed_logs=False,
            enable_failover_coordination=False,
        )
        mock_resolve_accessible_pvz_ids.return_value = {
            "accessible_pvz_ids": [],
            "skipped_pvz_ids": ["PVZ3"],
            "discovery_scope": None,
        }

        # Should return early without error
        reports_processor.main()


class TestOrchestratorBuildProcessorRunId(unittest.TestCase):
    def test_build_processor_run_id_format(self):
        from datetime import datetime
        run_id = reports_processor.build_processor_run_id("TEST_PVZ")
        self.assertIn("TEST_PVZ", run_id)
        self.assertGreaterEqual(len(run_id), len("TEST_PVZ") + 15)

    def test_build_processor_run_id_custom_time(self):
        from datetime import datetime
        run_id = reports_processor.build_processor_run_id("TEST_PVZ", started_at=datetime(2026, 4, 9, 21, 30, 0))
        self.assertIn("20260409213000", run_id)
        self.assertIn("TEST_PVZ", run_id)


class TestOrchestratorUsesRefactoredModules(unittest.TestCase):
    """Verify that the orchestrator imports from refactored modules correctly."""

    def test_imports_reports_summary(self):
        from ..reports_summary import (
            build_pvz_execution_result,
            build_owner_run_summary,
            build_failover_run_summary,
            build_reports_run_summary,
        )
        # These are re-exported via reports_processor imports
        self.assertTrue(callable(build_pvz_execution_result))
        self.assertTrue(callable(build_owner_run_summary))
        self.assertTrue(callable(build_failover_run_summary))
        self.assertTrue(callable(build_reports_run_summary))

    def test_imports_reports_upload(self):
        from ..reports_upload import (
            detect_missing_report_dates,
            detect_missing_report_dates_by_pvz,
            run_upload_microservice,
            run_upload_batch_microservice,
        )
        self.assertTrue(callable(detect_missing_report_dates))
        self.assertTrue(callable(detect_missing_report_dates_by_pvz))
        self.assertTrue(callable(run_upload_microservice))
        self.assertTrue(callable(run_upload_batch_microservice))

    def test_imports_reports_notifications(self):
        from ..reports_notifications import (
            prepare_notification_data,
            format_notification_message,
            prepare_batch_notification_data,
            format_reports_run_notification_message,
            send_notification_microservice,
        )
        self.assertTrue(callable(prepare_notification_data))
        self.assertTrue(callable(format_notification_message))
        self.assertTrue(callable(prepare_batch_notification_data))
        self.assertTrue(callable(format_reports_run_notification_message))
        self.assertTrue(callable(send_notification_microservice))

    def test_imports_reports_scope(self):
        from ..reports_scope import (
            resolve_accessible_pvz_ids,
            should_run_automatic_failover_coordination,
            build_jobs_from_missing_dates_by_pvz,
            group_jobs_by_pvz,
        )
        self.assertTrue(callable(resolve_accessible_pvz_ids))
        self.assertTrue(callable(should_run_automatic_failover_coordination))
        self.assertTrue(callable(build_jobs_from_missing_dates_by_pvz))
        self.assertTrue(callable(group_jobs_by_pvz))

    def test_imports_owner_state_sync(self):
        from ..owner_state_sync import sync_owner_failover_state_from_batch_result
        self.assertTrue(callable(sync_owner_failover_state_from_batch_result))

    def test_imports_failover_orchestration(self):
        from ..failover_orchestration import run_failover_coordination_pass
        self.assertTrue(callable(run_failover_coordination_pass))


if __name__ == "__main__":
    unittest.main()

