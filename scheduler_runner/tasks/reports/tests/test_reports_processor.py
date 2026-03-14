import unittest
from argparse import Namespace
from unittest.mock import Mock, patch

from scheduler_runner.utils.parser.core.contracts import ParserJobResult
from scheduler_runner.tasks.reports import reports_processor


class TestReportsProcessor(unittest.TestCase):
    def test_resolve_pvz_ids_defaults_to_global_pvz(self):
        self.assertEqual(reports_processor.resolve_pvz_ids(None), [reports_processor.PVZ_ID])

    @patch("scheduler_runner.tasks.reports.reports_processor.invoke_available_pvz_discovery")
    def test_resolve_accessible_pvz_ids_filters_inaccessible_colleagues(self, mock_invoke_available_pvz_discovery):
        mock_invoke_available_pvz_discovery.return_value = {
            "success": True,
            "available_pvz": ["ЧЕБОКСАРЫ_144", "ЧЕБОКСАРЫ_182"],
        }

        result = reports_processor.resolve_accessible_pvz_ids(
            raw_pvz_ids=["ЧЕБОКСАРЫ_144", "ЧЕБОКСАРЫ_182", "ЧЕБОКСАРЫ_143"],
            configured_pvz_id="ЧЕБОКСАРЫ_144",
            logger=Mock(),
            parser_logger=Mock(),
        )

        self.assertEqual(result["accessible_pvz_ids"], ["ЧЕБОКСАРЫ_144", "ЧЕБОКСАРЫ_182"])
        self.assertEqual(result["skipped_pvz_ids"], ["ЧЕБОКСАРЫ_143"])
        mock_invoke_available_pvz_discovery.assert_called_once()

    @patch("scheduler_runner.tasks.reports.reports_processor.invoke_available_pvz_discovery")
    def test_resolve_accessible_pvz_ids_falls_back_to_configured_pvz_when_discovery_fails(self, mock_invoke_available_pvz_discovery):
        mock_invoke_available_pvz_discovery.return_value = {
            "success": False,
            "error": "discovery_failed",
        }

        result = reports_processor.resolve_accessible_pvz_ids(
            raw_pvz_ids=["ЧЕБОКСАРЫ_144", "ЧЕБОКСАРЫ_182"],
            configured_pvz_id="ЧЕБОКСАРЫ_144",
            logger=Mock(),
            parser_logger=Mock(),
        )

        self.assertEqual(result["accessible_pvz_ids"], ["ЧЕБОКСАРЫ_144"])
        self.assertEqual(result["skipped_pvz_ids"], ["ЧЕБОКСАРЫ_182"])
        mock_invoke_available_pvz_discovery.assert_called_once()

    def test_build_jobs_from_missing_dates_by_pvz_creates_explicit_jobs(self):
        jobs = reports_processor.build_jobs_from_missing_dates_by_pvz(
            {
                "PVZ1": ["2026-03-01", "2026-03-02"],
                "PVZ2": ["2026-03-03"],
            }
        )

        self.assertEqual([(job.pvz_id, job.execution_date) for job in jobs], [
            ("PVZ1", "2026-03-01"),
            ("PVZ1", "2026-03-02"),
            ("PVZ2", "2026-03-03"),
        ])

    def test_execute_parser_jobs_for_pvz_keeps_compatible_batch_result_shape(self):
        jobs = reports_processor.build_jobs_for_pvz("PVZ1", ["2026-03-01"])
        with patch("scheduler_runner.utils.parser.parser_invocation.execute_parser_internal") as mock_execute_internal:
            mock_execute_internal.return_value = {
                "success": True,
                "mode": "batch",
                "total_dates": 1,
                "successful_dates": ["2026-03-01"],
                "failed_dates": [],
                "results_by_date": {"2026-03-01": {"success": True, "data": {}}},
            }
            result = reports_processor.execute_parser_jobs_for_pvz(jobs, parser_api="legacy")

        self.assertTrue(result["success"])
        self.assertEqual(result["mode"], "batch")
        self.assertIn("results_by_date", result)
        self.assertEqual(mock_execute_internal.call_args.kwargs["parser_api"], "legacy")
        self.assertEqual(mock_execute_internal.call_args.kwargs["pvz_id"], "PVZ1")
        self.assertEqual([(job.pvz_id, job.execution_date) for job in mock_execute_internal.call_args.kwargs["jobs"]], [("PVZ1", "2026-03-01")])

    def test_invoke_parser_for_pvz_builds_jobs_and_returns_compatible_batch_result(self):
        with patch("scheduler_runner.utils.parser.parser_invocation.execute_parser_internal") as mock_execute_internal:
            mock_execute_internal.return_value = {
                "success": True,
                "mode": "batch",
                "total_dates": 1,
                "successful_dates": ["2026-03-01"],
                "failed_dates": [],
                "results_by_date": {"2026-03-01": {"success": True, "data": {}}},
            }

            result = reports_processor.invoke_parser_for_pvz(
                parser_api="legacy",
                pvz_id="PVZ1",
                execution_dates=["2026-03-01"],
            )

        self.assertTrue(result["success"])
        called_jobs = mock_execute_internal.call_args.kwargs["jobs"]
        self.assertEqual([(job.pvz_id, job.execution_date) for job in called_jobs], [("PVZ1", "2026-03-01")])
        self.assertEqual(mock_execute_internal.call_args.kwargs["parser_api"], "legacy")
        self.assertEqual(mock_execute_internal.call_args.kwargs["pvz_id"], "PVZ1")

    def test_invoke_parser_for_grouped_jobs_calls_facade_once_per_pvz(self):
        grouped_jobs = {
            "PVZ1": reports_processor.build_jobs_for_pvz("PVZ1", ["2026-03-01"]),
            "PVZ2": reports_processor.build_jobs_for_pvz("PVZ2", ["2026-03-02"]),
        }
        with patch("scheduler_runner.utils.parser.parser_invocation.invoke_parser_for_pvz") as mock_invoke:
            mock_invoke.side_effect = [
                {"successful_dates": ["2026-03-01"]},
                {"successful_dates": ["2026-03-02"]},
            ]

            result = reports_processor.invoke_parser_for_grouped_jobs(
                grouped_jobs=grouped_jobs,
                pvz_ids=["PVZ1", "PVZ2"],
                parser_api="new",
            )

        self.assertEqual(result, {
            "PVZ1": {"successful_dates": ["2026-03-01"]},
            "PVZ2": {"successful_dates": ["2026-03-02"]},
        })
        self.assertEqual(mock_invoke.call_count, 2)
        self.assertEqual(mock_invoke.call_args_list[0].kwargs["jobs"][0].pvz_id, "PVZ1")
        self.assertEqual(mock_invoke.call_args_list[0].kwargs["parser_api"], "new")
        self.assertEqual(mock_invoke.call_args_list[0].kwargs["pvz_id"], "PVZ1")
        self.assertEqual(mock_invoke.call_args_list[1].kwargs["jobs"][0].pvz_id, "PVZ2")
        self.assertEqual(mock_invoke.call_args_list[1].kwargs["pvz_id"], "PVZ2")

    @patch("scheduler_runner.utils.parser.parser_invocation.OzonAvailablePvzParser")
    def test_invoke_available_pvz_discovery_applies_pvz_override(self, mock_parser_cls):
        mock_parser = mock_parser_cls.return_value
        mock_parser.run_discovery.return_value = {
            "success": True,
            "available_pvz": ["PVZ1", "PVZ2"],
        }

        result = reports_processor.invoke_available_pvz_discovery(
            pvz_id="PVZ2",
            logger=Mock(),
        )

        self.assertTrue(result["success"])
        parser_config = mock_parser_cls.call_args.args[0]
        self.assertEqual(parser_config["additional_params"]["location_id"], "PVZ2")
        mock_parser.run_discovery.assert_called_once_with(save_to_file=False, output_format="json")

    @patch("scheduler_runner.utils.parser.parser_invocation.execute_parser_internal")
    def test_invoke_parser_for_single_date_uses_internal_executor_and_pvz_override(
        self,
        mock_execute_parser_internal,
    ):
        mock_execute_parser_internal.return_value = {"summary": {"ok": 1}}

        result = reports_processor.invoke_parser_for_single_date(
            execution_date="2026-03-01",
            parser_api="legacy",
            pvz_id="PVZ1",
        )

        self.assertEqual(result, {"summary": {"ok": 1}})
        self.assertEqual(mock_execute_parser_internal.call_args.kwargs["parser_api"], "legacy")
        self.assertEqual(mock_execute_parser_internal.call_args.kwargs["pvz_id"], "PVZ1")
        self.assertEqual(mock_execute_parser_internal.call_args.kwargs["execution_dates"], ["2026-03-01"])
        self.assertEqual(mock_execute_parser_internal.call_args.kwargs["result_mode"], "single")

    @patch("scheduler_runner.utils.parser.parser_invocation.MultiStepOzonParser")
    def test_execute_parser_internal_applies_pvz_override_for_legacy_single(self, mock_parser_cls):
        mock_parser = mock_parser_cls.return_value
        mock_parser.run_parser.return_value = {"summary": {"ok": 1}}

        result = reports_processor.execute_parser_internal(
            parser_api="legacy",
            pvz_id="PVZ1",
            execution_dates=["2026-03-01"],
            result_mode="single",
            save_to_file=True,
            output_format="json",
        )

        self.assertEqual(result, {"summary": {"ok": 1}})
        parser_config = mock_parser_cls.call_args.args[0]
        self.assertEqual(parser_config["execution_date"], "2026-03-01")
        self.assertEqual(parser_config["additional_params"]["location_id"], "PVZ1")

    @patch("scheduler_runner.utils.parser.parser_invocation.MultiStepOzonParser")
    def test_execute_parser_internal_retries_legacy_batch_in_visible_browser_after_session_failure(self, mock_parser_cls):
        first_parser = Mock()
        first_parser.run_parser_batch.return_value = {
            "success": False,
            "mode": "batch",
            "total_dates": 2,
            "successful_dates": 1,
            "failed_dates": 1,
            "results_by_date": {
                "2026-03-01": {"success": True, "data": {"execution_date": "2026-03-01"}},
                "2026-03-02": {"success": False, "error": "InvalidSessionIdException: invalid session id"},
            },
        }
        second_parser = Mock()
        second_parser.run_parser_batch.return_value = {
            "success": True,
            "mode": "batch",
            "total_dates": 2,
            "successful_dates": 2,
            "failed_dates": 0,
            "results_by_date": {
                "2026-03-01": {"success": True, "data": {"execution_date": "2026-03-01"}},
                "2026-03-02": {"success": True, "data": {"execution_date": "2026-03-02"}},
            },
        }
        mock_parser_cls.side_effect = [first_parser, second_parser]

        result = reports_processor.execute_parser_internal(
            parser_api="legacy",
            pvz_id="PVZ1",
            execution_dates=["2026-03-01", "2026-03-02"],
            result_mode="batch",
            save_to_file=False,
            output_format="json",
        )

        self.assertTrue(result["success"])
        self.assertEqual(mock_parser_cls.call_count, 2)
        first_config = mock_parser_cls.call_args_list[0].args[0]
        second_config = mock_parser_cls.call_args_list[1].args[0]
        self.assertTrue(first_config["browser_config"]["headless"])
        self.assertFalse(second_config["browser_config"]["headless"])
        self.assertTrue(first_config["HEADLESS"])
        self.assertFalse(second_config["HEADLESS"])

    @patch("scheduler_runner.utils.parser.parser_invocation.MultiStepOzonParser")
    def test_execute_parser_internal_retries_new_batch_in_visible_browser_after_session_failure(self, mock_parser_cls):
        first_parser = Mock()
        first_parser.run_jobs_for_pvz.return_value = [
            ParserJobResult.from_success(
                report_type="ozon_reports",
                pvz_id="PVZ1",
                execution_date="2026-03-01",
                data={"execution_date": "2026-03-01"},
            ),
            ParserJobResult.from_error(
                report_type="ozon_reports",
                pvz_id="PVZ1",
                execution_date="2026-03-02",
                error_message="Message: invalid session id",
            ),
        ]
        second_parser = Mock()
        second_parser.run_jobs_for_pvz.return_value = [
            ParserJobResult.from_success(
                report_type="ozon_reports",
                pvz_id="PVZ1",
                execution_date="2026-03-01",
                data={"execution_date": "2026-03-01"},
            ),
            ParserJobResult.from_success(
                report_type="ozon_reports",
                pvz_id="PVZ1",
                execution_date="2026-03-02",
                data={"execution_date": "2026-03-02"},
            ),
        ]
        mock_parser_cls.side_effect = [first_parser, second_parser]

        result = reports_processor.execute_parser_internal(
            parser_api="new",
            pvz_id="PVZ1",
            execution_dates=["2026-03-01", "2026-03-02"],
            result_mode="batch",
            save_to_file=False,
            output_format="json",
        )

        self.assertTrue(result["success"])
        self.assertEqual(mock_parser_cls.call_count, 2)
        first_config = mock_parser_cls.call_args_list[0].args[0]
        second_config = mock_parser_cls.call_args_list[1].args[0]
        self.assertTrue(first_config["browser_config"]["headless"])
        self.assertFalse(second_config["browser_config"]["headless"])

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
        )
        mock_invoke_parser_for_single_date.return_value = {"summary": {"total": 1}}
        mock_run_upload_microservice.return_value = {"success": True}
        mock_prepare_notification_data.return_value = {"date": "2026-03-01"}
        mock_format_notification_message.return_value = "ok"

        reports_processor.main()

        mock_invoke_parser_for_single_date.assert_called_once_with(
            execution_date="2026-03-01",
            parser_api="new",
            pvz_id=reports_processor.PVZ_ID,
            logger=unittest.mock.ANY,
        )
        mock_run_upload_microservice.assert_called_once_with({"summary": {"total": 1}})
        mock_send_notification_microservice.assert_called_once()

    def test_build_aggregated_backfill_summary_collects_totals(self):
        summary = reports_processor.build_aggregated_backfill_summary(
            pvz_results={
                "PVZ1": {
                    "coverage_result": {"missing_dates": ["2026-03-01", "2026-03-02"]},
                    "batch_result": {"successful_dates": ["2026-03-01"], "failed_dates": ["2026-03-02"]},
                    "upload_result": {"uploaded_records": 1},
                },
                "PVZ2": {
                    "coverage_result": {"missing_dates": ["2026-03-03"]},
                    "batch_result": {"successful_dates": ["2026-03-03"], "failed_dates": []},
                    "upload_result": {"uploaded_records": 2},
                },
            },
            date_from="2026-03-01",
            date_to="2026-03-03",
        )

        self.assertIsInstance(summary, reports_processor.ReportsBackfillExecutionResult)
        self.assertEqual(summary.processed_pvz_count, 2)
        self.assertEqual(summary.missing_dates_count, 3)
        self.assertEqual(summary.successful_jobs_count, 2)
        self.assertEqual(summary.failed_jobs_count, 1)
        self.assertEqual(summary.uploaded_records, 3)
        self.assertIsInstance(summary.pvz_results["PVZ1"], reports_processor.PVZExecutionResult)

    def test_format_aggregated_backfill_notification_message_uses_execution_result_model(self):
        summary = reports_processor.ReportsBackfillExecutionResult(
            date_from="2026-03-01",
            date_to="2026-03-03",
            processed_pvz_count=1,
            missing_dates_count=2,
            successful_jobs_count=1,
            failed_jobs_count=1,
            uploaded_records=5,
            pvz_results={
                "PVZ1": reports_processor.build_pvz_execution_result(
                    pvz_id="PVZ1",
                    coverage_result={"missing_dates": ["2026-03-01", "2026-03-02"]},
                    batch_result={"successful_dates": ["2026-03-01"], "failed_dates": ["2026-03-02"]},
                    upload_result={"uploaded_records": 5},
                )
            },
        )

        message = reports_processor.format_aggregated_backfill_notification_message(summary)

        self.assertIn("KPI multi-PVZ backfill", message)
        self.assertIn("PVZ1: missing=2, ok=1, failed=1, uploaded=5", message)

    def test_detect_missing_report_dates_by_pvz_groups_results(self):
        with patch("scheduler_runner.tasks.reports.reports_processor.detect_missing_report_dates") as mock_detect:
            mock_detect.side_effect = [
                {"success": True, "pvz_id": "PVZ1", "missing_dates": ["2026-03-01"], "truncated": False},
                {"success": True, "pvz_id": "PVZ2", "missing_dates": ["2026-03-02"], "truncated": True},
            ]

            result = reports_processor.detect_missing_report_dates_by_pvz(
                date_from="2026-03-01",
                date_to="2026-03-03",
                pvz_ids=["PVZ1", "PVZ2"],
                logger=Mock(),
                max_missing_dates=2,
            )

        self.assertTrue(result["success"])
        self.assertEqual(result["missing_dates_by_pvz"]["PVZ1"], ["2026-03-01"])
        self.assertEqual(result["missing_dates_by_pvz"]["PVZ2"], ["2026-03-02"])
        self.assertEqual(result["truncated_pvz_ids"], ["PVZ2"])

    def test_prepare_coverage_filters_normalizes_pvz(self):
        filters = reports_processor.prepare_coverage_filters(
            date_from="2026-03-01",
            date_to="2026-03-03",
            pvz_id="\u0427\u0415\u0411\u041e\u041a\u0421\u0410\u0420\u042b_340",
        )

        self.assertEqual(filters["\u0414\u0430\u0442\u0430_from"], "2026-03-01")
        self.assertEqual(filters["\u0414\u0430\u0442\u0430_to"], "2026-03-03")
        self.assertEqual(filters["\u041f\u0412\u0417"], ["cheboksary_340"])

    @patch("scheduler_runner.tasks.reports.reports_processor.prepare_connection_params")
    @patch("scheduler_runner.tasks.reports.reports_processor.check_missing_items")
    def test_detect_missing_report_dates_converts_and_limits_dates(self, mock_check_missing_items, mock_prepare_connection_params):
        mock_prepare_connection_params.return_value = {"TABLE_CONFIG": Mock()}
        mock_check_missing_items.return_value = {
            "success": True,
            "data": {
                "missing_items": [
                    {"\u0414\u0430\u0442\u0430": "03.03.2026", "\u041f\u0412\u0417": "cheboksary_340"},
                    {"\u0414\u0430\u0442\u0430": "01.03.2026", "\u041f\u0412\u0417": "cheboksary_340"},
                    {"\u0414\u0430\u0442\u0430": "02.03.2026", "\u041f\u0412\u0417": "cheboksary_340"},
                ]
            }
        }

        result = reports_processor.detect_missing_report_dates(
            date_from="2026-03-01",
            date_to="2026-03-03",
            logger=Mock(),
            max_missing_dates=2,
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["missing_dates"], ["2026-03-01", "2026-03-02"])
        self.assertTrue(result["truncated"])

    def test_prepare_upload_data_batch_uses_only_successful_results(self):
        batch_result = {
            "results_by_date": {
                "2026-03-01": {"success": True, "data": {"execution_date": "2026-03-01", "location_info": "PVZ1", "summary": {}}},
                "2026-03-02": {"success": False, "error": "parse failed"},
            }
        }

        with patch("scheduler_runner.tasks.reports.reports_processor.prepare_upload_data") as mock_prepare_upload_data:
            mock_prepare_upload_data.return_value = [{"Р”Р°С‚Р°": "01.03.2026", "РџР’Р—": "PVZ1"}]
            result = reports_processor.prepare_upload_data_batch(batch_result)

        self.assertEqual(result, [{"Р”Р°С‚Р°": "01.03.2026", "РџР’Р—": "PVZ1"}])
        mock_prepare_upload_data.assert_called_once()

    def test_convert_job_results_to_batch_result(self):
        result = reports_processor.convert_job_results_to_batch_result(
            [
                ParserJobResult.from_success(
                    report_type="ozon_reports",
                    pvz_id="pvz1",
                    execution_date="2026-03-01",
                    data={"execution_date": "2026-03-01"},
                ),
                ParserJobResult.from_error(
                    report_type="ozon_reports",
                    pvz_id="pvz1",
                    execution_date="2026-03-02",
                    error_message="parse failed",
                ),
            ]
        )

        self.assertFalse(result["success"])
        self.assertEqual(result["successful_dates"], ["2026-03-01"])
        self.assertEqual(result["failed_dates"], ["2026-03-02"])
        self.assertEqual(result["results_by_date"]["2026-03-01"]["data"]["execution_date"], "2026-03-01")
        self.assertEqual(result["results_by_date"]["2026-03-02"]["error"], "parse failed")

    @patch("scheduler_runner.tasks.reports.reports_processor.send_notification_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.format_batch_notification_message")
    @patch("scheduler_runner.tasks.reports.reports_processor.prepare_batch_notification_data")
    @patch("scheduler_runner.tasks.reports.reports_processor.run_upload_batch_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.invoke_parser_for_pvz")
    @patch("scheduler_runner.tasks.reports.reports_processor.detect_missing_report_dates")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_backfill_uses_new_parser_api_when_requested(
        self,
        mock_parse_args,
        mock_detect_missing_report_dates,
        mock_invoke_parser_for_pvz,
        mock_run_upload_batch_microservice,
        mock_prepare_batch_notification_data,
        mock_format_batch_notification_message,
        mock_send_notification_microservice,
    ):
        mock_parse_args.return_value = Namespace(
            execution_date=None,
            date_from="2026-03-01",
            date_to="2026-03-02",
            backfill_days=7,
            mode="backfill",
            max_missing_dates=7,
            parser_api="new",
            pvz=None,
            detailed_logs=False,
        )
        mock_detect_missing_report_dates.return_value = {"success": True, "missing_dates": ["2026-03-01"]}
        mock_invoke_parser_for_pvz.return_value = {"results_by_date": {}, "successful_dates": [], "failed_dates": []}
        mock_run_upload_batch_microservice.return_value = {"success": True}
        mock_prepare_batch_notification_data.return_value = {"summary": "ok"}
        mock_format_batch_notification_message.return_value = "ok"

        reports_processor.main()

        self.assertEqual(mock_detect_missing_report_dates.call_args.kwargs["pvz_id"], reports_processor.PVZ_ID)
        self.assertEqual(mock_invoke_parser_for_pvz.call_args.kwargs["parser_api"], "new")
        self.assertEqual(
            [(job.pvz_id, job.execution_date) for job in mock_invoke_parser_for_pvz.call_args.kwargs["jobs"]],
            [(reports_processor.PVZ_ID, "2026-03-01")],
        )
        mock_run_upload_batch_microservice.assert_called_once()
        mock_send_notification_microservice.assert_called_once()

    @patch("scheduler_runner.tasks.reports.reports_processor.send_notification_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.format_batch_notification_message")
    @patch("scheduler_runner.tasks.reports.reports_processor.prepare_batch_notification_data")
    @patch("scheduler_runner.tasks.reports.reports_processor.run_upload_batch_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.invoke_parser_for_pvz")
    @patch("scheduler_runner.tasks.reports.reports_processor.detect_missing_report_dates")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_backfill_uses_legacy_parser_api_by_default(
        self,
        mock_parse_args,
        mock_detect_missing_report_dates,
        mock_invoke_parser_for_pvz,
        mock_run_upload_batch_microservice,
        mock_prepare_batch_notification_data,
        mock_format_batch_notification_message,
        mock_send_notification_microservice,
    ):
        mock_parse_args.return_value = Namespace(
            execution_date=None,
            date_from="2026-03-01",
            date_to="2026-03-02",
            backfill_days=7,
            mode="backfill",
            max_missing_dates=7,
            parser_api="legacy",
            pvz=None,
            detailed_logs=False,
        )
        mock_detect_missing_report_dates.return_value = {"success": True, "missing_dates": ["2026-03-01"]}
        mock_invoke_parser_for_pvz.return_value = {"results_by_date": {}, "successful_dates": [], "failed_dates": []}
        mock_run_upload_batch_microservice.return_value = {"success": True}
        mock_prepare_batch_notification_data.return_value = {"summary": "ok"}
        mock_format_batch_notification_message.return_value = "ok"

        reports_processor.main()

        self.assertEqual(mock_detect_missing_report_dates.call_args.kwargs["pvz_id"], reports_processor.PVZ_ID)
        self.assertEqual(mock_invoke_parser_for_pvz.call_args.kwargs["parser_api"], "legacy")
        self.assertEqual(
            [(job.pvz_id, job.execution_date) for job in mock_invoke_parser_for_pvz.call_args.kwargs["jobs"]],
            [(reports_processor.PVZ_ID, "2026-03-01")],
        )
        mock_run_upload_batch_microservice.assert_called_once()
        mock_send_notification_microservice.assert_called_once()

    @patch("scheduler_runner.tasks.reports.reports_processor.send_notification_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.format_batch_notification_message")
    @patch("scheduler_runner.tasks.reports.reports_processor.prepare_batch_notification_data")
    @patch("scheduler_runner.tasks.reports.reports_processor.run_upload_batch_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.invoke_available_pvz_discovery")
    @patch("scheduler_runner.tasks.reports.reports_processor.invoke_parser_for_pvz")
    @patch("scheduler_runner.tasks.reports.reports_processor.detect_missing_report_dates")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_backfill_with_single_pvz_keeps_single_pvz_path(
        self,
        mock_parse_args,
        mock_detect_missing_report_dates,
        mock_invoke_parser_for_pvz,
        mock_invoke_available_pvz_discovery,
        mock_run_upload_batch_microservice,
        mock_prepare_batch_notification_data,
        mock_format_batch_notification_message,
        mock_send_notification_microservice,
    ):
        mock_parse_args.return_value = Namespace(
            execution_date=None,
            date_from="2026-03-01",
            date_to="2026-03-02",
            backfill_days=7,
            mode="backfill",
            max_missing_dates=7,
            parser_api="legacy",
            pvz=["PVZ1"],
            detailed_logs=False,
        )
        mock_invoke_available_pvz_discovery.return_value = {
            "success": True,
            "available_pvz": ["PVZ1"],
        }
        mock_detect_missing_report_dates.return_value = {"success": True, "missing_dates": ["2026-03-01"]}
        mock_invoke_parser_for_pvz.return_value = {"results_by_date": {}, "successful_dates": [], "failed_dates": []}
        mock_run_upload_batch_microservice.return_value = {"success": True}
        mock_prepare_batch_notification_data.return_value = {"summary": "ok"}
        mock_format_batch_notification_message.return_value = "ok"

        reports_processor.main()

        self.assertEqual(mock_detect_missing_report_dates.call_args.kwargs["pvz_id"], "PVZ1")
        self.assertEqual(
            [(job.pvz_id, job.execution_date) for job in mock_invoke_parser_for_pvz.call_args.kwargs["jobs"]],
            [("PVZ1", "2026-03-01")],
        )
        mock_send_notification_microservice.assert_called_once()

    @patch("scheduler_runner.tasks.reports.reports_processor.send_notification_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.format_aggregated_backfill_notification_message")
    @patch("scheduler_runner.tasks.reports.reports_processor.build_aggregated_backfill_summary")
    @patch("scheduler_runner.tasks.reports.reports_processor.prepare_batch_notification_data")
    @patch("scheduler_runner.tasks.reports.reports_processor.run_upload_batch_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.invoke_available_pvz_discovery")
    @patch("scheduler_runner.tasks.reports.reports_processor.invoke_parser_for_grouped_jobs")
    @patch("scheduler_runner.tasks.reports.reports_processor.detect_missing_report_dates_by_pvz")
    @patch("scheduler_runner.tasks.reports.reports_processor.detect_missing_report_dates")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_backfill_with_multiple_pvz_uses_loop_per_pvz(
        self,
        mock_parse_args,
        mock_detect_missing_report_dates,
        mock_detect_missing_report_dates_by_pvz,
        mock_invoke_parser_for_grouped_jobs,
        mock_invoke_available_pvz_discovery,
        mock_run_upload_batch_microservice,
        mock_prepare_batch_notification_data,
        mock_build_aggregated_backfill_summary,
        mock_format_aggregated_backfill_notification_message,
        mock_send_notification_microservice,
    ):
        mock_parse_args.return_value = Namespace(
            execution_date=None,
            date_from="2026-03-01",
            date_to="2026-03-02",
            backfill_days=7,
            mode="backfill",
            max_missing_dates=7,
            parser_api="legacy",
            pvz=["PVZ1", "PVZ2"],
            detailed_logs=False,
        )
        mock_invoke_available_pvz_discovery.return_value = {
            "success": True,
            "available_pvz": ["PVZ1", "PVZ2"],
        }
        mock_detect_missing_report_dates_by_pvz.return_value = {
            "success": True,
            "missing_dates_by_pvz": {"PVZ1": ["2026-03-01"], "PVZ2": ["2026-03-02"]},
            "coverage_results_by_pvz": {"PVZ1": {"missing_dates": ["2026-03-01"]}, "PVZ2": {"missing_dates": ["2026-03-02"]}},
        }
        mock_invoke_parser_for_grouped_jobs.return_value = {
            "PVZ1": {"results_by_date": {}, "successful_dates": [], "failed_dates": []},
            "PVZ2": {"results_by_date": {}, "successful_dates": [], "failed_dates": []},
        }
        mock_run_upload_batch_microservice.return_value = {"success": True}
        mock_prepare_batch_notification_data.return_value = {"summary": "ok"}
        mock_build_aggregated_backfill_summary.return_value = reports_processor.ReportsBackfillExecutionResult(
            date_from="2026-03-01",
            date_to="2026-03-02",
            processed_pvz_count=2,
            missing_dates_count=2,
            successful_jobs_count=0,
            failed_jobs_count=0,
            uploaded_records=0,
            pvz_results={},
        )
        mock_format_aggregated_backfill_notification_message.return_value = "ok"

        reports_processor.main()

        mock_detect_missing_report_dates.assert_not_called()
        mock_detect_missing_report_dates_by_pvz.assert_called_once()
        mock_invoke_parser_for_grouped_jobs.assert_called_once()
        self.assertEqual(
            sorted(mock_invoke_parser_for_grouped_jobs.call_args.kwargs["grouped_jobs"].keys()),
            ["PVZ1", "PVZ2"],
        )
        self.assertEqual(mock_invoke_parser_for_grouped_jobs.call_args.kwargs["pvz_ids"], ["PVZ1", "PVZ2"])
        self.assertEqual(mock_invoke_parser_for_grouped_jobs.call_args.kwargs["parser_api"], "legacy")
        self.assertEqual(mock_run_upload_batch_microservice.call_count, 2)
        mock_build_aggregated_backfill_summary.assert_called_once()
        mock_format_aggregated_backfill_notification_message.assert_called_once_with(
            mock_build_aggregated_backfill_summary.return_value
        )
        mock_send_notification_microservice.assert_called_once()

    @patch("scheduler_runner.tasks.reports.reports_processor.send_notification_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.format_aggregated_backfill_notification_message")
    @patch("scheduler_runner.tasks.reports.reports_processor.build_aggregated_backfill_summary")
    @patch("scheduler_runner.tasks.reports.reports_processor.prepare_batch_notification_data")
    @patch("scheduler_runner.tasks.reports.reports_processor.run_upload_batch_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.invoke_available_pvz_discovery")
    @patch("scheduler_runner.tasks.reports.reports_processor.invoke_parser_for_pvz")
    @patch("scheduler_runner.tasks.reports.reports_processor.invoke_parser_for_grouped_jobs")
    @patch("scheduler_runner.tasks.reports.reports_processor.detect_missing_report_dates_by_pvz")
    @patch("scheduler_runner.tasks.reports.reports_processor.detect_missing_report_dates")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_backfill_with_multiple_pvz_skips_inaccessible_colleagues(
        self,
        mock_parse_args,
        mock_detect_missing_report_dates,
        mock_detect_missing_report_dates_by_pvz,
        mock_invoke_parser_for_grouped_jobs,
        mock_invoke_parser_for_pvz,
        mock_invoke_available_pvz_discovery,
        mock_run_upload_batch_microservice,
        mock_prepare_batch_notification_data,
        mock_build_aggregated_backfill_summary,
        mock_format_aggregated_backfill_notification_message,
        mock_send_notification_microservice,
    ):
        mock_parse_args.return_value = Namespace(
            execution_date=None,
            date_from="2026-03-01",
            date_to="2026-03-02",
            backfill_days=7,
            mode="backfill",
            max_missing_dates=7,
            parser_api="legacy",
            pvz=["PVZ1", "PVZ2"],
            detailed_logs=False,
        )
        mock_invoke_available_pvz_discovery.return_value = {
            "success": True,
            "available_pvz": ["PVZ1"],
        }
        mock_detect_missing_report_dates.return_value = {
            "success": True,
            "missing_dates": ["2026-03-01"],
        }
        mock_detect_missing_report_dates_by_pvz.return_value = {
            "success": True,
            "missing_dates_by_pvz": {"PVZ1": ["2026-03-01"]},
            "coverage_results_by_pvz": {"PVZ1": {"missing_dates": ["2026-03-01"]}},
        }
        mock_invoke_parser_for_grouped_jobs.return_value = {
            "PVZ1": {"results_by_date": {}, "successful_dates": [], "failed_dates": []},
        }
        mock_invoke_parser_for_pvz.return_value = {"results_by_date": {}, "successful_dates": [], "failed_dates": []}
        mock_run_upload_batch_microservice.return_value = {"success": True}
        mock_prepare_batch_notification_data.return_value = {"summary": "ok"}
        mock_build_aggregated_backfill_summary.return_value = reports_processor.ReportsBackfillExecutionResult(
            date_from="2026-03-01",
            date_to="2026-03-02",
            processed_pvz_count=1,
            missing_dates_count=1,
            successful_jobs_count=0,
            failed_jobs_count=0,
            uploaded_records=0,
            pvz_results={},
        )
        mock_format_aggregated_backfill_notification_message.return_value = "ok"

        reports_processor.main()

        mock_detect_missing_report_dates_by_pvz.assert_not_called()
        self.assertEqual(mock_detect_missing_report_dates.call_args.kwargs["pvz_id"], "PVZ1")
        self.assertEqual(
            [(job.pvz_id, job.execution_date) for job in mock_invoke_parser_for_pvz.call_args.kwargs["jobs"]],
            [("PVZ1", "2026-03-01")],
        )


if __name__ == "__main__":
    unittest.main()


