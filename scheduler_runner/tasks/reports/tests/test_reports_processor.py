import unittest
from argparse import Namespace
from unittest.mock import Mock, patch

from scheduler_runner.utils.parser.core.contracts import ParserJobResult
from scheduler_runner.tasks.reports import reports_processor
from scheduler_runner.tasks.reports.config.scripts.kpi_google_sheets_config import TABLE_CONFIG
from scheduler_runner.utils.uploader.core.providers.google_sheets.google_sheets_data_models import ColumnType


class TestReportsProcessor(unittest.TestCase):
    def test_kpi_table_config_includes_reward_formula_columns(self):
        reward_columns = {
            column.name: column
            for column in TABLE_CONFIG.columns
            if column.name in {
                "Сумма за Количество выдач",
                "Сумма за Прямой поток",
                "Сумма за Возвратный поток",
                "Итого вознаграждение",
            }
        }

        self.assertEqual(set(reward_columns.keys()), {
            "Сумма за Количество выдач",
            "Сумма за Прямой поток",
            "Сумма за Возвратный поток",
            "Итого вознаграждение",
        })
        self.assertEqual(reward_columns["Сумма за Количество выдач"].column_type, ColumnType.FORMULA)
        self.assertEqual(
            reward_columns["Сумма за Количество выдач"].formula_template,
            '=GET_REWARD("Количество выдач";D{row};$B{row};KPI_REWARD_RULES_RANGE)',
        )
        self.assertEqual(
            reward_columns["Сумма за Прямой поток"].formula_template,
            '=GET_REWARD("Прямой поток";E{row};$B{row};KPI_REWARD_RULES_RANGE)',
        )
        self.assertEqual(
            reward_columns["Сумма за Возвратный поток"].formula_template,
            '=GET_REWARD("Возвратный поток";F{row};$B{row};KPI_REWARD_RULES_RANGE)',
        )
        self.assertEqual(
            reward_columns["Итого вознаграждение"].formula_template,
            "=SUM(G{row}:I{row})",
        )

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

    def test_build_owner_final_failover_state_records_marks_success_and_failures(self):
        result = reports_processor.build_owner_final_failover_state_records(
            owner_pvz="PVZ1",
            missing_dates=["2026-03-01", "2026-03-02"],
            batch_result={
                "results_by_date": {
                    "2026-03-01": {"success": True, "data": {}},
                    "2026-03-02": {"success": False, "error": "parse_failed"},
                }
            },
            source_run_id="run-1",
            existing_state_rows_by_date={
                "2026-03-01": {"status": reports_processor.STATUS_OWNER_FAILED}
            },
        )

        statuses = [record["status"] for record in result["records"]]
        self.assertEqual(
            statuses,
            [
                reports_processor.STATUS_OWNER_SUCCESS,
                reports_processor.STATUS_OWNER_FAILED,
            ],
        )
        self.assertEqual(result["successful_dates"], ["2026-03-01"])
        self.assertEqual(result["failed_dates"], ["2026-03-02"])

    def test_classify_owner_success_history_returns_no_state_for_missing_row(self):
        result = reports_processor.classify_owner_success_history(None)

        self.assertEqual(result["classification"], "no_state")
        self.assertEqual(result["status"], "")
        self.assertFalse(result["should_persist_success_if_enabled"])

    def test_classify_owner_success_history_marks_owner_success_as_terminal_success_only(self):
        result = reports_processor.classify_owner_success_history({"status": reports_processor.STATUS_OWNER_SUCCESS})

        self.assertEqual(result["classification"], "terminal_success_only")
        self.assertEqual(result["status"], reports_processor.STATUS_OWNER_SUCCESS)
        self.assertFalse(result["should_persist_success_if_enabled"])

    def test_classify_owner_success_history_marks_incident_related_statuses(self):
        incident_statuses = [
            reports_processor.STATUS_OWNER_FAILED,
            reports_processor.STATUS_CLAIM_EXPIRED,
            reports_processor.STATUS_FAILOVER_FAILED,
            "failover_claimed",
            reports_processor.STATUS_FAILOVER_SUCCESS,
        ]

        for status in incident_statuses:
            with self.subTest(status=status):
                result = reports_processor.classify_owner_success_history({"status": status})
                self.assertEqual(result["classification"], "incident_related")
                self.assertEqual(result["status"], status)
                self.assertTrue(result["should_persist_success_if_enabled"])

    def test_classify_owner_success_history_marks_unexpected_status_as_other(self):
        result = reports_processor.classify_owner_success_history({"status": reports_processor.STATUS_OWNER_PENDING})

        self.assertEqual(result["classification"], "other")
        self.assertEqual(result["status"], reports_processor.STATUS_OWNER_PENDING)
        self.assertFalse(result["should_persist_success_if_enabled"])

    def test_should_persist_owner_success_from_history_suppresses_healthy_new_success(self):
        result = reports_processor.should_persist_owner_success_from_history(None)

        self.assertFalse(result["persisted"])
        self.assertEqual(result["classification"], "no_state")
        self.assertEqual(result["reason"], "healthy_new_success")

    def test_should_persist_owner_success_from_history_persists_incident_history(self):
        result = reports_processor.should_persist_owner_success_from_history(
            {"status": reports_processor.STATUS_OWNER_FAILED}
        )

        self.assertTrue(result["persisted"])
        self.assertEqual(result["classification"], "incident_related")
        self.assertEqual(result["reason"], "incident_history_present")

    def test_should_persist_owner_success_from_history_suppresses_duplicate_owner_success(self):
        result = reports_processor.should_persist_owner_success_from_history(
            {"status": reports_processor.STATUS_OWNER_SUCCESS}
        )

        self.assertFalse(result["persisted"])
        self.assertEqual(result["classification"], "terminal_success_only")
        self.assertEqual(result["reason"], "already_terminal_success")

    def test_build_owner_final_failover_state_records_suppresses_healthy_new_success_rows(self):
        result = reports_processor.build_owner_final_failover_state_records(
            owner_pvz="PVZ1",
            missing_dates=["2026-03-01"],
            batch_result={
                "results_by_date": {
                    "2026-03-01": {"success": True, "data": {}},
                }
            },
            source_run_id="run-1",
            existing_state_rows_by_date={},
        )

        self.assertEqual(result["records"], [])
        self.assertEqual(result["successful_dates"], ["2026-03-01"])
        self.assertEqual(result["failed_dates"], [])
        self.assertEqual(result["suppressed_success_dates"], ["2026-03-01"])
        self.assertFalse(result["success_persistence_by_date"]["2026-03-01"]["persisted"])

    def test_build_owner_final_failover_state_records_persists_success_for_prior_owner_failed(self):
        result = reports_processor.build_owner_final_failover_state_records(
            owner_pvz="PVZ1",
            missing_dates=["2026-03-01"],
            batch_result={
                "results_by_date": {
                    "2026-03-01": {"success": True, "data": {}},
                }
            },
            source_run_id="run-1",
            existing_state_rows_by_date={
                "2026-03-01": {"status": reports_processor.STATUS_OWNER_FAILED}
            },
        )

        self.assertEqual([record["status"] for record in result["records"]], [reports_processor.STATUS_OWNER_SUCCESS])
        self.assertEqual(result["suppressed_success_dates"], [])
        self.assertTrue(result["success_persistence_by_date"]["2026-03-01"]["persisted"])

    def test_build_owner_final_failover_state_records_marks_upload_failure_as_owner_failed(self):
        result = reports_processor.build_owner_final_failover_state_records(
            owner_pvz="PVZ1",
            missing_dates=["2026-03-01"],
            batch_result={
                "results_by_date": {
                    "2026-03-01": {"success": True, "data": {}},
                }
            },
            upload_result={"success": False, "error": "APIError: [503]: The service is currently unavailable"},
            source_run_id="run-1",
        )

        self.assertEqual([record["status"] for record in result["records"]], [reports_processor.STATUS_OWNER_FAILED])
        self.assertEqual(result["successful_dates"], [])
        self.assertEqual(result["failed_dates"], ["2026-03-01"])
        self.assertIn("503", result["records"][0]["last_error"])
        self.assertEqual(result["suppressed_success_dates"], [])

    @patch("scheduler_runner.tasks.reports.reports_processor.get_failover_state_rows_by_keys")
    @patch("scheduler_runner.tasks.reports.reports_processor.upsert_failover_state_records")
    @patch("scheduler_runner.tasks.reports.reports_processor.build_failover_state_record")
    def test_sync_owner_failover_state_from_batch_result_uses_batch_upsert_once(
        self,
        mock_build_failover_state_record,
        mock_upsert_failover_state_records,
        mock_get_failover_state_rows_by_keys,
    ):
        mock_get_failover_state_rows_by_keys.return_value = {
            ("2026-03-01", "pvz1"): {"Дата": "01.03.2026", "target_pvz": "PVZ1", "status": reports_processor.STATUS_OWNER_FAILED}
        }
        mock_build_failover_state_record.side_effect = [
            {"Дата": "2026-03-01", "status": reports_processor.STATUS_OWNER_SUCCESS},
            {"Дата": "2026-03-02", "status": reports_processor.STATUS_OWNER_FAILED},
        ]
        mock_upsert_failover_state_records.return_value = {
            "success": True,
            "results": [{"success": True}, {"success": True}],
        }

        result = reports_processor.sync_owner_failover_state_from_batch_result(
            owner_pvz="PVZ1",
            missing_dates=["2026-03-01", "2026-03-02"],
            batch_result={
                "results_by_date": {
                    "2026-03-01": {"success": True, "data": {}},
                    "2026-03-02": {"success": False, "error": "parse_failed"},
                }
            },
            upload_result={"success": True, "uploaded_records": 1},
            logger=Mock(),
            source_run_id="run-1",
        )

        mock_upsert_failover_state_records.assert_called_once()
        records = mock_upsert_failover_state_records.call_args.args[0]
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["status"], reports_processor.STATUS_OWNER_SUCCESS)
        self.assertEqual(records[1]["status"], reports_processor.STATUS_OWNER_FAILED)
        self.assertEqual(result["successful_dates"], ["2026-03-01"])
        self.assertEqual(result["failed_dates"], ["2026-03-02"])
        self.assertEqual(result["suppressed_success_dates"], [])
        self.assertEqual(result["persisted_rows_count"], 2)
        self.assertEqual(len(result["results"]), 2)

    @patch("scheduler_runner.tasks.reports.reports_processor.get_failover_state_rows_by_keys")
    @patch("scheduler_runner.tasks.reports.reports_processor.upsert_failover_state_records")
    def test_sync_owner_failover_state_from_batch_result_raises_when_batch_upsert_fails(self, mock_upsert, mock_get_existing):
        mock_get_existing.return_value = {
            ("2026-03-01", "pvz1"): {"Дата": "01.03.2026", "target_pvz": "PVZ1", "status": reports_processor.STATUS_OWNER_FAILED}
        }
        mock_upsert.return_value = {"success": False, "results": [{"success": False}]}

        with self.assertRaises(RuntimeError):
            reports_processor.sync_owner_failover_state_from_batch_result(
                owner_pvz="PVZ1",
                missing_dates=["2026-03-01"],
                batch_result={
                    "results_by_date": {
                        "2026-03-01": {"success": True, "data": {}},
                    }
                },
                upload_result={"success": True, "uploaded_records": 1},
                logger=Mock(),
                source_run_id="run-1",
            )

    @patch("scheduler_runner.tasks.reports.reports_processor.get_failover_state_rows_by_keys", return_value={})
    @patch("scheduler_runner.tasks.reports.reports_processor.upsert_failover_state_records")
    def test_sync_owner_failover_state_from_batch_result_skips_batch_upsert_when_all_success_rows_are_suppressed(
        self,
        mock_upsert,
        mock_get_existing,
    ):
        result = reports_processor.sync_owner_failover_state_from_batch_result(
            owner_pvz="PVZ1",
            missing_dates=["2026-03-01"],
            batch_result={
                "results_by_date": {
                    "2026-03-01": {"success": True, "data": {}},
                }
            },
            upload_result={"success": True, "uploaded_records": 1},
            logger=Mock(),
            source_run_id="run-1",
        )

        mock_get_existing.assert_called_once()
        mock_upsert.assert_not_called()
        self.assertEqual(result["successful_dates"], ["2026-03-01"])
        self.assertEqual(result["failed_dates"], [])
        self.assertEqual(result["suppressed_success_dates"], ["2026-03-01"])
        self.assertEqual(result["persisted_rows_count"], 0)
        self.assertEqual(result["results"], [])

    @patch("scheduler_runner.tasks.reports.reports_processor.get_failover_state_rows_by_keys")
    @patch("scheduler_runner.tasks.reports.reports_processor.upsert_failover_state_records")
    def test_sync_owner_failover_state_from_batch_result_persists_only_incident_related_success_rows_in_mixed_batch(
        self,
        mock_upsert,
        mock_get_existing,
    ):
        mock_get_existing.return_value = {
            ("2026-03-01", "pvz1"): {"Дата": "01.03.2026", "target_pvz": "PVZ1", "status": reports_processor.STATUS_OWNER_FAILED}
        }
        mock_upsert.return_value = {"success": True, "results": [{"success": True}]}

        result = reports_processor.sync_owner_failover_state_from_batch_result(
            owner_pvz="PVZ1",
            missing_dates=["2026-03-01", "2026-03-02"],
            batch_result={
                "results_by_date": {
                    "2026-03-01": {"success": True, "data": {}},
                    "2026-03-02": {"success": True, "data": {}},
                }
            },
            upload_result={"success": True, "uploaded_records": 2},
            logger=Mock(),
            source_run_id="run-1",
        )

        mock_upsert.assert_called_once()
        persisted_records = mock_upsert.call_args.args[0]
        self.assertEqual(len(persisted_records), 1)
        self.assertEqual(persisted_records[0]["status"], reports_processor.STATUS_OWNER_SUCCESS)
        self.assertEqual(result["successful_dates"], ["2026-03-01", "2026-03-02"])
        self.assertEqual(result["suppressed_success_dates"], ["2026-03-02"])
        self.assertEqual(result["persisted_rows_count"], 1)

    def test_run_google_sheets_upload_with_retry_retries_retryable_errors(self):
        logger = Mock()
        upload_callable = Mock(side_effect=[
            {"success": False, "error": "APIError: [503]: The service is currently unavailable"},
            {"success": True, "uploaded_records": 1},
        ])

        with patch.dict(
            reports_processor.BACKFILL_CONFIG,
            {"google_sheets_upload_max_attempts": 2, "google_sheets_upload_retry_delay_seconds": 0},
            clear=False,
        ):
            result = reports_processor.run_google_sheets_upload_with_retry(
                upload_callable=upload_callable,
                logger=logger,
            )

        self.assertTrue(result["success"])
        self.assertEqual(upload_callable.call_count, 2)

    def test_run_google_sheets_upload_with_retry_does_not_retry_non_retryable_errors(self):
        logger = Mock()
        upload_callable = Mock(return_value={
            "success": False,
            "error": "Не удалось подключиться к Google Sheets",
        })

        with patch.dict(
            reports_processor.BACKFILL_CONFIG,
            {"google_sheets_upload_max_attempts": 3, "google_sheets_upload_retry_delay_seconds": 0},
            clear=False,
        ):
            result = reports_processor.run_google_sheets_upload_with_retry(
                upload_callable=upload_callable,
                logger=logger,
            )

        self.assertFalse(result["success"])
        self.assertEqual(upload_callable.call_count, 1)

    @patch("scheduler_runner.tasks.reports.reports_processor.list_candidate_failover_rows_fast")
    def test_collect_claimable_failover_rows_filters_inaccessible_and_own_pvz(self, mock_list_candidate_failover_rows_fast):
        mock_list_candidate_failover_rows_fast.return_value = [
            {"Р”Р°С‚Р°": "2026-03-01", "target_pvz": "PVZ1", "owner_pvz": "PVZ1", "status": reports_processor.STATUS_OWNER_FAILED},
            {"Р”Р°С‚Р°": "2026-03-01", "target_pvz": "PVZ2", "owner_pvz": "PVZ2", "status": reports_processor.STATUS_OWNER_FAILED},
            {"Р”Р°С‚Р°": "2026-03-01", "target_pvz": "PVZ3", "owner_pvz": "PVZ3", "status": reports_processor.STATUS_OWNER_FAILED},
        ]

        result = reports_processor.collect_claimable_failover_rows(
            accessible_pvz_ids=["PVZ1", "PVZ2"],
            configured_pvz_id="PVZ1",
            max_claims=10,
            logger=Mock(),
        )

        self.assertEqual(result, [
            {"Р”Р°С‚Р°": "2026-03-01", "target_pvz": "PVZ2", "owner_pvz": "PVZ2", "status": reports_processor.STATUS_OWNER_FAILED},
        ])

    @patch("scheduler_runner.tasks.reports.reports_processor.evaluate_claimable_rows_by_policy")
    @patch("scheduler_runner.tasks.reports.reports_processor.list_candidate_failover_rows_fast")
    def test_collect_claimable_failover_rows_uses_policy_filter_when_enabled(
        self,
        mock_list_candidate_failover_rows_fast,
        mock_evaluate_claimable_rows_by_policy,
    ):
        mock_list_candidate_failover_rows_fast.return_value = [
            {"Дата": "2026-03-01", "target_pvz": "PVZ2", "owner_pvz": "PVZ2", "status": reports_processor.STATUS_FAILOVER_FAILED},
        ]
        mock_evaluate_claimable_rows_by_policy.return_value = {
            "mode": "priority_map_legacy",
            "decisions": [],
            "eligible_rows": mock_list_candidate_failover_rows_fast.return_value,
            "selected_rows": mock_list_candidate_failover_rows_fast.return_value,
            "eligible_count": 1,
            "selected_count": 1,
            "rejected_count": 0,
            "rejected_reasons": {},
        }

        with patch.dict(reports_processor.FAILOVER_POLICY_CONFIG, {"enabled": True}, clear=False):
            result = reports_processor.collect_claimable_failover_rows(
                accessible_pvz_ids=["PVZ2"],
                configured_pvz_id="PVZ1",
                max_claims=3,
                logger=Mock(),
            )

        self.assertEqual(result, mock_list_candidate_failover_rows_fast.return_value)
        mock_evaluate_claimable_rows_by_policy.assert_called_once()

    @patch("scheduler_runner.tasks.reports.reports_processor.evaluate_claimable_rows_by_policy")
    @patch("scheduler_runner.tasks.reports.reports_processor.list_candidate_failover_rows_fast")
    def test_collect_claimable_failover_rows_can_return_policy_evaluation(
        self,
        mock_list_candidate_failover_rows_fast,
        mock_evaluate_claimable_rows_by_policy,
    ):
        mock_list_candidate_failover_rows_fast.return_value = [
            {"Дата": "2026-03-01", "target_pvz": "PVZ2", "owner_pvz": "PVZ2", "status": reports_processor.STATUS_FAILOVER_FAILED},
        ]
        mock_evaluate_claimable_rows_by_policy.return_value = {
            "mode": "capability_ranked",
            "decisions": [],
            "eligible_rows": mock_list_candidate_failover_rows_fast.return_value,
            "selected_rows": mock_list_candidate_failover_rows_fast.return_value,
            "eligible_count": 1,
            "selected_count": 1,
            "rejected_count": 0,
            "rejected_reasons": {},
        }

        with patch.dict(reports_processor.FAILOVER_POLICY_CONFIG, {"enabled": True}, clear=False):
            result = reports_processor.collect_claimable_failover_rows(
                accessible_pvz_ids=["PVZ2"],
                configured_pvz_id="PVZ1",
                max_claims=3,
                logger=Mock(),
                return_evaluation=True,
            )

        self.assertEqual(result["mode"], "capability_ranked")
        self.assertEqual(result["selected_rows"], mock_list_candidate_failover_rows_fast.return_value)
        mock_evaluate_claimable_rows_by_policy.assert_called_once()

    def test_should_scan_failover_candidates_returns_false_for_explicit_empty_priority_list(self):
        with patch.dict(
            reports_processor.FAILOVER_POLICY_CONFIG,
            {
                "enabled": True,
                "priority_map": {"PVZ1": []},
                "allow_unlisted_fallback": False,
            },
            clear=False,
        ):
            decision = reports_processor.should_scan_failover_candidates(
                configured_pvz_id="PVZ1",
                accessible_pvz_ids=["PVZ1", "PVZ2"],
            )

        self.assertFalse(decision["should_scan"])
        self.assertEqual(decision["reason"], "empty_priority_list")

    def test_should_scan_failover_candidates_returns_false_when_priority_candidates_not_accessible(self):
        with patch.dict(
            reports_processor.FAILOVER_POLICY_CONFIG,
            {
                "enabled": True,
                "priority_map": {"PVZ1": ["PVZ2"]},
                "allow_unlisted_fallback": False,
            },
            clear=False,
        ):
            decision = reports_processor.should_scan_failover_candidates(
                configured_pvz_id="PVZ1",
                accessible_pvz_ids=["PVZ1"],
            )

        self.assertFalse(decision["should_scan"])
        self.assertEqual(decision["reason"], "priority_candidates_not_accessible")

    def test_should_scan_failover_candidates_returns_true_when_priority_candidate_accessible(self):
        with patch.dict(
            reports_processor.FAILOVER_POLICY_CONFIG,
            {
                "enabled": True,
                "priority_map": {"PVZ1": ["PVZ2"]},
                "allow_unlisted_fallback": False,
            },
            clear=False,
        ):
            decision = reports_processor.should_scan_failover_candidates(
                configured_pvz_id="PVZ1",
                accessible_pvz_ids=["PVZ1", "PVZ2"],
            )

        self.assertTrue(decision["should_scan"])
        self.assertEqual(decision["reason"], "accessible_priority_candidates")

    def test_should_scan_failover_candidates_capability_ranked_returns_false_for_empty_capability_list(self):
        with patch.dict(
            reports_processor.FAILOVER_POLICY_CONFIG,
            {
                "enabled": True,
                "selection_mode": "capability_ranked",
                "capability_map": {"PVZ1": []},
            },
            clear=False,
        ):
            decision = reports_processor.should_scan_failover_candidates(
                configured_pvz_id="PVZ1",
                accessible_pvz_ids=["PVZ1", "PVZ2"],
            )

        self.assertFalse(decision["should_scan"])
        self.assertEqual(decision["reason"], "empty_capability_list")

    def test_should_scan_failover_candidates_capability_ranked_returns_false_when_targets_not_accessible(self):
        with patch.dict(
            reports_processor.FAILOVER_POLICY_CONFIG,
            {
                "enabled": True,
                "selection_mode": "capability_ranked",
                "capability_map": {"PVZ1": ["PVZ2"]},
            },
            clear=False,
        ):
            decision = reports_processor.should_scan_failover_candidates(
                configured_pvz_id="PVZ1",
                accessible_pvz_ids=["PVZ1"],
            )

        self.assertFalse(decision["should_scan"])
        self.assertEqual(decision["reason"], "capability_targets_not_accessible")

    def test_should_scan_failover_candidates_capability_ranked_returns_true_when_target_accessible(self):
        with patch.dict(
            reports_processor.FAILOVER_POLICY_CONFIG,
            {
                "enabled": True,
                "selection_mode": "capability_ranked",
                "capability_map": {"PVZ1": ["PVZ2"]},
            },
            clear=False,
        ):
            decision = reports_processor.should_scan_failover_candidates(
                configured_pvz_id="PVZ1",
                accessible_pvz_ids=["PVZ1", "PVZ2"],
            )

        self.assertTrue(decision["should_scan"])
        self.assertEqual(decision["reason"], "accessible_capability_targets")

    def test_collect_failover_scan_decisions_adds_capability_ranked_dry_run(self):
        with patch.dict(
            reports_processor.FAILOVER_POLICY_CONFIG,
            {
                "enabled": True,
                "selection_mode": "priority_map_legacy",
                "priority_map": {"PVZ1": ["PVZ2"]},
                "capability_map": {"PVZ1": ["PVZ3"]},
                "dry_run_capability_ranked": True,
            },
            clear=False,
        ):
            decisions = reports_processor.collect_failover_scan_decisions(
                configured_pvz_id="PVZ1",
                accessible_pvz_ids=["PVZ1", "PVZ2"],
            )

        self.assertEqual(decisions["active_mode"], "priority_map_legacy")
        self.assertEqual(decisions["active"]["reason"], "accessible_priority_candidates")
        self.assertEqual(
            decisions["dry_run_capability_ranked"]["reason"],
            "capability_targets_not_accessible",
        )

    def test_build_filtered_batch_result_keeps_only_requested_dates(self):
        result = reports_processor.build_filtered_batch_result(
            batch_result={
                "results_by_date": {
                    "2026-03-01": {"success": True, "data": {}},
                    "2026-03-02": {"success": False, "error": "boom"},
                }
            },
            execution_dates=["2026-03-02"],
        )

        self.assertEqual(result["successful_dates"], [])
        self.assertEqual(result["failed_dates"], ["2026-03-02"])
        self.assertEqual(sorted(result["results_by_date"].keys()), ["2026-03-02"])

    @patch("scheduler_runner.tasks.reports.reports_processor.mark_failover_state")
    @patch("scheduler_runner.tasks.reports.reports_processor.run_upload_batch_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.detect_missing_report_dates")
    @patch("scheduler_runner.tasks.reports.reports_processor.invoke_parser_for_pvz")
    def test_run_claimed_failover_backfill_skips_upload_for_already_covered_dates(
        self,
        mock_invoke_parser_for_pvz,
        mock_detect_missing_report_dates,
        mock_run_upload_batch_microservice,
        mock_mark_failover_state,
    ):
        mock_invoke_parser_for_pvz.return_value = {
            "results_by_date": {
                "2026-03-01": {"success": True, "data": {"execution_date": "2026-03-01"}},
            }
        }
        mock_detect_missing_report_dates.return_value = {
            "success": True,
            "missing_dates": [],
        }

        result = reports_processor.run_claimed_failover_backfill(
            claimed_rows=[{"Р”Р°С‚Р°": "2026-03-01", "target_pvz": "PVZ2", "owner_pvz": "PVZ2"}],
            parser_api="legacy",
            parser_logger=Mock(),
            failover_logger=Mock(),
            claimer_pvz="PVZ1",
            source_run_id="run-1",
        )

        mock_run_upload_batch_microservice.assert_not_called()
        self.assertEqual(result["results_by_pvz"]["PVZ2"]["recoverable_dates"], [])
        self.assertEqual(result["results_by_pvz"]["PVZ2"]["upload_result"]["uploaded_records"], 0)
        self.assertEqual(mock_mark_failover_state.call_args.kwargs["last_error"], "skipped_upload_already_covered")

    @patch("scheduler_runner.tasks.reports.reports_processor.mark_failover_state")
    @patch("scheduler_runner.tasks.reports.reports_processor.run_upload_batch_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.detect_missing_report_dates")
    @patch("scheduler_runner.tasks.reports.reports_processor.invoke_parser_for_pvz")
    def test_run_claimed_failover_backfill_uploads_only_still_missing_dates(
        self,
        mock_invoke_parser_for_pvz,
        mock_detect_missing_report_dates,
        mock_run_upload_batch_microservice,
        mock_mark_failover_state,
    ):
        mock_invoke_parser_for_pvz.return_value = {
            "results_by_date": {
                "2026-03-01": {"success": True, "data": {"execution_date": "2026-03-01"}},
                "2026-03-02": {"success": True, "data": {"execution_date": "2026-03-02"}},
            }
        }
        mock_detect_missing_report_dates.return_value = {
            "success": True,
            "missing_dates": ["2026-03-02"],
        }
        mock_run_upload_batch_microservice.return_value = {"success": True, "uploaded_records": 1}

        result = reports_processor.run_claimed_failover_backfill(
            claimed_rows=[
                {"Р”Р°С‚Р°": "2026-03-01", "target_pvz": "PVZ2", "owner_pvz": "PVZ2"},
                {"Р”Р°С‚Р°": "2026-03-02", "target_pvz": "PVZ2", "owner_pvz": "PVZ2"},
            ],
            parser_api="legacy",
            parser_logger=Mock(),
            failover_logger=Mock(),
            claimer_pvz="PVZ1",
            source_run_id="run-1",
        )

        uploaded_batch_result = mock_run_upload_batch_microservice.call_args.args[0]
        self.assertEqual(sorted(uploaded_batch_result["results_by_date"].keys()), ["2026-03-02"])
        self.assertEqual(result["results_by_pvz"]["PVZ2"]["recoverable_dates"], ["2026-03-02"])

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

    def test_build_owner_run_summary_collects_owner_metrics(self):
        summary = reports_processor.build_owner_run_summary(
            pvz_id="PVZ1",
            coverage_result={"missing_dates": ["2026-03-01", "2026-03-02"]},
            batch_result={
                "successful_dates": ["2026-03-01"],
                "failed_dates": ["2026-03-02"],
            },
            upload_result={"success": True, "uploaded_records": 1},
        )

        self.assertEqual(summary.pvz_id, "PVZ1")
        self.assertEqual(summary.missing_dates_count, 2)
        self.assertEqual(summary.successful_dates, ["2026-03-01"])
        self.assertEqual(summary.failed_dates, ["2026-03-02"])
        self.assertEqual(summary.uploaded_records, 1)
        self.assertTrue(summary.upload_success)

    def test_build_failover_run_summary_collects_failover_metrics(self):
        summary = reports_processor.build_failover_run_summary(
            enabled=True,
            failover_result={
                "candidate_scan": {
                    "attempted": True,
                    "success": True,
                    "error": "",
                },
                "candidate_rows_count": 3,
                "claimed_rows_count": 2,
                "recovered_pvz_count": 1,
                "recovered_dates_count": 4,
                "failed_recovery_dates_count": 1,
                "uploaded_records": 4,
            },
        )

        self.assertTrue(summary.enabled)
        self.assertFalse(summary.owner_state_sync_attempted)
        self.assertIsNone(summary.owner_state_sync_success)
        self.assertEqual(summary.owner_state_sync_error, "")
        self.assertTrue(summary.candidate_scan_attempted)
        self.assertTrue(summary.candidate_scan_success)
        self.assertEqual(summary.candidate_scan_error, "")
        self.assertEqual(summary.candidate_rows_count, 3)
        self.assertEqual(summary.claimed_rows_count, 2)
        self.assertEqual(summary.recovered_pvz_count, 1)
        self.assertEqual(summary.recovered_dates_count, 4)
        self.assertEqual(summary.failed_recovery_dates_count, 1)
        self.assertEqual(summary.uploaded_records, 4)

    def test_build_failover_run_summary_collects_owner_state_sync_metrics(self):
        summary = reports_processor.build_failover_run_summary(
            enabled=True,
            failover_result={
                "owner_state_sync": {
                    "attempted": True,
                    "success": False,
                    "error": "429 read quota exceeded",
                }
            },
        )

        self.assertTrue(summary.enabled)
        self.assertTrue(summary.owner_state_sync_attempted)
        self.assertFalse(summary.owner_state_sync_success)
        self.assertEqual(summary.owner_state_sync_error, "429 read quota exceeded")
        self.assertFalse(summary.candidate_scan_attempted)
        self.assertIsNone(summary.candidate_scan_success)
        self.assertEqual(summary.candidate_scan_error, "")

    def test_build_failover_run_summary_collects_candidate_scan_metrics(self):
        summary = reports_processor.build_failover_run_summary(
            enabled=True,
            failover_result={
                "candidate_scan": {
                    "attempted": True,
                    "success": False,
                    "error": "Не удалось подключиться к KPI_FAILOVER_STATE",
                }
            },
        )

        self.assertTrue(summary.enabled)
        self.assertTrue(summary.candidate_scan_attempted)
        self.assertFalse(summary.candidate_scan_success)
        self.assertEqual(summary.candidate_scan_error, "Не удалось подключиться к KPI_FAILOVER_STATE")

    def test_format_reports_run_notification_message_builds_single_final_message(self):
        owner_summary = reports_processor.OwnerRunSummary(
            pvz_id="PVZ1",
            coverage_success=True,
            missing_dates=["2026-03-01", "2026-03-02"],
            missing_dates_count=2,
            truncated=False,
            parse_success=False,
            successful_dates=["2026-03-01"],
            successful_dates_count=1,
            failed_dates=["2026-03-02"],
            failed_dates_count=1,
            uploaded_records=1,
            upload_success=True,
            errors=["parse_failed"],
        )
        failover_summary = reports_processor.FailoverRunSummary(
            enabled=True,
            attempted=True,
            discovery_success=True,
            owner_state_sync_attempted=True,
            owner_state_sync_success=True,
            owner_state_sync_error="",
            candidate_scan_attempted=True,
            candidate_scan_success=True,
            candidate_scan_error="",
            available_pvz=["PVZ2"],
            candidate_rows_count=2,
            claimed_rows_count=1,
            recovered_pvz_count=1,
            recovered_dates_count=2,
            failed_recovery_dates_count=0,
            uploaded_records=2,
            results_by_pvz={},
        )
        run_summary = reports_processor.build_reports_run_summary(
            mode="backfill_single_pvz",
            configured_pvz_id="PVZ1",
            date_from="2026-03-01",
            date_to="2026-03-03",
            owner=owner_summary,
            failover=failover_summary,
        )

        message = reports_processor.format_reports_run_notification_message(run_summary)

        self.assertIn("KPI reports run", message)
        self.assertIn("partial", message)
        self.assertIn("PVZ1", message)
        self.assertIn("candidate rows: 2", message)
        self.assertIn("claimed rows: 1", message)
        self.assertIn("owner state sync: ok", message)

    def test_build_owner_run_summary_treats_numeric_failed_dates_as_empty_list(self):
        summary = reports_processor.build_owner_run_summary(
            pvz_id="PVZ1",
            coverage_result={"success": True, "missing_dates": ["2026-03-01"]},
            batch_result={
                "success": True,
                "successful_dates": ["2026-03-01"],
                "failed_dates": 0,
            },
            upload_result={"success": True, "uploaded_records": 1},
        )

        self.assertEqual(summary.failed_dates, [])
        self.assertEqual(summary.failed_dates_count, 0)

    def test_build_owner_run_summary_supports_legacy_numeric_successful_dates_contract(self):
        summary = reports_processor.build_owner_run_summary(
            pvz_id="PVZ1",
            coverage_result={"success": True, "missing_dates": ["2026-03-19"]},
            batch_result={
                "success": True,
                "successful_dates": 1,
                "failed_dates": 0,
                "results_by_date": {
                    "2026-03-19": {"success": True, "data": {}},
                },
            },
            upload_result={"success": True, "uploaded_records": 1},
        )
        run_summary = reports_processor.build_reports_run_summary(
            mode="backfill_single_pvz",
            configured_pvz_id="PVZ1",
            date_from="2026-03-19",
            date_to="2026-03-19",
            owner=summary,
            failover=reports_processor.build_failover_run_summary(enabled=False),
        )
        message = reports_processor.format_reports_run_notification_message(run_summary)

        self.assertEqual(summary.successful_dates, [])
        self.assertEqual(summary.successful_dates_count, 1)
        self.assertEqual(summary.failed_dates_count, 0)
        self.assertEqual(run_summary.final_status, "success")
        self.assertIn("успешно спарсено: 1", message)

    def test_format_reports_run_notification_message_handles_empty_numeric_failed_dates_regression(self):
        owner_summary = reports_processor.build_owner_run_summary(
            pvz_id="PVZ1",
            coverage_result={"success": True, "missing_dates": ["2026-03-01"]},
            batch_result={
                "success": True,
                "successful_dates": ["2026-03-01"],
                "failed_dates": 0,
            },
            upload_result={"success": True, "uploaded_records": 1},
        )
        run_summary = reports_processor.build_reports_run_summary(
            mode="backfill_single_pvz",
            configured_pvz_id="PVZ1",
            date_from="2026-03-01",
            date_to="2026-03-01",
            owner=owner_summary,
            failover=reports_processor.build_failover_run_summary(enabled=False),
        )

        message = reports_processor.format_reports_run_notification_message(run_summary)

        self.assertIn("KPI reports run", message)
        self.assertIn("неуспешные даты: -", message)

    def test_build_reports_run_summary_marks_owner_skipped_and_failover_success_as_success(self):
        owner_summary = reports_processor.build_owner_run_summary(
            pvz_id="PVZ1",
            coverage_result={"success": True, "missing_dates": []},
            batch_result={},
            upload_result={},
        )
        failover_summary = reports_processor.build_failover_run_summary(
            enabled=True,
            failover_result={
                "attempted": True,
                "candidate_rows_count": 2,
                "claimed_rows_count": 1,
                "recovered_pvz_count": 1,
                "recovered_dates_count": 2,
                "failed_recovery_dates_count": 0,
                "uploaded_records": 2,
            },
        )

        run_summary = reports_processor.build_reports_run_summary(
            mode="backfill_single_pvz",
            configured_pvz_id="PVZ1",
            owner=owner_summary,
            failover=failover_summary,
        )
        message = reports_processor.format_reports_run_notification_message(run_summary)

        self.assertEqual(run_summary.final_status, "success")
        self.assertIn("missing dates не было", message)
        self.assertIn("claimed rows: 1", message)
        self.assertIn("восстановлено дат: 2", message)

    def test_build_reports_run_summary_marks_no_owner_work_and_no_failover_work_as_skipped(self):
        owner_summary = reports_processor.build_owner_run_summary(
            pvz_id="PVZ1",
            coverage_result={"success": True, "missing_dates": []},
            batch_result={},
            upload_result={},
        )
        failover_summary = reports_processor.build_failover_run_summary(
            enabled=True,
            failover_result={
                "attempted": True,
                "candidate_rows_count": 0,
                "claimed_rows_count": 0,
                "recovered_pvz_count": 0,
                "recovered_dates_count": 0,
                "failed_recovery_dates_count": 0,
                "uploaded_records": 0,
            },
        )

        run_summary = reports_processor.build_reports_run_summary(
            mode="backfill_single_pvz",
            configured_pvz_id="PVZ1",
            owner=owner_summary,
            failover=failover_summary,
        )
        message = reports_processor.format_reports_run_notification_message(run_summary)

        self.assertEqual(run_summary.final_status, "skipped")
        self.assertIn("missing dates не было", message)
        self.assertIn("coordination включен, recovery работа не потребовалась", message)
        self.assertIn("candidate rows: 0", message)

    def test_build_reports_run_summary_marks_owner_success_and_sync_failure_as_partial(self):
        owner_summary = reports_processor.build_owner_run_summary(
            pvz_id="PVZ1",
            coverage_result={"success": True, "missing_dates": ["2026-03-19"]},
            batch_result={
                "success": True,
                "successful_dates": 1,
                "failed_dates": 0,
                "results_by_date": {
                    "2026-03-19": {"success": True, "data": {}},
                },
            },
            upload_result={"success": True, "uploaded_records": 1},
        )
        failover_summary = reports_processor.build_failover_run_summary(
            enabled=True,
            failover_result={
                "owner_state_sync": {
                    "attempted": True,
                    "success": False,
                    "error": "429 read quota exceeded",
                }
            },
        )

        run_summary = reports_processor.build_reports_run_summary(
            mode="backfill_single_pvz",
            configured_pvz_id="PVZ1",
            owner=owner_summary,
            failover=failover_summary,
        )
        message = reports_processor.format_reports_run_notification_message(run_summary)

        self.assertEqual(run_summary.final_status, "partial")
        self.assertIn("успешно спарсено: 1", message)
        self.assertIn("owner state sync: failed", message)
        self.assertIn("owner state sync error: 429 read quota exceeded", message)

    def test_build_reports_run_summary_marks_owner_success_and_candidate_scan_failure_as_partial(self):
        owner_summary = reports_processor.build_owner_run_summary(
            pvz_id="PVZ1",
            coverage_result={"success": True, "missing_dates": ["2026-03-20"]},
            batch_result={
                "success": True,
                "successful_dates": 1,
                "failed_dates": 0,
                "results_by_date": {
                    "2026-03-20": {"success": True, "data": {}},
                },
            },
            upload_result={"success": True, "uploaded_records": 1},
        )
        failover_summary = reports_processor.build_failover_run_summary(
            enabled=True,
            failover_result={
                "attempted": True,
                "candidate_scan": {
                    "attempted": True,
                    "success": False,
                    "error": "Не удалось подключиться к KPI_FAILOVER_STATE",
                },
            },
        )

        run_summary = reports_processor.build_reports_run_summary(
            mode="backfill_single_pvz",
            configured_pvz_id="PVZ1",
            owner=owner_summary,
            failover=failover_summary,
        )
        message = reports_processor.format_reports_run_notification_message(run_summary)

        self.assertEqual(run_summary.final_status, "partial")
        self.assertIn("candidate scan: failed", message)
        self.assertIn("candidate scan error: Не удалось подключиться к KPI_FAILOVER_STATE", message)

    def test_resolve_final_run_status_marks_owner_partial_and_failover_success_as_partial(self):
        owner_summary = reports_processor.OwnerRunSummary(
            pvz_id="PVZ1",
            coverage_success=True,
            missing_dates=["2026-03-01", "2026-03-02"],
            missing_dates_count=2,
            truncated=False,
            parse_success=False,
            successful_dates=["2026-03-01"],
            successful_dates_count=1,
            failed_dates=["2026-03-02"],
            failed_dates_count=1,
            uploaded_records=1,
            upload_success=True,
            errors=["parse_failed"],
        )
        failover_summary = reports_processor.build_failover_run_summary(
            enabled=True,
            failover_result={
                "attempted": True,
                "candidate_rows_count": 2,
                "claimed_rows_count": 1,
                "recovered_pvz_count": 1,
                "recovered_dates_count": 2,
                "failed_recovery_dates_count": 0,
                "uploaded_records": 2,
            },
        )

        status = reports_processor.resolve_final_run_status(owner=owner_summary, failover=failover_summary)

        self.assertEqual(status, "partial")

    def test_resolve_final_run_status_marks_owner_success_and_candidate_scan_failure_as_partial(self):
        owner_summary = reports_processor.OwnerRunSummary(
            pvz_id="PVZ1",
            coverage_success=True,
            missing_dates=["2026-03-20"],
            missing_dates_count=1,
            truncated=False,
            parse_success=True,
            successful_dates=[],
            successful_dates_count=1,
            failed_dates=[],
            failed_dates_count=0,
            uploaded_records=1,
            upload_success=True,
            errors=[],
        )
        failover_summary = reports_processor.build_failover_run_summary(
            enabled=True,
            failover_result={
                "attempted": True,
                "candidate_scan": {
                    "attempted": True,
                    "success": False,
                    "error": "Не удалось подключиться к KPI_FAILOVER_STATE",
                },
            },
        )

        status = reports_processor.resolve_final_run_status(owner=owner_summary, failover=failover_summary)

        self.assertEqual(status, "partial")

    def test_resolve_final_run_status_marks_owner_success_and_failover_partial_as_partial(self):
        owner_summary = reports_processor.OwnerRunSummary(
            pvz_id="PVZ1",
            coverage_success=True,
            missing_dates=["2026-03-01"],
            missing_dates_count=1,
            truncated=False,
            parse_success=True,
            successful_dates=["2026-03-01"],
            successful_dates_count=1,
            failed_dates=[],
            failed_dates_count=0,
            uploaded_records=1,
            upload_success=True,
            errors=[],
        )
        failover_summary = reports_processor.build_failover_run_summary(
            enabled=True,
            failover_result={
                "attempted": True,
                "candidate_rows_count": 2,
                "claimed_rows_count": 1,
                "recovered_pvz_count": 1,
                "recovered_dates_count": 1,
                "failed_recovery_dates_count": 1,
                "uploaded_records": 1,
            },
        )

        status = reports_processor.resolve_final_run_status(owner=owner_summary, failover=failover_summary)

        self.assertEqual(status, "partial")

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

    def test_should_run_automatic_failover_coordination_for_owner_path(self):
        result = reports_processor.should_run_automatic_failover_coordination(
            enabled=True,
            raw_pvz_ids=None,
            resolved_pvz_ids=[reports_processor.PVZ_ID],
            current_pvz_id=reports_processor.PVZ_ID,
            configured_pvz_id=reports_processor.PVZ_ID,
        )

        self.assertTrue(result)

    def test_should_run_automatic_failover_coordination_is_disabled_for_explicit_pvz(self):
        result = reports_processor.should_run_automatic_failover_coordination(
            enabled=True,
            raw_pvz_ids=["PVZ1"],
            resolved_pvz_ids=["PVZ1"],
            current_pvz_id="PVZ1",
            configured_pvz_id=reports_processor.PVZ_ID,
        )

        self.assertFalse(result)

    def test_should_run_automatic_failover_coordination_is_disabled_for_multi_pvz(self):
        result = reports_processor.should_run_automatic_failover_coordination(
            enabled=True,
            raw_pvz_ids=None,
            resolved_pvz_ids=["PVZ1", "PVZ2"],
            configured_pvz_id=reports_processor.PVZ_ID,
        )

        self.assertFalse(result)

    def test_should_run_automatic_failover_coordination_allows_empty_resolved_scope_without_explicit_pvz(self):
        result = reports_processor.should_run_automatic_failover_coordination(
            enabled=True,
            raw_pvz_ids=None,
            resolved_pvz_ids=[],
            configured_pvz_id=reports_processor.PVZ_ID,
        )

        self.assertTrue(result)

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
    @patch("scheduler_runner.tasks.reports.reports_processor.format_reports_run_notification_message")
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
        mock_format_reports_run_notification_message,
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
            enable_failover_coordination=False,
        )
        mock_detect_missing_report_dates.return_value = {"success": True, "missing_dates": ["2026-03-01"]}
        mock_invoke_parser_for_pvz.return_value = {"results_by_date": {}, "successful_dates": [], "failed_dates": []}
        mock_run_upload_batch_microservice.return_value = {"success": True}
        mock_format_reports_run_notification_message.return_value = "ok"

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
    @patch("scheduler_runner.tasks.reports.reports_processor.format_reports_run_notification_message")
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
        mock_format_reports_run_notification_message,
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
            enable_failover_coordination=False,
        )
        mock_detect_missing_report_dates.return_value = {"success": True, "missing_dates": ["2026-03-01"]}
        mock_invoke_parser_for_pvz.return_value = {"results_by_date": {}, "successful_dates": [], "failed_dates": []}
        mock_run_upload_batch_microservice.return_value = {"success": True}
        mock_format_reports_run_notification_message.return_value = "ok"

        reports_processor.main()

        self.assertEqual(mock_detect_missing_report_dates.call_args.kwargs["pvz_id"], reports_processor.PVZ_ID)
        self.assertEqual(mock_invoke_parser_for_pvz.call_args.kwargs["parser_api"], "legacy")
        self.assertEqual(
            [(job.pvz_id, job.execution_date) for job in mock_invoke_parser_for_pvz.call_args.kwargs["jobs"]],
            [(reports_processor.PVZ_ID, "2026-03-01")],
        )
        mock_run_upload_batch_microservice.assert_called_once()
        mock_send_notification_microservice.assert_called_once()

    @patch("scheduler_runner.tasks.reports.reports_processor.run_failover_coordination_pass")
    @patch("scheduler_runner.tasks.reports.reports_processor.sync_owner_failover_state_from_batch_result")
    @patch("scheduler_runner.tasks.reports.reports_processor.send_notification_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.format_reports_run_notification_message")
    @patch("scheduler_runner.tasks.reports.reports_processor.run_upload_batch_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.invoke_parser_for_pvz")
    @patch("scheduler_runner.tasks.reports.reports_processor.detect_missing_report_dates")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_backfill_with_failover_coordination_syncs_owner_state_and_runs_coordination_pass(
        self,
        mock_parse_args,
        mock_detect_missing_report_dates,
        mock_invoke_parser_for_pvz,
        mock_run_upload_batch_microservice,
        mock_format_reports_run_notification_message,
        mock_send_notification_microservice,
        mock_sync_owner_failover_state_from_batch_result,
        mock_run_failover_coordination_pass,
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
            enable_failover_coordination=True,
        )
        mock_detect_missing_report_dates.return_value = {"success": True, "missing_dates": ["2026-03-01"]}
        mock_invoke_parser_for_pvz.return_value = {
            "results_by_date": {"2026-03-01": {"success": True, "data": {}}},
            "successful_dates": ["2026-03-01"],
            "failed_dates": [],
        }
        mock_run_upload_batch_microservice.return_value = {"success": True}
        mock_format_reports_run_notification_message.return_value = "ok"

        reports_processor.main()

        mock_sync_owner_failover_state_from_batch_result.assert_called_once()
        self.assertEqual(mock_sync_owner_failover_state_from_batch_result.call_args.kwargs["owner_pvz"], reports_processor.PVZ_ID)
        self.assertEqual(mock_sync_owner_failover_state_from_batch_result.call_args.kwargs["missing_dates"], ["2026-03-01"])
        mock_run_failover_coordination_pass.assert_called_once()

    @patch("scheduler_runner.tasks.reports.reports_processor.run_failover_coordination_pass")
    @patch("scheduler_runner.tasks.reports.reports_processor.sync_owner_failover_state_from_batch_result")
    @patch("scheduler_runner.tasks.reports.reports_processor.send_notification_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.format_reports_run_notification_message")
    @patch("scheduler_runner.tasks.reports.reports_processor.run_upload_batch_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.invoke_available_pvz_discovery")
    @patch("scheduler_runner.tasks.reports.reports_processor.invoke_parser_for_pvz")
    @patch("scheduler_runner.tasks.reports.reports_processor.detect_missing_report_dates")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_backfill_with_explicit_pvz_does_not_run_automatic_coordination(
        self,
        mock_parse_args,
        mock_detect_missing_report_dates,
        mock_invoke_parser_for_pvz,
        mock_invoke_available_pvz_discovery,
        mock_run_upload_batch_microservice,
        mock_format_reports_run_notification_message,
        mock_send_notification_microservice,
        mock_sync_owner_failover_state_from_batch_result,
        mock_run_failover_coordination_pass,
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
            enable_failover_coordination=True,
        )
        mock_invoke_available_pvz_discovery.return_value = {
            "success": True,
            "available_pvz": ["PVZ1"],
        }
        mock_detect_missing_report_dates.return_value = {"success": True, "missing_dates": ["2026-03-01"]}
        mock_invoke_parser_for_pvz.return_value = {
            "results_by_date": {"2026-03-01": {"success": True, "data": {}}},
            "successful_dates": ["2026-03-01"],
            "failed_dates": [],
        }
        mock_run_upload_batch_microservice.return_value = {"success": True, "uploaded_records": 1}
        mock_format_reports_run_notification_message.return_value = "ok"

        reports_processor.main()

        mock_sync_owner_failover_state_from_batch_result.assert_not_called()
        mock_run_failover_coordination_pass.assert_not_called()
        mock_send_notification_microservice.assert_called_once()

    @patch("scheduler_runner.tasks.reports.reports_processor.run_failover_coordination_pass")
    @patch("scheduler_runner.tasks.reports.reports_processor.sync_owner_failover_state_from_batch_result")
    @patch("scheduler_runner.tasks.reports.reports_processor.send_notification_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.format_reports_run_notification_message")
    @patch("scheduler_runner.tasks.reports.reports_processor.run_upload_batch_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.invoke_parser_for_pvz")
    @patch("scheduler_runner.tasks.reports.reports_processor.detect_missing_report_dates")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_backfill_with_failover_sends_notification_only_after_coordination(
        self,
        mock_parse_args,
        mock_detect_missing_report_dates,
        mock_invoke_parser_for_pvz,
        mock_run_upload_batch_microservice,
        mock_format_reports_run_notification_message,
        mock_send_notification_microservice,
        mock_sync_owner_failover_state_from_batch_result,
        mock_run_failover_coordination_pass,
    ):
        call_order = []
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
            enable_failover_coordination=True,
        )
        mock_detect_missing_report_dates.return_value = {"success": True, "missing_dates": ["2026-03-01"]}
        mock_invoke_parser_for_pvz.return_value = {
            "results_by_date": {"2026-03-01": {"success": True, "data": {}}},
            "successful_dates": ["2026-03-01"],
            "failed_dates": [],
        }
        mock_run_upload_batch_microservice.return_value = {"success": True, "uploaded_records": 1}
        mock_format_reports_run_notification_message.return_value = "ok"
        mock_run_failover_coordination_pass.side_effect = lambda **kwargs: call_order.append("coordination") or {}
        mock_send_notification_microservice.side_effect = lambda *args, **kwargs: call_order.append("notification")

        reports_processor.main()

        self.assertEqual(call_order, ["coordination", "notification"])

    @patch("scheduler_runner.tasks.reports.reports_processor.run_failover_coordination_pass")
    @patch("scheduler_runner.tasks.reports.reports_processor.sync_owner_failover_state_from_batch_result")
    @patch("scheduler_runner.tasks.reports.reports_processor.send_notification_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.invoke_parser_for_pvz")
    @patch("scheduler_runner.tasks.reports.reports_processor.run_upload_batch_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.detect_missing_report_dates")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_backfill_with_owner_state_sync_failure_keeps_owner_result_and_skips_failover_pass(
        self,
        mock_parse_args,
        mock_detect_missing_report_dates,
        mock_run_upload_batch_microservice,
        mock_invoke_parser_for_pvz,
        mock_send_notification_microservice,
        mock_sync_owner_failover_state_from_batch_result,
        mock_run_failover_coordination_pass,
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
            enable_failover_coordination=True,
        )
        mock_detect_missing_report_dates.return_value = {"success": True, "missing_dates": ["2026-03-01"]}
        mock_invoke_parser_for_pvz.return_value = {
            "success": True,
            "results_by_date": {"2026-03-01": {"success": True, "data": {}}},
            "successful_dates": 1,
            "failed_dates": 0,
        }
        mock_run_upload_batch_microservice.return_value = {"success": True, "uploaded_records": 1}
        mock_sync_owner_failover_state_from_batch_result.side_effect = Exception("429 read quota exceeded")

        reports_processor.main()

        mock_sync_owner_failover_state_from_batch_result.assert_called_once()
        mock_run_failover_coordination_pass.assert_not_called()
        mock_send_notification_microservice.assert_called_once()
        notification_message = mock_send_notification_microservice.call_args.args[0]
        self.assertIn("Статус: partial", notification_message)
        self.assertIn("успешно спарсено: 1", notification_message)
        self.assertIn("owner state sync: failed", notification_message)
        self.assertIn("owner state sync error: 429 read quota exceeded", notification_message)

    @patch("scheduler_runner.tasks.reports.reports_processor.run_failover_coordination_pass")
    @patch("scheduler_runner.tasks.reports.reports_processor.sync_owner_failover_state_from_batch_result")
    @patch("scheduler_runner.tasks.reports.reports_processor.send_notification_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.invoke_parser_for_pvz")
    @patch("scheduler_runner.tasks.reports.reports_processor.run_upload_batch_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.detect_missing_report_dates")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_backfill_with_upload_failure_skips_owner_state_sync_and_failover_pass(
        self,
        mock_parse_args,
        mock_detect_missing_report_dates,
        mock_run_upload_batch_microservice,
        mock_invoke_parser_for_pvz,
        mock_send_notification_microservice,
        mock_sync_owner_failover_state_from_batch_result,
        mock_run_failover_coordination_pass,
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
            enable_failover_coordination=True,
        )
        mock_detect_missing_report_dates.return_value = {"success": True, "missing_dates": ["2026-03-01"]}
        mock_invoke_parser_for_pvz.return_value = {
            "success": True,
            "results_by_date": {"2026-03-01": {"success": True, "data": {}}},
            "successful_dates": 1,
            "failed_dates": 0,
        }
        mock_run_upload_batch_microservice.return_value = {
            "success": False,
            "error": "APIError: [503]: The service is currently unavailable",
            "uploaded_records": 0,
        }

        reports_processor.main()

        mock_sync_owner_failover_state_from_batch_result.assert_not_called()
        mock_run_failover_coordination_pass.assert_not_called()
        mock_send_notification_microservice.assert_called_once()
        notification_message = mock_send_notification_microservice.call_args.args[0]
        self.assertIn("Статус: partial", notification_message)
        self.assertIn("успешно спарсено: 1", notification_message)
        self.assertIn("загружено записей: 0", notification_message)
        self.assertIn("service is currently unavailable", notification_message)

    @patch("scheduler_runner.tasks.reports.reports_processor.claim_failover_rows")
    @patch("scheduler_runner.tasks.reports.reports_processor.collect_claimable_failover_rows")
    @patch("scheduler_runner.tasks.reports.reports_processor.discover_available_pvz_scope")
    def test_run_failover_coordination_pass_keeps_candidate_scan_failure_non_fatal(
        self,
        mock_discover_available_pvz_scope,
        mock_collect_claimable_failover_rows,
        mock_claim_failover_rows,
    ):
        mock_discover_available_pvz_scope.return_value = {
            "discovery_result": {"success": True},
            "available_pvz": ["PVZ1"],
        }
        mock_collect_claimable_failover_rows.side_effect = RuntimeError(
            "Не удалось подключиться к KPI_FAILOVER_STATE"
        )

        result = reports_processor.run_failover_coordination_pass(
            configured_pvz_id="PVZ1",
            parser_api="legacy",
            parser_logger=Mock(),
            processor_logger=Mock(),
            source_run_id="run-1",
        )

        self.assertTrue(result["attempted"])
        self.assertEqual(result["available_pvz"], ["PVZ1"])
        self.assertEqual(result["candidate_rows"], [])
        self.assertEqual(result["candidate_rows_count"], 0)
        self.assertEqual(result["claimed_rows"], [])
        self.assertEqual(result["claimed_rows_count"], 0)
        self.assertTrue(result["candidate_scan"]["attempted"])
        self.assertFalse(result["candidate_scan"]["success"])
        self.assertEqual(
            result["candidate_scan"]["error"],
            "Не удалось подключиться к KPI_FAILOVER_STATE",
        )
        mock_claim_failover_rows.assert_not_called()

    @patch("scheduler_runner.tasks.reports.reports_processor.failover_state_connection")
    @patch("scheduler_runner.tasks.reports.reports_processor.run_claimed_failover_backfill")
    @patch("scheduler_runner.tasks.reports.reports_processor.claim_failover_rows")
    @patch("scheduler_runner.tasks.reports.reports_processor.collect_claimable_failover_rows")
    @patch("scheduler_runner.tasks.reports.reports_processor.discover_available_pvz_scope")
    def test_run_failover_coordination_pass_reuses_single_failover_uploader_for_scan_and_claim(
        self,
        mock_discover_available_pvz_scope,
        mock_collect_claimable_failover_rows,
        mock_claim_failover_rows,
        mock_run_claimed_failover_backfill,
        mock_failover_state_connection,
    ):
        shared_uploader = Mock()
        connection = mock_failover_state_connection.return_value
        connection.__enter__.return_value = shared_uploader
        connection.__exit__.return_value = False
        mock_discover_available_pvz_scope.return_value = {
            "discovery_result": {"success": True},
            "available_pvz": ["PVZ1", "PVZ2"],
        }
        mock_collect_claimable_failover_rows.return_value = [
            {"Дата": "2026-03-14", "target_pvz": "PVZ2", "owner_pvz": "PVZ2", "status": reports_processor.STATUS_OWNER_FAILED},
        ]
        mock_claim_failover_rows.return_value = [
            {"Дата": "2026-03-14", "target_pvz": "PVZ2", "owner_pvz": "PVZ2", "status": reports_processor.STATUS_OWNER_FAILED},
        ]
        mock_run_claimed_failover_backfill.return_value = {
            "results_by_pvz": {},
            "recovered_pvz_count": 0,
            "recovered_dates_count": 0,
            "failed_recovery_dates_count": 0,
            "uploaded_records": 0,
        }

        with patch.dict(
            reports_processor.FAILOVER_POLICY_CONFIG,
            {
                "enabled": True,
                "priority_map": {"PVZ1": ["PVZ2"]},
                "allow_unlisted_fallback": False,
            },
            clear=False,
        ):
            result = reports_processor.run_failover_coordination_pass(
                configured_pvz_id="PVZ1",
                parser_api="legacy",
                parser_logger=Mock(),
                processor_logger=Mock(),
                source_run_id="run-1",
            )

        mock_failover_state_connection.assert_called_once()
        self.assertIs(mock_collect_claimable_failover_rows.call_args.kwargs["uploader"], shared_uploader)
        self.assertIs(mock_claim_failover_rows.call_args.kwargs["uploader"], shared_uploader)
        self.assertEqual(result["candidate_rows_count"], 1)
        self.assertEqual(result["claimed_rows_count"], 1)

    @patch("scheduler_runner.tasks.reports.reports_processor.claim_failover_rows")
    @patch("scheduler_runner.tasks.reports.reports_processor.collect_claimable_failover_rows")
    @patch("scheduler_runner.tasks.reports.reports_processor.discover_available_pvz_scope")
    def test_run_failover_coordination_pass_skips_candidate_scan_when_policy_cannot_claim(
        self,
        mock_discover_available_pvz_scope,
        mock_collect_claimable_failover_rows,
        mock_claim_failover_rows,
    ):
        mock_discover_available_pvz_scope.return_value = {
            "discovery_result": {"success": True},
            "available_pvz": ["PVZ1"],
        }

        with patch.dict(
            reports_processor.FAILOVER_POLICY_CONFIG,
            {
                "enabled": True,
                "priority_map": {"PVZ1": []},
                "allow_unlisted_fallback": False,
            },
            clear=False,
        ):
            result = reports_processor.run_failover_coordination_pass(
                configured_pvz_id="PVZ1",
                parser_api="legacy",
                parser_logger=Mock(),
                processor_logger=Mock(),
                source_run_id="run-1",
            )

        self.assertTrue(result["attempted"])
        self.assertEqual(result["available_pvz"], ["PVZ1"])
        self.assertEqual(result["scan_policy_mode"], "priority_map_legacy")
        self.assertFalse(result["candidate_scan"]["attempted"])
        self.assertIsNone(result["candidate_scan"]["success"])
        self.assertEqual(result["candidate_rows"], [])
        self.assertEqual(result["candidate_rows_count"], 0)
        self.assertEqual(result["claimed_rows"], [])
        self.assertEqual(result["claimed_rows_count"], 0)
        mock_collect_claimable_failover_rows.assert_not_called()
        mock_claim_failover_rows.assert_not_called()

    @patch("scheduler_runner.tasks.reports.reports_processor.failover_state_connection")
    @patch("scheduler_runner.tasks.reports.reports_processor.claim_failover_rows")
    @patch("scheduler_runner.tasks.reports.reports_processor.collect_claimable_failover_rows")
    @patch("scheduler_runner.tasks.reports.reports_processor.discover_available_pvz_scope")
    def test_run_failover_coordination_pass_records_dry_run_scan_decisions(
        self,
        mock_discover_available_pvz_scope,
        mock_collect_claimable_failover_rows,
        mock_claim_failover_rows,
        mock_failover_state_connection,
    ):
        shared_uploader = Mock()
        connection = mock_failover_state_connection.return_value
        connection.__enter__.return_value = shared_uploader
        connection.__exit__.return_value = False
        mock_discover_available_pvz_scope.return_value = {
            "discovery_result": {"success": True},
            "available_pvz": ["PVZ1", "PVZ2"],
        }
        mock_collect_claimable_failover_rows.return_value = []

        with patch.dict(
            reports_processor.FAILOVER_POLICY_CONFIG,
            {
                "enabled": True,
                "selection_mode": "priority_map_legacy",
                "priority_map": {"PVZ1": ["PVZ2"]},
                "capability_map": {"PVZ1": ["PVZ3"]},
                "dry_run_capability_ranked": True,
            },
            clear=False,
        ):
            result = reports_processor.run_failover_coordination_pass(
                configured_pvz_id="PVZ1",
                parser_api="legacy",
                parser_logger=Mock(),
                processor_logger=Mock(),
                source_run_id="run-1",
            )

        self.assertEqual(result["scan_policy_mode"], "priority_map_legacy")
        self.assertIn("dry_run_capability_ranked", result["scan_decisions"])
        self.assertEqual(
            result["scan_decisions"]["dry_run_capability_ranked"]["reason"],
            "capability_targets_not_accessible",
        )
        mock_collect_claimable_failover_rows.assert_called_once()
        mock_claim_failover_rows.assert_not_called()

    @patch("scheduler_runner.tasks.reports.reports_processor.failover_state_connection")
    @patch("scheduler_runner.tasks.reports.reports_processor.run_claimed_failover_backfill")
    @patch("scheduler_runner.tasks.reports.reports_processor.claim_failover_rows")
    @patch("scheduler_runner.tasks.reports.reports_processor.collect_claimable_failover_rows")
    @patch("scheduler_runner.tasks.reports.reports_processor.discover_available_pvz_scope")
    def test_run_failover_coordination_pass_propagates_capability_ranked_arbitration_summary(
        self,
        mock_discover_available_pvz_scope,
        mock_collect_claimable_failover_rows,
        mock_claim_failover_rows,
        mock_run_claimed_failover_backfill,
        mock_failover_state_connection,
    ):
        shared_uploader = Mock()
        connection = mock_failover_state_connection.return_value
        connection.__enter__.return_value = shared_uploader
        connection.__exit__.return_value = False
        mock_discover_available_pvz_scope.return_value = {
            "discovery_result": {"success": True},
            "available_pvz": ["PVZ_HELPER_A", "PVZ_TARGET_1"],
        }
        selected_rows = [
            {"Дата": "2026-03-14", "target_pvz": "PVZ_TARGET_1", "owner_pvz": "PVZ_TARGET_1", "status": reports_processor.STATUS_OWNER_FAILED},
        ]
        mock_collect_claimable_failover_rows.return_value = {
            "mode": "capability_ranked",
            "decisions": [
                {
                    "execution_date": "2026-03-14",
                    "target_pvz": "PVZ_TARGET_1",
                    "status": reports_processor.STATUS_OWNER_FAILED,
                    "eligible": True,
                    "reason": "eligible",
                    "preferred_helper": "pvz_helper_a",
                    "selected_for_claim": True,
                }
            ],
            "eligible_rows": selected_rows,
            "selected_rows": selected_rows,
            "eligible_count": 1,
            "selected_count": 1,
            "rejected_count": 0,
            "rejected_reasons": {},
        }
        mock_claim_failover_rows.return_value = selected_rows
        mock_run_claimed_failover_backfill.return_value = {
            "results_by_pvz": {},
            "recovered_pvz_count": 0,
            "recovered_dates_count": 0,
            "failed_recovery_dates_count": 0,
            "uploaded_records": 0,
        }

        with patch.dict(
            reports_processor.FAILOVER_POLICY_CONFIG,
            {
                "enabled": True,
                "selection_mode": "capability_ranked",
                "capability_map": {"PVZ_HELPER_A": ["PVZ_TARGET_1"]},
            },
            clear=False,
        ):
            result = reports_processor.run_failover_coordination_pass(
                configured_pvz_id="PVZ_HELPER_A",
                parser_api="legacy",
                parser_logger=Mock(),
                processor_logger=Mock(),
                source_run_id="run-1",
            )

        self.assertEqual(result["scan_policy_mode"], "capability_ranked")
        self.assertEqual(result["candidate_policy_evaluation"]["mode"], "capability_ranked")
        self.assertEqual(result["candidate_policy_evaluation"]["selected_count"], 1)
        self.assertEqual(result["candidate_rows_count"], 1)
        self.assertEqual(result["claimed_rows_count"], 1)
        mock_collect_claimable_failover_rows.assert_called_once()
        self.assertTrue(mock_collect_claimable_failover_rows.call_args.kwargs["return_evaluation"])

    @patch("scheduler_runner.tasks.reports.reports_processor.run_failover_coordination_pass")
    @patch("scheduler_runner.tasks.reports.reports_processor.sync_owner_failover_state_from_batch_result")
    @patch("scheduler_runner.tasks.reports.reports_processor.send_notification_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.format_reports_run_notification_message")
    @patch("scheduler_runner.tasks.reports.reports_processor.run_upload_batch_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.invoke_parser_for_pvz")
    @patch("scheduler_runner.tasks.reports.reports_processor.detect_missing_report_dates")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_backfill_with_no_owner_missing_dates_still_runs_failover_coordination(
        self,
        mock_parse_args,
        mock_detect_missing_report_dates,
        mock_invoke_parser_for_pvz,
        mock_run_upload_batch_microservice,
        mock_format_reports_run_notification_message,
        mock_send_notification_microservice,
        mock_sync_owner_failover_state_from_batch_result,
        mock_run_failover_coordination_pass,
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
            enable_failover_coordination=True,
        )
        mock_detect_missing_report_dates.return_value = {"success": True, "missing_dates": []}
        mock_format_reports_run_notification_message.return_value = "ok"
        mock_run_failover_coordination_pass.return_value = {"attempted": False}

        reports_processor.main()

        mock_invoke_parser_for_pvz.assert_not_called()
        mock_run_upload_batch_microservice.assert_not_called()
        mock_sync_owner_failover_state_from_batch_result.assert_not_called()
        mock_run_failover_coordination_pass.assert_called_once()
        mock_send_notification_microservice.assert_called_once()

    @patch("scheduler_runner.tasks.reports.reports_processor.run_failover_coordination_pass")
    @patch("scheduler_runner.tasks.reports.reports_processor.send_notification_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.detect_missing_report_dates")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_backfill_with_no_owner_missing_dates_and_disabled_failover_returns_early(
        self,
        mock_parse_args,
        mock_detect_missing_report_dates,
        mock_send_notification_microservice,
        mock_run_failover_coordination_pass,
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
            enable_failover_coordination=False,
        )
        mock_detect_missing_report_dates.return_value = {"success": True, "missing_dates": []}

        reports_processor.main()

        mock_run_failover_coordination_pass.assert_not_called()
        mock_send_notification_microservice.assert_not_called()

    @patch("scheduler_runner.tasks.reports.reports_processor.send_notification_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.format_reports_run_notification_message")
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
        mock_format_reports_run_notification_message,
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
        mock_format_reports_run_notification_message.return_value = "ok"

        reports_processor.main()

        self.assertEqual(mock_detect_missing_report_dates.call_args.kwargs["pvz_id"], "PVZ1")
        self.assertEqual(
            [(job.pvz_id, job.execution_date) for job in mock_invoke_parser_for_pvz.call_args.kwargs["jobs"]],
            [("PVZ1", "2026-03-01")],
        )
        mock_send_notification_microservice.assert_called_once()

    @patch("scheduler_runner.tasks.reports.reports_processor.run_failover_coordination_pass")
    @patch("scheduler_runner.tasks.reports.reports_processor.send_notification_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.format_reports_run_notification_message")
    @patch("scheduler_runner.tasks.reports.reports_processor.build_reports_run_summary")
    @patch("scheduler_runner.tasks.reports.reports_processor.build_aggregated_backfill_summary")
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
        mock_build_aggregated_backfill_summary,
        mock_build_reports_run_summary,
        mock_format_reports_run_notification_message,
        mock_send_notification_microservice,
        mock_run_failover_coordination_pass,
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
            enable_failover_coordination=True,
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
        mock_build_reports_run_summary.return_value = Mock()
        mock_format_reports_run_notification_message.return_value = "ok"

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
        mock_run_failover_coordination_pass.assert_not_called()
        mock_build_aggregated_backfill_summary.assert_called_once()
        mock_build_reports_run_summary.assert_called_once()
        mock_format_reports_run_notification_message.assert_called_once_with(
            mock_build_reports_run_summary.return_value
        )
        mock_send_notification_microservice.assert_called_once()

    @patch("scheduler_runner.tasks.reports.reports_processor.send_notification_microservice")
    @patch("scheduler_runner.tasks.reports.reports_processor.format_reports_run_notification_message")
    @patch("scheduler_runner.tasks.reports.reports_processor.build_reports_run_summary")
    @patch("scheduler_runner.tasks.reports.reports_processor.build_aggregated_backfill_summary")
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
        mock_build_aggregated_backfill_summary,
        mock_build_reports_run_summary,
        mock_format_reports_run_notification_message,
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
        mock_build_reports_run_summary.return_value = Mock()
        mock_format_reports_run_notification_message.return_value = "ok"

        reports_processor.main()

        mock_detect_missing_report_dates_by_pvz.assert_not_called()
        self.assertEqual(mock_detect_missing_report_dates.call_args.kwargs["pvz_id"], "PVZ1")
        self.assertEqual(
            [(job.pvz_id, job.execution_date) for job in mock_invoke_parser_for_pvz.call_args.kwargs["jobs"]],
            [("PVZ1", "2026-03-01")],
        )


if __name__ == "__main__":
    unittest.main()
