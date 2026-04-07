import unittest
from unittest.mock import Mock, patch

from scheduler_runner.tasks.reports import failover_state


class TestFailoverState(unittest.TestCase):
    def test_build_failover_state_record_formats_primary_fields(self):
        record = failover_state.build_failover_state_record(
            execution_date="2026-03-14",
            target_pvz="ЧЕБОКСАРЫ_144",
            owner_pvz="ЧЕБОКСАРЫ_144",
            status=failover_state.STATUS_OWNER_PENDING,
        )

        self.assertEqual(record["Дата"], "2026-03-14")
        self.assertEqual(record["target_pvz"], "ЧЕБОКСАРЫ_144")
        self.assertEqual(record["owner_pvz"], "ЧЕБОКСАРЫ_144")
        self.assertEqual(record["request_id"], "2026-03-14|ЧЕБОКСАРЫ_144")
        self.assertEqual(record["status"], failover_state.STATUS_OWNER_PENDING)

    def test_is_claim_active_true_only_for_unexpired_claim(self):
        state_row = {
            "status": failover_state.STATUS_FAILOVER_CLAIMED,
            "claim_expires_at": "14.03.2026 12:30:00",
        }
        now = failover_state.datetime.strptime("14.03.2026 12:00:00", "%d.%m.%Y %H:%M:%S")

        self.assertTrue(failover_state.is_claim_active(state_row, now=now))

    def test_is_claim_active_false_for_expired_claim(self):
        state_row = {
            "status": failover_state.STATUS_FAILOVER_CLAIMED,
            "claim_expires_at": "14.03.2026 11:30:00",
        }
        now = failover_state.datetime.strptime("14.03.2026 12:00:00", "%d.%m.%Y %H:%M:%S")

        self.assertFalse(failover_state.is_claim_active(state_row, now=now))

    @patch("scheduler_runner.tasks.reports.failover_state.get_failover_claim_backend", return_value="sheets")
    @patch("scheduler_runner.tasks.reports.failover_state.upsert_failover_state")
    @patch("scheduler_runner.tasks.reports.failover_state.get_failover_state")
    def test_try_claim_failover_claims_when_no_active_claim(self, mock_get_failover_state, mock_upsert_failover_state, _mock_backend):
        mock_get_failover_state.side_effect = [
            {
                "status": failover_state.STATUS_OWNER_FAILED,
                "attempt_no": 1,
                "last_error": "boom",
            },
            {
                "status": failover_state.STATUS_FAILOVER_CLAIMED,
                "claimed_by": "ЧЕБОКСАРЫ_144",
                "source_run_id": "run-1",
                "attempt_no": 2,
            },
        ]
        mock_upsert_failover_state.return_value = {"success": True}

        result = failover_state.try_claim_failover(
            execution_date="2026-03-14",
            target_pvz="ЧЕБОКСАРЫ_143",
            owner_pvz="ЧЕБОКСАРЫ_143",
            claimer_pvz="ЧЕБОКСАРЫ_144",
            ttl_minutes=10,
            source_run_id="run-1",
            logger=Mock(),
        )

        self.assertTrue(result["success"])
        self.assertTrue(result["claimed"])
        self.assertEqual(result["state"]["status"], failover_state.STATUS_FAILOVER_CLAIMED)
        self.assertEqual(result["state"]["claimed_by"], "ЧЕБОКСАРЫ_144")
        self.assertEqual(result["state"]["attempt_no"], 2)

    @patch("scheduler_runner.tasks.reports.failover_state.get_failover_claim_backend", return_value="sheets")
    @patch("scheduler_runner.tasks.reports.failover_state.get_failover_state")
    def test_try_claim_failover_returns_already_claimed_when_claim_is_active(self, mock_get_failover_state, _mock_backend):
        mock_get_failover_state.return_value = {
            "status": failover_state.STATUS_FAILOVER_CLAIMED,
            "claim_expires_at": "14.03.2099 12:30:00",
            "claimed_by": "ЧЕБОКСАРЫ_182",
        }

        result = failover_state.try_claim_failover(
            execution_date="2026-03-14",
            target_pvz="ЧЕБОКСАРЫ_143",
            owner_pvz="ЧЕБОКСАРЫ_143",
            claimer_pvz="ЧЕБОКСАРЫ_144",
            logger=Mock(),
        )

        self.assertTrue(result["success"])
        self.assertFalse(result["claimed"])
        self.assertEqual(result["reason"], "already_claimed")

    @patch("scheduler_runner.tasks.reports.failover_state.get_failover_claim_backend", return_value="sheets")
    @patch("scheduler_runner.tasks.reports.failover_state.upsert_failover_state")
    @patch("scheduler_runner.tasks.reports.failover_state.get_failover_state")
    def test_try_claim_failover_returns_verification_failed_on_ownership_mismatch(self, mock_get_failover_state, mock_upsert_failover_state, _mock_backend):
        mock_get_failover_state.side_effect = [
            {
                "status": failover_state.STATUS_OWNER_FAILED,
                "attempt_no": 0,
                "last_error": "",
            },
            {
                "status": failover_state.STATUS_FAILOVER_CLAIMED,
                "claimed_by": "Р§Р•Р‘РћРљРЎРђР Р«_182",
                "source_run_id": "other-run",
            },
        ]
        mock_upsert_failover_state.return_value = {"success": True}

        result = failover_state.try_claim_failover(
            execution_date="2026-03-14",
            target_pvz="Р§Р•Р‘РћРљРЎРђР Р«_143",
            owner_pvz="Р§Р•Р‘РћРљРЎРђР Р«_143",
            claimer_pvz="Р§Р•Р‘РћРљРЎРђР Р«_144",
            source_run_id="run-1",
            logger=Mock(),
        )

        self.assertTrue(result["success"])
        self.assertFalse(result["claimed"])
        self.assertEqual(result["reason"], "claim_verification_failed")

    @patch("scheduler_runner.tasks.reports.failover_state.get_failover_claim_backend", return_value="sheets")
    @patch("scheduler_runner.tasks.reports.failover_state.get_failover_state")
    def test_try_claim_failover_returns_already_completed_for_terminal_state(self, mock_get_failover_state, _mock_backend):
        mock_get_failover_state.return_value = {
            "status": failover_state.STATUS_FAILOVER_SUCCESS,
            "claimed_by": "ЧЕБОКСАРЫ_182",
        }

        result = failover_state.try_claim_failover(
            execution_date="2026-03-14",
            target_pvz="ЧЕБОКСАРЫ_143",
            owner_pvz="ЧЕБОКСАРЫ_143",
            claimer_pvz="ЧЕБОКСАРЫ_144",
            logger=Mock(),
        )

        self.assertTrue(result["success"])
        self.assertFalse(result["claimed"])
        self.assertEqual(result["reason"], "already_completed")

    @patch("scheduler_runner.tasks.reports.failover_state.failover_state_connection")
    def test_get_failover_state_reads_row_by_unique_keys(self, mock_connection_factory):
        uploader = Mock()
        uploader.table_config = Mock()
        uploader.sheets_reporter.get_row_by_unique_keys.return_value = {"status": failover_state.STATUS_OWNER_PENDING}
        connection = mock_connection_factory.return_value
        connection.__enter__.return_value = uploader
        connection.__exit__.return_value = False

        result = failover_state.get_failover_state(
            execution_date="2026-03-14",
            target_pvz="ЧЕБОКСАРЫ_144",
            logger=Mock(),
        )

        self.assertEqual(result["status"], failover_state.STATUS_OWNER_PENDING)
        uploader.sheets_reporter.get_row_by_unique_keys.assert_called_once()

    @patch("scheduler_runner.tasks.reports.failover_state.failover_state_connection")
    def test_upsert_failover_state_records_reuses_single_connection(self, mock_connection_factory):
        uploader = Mock()
        uploader.table_config.get_column_index.side_effect = lambda name: {"Дата": 2, "target_pvz": 3}.get(name)
        uploader.table_config.get_column_letter.side_effect = lambda name: {"Дата": "B", "target_pvz": "C"}.get(name)
        uploader.table_config.get_column.side_effect = lambda name: None
        uploader.sheets_reporter.get_last_row_with_data.return_value = 2
        uploader.sheets_reporter._prepare_value_for_search.side_effect = lambda value: value
        uploader.sheets_reporter._normalize_for_comparison.side_effect = lambda value: value
        uploader.sheets_reporter.worksheet.batch_get.return_value = [
            [["2026-03-14"]],
            [["PVZ1"]],
        ]
        uploader.sheets_reporter.worksheet.row_values.return_value = [
            "request_id",
            "Дата",
            "target_pvz",
            "owner_pvz",
            "status",
            "updated_at",
        ]
        uploader.sheets_reporter._update_existing_row.return_value = {"success": True, "action": "updated", "row_number": 2}
        uploader.sheets_reporter._create_result.side_effect = lambda **kwargs: kwargs
        connection = mock_connection_factory.return_value
        connection.__enter__.return_value = uploader
        connection.__exit__.return_value = False

        result = failover_state.upsert_failover_state_records(
            [
                {"Дата": "2026-03-14", "target_pvz": "PVZ1", "status": failover_state.STATUS_OWNER_SUCCESS},
                {"Дата": "2026-03-15", "target_pvz": "PVZ2", "status": failover_state.STATUS_OWNER_FAILED},
            ],
            logger=Mock(),
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["updated"], 2)
        self.assertEqual(len(result["results"]), 2)
        mock_connection_factory.assert_called_once()
        uploader.sheets_reporter.worksheet.batch_get.assert_called_once()
        uploader.sheets_reporter._update_existing_row.assert_called_once()
        uploader.sheets_reporter.worksheet.append_rows.assert_called_once()
        uploader._perform_upload.assert_not_called()

    @patch("scheduler_runner.tasks.reports.failover_state.failover_state_connection")
    def test_upsert_failover_state_records_batches_multiple_appends_into_single_request(self, mock_connection_factory):
        uploader = Mock()
        uploader.table_config.get_column_index.side_effect = lambda name: {"Дата": 2, "target_pvz": 3}.get(name)
        uploader.table_config.get_column_letter.side_effect = lambda name: {"Дата": "B", "target_pvz": "C"}.get(name)
        uploader.table_config.get_column.side_effect = lambda name: None
        uploader.sheets_reporter.get_last_row_with_data.return_value = 5
        uploader.sheets_reporter._prepare_value_for_search.side_effect = lambda value: value
        uploader.sheets_reporter._normalize_for_comparison.side_effect = lambda value: value
        uploader.sheets_reporter.worksheet.batch_get.return_value = [
            [["01.04.2026"]],
            [["PVZ0"]],
        ]
        uploader.sheets_reporter.worksheet.row_values.return_value = [
            "request_id",
            "Дата",
            "target_pvz",
            "owner_pvz",
            "status",
            "updated_at",
        ]
        uploader.sheets_reporter._create_result.side_effect = lambda **kwargs: kwargs
        connection = mock_connection_factory.return_value
        connection.__enter__.return_value = uploader
        connection.__exit__.return_value = False

        result = failover_state.upsert_failover_state_records(
            [
                {"Дата": "2026-03-14", "target_pvz": "PVZ1", "owner_pvz": "PVZ1", "status": failover_state.STATUS_OWNER_SUCCESS, "updated_at": "14.03.2026 21:30:00"},
                {"Дата": "2026-03-15", "target_pvz": "PVZ2", "owner_pvz": "PVZ2", "status": failover_state.STATUS_OWNER_FAILED, "updated_at": "15.03.2026 21:30:00"},
            ],
            logger=Mock(),
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["updated"], 2)
        uploader.sheets_reporter.worksheet.append_rows.assert_called_once()
        appended_values = uploader.sheets_reporter.worksheet.append_rows.call_args.kwargs["values"]
        self.assertEqual(len(appended_values), 2)
        self.assertEqual(result["results"][0]["row_number"], 6)
        self.assertEqual(result["results"][1]["row_number"], 7)
        uploader.sheets_reporter._update_existing_row.assert_not_called()
        uploader._perform_upload.assert_not_called()

    @patch("scheduler_runner.tasks.reports.failover_state.urllib.request.urlopen")
    def test_try_claim_failover_via_apps_script_returns_remote_payload(self, mock_urlopen):
        response = Mock()
        response.read.return_value = (
            b'{"success": true, "claimed": true, "reason": "claimed", "state": {"status": "failover_claimed"}}'
        )
        mock_urlopen.return_value.__enter__.return_value = response

        with patch(
            "scheduler_runner.tasks.reports.failover_state.get_failover_apps_script_config",
            return_value={
                "url": "https://example.test/exec",
                "shared_secret": "secret",
                "timeout_seconds": 15,
            },
        ):
            result = failover_state.try_claim_failover_via_apps_script(
                execution_date="2026-03-14",
                target_pvz="PVZ1",
                owner_pvz="PVZ1",
                claimer_pvz="PVZ2",
                source_run_id="run-1",
                logger=Mock(),
            )

        self.assertTrue(result["success"])
        self.assertTrue(result["claimed"])
        self.assertEqual(result["reason"], "claimed")

    @patch("scheduler_runner.tasks.reports.failover_state.failover_state_connection")
    def test_list_failover_state_rows_filters_by_status_and_target(self, mock_connection_factory):
        uploader = Mock()
        uploader.table_config.column_names = ["request_id", "Дата", "target_pvz", "status"]
        uploader.sheets_reporter.worksheet.get_all_records.return_value = [
            {"request_id": "1", "Дата": "14.03.2026", "target_pvz": "PVZ1", "status": failover_state.STATUS_OWNER_FAILED},
            {"request_id": "2", "Дата": "14.03.2026", "target_pvz": "PVZ2", "status": failover_state.STATUS_OWNER_SUCCESS},
        ]
        connection = mock_connection_factory.return_value
        connection.__enter__.return_value = uploader
        connection.__exit__.return_value = False

        result = failover_state.list_failover_state_rows(
            statuses=[failover_state.STATUS_OWNER_FAILED],
            target_pvz="PVZ1",
            logger=Mock(),
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["target_pvz"], "PVZ1")

    @patch("scheduler_runner.tasks.reports.failover_state.failover_state_connection")
    def test_list_candidate_failover_rows_fast_uses_batch_get_and_filters_statuses(self, mock_connection_factory):
        uploader = Mock()
        uploader.table_config.get_column_index.side_effect = lambda name: {
            "Дата": 2,
            "target_pvz": 3,
            "owner_pvz": 4,
            "status": 5,
            "claimed_by": 6,
            "claim_expires_at": 7,
            "attempt_no": 8,
            "source_run_id": 9,
            "last_error": 10,
            "updated_at": 11,
        }.get(name)
        uploader.table_config.get_column_letter.side_effect = lambda name: {
            "Дата": "B",
            "target_pvz": "C",
            "owner_pvz": "D",
            "status": "E",
            "claimed_by": "F",
            "claim_expires_at": "G",
            "attempt_no": "H",
            "source_run_id": "I",
            "last_error": "J",
            "updated_at": "K",
        }.get(name)
        uploader.sheets_reporter.get_last_row_with_data.return_value = 4
        uploader.sheets_reporter.worksheet.batch_get.return_value = [
            [["2026-03-14"], ["2026-03-15"], ["2026-03-16"]],
            [["PVZ1"], ["PVZ2"], ["PVZ3"]],
            [["PVZ1"], ["PVZ2"], ["PVZ3"]],
            [[failover_state.STATUS_OWNER_FAILED], [failover_state.STATUS_OWNER_SUCCESS], [failover_state.STATUS_FAILOVER_FAILED]],
            [[""], [""], ["PVZ9"]],
            [[""], [""], ["14.03.2099 12:30:00"]],
            [["0"], ["0"], ["1"]],
            [["run-1"], ["run-2"], ["run-3"]],
            [["boom"], [""], ["retry"]],
            [["20.03.2026 21:30:00"], ["20.03.2026 21:31:00"], ["20.03.2026 21:32:00"]],
        ]
        connection = mock_connection_factory.return_value
        connection.__enter__.return_value = uploader
        connection.__exit__.return_value = False

        result = failover_state.list_candidate_failover_rows_fast(
            statuses=[failover_state.STATUS_OWNER_FAILED, failover_state.STATUS_FAILOVER_FAILED],
            logger=Mock(),
        )

        uploader.sheets_reporter.worksheet.batch_get.assert_called_once()
        uploader.sheets_reporter.worksheet.get_all_records.assert_not_called()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["target_pvz"], "PVZ1")
        self.assertEqual(result[1]["target_pvz"], "PVZ3")
        self.assertEqual(result[1]["claimed_by"], "PVZ9")

    def test_list_candidate_failover_rows_fast_reuses_existing_uploader(self):
        uploader = Mock()
        uploader.table_config.get_column_index.side_effect = lambda name: {"Дата": 2}.get(name)
        uploader.table_config.get_column_letter.side_effect = lambda name: {
            "Дата": "B",
            "target_pvz": "C",
            "owner_pvz": "D",
            "status": "E",
            "claimed_by": "F",
            "claim_expires_at": "G",
            "attempt_no": "H",
            "source_run_id": "I",
            "last_error": "J",
            "updated_at": "K",
        }.get(name)
        uploader.sheets_reporter.get_last_row_with_data.return_value = 2
        uploader.sheets_reporter.worksheet.batch_get.return_value = [
            [["2026-03-14"]],
            [["PVZ1"]],
            [["PVZ1"]],
            [[failover_state.STATUS_OWNER_FAILED]],
            [[""]],
            [[""]],
            [["0"]],
            [["run-1"]],
            [["boom"]],
            [["20.03.2026 21:30:00"]],
        ]

        result = failover_state.list_candidate_failover_rows_fast(
            statuses=[failover_state.STATUS_OWNER_FAILED],
            logger=Mock(),
            uploader=uploader,
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["target_pvz"], "PVZ1")

    def test_get_failover_state_reuses_existing_uploader(self):
        uploader = Mock()
        uploader.table_config = Mock()
        uploader.sheets_reporter.get_row_by_unique_keys.return_value = {"status": failover_state.STATUS_OWNER_FAILED}

        result = failover_state.get_failover_state(
            execution_date="2026-03-14",
            target_pvz="PVZ1",
            logger=Mock(),
            uploader=uploader,
        )

        self.assertEqual(result["status"], failover_state.STATUS_OWNER_FAILED)
        uploader.sheets_reporter.get_row_by_unique_keys.assert_called_once()

    def test_upsert_failover_state_reuses_existing_uploader(self):
        uploader = Mock()
        uploader._perform_upload.return_value = {"success": True}

        result = failover_state.upsert_failover_state(
            {"Дата": "2026-03-14", "target_pvz": "PVZ1", "status": failover_state.STATUS_OWNER_SUCCESS},
            logger=Mock(),
            uploader=uploader,
        )

        self.assertTrue(result["success"])
        uploader._perform_upload.assert_called_once()
