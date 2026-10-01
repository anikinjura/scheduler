"""
Этап C failover: молчащие ПВЗ (не запускались — нет ни данных в KPI, ни строки KPI_FAILOVER_STATE).

Проверяют:
- выбор целей помощника по priority_map и capability_map
- поиск дат: окно до вчерашнего дня, исключение известных строк, задержка для ранга > 1
- захват с create_if_missing и предупреждение при старом Apps Script
- сбор кандидатов: статусы фильтруются локально, молчащие даты добавляются по флагу
- payload Apps Script
"""
import json
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from .. import failover_orchestration
from ..storage import google_sheets_store

NOW = datetime(2026, 10, 1, 21, 45, 0)
PRIORITY_MAP = {
    "ЧЕБОКСАРЫ_143": ["ЧЕБОКСАРЫ_144"],
    "ЧЕБОКСАРЫ_182": ["ЧЕБОКСАРЫ_144"],
    "ЧЕБОКСАРЫ_144": ["ЧЕБОКСАРЫ_182", "ЧЕБОКСАРЫ_143"],
    "ЧЕБОКСАРЫ_340": [],
}
ALL_PVZ = ["ЧЕБОКСАРЫ_143", "ЧЕБОКСАРЫ_144", "ЧЕБОКСАРЫ_182"]


def policy(**overrides):
    config = {"selection_mode": "priority_map_legacy", "priority_map": PRIORITY_MAP, "candidate_window_days": 7}
    config.update(overrides)
    return patch.dict(failover_orchestration.FAILOVER_POLICY_CONFIG, config)


class TestResolveSilentOwnerTargets(unittest.TestCase):
    def test_helper_144_watches_accessible_targets_rank_1(self):
        with policy():
            targets = failover_orchestration.resolve_silent_owner_targets(
                configured_pvz_id="ЧЕБОКСАРЫ_144", available_pvz_ids=["ЧЕБОКСАРЫ_143", "ЧЕБОКСАРЫ_144"])
        self.assertEqual(targets, [("ЧЕБОКСАРЫ_143", 1)])

    def test_second_helper_gets_rank_2(self):
        with policy():
            targets = failover_orchestration.resolve_silent_owner_targets(
                configured_pvz_id="ЧЕБОКСАРЫ_143", available_pvz_ids=ALL_PVZ)
        self.assertEqual(targets, [("ЧЕБОКСАРЫ_144", 2)])

    def test_capability_ranked_only_preferred_helper(self):
        with policy(selection_mode="capability_ranked", helper_bias={},
                    capability_map={"ЧЕБОКСАРЫ_144": ["ЧЕБОКСАРЫ_143"], "ЧЕБОКСАРЫ_182": ["ЧЕБОКСАРЫ_143"]}):
            preferred = failover_orchestration.resolve_silent_owner_targets(
                configured_pvz_id="ЧЕБОКСАРЫ_144", available_pvz_ids=ALL_PVZ)
            other = failover_orchestration.resolve_silent_owner_targets(
                configured_pvz_id="ЧЕБОКСАРЫ_182", available_pvz_ids=ALL_PVZ)
        # лексический порядок после нормализации: ..._144 < ..._182
        self.assertEqual(preferred, [("ЧЕБОКСАРЫ_143", 1)])
        self.assertEqual(other, [])


@patch.object(failover_orchestration, "create_uploader_logger", return_value=MagicMock())
@patch.object(failover_orchestration, "detect_missing_report_dates_by_pvz")
class TestFindSilentOwnerRows(unittest.TestCase):
    def find(self, helper="ЧЕБОКСАРЫ_144", state_rows=(), **policy_overrides):
        with policy(**policy_overrides), patch.dict(failover_orchestration.BACKFILL_CONFIG,
                                                    {"failover_silent_rank_lag_days": 1}):
            return failover_orchestration.find_silent_owner_rows(
                configured_pvz_id=helper, available_pvz_ids=ALL_PVZ, state_rows=list(state_rows),
                logger=MagicMock(), now=NOW)

    def test_window_ends_yesterday_and_known_rows_excluded(self, mock_coverage, _logger):
        mock_coverage.return_value = {"success": True, "missing_dates_by_pvz": {
            "ЧЕБОКСАРЫ_143": ["2026-09-28", "2026-09-30"], "ЧЕБОКСАРЫ_182": ["2026-09-29"]}}

        rows = self.find(state_rows=[{"work_date": "30.09.2026", "target_object_name": "ЧЕБОКСАРЫ_143",
                                      "status": "owner_failed"}])

        kwargs = mock_coverage.call_args.kwargs
        self.assertEqual((kwargs["date_from"], kwargs["date_to"]), ("2026-09-25", "2026-09-30"))
        self.assertEqual(sorted(kwargs["pvz_ids"]), ["ЧЕБОКСАРЫ_143", "ЧЕБОКСАРЫ_182"])
        self.assertEqual([(r["target_object_name"], r["work_date"]) for r in rows],
                         [("ЧЕБОКСАРЫ_143", "2026-09-28"), ("ЧЕБОКСАРЫ_182", "2026-09-29")])
        self.assertTrue(all(r["silent"] and r["status"] == "owner_silent" for r in rows))

    def test_rank_2_waits_one_more_day(self, mock_coverage, _logger):
        mock_coverage.return_value = {"success": True, "missing_dates_by_pvz": {
            "ЧЕБОКСАРЫ_144": ["2026-09-29", "2026-09-30"]}}

        rows = self.find(helper="ЧЕБОКСАРЫ_143")

        self.assertEqual([r["work_date"] for r in rows], ["2026-09-29"])

    def test_coverage_failure_raises(self, mock_coverage, _logger):
        mock_coverage.return_value = {"success": False, "error": "quota"}
        with self.assertRaises(RuntimeError):
            self.find()

    def test_no_targets_no_coverage_check(self, mock_coverage, _logger):
        self.assertEqual(self.find(helper="ЧЕБОКСАРЫ_340"), [])
        mock_coverage.assert_not_called()


@patch.object(failover_orchestration, "try_claim_failover")
class TestClaimSilentRows(unittest.TestCase):
    def test_silent_row_claimed_with_create_if_missing(self, mock_claim):
        mock_claim.return_value = {"claimed": True, "reason": "created_and_claimed"}

        claimed = failover_orchestration.claim_failover_rows(
            candidate_rows=[{"work_date": "2026-09-28", "target_object_name": "ЧЕБОКСАРЫ_143", "silent": True},
                            {"work_date": "2026-09-29", "target_object_name": "ЧЕБОКСАРЫ_182"}],
            claimer_pvz="ЧЕБОКСАРЫ_144", ttl_minutes=30, logger=MagicMock())

        self.assertEqual(len(claimed), 2)
        self.assertTrue(mock_claim.call_args_list[0].kwargs["create_if_missing"])
        self.assertNotIn("create_if_missing", mock_claim.call_args_list[1].kwargs)

    def test_old_apps_script_warns(self, mock_claim):
        mock_claim.return_value = {"success": False, "claimed": False, "reason": "row_not_found"}
        logger = MagicMock()

        claimed = failover_orchestration.claim_failover_rows(
            candidate_rows=[{"work_date": "2026-09-28", "target_object_name": "ЧЕБОКСАРЫ_143", "silent": True}],
            claimer_pvz="ЧЕБОКСАРЫ_144", ttl_minutes=30, logger=logger)

        self.assertEqual(claimed, [])
        self.assertIn("FAILOVER_SILENT_CLAIM_UNSUPPORTED", logger.warning.call_args.args[0])


@patch.object(failover_orchestration, "find_silent_owner_rows")
@patch.object(failover_orchestration, "list_candidate_failover_rows_fast")
class TestCollectWithSilentOwners(unittest.TestCase):
    STATE_ROWS = [
        {"work_date": "2026-09-30", "target_object_name": "ЧЕБОКСАРЫ_143", "status": "owner_failed", "attempt_no": 0},
        {"work_date": "2026-09-30", "target_object_name": "ЧЕБОКСАРЫ_182", "status": "owner_success"},
    ]
    SILENT = [{"work_date": "2026-09-28", "target_object_name": "ЧЕБОКСАРЫ_182", "owner_object_name": "ЧЕБОКСАРЫ_182",
               "status": "owner_silent", "attempt_no": 0, "updated_at": "", "silent": True}]

    def collect(self, flag):
        with policy(enabled=True), patch.dict(failover_orchestration.BACKFILL_CONFIG,
                                              {"failover_detect_silent_owners": flag}),                 patch.object(failover_orchestration, "datetime") as mock_dt:
            mock_dt.now.return_value = NOW
            return failover_orchestration.collect_claimable_failover_rows(
                available_pvz_ids=ALL_PVZ, configured_pvz_id="ЧЕБОКСАРЫ_144", max_claims=3,
                logger=MagicMock(), uploader=MagicMock(), return_evaluation=True)

    def test_flag_off_only_state_candidates(self, mock_list, mock_silent):
        mock_list.return_value = self.STATE_ROWS
        evaluation = self.collect(False)
        self.assertIsNone(mock_list.call_args.kwargs["statuses"])
        mock_silent.assert_not_called()
        self.assertEqual([r["target_object_name"] for r in evaluation["selected_rows"]], ["ЧЕБОКСАРЫ_143"])

    def test_flag_on_adds_silent_dates(self, mock_list, mock_silent):
        mock_list.return_value = self.STATE_ROWS
        mock_silent.return_value = self.SILENT
        evaluation = self.collect(True)
        self.assertEqual(mock_silent.call_args.kwargs["state_rows"], self.STATE_ROWS)
        self.assertEqual([(r["target_object_name"], r["work_date"]) for r in evaluation["selected_rows"]],
                         [("ЧЕБОКСАРЫ_182", "2026-09-28"), ("ЧЕБОКСАРЫ_143", "2026-09-30")])

    def test_silent_scan_failure_keeps_state_candidates(self, mock_list, mock_silent):
        mock_list.return_value = self.STATE_ROWS
        mock_silent.side_effect = RuntimeError("quota")
        evaluation = self.collect(True)
        self.assertEqual(len(evaluation["selected_rows"]), 1)


class TestAppsScriptPayload(unittest.TestCase):
    def claim(self, **kwargs):
        response = MagicMock()
        response.read.return_value = json.dumps({"success": True, "claimed": True}).encode()
        response.__enter__.return_value = response
        with patch.object(google_sheets_store, "get_failover_apps_script_config",
                          return_value={"url": "https://example.invalid/exec", "shared_secret": "s"}), \
                patch.object(google_sheets_store.urllib.request, "urlopen", return_value=response) as mock_open:
            google_sheets_store.try_claim_failover_via_apps_script(
                execution_date="2026-09-28", target_object_name="ЧЕБОКСАРЫ_143", owner_object_name="ЧЕБОКСАРЫ_143",
                claimer_pvz="ЧЕБОКСАРЫ_144", ttl_minutes=30, logger=MagicMock(), **kwargs)
        return json.loads(mock_open.call_args.args[0].data.decode())

    def test_create_if_missing_sent_only_when_requested(self):
        self.assertTrue(self.claim(create_if_missing=True)["create_if_missing"])
        self.assertNotIn("create_if_missing", self.claim())


if __name__ == "__main__":
    unittest.main()
