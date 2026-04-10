"""
Unit tests for failover_orchestration (Phase 3).

Проверяют:
- should_scan_failover_candidates (mocked policy)
- should_scan_failover_candidates_legacy
- should_scan_failover_candidates_capability_ranked
- collect_failover_scan_decisions
- normalize_claimable_failover_evaluation
- claim_failover_rows (mocked)
"""
import unittest
from unittest.mock import patch, MagicMock


class TestShouldScanFailoverCandidatesLegacy(unittest.TestCase):
    @patch("scheduler_runner.tasks.reports.failover_orchestration.has_explicit_priority_rule")
    @patch("scheduler_runner.tasks.reports.failover_orchestration.get_priority_list")
    @patch("scheduler_runner.tasks.reports.failover_orchestration.get_selection_mode")
    def test_no_explicit_rule_scans(self, mock_mode, mock_priority, mock_has_rule):
        mock_mode.return_value = "priority_map_legacy"
        mock_has_rule.return_value = False
        from scheduler_runner.tasks.reports.failover_orchestration import should_scan_failover_candidates_legacy

        result = should_scan_failover_candidates_legacy(
            configured_pvz_id="PVZ1",
            accessible_pvz_ids=["PVZ2"],
        )
        self.assertTrue(result["should_scan"])
        self.assertEqual(result["reason"], "no_explicit_rule")

    @patch("scheduler_runner.tasks.reports.failover_orchestration.has_explicit_priority_rule")
    @patch("scheduler_runner.tasks.reports.failover_orchestration.get_priority_list")
    @patch("scheduler_runner.tasks.reports.failover_orchestration.get_selection_mode")
    def test_empty_priority_list_skips(self, mock_mode, mock_priority, mock_has_rule):
        mock_mode.return_value = "priority_map_legacy"
        mock_has_rule.return_value = True
        mock_priority.return_value = []
        from scheduler_runner.tasks.reports.failover_orchestration import should_scan_failover_candidates_legacy

        result = should_scan_failover_candidates_legacy(
            configured_pvz_id="PVZ1",
            accessible_pvz_ids=["PVZ2"],
        )
        self.assertFalse(result["should_scan"])
        self.assertEqual(result["reason"], "empty_priority_list")

    @patch("scheduler_runner.tasks.reports.failover_orchestration.has_explicit_priority_rule")
    @patch("scheduler_runner.tasks.reports.failover_orchestration.get_priority_list")
    @patch("scheduler_runner.tasks.reports.failover_orchestration.get_selection_mode")
    def test_accessible_priority_candidates_scan(self, mock_mode, mock_priority, mock_has_rule):
        mock_mode.return_value = "priority_map_legacy"
        mock_has_rule.return_value = True
        mock_priority.return_value = ["PVZ2"]
        from scheduler_runner.tasks.reports.failover_orchestration import should_scan_failover_candidates_legacy

        result = should_scan_failover_candidates_legacy(
            configured_pvz_id="PVZ1",
            accessible_pvz_ids=["PVZ2"],
        )
        self.assertTrue(result["should_scan"])
        self.assertEqual(result["reason"], "accessible_priority_candidates")

    @patch("scheduler_runner.tasks.reports.failover_orchestration.has_explicit_priority_rule")
    @patch("scheduler_runner.tasks.reports.failover_orchestration.get_priority_list")
    @patch("scheduler_runner.tasks.reports.failover_orchestration.get_selection_mode")
    def test_priority_not_accessible_skips(self, mock_mode, mock_priority, mock_has_rule):
        mock_mode.return_value = "priority_map_legacy"
        mock_has_rule.return_value = True
        mock_priority.return_value = ["PVZ3"]
        from scheduler_runner.tasks.reports.failover_orchestration import should_scan_failover_candidates_legacy

        result = should_scan_failover_candidates_legacy(
            configured_pvz_id="PVZ1",
            accessible_pvz_ids=["PVZ2"],
        )
        self.assertFalse(result["should_scan"])
        self.assertEqual(result["reason"], "priority_candidates_not_accessible")


class TestShouldScanCapabilityRanked(unittest.TestCase):
    @patch("scheduler_runner.tasks.reports.failover_orchestration.get_capability_targets_for_helper")
    def test_empty_capability_list_skips(self, mock_targets):
        mock_targets.return_value = []
        from scheduler_runner.tasks.reports.failover_orchestration import should_scan_failover_candidates_capability_ranked

        result = should_scan_failover_candidates_capability_ranked(
            configured_pvz_id="PVZ1",
            accessible_pvz_ids=["PVZ2"],
        )
        self.assertFalse(result["should_scan"])
        self.assertEqual(result["reason"], "empty_capability_list")

    @patch("scheduler_runner.tasks.reports.failover_orchestration.get_capability_targets_for_helper")
    def test_accessible_targets_scan(self, mock_targets):
        mock_targets.return_value = ["PVZ2"]
        from scheduler_runner.tasks.reports.failover_orchestration import should_scan_failover_candidates_capability_ranked

        result = should_scan_failover_candidates_capability_ranked(
            configured_pvz_id="PVZ1",
            accessible_pvz_ids=["PVZ2"],
        )
        self.assertTrue(result["should_scan"])
        self.assertEqual(result["reason"], "accessible_capability_targets")

    @patch("scheduler_runner.tasks.reports.failover_orchestration.get_capability_targets_for_helper")
    def test_targets_not_accessible_skips(self, mock_targets):
        mock_targets.return_value = ["PVZ3"]
        from scheduler_runner.tasks.reports.failover_orchestration import should_scan_failover_candidates_capability_ranked

        result = should_scan_failover_candidates_capability_ranked(
            configured_pvz_id="PVZ1",
            accessible_pvz_ids=["PVZ2"],
        )
        self.assertFalse(result["should_scan"])
        self.assertEqual(result["reason"], "capability_targets_not_accessible")


class TestNormalizeClaimableFailoverEvaluation(unittest.TestCase):
    def test_none_returns_default(self):
        from scheduler_runner.tasks.reports.failover_orchestration import normalize_claimable_failover_evaluation

        result = normalize_claimable_failover_evaluation(None)
        self.assertEqual(result["mode"], "unknown")
        self.assertEqual(result["total_candidates"], 0)
        self.assertEqual(result["selected_rows"], [])

    def test_valid_evaluation_passes_through(self):
        from scheduler_runner.tasks.reports.failover_orchestration import normalize_claimable_failover_evaluation

        result = normalize_claimable_failover_evaluation({
            "mode": "capability_ranked",
            "total_candidates": 10,
            "eligible_count": 5,
            "selected_count": 2,
            "rejected_count": 3,
            "rejected_reasons": {"not_accessible": 2, "own_target_pvz": 1},
            "selected_rows": [{"row1": "data"}],
            "decisions": [],
        })
        self.assertEqual(result["mode"], "capability_ranked")
        self.assertEqual(result["total_candidates"], 10)
        self.assertEqual(result["selected_rows"], [{"row1": "data"}])


class TestCollectFailoverScanDecisions(unittest.TestCase):
    @patch("scheduler_runner.tasks.reports.failover_orchestration.get_selection_mode")
    @patch("scheduler_runner.tasks.reports.failover_orchestration.should_scan_failover_candidates")
    def test_returns_active_and_mode(self, mock_scan, mock_mode):
        mock_mode.return_value = "priority_map_legacy"
        mock_scan.return_value = {"should_scan": True, "reason": "no_explicit_rule"}

        # Override dry_run config
        with patch("scheduler_runner.tasks.reports.failover_orchestration.FAILOVER_POLICY_CONFIG", {"enabled": True}):
            from scheduler_runner.tasks.reports.failover_orchestration import collect_failover_scan_decisions

            result = collect_failover_scan_decisions(
                configured_pvz_id="PVZ1",
                accessible_pvz_ids=["PVZ2"],
            )
            self.assertIn("active", result)
            self.assertTrue(result["active"]["should_scan"])


@patch("scheduler_runner.tasks.reports.failover_orchestration.try_claim_failover")
class TestClaimFailoverRows(unittest.TestCase):
    def test_claims_successful_rows(self, mock_claim):
        mock_claim.return_value = {"claimed": True}
        from scheduler_runner.tasks.reports.failover_orchestration import claim_failover_rows

        claimed = claim_failover_rows(
            candidate_rows=[
                {"Дата": "2026-04-01", "target_pvz": "PVZ2", "owner_pvz": "PVZ2"},
                {"Дата": "2026-04-02", "target_pvz": "PVZ3", "owner_pvz": "PVZ3"},
            ],
            claimer_pvz="PVZ1",
            ttl_minutes=15,
            source_run_id="test",
        )
        self.assertEqual(len(claimed), 2)
        self.assertEqual(mock_claim.call_count, 2)

    def test_skips_unclaimed_rows(self, mock_claim):
        mock_claim.side_effect = [{"claimed": True}, {"claimed": False}]
        from scheduler_runner.tasks.reports.failover_orchestration import claim_failover_rows

        claimed = claim_failover_rows(
            candidate_rows=[
                {"Дата": "2026-04-01", "target_pvz": "PVZ2", "owner_pvz": "PVZ2"},
                {"Дата": "2026-04-02", "target_pvz": "PVZ3", "owner_pvz": "PVZ3"},
            ],
            claimer_pvz="PVZ1",
            ttl_minutes=15,
            source_run_id="test",
        )
        self.assertEqual(len(claimed), 1)


if __name__ == "__main__":
    unittest.main()

