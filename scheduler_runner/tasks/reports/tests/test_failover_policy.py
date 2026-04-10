import unittest
from datetime import datetime
from unittest.mock import patch

from scheduler_runner.tasks.reports import failover_policy


class TestFailoverPolicy(unittest.TestCase):
    def test_priority_map_contains_expected_pilot_links(self):
        self.assertEqual(failover_policy.get_priority_list("ЧЕБОКСАРЫ_143"), ["ЧЕБОКСАРЫ_144"])
        self.assertEqual(failover_policy.get_priority_list("ЧЕБОКСАРЫ_182"), ["ЧЕБОКСАРЫ_144"])
        self.assertEqual(failover_policy.get_priority_list("СОСНОВКА_10"), ["ЧЕБОКСАРЫ_144"])
        self.assertEqual(failover_policy.get_priority_list("ЧЕБОКСАРЫ_340"), [])

    def test_pilot_map_allows_cheboksary_144_to_claim_cheboksary_143(self):
        result = failover_policy.can_attempt_failover_claim(
            state_row={
                "Дата": "2026-03-14",
                "target_pvz": "ЧЕБОКСАРЫ_143",
                "status": "owner_failed",
                "attempt_no": 0,
                "updated_at": "14.03.2026 10:00:00",
            },
            configured_pvz_id="ЧЕБОКСАРЫ_144",
            available_pvz=["ЧЕБОКСАРЫ_143", "ЧЕБОКСАРЫ_144"],
            now=datetime.strptime("14.03.2026 10:00:00", "%d.%m.%Y %H:%M:%S"),
        )

        self.assertTrue(result["eligible"])
        self.assertEqual(result["rank"], 1)

    def test_pilot_map_keeps_cheboksary_340_isolated(self):
        result = failover_policy.can_attempt_failover_claim(
            state_row={
                "Дата": "2026-03-14",
                "target_pvz": "ЧЕБОКСАРЫ_340",
                "status": "owner_failed",
                "attempt_no": 0,
                "updated_at": "14.03.2026 10:00:00",
            },
            configured_pvz_id="ЧЕБОКСАРЫ_144",
            available_pvz=["ЧЕБОКСАРЫ_340", "ЧЕБОКСАРЫ_144"],
            now=datetime.strptime("14.03.2026 10:00:00", "%d.%m.%Y %H:%M:%S"),
        )

        self.assertFalse(result["eligible"])
        self.assertEqual(result["reason"], "not_in_priority")

    def test_pilot_map_enforces_rank_order_for_cheboksary_144_recovery(self):
        early_result = failover_policy.can_attempt_failover_claim(
            state_row={
                "Дата": "2026-03-14",
                "target_pvz": "ЧЕБОКСАРЫ_144",
                "status": "owner_failed",
                "attempt_no": 0,
                "updated_at": "14.03.2026 10:00:00",
            },
            configured_pvz_id="ЧЕБОКСАРЫ_143",
            available_pvz=["ЧЕБОКСАРЫ_144", "ЧЕБОКСАРЫ_143"],
            now=datetime.strptime("14.03.2026 10:05:00", "%d.%m.%Y %H:%M:%S"),
        )
        late_result = failover_policy.can_attempt_failover_claim(
            state_row={
                "Дата": "2026-03-14",
                "target_pvz": "ЧЕБОКСАРЫ_144",
                "status": "owner_failed",
                "attempt_no": 0,
                "updated_at": "14.03.2026 10:00:00",
            },
            configured_pvz_id="ЧЕБОКСАРЫ_143",
            available_pvz=["ЧЕБОКСАРЫ_144", "ЧЕБОКСАРЫ_143"],
            now=datetime.strptime("14.03.2026 10:15:00", "%d.%m.%Y %H:%M:%S"),
        )

        self.assertFalse(early_result["eligible"])
        self.assertEqual(early_result["reason"], "rank_delay")
        self.assertTrue(late_result["eligible"])
        self.assertEqual(late_result["rank"], 2)

    def test_can_attempt_failover_claim_rejects_own_target(self):
        with patch.dict(
            failover_policy.FAILOVER_POLICY_CONFIG,
            {"priority_map": {}, "allow_unlisted_fallback": False},
            clear=False,
        ):
            result = failover_policy.can_attempt_failover_claim(
                state_row={
                    "Дата": "2026-03-14",
                    "target_pvz": "PVZ1",
                    "status": "owner_failed",
                    "attempt_no": 0,
                },
                configured_pvz_id="PVZ1",
                available_pvz=["PVZ1"],
            )

        self.assertFalse(result["eligible"])
        self.assertEqual(result["reason"], "own_target_pvz")

    def test_can_attempt_failover_claim_rejects_not_in_priority(self):
        with patch.dict(
            failover_policy.FAILOVER_POLICY_CONFIG,
            {
                "priority_map": {"PVZ2": ["PVZ3"]},
                "allow_unlisted_fallback": False,
            },
            clear=False,
        ):
            result = failover_policy.can_attempt_failover_claim(
                state_row={
                    "Дата": "2026-03-14",
                    "target_pvz": "PVZ2",
                    "status": "owner_failed",
                    "attempt_no": 0,
                    "updated_at": "14.03.2026 10:00:00",
                },
                configured_pvz_id="PVZ4",
                available_pvz=["PVZ2"],
                now=datetime.strptime("14.03.2026 10:15:00", "%d.%m.%Y %H:%M:%S"),
            )

        self.assertFalse(result["eligible"])
        self.assertEqual(result["reason"], "not_in_priority")

    def test_can_attempt_failover_claim_enforces_rank_delay(self):
        with patch.dict(
            failover_policy.FAILOVER_POLICY_CONFIG,
            {
                "priority_map": {"PVZ2": ["PVZ3", "PVZ4"]},
                "default_rank_delay_minutes": 10,
            },
            clear=False,
        ):
            result = failover_policy.can_attempt_failover_claim(
                state_row={
                    "Дата": "2026-03-14",
                    "target_pvz": "PVZ2",
                    "status": "owner_failed",
                    "attempt_no": 0,
                    "updated_at": "14.03.2026 10:00:00",
                },
                configured_pvz_id="PVZ4",
                available_pvz=["PVZ2"],
                now=datetime.strptime("14.03.2026 10:05:00", "%d.%m.%Y %H:%M:%S"),
            )

        self.assertFalse(result["eligible"])
        self.assertEqual(result["reason"], "rank_delay")

    def test_can_attempt_failover_claim_allows_rank_after_delay(self):
        with patch.dict(
            failover_policy.FAILOVER_POLICY_CONFIG,
            {
                "priority_map": {"PVZ2": ["PVZ3", "PVZ4"]},
                "default_rank_delay_minutes": 10,
            },
            clear=False,
        ):
            result = failover_policy.can_attempt_failover_claim(
                state_row={
                    "Дата": "2026-03-14",
                    "target_pvz": "PVZ2",
                    "status": "owner_failed",
                    "attempt_no": 0,
                    "updated_at": "14.03.2026 10:00:00",
                },
                configured_pvz_id="PVZ4",
                available_pvz=["PVZ2"],
                now=datetime.strptime("14.03.2026 10:15:00", "%d.%m.%Y %H:%M:%S"),
            )

        self.assertTrue(result["eligible"])
        self.assertEqual(result["rank"], 2)

    def test_can_attempt_failover_claim_rejects_max_attempts(self):
        with patch.dict(
            failover_policy.FAILOVER_POLICY_CONFIG,
            {
                "priority_map": {"PVZ2": ["PVZ3"]},
                "max_attempts_per_date": 3,
            },
            clear=False,
        ):
            result = failover_policy.can_attempt_failover_claim(
                state_row={
                    "Дата": "2026-03-14",
                    "target_pvz": "PVZ2",
                    "status": "owner_failed",
                    "attempt_no": 3,
                    "updated_at": "14.03.2026 10:00:00",
                },
                configured_pvz_id="PVZ3",
                available_pvz=["PVZ2"],
            )

        self.assertFalse(result["eligible"])
        self.assertEqual(result["reason"], "max_attempts_reached")

    def test_filter_claimable_rows_by_policy_returns_only_eligible(self):
        with patch.dict(
            failover_policy.FAILOVER_POLICY_CONFIG,
            {
                "priority_map": {"PVZ2": ["PVZ3"]},
                "default_rank_delay_minutes": 10,
                "max_attempts_per_date": 3,
            },
            clear=False,
        ):
            rows = failover_policy.filter_claimable_rows_by_policy(
                rows=[
                    {"Дата": "2026-03-14", "target_pvz": "PVZ2", "status": "owner_failed", "attempt_no": 0, "updated_at": "14.03.2026 10:00:00"},
                    {"Дата": "2026-03-14", "target_pvz": "PVZ3", "status": "owner_failed", "attempt_no": 0, "updated_at": "14.03.2026 10:00:00"},
                ],
                configured_pvz_id="PVZ3",
                available_pvz=["PVZ2", "PVZ3"],
                now=datetime.strptime("14.03.2026 10:00:00", "%d.%m.%Y %H:%M:%S"),
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["target_pvz"], "PVZ2")

    def test_get_selection_mode_defaults_to_legacy(self):
        with patch.dict(failover_policy.FAILOVER_POLICY_CONFIG, {}, clear=True):
            self.assertEqual(
                failover_policy.get_selection_mode(),
                failover_policy.SELECTION_MODE_PRIORITY_MAP_LEGACY,
            )

    def test_get_helper_candidates_for_target_uses_capability_reverse_lookup(self):
        with patch.dict(
            failover_policy.FAILOVER_POLICY_CONFIG,
            {
                "capability_map": {
                    "PVZ_HELPER_B": ["PVZ_TARGET_1"],
                    "PVZ_HELPER_A": ["PVZ_TARGET_1", "PVZ_TARGET_2"],
                },
            },
            clear=False,
        ):
            result = failover_policy.get_helper_candidates_for_target("PVZ_TARGET_1")

        self.assertEqual(result, ["pvz_helper_a", "pvz_helper_b"])

    def test_select_preferred_helper_for_target_uses_bias_then_lexical_tiebreak(self):
        with patch.dict(
            failover_policy.FAILOVER_POLICY_CONFIG,
            {
                "capability_map": {
                    "PVZ_HELPER_B": ["PVZ_TARGET_1"],
                    "PVZ_HELPER_A": ["PVZ_TARGET_1"],
                },
                "helper_bias": {
                    "PVZ_HELPER_B": 5,
                    "PVZ_HELPER_A": 1,
                },
            },
            clear=False,
        ):
            result = failover_policy.select_preferred_helper_for_target(
                "PVZ_TARGET_1",
                ["PVZ_TARGET_1", "PVZ_HELPER_A"],
            )

        self.assertEqual(result, "pvz_helper_a")

    def test_select_preferred_helper_for_target_uses_lexical_tiebreak_when_bias_equal(self):
        with patch.dict(
            failover_policy.FAILOVER_POLICY_CONFIG,
            {
                "capability_map": {
                    "PVZ_HELPER_B": ["PVZ_TARGET_1"],
                    "PVZ_HELPER_A": ["PVZ_TARGET_1"],
                },
                "helper_bias": {},
            },
            clear=False,
        ):
            result = failover_policy.select_preferred_helper_for_target(
                "PVZ_TARGET_1",
                ["PVZ_TARGET_1", "PVZ_HELPER_A"],
            )

        self.assertEqual(result, "pvz_helper_a")

    def test_capability_ranked_allows_preferred_helper(self):
        with patch.dict(
            failover_policy.FAILOVER_POLICY_CONFIG,
            {
                "selection_mode": failover_policy.SELECTION_MODE_CAPABILITY_RANKED,
                "capability_map": {
                    "PVZ_HELPER_A": ["PVZ_TARGET_1"],
                    "PVZ_HELPER_B": ["PVZ_TARGET_1"],
                },
                "helper_bias": {"PVZ_HELPER_A": 1, "PVZ_HELPER_B": 5},
                "max_attempts_per_date": 3,
            },
            clear=False,
        ):
            result = failover_policy.can_attempt_failover_claim(
                state_row={
                    "Дата": "2026-03-14",
                    "target_pvz": "PVZ_TARGET_1",
                    "status": "owner_failed",
                    "attempt_no": 0,
                },
                configured_pvz_id="PVZ_HELPER_A",
                available_pvz=["PVZ_TARGET_1", "PVZ_HELPER_A"],
            )

        self.assertTrue(result["eligible"])
        self.assertEqual(result["preferred_helper"], "pvz_helper_a")

    def test_capability_ranked_rejects_non_preferred_helper(self):
        with patch.dict(
            failover_policy.FAILOVER_POLICY_CONFIG,
            {
                "selection_mode": failover_policy.SELECTION_MODE_CAPABILITY_RANKED,
                "capability_map": {
                    "PVZ_HELPER_A": ["PVZ_TARGET_1"],
                    "PVZ_HELPER_B": ["PVZ_TARGET_1"],
                },
                "helper_bias": {"PVZ_HELPER_A": 1, "PVZ_HELPER_B": 5},
                "max_attempts_per_date": 3,
            },
            clear=False,
        ):
            result = failover_policy.can_attempt_failover_claim(
                state_row={
                    "Дата": "2026-03-14",
                    "target_pvz": "PVZ_TARGET_1",
                    "status": "owner_failed",
                    "attempt_no": 0,
                },
                configured_pvz_id="PVZ_HELPER_B",
                available_pvz=["PVZ_TARGET_1", "PVZ_HELPER_B"],
            )

        self.assertFalse(result["eligible"])
        self.assertEqual(result["reason"], "not_preferred_helper")
        self.assertEqual(result["preferred_helper"], "pvz_helper_a")

    def test_capability_ranked_rejects_not_accessible_target(self):
        with patch.dict(
            failover_policy.FAILOVER_POLICY_CONFIG,
            {
                "selection_mode": failover_policy.SELECTION_MODE_CAPABILITY_RANKED,
                "capability_map": {"PVZ_HELPER_A": ["PVZ_TARGET_1"]},
            },
            clear=False,
        ):
            result = failover_policy.can_attempt_failover_claim(
                state_row={
                    "Дата": "2026-03-14",
                    "target_pvz": "PVZ_TARGET_1",
                    "status": "owner_failed",
                    "attempt_no": 0,
                },
                configured_pvz_id="PVZ_HELPER_A",
                available_pvz=["PVZ_HELPER_A"],
            )

        self.assertFalse(result["eligible"])
        self.assertEqual(result["reason"], "not_accessible")

    def test_capability_ranked_rejects_max_attempts(self):
        with patch.dict(
            failover_policy.FAILOVER_POLICY_CONFIG,
            {
                "selection_mode": failover_policy.SELECTION_MODE_CAPABILITY_RANKED,
                "capability_map": {"PVZ_HELPER_A": ["PVZ_TARGET_1"]},
                "max_attempts_per_date": 3,
            },
            clear=False,
        ):
            result = failover_policy.can_attempt_failover_claim(
                state_row={
                    "Дата": "2026-03-14",
                    "target_pvz": "PVZ_TARGET_1",
                    "status": "owner_failed",
                    "attempt_no": 3,
                },
                configured_pvz_id="PVZ_HELPER_A",
                available_pvz=["PVZ_TARGET_1", "PVZ_HELPER_A"],
            )

        self.assertFalse(result["eligible"])
        self.assertEqual(result["reason"], "max_attempts_reached")

    def test_evaluate_claimable_rows_by_policy_returns_selected_and_rejected_summary(self):
        with patch.dict(
            failover_policy.FAILOVER_POLICY_CONFIG,
            {
                "selection_mode": failover_policy.SELECTION_MODE_CAPABILITY_RANKED,
                "capability_map": {
                    "PVZ_HELPER_A": ["PVZ_TARGET_1", "PVZ_TARGET_2"],
                    "PVZ_HELPER_B": ["PVZ_TARGET_1"],
                },
                "helper_bias": {"PVZ_HELPER_A": 1, "PVZ_HELPER_B": 5},
                "max_attempts_per_date": 3,
            },
            clear=False,
        ):
            evaluation = failover_policy.evaluate_claimable_rows_by_policy(
                rows=[
                    {"Дата": "2026-03-14", "target_pvz": "PVZ_TARGET_1", "status": "owner_failed", "attempt_no": 0},
                    {"Дата": "2026-03-14", "target_pvz": "PVZ_TARGET_2", "status": "owner_failed", "attempt_no": 3},
                ],
                configured_pvz_id="PVZ_HELPER_A",
                available_pvz=["PVZ_TARGET_1", "PVZ_TARGET_2", "PVZ_HELPER_A"],
                max_claims=1,
            )

        self.assertEqual(evaluation["mode"], failover_policy.SELECTION_MODE_CAPABILITY_RANKED)
        self.assertEqual(evaluation["eligible_count"], 1)
        self.assertEqual(evaluation["selected_count"], 1)
        self.assertEqual(evaluation["rejected_count"], 1)
        self.assertEqual(evaluation["rejected_reasons"]["max_attempts_reached"], 1)
        self.assertEqual(len(evaluation["selected_rows"]), 1)
        self.assertEqual(evaluation["selected_rows"][0]["target_pvz"], "PVZ_TARGET_1")
        selected_decisions = [item for item in evaluation["decisions"] if item["selected_for_claim"]]
        self.assertEqual(len(selected_decisions), 1)
        self.assertEqual(selected_decisions[0]["preferred_helper"], "pvz_helper_a")


if __name__ == "__main__":
    unittest.main()


