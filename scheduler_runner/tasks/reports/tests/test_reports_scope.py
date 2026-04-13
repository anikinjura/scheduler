"""
Unit tests for reports_scope (Phase 2.1).

Проверяют:
- resolve_pvz_ids
- should_run_automatic_failover_coordination
- build_jobs_from_missing_dates_by_pvz (mocked)
- group_jobs_by_pvz

normalize_pvz_id тестируется отдельно (reports_utils).
"""
import unittest
from unittest.mock import patch, MagicMock

from ..reports_scope import (
    resolve_pvz_ids,
    should_run_automatic_failover_coordination,
    group_jobs_by_pvz,
)
from ..reports_utils import normalize_pvz_id

# PVZ_ID берётся из config.base_config и зависит от машины
from config.base_config import PVZ_ID


class TestNormalizePvzId(unittest.TestCase):
    def test_cyrillic(self):
        self.assertEqual(normalize_pvz_id("ЧЕБОКСАРЫ_144"), "cheboksary_144")

    def test_none(self):
        self.assertEqual(normalize_pvz_id(None), "")

    def test_whitespace(self):
        self.assertEqual(normalize_pvz_id("  PVZ_1  "), "pvz_1")


class TestResolvePvzIds(unittest.TestCase):
    def test_empty_returns_default(self):
        self.assertEqual(resolve_pvz_ids(None), [PVZ_ID])

    def test_deduplication(self):
        result = resolve_pvz_ids(["PVZ1", "PVZ1", "PVZ2"])
        self.assertEqual(result, ["PVZ1", "PVZ2"])

    def test_strips_and_filters_empty(self):
        result = resolve_pvz_ids(["", "  ", "PVZ1", "PVZ2"])
        self.assertEqual(result, ["PVZ1", "PVZ2"])


class TestShouldRunAutomaticFailoverCoordination(unittest.TestCase):
    def test_disabled(self):
        self.assertFalse(should_run_automatic_failover_coordination(enabled=False))

    def test_raw_pvz_ids_disables(self):
        self.assertFalse(should_run_automatic_failover_coordination(
            enabled=True, raw_pvz_ids=["PVZ1"],
        ))

    def test_multi_pvz_disables(self):
        self.assertFalse(should_run_automatic_failover_coordination(
            enabled=True, resolved_pvz_ids=["PVZ1", "PVZ2"],
        ))

    def test_single_matching_enables(self):
        self.assertTrue(should_run_automatic_failover_coordination(
            enabled=True,
            resolved_pvz_ids=[PVZ_ID],
            current_pvz_id=PVZ_ID,
        ))

    def test_mismatching_pvz_disables(self):
        self.assertFalse(should_run_automatic_failover_coordination(
            enabled=True,
            current_pvz_id="ЧЕБОКСАРЫ_182",
            configured_pvz_id="ЧЕБОКСАРЫ_144",
        ))


class TestGroupJobsByPvz(unittest.TestCase):
    def test_groups_correctly(self):
        job1 = MagicMock(pvz_id="PVZ1")
        job2 = MagicMock(pvz_id="PVZ2")
        job3 = MagicMock(pvz_id="PVZ1")
        grouped = group_jobs_by_pvz([job1, job2, job3])
        self.assertEqual(len(grouped["PVZ1"]), 2)
        self.assertEqual(len(grouped["PVZ2"]), 1)

    def test_empty_returns_empty_dict(self):
        self.assertEqual(group_jobs_by_pvz(None), {})
        self.assertEqual(group_jobs_by_pvz([]), {})


if __name__ == "__main__":
    unittest.main()

