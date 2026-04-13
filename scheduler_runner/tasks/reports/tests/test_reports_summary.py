"""
Unit tests for reports_summary (Phase 1.1).

Проверяют:
- dataclasses создаются корректно
- build_* helpers работают
- resolve_final_run_status выдаёт правильные статусы
- _owner_* и _failover_* хелперы корректно классифицируют
"""
import unittest

from ..reports_summary import (
    PVZExecutionResult,
    ReportsBackfillExecutionResult,
    OwnerRunSummary,
    FailoverRunSummary,
    ReportsRunSummary,
    build_pvz_execution_result,
    build_owner_run_summary,
    build_failover_run_summary,
    build_reports_run_summary,
    resolve_final_run_status,
    _owner_has_work,
    _is_owner_skipped_no_missing,
    _owner_had_meaningful_success,
    _owner_had_meaningful_failure,
    _failover_had_meaningful_success,
    _failover_had_meaningful_failure,
    _failover_sync_had_failure,
    _failover_candidate_scan_had_failure,
    _failover_had_any_work,
    _as_date_list,
    _count_batch_successful_dates,
    _count_batch_failed_dates,
    _format_failed_dates,
)


class TestPVZExecutionResult(unittest.TestCase):
    def test_empty_results(self):
        result = build_pvz_execution_result("PVZ1")
        self.assertEqual(result.pvz_id, "PVZ1")
        self.assertEqual(result.missing_dates_count, 0)
        self.assertEqual(result.successful_jobs_count, 0)
        self.assertEqual(result.failed_jobs_count, 0)
        self.assertEqual(result.uploaded_records, 0)

    def test_with_data(self):
        result = build_pvz_execution_result(
            "PVZ1",
            coverage_result={"missing_dates": ["2026-04-01", "2026-04-02"]},
            batch_result={"successful_dates": ["2026-04-01"], "failed_dates": ["2026-04-02"]},
            upload_result={"uploaded_records": 10},
        )
        self.assertEqual(result.missing_dates_count, 2)
        self.assertEqual(result.successful_jobs_count, 1)
        self.assertEqual(result.failed_jobs_count, 1)
        self.assertEqual(result.uploaded_records, 10)


class TestOwnerRunSummary(unittest.TestCase):
    def test_build_empty(self):
        summary = build_owner_run_summary(pvz_id="PVZ1")
        self.assertEqual(summary.pvz_id, "PVZ1")
        self.assertEqual(summary.missing_dates_count, 0)
        self.assertEqual(summary.successful_dates_count, 0)
        self.assertEqual(summary.failed_dates_count, 0)

    def test_build_with_results(self):
        summary = build_owner_run_summary(
            pvz_id="PVZ1",
            coverage_result={"success": True, "missing_dates": ["2026-04-01", "2026-04-02"]},
            batch_result={"success": True, "successful_dates": ["2026-04-01"], "failed_dates": ["2026-04-02"]},
            upload_result={"success": True, "uploaded_records": 5},
        )
        self.assertEqual(summary.pvz_id, "PVZ1")
        self.assertTrue(summary.coverage_success)
        self.assertEqual(summary.missing_dates_count, 2)
        self.assertEqual(summary.successful_dates_count, 1)
        self.assertEqual(summary.failed_dates_count, 1)
        self.assertTrue(summary.upload_success)
        self.assertEqual(summary.uploaded_records, 5)


class TestFailoverRunSummary(unittest.TestCase):
    def test_build_disabled(self):
        summary = build_failover_run_summary(enabled=False)
        self.assertFalse(summary.enabled)
        self.assertFalse(summary.attempted)

    def test_build_with_results(self):
        summary = build_failover_run_summary(
            enabled=True,
            failover_result={
                "attempted": True,
                "available_pvz": ["PVZ2"],
                "candidate_rows_count": 3,
                "claimed_rows_count": 1,
                "recovered_pvz_count": 1,
                "recovered_dates_count": 2,
                "failed_recovery_dates_count": 0,
                "uploaded_records": 10,
                "discovery_result": {"success": True},
                "owner_state_sync": {"attempted": True, "success": True},
                "candidate_scan": {"attempted": True, "success": True},
            },
        )
        self.assertTrue(summary.enabled)
        self.assertTrue(summary.attempted)
        self.assertEqual(summary.available_pvz, ["PVZ2"])
        self.assertEqual(summary.recovered_dates_count, 2)


class TestResolveFinalRunStatus(unittest.TestCase):
    def test_success_owner(self):
        owner = build_owner_run_summary(
            pvz_id="PVZ1",
            coverage_result={"success": True, "missing_dates": ["2026-04-01"]},
            batch_result={"success": True, "successful_dates": ["2026-04-01"]},
            upload_result={"success": True, "uploaded_records": 5},
        )
        status = resolve_final_run_status(owner=owner)
        self.assertEqual(status, "success")

    def test_failed_coverage(self):
        owner = build_owner_run_summary(
            pvz_id="PVZ1",
            coverage_result={"success": False, "error": "coverage_check_failed"},
        )
        status = resolve_final_run_status(owner=owner)
        self.assertEqual(status, "failed")

    def test_skipped_no_missing(self):
        owner = build_owner_run_summary(
            pvz_id="PVZ1",
            coverage_result={"success": True, "missing_dates": []},
            batch_result={"success": True, "successful_dates": []},
        )
        status = resolve_final_run_status(owner=owner)
        self.assertEqual(status, "skipped")

    def test_partial_with_failures(self):
        # Owner has both success and failure → partial
        owner = build_owner_run_summary(
            pvz_id="PVZ1",
            coverage_result={"success": True, "missing_dates": ["2026-04-01", "2026-04-02"]},
            batch_result={"success": False, "successful_dates": ["2026-04-01"], "failed_dates": ["2026-04-02"]},
            upload_result={"success": False, "error": "upload_failed"},
        )
        status = resolve_final_run_status(owner=owner)
        self.assertEqual(status, "partial")

    def test_multi_pvz_success(self):
        multi_pvz = ReportsBackfillExecutionResult(
            date_from="2026-04-01",
            date_to="2026-04-07",
            processed_pvz_count=2,
            missing_dates_count=4,
            successful_jobs_count=4,
            failed_jobs_count=0,
            uploaded_records=20,
            pvz_results={},
        )
        status = resolve_final_run_status(multi_pvz=multi_pvz)
        self.assertEqual(status, "success")


class TestOwnerStatusHelpers(unittest.TestCase):
    def test_owner_has_work(self):
        owner = build_owner_run_summary(
            pvz_id="PVZ1",
            coverage_result={"success": True, "missing_dates": ["2026-04-01"]},
            batch_result={"success": True, "successful_dates": ["2026-04-01"]},
            upload_result={"success": True},
        )
        self.assertTrue(_owner_has_work(owner))

    def test_owner_has_no_work(self):
        owner = build_owner_run_summary(
            pvz_id="PVZ1",
            coverage_result={"success": True, "missing_dates": []},
        )
        self.assertFalse(_owner_has_work(owner))

    def test_owner_skipped_no_missing(self):
        owner = build_owner_run_summary(
            pvz_id="PVZ1",
            coverage_result={"success": True, "missing_dates": []},
        )
        self.assertTrue(_is_owner_skipped_no_missing(owner))

    def test_owner_meaningful_success(self):
        owner = build_owner_run_summary(
            pvz_id="PVZ1",
            coverage_result={"success": True, "missing_dates": ["2026-04-01"]},
            batch_result={"success": True, "successful_dates": ["2026-04-01"]},
            upload_result={"success": True, "uploaded_records": 5},
        )
        self.assertTrue(_owner_had_meaningful_success(owner))

    def test_owner_meaningful_failure(self):
        owner = build_owner_run_summary(
            pvz_id="PVZ1",
            coverage_result={"success": True, "missing_dates": ["2026-04-01"]},
            batch_result={"success": False, "successful_dates": [], "failed_dates": ["2026-04-01"]},
            upload_result={"success": False},
        )
        self.assertTrue(_owner_had_meaningful_failure(owner))


class TestFailoverStatusHelpers(unittest.TestCase):
    def test_failover_meaningful_success(self):
        failover = build_failover_run_summary(
            enabled=True,
            failover_result={
                "recovered_dates_count": 2,
                "recovered_pvz_count": 1,
                "uploaded_records": 10,
            },
        )
        self.assertTrue(_failover_had_meaningful_success(failover))

    def test_failover_no_meaningful_success(self):
        failover = build_failover_run_summary(enabled=True)
        self.assertFalse(_failover_had_meaningful_success(failover))

    def test_failover_meaningful_failure(self):
        failover = build_failover_run_summary(
            enabled=True,
            failover_result={"failed_recovery_dates_count": 1},
        )
        self.assertTrue(_failover_had_meaningful_failure(failover))

    def test_failover_sync_failure(self):
        failover = build_failover_run_summary(
            enabled=True,
            failover_result={
                "owner_state_sync": {"attempted": True, "success": False},
            },
        )
        self.assertTrue(_failover_sync_had_failure(failover))

    def test_failover_candidate_scan_failure(self):
        failover = build_failover_run_summary(
            enabled=True,
            failover_result={
                "candidate_scan": {"attempted": True, "success": False},
            },
        )
        self.assertTrue(_failover_candidate_scan_had_failure(failover))

    def test_failover_had_any_work(self):
        failover = build_failover_run_summary(
            enabled=True,
            failover_result={
                "candidate_scan": {"attempted": True, "success": False},  # failure counts as "work"
            },
        )
        self.assertTrue(_failover_had_any_work(failover))

    def test_failover_no_work(self):
        failover = build_failover_run_summary(enabled=True)
        self.assertFalse(_failover_had_any_work(failover))


class TestBatchResultHelpers(unittest.TestCase):
    def test_as_date_list(self):
        self.assertEqual(_as_date_list(None), [])
        self.assertEqual(_as_date_list("2026-04-01"), ["2026-04-01"])
        self.assertEqual(_as_date_list(["2026-04-01"]), ["2026-04-01"])
        self.assertEqual(_as_date_list(42), [])

    def test_count_successful_dates(self):
        self.assertEqual(_count_batch_successful_dates(None), 0)
        self.assertEqual(_count_batch_successful_dates({"successful_dates": ["a", "b"]}), 2)
        self.assertEqual(_count_batch_successful_dates({"successful_dates": 5}), 5)

    def test_count_failed_dates(self):
        self.assertEqual(_count_batch_failed_dates(None), 0)
        self.assertEqual(_count_batch_failed_dates({"failed_dates": ["a"]}), 1)

    def test_format_failed_dates(self):
        self.assertEqual(_format_failed_dates([]), "-")
        self.assertEqual(_format_failed_dates(None), "-")
        self.assertEqual(_format_failed_dates(["2026-04-01"]), "2026-04-01")
        self.assertEqual(
            _format_failed_dates(["2026-04-01", "2026-04-02", "2026-04-03", "2026-04-04", "2026-04-05", "2026-04-06"]),
            "2026-04-01, 2026-04-02, 2026-04-03, 2026-04-04, 2026-04-05",
        )


class TestReportsRunSummary(unittest.TestCase):
    def test_build_with_all_parts(self):
        owner = build_owner_run_summary(
            pvz_id="PVZ1",
            coverage_result={"success": True, "missing_dates": ["2026-04-01"]},
            batch_result={"success": True, "successful_dates": ["2026-04-01"]},
            upload_result={"success": True, "uploaded_records": 5},
        )
        summary = build_reports_run_summary(
            mode="backfill_single_pvz",
            configured_pvz_id="PVZ1",
            date_from="2026-04-01",
            date_to="2026-04-07",
            owner=owner,
            failover=build_failover_run_summary(enabled=False),
        )
        self.assertEqual(summary.mode, "backfill_single_pvz")
        self.assertEqual(summary.final_status, "success")
        self.assertIsNotNone(summary.owner)


if __name__ == "__main__":
    unittest.main()

