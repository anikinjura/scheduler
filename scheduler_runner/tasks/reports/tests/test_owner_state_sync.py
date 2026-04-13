"""
Unit tests for owner_state_sync (Phase 2.2).

Проверяют:
- classify_owner_success_history
- should_persist_owner_success_from_history
- build_owner_final_failover_state_records
- sync_owner_failover_state_from_batch_result (mocked failover_state)
"""
import unittest
from unittest.mock import patch, MagicMock

from ..owner_state_sync import (
    classify_owner_success_history,
    should_persist_owner_success_from_history,
    build_owner_final_failover_state_records,
    sync_owner_failover_state_from_batch_result,
)


class TestClassifyOwnerSuccessHistory(unittest.TestCase):
    def test_no_state(self):
        result = classify_owner_success_history(None)
        self.assertEqual(result["classification"], "no_state")
        self.assertFalse(result["should_persist_success_if_enabled"])

    def test_incident_related_owner_failed(self):
        result = classify_owner_success_history({"status": "owner_failed"})
        self.assertEqual(result["classification"], "incident_related")
        self.assertTrue(result["should_persist_success_if_enabled"])

    def test_incident_related_claim_expired(self):
        result = classify_owner_success_history({"status": "claim_expired"})
        self.assertEqual(result["classification"], "incident_related")
        self.assertTrue(result["should_persist_success_if_enabled"])

    def test_incident_related_failover_failed(self):
        result = classify_owner_success_history({"status": "failover_failed"})
        self.assertEqual(result["classification"], "incident_related")
        self.assertTrue(result["should_persist_success_if_enabled"])

    def test_incident_related_failover_claimed(self):
        result = classify_owner_success_history({"status": "failover_claimed"})
        self.assertEqual(result["classification"], "incident_related")
        self.assertTrue(result["should_persist_success_if_enabled"])

    def test_incident_related_failover_success(self):
        result = classify_owner_success_history({"status": "failover_success"})
        self.assertEqual(result["classification"], "incident_related")
        self.assertTrue(result["should_persist_success_if_enabled"])

    def test_terminal_success_only(self):
        result = classify_owner_success_history({"status": "owner_success"})
        self.assertEqual(result["classification"], "terminal_success_only")
        self.assertFalse(result["should_persist_success_if_enabled"])

    def test_other_status(self):
        result = classify_owner_success_history({"status": "unknown_status"})
        self.assertEqual(result["classification"], "other")
        self.assertFalse(result["should_persist_success_if_enabled"])


class TestShouldPersistOwnerSuccessFromHistory(unittest.TestCase):
    def test_incident_related_persists(self):
        result = should_persist_owner_success_from_history({"status": "owner_failed"})
        self.assertTrue(result["persisted"])
        self.assertEqual(result["classification"], "incident_related")
        self.assertEqual(result["reason"], "incident_history_present")

    def test_no_state_suppress(self):
        result = should_persist_owner_success_from_history(None)
        self.assertFalse(result["persisted"])
        self.assertEqual(result["classification"], "no_state")
        self.assertEqual(result["reason"], "healthy_new_success")

    def test_terminal_success_suppress(self):
        result = should_persist_owner_success_from_history({"status": "owner_success"})
        self.assertFalse(result["persisted"])
        self.assertEqual(result["classification"], "terminal_success_only")
        self.assertEqual(result["reason"], "already_terminal_success")


class TestBuildOwnerFinalFailoverStateRecords(unittest.TestCase):
    def test_failed_parse_dates(self):
        result = build_owner_final_failover_state_records(
            owner_object_name="PVZ1",
            missing_dates=["2026-04-01", "2026-04-02"],
            batch_result={
                "results_by_date": {
                    "2026-04-01": {"success": False, "error": "parse_failed"},
                    "2026-04-02": {"success": True, "data": {}},
                }
            },
            upload_result=None,  # None means upload wasn't called yet → treat as not provided → upload_success=True
        )
        # One failed parse → owner_failed record
        # One successful parse → owner_success suppressed (no prior state)
        self.assertEqual(len(result["failed_dates"]), 1)
        self.assertEqual(len(result["successful_dates"]), 1)
        # Only the failed date gets a record; successful is suppressed
        self.assertEqual(len(result["records"]), 1)

    def test_upload_failure(self):
        result = build_owner_final_failover_state_records(
            owner_object_name="PVZ1",
            missing_dates=["2026-04-01"],
            batch_result={
                "results_by_date": {
                    "2026-04-01": {"success": True, "data": {}},
                }
            },
            upload_result={"success": False, "error": "upload_failed"},
        )
        self.assertEqual(len(result["failed_dates"]), 1)
        self.assertEqual(len(result["successful_dates"]), 0)
        self.assertEqual(len(result["records"]), 1)
        self.assertEqual(result["records"][0]["status"], "owner_failed")

    def test_success_with_suppression(self):
        result = build_owner_final_failover_state_records(
            owner_object_name="PVZ1",
            missing_dates=["2026-04-01"],
            batch_result={
                "results_by_date": {
                    "2026-04-01": {"success": True, "data": {}},
                }
            },
            upload_result={"success": True, "uploaded_records": 5},
            existing_state_rows_by_date={"2026-04-01": None},
        )
        self.assertEqual(len(result["successful_dates"]), 1)
        self.assertEqual(len(result["suppressed_success_dates"]), 1)
        self.assertEqual(len(result["records"]), 0)

    def test_success_with_persist(self):
        result = build_owner_final_failover_state_records(
            owner_object_name="PVZ1",
            missing_dates=["2026-04-01"],
            batch_result={
                "results_by_date": {
                    "2026-04-01": {"success": True, "data": {}},
                }
            },
            upload_result={"success": True, "uploaded_records": 5},
            existing_state_rows_by_date={"2026-04-01": {"status": "owner_failed"}},
        )
        self.assertEqual(len(result["successful_dates"]), 1)
        self.assertEqual(len(result["suppressed_success_dates"]), 0)
        self.assertEqual(len(result["records"]), 1)
        self.assertEqual(result["records"][0]["status"], "owner_success")


@patch("scheduler_runner.tasks.reports.storage.failover_state.get_default_store")
class TestSyncOwnerFailoverStateFromBatchResult(unittest.TestCase):
    def test_sync_success(self, mock_get_store):
        mock_store = MagicMock()
        mock_store.get_rows_by_keys.return_value = {}
        mock_store.upsert_records.return_value = {
            "success": True,
            "results": [],
            "diagnostics": {"updated_count": 1, "appended_count": 0},
        }
        mock_get_store.return_value = mock_store

        result = build_owner_final_failover_state_records(
            owner_object_name="PVZ1",
            missing_dates=["2026-04-01"],
            batch_result={
                "results_by_date": {
                    "2026-04-01": {"success": True, "data": {}},
                }
            },
            upload_result={"success": True, "uploaded_records": 5},
            existing_state_rows_by_date={"2026-04-01": {"status": "owner_failed"}},
        )
        self.assertEqual(len(result["successful_dates"]), 1)
        self.assertEqual(len(result["records"]), 1)

    def test_sync_raises_on_upsert_failure(self, mock_get_store):
        mock_store = MagicMock()
        mock_store.get_rows_by_keys.return_value = {}
        mock_store.upsert_records.return_value = {"success": False, "error": "upsert_failed"}
        mock_get_store.return_value = mock_store

        from ..owner_state_sync import sync_owner_failover_state_from_batch_result

        # build_owner_final_failover_state_records with upload failure → records with owner_failed
        # Then upsert fails → RuntimeError
        # We need records to be generated, so trigger upload failure path:
        with self.assertRaises(RuntimeError) as ctx:
            sync_owner_failover_state_from_batch_result(
                owner_object_name="PVZ1",
                missing_dates=["2026-04-01"],
                batch_result={
                    "results_by_date": {
                        "2026-04-01": {"success": True, "data": {}},
                    }
                },
                upload_result={"success": False, "error": "upload_failed"},
                source_run_id="test",
            )
        self.assertIn("KPI_FAILOVER_STATE", str(ctx.exception))


@patch("scheduler_runner.tasks.reports.owner_state_sync.time.sleep", return_value=None)
class TestOwnerStateSyncRetry(unittest.TestCase):
    """Retry с jitter для get_rows_by_keys в owner state sync."""

    def test_retries_on_429_then_succeeds(self, mock_sleep):
        """429 на первой попытке → retry → success."""
        mock_store = MagicMock()
        mock_store.get_rows_by_keys.side_effect = [
            RuntimeError("APIError: [429]: Quota exceeded"),
            {},
        ]
        mock_store.upsert_records.return_value = {"success": True, "results": [], "diagnostics": {}}

        result = sync_owner_failover_state_from_batch_result(
            owner_object_name="PVZ1",
            missing_dates=["2026-04-01"],
            batch_result={
                "results_by_date": {
                    "2026-04-01": {"success": True, "data": {}},
                }
            },
            upload_result={"success": True, "uploaded_records": 1},
            store=mock_store,
        )
        self.assertEqual(mock_store.get_rows_by_keys.call_count, 2)
        self.assertEqual(mock_sleep.call_count, 1)
        self.assertEqual(result["successful_dates"], ["2026-04-01"])

    def test_retries_on_503_then_succeeds(self, mock_sleep):
        """503 на первой попытке → retry → success."""
        mock_store = MagicMock()
        mock_store.get_rows_by_keys.side_effect = [
            RuntimeError("APIError: [503]: Service unavailable"),
            {},
        ]
        mock_store.upsert_records.return_value = {"success": True, "results": [], "diagnostics": {}}

        result = sync_owner_failover_state_from_batch_result(
            owner_object_name="PVZ1",
            missing_dates=["2026-04-01"],
            batch_result={
                "results_by_date": {
                    "2026-04-01": {"success": True, "data": {}},
                }
            },
            upload_result={"success": True, "uploaded_records": 1},
            store=mock_store,
        )
        self.assertEqual(mock_store.get_rows_by_keys.call_count, 2)
        self.assertEqual(result["successful_dates"], ["2026-04-01"])

    def test_raises_after_max_attempts(self, mock_sleep):
        """3× 429 → RuntimeError после max_attempts."""
        mock_store = MagicMock()
        mock_store.get_rows_by_keys.side_effect = RuntimeError("APIError: [429]: Quota exceeded")

        with self.assertRaises(RuntimeError) as ctx:
            sync_owner_failover_state_from_batch_result(
                owner_object_name="PVZ1",
                missing_dates=["2026-04-01"],
                batch_result={
                    "results_by_date": {
                        "2026-04-01": {"success": True, "data": {}},
                    }
                },
                upload_result={"success": True, "uploaded_records": 1},
                store=mock_store,
            )
        self.assertIn("3 попыток", str(ctx.exception))
        self.assertEqual(mock_store.get_rows_by_keys.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)

    def test_non_retryable_error_raises_immediately(self, mock_sleep):
        """Non-retryable ошибка → immediate raise, без retry."""
        mock_store = MagicMock()
        mock_store.get_rows_by_keys.side_effect = RuntimeError("Authentication error: invalid credentials")

        with self.assertRaises(RuntimeError):
            sync_owner_failover_state_from_batch_result(
                owner_object_name="PVZ1",
                missing_dates=["2026-04-01"],
                batch_result={
                    "results_by_date": {
                        "2026-04-01": {"success": True, "data": {}},
                    }
                },
                upload_result={"success": True, "uploaded_records": 1},
                store=mock_store,
            )
        self.assertEqual(mock_store.get_rows_by_keys.call_count, 1)
        mock_sleep.assert_not_called()

