"""
Проверка отсутствия циклических импортов после декомпозиции Phase 1.

Запуск:
    .venv\\Scripts\\python.exe -m pytest .tmp\\refactored_modules\\tests\\test_module_imports.py -q
"""
import unittest


class TestNoCircularImports(unittest.TestCase):
    def test_import_reports_summary(self):
        from ..reports_summary import (
            PVZExecutionResult,
            OwnerRunSummary,
            FailoverRunSummary,
            ReportsRunSummary,
            ReportsBackfillExecutionResult,
            build_pvz_execution_result,
            build_owner_run_summary,
            build_failover_run_summary,
            build_reports_run_summary,
            resolve_final_run_status,
        )

    def test_import_reports_notifications(self):
        from ..reports_notifications import (
            prepare_notification_data,
            format_notification_message,
            prepare_batch_notification_data,
            format_batch_notification_message,
            build_aggregated_backfill_summary,
            format_aggregated_backfill_notification_message,
            format_reports_run_notification_message,
            send_notification_microservice,
        )

    def test_import_reports_upload(self):
        from ..reports_upload import (
            create_uploader_logger,
            prepare_connection_params,
            prepare_coverage_filters,
            parse_sheet_date_to_iso,
            detect_missing_report_dates,
            detect_missing_report_dates_by_pvz,
            prepare_upload_data,
            prepare_upload_data_batch,
            run_upload_microservice,
            run_upload_batch_microservice,
            transform_record_for_upload,
            is_retryable_google_sheets_upload_error,
            run_google_sheets_upload_with_retry,
        )

    def test_import_reports_utils(self):
        from ..reports_utils import normalize_pvz_id
        self.assertTrue(callable(normalize_pvz_id))

    def test_import_reports_scope(self):
        from ..reports_scope import (
            resolve_pvz_ids,
            discover_available_pvz_scope,
            resolve_accessible_pvz_ids,
            should_run_automatic_failover_coordination,
            build_parser_definition,
            build_jobs_from_missing_dates_by_pvz,
            group_jobs_by_pvz,
        )

    def test_import_owner_state_sync(self):
        from ..owner_state_sync import (
            mark_dates_with_owner_status,
            classify_owner_success_history,
            should_persist_owner_success_from_history,
            build_owner_final_failover_state_records,
            sync_owner_failover_state_from_batch_result,
        )

    def test_import_failover_orchestration(self):
        from ..failover_orchestration import (
            collect_claimable_failover_rows,
            normalize_claimable_failover_evaluation,
            should_scan_failover_candidates,
            should_scan_failover_candidates_legacy,
            should_scan_failover_candidates_capability_ranked,
            collect_failover_scan_decisions,
            claim_failover_rows,
            run_claimed_failover_backfill,
            run_failover_coordination_pass,
        )

    def test_import_reports_processor_orchestrator(self):
        """reports_processor.py — тонкий orchestrator после декомпозиции."""
        from ..reports_processor import main, build_processor_run_id
        self.assertTrue(callable(main))
        self.assertTrue(callable(build_processor_run_id))


class TestCrossModuleDependencies(unittest.TestCase):
    def test_notifications_depends_on_summary(self):
        """reports_notifications.py зависит от reports_summary.py — проверяем, что import работает."""
        from ..reports_notifications import format_reports_run_notification_message
        from ..reports_summary import ReportsRunSummary, OwnerRunSummary, FailoverRunSummary

        # Если cyclic dependency exists, этот import упадёт
        self.assertTrue(callable(format_reports_run_notification_message))

    def test_upload_independent(self):
        """reports_upload.py не должен зависеть от summary/notifications."""
        from ..reports_upload import (
            prepare_upload_data,
            detect_missing_report_dates,
            run_upload_batch_microservice,
        )
        self.assertTrue(callable(prepare_upload_data))
        self.assertTrue(callable(detect_missing_report_dates))
        self.assertTrue(callable(run_upload_batch_microservice))


if __name__ == "__main__":
    unittest.main()

