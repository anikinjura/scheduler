"""
Unit tests for reports_upload (Phase 1.3).

Проверяют:
- normalize_pvz_id
- prepare_coverage_filters
- parse_sheet_date_to_iso
- is_retryable_google_sheets_upload_error
- transform_record_for_upload
- prepare_upload_data
- prepare_upload_data_batch
"""
import unittest

from scheduler_runner.tasks.reports.reports_upload import (
    normalize_pvz_id,
    prepare_coverage_filters,
    parse_sheet_date_to_iso,
    is_retryable_google_sheets_upload_error,
    transform_record_for_upload,
    prepare_upload_data,
    prepare_upload_data_batch,
)


class TestNormalizePvzId(unittest.TestCase):
    def test_cyrillic_to_translit(self):
        result = normalize_pvz_id("ЧЕБОКСАРЫ_144")
        self.assertEqual(result, "cheboksary_144")

    def test_already_translit(self):
        result = normalize_pvz_id("CHEBOKSARY_144")
        self.assertEqual(result, "cheboksary_144")

    def test_lowercase_and_strip(self):
        result = normalize_pvz_id("  ЧЕБОКСАРЫ_340  ")
        self.assertTrue(result.islower())
        self.assertEqual(result, result.strip())


class TestPrepareCoverageFilters(unittest.TestCase):
    def test_basic_filter(self):
        filters = prepare_coverage_filters("2026-04-01", "2026-04-07", "ЧЕБОКСАРЫ_144")
        self.assertEqual(filters["Дата_from"], "2026-04-01")
        self.assertEqual(filters["Дата_to"], "2026-04-07")
        self.assertIn("cheboksary_144", filters["ПВЗ"])


class TestParseSheetDateToIso(unittest.TestCase):
    def test_parse_ddmmyyyy(self):
        result = parse_sheet_date_to_iso("01.04.2026")
        self.assertEqual(result, "2026-04-01")

    def test_parse_31dec(self):
        result = parse_sheet_date_to_iso("31.12.2025")
        self.assertEqual(result, "2025-12-31")


class TestRetryableErrors(unittest.TestCase):
    def test_503_is_retryable(self):
        self.assertTrue(is_retryable_google_sheets_upload_error("[503] The service is currently unavailable"))

    def test_timeout_is_retryable(self):
        self.assertTrue(is_retryable_google_sheets_upload_error("Request timed out"))

    def test_parse_error_is_not_retryable(self):
        self.assertFalse(is_retryable_google_sheets_upload_error("Invalid JSON: parse error"))


class TestTransformRecordForUpload(unittest.TestCase):
    def test_basic_mapping(self):
        record = {
            "date": "2026-04-01",
            "pvz": "ЧЕБОКСАРЫ_144",
            "issued_packages": 150,
            "direct_flow": 10,
            "return_flow": 3,
        }
        result = transform_record_for_upload(record)
        self.assertEqual(result["Дата"], "2026-04-01")
        self.assertEqual(result["ПВЗ"], "ЧЕБОКСАРЫ_144")
        self.assertEqual(result["Количество выдач"], 150)
        self.assertEqual(result["Прямой поток"], 10)
        self.assertEqual(result["Возвратный поток"], 3)

    def test_empty_record(self):
        result = transform_record_for_upload({})
        self.assertIn("Дата", result)
        self.assertIn("ПВЗ", result)

    def test_non_dict(self):
        result = transform_record_for_upload("not_a_dict")
        self.assertIsNone(result)


class TestPrepareUploadData(unittest.TestCase):
    def test_single_date(self):
        result = prepare_upload_data({
            "execution_date": "2026-04-01",
            "location_info": "ЧЕБОКСАРЫ_144",
            "summary": {
                "giveout": {"value": 150},
                "direct_flow_total": {"total_carriages": 10},
                "return_flow_total": {"total_carriages": 3},
            },
        })
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["Дата"], "01.04.2026")
        self.assertEqual(result[0]["ПВЗ"], "ЧЕБОКСАРЫ_144")
        self.assertEqual(result[0]["Количество выдач"], 150)

    def test_empty_input(self):
        result = prepare_upload_data(None)
        self.assertEqual(result, [])

    def test_empty_dict(self):
        result = prepare_upload_data({})
        self.assertEqual(result, [])


class TestPrepareUploadDataBatch(unittest.TestCase):
    def test_batch_with_mixed_results(self):
        result = prepare_upload_data_batch({
            "results_by_date": {
                "2026-04-01": {
                    "success": True,
                    "data": {
                        "execution_date": "2026-04-01",
                        "location_info": "PVZ1",
                    },
                },
                "2026-04-02": {
                    "success": False,
                    "error": "parse_failed",
                },
            },
        })
        # Only successful date should produce upload data
        self.assertEqual(len(result), 1)

    def test_batch_empty(self):
        result = prepare_upload_data_batch({})
        self.assertEqual(result, [])

    def test_batch_none(self):
        result = prepare_upload_data_batch(None)
        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()

