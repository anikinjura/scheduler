"""
Тесты извлечения шагов из ответов API по DATA_SOURCE_MODE (этап 2 docs/MODERNIZATION_PLAN.md).

Браузера нет: перехватчик — mock с wait_for, навигация — mock navigate_to_target.
"""
import unittest
from copy import deepcopy
from unittest.mock import MagicMock, Mock

from scheduler_runner.utils.parser.configs.base_configs.base_report_config import BASE_REPORT_CONFIG
from scheduler_runner.utils.parser.core.api_response_capture import (
    ApiCaptureError,
    ApiFieldNotFound,
    ApiResponseNotFound,
)
from scheduler_runner.utils.parser.core.tests.test_base_report_parser import TestConcreteReportParser

GIVEOUT_STEP = {
    "base_url": "https://turbo-pvz.ozon.ru/reports/giveout",
    "processing_type": "simple",
    "data_extraction": {"selector": "//div", "pattern": r"Найдено:\s*(\d+)"},
    "result_key": "giveout_count",
    "api_extraction": {
        "type": "value",
        "request": {"path": "/api2/reports/give_out/logV2",
                    "query_contains": {"startDate": "{date}T00:00+03:00", "operationTypes": "GiveoutAll"}},
        "value_path": "logV2.totalCount",
        "post_processing": {"convert_to": "int"},
    },
}

FLOW_STEP = {
    "base_url": "https://turbo-pvz.ozon.ru/outbound/carriages-archive",
    "filter_template": "?f",
    "data_type_filter_template": "Direct",
    "processing_type": "table_nested",
    "result_key": "direct_flow_data",
    "nested_processing": {
        "enabled": True,
        "base_url_template": "https://turbo-pvz.ozon.ru/outbound/carriages-archive/{carriage_id}",
        "filter_template": "?filter={{{data_type_filter_template}}}",
        "data_type_filter_template": '"articleState":"Took"',
        "aggregation": {"method": "sum", "target_field": "total_carriages"},
    },
    "api_extraction": {
        "type": "list_nested",
        "list_request": {"path": "/GetCarriages", "query_contains": {"startSentMoment": "{date}T00:00:00+03:00", "flowType": "Direct"}},
        "items_path": "carriages", "id_field": "carriageId", "total_path": "totalCount",
        "nested_request": {"path": "/GetCarriageArticles", "query_contains": {"carriageId": "{identifier}", "articleState": "Took"}},
        "nested_value_path": "totalCount",
        "post_processing": {"convert_to": "int"},
    },
}


class ApiExtractionTestCase(unittest.TestCase):
    def setUp(self):
        config = deepcopy(BASE_REPORT_CONFIG)
        config["execution_date"] = "2026-09-28"
        self.parser = TestConcreteReportParser(config, logger=MagicMock())
        self.parser.driver = Mock()
        self.parser.api_capture = Mock()
        self.parser.navigate_to_target = Mock(return_value=True)
        self.parser._handle_dom_extraction = Mock(return_value=508)

    def set_mode(self, mode):
        self.parser.config["DATA_SOURCE_MODE"] = mode

    def logged(self, level):
        return " ".join(str(c.args[0]) for c in getattr(self.parser.logger, level).call_args_list)


class TestValueExtraction(ApiExtractionTestCase):
    def test_dom_mode_does_not_touch_api(self):
        self.set_mode("dom")
        result, source = self.parser._extract_step_result(GIVEOUT_STEP)
        self.assertEqual((result, source), (508, "dom"))
        self.parser.api_capture.wait_for.assert_not_called()

    def test_api_mode_reads_value_and_formats_query_with_date(self):
        self.set_mode("api")
        self.parser.api_capture.wait_for.return_value = {"logV2": {"totalCount": 508}}

        result, source = self.parser._extract_step_result(GIVEOUT_STEP)

        self.assertEqual((result, source), (508, "api"))
        self.parser._handle_dom_extraction.assert_not_called()
        path, query = self.parser.api_capture.wait_for.call_args.args[1:3]
        self.assertEqual(path, "/api2/reports/give_out/logV2")
        self.assertEqual(query, {"startDate": "2026-09-28T00:00+03:00", "operationTypes": "GiveoutAll"})

    def test_api_mode_missing_field_is_error_not_zero(self):
        self.set_mode("api")
        self.parser.api_capture.wait_for.return_value = {"logV2": {}}
        with self.assertRaises(ApiFieldNotFound):
            self.parser._extract_step_result(GIVEOUT_STEP)

    def test_api_mode_without_capture_is_error(self):
        self.set_mode("api")
        self.parser.api_capture = None
        with self.assertRaises(ApiCaptureError):
            self.parser._extract_step_result(GIVEOUT_STEP)

    def test_fallback_uses_dom_when_api_fails(self):
        self.set_mode("api_with_dom_fallback")
        self.parser.api_capture.wait_for.side_effect = ApiResponseNotFound("нет запроса")

        result, source = self.parser._extract_step_result(GIVEOUT_STEP)

        self.assertEqual((result, source), (508, "dom_fallback"))
        self.assertIn("API_EXTRACTION_FALLBACK step=giveout_count", self.logged("warning"))

    def test_fallback_does_not_hide_auth_errors(self):
        self.set_mode("api_with_dom_fallback")
        self.parser.api_capture.wait_for.side_effect = Exception("AUTH_REQUIRED: redirected_to_login")
        with self.assertRaises(Exception):
            self.parser._extract_step_result(GIVEOUT_STEP)

    def test_shadow_returns_dom_and_logs_match(self):
        self.set_mode("shadow")
        self.parser.api_capture.wait_for.return_value = {"logV2": {"totalCount": 508}}
        self.assertEqual(self.parser._extract_step_result(GIVEOUT_STEP), (508, "dom"))
        self.assertIn("API_SHADOW_MATCH step=giveout_count value=508", self.logged("info"))

    def test_shadow_logs_mismatch(self):
        self.set_mode("shadow")
        self.parser.api_capture.wait_for.return_value = {"logV2": {"totalCount": 507}}
        self.assertEqual(self.parser._extract_step_result(GIVEOUT_STEP), (508, "dom"))
        self.assertIn("API_SHADOW_MISMATCH step=giveout_count dom=508 api=507", self.logged("warning"))

    def test_shadow_logs_api_error_and_keeps_dom(self):
        self.set_mode("shadow")
        self.parser.api_capture.wait_for.side_effect = ApiResponseNotFound("нет запроса")
        self.assertEqual(self.parser._extract_step_result(GIVEOUT_STEP), (508, "dom"))
        self.assertIn("API_SHADOW_ERROR step=giveout_count", self.logged("warning"))

    def test_missing_response_reloads_page_once(self):
        self.set_mode("api")
        self.parser.api_capture.wait_for.side_effect = [ApiResponseNotFound("нет ответа"), {"logV2": {"totalCount": 508}}]

        self.assertEqual(self.parser._extract_step_result(GIVEOUT_STEP), (508, "api"))
        self.parser.navigate_to_target.assert_called_once()
        self.assertIn("API_RETRY_RELOAD step=giveout_count", self.logged("warning"))

    def test_missing_response_after_reload_is_error(self):
        self.set_mode("api")
        self.parser.api_capture.wait_for.side_effect = ApiResponseNotFound("нет ответа")
        with self.assertRaises(ApiResponseNotFound):
            self.parser._extract_step_result(GIVEOUT_STEP)
        self.assertEqual(self.parser.api_capture.wait_for.call_count, 2)

    def test_step_without_api_extraction_stays_dom(self):
        self.set_mode("api")
        step = {k: v for k, v in GIVEOUT_STEP.items() if k != "api_extraction"}
        self.assertEqual(self.parser._extract_step_result(step), (508, "dom"))


class TestListNestedExtraction(ApiExtractionTestCase):
    def responses(self, carriages, articles, total=None):
        listing = {"totalCount": len(carriages) if total is None else total,
                   "carriages": [{"carriageId": c} for c in carriages]}
        return [listing] + [{"totalCount": n} for n in articles]

    def test_sum_and_details_like_dom(self):
        self.set_mode("api")
        self.parser.api_capture.wait_for.side_effect = self.responses([101, 202], [89, 11])
        self.parser.config.update({"base_url": "archive", "filter_template": "?f", "data_type_filter_template": "Direct"})

        result, source = self.parser._extract_step_result(FLOW_STEP)

        self.assertEqual(source, "api")
        self.assertEqual(result["total_carriages"], 100)
        self.assertEqual([(d["identifier"], d["value"]) for d in result["details"]], [("101", 89), ("202", 11)])
        nested_query = self.parser.api_capture.wait_for.call_args_list[2].args[2]
        self.assertEqual(nested_query, {"carriageId": "202", "articleState": "Took"})
        # конфигурация шага восстановлена после переходов по перевозкам
        self.assertEqual((self.parser.config["base_url"], self.parser.config["data_type_filter_template"]), ("archive", "Direct"))

    def test_no_carriages_gives_zero_total(self):
        self.set_mode("api")
        self.parser.api_capture.wait_for.side_effect = self.responses([], [])
        result, _ = self.parser._extract_step_result(FLOW_STEP)
        self.assertEqual(result["total_carriages"], 0)
        self.parser.navigate_to_target.assert_not_called()

    def test_failed_carriage_page_is_error_not_silent_skip(self):
        self.set_mode("api")
        self.parser.api_capture.wait_for.side_effect = self.responses([101], [89])
        self.parser.navigate_to_target = Mock(return_value=False)
        with self.assertRaises(ApiCaptureError):
            self.parser._extract_step_result(FLOW_STEP)

    def test_truncated_list_is_error_in_api_mode(self):
        self.set_mode("api")
        self.parser.api_capture.wait_for.side_effect = self.responses([101], [89], total=25)
        with self.assertRaises(ApiCaptureError):
            self.parser._extract_step_result(FLOW_STEP)

    def test_truncated_list_is_warning_in_shadow_mode(self):
        self.set_mode("shadow")
        dom = {"total_carriages": 89, "details": [{"identifier": "101", "value": 89}]}
        self.parser._handle_dom_extraction = Mock(return_value=dom)
        self.parser.api_capture.wait_for.side_effect = self.responses([101], [89], total=25)

        result, source = self.parser._extract_step_result(FLOW_STEP)

        self.assertEqual((result, source), (dom, "dom"))
        self.assertIn("API_LIST_TRUNCATED", self.logged("warning"))
        self.assertIn("API_SHADOW_MATCH step=direct_flow_data", self.logged("info"))

    def test_shadow_returns_to_step_page_before_dom(self):
        self.set_mode("shadow")
        self.parser._handle_dom_extraction = Mock(return_value={"total_carriages": 89, "details": [{"identifier": "101", "value": 89}]})
        self.parser.api_capture.wait_for.side_effect = self.responses([101], [89])

        self.parser._extract_step_result(FLOW_STEP)

        # 1 переход на перевозку + возврат на страницу шага для чтения разметки
        self.assertEqual(self.parser.navigate_to_target.call_count, 2)


class TestConvertApiValue(unittest.TestCase):
    convert = staticmethod(TestConcreteReportParser._convert_api_value)

    def test_int_conversion(self):
        self.assertEqual(self.convert("89", {"convert_to": "int"}), 89)
        self.assertEqual(self.convert(0, {"convert_to": "int"}), 0)

    def test_bad_values_raise(self):
        for bad in (None, True, "abc"):
            with self.assertRaises(ApiCaptureError):
                self.convert(bad, {"convert_to": "int"})


if __name__ == "__main__":
    unittest.main()
