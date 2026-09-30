"""
Тесты: неверные данные не попадают в результат молча.

- пустой обязательный счетчик в разметке — ошибка шага, а не 0; «Найдено: 0» остается нулем;
- strict-вложенная обработка: недоступная страница перевозки или ненайденная таблица — ошибка, пустая таблица — 0;
- дата с ошибкой шага при FAIL_DATE_ON_STEP_ERROR — ошибка даты (не выгружается), без флага — частичный результат.
"""
import unittest
from copy import deepcopy
from unittest.mock import MagicMock, Mock, patch

from scheduler_runner.utils.parser.configs.base_configs.base_report_config import BASE_REPORT_CONFIG
from scheduler_runner.utils.parser.core.base_report_parser import DomValueNotFound
from scheduler_runner.utils.parser.core.tests.test_base_report_parser import TestConcreteReportParser

COUNTER = {"selector": "//div", "pattern": r"Найдено:\s*(\d+)", "element_type": "div", "required": True,
           "post_processing": {"convert_to": "int", "default_value": 0}}

NESTED_STEP = {
    "processing_type": "table_nested",
    "result_key": "direct_flow_data",
    "table_processing": {"enabled": True, "table_config_key": "carriages_table", "id_column": "carriage_number"},
    "nested_processing": {
        "enabled": True, "strict": True,
        "base_url_template": "https://x/carriages-archive/{carriage_id}",
        "data_extraction": COUNTER,
        "aggregation": {"method": "sum", "target_field": "total_carriages"},
    },
}


class StrictTestCase(unittest.TestCase):
    def setUp(self):
        config = deepcopy(BASE_REPORT_CONFIG)
        config["execution_date"] = "2026-09-28"
        self.parser = TestConcreteReportParser(config, logger=MagicMock())
        self.parser.driver = Mock()
        self.parser.navigate_to_target = Mock(return_value=True)


class TestRequiredCounter(StrictTestCase):
    def test_empty_counter_is_error_not_zero(self):
        self.parser.get_element_value = Mock(return_value="")
        with self.assertRaises(DomValueNotFound):
            self.parser._extract_value_by_config(COUNTER)

    def test_real_zero_stays_zero(self):
        self.parser.get_element_value = Mock(return_value="0")
        self.assertEqual(self.parser._extract_value_by_config(COUNTER), 0)

    def test_simple_step_reports_error_instead_of_zero(self):
        self.parser.get_element_value = Mock(return_value=None)
        result = self.parser._handle_simple_extraction({"data_extraction": COUNTER})
        self.assertIn("DOM_VALUE_NOT_FOUND", result["error"])

    def test_not_required_keeps_old_default(self):
        self.parser.get_element_value = Mock(return_value="")
        counter = {k: v for k, v in COUNTER.items() if k != "required"}
        self.assertEqual(self.parser._extract_value_by_config(counter), 0)


class TestStrictNested(StrictTestCase):
    def test_empty_table_is_real_zero(self):
        self.parser.extract_table_data = Mock(return_value=[])
        result = self.parser._handle_table_nested_extraction(NESTED_STEP)
        self.assertEqual(result["total_carriages"], 0)

    def test_missing_table_is_error(self):
        self.parser.extract_table_data = Mock(side_effect=Exception("таблица не найдена"))
        with self.assertRaises(Exception) as ctx:
            self.parser._handle_table_nested_extraction(NESTED_STEP)
        self.assertIn("DOM_TABLE_ERROR", str(ctx.exception))

    def test_failed_carriage_page_is_error_not_skip(self):
        self.parser.extract_table_data = Mock(return_value=[{"carriage_number": "101"}, {"carriage_number": "202"}])
        self.parser.navigate_to_target = Mock(side_effect=[True, False])
        self.parser.get_element_value = Mock(return_value="89")
        with self.assertRaises(Exception) as ctx:
            self.parser._handle_table_nested_extraction(NESTED_STEP)
        self.assertIn("DOM_NESTED_PAGE_FAILED", str(ctx.exception))

    def test_empty_carriage_counter_is_error(self):
        self.parser.extract_table_data = Mock(return_value=[{"carriage_number": "101"}])
        self.parser.get_element_value = Mock(return_value="")
        with self.assertRaises(DomValueNotFound):
            self.parser._handle_table_nested_extraction(NESTED_STEP)

    def test_non_strict_keeps_skipping(self):
        step = deepcopy(NESTED_STEP)
        step["nested_processing"]["strict"] = False
        self.parser.extract_table_data = Mock(return_value=[{"carriage_number": "101"}, {"carriage_number": "202"}])
        self.parser.navigate_to_target = Mock(side_effect=[True, False])
        self.parser.get_element_value = Mock(return_value="89")
        result = self.parser._handle_table_nested_extraction(step)
        self.assertEqual(result["total_carriages"], 89)

    def test_login_redirect_in_nested_is_auth_error(self):
        step = deepcopy(NESTED_STEP)
        step["nested_processing"]["strict"] = False
        self.parser.extract_table_data = Mock(return_value=[{"carriage_number": "101"}])

        def fail_with_login():
            self.parser.config["_last_navigation_failure_reason"] = "login_redirect"
            return False
        self.parser.navigate_to_target = Mock(side_effect=fail_with_login)
        with self.assertRaises(Exception) as ctx:
            self.parser._handle_table_nested_extraction(step)
        self.assertTrue(str(ctx.exception).startswith("AUTH_REQUIRED:"))


class TestPartialDate(StrictTestCase):
    PARTIAL = {"summary": {"giveout": {"error": "DOM_VALUE_NOT_FOUND"}, "direct_flow_total": {"total_carriages": 89}},
               "__RUN_STATUS__": "partial"}

    def run_date(self):
        self.parser.config["multi_step_config"] = {"steps": ["giveout"]}
        with patch.object(self.parser, "_execute_multi_step_processing", return_value=dict(self.PARTIAL)):
            return self.parser._run_single_date_in_current_session(execution_date="2026-09-28", save_to_file=False)

    def test_partial_date_rejected_when_flag_set(self):
        self.parser.config["FAIL_DATE_ON_STEP_ERROR"] = True
        with self.assertRaises(Exception) as ctx:
            self.run_date()
        self.assertIn("PARTIAL_DATE_REJECTED", str(ctx.exception))
        self.assertIn("giveout", str(ctx.exception))

    def test_partial_date_returned_without_flag(self):
        self.parser.config["FAIL_DATE_ON_STEP_ERROR"] = False
        self.assertEqual(self.run_date()["__RUN_STATUS__"], "partial")


if __name__ == "__main__":
    unittest.main()
