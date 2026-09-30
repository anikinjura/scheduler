"""Тесты перехвата ответов API (без браузера: драйвер — mock с execute_script)."""
import json
import unittest
from unittest.mock import MagicMock

from scheduler_runner.utils.parser.core.api_response_capture import (
    ApiFieldNotFound,
    ApiResponseCapture,
    ApiResponseError,
    ApiResponseNotFound,
    resolve_path,
)

GIVEOUT = "https://turbo-pvz.ozon.ru/api2/reports/give_out/logV2?startDate=2026-09-28T00:00%2B03:00&operationTypes=GiveoutAll&take=50"
ARTICLES = "https://turbo-pvz.ozon.ru/api2/reports/CarriageReport/GetCarriageArticles?carriageId={cid}&articleState=Took"


def rec(url, body, status=200):
    return {"url": url, "method": "GET", "status": status, "body": json.dumps(body) if not isinstance(body, str) else body}


def driver_with(*snapshots):
    """execute_script возвращает записи по очереди (имитация появления ответов со временем)."""
    driver = MagicMock()
    driver.execute_script.side_effect = list(snapshots) + [snapshots[-1]] * 100
    return driver


class TestWaitFor(unittest.TestCase):
    def setUp(self):
        self.capture = ApiResponseCapture(wait_timeout=0.2, poll_interval=0.01)

    def test_returns_json_of_matching_request(self):
        driver = driver_with([rec(GIVEOUT, {"logV2": {"totalCount": 508}})])
        data = self.capture.wait_for(driver, "/api2/reports/give_out/logV2", {"operationTypes": "GiveoutAll"})
        self.assertEqual(resolve_path(data, "logV2.totalCount"), 508)

    def test_query_filter_selects_right_carriage(self):
        driver = driver_with([rec(ARTICLES.format(cid=1), {"totalCount": 89}), rec(ARTICLES.format(cid=2), {"totalCount": 24})])
        data = self.capture.wait_for(driver, "/GetCarriageArticles", {"carriageId": "2", "articleState": "Took"})
        self.assertEqual(data["totalCount"], 24)

    def test_date_with_plus_is_matched_after_url_decoding(self):
        driver = driver_with([rec(GIVEOUT, {"logV2": {"totalCount": 1}})])
        data = self.capture.wait_for(driver, "/give_out/logV2", {"startDate": "2026-09-28T00:00+03:00"})
        self.assertEqual(data["logV2"]["totalCount"], 1)

    def test_waits_until_request_completes(self):
        pending = {"url": GIVEOUT, "method": "GET", "status": None, "body": None}
        driver = driver_with([], [pending], [rec(GIVEOUT, {"logV2": {"totalCount": 7}})])
        data = self.capture.wait_for(driver, "/give_out/logV2")
        self.assertEqual(data["logV2"]["totalCount"], 7)

    def test_latest_matching_response_wins(self):
        driver = driver_with([rec(GIVEOUT, {"logV2": {"totalCount": 1}}), rec(GIVEOUT, {"logV2": {"totalCount": 2}})])
        self.assertEqual(self.capture.wait_for(driver, "/give_out/logV2")["logV2"]["totalCount"], 2)

    def test_not_found_raises(self):
        driver = driver_with([rec(ARTICLES.format(cid=1), {"totalCount": 1})])
        with self.assertRaises(ApiResponseNotFound):
            self.capture.wait_for(driver, "/give_out/logV2")

    def test_wrong_query_value_is_not_a_match(self):
        driver = driver_with([rec(ARTICLES.format(cid=1), {"totalCount": 1})])
        with self.assertRaises(ApiResponseNotFound):
            self.capture.wait_for(driver, "/GetCarriageArticles", {"carriageId": "999"})

    def test_error_status_raises(self):
        driver = driver_with([rec(GIVEOUT, {"error": "x"}, status=401)])
        with self.assertRaises(ApiResponseError):
            self.capture.wait_for(driver, "/give_out/logV2")

    def test_non_json_body_raises(self):
        driver = driver_with([rec(GIVEOUT, "<html>", status=200)])
        with self.assertRaises(ApiResponseError):
            self.capture.wait_for(driver, "/give_out/logV2")


class TestResolvePath(unittest.TestCase):
    DATA = {"logV2": {"totalCount": 508}, "store": {"name": "ЧЕБОКСАРЫ_144"},
            "carriages": [{"carriageId": 1, "info": {"n": "a"}}, {"carriageId": 2, "info": {"n": "b"}}]}

    def test_nested_value(self):
        self.assertEqual(resolve_path(self.DATA, "logV2.totalCount"), 508)
        self.assertEqual(resolve_path(self.DATA, "store.name"), "ЧЕБОКСАРЫ_144")

    def test_list_projection(self):
        self.assertEqual(resolve_path(self.DATA, "carriages[].carriageId"), [1, 2])
        self.assertEqual(resolve_path(self.DATA, "carriages[].info.n"), ["a", "b"])

    def test_zero_is_a_value_not_missing(self):
        self.assertEqual(resolve_path({"totalCount": 0}, "totalCount"), 0)

    def test_missing_field_raises(self):
        with self.assertRaises(ApiFieldNotFound):
            resolve_path(self.DATA, "logV2.total")
        with self.assertRaises(ApiFieldNotFound):
            resolve_path(self.DATA, "items[].id")


class TestInstall(unittest.TestCase):
    def test_hook_registered_before_documents_with_limits(self):
        driver = MagicMock()
        capture = ApiResponseCapture(max_records=10, max_body_chars=123)
        capture.install(driver)
        command, params = driver.execute_cdp_cmd.call_args.args
        self.assertEqual(command, "Page.addScriptToEvaluateOnNewDocument")
        self.assertIn("MAX_BODY = 123, MAX_RECORDS = 10", params["source"])
        self.assertNotIn("setRequestHeader", params["source"])  # заголовки не перехватываются


if __name__ == "__main__":
    unittest.main()
