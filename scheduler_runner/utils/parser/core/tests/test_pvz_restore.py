"""
Возврат своего ПВЗ после пакета соседнего (этап D failover, RESTORE_PVZ_AFTER_BATCH).

Проверяют:
- OzonReportParser._after_batch_dates: без параметра / тот же ПВЗ — ничего; иначе навигация и ensure_correct_pvz
  для своего ПВЗ, location_id пакета восстанавливается, результат в pvz_restore_result
- BaseReportParser.run_parser_batch: хук до logout, результат в batch_result["pvz_restore"], при AUTH_REQUIRED хук не вызывается
- facade: restore_pvz попадает в конфиг parser-а
"""
import unittest
from unittest.mock import Mock, patch

from scheduler_runner.utils.parser import parser_invocation
from scheduler_runner.utils.parser.configs.base_configs.ozon_report_config import OZON_BASE_CONFIG
from scheduler_runner.utils.parser.core.base_report_parser import BaseReportParser
from scheduler_runner.utils.parser.core.tests.test_ozon_report_parser import TestOzonReportParserImpl


def make_parser(restore_pvz="ЧЕБОКСАРЫ_144", batch_pvz="ЧЕБОКСАРЫ_143"):
    config = OZON_BASE_CONFIG.copy()
    config["additional_params"] = {"location_id": batch_pvz}
    if restore_pvz:
        config["RESTORE_PVZ_AFTER_BATCH"] = restore_pvz
    parser = TestOzonReportParserImpl(config, logger=Mock())
    parser.dump_debug_artifacts = Mock()
    return parser


class TestAfterBatchDates(unittest.TestCase):
    def test_no_restore_param_does_nothing(self):
        parser = make_parser(restore_pvz=None)
        parser.ensure_correct_pvz = Mock()
        parser._after_batch_dates()
        parser.ensure_correct_pvz.assert_not_called()
        self.assertFalse(hasattr(parser, "pvz_restore_result"))

    def test_same_pvz_does_nothing(self):
        parser = make_parser(restore_pvz="ЧЕБОКСАРЫ_144", batch_pvz="ЧЕБОКСАРЫ_144")
        parser.ensure_correct_pvz = Mock()
        parser._after_batch_dates()
        parser.ensure_correct_pvz.assert_not_called()

    @patch.object(BaseReportParser, "navigate_to_target", return_value=True)
    def test_restores_own_pvz_and_keeps_batch_location(self, _navigate):
        parser = make_parser()
        seen = []
        parser.ensure_correct_pvz = Mock(side_effect=lambda: seen.append(
            parser.config["additional_params"]["location_id"]) or True)

        parser._after_batch_dates()

        self.assertEqual(seen, ["ЧЕБОКСАРЫ_144"])
        self.assertEqual(parser.config["additional_params"]["location_id"], "ЧЕБОКСАРЫ_143")
        self.assertEqual(parser.pvz_restore_result, {"pvz": "ЧЕБОКСАРЫ_144", "success": True})

    @patch.object(BaseReportParser, "navigate_to_target", return_value=True)
    def test_failed_restore_reported(self, _navigate):
        parser = make_parser()
        parser.ensure_correct_pvz = Mock(return_value=False)

        parser._after_batch_dates()

        self.assertEqual(parser.pvz_restore_result, {"pvz": "ЧЕБОКСАРЫ_144", "success": False})
        parser.dump_debug_artifacts.assert_called_once_with("pvz_restore_failed")
        self.assertIn("FAILOVER_PVZ_RESTORE_FAILED", parser.logger.error.call_args.args[0])


class TestRunParserBatchHook(unittest.TestCase):
    def make_batch_parser(self, side_effect):
        parser = make_parser()
        parser.setup_browser = Mock(return_value=True)
        parser.close_browser = Mock()
        parser._close_parser_session = Mock()
        parser._run_single_date_in_current_session = Mock(side_effect=side_effect)
        calls = []
        parser.logout = Mock(side_effect=lambda: calls.append("logout") or True)

        def after():
            calls.append("restore")
            parser.pvz_restore_result = {"pvz": "ЧЕБОКСАРЫ_144", "success": True}

        parser._after_batch_dates = Mock(side_effect=after)
        return parser, calls

    def test_hook_runs_before_logout_and_result_returned(self):
        parser, calls = self.make_batch_parser([{"execution_date": "2026-09-30"}])
        result = parser.run_parser_batch(["2026-09-30"], save_to_file=False)
        self.assertEqual(calls, ["restore", "logout"])
        self.assertEqual(result["pvz_restore"], {"pvz": "ЧЕБОКСАРЫ_144", "success": True})

    def test_hook_error_does_not_break_batch(self):
        parser, _calls = self.make_batch_parser([{"execution_date": "2026-09-30"}])
        parser._after_batch_dates = Mock(side_effect=RuntimeError("boom"))
        result = parser.run_parser_batch(["2026-09-30"], save_to_file=False)
        self.assertTrue(result["success"])
        self.assertNotIn("pvz_restore", result)

    def test_auth_required_skips_hook(self):
        parser, _calls = self.make_batch_parser(Exception("AUTH_REQUIRED: session revoked"))
        with self.assertRaises(Exception):
            parser.run_parser_batch(["2026-09-30"], save_to_file=False)
        parser._after_batch_dates.assert_not_called()


class TestFacadeRestorePvz(unittest.TestCase):
    @patch.object(parser_invocation, "execute_legacy_batch_once", return_value={"results_by_date": {}})
    def test_restore_pvz_in_parser_config(self, mock_batch):
        parser_invocation.invoke_parser_for_pvz(
            pvz_id="ЧЕБОКСАРЫ_143", execution_dates=["2026-09-30"], logger=Mock(), restore_pvz="ЧЕБОКСАРЫ_144")
        config = mock_batch.call_args.kwargs["parser_config"]
        self.assertEqual(config["RESTORE_PVZ_AFTER_BATCH"], "ЧЕБОКСАРЫ_144")
        self.assertEqual(config["additional_params"]["location_id"], "ЧЕБОКСАРЫ_143")

    @patch.object(parser_invocation, "execute_legacy_batch_once", return_value={"results_by_date": {}})
    def test_no_restore_by_default(self, mock_batch):
        parser_invocation.invoke_parser_for_pvz(pvz_id="ЧЕБОКСАРЫ_144", execution_dates=["2026-09-30"], logger=Mock())
        self.assertNotIn("RESTORE_PVZ_AFTER_BATCH", mock_batch.call_args.kwargs["parser_config"])


if __name__ == "__main__":
    unittest.main()
