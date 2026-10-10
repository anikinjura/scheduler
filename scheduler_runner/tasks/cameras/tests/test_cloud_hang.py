"""
Зависшее облачное хранилище («Диск-О:» перестал отвечать): мониторинг и копирование должны уведомить, а не молча
упереться в таймаут runner (144, 05–10.10.2026). Без сети и без реального облака: зависание — sleep в подмененной
проверке.
"""
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest

import scheduler_runner.tasks.cameras.CloudMonitorScript as cms
import scheduler_runner.tasks.cameras.CopyScript as copy_script
import scheduler_runner.tasks.cameras.VideoMonitorScript as vms
from scheduler_runner.tasks.cameras.tests._test_tmp_utils import cleanup_dir, make_temp_dir
from scheduler_runner.utils.filesystem import call_with_timeout

NOW = datetime(2026, 10, 10, 10, 30, 0)


class DummyLogger:
    def __getattr__(self, name):
        return lambda *a, **k: None


def hang(*args, **kwargs):
    time.sleep(3)


# --- call_with_timeout ---

def test_call_with_timeout_returns_result_raises_errors_and_gives_up_on_hang():
    assert call_with_timeout(lambda x: x * 2, 1, 21) == (True, 42)
    with pytest.raises(ValueError):
        call_with_timeout(lambda: (_ for _ in ()).throw(ValueError("boom")), 1)
    started = time.monotonic()
    assert call_with_timeout(hang, 0.2) == (False, None)
    assert time.monotonic() - started < 2


# --- CloudMonitorScript ---

def test_check_cloud_reports_hung_storage(monkeypatch):
    monkeypatch.setattr(cms, "test_cloud_accessibility", hang)
    status, message = cms.check_cloud(Path("O:/cameras/X"), DummyLogger(), 0.2)
    assert status == cms.STATUS_HUNG and "не отвечает" in message


def test_plan_notification_first_failure_reminder_and_recovery():
    assert cms.plan_notification({}, False, NOW, 3) == "failure"
    assert cms.plan_notification({}, True, NOW, 3) is None
    failing = {"failing_since": "2026-10-10 09:30:00", "last_alert_at": "2026-10-10 09:30:00"}
    assert cms.plan_notification(failing, False, NOW, 3) is None                       # через час — тишина
    assert cms.plan_notification(failing, False, NOW + timedelta(hours=2), 3) == "reminder"
    assert cms.plan_notification(failing, True, NOW, 3) == "recovery"
    # прошлое уведомление не ушло (нет сети) — пробуем снова, а не молчим
    assert cms.plan_notification({"failing_since": "2026-10-10 09:30:00"}, False, NOW, 3) == "failure"


def run_cloud_monitor(monkeypatch, state_file, accessible, sent):
    config = dict(cms.SCRIPT_CONFIG)
    config.update({"PVZ_ID": "ЧЕБОКСАРЫ_144", "CHECK_DIR": "O:/cameras/ChEBOKSARY_144", "PROBE_TIMEOUT_SECONDS": 0.2,
                   "STATE_FILE": state_file, "REMIND_HOURS": 3})
    monkeypatch.setattr(cms, "SCRIPT_CONFIG", config)
    monkeypatch.setattr(cms, "configure_logger", lambda **kwargs: DummyLogger())
    monkeypatch.setattr(cms, "test_cloud_accessibility", (lambda d, l: (True, "Успешно")) if accessible else hang)
    monkeypatch.setattr(cms, "send_telegram_notification", lambda message, logger=None: sent.append(message) or True)
    monkeypatch.setattr(sys, "argv", ["CloudMonitorScript.py", "--retries", "3", "--delay", "0"])
    with pytest.raises(SystemExit) as exit_info:
        cms.main()
    return exit_info.value.code


def test_cloud_monitor_hung_storage_alerts_once_then_recovers(monkeypatch):
    root = make_temp_dir("cloud_hang")
    try:
        state_file = root / "cloud_monitor_state.json"
        sent = []
        started = time.monotonic()
        assert run_cloud_monitor(monkeypatch, state_file, False, sent) == 1
        assert time.monotonic() - started < 2           # зависание не повторяется 3 раза
        assert len(sent) == 1 and "ЧЕБОКСАРЫ_144" in sent[0] and "зависло" in sent[0] and "Диск-О" in sent[0]

        assert run_cloud_monitor(monkeypatch, state_file, False, sent) == 1
        assert len(sent) == 1                           # через час — без повтора

        assert run_cloud_monitor(monkeypatch, state_file, True, sent) == 0
        assert len(sent) == 2 and "снова доступно" in sent[1]
        assert run_cloud_monitor(monkeypatch, state_file, True, sent) == 0
        assert len(sent) == 2
    finally:
        cleanup_dir(root)


# --- VideoMonitorScript ---

def test_videomonitor_network_hung_storage_sends_notification(monkeypatch):
    config = {
        "PVZ_ID": "ЧЕБОКСАРЫ_144",
        "CAMERAS": {"zone": [{"uid": "u1", "id": "unv_001"}]},
        "network": {"CHECK_DIR": "O:/cameras/ChEBOKSARY_144", "MAX_LOOKBACK_HOURS": 24, "SCAN_TIMEOUT_SECONDS": 0.2,
                    "DETAILED_LOGS": False, "USER": "test", "TASK_NAME": "VideoMonitorScript_network"},
    }
    sent = []
    monkeypatch.setattr(vms, "SCRIPT_CONFIG", config)
    monkeypatch.setattr(vms, "configure_logger", lambda **kwargs: DummyLogger())
    monkeypatch.setattr(vms, "find_missing_records", hang)
    monkeypatch.setattr(vms, "send_telegram_notification", lambda message, main_logger=None: sent.append(message) or True)
    monkeypatch.setattr(sys, "argv", ["VideoMonitorScript.py", "--check_type", "network"])
    with pytest.raises(SystemExit) as exit_info:
        vms.main()
    assert exit_info.value.code == 1
    assert len(sent) == 1 and "в облаке" in sent[0] and "не отвечает" in sent[0]


# --- CopyScript ---

def test_copy_hung_destination_notifies_and_still_shuts_down(monkeypatch):
    root = make_temp_dir("copy_hang")
    try:
        src = root / "src"
        src.mkdir()
        config = dict(copy_script.SCRIPT_CONFIG)
        config.update({"PVZ_ID": "ЧЕБОКСАРЫ_144", "INPUT_DIRS": [str(src)], "OUTPUT_DIR": "O:/cameras/ChEBOKSARY_144",
                       "DEST_CHECK_TIMEOUT_SECONDS": 0.2, "SHUTDOWN_ENABLED": True, "SHUTDOWN_ON_FAILURE": True})
        monkeypatch.setattr(copy_script, "SCRIPT_CONFIG", config)
        monkeypatch.setattr(copy_script, "configure_logger", lambda **kwargs: DummyLogger())
        monkeypatch.setattr(copy_script.FileSystemUtils, "validate_writable_path", staticmethod(hang))
        reasons, shutdowns = [], []
        monkeypatch.setattr(copy_script, "send_failure_notification", lambda reason, logger: reasons.append(reason) or True)
        monkeypatch.setattr(copy_script.SystemUtils, "shutdown_computer",
                            staticmethod(lambda logger=None, force=False: shutdowns.append(force) or True))
        monkeypatch.setattr(sys, "argv", ["CopyScript.py", "--shutdown", "1", "--no-shutdown-delay"])
        with pytest.raises(SystemExit) as exit_info:
            copy_script.main()
        assert exit_info.value.code == 1
        assert len(reasons) == 1 and "не отвечает" in reasons[0]
        assert shutdowns == [False]
    finally:
        cleanup_dir(root)


def test_copy_failure_without_shutdown_flag_does_not_shut_down(monkeypatch):
    reasons, shutdowns = [], []
    monkeypatch.setattr(copy_script, "configure_logger", lambda **kwargs: DummyLogger())
    monkeypatch.setattr(copy_script, "send_failure_notification", lambda reason, logger: reasons.append(reason) or True)
    monkeypatch.setattr(copy_script.SystemUtils, "shutdown_computer",
                        staticmethod(lambda logger=None, force=False: shutdowns.append(force) or True))
    monkeypatch.setattr(sys, "argv", ["CopyScript.py", "--source_dir", "Z:/not_exists", "--dest_dir", "Z:/not_exists2"])
    with pytest.raises(SystemExit) as exit_info:
        copy_script.main()
    assert exit_info.value.code == 1
    assert len(reasons) == 1 and "исходная директория" in reasons[0]
    assert shutdowns == []


def test_copy_partial_errors_notify_exit_1_and_shut_down_once(monkeypatch):
    # 144, 10.10.2026: «Диск-О:» отключился посреди копирования — 12139 из 14715 файлов с ошибкой, задача «успешна»
    root = make_temp_dir("copy_partial")
    try:
        src, dst = root / "src", root / "dst"
        src.mkdir()
        dst.mkdir()
        config = dict(copy_script.SCRIPT_CONFIG)
        config.update({"PVZ_ID": "ЧЕБОКСАРЫ_144", "INPUT_DIRS": [str(src)], "OUTPUT_DIR": str(dst),
                       "SHUTDOWN_ENABLED": True, "SHUTDOWN_ON_FAILURE": True})
        monkeypatch.setattr(copy_script, "SCRIPT_CONFIG", config)
        monkeypatch.setattr(copy_script, "configure_logger", lambda **kwargs: DummyLogger())
        monkeypatch.setattr(copy_script, "count_files_within_days", lambda directory, days: 14715)
        monkeypatch.setattr(copy_script, "copy_recent_files", lambda **kwargs: {
            "CopiedFiles": 2576, "SkippedFiles": 0, "FileCopyingErrors": 12139})
        reasons, shutdowns = [], []
        monkeypatch.setattr(copy_script, "send_failure_notification", lambda reason, logger: reasons.append(reason) or True)
        monkeypatch.setattr(copy_script.SystemUtils, "shutdown_computer",
                            staticmethod(lambda logger=None, force=False: shutdowns.append(force) or True))
        monkeypatch.setattr(sys, "argv", ["CopyScript.py", "--shutdown", "1", "--no-shutdown-delay"])
        with pytest.raises(SystemExit) as exit_info:
            copy_script.main()
        assert exit_info.value.code == 1
        assert len(reasons) == 1 and "12139 из 14715" in reasons[0]
        assert shutdowns == [False]
    finally:
        cleanup_dir(root)
