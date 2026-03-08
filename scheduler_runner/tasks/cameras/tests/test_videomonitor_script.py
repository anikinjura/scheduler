import sys
from datetime import datetime
from pathlib import Path

import pytest

import scheduler_runner.tasks.cameras.VideoMonitorScript as vms
from scheduler_runner.tasks.cameras.tests._test_tmp_utils import cleanup_dir, make_temp_dir


class DummyLogger:
    def info(self, *a, **k):
        pass

    def warning(self, *a, **k):
        pass

    def error(self, *a, **k):
        pass

    def debug(self, *a, **k):
        pass


def make_cam(uid, cam_id, root_key=None):
    cam = {"uid": uid, "id": cam_id}
    if root_key:
        cam["root_key"] = root_key
    return cam


def test_has_recent_records_found():
    root = make_temp_dir("vms_found")
    try:
        now = datetime.now()
        uid = "cam1"
        rec_dir = root / "unv_camera" / uid / now.strftime("%Y%m%d") / now.strftime("%H")
        rec_dir.mkdir(parents=True)
        (rec_dir / "file1.mp4").write_text("data", encoding="utf-8")

        assert vms.has_recent_records(
            root,
            uid,
            min_files=1,
            max_lookback_hours=2,
            path_builder=vms.unv_path_builder,
            logger=DummyLogger(),
            camera_type="UNV",
        )
    finally:
        cleanup_dir(root)


def test_has_recent_records_not_found():
    root = make_temp_dir("vms_not_found")
    try:
        assert not vms.has_recent_records(
            root,
            "cam2",
            min_files=1,
            max_lookback_hours=2,
            path_builder=vms.unv_path_builder,
            logger=DummyLogger(),
            camera_type="UNV",
        )
    finally:
        cleanup_dir(root)


def test_main_uses_root_key_for_local(monkeypatch):
    base = make_temp_dir("vms_root_key")
    try:
        root1 = base / "root1"
        root2 = base / "root2"
        root1.mkdir()
        root2.mkdir()

        now = datetime.now()
        uid = "u1"
        rec_dir = root2 / "unv_camera" / uid / now.strftime("%Y%m%d") / now.strftime("%H")
        rec_dir.mkdir(parents=True)
        (rec_dir / "ok.jpg").write_text("x", encoding="utf-8")

        test_config = {
            "PVZ_ID": "TEST",
            "CAMERAS": {"zone": [make_cam(uid, "unv_001", root_key="local_2")]},
            "TOKEN": "t",
            "CHAT_ID": "c",
            "local": {
                "CHECK_DIR": str(root1),
                "LOCAL_ROOTS": {"local_1": root1, "local_2": root2},
                "MAX_LOOKBACK_HOURS": 2,
                "DETAILED_LOGS": False,
                "USER": "test",
                "TASK_NAME": "VideoMonitorScript_local",
            },
        }
        monkeypatch.setattr(vms, "SCRIPT_CONFIG", test_config)
        monkeypatch.setattr(vms, "configure_logger", lambda **kwargs: DummyLogger())

        sent = {"called": False}

        def fake_send(message, main_logger=None):
            sent["called"] = True
            return True

        monkeypatch.setattr(vms, "send_telegram_notification", fake_send)
        monkeypatch.setattr(sys, "argv", ["VideoMonitorScript.py", "--check_type", "local"])

        vms.main()
        assert sent["called"] is False
    finally:
        cleanup_dir(base)


def test_main_missing_records_sends_notification(monkeypatch):
    root = make_temp_dir("vms_missing")
    try:
        test_config = {
            "PVZ_ID": "TEST",
            "CAMERAS": {"zone": [make_cam("u-miss", "unv_002")]},
            "TOKEN": "t",
            "CHAT_ID": "c",
            "local": {
                "CHECK_DIR": str(root),
                "LOCAL_ROOTS": {"default": root},
                "MAX_LOOKBACK_HOURS": 2,
                "DETAILED_LOGS": False,
                "USER": "test",
                "TASK_NAME": "VideoMonitorScript_local",
            },
        }
        monkeypatch.setattr(vms, "SCRIPT_CONFIG", test_config)
        monkeypatch.setattr(vms, "configure_logger", lambda **kwargs: DummyLogger())

        sent = {}

        def fake_send(message, main_logger=None):
            sent["message"] = message
            return True

        monkeypatch.setattr(vms, "send_telegram_notification", fake_send)
        monkeypatch.setattr(sys, "argv", ["VideoMonitorScript.py", "--check_type", "local"])

        vms.main()
        assert "unv_002" in sent["message"]
    finally:
        cleanup_dir(root)


def test_main_no_cameras(monkeypatch):
    test_config = {
        "CAMERAS": {},
        "TOKEN": "t",
        "CHAT_ID": "c",
        "local": {
            "CHECK_DIR": ".",
            "MAX_LOOKBACK_HOURS": 2,
            "DETAILED_LOGS": False,
            "USER": "test",
            "TASK_NAME": "VideoMonitorScript_local",
        },
    }
    monkeypatch.setattr(vms, "SCRIPT_CONFIG", test_config)
    monkeypatch.setattr(vms, "configure_logger", lambda **kwargs: DummyLogger())
    monkeypatch.setattr(sys, "argv", ["VideoMonitorScript.py", "--check_type", "local"])

    with pytest.raises(SystemExit) as e:
        vms.main()
    assert e.value.code == 1
