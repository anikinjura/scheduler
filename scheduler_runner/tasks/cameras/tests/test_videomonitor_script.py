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


# --- Камера с отставанием и уведомления без повторов (144, 17–18.10.2026) ---

import os
from datetime import timedelta

NOW = datetime(2026, 10, 18, 17, 30, 0)


def write_file(folder: Path, name: str, at: datetime) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / name
    path.write_text("x", encoding="utf-8")
    os.utime(path, (at.timestamp(), at.timestamp()))


def test_lagging_xiaomi_is_not_missing_but_dead_one_is():
    root = make_temp_dir("vms_lag")
    try:
        # xiaomi_002 догружает 15-й час в 17:25 — пишет, но с отставанием; xiaomi_009 последний раз писала вчера
        write_file(root / "xiaomi_camera_videos" / "788b2aab4458" / "2026101815", "00M40S_1.mp4", NOW - timedelta(minutes=5))
        write_file(root / "xiaomi_camera_videos" / "dead00000001" / "2026101715", "00M40S_1.mp4", NOW - timedelta(hours=26))
        cameras = {"клиентская зона": [make_cam("788b2aab4458", "xiaomi_002"), make_cam("dead00000001", "xiaomi_009")]}
        missing = vms.find_missing_records(cameras, "local", root, {"default": root}, 1, 2, DummyLogger(), NOW)
        assert missing == ["xiaomi_009 (клиентская зона)"]
    finally:
        cleanup_dir(root)


def test_latest_write_time_descends_unv_day_and_hour():
    root = make_temp_dir("vms_unv_latest")
    try:
        write_file(root / "20261017" / "21", "a.jpg", NOW - timedelta(hours=20))
        write_file(root / "20261018" / "16", "b.jpg", NOW - timedelta(minutes=40))
        write_file(root / "20261018" / "09", "c.jpg", NOW - timedelta(hours=8))
        folder, written_at = vms.latest_write_time(root, 2)
        assert folder == "20261018/16" and written_at == (NOW - timedelta(minutes=40)).replace(microsecond=0)
        assert vms.latest_write_time(root / "nothing", 2) is None
    finally:
        cleanup_dir(root)


def test_plan_notifications_new_reminder_and_recovered():
    plan = vms.plan_notifications({}, ["xiaomi_002 (зона)"], NOW, 3)
    assert plan == {"missing": ["xiaomi_002 (зона)"], "reminder": False, "recovered": []}
    state = {"missing": {"xiaomi_002 (зона)": "2026-10-18 14:30:00"}, "last_alert_at": "2026-10-18 16:30:00"}
    assert vms.plan_notifications(state, ["xiaomi_002 (зона)"], NOW, 3)["missing"] == []          # через час — тишина
    later = vms.plan_notifications(state, ["xiaomi_002 (зона)"], NOW + timedelta(hours=2), 3)
    assert later["missing"] == ["xiaomi_002 (зона)"] and later["reminder"] is True
    both = vms.plan_notifications(state, ["xiaomi_002 (зона)", "unv_001 (склад)"], NOW, 3)
    assert both["missing"] == ["xiaomi_002 (зона)", "unv_001 (склад)"] and both["reminder"] is False
    back = vms.plan_notifications(state, [], NOW, 3)
    assert back == {"missing": [], "reminder": False, "recovered": ["xiaomi_002 (зона)"]}


def run_local_monitor(monkeypatch, root, state_file, when, sent):
    config = {
        "PVZ_ID": "ЧЕБОКСАРЫ_144",
        "CAMERAS": {"клиентская зона": [make_cam("u-miss", "xiaomi_002")]},
        "local": {"CHECK_DIR": str(root), "LOCAL_ROOTS": {"default": root}, "MAX_LOOKBACK_HOURS": 2,
                  "DETAILED_LOGS": False, "USER": "test", "TASK_NAME": "VideoMonitorScript_local",
                  "STATE_FILE": state_file, "REMIND_HOURS": 3},
    }
    monkeypatch.setattr(vms, "SCRIPT_CONFIG", config)
    monkeypatch.setattr(vms, "configure_logger", lambda **kwargs: DummyLogger())
    monkeypatch.setattr(vms, "send_telegram_notification", lambda message, main_logger=None: sent.append(message) or True)
    monkeypatch.setattr(sys, "argv", ["VideoMonitorScript.py", "--check_type", "local",
                                      "--current-time", when.strftime("%Y-%m-%d %H:%M:%S")])
    vms.main()


def test_main_hourly_checks_alert_once_remind_and_report_recovery(monkeypatch):
    root = make_temp_dir("vms_dedupe")
    try:
        state_file = root / "state.json"
        sent = []
        for hour in (9, 10, 11):
            run_local_monitor(monkeypatch, root, state_file, NOW.replace(hour=hour), sent)
        assert len(sent) == 1 and "xiaomi_002" in sent[0] and "по-прежнему" not in sent[0]
        run_local_monitor(monkeypatch, root, state_file, NOW.replace(hour=12), sent)
        assert len(sent) == 2 and "по-прежнему" in sent[1] and "(с 18.10 09:30)" in sent[1]
        write_file(root / "xiaomi_camera_videos" / "u-miss" / "2026101813", "00M40S_1.mp4", NOW.replace(hour=13))
        run_local_monitor(monkeypatch, root, state_file, NOW.replace(hour=13), sent)
        assert len(sent) == 3 and "снова есть записи" in sent[2]
        run_local_monitor(monkeypatch, root, state_file, NOW.replace(hour=14), sent)
        assert len(sent) == 3
    finally:
        cleanup_dir(root)
