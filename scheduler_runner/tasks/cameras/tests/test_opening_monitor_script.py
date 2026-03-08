import os
from datetime import datetime, time
from pathlib import Path
from unittest import mock

import pytest

import scheduler_runner.tasks.cameras.OpeningMonitorScript as oms
from scheduler_runner.tasks.cameras.tests._test_tmp_utils import cleanup_dir, make_temp_dir


@pytest.mark.parametrize(
    "filename, expected_time",
    [
        ("08-30-15.jpg", time(8, 30, 15)),
        ("09-01-02.jpg", time(9, 1, 2)),
        ("some_prefix_1751862000.mp4", datetime.fromtimestamp(1751862000).time()),
        ("another_video_1751862065.mp4", datetime.fromtimestamp(1751862065).time()),
    ],
)
def test_parse_time_from_filename_valid(filename, expected_time):
    assert oms._parse_time_from_filename(filename) == expected_time


@pytest.mark.parametrize("filename", ["invalid-name.txt", "08_30_15.jpg", "video.mp4", "_12345.mp4", "10-20-30.gif"])
def test_parse_time_from_filename_invalid(filename):
    assert oms._parse_time_from_filename(filename) is None


def test_find_earliest_file_time_found():
    tmp_path = make_temp_dir("opening_found")
    try:
        today_ts = datetime.now().timestamp()
        yesterday_ts = today_ts - 86400
        for name, mtime_ts in [
            ("08-30-00.jpg", today_ts),
            ("08-15-00.jpg", today_ts),
            ("09-59-59.jpg", today_ts),
            ("10-00-01.jpg", today_ts),
            ("07-59-59.jpg", today_ts),
            ("prefix_1751862000.mp4", today_ts),
            ("yesterday-file.jpg", yesterday_ts),
        ]:
            p = tmp_path / name
            p.touch()
            os.utime(p, (mtime_ts, mtime_ts))

        earliest = oms.find_earliest_file_time(tmp_path, time(8, 0), time(10, 0), mock.Mock())
        assert earliest == time(8, 15, 0)
    finally:
        cleanup_dir(tmp_path)


def test_find_earliest_file_time_not_found():
    tmp_path = make_temp_dir("opening_not_found")
    try:
        today_ts = datetime.now().timestamp()
        for name in ["07-50-00.jpg", "10-10-10.jpg"]:
            p = tmp_path / name
            p.touch()
            os.utime(p, (today_ts, today_ts))

        earliest = oms.find_earliest_file_time(tmp_path, time(8, 0), time(10, 0), mock.Mock())
        assert earliest is None
    finally:
        cleanup_dir(tmp_path)


def test_main_uses_search_dirs_and_sends_success(monkeypatch):
    test_config = {
        "PVZ_ID": "TEST",
        "SEARCH_DIR": "/fake/dir",
        "SEARCH_DIRS": ["/fake/dir1", "/fake/dir2"],
        "START_TIME": "08:00:00",
        "END_TIME": "10:00:00",
        "USER": "test",
        "TASK_NAME": "test",
        "DETAILED_LOGS": False,
        "TELEGRAM_TOKEN": "fake_token",
        "TELEGRAM_CHAT_ID": "fake_chat_id",
        "COMBINED_ANALYSIS_ENABLED": False,
    }

    monkeypatch.setattr(oms, "SCRIPT_CONFIG", test_config)
    monkeypatch.setattr(oms, "configure_logger", lambda **kwargs: mock.Mock())
    monkeypatch.setattr(oms, "test_notification_connection", lambda *a, **k: {"success": True})
    monkeypatch.setattr(oms, "send_telegram_notification", lambda *a, **k: True)

    calls = {"n": 0}

    def fake_find(search_dir, start_t, end_t, logger):
        calls["n"] += 1
        return time(8, 25, 10) if "dir2" in str(search_dir) else None

    monkeypatch.setattr(oms, "find_earliest_file_time", fake_find)
    monkeypatch.setattr("sys.argv", ["OpeningMonitorScript.py"])

    oms.main()
    assert calls["n"] == 2


def test_main_sends_failure_message_when_no_files(monkeypatch):
    test_config = {
        "PVZ_ID": "TEST",
        "SEARCH_DIR": "/fake/dir",
        "SEARCH_DIRS": ["/fake/dir1"],
        "START_TIME": "08:00:00",
        "END_TIME": "10:00:00",
        "USER": "test",
        "TASK_NAME": "test",
        "DETAILED_LOGS": False,
        "TELEGRAM_TOKEN": "fake_token",
        "TELEGRAM_CHAT_ID": "fake_chat_id",
        "COMBINED_ANALYSIS_ENABLED": False,
    }

    monkeypatch.setattr(oms, "SCRIPT_CONFIG", test_config)
    monkeypatch.setattr(oms, "configure_logger", lambda **kwargs: mock.Mock())
    monkeypatch.setattr(oms, "test_notification_connection", lambda *a, **k: {"success": True})
    monkeypatch.setattr(oms, "find_earliest_file_time", lambda *a, **k: None)

    captured = {}

    def fake_send(message, main_logger=None):
        captured["message"] = message
        return True

    monkeypatch.setattr(oms, "send_telegram_notification", fake_send)
    monkeypatch.setattr("sys.argv", ["OpeningMonitorScript.py"])

    oms.main()
    assert "не начал работу" in captured["message"]
