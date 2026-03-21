from pathlib import Path
from unittest.mock import MagicMock, patch

import scheduler_runner.tasks.cameras.CloudMonitorScript as cms
from scheduler_runner.tasks.cameras.tests._test_tmp_utils import cleanup_dir, make_temp_dir


def test_cloud_accessibility_success():
    logger = MagicMock()
    td = make_temp_dir("cloud_ok")
    try:
        result, msg = cms.test_cloud_accessibility(Path(td), logger)
    finally:
        cleanup_dir(td)
    assert result is True
    assert msg == "Успешно"


def test_cloud_accessibility_no_dir():
    logger = MagicMock()
    fake_dir = Path("Z:/nonexistent_dir_12345")
    result, msg = cms.test_cloud_accessibility(fake_dir, logger)
    assert result is False
    assert "не существует" in msg


def test_cloud_accessibility_not_a_dir():
    logger = MagicMock()
    td = make_temp_dir("cloud_notdir")
    try:
        file_path = Path(td) / "file.txt"
        file_path.write_text("test", encoding="utf-8")
        result, msg = cms.test_cloud_accessibility(file_path, logger)
    finally:
        cleanup_dir(td)
    assert result is False
    assert "не является директорией" in msg


@patch("scheduler_runner.tasks.cameras.CloudMonitorScript.send_notification")
def test_send_telegram_notification_success(mock_send_notification):
    cms.SCRIPT_CONFIG["NOTIFICATION_CONNECTION_PARAMS"] = {
        "NOTIFICATION_PROVIDER": "telegram",
        "TELEGRAM_BOT_TOKEN": "token",
        "TELEGRAM_CHAT_ID": "chat",
    }
    mock_send_notification.return_value = {"success": True}

    assert cms.send_telegram_notification("msg", main_logger=MagicMock()) is True


@patch("scheduler_runner.tasks.cameras.CloudMonitorScript.send_notification")
def test_send_telegram_notification_connection_fail(mock_send_notification):
    cms.SCRIPT_CONFIG["NOTIFICATION_CONNECTION_PARAMS"] = {
        "NOTIFICATION_PROVIDER": "telegram",
        "TELEGRAM_BOT_TOKEN": "token",
        "TELEGRAM_CHAT_ID": "chat",
    }
    mock_send_notification.return_value = {"success": False, "error": "bad connection"}

    assert cms.send_telegram_notification("msg", main_logger=MagicMock()) is False


def test_send_telegram_notification_no_token():
    cms.SCRIPT_CONFIG["NOTIFICATION_CONNECTION_PARAMS"] = {}
    assert cms.send_telegram_notification("msg", main_logger=MagicMock()) is False
