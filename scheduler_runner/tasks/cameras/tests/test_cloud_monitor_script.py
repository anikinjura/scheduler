import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path

import scheduler_runner.tasks.cameras.CloudMonitorScript as cms

@pytest.fixture
def logger():
    return MagicMock()

def test_cloud_accessibility_success(tmp_path, logger):
    # tmp_path — временная директория pytest
    result, msg = cms.test_cloud_accessibility(tmp_path, logger)
    assert result is True
    assert msg == "Успешно"
    logger.info.assert_called_with("Проверка доступности облачного хранилища: Успешно")

def test_cloud_accessibility_no_dir(logger):
    fake_dir = Path("Z:/nonexistent_dir_12345")
    result, msg = cms.test_cloud_accessibility(fake_dir, logger)
    assert result is False
    assert "не существует" in msg
    logger.warning.assert_called()

def test_cloud_accessibility_not_a_dir(tmp_path, logger):
    file_path = tmp_path / "file.txt"
    file_path.write_text("test")
    result, msg = cms.test_cloud_accessibility(file_path, logger)
    assert result is False
    assert "не является директорией" in msg
    logger.error.assert_called()

@patch("scheduler_runner.tasks.cameras.CloudMonitorScript.send_telegram_message")
def test_send_notification_success(mock_send, logger):
    mock_send.return_value = (True, "ok")
    cms.SCRIPT_CONFIG["TOKEN"] = "token"
    cms.SCRIPT_CONFIG["CHAT_ID"] = "chat"
    result = cms.send_notification("msg", logger)
    assert result is True
    logger.info.assert_called_with("Уведомление успешно отправлено через Telegram")

@patch("scheduler_runner.tasks.cameras.CloudMonitorScript.send_telegram_message")
def test_send_notification_fail(mock_send, logger):
    mock_send.return_value = (False, "fail")
    cms.SCRIPT_CONFIG["TOKEN"] = "token"
    cms.SCRIPT_CONFIG["CHAT_ID"] = "chat"
    result = cms.send_notification("msg", logger)
    assert result is False
    logger.error.assert_called()

def test_send_notification_no_token(logger):
    cms.SCRIPT_CONFIG["TOKEN"] = ""
    cms.SCRIPT_CONFIG["CHAT_ID"] = ""
    result = cms.send_notification("msg", logger)
    assert result is False
    logger.warning.assert_called_with("Параметры Telegram не заданы, уведомление не отправлено")