"""
test_opening_monitor_script.py

Модуль тестов для OpeningMonitorScript.

Проверяются:
- `_parse_time_from_filename`: корректность разбора времени из разных форматов имен файлов.
- `find_earliest_file_time`: логика поиска самого раннего файла в заданном диапазоне.
- `main`: общая логика, включая формирование сообщений и вызов отправки в Telegram.
"""
import pytest
from unittest import mock
from pathlib import Path
from datetime import time, datetime
import os

# Добавляем корень проекта в sys.path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from scheduler_runner.tasks.cameras.OpeningMonitorScript import (
    _parse_time_from_filename,
    find_earliest_file_time,
    main as opening_monitor_main
)

# --- Тесты для _parse_time_from_filename ---

@pytest.mark.parametrize("filename, expected_time", [
    ("08-30-15.jpg", time(8, 30, 15)),
    ("09-01-02.jpg", time(9, 1, 2)),
    # 1751862000 -> 2025-07-09 09:00:00
    ("some_prefix_1751862000.mp4", time(9, 0, 0)),
    # 1751862065 -> 2025-07-09 09:01:05
    ("another_video_1751862065.mp4", time(9, 1, 5)),
])
def test_parse_time_from_filename_valid(filename, expected_time):
    """Проверяет корректный парсинг валидных имен файлов."""
    assert _parse_time_from_filename(filename) == expected_time

@pytest.mark.parametrize("filename", [
    "invalid-name.txt",
    "08_30_15.jpg",
    "video.mp4",
    "_12345.mp4", # Неверная длина timestamp
    "10-20-30.gif", # Неверное расширение
])
def test_parse_time_from_filename_invalid(filename):
    """Проверяет, что для невалидных имен возвращается None."""
    assert _parse_time_from_filename(filename) is None

# --- Тесты для find_earliest_file_time ---

@pytest.fixture
def create_files(tmp_path):
    """Фикстура для создания тестовых файлов."""
    def _creator(file_specs):
        for name, mtime_ts in file_specs:
            file_path = tmp_path / name
            file_path.touch()
            # Устанавливаем время модификации
            if mtime_ts:
                os.utime(file_path, (mtime_ts, mtime_ts))
    return _creator

def test_find_earliest_file_time_found(create_files, tmp_path):
    """Проверяет успешный поиск самого раннего файла."""
    today_ts = datetime.now().timestamp()
    yesterday_ts = today_ts - 86400
    
    files = [
        ("08-30-00.jpg", today_ts),      # Входит в диапазон
        ("08-15-00.jpg", today_ts),      # Самый ранний
        ("09-59-59.jpg", today_ts),      # Входит в диапазон
        ("10-00-01.jpg", today_ts),      # Не входит (позже)
        ("07-59-59.jpg", today_ts),      # Не входит (раньше)
        ("prefix_1751862000.mp4", today_ts), # 09:00, входит
        ("yesterday-file.jpg", yesterday_ts) # Не сегодняшний
    ]
    create_files(files)
    
    logger = mock.Mock()
    start_time = time(8, 0)
    end_time = time(10, 0)
    
    earliest = find_earliest_file_time(tmp_path, start_time, end_time, logger)
    assert earliest == time(8, 15, 0)

def test_find_earliest_file_time_not_found(create_files, tmp_path):
    """Проверяет случай, когда файлы в диапазоне не найдены."""
    today_ts = datetime.now().timestamp()
    files = [
        ("07-50-00.jpg", today_ts),
        ("10-10-10.jpg", today_ts),
    ]
    create_files(files)
    
    logger = mock.Mock()
    start_time = time(8, 0)
    end_time = time(10, 0)
    
    earliest = find_earliest_file_time(tmp_path, start_time, end_time, logger)
    assert earliest is None

# --- Тесты для main ---

@mock.patch('scheduler_runner.tasks.cameras.OpeningMonitorScript.send_telegram_message')
@mock.patch('scheduler_runner.tasks.cameras.OpeningMonitorScript.find_earliest_file_time')
@mock.patch('scheduler_runner.tasks.cameras.OpeningMonitorScript.configure_logger')
@mock.patch('scheduler_runner.tasks.cameras.OpeningMonitorScript.SCRIPT_CONFIG', {
    "SEARCH_DIR": "/fake/dir",
    "START_TIME": "08:00:00",
    "END_TIME": "10:00:00",
    "USER": "test",
    "TASK_NAME": "test",
    "TELEGRAM_TOKEN": "fake_token",
    "TELEGRAM_CHAT_ID": "fake_chat_id",
})
def test_main_sends_success_message(mock_config, mock_logger, mock_find_earliest, mock_send_telegram):
    """Проверяет отправку успешного сообщения, когда файл найден."""
    mock_find_earliest.return_value = time(8, 25, 10)
    
    opening_monitor_main()
    
    expected_message = "✅ Объект начал работу в 08:25:10."
    mock_send_telegram.assert_called_once_with(
        "fake_token", "fake_chat_id", expected_message, mock.ANY
    )

@mock.patch('scheduler_runner.tasks.cameras.OpeningMonitorScript.send_telegram_message')
@mock.patch('scheduler_runner.tasks.cameras.OpeningMonitorScript.find_earliest_file_time')
@mock.patch('scheduler_runner.tasks.cameras.OpeningMonitorScript.configure_logger')
@mock.patch('scheduler_runner.tasks.cameras.OpeningMonitorScript.SCRIPT_CONFIG', {
    "SEARCH_DIR": "/fake/dir",
    "START_TIME": "08:00:00",
    "END_TIME": "10:00:00",
    "USER": "test",
    "TASK_NAME": "test",
    "TELEGRAM_TOKEN": "fake_token",
    "TELEGRAM_CHAT_ID": "fake_chat_id",
})
def test_main_sends_failure_message(mock_config, mock_logger, mock_find_earliest, mock_send_telegram):
    """Проверяет отправку сообщения, когда файлы не найдены."""
    mock_find_earliest.return_value = None
    
    opening_monitor_main()
    
    expected_message = "⚠️ Объект не начал работу до 10:00. Видеофайлы не обнаружены."
    mock_send_telegram.assert_called_once_with(
        "fake_token", "fake_chat_id", expected_message, mock.ANY
    )
