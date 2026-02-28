"""
Тесты для модуля timing.py.

Проверка функциональности:
    - should_run_now() для hourly с time_window и без
    - should_run_now() для daily
    - _is_within_time_window()
    - _parse_time_window()

Author: anikinjura
"""
__version__ = '0.0.1'

import pytest
from datetime import datetime
from scheduler_runner.utils.timing import (
    should_run_now,
    _is_within_time_window,
    _parse_time_window,
)


class TestParseTimeWindow:
    """Тесты парсинга временного окна."""

    def test_valid_window(self):
        """Парсинг корректного окна."""
        result = _parse_time_window("09:00-21:00")
        assert result == (9, 0, 21, 0)

    def test_valid_window_with_minutes(self):
        """Парсинг окна с минутами."""
        result = _parse_time_window("08:30-17:45")
        assert result == (8, 30, 17, 45)

    def test_midnight_start(self):
        """Окно с полуночи."""
        result = _parse_time_window("00:00-08:00")
        assert result == (0, 0, 8, 0)

    def test_invalid_format_no_dash(self):
        """Неверный формат без дефиса."""
        with pytest.raises(ValueError, match="ожидается 'HH:MM-HH:MM'"):
            _parse_time_window("09:0021:00")

    def test_invalid_hour(self):
        """Неверный час (25)."""
        with pytest.raises(ValueError, match="Неверный формат time_window"):
            _parse_time_window("25:00-21:00")

    def test_invalid_minute(self):
        """Неверная минута (60)."""
        with pytest.raises(ValueError, match="Неверный формат time_window"):
            _parse_time_window("09:60-21:00")


class TestIsWithinTimeWindow:
    """Тесты проверки попадания в временное окно."""

    def test_within_window(self):
        """Время внутри окна."""
        now = datetime(2025, 2, 28, 10, 30)
        assert _is_within_time_window(now, "09:00-21:00") is True

    def test_at_start_boundary(self):
        """Время на границе начала (включительно)."""
        now = datetime(2025, 2, 28, 9, 0)
        assert _is_within_time_window(now, "09:00-21:00") is True

    def test_at_end_boundary(self):
        """Время на границе конца (исключительно)."""
        now = datetime(2025, 2, 28, 21, 0)
        assert _is_within_time_window(now, "09:00-21:00") is False

    def test_before_window(self):
        """Время до окна."""
        now = datetime(2025, 2, 28, 8, 59)
        assert _is_within_time_window(now, "09:00-21:00") is False

    def test_after_window(self):
        """Время после окна."""
        now = datetime(2025, 2, 28, 21, 1)
        assert _is_within_time_window(now, "09:00-21:00") is False

    def test_early_morning_before_work(self):
        """Раннее утро до работы (проблема в ТЗ)."""
        now = datetime(2025, 2, 28, 7, 0)
        assert _is_within_time_window(now, "09:00-21:00") is False

    def test_late_evening_after_work(self):
        """Поздний вечер после работы (проблема в ТЗ)."""
        now = datetime(2025, 2, 28, 22, 30)
        assert _is_within_time_window(now, "09:00-21:00") is False

    def test_window_with_minutes(self):
        """Окно с минутами."""
        now = datetime(2025, 2, 28, 12, 0)
        assert _is_within_time_window(now, "08:30-17:45") is True


class TestShouldRunNowHourly:
    """Тесты should_run_now для hourly расписания."""

    def test_hourly_without_time_window(self):
        """Hourly без time_window выполняется всегда."""
        task = {"schedule": "hourly"}
        now = datetime(2025, 2, 28, 3, 0)  # 3 часа ночи
        assert should_run_now(task, now) is True

    def test_hourly_with_time_window_within(self):
        """Hourly с time_window, время внутри."""
        task = {"schedule": "hourly", "time_window": "09:00-21:00"}
        now = datetime(2025, 2, 28, 15, 30)
        assert should_run_now(task, now) is True

    def test_hourly_with_time_window_before(self):
        """Hourly с time_window, время до окна (проблема в ТЗ)."""
        task = {"schedule": "hourly", "time_window": "09:00-21:00"}
        now = datetime(2025, 2, 28, 8, 0)
        assert should_run_now(task, now) is False

    def test_hourly_with_time_window_after(self):
        """Hourly с time_window, время после окна (проблема в ТЗ)."""
        task = {"schedule": "hourly", "time_window": "09:00-21:00"}
        now = datetime(2025, 2, 28, 21, 30)
        assert should_run_now(task, now) is False

    def test_hourly_with_time_window_at_start(self):
        """Hourly с time_window, время на старте."""
        task = {"schedule": "hourly", "time_window": "09:00-21:00"}
        now = datetime(2025, 2, 28, 9, 0)
        assert should_run_now(task, now) is True

    def test_hourly_with_time_window_at_end(self):
        """Hourly с time_window, время на конце (исключается)."""
        task = {"schedule": "hourly", "time_window": "09:00-21:00"}
        now = datetime(2025, 2, 28, 21, 0)
        assert should_run_now(task, now) is False


class TestShouldRunNowDaily:
    """Тесты should_run_now для daily расписания."""

    def test_daily_exact_hour(self):
        """Daily, точное совпадение часа."""
        task = {"schedule": "daily", "time": "14:00"}
        now = datetime(2025, 2, 28, 14, 30)
        assert should_run_now(task, now) is True

    def test_daily_different_hour(self):
        """Daily, несовпадение часа."""
        task = {"schedule": "daily", "time": "14:00"}
        now = datetime(2025, 2, 28, 15, 0)
        assert should_run_now(task, now) is False

    def test_daily_no_time_param(self):
        """Daily без time параметра."""
        task = {"schedule": "daily"}
        now = datetime(2025, 2, 28, 12, 0)
        with pytest.raises(ValueError, match="требует параметр 'time'"):
            should_run_now(task, now)

    def test_daily_invalid_time_format(self):
        """Daily с неверным форматом time."""
        task = {"schedule": "daily", "time": "25:00"}
        now = datetime(2025, 2, 28, 12, 0)
        with pytest.raises(ValueError, match="Неверный формат времени"):
            should_run_now(task, now)


class TestShouldRunNowUnsupported:
    """Тесты should_run_now для неподдерживаемых типов."""

    def test_unsupported_schedule(self):
        """Неподдерживаемый тип расписания."""
        task = {"schedule": "weekly"}
        now = datetime(2025, 2, 28, 12, 0)
        with pytest.raises(ValueError, match="Неподдерживаемый тип"):
            should_run_now(task, now)

    def test_once_schedule(self):
        """Тип 'once' зарезервирован."""
        task = {"schedule": "once"}
        now = datetime(2025, 2, 28, 12, 0)
        assert should_run_now(task, now) is False


class TestIntegration:
    """Интеграционные тесты для реальных сценариев."""

    def test_cloudmonitor_script_during_work_hours(self):
        """CloudMonitorScript в рабочее время."""
        task = {
            "name": "CloudMonitorScript",
            "schedule": "hourly",
            "time_window": "09:00-21:00",
        }
        now = datetime(2025, 2, 28, 14, 0)
        assert should_run_now(task, now) is True

    def test_cloudmonitor_script_night(self):
        """CloudMonitorScript ночью (проблема в ТЗ)."""
        task = {
            "name": "CloudMonitorScript",
            "schedule": "hourly",
            "time_window": "09:00-21:00",
        }
        now = datetime(2025, 2, 28, 3, 0)
        assert should_run_now(task, now) is False

    def test_videomonitor_local_during_work_hours(self):
        """VideoMonitorScript_local в рабочее время."""
        task = {
            "name": "VideoMonitorScript_local",
            "schedule": "hourly",
            "time_window": "09:00-21:00",
        }
        now = datetime(2025, 2, 28, 10, 0)
        assert should_run_now(task, now) is True

    def test_videomonitor_local_after_work(self):
        """VideoMonitorScript_local после работы (проблема в ТЗ)."""
        task = {
            "name": "VideoMonitorScript_local",
            "schedule": "hourly",
            "time_window": "09:00-21:00",
        }
        now = datetime(2025, 2, 28, 22, 0)
        assert should_run_now(task, now) is False
