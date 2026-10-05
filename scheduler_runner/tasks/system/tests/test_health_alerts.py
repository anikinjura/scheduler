"""
Тесты правил тревог (health_alerts.py) и сохранения состояния в HealthMonitorScript — без сети и Windows.
"""
from datetime import datetime, timedelta
from unittest.mock import MagicMock

import scheduler_runner.tasks.system.HealthMonitorScript as hm
from scheduler_runner.tasks.system import health_alerts as ha

NOW = datetime(2026, 10, 6, 13, 10, 0)


def health(at=NOW, **overrides):
    """Строка сбора на момент at: последний успешный сбор отчетов — накануне в 21:10 (как в жизни)."""
    row = {
        "pvz_id": "ЧЕБОКСАРЫ_144", "c_free_gb": 66.1, "c_free_pct": 55.9, "disks_warning": 0,
        "unexpected_shutdowns": 0, "bugchecks": 0, "whea_errors": 0, "disk_errors": 0, "ntfs_errors": 0,
        "events_window_hours": 24, "code_version": "8eb0481", "main_version": "8eb0481", "code_up_to_date": True,
        "reports_last_success": (at - timedelta(hours=16)).strftime("%Y-%m-%d %H:%M:%S"),
        "task_operator_result": 0, "task_camera_result": 0, "task_system_result": 0,
    }
    row.update(overrides)
    return row


def disk(**overrides):
    row = {"disk_serial": "1788N1VAS", "model": "TOSHIBA DT01ACA050", "volumes": "D", "health_status": "Healthy",
           "predict_failure": False, "smart_5_reallocated": 0, "smart_197_pending": 0,
           "smart_198_offline_uncorrectable": 0, "smart_199_crc_errors": 0, "temperature_c": 30}
    row.update(overrides)
    return row


FAILING = dict(disk_serial="9VMGFAQH", model="ST3320418AS", volumes="C,F", health_status="Warning",
               predict_failure=True, smart_5_reallocated=4095, smart_199_crc_errors=128374, temperature_c=31)


def codes(alerts):
    return sorted(a.code for a in alerts)


def test_healthy_computer_has_no_alerts():
    result = ha.evaluate(health(), [disk()], {}, NOW)
    assert result.alerts == [] and result.status == "ok" and not result.has_news
    assert result.state["disks"]["1788N1VAS"]["smart_5_reallocated"] == 0


def test_144_first_run_reports_everything_as_new():
    result = ha.evaluate(health(unexpected_shutdowns=1, disk_errors=1, disks_warning=1),
                         [disk(**FAILING), disk()], {}, NOW)
    assert codes(result.alerts) == ["DISK_IO_ERRORS", "DISK_PREDICT_FAILURE", "UNEXPECTED_SHUTDOWN"]
    assert codes(result.new) == codes(result.alerts)
    assert result.status == "critical"
    assert result.codes == "DISK_IO_ERRORS,DISK_PREDICT_FAILURE,UNEXPECTED_SHUTDOWN"
    message = ha.format_message("ЧЕБОКСАРЫ_144", health(disks_warning=1), result)
    assert message.startswith("🔴 ЧЕБОКСАРЫ_144") and "Новое:" in message and "9VMGFAQH" in message


def test_same_state_next_day_is_silent_and_week_later_reminds_critical_only():
    first = ha.evaluate(health(c_free_gb=7.9, c_free_pct=7.1), [disk(**FAILING)], {}, NOW)
    next_day = ha.evaluate(health(at=NOW + timedelta(days=1), c_free_gb=7.9, c_free_pct=7.1), [disk(**FAILING)], first.state, NOW + timedelta(days=1))
    assert not next_day.has_news
    assert next_day.state["alerts"]["DISK_PREDICT_FAILURE|9VMGFAQH"]["last_notified"] == "2026-10-06 13:10:00"

    week = ha.evaluate(health(at=NOW + timedelta(days=7), c_free_gb=7.9, c_free_pct=7.1), [disk(**FAILING)], next_day.state, NOW + timedelta(days=7))
    assert codes(week.reminders) == ["DISK_PREDICT_FAILURE"]  # LOW_SPACE_C — warning, без повторов
    assert "Не устранено:" in ha.format_message("X", health(), week)


def test_low_space_worsens_and_resolves():
    warning = ha.evaluate(health(c_free_gb=7.9, c_free_pct=7.1), [], {}, NOW)
    assert [(a.code, a.level) for a in warning.alerts] == [("LOW_SPACE_C", "warning")]
    critical = ha.evaluate(health(at=NOW + timedelta(days=1), c_free_gb=4.0, c_free_pct=3.6), [], warning.state, NOW + timedelta(days=1))
    assert codes(critical.worsened) == ["LOW_SPACE_C"] and critical.status == "critical"
    cleaned = ha.evaluate(health(at=NOW + timedelta(days=2), c_free_gb=50.0, c_free_pct=45.0), [], critical.state, NOW + timedelta(days=2))
    assert [r["code"] for r in cleaned.resolved] == ["LOW_SPACE_C"]
    assert "Устранено:" in ha.format_message("X", health(), cleaned)


def test_low_space_by_gigabytes_on_big_disk():
    result = ha.evaluate(health(c_free_gb=9.0, c_free_pct=20.0), [], {}, NOW)
    assert codes(result.alerts) == ["LOW_SPACE_C"]


def test_sector_growth_is_an_event_not_resolved_later():
    previous = ha.evaluate(health(), [disk(smart_5_reallocated=8, smart_199_crc_errors=10)], {}, NOW).state
    grown = ha.evaluate(health(at=NOW + timedelta(days=1)), [disk(smart_5_reallocated=12, smart_199_crc_errors=15)], previous, NOW + timedelta(days=1))
    assert codes(grown.new) == ["DISK_CRC_GROWTH", "DISK_REALLOCATED_GROWTH"]
    assert "8 → 12" in next(a.text for a in grown.alerts if a.code == "DISK_REALLOCATED_GROWTH")
    stable = ha.evaluate(health(at=NOW + timedelta(days=2)), [disk(smart_5_reallocated=12, smart_199_crc_errors=15)], grown.state, NOW + timedelta(days=2))
    assert stable.alerts == [] and stable.resolved == [] and not stable.has_news
    again = ha.evaluate(health(at=NOW + timedelta(days=3)), [disk(smart_5_reallocated=13, smart_199_crc_errors=15)], stable.state, NOW + timedelta(days=3))
    assert codes(again.new) == ["DISK_REALLOCATED_GROWTH"]


def test_new_disk_without_history_is_not_growth():
    result = ha.evaluate(health(), [disk(disk_serial="NEW", smart_5_reallocated=3)], {"disks": {}}, NOW)
    assert result.alerts == []


def test_code_outdated_only_after_24_hours():
    first = ha.evaluate(health(code_up_to_date=False), [], {}, NOW)
    assert first.alerts == [] and "code_outdated" in first.state["conditions"]
    later = ha.evaluate(health(at=NOW + timedelta(hours=25), code_up_to_date=False), [], first.state, NOW + timedelta(hours=25))
    assert codes(later.new) == ["CODE_OUTDATED"]
    fixed = ha.evaluate(health(at=NOW + timedelta(hours=26), code_up_to_date=True), [], later.state, NOW + timedelta(hours=26))
    assert "code_outdated" not in fixed.state["conditions"] and [r["code"] for r in fixed.resolved] == ["CODE_OUTDATED"]
    unknown = ha.evaluate(health(at=NOW + timedelta(hours=25), code_up_to_date=None), [], first.state, NOW + timedelta(hours=25))
    assert unknown.alerts == []  # нет сети — не тревога


def test_reports_stale_and_task_results():
    result = ha.evaluate(health(reports_last_success="2026-10-04 21:37:00", task_operator_result=0x41301,
                                task_camera_result=1, task_system_result=None), [], {}, NOW)
    assert codes(result.alerts) == ["REPORTS_STALE", "TASK_FAILED"]
    assert next(a for a in result.alerts if a.code == "TASK_FAILED").subject == "camera"
    assert codes(ha.evaluate(health(reports_last_success=""), [], {}, NOW).alerts) == ["REPORTS_STALE"]


def test_shutdowns_whea_and_hot_disk_levels():
    result = ha.evaluate(health(unexpected_shutdowns=2, bugchecks=1, whea_errors=1), [disk(temperature_c=55)], {}, NOW)
    levels = {a.code: a.level for a in result.alerts}
    assert levels == {"UNEXPECTED_SHUTDOWN": "critical", "WHEA": "critical", "DISK_HOT": "warning"}


# ---------- сохранение состояния ----------

def test_notify_and_save_rules(tmp_path, monkeypatch):
    state_file = str(tmp_path / "health_state.json")
    logger = MagicMock()
    news = ha.evaluate(health(c_free_gb=7.9, c_free_pct=7.1), [], {}, NOW)

    hm.notify_and_save(news, health(), state_file, "X", notify=False, logger=logger)
    assert hm.load_state(state_file, logger) == {}  # не отправили — изменение будет сообщено в следующий раз

    monkeypatch.setattr(hm, "send_alert_message", lambda message, logger: {"success": False, "error": "vk"})
    hm.notify_and_save(news, health(), state_file, "X", notify=True, logger=logger)
    assert hm.load_state(state_file, logger) == {}

    sent = []
    monkeypatch.setattr(hm, "send_alert_message", lambda message, logger: sent.append(message) or {"success": True})
    hm.notify_and_save(news, health(), state_file, "X", notify=True, logger=logger)
    assert len(sent) == 1 and "LOW_SPACE_C|C" in hm.load_state(state_file, logger)["alerts"]

    quiet = ha.evaluate(health(), [], {}, NOW)
    hm.notify_and_save(quiet, health(), state_file, "X", notify=True, logger=logger)
    assert len(sent) == 1 and hm.load_state(state_file, logger)["alerts"] == {}


def test_load_state_survives_broken_file(tmp_path):
    broken = tmp_path / "health_state.json"
    broken.write_text("{not json", encoding="utf-8")
    logger = MagicMock()
    assert hm.load_state(str(broken), logger) == {}
    assert "HEALTH_STATE_UNREADABLE" in logger.warning.call_args.args[0]
