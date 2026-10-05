"""
Тесты HealthMonitorScript (этап 1: сбор и построение строк) — без PowerShell, git и сети.

Пример данных повторяет реальный сбор с ЧЕБОКСАРЫ_144 (05.10.2026): системный диск с прогнозом отказа.
"""
import json
from datetime import date, datetime

import pytest

import scheduler_runner.tasks.system.HealthMonitorScript as hm


def probe_sample():
    return {
        "probe_version": 1,
        "collected_at": "2026-10-05T13:10:05",
        "elevated": True,
        "limited": [],
        "system": {"computer_name": "OZON", "os_version": "Windows 10 Pro 10.0.19045", "boot_time": "2026-10-05T09:13:44",
                   "uptime_hours": 3.9, "cpu": "Intel(R) Core(TM) i5-4460", "ram_gb": 7.7},
        "volumes": [
            {"letter": "C", "size_gb": 118.4, "free_gb": 66.2, "free_pct": 55.9, "health": "Healthy"},
            {"letter": "D", "size_gb": 465.8, "free_gb": 346.1, "free_pct": 74.3, "health": "Healthy"},
        ],
        "disks": [
            {"disk_number": 0, "serial": "9VMGFAQH", "model": "ST3320418AS", "media_type": "HDD", "size_gb": 298.1,
             "volumes": "C,F", "health_status": "Warning", "predict_failure": True, "temperature_c": 31,
             "power_on_hours": 38230,
             "smart": {"5": {"raw": 4095}, "197": {"raw": 0}, "198": {"raw": 0}, "187": {"raw": 73}, "199": {"raw": 128374}}},
            {"disk_number": 1, "serial": "1788N1VAS", "model": "TOSHIBA DT01ACA050", "media_type": "HDD", "size_gb": 465.8,
             "volumes": "D", "health_status": "Healthy", "predict_failure": False, "temperature_c": 29,
             "power_on_hours": 32414, "smart": {"5": {"raw": 0}, "187": {"raw": 65536}}},
            {"disk_number": 2, "serial": "", "model": "SSD 128GB", "media_type": "SSD", "size_gb": 119.2,
             "volumes": "", "health_status": "Healthy", "predict_failure": None, "temperature_c": None,
             "power_on_hours": None, "smart": {}},
        ],
        "events": {"window_hours": 24, "unexpected_shutdowns": 1, "bugchecks": 0, "whea_errors": 0, "whea_warnings": 0,
                   "disk_errors": 1, "ntfs_errors": 0, "display_resets": 0},
        "tasks": [
            {"name": "\\Задачи operator", "found": True, "state": "Ready", "last_result": 0},
            {"name": "\\Задачи camera", "found": False},
            {"name": "\\Задачи system", "found": True, "state": "Ready", "last_result": 1},
        ],
    }


def test_parse_probe_output_skips_bom_and_noise():
    payload = "﻿предупреждение\r\n" + json.dumps({"probe_version": 1})
    assert hm.parse_probe_output(0, payload.encode("utf-8"), b"") == {"probe_version": 1}


@pytest.mark.parametrize("code,stdout", [(1, b'{"a": 1}'), (0, b"no json here")])
def test_parse_probe_output_errors(code, stdout):
    with pytest.raises(hm.ProbeError):
        hm.parse_probe_output(code, stdout, b"boom")


def test_run_probe_hides_window_and_passes_tasks(monkeypatch):
    captured = {}

    class Result:
        returncode = 0
        stdout = b'{"ok": true}'
        stderr = b""

    def fake_run(command, **kwargs):
        captured["command"], captured["kwargs"] = command, kwargs
        return Result()

    monkeypatch.setattr(hm.subprocess, "run", fake_run)
    monkeypatch.setattr(hm.sys, "platform", "win32")
    assert hm.run_probe("probe.ps1", 24, ["\\Задачи operator", "\\Задачи system"], 60) == {"ok": True}
    assert captured["kwargs"]["creationflags"] == hm.CREATE_NO_WINDOW
    assert captured["command"][-2:] == ["-TaskNames", "\\Задачи operator,\\Задачи system"]
    assert "-NonInteractive" in captured["command"]


def test_build_disk_rows_smart_and_fallbacks():
    rows = hm.build_disk_rows(probe_sample(), "ЧЕБОКСАРЫ_144", "2026-10-05")
    failing, toshiba, ssd = rows
    assert failing["disk_serial"] == "9VMGFAQH"
    assert failing["smart_5_reallocated"] == 4095
    assert failing["smart_199_crc_errors"] == 128374
    assert failing["predict_failure"] is True
    assert toshiba["smart_187_reported_uncorrectable"] == 0  # упакованное 65536 -> младшие 16 бит
    assert ssd["disk_serial"] == "disk2"  # нет серийного номера
    assert ssd["smart_5_reallocated"] is None


def test_build_health_row():
    probe = probe_sample()
    disk_rows = hm.build_disk_rows(probe, "ЧЕБОКСАРЫ_144", "2026-10-05")
    code = {"code_version": "8eb0481", "main_version": "8eb0481", "code_up_to_date": True}
    sched = {"reports_last_success": "2026-10-04 21:37:00", "copy_last_success": "", "updater_last_success": "2026-10-05 09:10:05"}
    row = hm.build_health_row(probe, code, sched, disk_rows, "ЧЕБОКСАРЫ_144", "2026-10-05")

    assert (row["c_free_gb"], row["c_free_pct"], row["d_free_pct"]) == (66.2, 55.9, 74.3)
    assert row["disks_total"] == 3 and row["disks_warning"] == 1
    assert row["worst_disk"] == "ST3320418AS [9VMGFAQH]"
    assert (row["task_operator_result"], row["task_camera_result"], row["task_system_result"]) == (0, None, 1)
    assert row["unexpected_shutdowns"] == 1 and row["disk_errors"] == 1
    assert row["code_up_to_date"] is True and row["reports_last_success"] == "2026-10-04 21:37:00"
    assert row["probe_limited"] == "" and row["status"] == ""
    assert "HEALTH_SUMMARY pvz=ЧЕБОКСАРЫ_144" in hm.format_summary(row)


def test_build_health_row_without_volumes_and_limited():
    probe = probe_sample()
    probe["volumes"], probe["limited"] = [], ["smart: Отказано в доступе"]
    row = hm.build_health_row(probe, {"code_version": "", "main_version": "", "code_up_to_date": None},
                              {}, [], "X", "2026-10-05")
    assert row["c_free_gb"] is None
    assert row["probe_limited"] == "smart: Отказано в доступе"
    # колонки листа одинаковы, даже если логов задач нет
    assert (row["reports_last_success"], row["copy_last_success"], row["updater_last_success"]) == ("", "", "")
    hm.format_summary(row)


def _write_log(root, user, task, day, lines):
    folder = root / user / task
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"{day}.log").write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_find_last_task_success_takes_latest_line_and_day(tmp_path):
    _write_log(tmp_path, "operator", "ReportsProcessor", "2026-10-03",
               ["2026-10-03 21:37:00 INFO [operator.ReportsProcessor] Задача 'ReportsProcessor' успешно завершена"])
    _write_log(tmp_path, "operator", "ReportsProcessor", "2026-10-05",
               ["2026-10-05 10:30:00 INFO [operator.ReportsProcessor] Задача 'ReportsProcessor' не должна запускаться сейчас по расписанию"])
    found = hm.find_last_task_success(str(tmp_path), "operator", "ReportsProcessor", 14, date(2026, 10, 5))
    assert found == "2026-10-03 21:37:00"
    assert hm.find_last_task_success(str(tmp_path), "operator", "ReportsProcessor", 2, date(2026, 10, 5)) is None


def test_collect_scheduler_state_takes_newest_of_several_tasks(tmp_path):
    _write_log(tmp_path, "system", "UpdaterScript_morning", "2026-10-05",
               ["2026-10-05 09:10:05 INFO [system.UpdaterScript_morning] Задача 'UpdaterScript_morning' успешно завершена"])
    _write_log(tmp_path, "system", "UpdaterScript_evening", "2026-10-04",
               ["2026-10-04 18:10:07 INFO [system.UpdaterScript_evening] Задача 'UpdaterScript_evening' успешно завершена"])
    config = {"LOGS_ROOT": str(tmp_path), "TASK_LOG_LOOKBACK_DAYS": 14, "TRACKED_TASK_LOGS": {
        "updater_last_success": [("system", "UpdaterScript_morning"), ("system", "UpdaterScript_evening")],
        "copy_last_success": [("operator", "CopyScript")]}}
    state = hm.collect_scheduler_state(config, date(2026, 10, 5))
    assert state == {"updater_last_success": "2026-10-05 09:10:05", "copy_last_success": ""}


def test_collect_code_state(monkeypatch):
    answers = {"rev-parse": "8eb0481aaaa", "ls-remote": "8eb0481aaaa\trefs/heads/main"}
    monkeypatch.setattr(hm, "_git", lambda repo, args, timeout: answers[args[0]])
    assert hm.collect_code_state("repo", "main", 5) == {"code_version": "8eb0481", "main_version": "8eb0481", "code_up_to_date": True}
    monkeypatch.setattr(hm, "_git", lambda repo, args, timeout: None)
    assert hm.collect_code_state("repo", "main", 5)["code_up_to_date"] is None


def test_schedule_is_daily_system_task():
    from scheduler_runner.tasks.system.config.scripts.health_monitor_config import SCHEDULE
    assert SCHEDULE[0]["user"] == "system" and SCHEDULE[0]["schedule"] == "daily" and SCHEDULE[0]["time"] == "13:10"


# ---------- Этап 2: листы и запись ----------

from unittest.mock import MagicMock

from scheduler_runner.tasks.system.config import health_sheets_config as sheets


def test_sheet_columns_match_built_rows():
    probe = probe_sample()
    disk_rows = hm.build_disk_rows(probe, "ЧЕБОКСАРЫ_144", "2026-10-05")
    row = hm.build_health_row(probe, {"code_version": "", "main_version": "", "code_up_to_date": None}, {},
                              disk_rows, "ЧЕБОКСАРЫ_144", "2026-10-05")
    assert set(sheets.HEALTH_COLUMNS) == set(row)
    assert set(sheets.DISK_COLUMNS) == set(disk_rows[0])
    assert len(sheets.HEALTH_COLUMNS) == len(set(sheets.HEALTH_COLUMNS))
    assert sheets.HEALTH_TABLE_CONFIG.unique_key_columns == ["work_date", "pvz_id"]
    assert sheets.DISK_TABLE_CONFIG.unique_key_columns == ["work_date", "pvz_id", "disk_serial"]


def test_rows_for_sheet_drops_none_and_formats_time():
    rows = hm.rows_for_sheet([{"work_date": "2026-10-05", "collected_at": "2026-10-05T13:10:05", "cpu": None,
                               "extra": 1}], ["work_date", "collected_at", "cpu"])
    assert rows == [{"work_date": "2026-10-05", "collected_at": "2026-10-05 13:10:05"}]


def _fake_spreadsheet(existing_titles, headers_by_title=None):
    headers_by_title = headers_by_title or {}
    spreadsheet = MagicMock()
    worksheets = {}

    def make_ws(title):
        ws = MagicMock(title=title)
        ws.row_values.return_value = headers_by_title.get(title, [])
        worksheets[title] = ws
        return ws

    for title in existing_titles:
        make_ws(title)
    spreadsheet.worksheets.side_effect = lambda: list(worksheets.values())
    spreadsheet.add_worksheet.side_effect = lambda title, rows, cols: make_ws(title)
    return spreadsheet, worksheets


def test_ensure_worksheets_creates_missing_with_headers(monkeypatch):
    spreadsheet, worksheets = _fake_spreadsheet(
        ["KPI", "PVZ_HEALTH"], {"PVZ_HEALTH": list(sheets.HEALTH_COLUMNS)})
    client = MagicMock()
    client.open_by_key.return_value = spreadsheet
    import gspread
    monkeypatch.setattr(gspread, "service_account", lambda filename: client)
    logger = MagicMock()

    hm.ensure_worksheets("creds.json", "sheet-id", [sheets.HEALTH_TABLE_CONFIG, sheets.DISK_TABLE_CONFIG], logger)

    spreadsheet.add_worksheet.assert_called_once()
    assert spreadsheet.add_worksheet.call_args.kwargs["title"] == "PVZ_DISK_HEALTH"
    worksheets["PVZ_DISK_HEALTH"].update.assert_called_once_with(range_name="A1", values=[list(sheets.DISK_COLUMNS)])
    worksheets["PVZ_HEALTH"].update.assert_not_called()  # существующий лист с верным заголовком не трогаем
    logger.warning.assert_not_called()


def test_ensure_worksheets_warns_on_header_mismatch(monkeypatch):
    spreadsheet, worksheets = _fake_spreadsheet(
        ["PVZ_HEALTH", "PVZ_DISK_HEALTH"], {"PVZ_HEALTH": ["work_date", "other"], "PVZ_DISK_HEALTH": list(sheets.DISK_COLUMNS)})
    client = MagicMock()
    client.open_by_key.return_value = spreadsheet
    import gspread
    monkeypatch.setattr(gspread, "service_account", lambda filename: client)
    logger = MagicMock()

    hm.ensure_worksheets("creds.json", "sheet-id", [sheets.HEALTH_TABLE_CONFIG, sheets.DISK_TABLE_CONFIG], logger)

    worksheets["PVZ_HEALTH"].update.assert_not_called()
    assert "HEALTH_SHEET_HEADER_MISMATCH" in logger.warning.call_args.args[0]


def test_upload_rows_uses_batch_upsert(monkeypatch):
    import scheduler_runner.utils.uploader as uploader
    captured = {}

    def fake_upload(data_list, connection_params, logger, **kwargs):
        captured.update(data_list=data_list, params=connection_params, kwargs=kwargs)
        return {"success": True, "uploaded_records": len(data_list)}

    monkeypatch.setattr(uploader, "upload_batch_data", fake_upload)
    rows = [{"work_date": "2026-10-05", "pvz_id": "ЧЕБОКСАРЫ_144", "disk_serial": "9VMGFAQH", "smart_5_reallocated": 4095,
             "temperature_c": None}]
    result = hm.upload_rows(rows, sheets.DISK_TABLE_CONFIG, "creds.json", "sheet-id", MagicMock())

    assert result["success"] is True
    assert captured["kwargs"] == {"strategy": "update_or_append", "UPLOAD_MODE": "batch"}
    assert captured["params"]["WORKSHEET_NAME"] == "PVZ_DISK_HEALTH"
    assert captured["params"]["TABLE_CONFIG"] is not sheets.DISK_TABLE_CONFIG  # копия: загрузчик меняет индексы
    assert captured["data_list"] == [{"work_date": "2026-10-05", "pvz_id": "ЧЕБОКСАРЫ_144", "disk_serial": "9VMGFAQH",
                                      "smart_5_reallocated": 4095}]
    assert hm.upload_rows([], sheets.DISK_TABLE_CONFIG, "c", "s", MagicMock()) == {"success": True, "uploaded_records": 0}


@pytest.mark.parametrize("pvz,expected", [("ЧЕБОКСАРЫ_144", True), ("НЕ_НАСТРОЕН", False), ("UNKNOWN", False), ("", False)])
def test_is_configured_pvz(pvz, expected):
    assert hm.is_configured_pvz(pvz) is expected
