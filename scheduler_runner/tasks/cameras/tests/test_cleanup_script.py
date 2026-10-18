import logging
import os
from pathlib import Path
from unittest import mock

import pytest

from scheduler_runner.tasks.cameras.CleanupScript import execute_cleanup_operations, get_scenario_config
from scheduler_runner.tasks.cameras.tests._test_tmp_utils import cleanup_dir, make_temp_dir


def test_get_scenario_config_valid():
    config = get_scenario_config("local")
    assert "CLEANUP_DIR" in config
    assert "MAX_AGE_DAYS" in config


def test_get_scenario_config_invalid():
    with pytest.raises(ValueError):
        get_scenario_config("unknown")


def test_execute_cleanup_operations_removes_old_files_and_empty_folders():
    d = make_temp_dir("cleanup_ok")
    try:
        old_file = d / "old_file.txt"
        old_file.write_text("old", encoding="utf-8")
        os.utime(old_file, (1, 1))
        (d / "new_file.txt").write_text("new", encoding="utf-8")
        (d / "empty_folder").mkdir()
        non_empty_folder = d / "non_empty_folder"
        non_empty_folder.mkdir()
        (non_empty_folder / "file.txt").write_text("data", encoding="utf-8")

        execute_cleanup_operations(logging.getLogger("test_cleanup"), d, max_age_days=0)

        names = [f.name for f in d.iterdir()]
        assert "new_file.txt" in names
        assert "old_file.txt" not in names
        assert "empty_folder" not in names
        assert "non_empty_folder" in names
    finally:
        cleanup_dir(d)


def test_execute_cleanup_operations_handles_errors(caplog):
    d = make_temp_dir("cleanup_err")
    try:
        error_file = d / "error.txt"
        error_file.write_text("fail", encoding="utf-8")
        os.utime(error_file, (1, 1))

        original_unlink = Path.unlink

        def unlink_side_effect(self, *args, **kwargs):
            if self.name == "error.txt":
                raise PermissionError("Mocked permission error")
            return original_unlink(self, *args, **kwargs)

        with mock.patch("pathlib.Path.unlink", new=unlink_side_effect):
            with caplog.at_level(logging.WARNING):
                execute_cleanup_operations(logging.getLogger("test_cleanup_error"), d, max_age_days=0)
                assert "Ошибка при удалении файла" in caplog.text
    finally:
        cleanup_dir(d)


# --- Облако: удаление папок с датой в имени (remove_old_dated_folders) ---

from datetime import datetime

from scheduler_runner.tasks.cameras.CleanupScript import execute_dated_cleanup
from scheduler_runner.utils.filesystem import remove_old_dated_folders

NOW = datetime(2026, 10, 18, 20, 55, 0)


def make_archive(root: Path) -> None:
    """Структура как в облаке: UNV — <uid>/YYYYMMDD/HH/файлы, Xiaomi — <uid>/YYYYMMDDHH/файлы."""
    for day, hour in (("20260610", "09"), ("20260619", "21"), ("20260620", "10"), ("20261018", "12")):
        folder = root / "unv_camera" / "client_area" / day / hour
        folder.mkdir(parents=True)
        (folder / "09-00-01.jpg").write_text("x", encoding="utf-8")
    for stamp in ("2026061023", "2026061923", "2026062000", "2026101812"):
        folder = root / "xiaomi_camera_videos" / "788b2aab4458" / stamp
        folder.mkdir(parents=True)
        (folder / "00M40S_1.mp4").write_text("x", encoding="utf-8")
    (root / "unv_camera" / "client_area" / "notes").mkdir()      # не дата — не трогаем
    (root / "unv_camera" / "client_area" / "20261399").mkdir()   # не настоящая дата — не трогаем


def names(root: Path, *parts) -> list:
    return sorted(p.name for p in root.joinpath(*parts).iterdir())


def test_remove_old_dated_folders_deletes_only_whole_periods_older_than_threshold():
    root = make_temp_dir("dated_cleanup")
    try:
        make_archive(root)
        # 120 дней от 18.10 20:55 — граница 20.06 20:55: сутки 19.06 и час 20.06 00:00 целиком старше, сутки 20.06 — нет
        result = remove_old_dated_folders(root, 120, logging.getLogger("test"), now=NOW)
        assert names(root, "unv_camera", "client_area") == ["20260620", "20261018", "20261399", "notes"]
        assert names(root, "xiaomi_camera_videos", "788b2aab4458") == ["2026101812"]
        assert result["removed_folders"] == 5 and result["errors"] == 0 and result["remaining_old"] == 0
        assert result["oldest_kept"] == "2026-06-20" and result["budget_exhausted"] is False
    finally:
        cleanup_dir(root)


def test_remove_old_dated_folders_stops_on_time_budget_and_keeps_rest_for_next_run():
    root = make_temp_dir("dated_budget")
    try:
        make_archive(root)
        result = remove_old_dated_folders(root, 120, logging.getLogger("test"), time_budget_seconds=0, now=NOW)
        assert result["removed_folders"] == 0 and result["remaining_old"] == 5 and result["budget_exhausted"] is True
        assert result["oldest_kept"] == "2026-06-10"
        assert "20260610" in names(root, "unv_camera", "client_area")
    finally:
        cleanup_dir(root)


def test_remove_old_dated_folders_removes_oldest_first(monkeypatch):
    root = make_temp_dir("dated_order")
    try:
        make_archive(root)
        removed = []
        with monkeypatch.context() as patched:
            patched.setattr("scheduler_runner.utils.filesystem.shutil.rmtree", lambda path: removed.append(Path(path).name))
            remove_old_dated_folders(root, 120, logging.getLogger("test"), now=NOW)
        assert removed == ["20260610", "2026061023", "20260619", "2026061923", "2026062000"]
    finally:
        cleanup_dir(root)


def test_execute_dated_cleanup_logs_budget_warning(caplog):
    root = make_temp_dir("dated_exec")
    try:
        make_archive(root)
        with caplog.at_level(logging.INFO):
            result = execute_dated_cleanup(logging.getLogger("test_dated"), root, 1, time_budget_seconds=0)
        assert result["budget_exhausted"] is True
        assert "Сводка" in caplog.text and "продолжим при следующем запуске" in caplog.text
    finally:
        cleanup_dir(root)


def test_network_scenario_uses_dated_folders():
    config = get_scenario_config("network")
    assert config["DATED_FOLDERS"] is True
    assert config["TIME_BUDGET_SECONDS"] < 600
