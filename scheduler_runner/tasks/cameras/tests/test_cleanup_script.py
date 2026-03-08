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
