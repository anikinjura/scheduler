import os
import sys
import logging
from pathlib import Path

import pytest

from scheduler_runner.tasks.cameras import CopyScript
from scheduler_runner.tasks.cameras.tests._test_tmp_utils import cleanup_dir, make_temp_dir
from scheduler_runner.utils.filesystem import copy_recent_files


def test_copy_recent_files_skip():
    root = make_temp_dir("copy_skip")
    try:
        src = root / "src"
        dst = root / "dst"
        src.mkdir(); dst.mkdir()
        (src / "new.txt").write_text("new", encoding="utf-8")
        (dst / "new.txt").write_text("conflict", encoding="utf-8")

        result = copy_recent_files(src=src, dst=dst, days_threshold=10000, conflict_mode="skip", logger=logging.getLogger("t"))
        assert (dst / "new.txt").read_text(encoding="utf-8") == "conflict"
        assert result["CopiedFiles"] in (0, 1)
    finally:
        cleanup_dir(root)


def test_copy_recent_files_rename():
    root = make_temp_dir("copy_rename")
    try:
        src = root / "src"
        dst = root / "dst"
        src.mkdir(); dst.mkdir()
        (src / "new.txt").write_text("new", encoding="utf-8")
        (dst / "new.txt").write_text("conflict", encoding="utf-8")

        result = copy_recent_files(src=src, dst=dst, days_threshold=10000, conflict_mode="rename", logger=logging.getLogger("t"))
        assert (dst / "new.txt").read_text(encoding="utf-8") == "conflict"
        assert list(dst.glob("new_*.txt"))
        assert result["CopiedFiles"] >= 1
    finally:
        cleanup_dir(root)


def test_copy_recent_files_invalid_conflict_mode():
    root = make_temp_dir("copy_invalid")
    try:
        src = root / "src"
        dst = root / "dst"
        src.mkdir(); dst.mkdir()
        with pytest.raises(ValueError):
            copy_recent_files(src=src, dst=dst, days_threshold=10000, conflict_mode="overwrite", logger=logging.getLogger("t"))
    finally:
        cleanup_dir(root)


def test_main_with_invalid_dirs(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["CopyScript.py", "--source_dir", "Z:/not_exists", "--dest_dir", "Z:/not_exists2"])
    with pytest.raises(SystemExit) as e:
        CopyScript.main()
    assert e.value.code == 1


def test_main_multi_input_dirs(monkeypatch):
    root = make_temp_dir("copy_main_multi")
    try:
        src1 = root / "src1"
        src2 = root / "src2"
        dst = root / "dst"
        src1.mkdir(); src2.mkdir(); dst.mkdir()
        (src1 / "a.txt").write_text("a", encoding="utf-8")
        (src2 / "b.txt").write_text("b", encoding="utf-8")

        cfg = dict(CopyScript.SCRIPT_CONFIG)
        cfg["INPUT_DIRS"] = [str(src1), str(src2)]
        cfg["INPUT_DIR"] = str(src1)
        cfg["OUTPUT_DIR"] = str(dst)
        cfg["SHUTDOWN_ENABLED"] = False
        monkeypatch.setattr(CopyScript, "SCRIPT_CONFIG", cfg)
        monkeypatch.setattr(sys, "argv", ["CopyScript.py", "--max_age_days", "3650"])

        CopyScript.main()
        assert (dst / "a.txt").exists()
        assert (dst / "b.txt").exists()
    finally:
        cleanup_dir(root)
