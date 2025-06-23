import pytest
import logging
import os
from pathlib import Path
from unittest import mock

from scheduler_runner.tasks.cameras.CopyScript import main as copy_main
from scheduler_runner.utils.filesystem import copy_recent_files

@pytest.fixture
def temp_dirs(tmp_path):
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.mkdir()
    dst.mkdir()
    # Старый файл (не должен копироваться)
    old_file = src / "old.txt"
    old_file.write_text("old")
    os.utime(old_file, (1, 1))
    # Новый файл (должен копироваться)
    new_file = src / "new.txt"
    new_file.write_text("new")
    return src, dst

def test_copy_recent_files_skip(temp_dirs):
    src, dst = temp_dirs
    # Создаём файл-конфликт в dst
    conflict_file = dst / "new.txt"
    conflict_file.write_text("conflict")
    logger = logging.getLogger("test_copy_skip")
    result = copy_recent_files(
        src=src,
        dst=dst,
        days_threshold=10000,
        conflict_mode="skip",
        logger=logger
    )
    # Файл не должен быть перезаписан
    assert (dst / "new.txt").read_text() == "conflict"
    assert result["CopiedFiles"] == 0 or result["CopiedFiles"] == 1  # зависит от времени создания

def test_copy_recent_files_rename(temp_dirs):
    src, dst = temp_dirs
    # Создаём файл-конфликт в dst
    conflict_file = dst / "new.txt"
    conflict_file.write_text("conflict")
    logger = logging.getLogger("test_copy_rename")
    result = copy_recent_files(
        src=src,
        dst=dst,
        days_threshold=10000,
        conflict_mode="rename",
        logger=logger
    )
    # Оригинальный конфликтный файл должен остаться, новый файл с суффиксом должен появиться
    assert (dst / "new.txt").read_text() == "conflict"
    renamed = list(dst.glob("new_*.txt"))
    assert renamed
    assert any("new" in f.name for f in renamed)
    assert result["CopiedFiles"] >= 1

def test_copy_recent_files_invalid_conflict_mode(temp_dirs):
    src, dst = temp_dirs
    logger = logging.getLogger("test_copy_invalid")
    with pytest.raises(ValueError):
        copy_recent_files(
            src=src,
            dst=dst,
            days_threshold=10000,
            conflict_mode="overwrite",  # не поддерживается
            logger=logger
        )

def test_main_with_invalid_dirs(monkeypatch):
    # Проверяем, что main завершится с sys.exit(1) при невалидных директориях
    import sys
    from scheduler_runner.tasks.cameras import CopyScript

    test_args = [
        "CopyScript.py",
        "--source_dir", "Z:/not_exists",
        "--dest_dir", "Z:/not_exists2"
    ]
    monkeypatch.setattr(sys, "argv", test_args)
    with pytest.raises(SystemExit) as e:
        CopyScript.main()
    assert e.value.code == 1

def test_main_with_shutdown(monkeypatch, tmp_path):
    """
    Проверяет, что при передаче --shutdown вызывается SystemUtils.shutdown_computer.
    """
    import sys
    from scheduler_runner.tasks.cameras import CopyScript

    # Создаём временные директории для теста
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.mkdir()
    dst.mkdir()
    (src / "file.txt").write_text("data")

    # Подменяем sys.argv для передачи аргументов
    test_args = [
        "CopyScript.py",
        "--source_dir", str(src),
        "--dest_dir", str(dst),
        "--shutdown"
    ]
    monkeypatch.setattr(sys, "argv", test_args)

    # Мокаем функцию shutdown_computer
    called = {}

    def fake_shutdown_computer(logger, force):
        called["shutdown"] = True

    monkeypatch.setattr(CopyScript.SystemUtils, "shutdown_computer", fake_shutdown_computer)

    # Запускаем main
    CopyScript.main()

    assert called.get("shutdown") is True