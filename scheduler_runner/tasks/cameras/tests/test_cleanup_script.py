"""
Юнит-тесты для скрипта CleanupScript задачи cameras.

Покрытие:
    - Проверка получения конфигурации сценария (get_scenario_config)
    - Проверка удаления старых файлов и пустых папок (execute_cleanup_operations)
    - Проверка обработки ошибок
    - Использование временных директорий для изоляции тестов

Запуск:
    pytest scheduler_runner/tasks/cameras/tests/test_cleanup_script.py

Author: anikinjura
"""

from unittest import mock
import pytest
import logging
import os
from pathlib import Path

from scheduler_runner.tasks.cameras.CleanupScript import (
    get_scenario_config,
    execute_cleanup_operations,
)

@pytest.fixture
def temp_dir(tmp_path):
    """
    Создаёт временную директорию с файлами разного возраста и вложенными папками.
    """
    d = tmp_path / "test_cleanup"
    d.mkdir()
    # Старый файл (mtime = 1)
    old_file = d / "old_file.txt"
    old_file.write_text("old")
    os.utime(old_file, (1, 1))
    # Новый файл (mtime = сейчас)
    new_file = d / "new_file.txt"
    new_file.write_text("new")
    # Пустая папка
    empty_folder = d / "empty_folder"
    empty_folder.mkdir()
    # Папка с файлом
    non_empty_folder = d / "non_empty_folder"
    non_empty_folder.mkdir()
    (non_empty_folder / "file.txt").write_text("data")
    return d

def test_get_scenario_config_valid():
    """
    Проверяет, что get_scenario_config возвращает корректный конфиг для существующего сценария.
    """
    config = get_scenario_config("local")
    assert "CLEANUP_DIR" in config
    assert "MAX_AGE_DAYS" in config
    assert "DETAILED_LOGS" in config

def test_get_scenario_config_invalid():
    """
    Проверяет, что get_scenario_config выбрасывает ValueError для несуществующего сценария.
    """
    with pytest.raises(ValueError):
        get_scenario_config("unknown")

def test_execute_cleanup_operations_removes_old_files_and_empty_folders(temp_dir):
    """
    Проверяет, что execute_cleanup_operations удаляет старые файлы и пустые папки.
    """
    logger = logging.getLogger("test_cleanup")
    # MAX_AGE_DAYS = 0, удалит только old_file.txt (mtime=1)
    execute_cleanup_operations(logger, temp_dir, max_age_days=0)
    files = [f.name for f in temp_dir.iterdir()]
    assert "new_file.txt" in files
    assert "old_file.txt" not in files
    assert "empty_folder" not in files  # пустая папка должна быть удалена
    assert "non_empty_folder" in files  # не пустая папка должна остаться

def test_execute_cleanup_operations_handles_errors(tmp_path, caplog):
    """
    Проверяет, что ошибки при удалении файлов корректно логируются.
    """
    d = tmp_path / "test_cleanup"
    d.mkdir()
    error_file = d / "error.txt"
    error_file.write_text("fail")
    # Сделаем файл очень старым, чтобы он точно попал под удаление
    os.utime(error_file, (1, 1))

    logger = logging.getLogger("test_cleanup_error")

    def unlink_side_effect(self, *args, **kwargs):
        if self.name == "error.txt":
            raise PermissionError("Mocked permission error")
        return original_unlink(self, *args, **kwargs)

    original_unlink = Path.unlink

    with mock.patch("pathlib.Path.unlink", new=unlink_side_effect):
        with caplog.at_level(logging.WARNING):
            execute_cleanup_operations(logger, d, max_age_days=0)
            assert "Ошибка при удалении файла" in caplog.text