"""
filesystem.py

Утилиты для работы с файловой системой: удаление старых файлов, удаление пустых папок,
копирование файлов, создание директорий, валидация путей.

Функции:
    - remove_old_files(target_folder: Path, days_threshold: int, logger: logging.Logger) -> Dict[str, int]
        Удаляет файлы в target_folder, которые старше days_threshold дней.
        Возвращает статистику: {'removed': <int>, 'errors': <int>}.

    - remove_empty_folders(target_folder: Path, logger: logging.Logger) -> Dict[str, int]
        Рекурсивно удаляет пустые папки в target_folder.
        Возвращает статистику: {'removed': <int>, 'errors': <int>}.

    - copy_recent_files(src: Path, dst: Path, days_threshold: int, logger: logging.Logger) -> Dict[str, int]
        Копирует файлы из src в dst, которые были изменены за последние days_threshold дней.
        Возвращает статистику: {'copied': <int>, 'errors': <int>}.

    - ensure_directory_exists(path: Path, logger: Optional[logging.Logger] = None) -> bool
        Создаёт директорию, если она не существует. Логирует создание.

    - FileSystemUtils.validate_readable_path(path: Path) -> bool
        Проверяет, существует ли путь и доступен ли для чтения.

    - FileSystemUtils.validate_writable_path(path: Path) -> bool
        Проверяет, существует ли путь и доступен ли для записи.

Примеры использования:
    from scheduler_runner.utils.filesystem import remove_old_files, ensure_directory_exists

    logger = logging.getLogger("mytask")
    remove_old_files(Path("D:/camera"), 30, logger)
    ensure_directory_exists(Path("D:/camera/backup"), logger)

Author: anikinjura
"""
__version__ = '0.0.1'

import os
import shutil
import datetime
from pathlib import Path
from typing import Dict, Optional
import logging

def remove_old_files(target_folder: Path, days_threshold: int, logger: logging.Logger) -> Dict[str, int]:
    """
    Удаляет файлы в target_folder, которые старше days_threshold дней.
    Возвращает статистику: {'removed': <int>, 'errors': <int>}.
    """
    removed = 0
    errors = 0
    now = datetime.datetime.now()
    for root, _, files in os.walk(target_folder):
        for file in files:
            file_path = Path(root) / file
            try:
                mtime = datetime.datetime.fromtimestamp(file_path.stat().st_mtime)
                if (now - mtime).days > days_threshold:
                    file_path.unlink()
                    logger.debug(f"Удалён старый файл: {file_path}")
                    removed += 1
            except Exception as e:
                logger.warning(f"Ошибка при удалении файла {file_path}: {e}")
                errors += 1
    return {'removed': removed, 'errors': errors}

def remove_empty_folders(target_folder: Path, logger: logging.Logger) -> Dict[str, int]:
    """
    Рекурсивно удаляет пустые папки в target_folder.
    Возвращает статистику: {'removed': <int>, 'errors': <int>}.
    """
    removed = 0
    errors = 0
    for root, dirs, _ in os.walk(target_folder, topdown=False):
        for d in dirs:
            dir_path = Path(root) / d
            try:
                if not any(dir_path.iterdir()):
                    dir_path.rmdir()
                    logger.debug(f"Удалена пустая папка: {dir_path}")
                    removed += 1
            except Exception as e:
                logger.warning(f"Ошибка при удалении папки {dir_path}: {e}")
                errors += 1
    return {'removed': removed, 'errors': errors}

def copy_recent_files(
    src: Path,
    dst: Path,
    days_threshold: int,
    conflict_mode: str,
    logger: logging.Logger
) -> Dict[str, int]:
    """
    Копирует файлы из src в dst, которые были изменены за последние days_threshold дней.
    Обрабатывает конфликты: skip/rename.
    Возвращает статистику: {'CopiedFiles': <int>, 'FileCopyingErrors': <int>}.
    """
    if conflict_mode not in ("skip", "rename"):
        raise ValueError("Недопустимый режим разрешения конфликтов. Допустимы: 'skip', 'rename'")

    copied_files = 0
    copying_errors = 0
    now = datetime.datetime.now()
    threshold_time = now - datetime.timedelta(days=days_threshold)

    for root, _, files in os.walk(src):
        for file in files:
            src_file = Path(root) / file
            try:
                mtime = datetime.datetime.fromtimestamp(src_file.stat().st_mtime)
                # Копируем, если файл модифицирован позже (то есть младше по возрасту) чем пороговое время.
                if mtime >= threshold_time:
                    rel_path = src_file.relative_to(src)
                    dst_file = dst / rel_path
                    dst_file.parent.mkdir(parents=True, exist_ok=True)

                    # Обработка конфликтов
                    if dst_file.exists():
                        if conflict_mode == "skip":
                            logger.debug(f"Пропущен файл (уже существует): {dst_file}")
                            continue
                        elif conflict_mode == "rename":
                            counter = 1
                            new_dst_file = dst_file
                            while new_dst_file.exists():
                                new_dst_file = dst_file.with_name(f"{dst_file.stem}_{counter:03d}{dst_file.suffix}")
                                counter += 1
                            logger.debug(f"Файл {src_file} переименован в {new_dst_file}")
                            dst_file = new_dst_file

                    shutil.copy2(src_file, dst_file)
                    copied_files += 1
                    logger.debug(f"Скопирован файл: {src_file} -> {dst_file}")
            except Exception as e:
                copying_errors += 1
                logger.warning(f"Ошибка при копировании файла {src_file}: {e}")
                if copying_errors > 5:
                    logger.error("Слишком много ошибок копирования, остановка.")
                    raise RuntimeError("Слишком много ошибок копирования")

    return {"CopiedFiles": copied_files, "FileCopyingErrors": copying_errors}

def ensure_directory_exists(path: Path, logger: Optional[logging.Logger] = None) -> bool:
    """
    Создаёт директорию, если она не существует. Логирует создание.
    Возвращает True, если директория существует или была создана, иначе False.
    """
    try:
        path.mkdir(parents=True, exist_ok=True)
        if logger:
            logger.debug(f"Папка существует или создана: {path}")
        return True
    except Exception as e:
        if logger:
            logger.error(f"Ошибка при создании папки {path}: {e}")
        return False

class FileSystemUtils:
    @staticmethod
    def validate_readable_path(path: Path) -> Optional[Path]:
        """
        Проверяет, существует ли путь и доступен ли для чтения.
        Возвращает Path, если путь валиден, иначе None.
        """
        if path.exists() and os.access(path, os.R_OK):
            return path
        return None

    @staticmethod
    def validate_writable_path(path: Path) -> Optional[Path]:
        """
        Проверяет, существует ли путь и доступен ли для записи.
        Возвращает Path, если путь валиден, иначе None.
        """
        if path.exists() and os.access(path, os.W_OK):
            return path
        return None