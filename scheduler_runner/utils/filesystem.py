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

    - remove_old_dated_folders(target_folder: Path, days_threshold: int, logger, time_budget_seconds=None) -> Dict
        Удаляет целиком папки записей с датой в имени (YYYYMMDD, YYYYMMDDHH) старше порога — без чтения каждого
        файла; для облачного архива, где обход всех файлов не укладывается в таймаут.

    - call_with_timeout(func, timeout, *args, **kwargs) -> Tuple[bool, Any]
        Выполняет func не дольше timeout секунд (для сетевых дисков, которые могут зависнуть).

Примеры использования:
    from scheduler_runner.utils.filesystem import remove_old_files, ensure_directory_exists

    logger = logging.getLogger("mytask")
    remove_old_files(Path("D:/camera"), 30, logger)
    ensure_directory_exists(Path("D:/camera/backup"), logger)

Author: anikinjura
"""
__version__ = '0.0.1'

import os
import re
import shutil
import datetime
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple
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
    Возвращает статистику: {'CopiedFiles': <int>, 'FileCopyingErrors': <int>, 'SkippedFiles': <int>}.
    """
    if conflict_mode not in ("skip", "rename"):
        raise ValueError("Недопустимый режим разрешения конфликтов. Допустимы: 'skip', 'rename'")

    copied_files = 0
    skipped_files = 0
    copying_errors = 0
    now = datetime.datetime.now()
    threshold_time = now - datetime.timedelta(days=days_threshold)

    logger.info(f"Начало копирования файлов из {src} в {dst} за последние {days_threshold} дней")

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
                            logger.debug(f"Файл пропущен (уже существует): {dst_file}")  # итог — одной строкой в конце
                            skipped_files += 1
                            continue
                        elif conflict_mode == "rename":
                            counter = 1
                            new_dst_file = dst_file
                            while new_dst_file.exists():
                                new_dst_file = dst_file.with_name(f"{dst_file.stem}_{counter:03d}{dst_file.suffix}")
                                counter += 1
                            logger.info(f"Файл будет переименован: {src_file} -> {new_dst_file}")  # Теперь INFO
                            dst_file = new_dst_file

                    shutil.copy2(src_file, dst_file)
                    copied_files += 1
                    logger.info(f"Скопирован файл: {src_file} -> {dst_file}")  # Теперь INFO
                    
            except PermissionError as pe:
                copying_errors += 1
                logger.warning(f"Нет прав для копирования файла {src_file}: {pe}")
            except FileNotFoundError as fnfe:
                copying_errors += 1
                logger.warning(f"Файл не найден для копирования {src_file}: {fnfe}")
            except Exception as e:
                copying_errors += 1
                logger.warning(f"Ошибка при копировании файла {src_file}: {e}")
                if copying_errors > 5:
                    logger.error("Слишком много ошибок копирования, остановка.")
                    raise RuntimeError("Слишком много ошибок копирования")

    logger.info(f"Копирование завершено. Скопировано: {copied_files}, пропущено: {skipped_files}, ошибок: {copying_errors}")
    return {"CopiedFiles": copied_files, "FileCopyingErrors": copying_errors, "SkippedFiles": skipped_files}

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


DATED_FOLDER_PATTERN = re.compile(r"^(\d{8})(\d{2})?$")


def _dated_folder_span(name: str) -> Optional[Tuple[datetime.datetime, datetime.datetime]]:
    """Начало и конец периода записей по имени папки: YYYYMMDD — сутки, YYYYMMDDHH — час; иначе None."""
    match = DATED_FOLDER_PATTERN.match(name)
    if not match:
        return None
    try:
        if match.group(2) is None:
            start = datetime.datetime.strptime(match.group(1), "%Y%m%d")
            return start, start + datetime.timedelta(days=1)
        start = datetime.datetime.strptime(name, "%Y%m%d%H")
        return start, start + datetime.timedelta(hours=1)
    except ValueError:
        return None


def remove_old_dated_folders(
    target_folder: Path,
    days_threshold: int,
    logger: logging.Logger,
    time_budget_seconds: Optional[float] = None,
    max_depth: int = 4,
    now: Optional[datetime.datetime] = None,
) -> Dict[str, Any]:
    """
    Удаляет целиком папки записей, у которых дата в имени старше days_threshold дней.

    Записи камер лежат в папках с датой: UNV — unv_camera/<uid>/YYYYMMDD/HH/..., Xiaomi —
    xiaomi_camera_videos/<uid>/YYYYMMDDHH/.... Функция листает только каталоги до уровня даты (не глубже max_depth) и
    не заходит внутрь папок с датой: в облаке это сотни запросов вместо сотен тысяч (обход всех файлов не укладывался
    в таймаут — 143, 144, 10.2026). Папка удаляется, если весь ее период (сутки/час) старше порога.

    Самые старые папки удаляются первыми; при time_budget_seconds удаление останавливается по исчерпании времени —
    следующий запуск продолжит с оставшихся.

    Возвращает: removed_folders, errors, remaining_old (старые, но не удаленные из-за лимита времени),
    oldest_kept (YYYY-MM-DD самой старой оставшейся папки с датой или None), budget_exhausted.
    """
    started = time.monotonic()
    current = now or datetime.datetime.now()
    cutoff = current - datetime.timedelta(days=days_threshold)
    old: list = []
    kept_starts: list = []
    errors = 0

    pending = [(Path(target_folder), 0)]
    while pending:
        folder, depth = pending.pop()
        try:
            entries = [entry for entry in os.scandir(folder) if entry.is_dir(follow_symlinks=False)]
        except OSError as e:
            logger.warning(f"Ошибка при чтении папки {folder}: {e}")
            errors += 1
            continue
        for entry in entries:
            span = _dated_folder_span(entry.name)
            if span is None:
                if depth + 1 < max_depth:
                    pending.append((Path(entry.path), depth + 1))
            elif span[1] <= cutoff:
                old.append((span[0], Path(entry.path)))
            else:
                kept_starts.append(span[0])

    old.sort(key=lambda item: item[0])
    removed = 0
    budget_exhausted = False
    for index, (start, path) in enumerate(old):
        if time_budget_seconds is not None and time.monotonic() - started >= time_budget_seconds:
            budget_exhausted = True
            kept_starts.extend(item[0] for item in old[index:])
            break
        try:
            shutil.rmtree(path)
            removed += 1
            logger.debug(f"Удалена папка записей: {path}")
        except OSError as e:
            errors += 1
            kept_starts.append(start)
            logger.warning(f"Ошибка при удалении папки {path}: {e}")

    remaining_old = len(old) - removed
    oldest_kept = min(kept_starts).strftime("%Y-%m-%d") if kept_starts else None
    return {
        "removed_folders": removed,
        "errors": errors,
        "remaining_old": remaining_old,
        "oldest_kept": oldest_kept,
        "budget_exhausted": budget_exhausted,
    }


def call_with_timeout(func: Callable[..., Any], timeout: float, *args, **kwargs) -> Tuple[bool, Any]:
    """
    Выполняет func(*args, **kwargs) в фоновом потоке и ждет не дольше timeout секунд.

    Нужна для обращений к сетевому/облачному диску: зависший «Диск-О:» не возвращает даже os.stat, и задача висит до
    таймаута runner, который убивает ее раньше, чем она успеет отправить уведомление (144, 05–10.10.2026).

    Возвращает (True, результат), если func успела, и (False, None), если нет; исключение func пробрасывается.
    Зависший вызов остается в потоке-демоне и не мешает завершению процесса.
    """
    box: Dict[str, Any] = {}

    def target():
        try:
            box["result"] = func(*args, **kwargs)
        except BaseException as exc:  # noqa: B036 — передаем вызывающему как есть
            box["error"] = exc

    worker = threading.Thread(target=target, name=f"timeout:{getattr(func, '__name__', 'call')}", daemon=True)
    worker.start()
    worker.join(timeout)
    if worker.is_alive():
        return False, None
    if "error" in box:
        raise box["error"]
    return True, box.get("result")
