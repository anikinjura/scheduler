"""
UpdaterScript.py

Скрипт для автоматического обновления файлов проекта из git-репозитория.
Работает как задача ядра, использует централизованный конфиг и логгер.

Author: anikinjura
"""
__version__ = '0.0.1'

from typing import Optional
import subprocess
import sys
import os
from pathlib import Path
import argparse

from scheduler_runner.tasks.system.config.scripts.updater_config import SCRIPT_CONFIG
from scheduler_runner.utils.logging import configure_logger

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Автоматическое обновление файлов проекта из git-репозитория"
    )
    parser.add_argument(
        "--branch", type=str, default=SCRIPT_CONFIG["BRANCH"],
        help=f"Ветка для обновления (по умолчанию: {SCRIPT_CONFIG['BRANCH']})"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Только проверить наличие обновлений, не выполнять pull"
    )
    return parser.parse_args()

def get_local_commit(repo_dir: Path, branch: str) -> Optional[str]:
    try:
        result = subprocess.run(
            ["git", "rev-parse", branch],
            cwd=repo_dir,
            capture_output=True, text=True, check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return None

def get_remote_commit(repo_dir: Path, branch: str) -> Optional[str]:
    try:
        result = subprocess.run(
            ["git", "ls-remote", "origin", branch],
            cwd=repo_dir,
            capture_output=True, text=True, check=True
        )
        return result.stdout.split()[0] if result.stdout else None
    except subprocess.CalledProcessError:
        return None

def pull_updates(repo_dir: Path, branch: str, logger) -> bool:
    result = subprocess.run(
        ["git", "pull", "origin", branch],
        cwd=repo_dir,
        capture_output=True, text=True
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        return False
    return True

def main():
    args = parse_arguments()
    repo_dir = Path(SCRIPT_CONFIG["REPO_DIR"]).resolve()

    logger = configure_logger(
        user=SCRIPT_CONFIG["USER"],
        task_name=SCRIPT_CONFIG["TASK_NAME"],
        detailed=SCRIPT_CONFIG["DETAILED_LOGS"],
    )

    logger.info("Проверка обновлений проекта...")
    branch = args.branch

    if not (repo_dir / ".git").exists():
        logger.error("В текущей директории не найден git-репозиторий!")
        sys.exit(2)

    local_commit = get_local_commit(repo_dir, branch)
    remote_commit = get_remote_commit(repo_dir, branch)
    logger.info(f"Локальный коммит ({branch}): {local_commit}")
    logger.info(f"Удалённый коммит ({branch}): {remote_commit}")

    if not local_commit or not remote_commit:
        logger.error("Не удалось получить информацию о коммитах. Проверьте git и подключение к origin.")
        sys.exit(3)

    if local_commit != remote_commit:
        logger.info("Обнаружены обновления.")
        if args.dry_run:
            logger.info("Режим dry-run: обновление не выполняется.")
            sys.exit(0)
        logger.info("Начинаю обновление...")
        success = pull_updates(repo_dir, branch, logger)
        if success:
            logger.info("Обновление завершено успешно.")
            # Если обновился сам скрипт, перезапустить себя
            script_path = Path(__file__).resolve()
            if os.path.getmtime(script_path) != os.path.getmtime(sys.argv[0]):
                logger.info("Скрипт обновился, перезапуск...")
                os.execv(sys.executable, [sys.executable] + sys.argv)
            sys.exit(0)
        else:
            logger.error("Ошибка при обновлении.")
            sys.exit(4)
    else:
        logger.info("Обновлений не найдено.")
        sys.exit(0)

if __name__ == "__main__":
    main()