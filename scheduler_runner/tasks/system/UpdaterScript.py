"""
UpdaterScript.py

Скрипт для автоматического обновления файлов проекта из git-репозитория.

- Проверяет наличие и правильность настройки origin.
- Сравнивает локальный и удалённый коммиты выбранной ветки.
- При обнаружении обновлений выполняет git pull.
- В случае обновления самого скрипта — перезапускает себя.
- Использует централизованный логгер с разделением на сводные (INFO) и детальные (DEBUG) логи.

Аргументы командной строки:
    --branch <branch>   Ветка для обновления (по умолчанию из конфига)
    --dry-run           Только проверить наличие обновлений, не выполнять pull

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
    """
    Парсит аргументы командной строки.
    --branch: ветка для обновления (по умолчанию из конфига)
    --dry-run: только проверить наличие обновлений, не выполнять pull
    --detailed: включить детализированные логи (DEBUG)
    """
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
    parser.add_argument(
        "--detailed", action="store_true",
        help="Включить детализированные логи (DEBUG)"
    )    
    return parser.parse_args()

def get_local_commit(repo_dir: Path, branch: str) -> Optional[str]:
    """
    Получает хеш последнего локального коммита для указанной ветки.
    Возвращает строку-хеш или None при ошибке.
    """
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
    """
    Получает хеш последнего коммита на удалённой ветке origin/<branch>.
    Возвращает строку-хеш или None при ошибке.
    """
    try:
        result = subprocess.run(
            ["git", "ls-remote", "origin", branch],
            cwd=repo_dir,
            capture_output=True, text=True, check=True
        )
        # stdout вида: "<hash>\trefs/heads/<branch>"
        return result.stdout.split()[0] if result.stdout else None
    except subprocess.CalledProcessError:
        return None

def pull_updates(repo_dir: Path, branch: str, logger) -> bool:
    """
    Выполняет git pull для указанной ветки.
    Логирует stdout на уровне DEBUG, ошибки — на уровне ERROR.
    Возвращает True при успехе, False при ошибке.
    """
    result = subprocess.run(
        ["git", "pull", "origin", branch],
        cwd=repo_dir,
        capture_output=True, text=True
    )
    logger.debug(f"git pull stdout:\n{result.stdout}")
    if result.returncode != 0:
        logger.error(f"git pull stderr:\n{result.stderr}")
        return False
    return True

def ensure_origin(repo_dir: Path, repo_url: str, logger) -> None:
    """
    Проверяет наличие origin и при необходимости настраивает его на repo_url.
    Если origin уже настроен, но url отличается — выводит предупреждение.
    При ошибке добавления origin завершает работу с кодом 5.
    При других ошибках завершает работу с кодом 6.
    """
    try:
        # Проверяем, настроен ли origin
        result = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            cwd=repo_dir,
            capture_output=True, text=True
        )
        if result.returncode != 0:
            # origin не настроен, добавляем
            logger.info(f"origin не найден, настраиваем на {repo_url}")
            add_result = subprocess.run(
                ["git", "remote", "add", "origin", repo_url],
                cwd=repo_dir,
                capture_output=True, text=True
            )
            logger.debug(f"git remote add stdout:\n{add_result.stdout}")
            if add_result.returncode != 0:
                logger.error(f"Не удалось добавить origin: {add_result.stderr.strip()}")
                sys.exit(5)
        else:
            # origin уже настроен, сверяем url
            current_url = result.stdout.strip()
            logger.debug(f"origin get-url stdout: {current_url}")
            if current_url != repo_url:
                logger.warning(f"origin уже настроен на {current_url}, а не на {repo_url}")
    except Exception as e:
        logger.error(f"Ошибка при проверке/установке origin: {e}")
        sys.exit(6)

def main():
    """
    Основная функция:
    - Парсит аргументы.
    - Настраивает логгер.
    - Проверяет и настраивает origin.
    - Проверяет наличие git-репозитория.
    - Сравнивает локальный и удалённый коммиты.
    - При необходимости выполняет обновление.
    - При обновлении самого скрипта — перезапускает себя.
    """
    args = parse_arguments()
    repo_dir = Path(SCRIPT_CONFIG["REPO_DIR"]).resolve()

    # Настройка централизованного логгера
    logger = configure_logger(
        user=SCRIPT_CONFIG["USER"],
        task_name=SCRIPT_CONFIG["TASK_NAME"],
        detailed=args.detailed or SCRIPT_CONFIG["DETAILED_LOGS"],
    )

    logger.info("Проверка обновлений проекта...")
    branch = args.branch

    # Проверка и установка origin
    ensure_origin(repo_dir, SCRIPT_CONFIG["REPO_URL"], logger)

    # Проверяем, что это git-репозиторий
    if not (repo_dir / ".git").exists():
        logger.error("В текущей директории не найден git-репозиторий!")
        sys.exit(2)

    # Получаем локальный и удалённый коммиты
    local_commit = get_local_commit(repo_dir, branch)
    remote_commit = get_remote_commit(repo_dir, branch)
    logger.debug(f"Локальный коммит ({branch}): {local_commit}")
    logger.debug(f"Удалённый коммит ({branch}): {remote_commit}")

    if not local_commit or not remote_commit:
        logger.error("Не удалось получить информацию о коммитах. Проверьте git и подключение к origin.")
        sys.exit(3)

    # Сравниваем коммиты
    if local_commit != remote_commit:
        logger.info("Обнаружены обновления.")
        if args.dry_run:
            logger.info("Режим dry-run: обновление не выполняется.")
            sys.exit(0)
        logger.info("Начинаю обновление...")
        success = pull_updates(repo_dir, branch, logger)
        if success:
            logger.info("Обновление завершено успешно.")
            # Проверяем, обновился ли сам скрипт, и если да — перезапускаем
            script_path = Path(__file__).resolve()
            try:
                # Подробности проверки обновления скрипта в DEBUG
                script_mtime = os.path.getmtime(script_path)
                argv_mtime = os.path.getmtime(sys.argv[0])
                logger.debug(f"mtime скрипта: {script_mtime}, mtime sys.argv[0]: {argv_mtime}")
                if script_mtime != argv_mtime:
                    logger.info("Скрипт обновился, перезапуск...")
                    os.execv(sys.executable, [sys.executable] + sys.argv)
            except Exception as e:
                logger.debug(f"Не удалось проверить или выполнить перезапуск скрипта: {e}")
            sys.exit(0)
        else:
            logger.error("Ошибка при обновлении.")
            sys.exit(4)
    else:
        logger.info("Обновлений не найдено.")
        sys.exit(0)

if __name__ == "__main__":
    main()