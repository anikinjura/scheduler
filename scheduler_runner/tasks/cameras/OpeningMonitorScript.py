"""
OpeningMonitorScript.py

Скрипт для определения времени начала работы объекта по первому видеофайлу.

- Сканирует директорию с видеозаписями.
- Ищет файлы, созданные сегодня в заданном временном интервале (например, с 8 до 10 утра).
- Определяет время создания файла по его имени, поддерживая форматы:
  - `ЧЧ-ММ-СС.jpg` (камеры UNV)
  - `..._unix-timestamp.mp4` (камеры Xiaomi)
- Отправляет в Telegram сообщение о времени первого найденного файла или об их отсутствии.

Author: anikinjura
"""
__version__ = '1.0.0'

import argparse
import sys
import os
import re
from datetime import datetime, time, date
from pathlib import Path
from typing import Optional, List, Tuple
from logging import Logger

# Добавляем корень проекта в sys.path для корректного импорта утилит
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from scheduler_runner.tasks.cameras.config.scripts.openingmonitor_config import SCRIPT_CONFIG
from scheduler_runner.utils.logging import configure_logger
from scheduler_runner.utils.notify import send_telegram_message
from scheduler_runner.utils.filesystem import FileSystemUtils

def _parse_time_from_filename(filename: str) -> Optional[time]:
    """
    Извлекает время из имени файла на основе известных форматов.

    Поддерживаемые форматы:
    1. `ЧЧ-ММ-СС.jpg` (UNV)
    2. `*_<unix_timestamp>.mp4` (Xiaomi)

    :param filename: Имя файла.
    :return: Объект `datetime.time` или None, если формат не распознан.
    """
    # Попытка 1: формат UNV (e.g., 08-53-34.jpg)
    match_unv = re.match(r'(\d{2}-\d{2}-\d{2})\.jpg', filename)
    if match_unv:
        try:
            return time.fromisoformat(match_unv.group(1).replace('-', ':'))
        except ValueError:
            return None

    # Попытка 2: формат Xiaomi (e.g., 04M22S_1751868262.mp4)
    match_xiaomi = re.search(r'_(\d{10})\.mp4', filename)
    if match_xiaomi:
        try:
            unix_timestamp = int(match_xiaomi.group(1))
            return datetime.fromtimestamp(unix_timestamp).time()
        except (ValueError, OSError):
            return None
            
    return None

def find_earliest_file_time(
    search_dir: Path, 
    start_time: time, 
    end_time: time, 
    logger: Logger
) -> Optional[time]:
    """
    Находит самое раннее время файла в директории за сегодняшний день
    в указанном временном диапазоне.

    :param search_dir: Директория для поиска.
    :param start_time: Начало временного окна.
    :param end_time: Конец временного окна.
    :param logger: Экземпляр логгера.
    :return: Самое раннее время или None, если файлы не найдены.
    """
    earliest_time: Optional[time] = None
    today = date.today()
    
    logger.info(f"Поиск файлов за {today} в директории: {search_dir}")

    try:
        for item in os.scandir(search_dir):
            if item.is_file():
                try:
                    # Проверяем, что файл создан сегодня
                    mtime = datetime.fromtimestamp(item.stat().st_mtime).date()
                    if mtime != today:
                        continue

                    file_time = _parse_time_from_filename(item.name)
                    
                    if file_time and start_time <= file_time <= end_time:
                        logger.debug(f"Найден файл: {item.name} (время: {file_time})")
                        if earliest_time is None or file_time < earliest_time:
                            earliest_time = file_time
                            logger.info(f"Новое самое раннее время: {earliest_time} (файл: {item.name})")

                except (FileNotFoundError, OSError) as e:
                    logger.warning(f"Не удалось обработать файл {item.name}: {e}")
                    continue
    except FileNotFoundError:
        logger.error(f"Директория для поиска не найдена: {search_dir}")
        return None

    return earliest_time

def main():
    """
    Основная функция скрипта.
    """
    parser = argparse.ArgumentParser(description="Мониторинг времени начала работы объекта.")
    parser.add_argument("--detailed_logs", action="store_true", help="Включить детализированные логи.")
    args = parser.parse_args()

    detailed_logs = args.detailed_logs or SCRIPT_CONFIG.get("DETAILED_LOGS", False)
    
    logger = configure_logger(
        user=SCRIPT_CONFIG["USER"],
        task_name=SCRIPT_CONFIG["TASK_NAME"],
        detailed=detailed_logs
    )

    try:
        search_dir = Path(SCRIPT_CONFIG["SEARCH_DIR"])
        start_time = time.fromisoformat(SCRIPT_CONFIG["START_TIME"])
        end_time = time.fromisoformat(SCRIPT_CONFIG["END_TIME"])
        
        token = SCRIPT_CONFIG.get("TELEGRAM_TOKEN")
        chat_id = SCRIPT_CONFIG.get("TELEGRAM_CHAT_ID")

        pvz_id = SCRIPT_CONFIG.get("PVZ_ID", "-")

        if not isinstance(token, str) or not isinstance(chat_id, str) or not token or not chat_id:
            logger.critical("Токен или ID чата Telegram не заданы в конфигурации!")
            sys.exit(1)

        logger.info(f"Запуск мониторинга. Временной интервал: {start_time} - {end_time}")

        earliest_time = find_earliest_file_time(search_dir, start_time, end_time, logger)

        if earliest_time:
            message = f"✅ ПВЗ: {pvz_id}. Объект начал работу в {earliest_time.strftime('%H:%M:%S')}."
            logger.info(message)
        else:
            message = f"⚠️ ПВЗ: {pvz_id}. Объект не начал работу до {end_time.strftime('%H:%M')}. Видеофайлы не обнаружены."
            logger.warning(message)

        send_telegram_message(token, chat_id, message, logger)

    except Exception as e:
        logger.critical(f"Критическая ошибка выполнения: {e}", exc_info=True)
        sys.exit(1)

    logger.info("Мониторинг успешно завершен.")

if __name__ == "__main__":
    main()
