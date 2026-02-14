"""
OpeningMonitorScript.py

Скрипт для определения времени начала работы объекта по первому видеофайлу и времени включения компьютера.

- Сканирует директорию с видеозаписями.
- Ищет файлы, созданные сегодня в заданном временном интервале (например, с 8 до 10 утра).
- Определяет время создания файла по его имени, поддерживая форматы:
  - `ЧЧ-ММ-СС.jpg` (камеры UNV)
  - `..._unix-timestamp.mp4` (камеры Xiaomi)
- Получает время включения компьютера из системных журналов
- Анализирует оба источника данных для определения времени начала работы
- Отправляет в Telegram сообщение о времени начала работы на основе комбинированного анализа.

Author: anikinjura
"""
__version__ = '2.0.0'

import argparse
import sys
import os
import re
import subprocess
from datetime import datetime, time, date, timedelta
from pathlib import Path
from typing import Optional, List, Tuple
from logging import Logger

# Добавляем корень проекта в sys.path для корректного импорта утилит
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from scheduler_runner.tasks.cameras.config.scripts.openingmonitor_config import SCRIPT_CONFIG
from scheduler_runner.utils.logging import configure_logger, TRACE_LEVEL
from scheduler_runner.utils.notifications import send_notification, test_connection as test_notification_connection
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
    в указанном временном диапазоне. Рекурсивно сканирует все подкаталоги.

    :param search_dir: Директория для поиска.
    :param start_time: Начало временного окна.
    :param end_time: Конец временного окна.
    :param logger: Экземпляр логгера.
    :return: Самое раннее время или None, если файлы не найдены.
    """
    earliest_time: Optional[time] = None
    today = date.today()
    
    logger.info(f"Поиск файлов за {today} в директории: {search_dir} (и всех подкаталогах)")

    try:
        # Рекурсивный обход всех подкаталогов
        for root, dirs, files in os.walk(search_dir):
            for filename in files:
                try:
                    file_path = Path(root) / filename
                    
                    # Проверяем, что файл создан сегодня
                    mtime = datetime.fromtimestamp(file_path.stat().st_mtime).date()
                    if mtime != today:
                        continue

                    file_time = _parse_time_from_filename(filename)
                    
                    if file_time and start_time <= file_time <= end_time:
                        logger.debug(f"Найден файл: {file_path} (время: {file_time})")
                        if earliest_time is None or file_time < earliest_time:
                            earliest_time = file_time
                            logger.info(f"Новое самое раннее время: {earliest_time} (файл: {file_path})")

                except (FileNotFoundError, OSError, ValueError) as e:
                    logger.warning(f"Не удалось обработать файл {Path(root) / filename}: {e}")
                    continue
    except Exception as e:
        logger.error(f"Ошибка при рекурсивном сканировании: {e}")
        return None

    return earliest_time

def get_system_boot_time() -> Optional[datetime]:
    """
    Получает время последнего включения системы через PowerShell
    
    Returns:
        datetime: Время последнего включения системы или None в случае ошибки
    """
    try:
        # Команда для получения времени последнего включения системы
        ps_command = "[DateTime]::Now.ToString('yyyy-MM-dd HH:mm:ss')"
        result = subprocess.run(['powershell', '-Command', f'(Get-CimInstance -ClassName Win32_OperatingSystem).LastBootUpTime.ToString("yyyy-MM-dd HH:mm:ss")'], 
                               capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            boot_time_str = result.stdout.strip()
            if boot_time_str and boot_time_str != '':
                # Преобразуем строку в datetime объект
                boot_time = datetime.strptime(boot_time_str, '%Y-%m-%d %H:%M:%S')
                return boot_time
    except subprocess.TimeoutExpired:
        print("Время ожидания выполнения команды истекло")
    except Exception as e:
        print(f"Ошибка при получении времени включения системы: {e}")
    return None


def get_wake_time() -> Optional[datetime]:
    """
    Получает время последнего выхода из спящего режима/гибернации из системного журнала событий

    Returns:
        datetime: Время последнего выхода из спящего режима или None в случае ошибки или отсутствия события
    """
    try:
        # Команда для получения последнего события ID 1 (Power Action - выход из гибернации/спящего режима)
        ps_command = "(Get-WinEvent -FilterHashtable @{LogName='System'; ID=1; StartTime=(Get-Date).AddDays(-1)} | Select-Object -First 1).TimeCreated.ToString('yyyy-MM-dd HH:mm:ss')"
        result = subprocess.run(['powershell', '-Command', ps_command],
                               capture_output=True, text=True, timeout=15)
        if result.returncode == 0:
            wake_time_str = result.stdout.strip()
            if wake_time_str and wake_time_str != '':
                # Преобразуем строку в datetime объект
                wake_time = datetime.strptime(wake_time_str, '%Y-%m-%d %H:%M:%S')
                return wake_time
    except subprocess.TimeoutExpired:
        print("Время ожидания выполнения команды истекло")
    except Exception as e:
        print(f"Ошибка при получении времени выхода из спящего режима: {e}")
    return None


def determine_start_time(camera_time: Optional[time], boot_time: Optional[datetime], wake_time: Optional[datetime], logger: Logger) -> tuple[Optional[time], str]:
    """
    Анализирует все источники данных и определяет итоговое время начала работы и источник данных
    
    Args:
        camera_time: Время первого кадра с камер
        boot_time: Время включения компьютера
        wake_time: Время выхода из спящего режима/гибернации
        logger: Логгер для записи информации
        
    Returns:
        tuple: (итоговое время начала работы, источник данных)
    """
    today = date.today()
    
    # Если есть время включения компьютера, извлекаем время суток
    boot_time_of_day = None
    if boot_time:
        if boot_time.date() == today:
            boot_time_of_day = boot_time.time()
        else:
            logger.info(f"Время включения компьютера ({boot_time}) не относится к сегодняшней дате")
    
    # Если есть время выхода из спящего режима, извлекаем время суток
    wake_time_of_day = None
    if wake_time:
        if wake_time.date() == today:
            wake_time_of_day = wake_time.time()
        else:
            logger.info(f"Время выхода из спящего режима ({wake_time}) не относится к сегодняшней дате")
    
    # Получаем параметры из конфигурации
    priority_source = SCRIPT_CONFIG.get("PRIORITY_SOURCE", "both")
    tolerance_minutes = SCRIPT_CONFIG.get("BOOT_TIME_TOLERANCE_MINUTES", 30)
    
    # Определяем доступные источники данных на сегодня
    available_sources = []
    if camera_time:
        available_sources.append(("камеры", camera_time))
    if boot_time_of_day:
        available_sources.append(("включение компьютера", boot_time_of_day))
    if wake_time_of_day:
        available_sources.append(("выход из сна", wake_time_of_day))
    
    # Если нет доступных источников
    if not available_sources:
        logger.info("Ни один из источников данных недоступен")
        return None, "отсутствие данных"
    
    # Если только один источник доступен
    if len(available_sources) == 1:
        source_name, source_time = available_sources[0]
        logger.info(f"Используется только один доступный источник: {source_name} ({source_time})")
        return source_time, source_name
    
    # Если несколько источников доступны
    if priority_source == "both" or priority_source == "all":
        # Сортируем источники по времени (берем самый ранний)
        sorted_sources = sorted(available_sources, key=lambda x: x[1])
        earliest_source, earliest_time = sorted_sources[0]
        logger.info(f"Выбрано самое раннее время: {earliest_time} из источника '{earliest_source}'")
        return earliest_time, earliest_source
    elif priority_source == "camera":
        # Приоритет у камер
        if camera_time:
            logger.info(f"Приоритет у камер: {camera_time}")
            return camera_time, "камеры"
        elif wake_time_of_day:
            logger.info(f"Вторичный выбор - время выхода из сна: {wake_time_of_day}")
            return wake_time_of_day, "выход из сна"
        elif boot_time_of_day:
            logger.info(f"Вторичный выбор - время включения компьютера: {boot_time_of_day}")
            return boot_time_of_day, "включение компьютера"
    elif priority_source == "wake_time":
        # Приоритет у времени выхода из сна
        if wake_time_of_day:
            logger.info(f"Приоритет у времени выхода из сна: {wake_time_of_day}")
            return wake_time_of_day, "выход из сна"
        elif camera_time:
            logger.info(f"Вторичный выбор - время с камер: {camera_time}")
            return camera_time, "камеры"
        elif boot_time_of_day:
            logger.info(f"Вторичный выбор - время включения компьютера: {boot_time_of_day}")
            return boot_time_of_day, "включение компьютера"
    elif priority_source == "boot_time":
        # Приоритет у времени включения компьютера
        if boot_time_of_day:
            logger.info(f"Приоритет у времени включения компьютера: {boot_time_of_day}")
            return boot_time_of_day, "включение компьютера"
        elif wake_time_of_day:
            logger.info(f"Вторичный выбор - время выхода из сна: {wake_time_of_day}")
            return wake_time_of_day, "выход из сна"
        elif camera_time:
            logger.info(f"Вторичный выбор - время с камер: {camera_time}")
            return camera_time, "камеры"
    
    # Если приоритетный источник недоступен, возвращаем самый ранний из доступных
    sorted_sources = sorted(available_sources, key=lambda x: x[1])
    earliest_source, earliest_time = sorted_sources[0]
    logger.info(f"Выбрано самое раннее время из доступных: {earliest_time} из источника '{earliest_source}'")
    return earliest_time, earliest_source


def create_notification_logger():
    """
    Создает и настраивает логгер для микросервиса уведомлений

    Returns:
        logging.Logger: Настроенный объект логгера для уведомлений
    """
    import logging
    logger = configure_logger(
        user="cameras_domain",
        task_name="Notification",
        log_levels=[TRACE_LEVEL, logging.DEBUG],
        single_file_for_levels=False
    )

    return logger

def send_telegram_notification(message: str, main_logger=None) -> bool:
    """
    Отправляет уведомление через изолированный микросервис уведомлений.

    Аргументы:
        message: Текст уведомления.
        main_logger: Основной логгер для записи информации о попытке отправки (не используется в микросервисе уведомлений).
    Возвращает:
        True, если отправлено успешно, False в противном случае.
    """
    # Создаем изолированный логгер для работы с микросервисом уведомлений
    notification_logger = create_notification_logger()

    # Подготовим параметры подключения из конфигурации
    token = SCRIPT_CONFIG.get("TELEGRAM_TOKEN")
    chat_id = SCRIPT_CONFIG.get("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        notification_logger.warning("Параметры Telegram не заданы, уведомление не отправлено")
        return False

    # Подготовим параметры подключения
    connection_params = {
        "TELEGRAM_BOT_TOKEN": token,
        "TELEGRAM_CHAT_ID": chat_id
    }

    # Проверим подключение к Telegram
    notification_logger.info("Проверка подключения к Telegram...")
    connection_result = test_notification_connection(connection_params, logger=notification_logger)
    notification_logger.info(f"Результат проверки подключения к Telegram: {connection_result}")

    if not connection_result.get("success", False):
        notification_logger.error("Подключение к Telegram не удалось")
        return False

    # Отправим уведомление
    notification_logger.info(f"Отправка уведомления в Telegram: {len(message)} символов")
    notification_result = send_notification(
        message=message,
        connection_params=connection_params,
        logger=notification_logger
    )

    notification_logger.info(f"Результат отправки уведомления: {notification_result}")

    # Если передан основной логгер, сообщим ему об итогах
    if main_logger:
        if notification_result.get("success", False):
            main_logger.info("Уведомление успешно отправлено через микросервис уведомлений")
        else:
            main_logger.error(f"Ошибка при отправке уведомления: {notification_result.get('error', 'Неизвестная ошибка')}")

    return notification_result.get("success", False)

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

        # Получаем параметры из конфигурации
        combined_analysis_enabled = SCRIPT_CONFIG.get("COMBINED_ANALYSIS_ENABLED", True)

        # Получаем время включения компьютера и время выхода из сна, если комбинированный анализ включен
        boot_time = None
        wake_time = None
        if combined_analysis_enabled:
            logger.info("Получение времени включения компьютера...")
            boot_time = get_system_boot_time()
            if boot_time:
                logger.info(f"Время включения компьютера: {boot_time}")
            else:
                logger.warning("Не удалось получить время включения компьютера")
            
            logger.info("Получение времени выхода из спящего режима...")
            wake_time = get_wake_time()
            if wake_time:
                logger.info(f"Время выхода из спящего режима: {wake_time}")
            else:
                logger.warning("Не удалось получить время выхода из спящего режима")

        # Получаем время первого кадра с камер
        earliest_time = find_earliest_file_time(search_dir, start_time, end_time, logger)

        # Определяем итоговое время начала работы
        if combined_analysis_enabled:
            # Используем комбинированный анализ
            final_time, source = determine_start_time(earliest_time, boot_time, wake_time, logger)
        else:
            # Используем только данные с камер
            final_time = earliest_time
            source = "камеры" if earliest_time else "отсутствие данных"

        if final_time:
            message = f"✅ ПВЗ: {pvz_id}. Объект начал работу в {final_time.strftime('%H:%M:%S')} (источник: {source})."
            if combined_analysis_enabled:
                if earliest_time:
                    message += f" Время первого кадра с камер: {earliest_time.strftime('%H:%M:%S')}."
                if boot_time and boot_time.date() == date.today():
                    message += f" Время включения компьютера: {boot_time.time().strftime('%H:%M:%S')}."
                if wake_time and wake_time.date() == date.today():
                    message += f" Время выхода из спящего режима: {wake_time.time().strftime('%H:%M:%S')}."
            else:
                if earliest_time:
                    message += f" Время первого кадра с камер: {earliest_time.strftime('%H:%M:%S')}."
            logger.info(message)
        else:
            if combined_analysis_enabled:
                message = f"⚠️ ПВЗ: {pvz_id}. Объект не начал работу до {end_time.strftime('%H:%M')}. Ни один из источников данных не дал результата."
            else:
                message = f"⚠️ ПВЗ: {pvz_id}. Объект не начал работу до {end_time.strftime('%H:%M')}. Видеофайлы не обнаружены."
            logger.warning(message)

        # Подготовим параметры подключения
        connection_params = {
            "TELEGRAM_BOT_TOKEN": token,
            "TELEGRAM_CHAT_ID": chat_id
        }

        # Проверим подключение к Telegram
        logger.info("Проверка подключения к Telegram...")
        connection_result = test_notification_connection(connection_params, logger=logger)
        logger.info(f"Результат проверки подключения к Telegram: {connection_result}")

        if not connection_result.get("success", False):
            logger.error("Подключение к Telegram не удалось")
            sys.exit(1)  # Изменили с return на sys.exit(1), чтобы завершить программу с ошибкой

        # Отправим уведомление
        logger.info(f"Отправка уведомления в Telegram: {len(message)} символов")
        notification_result = send_telegram_notification(
            message=message,
            main_logger=logger
        )

        logger.info(f"Результат отправки уведомления: {notification_result}")

    except Exception as e:
        logger.critical(f"Критическая ошибка выполнения: {e}", exc_info=True)
        sys.exit(1)

    logger.info("Мониторинг успешно завершен.")

if __name__ == "__main__":
    main()
