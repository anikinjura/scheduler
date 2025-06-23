"""
VideoMonitorScript.py

Скрипт для мониторинга наличия видеозаписей с камер видеонаблюдения.

Основной функционал:
- Локальная проверка: по каждому uid камеры ищет записи за последние N часов (N задаётся в конфиге) в локальной директории.
- Облачная проверка: по каждому uid камеры ищет записи за последние N часов (N задаётся в конфиге) в облачной директории.
- Если для какой-либо камеры не найдено нужного количества файлов ни в одном из последних N часов — формируется уведомление.
- Уведомления отправляются через утилиту ядра notify.py (Telegram).

Архитектура:
- Все параметры (пути, список камер, параметры Telegram, глубина проверки) задаются в cameras/config/scripts/videomonitor_config.py.
- Логика поиска записей за последние N часов вынесена в универсальную функцию has_recent_records.
- Для разных типов камер (UNV, Xiaomi) используется свой path_builder для построения пути к папке с записями.
- Скрипт поддерживает детализированные логи и параметр --min_files для задания минимального количества файлов.

Пример использования:
    python VideoMonitorScript.py --check_type local --detailed_logs --min_files 2

Author: anikinjura
"""
__version__ = '0.0.2'

from typing import Callable
import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta

from scheduler_runner.tasks.cameras.config.scripts.videomonitor_config import SCRIPT_CONFIG
from scheduler_runner.utils.logging import configure_logger
from scheduler_runner.utils.notify import send_telegram_message

def parse_arguments() -> argparse.Namespace:
    """
    Парсит аргументы командной строки для скрипта мониторинга видеозаписей.

    --check_type local|network   - сценарий проверки (локально или в облаке)
    --detailed_logs              - включить детализированные логи
    --min_files N                - минимальное количество файлов для успешной проверки (по умолчанию 1)
    """
    parser = argparse.ArgumentParser(
        description="Мониторинг видеозаписей с камер",
        epilog="Пример: python VideoMonitorScript.py --check_type local --detailed_logs --min_files 2"
    )
    parser.add_argument(
        "--check_type",
        type=str,
        choices=["local", "network"],
        required=True,
        help="Тип проверки: local или network"
    )
    parser.add_argument(
        "--detailed_logs",
        action="store_true",
        default=False,
        help="Включить детализированные логи"
    )
    parser.add_argument(
        "--min_files",
        type=int,
        default=1,
        help="Минимальное количество файлов для успешной проверки"
    )
    return parser.parse_args()

def has_recent_records(
    root_dir: Path,
    uid: str,
    min_files: int,
    max_lookback_hours: int,
    path_builder: Callable[[Path, str, datetime], Path],
    logger: logging.Logger,
    camera_type: str
) -> bool:
    """
    Проверяет наличие записей за последние max_lookback_hours часов для камеры с заданным uid.

    Аргументы:
        root_dir: Корневая директория для поиска записей.
        uid: Уникальный идентификатор камеры.
        min_files: Минимальное количество файлов для успешной проверки.
        max_lookback_hours: Глубина проверки в часах (например, 2 — текущий и предыдущий час).
        path_builder: Функция, формирующая путь к папке с записями по типу камеры.
        logger: Логгер для записи информации.
        camera_type: Строка для логов ("UNV" или "Xiaomi").

    Возвращает:
        True, если записи найдены хотя бы за один из последних max_lookback_hours часов, иначе False.
    """
    now = datetime.now()
    for delta in range(0, max_lookback_hours):
        check_time = now - timedelta(hours=delta)
        path = path_builder(root_dir, uid, check_time)
        # Проверяем наличие папки и достаточного количества файлов
        if path.exists() and sum(1 for _ in path.iterdir()) >= min_files:
            if delta == 0:
                logger.info(f"{camera_type} камера {uid}: найдены записи за текущий час ({check_time.strftime('%H')})")
            else:
                logger.info(f"{camera_type} камера {uid}: найдены записи за {delta}-й час назад ({check_time.strftime('%H')})")
            return True
    logger.warning(f"{camera_type} камера {uid}: записи отсутствуют за последние {max_lookback_hours} часов")
    return False

def unv_path_builder(root_dir: Path, uid: str, check_time: datetime) -> Path:
    """
    Формирует путь к папке с записями UNV-камеры за указанный час.
    Пример: root_dir/unv_camera/uid/YYYYMMDD/HH
    """
    target_date = check_time.strftime("%Y%m%d")
    hour_str = check_time.strftime("%H")
    return root_dir / "unv_camera" / uid / target_date / hour_str

def xiaomi_path_builder(root_dir: Path, uid: str, check_time: datetime) -> Path:
    """
    Формирует путь к папке с записями Xiaomi-камеры за указанный час.
    Пример: root_dir/xiaomi_camera_videos/uid/YYYYMMDDHH
    """
    target_datetime = check_time.strftime("%Y%m%d%H")
    return root_dir / "xiaomi_camera_videos" / uid / target_datetime

def send_notification(message: str, logger: logging.Logger) -> bool:
    """
    Отправляет уведомление через утилиту ядра scheduler_runner/utils/notify.py.

    Аргументы:
        message: Текст уведомления.
        logger: Логгер для записи информации.
    Возвращает:
        True, если отправлено успешно, False в противном случае.
    """
    token = SCRIPT_CONFIG["TOKEN"]
    chat_id = SCRIPT_CONFIG["CHAT_ID"]
    if not token or not chat_id:
        logger.warning("Параметры Telegram не заданы, уведомление не отправлено")
        return False
    success, result = send_telegram_message(token, chat_id, message, logger)
    if success:
        logger.info("Уведомление успешно отправлено через Telegram")
    else:
        logger.error("Ошибка отправки уведомления через Telegram: %s", result)
    return success

def main() -> None:
    """
    Основная функция скрипта мониторинга видеозаписей.

    Этапы:
    1. Разбор аргументов командной строки.
    2. Настройка логирования.
    3. Получение конфигурации камер и параметров сценария.
    4. Для каждой камеры строится путь к папке с записями и ищутся записи за последние N часов.
    5. Если хотя бы для одной камеры не найдено нужного количества файлов — отправляется уведомление.
    """
    args = parse_arguments()
    scenario = args.check_type
    scenario_config = SCRIPT_CONFIG[scenario]
    max_lookback_hours = scenario_config.get("MAX_LOOKBACK_HOURS", 2)
    cameras = SCRIPT_CONFIG["CAMERAS"]

    # Настройка логгера с учётом сценария и детализированности
    logger = configure_logger(
        user=scenario_config["USER"],
        task_name=scenario_config["TASK_NAME"],
        detailed=args.detailed_logs if args.detailed_logs is not None else scenario_config["DETAILED_LOGS"]
    )

    if not cameras:
        logger.error("Конфигурация камер недоступна или PVZ_ID не найден.")
        sys.exit(1)

    root_dir = Path(scenario_config["CHECK_DIR"])
    missing_records = []

    # Основной цикл по всем камерам
    for area, cam_list in cameras.items():
        for cam in cam_list:
            uid = cam["uid"]
            cam_id = cam["id"]
            # Для UNV-камер используем unv_path_builder, для Xiaomi — xiaomi_path_builder
            if cam_id.startswith("unv"):
                if not has_recent_records(root_dir, uid, args.min_files, max_lookback_hours, unv_path_builder, logger, "UNV"):
                    missing_records.append(f"{cam_id} ({area})")
            elif cam_id.startswith("xiaomi"):
                if not has_recent_records(root_dir, uid, args.min_files, max_lookback_hours, xiaomi_path_builder, logger, "Xiaomi"):
                    missing_records.append(f"{cam_id} ({area})")

    # Если есть камеры без записей — отправляем уведомление
    if missing_records:
        check_type_str = "на компьютере" if scenario == "local" else "в облаке"
        message = f"ПВЗ: {SCRIPT_CONFIG.get('PVZ_ID', '-')}, {check_type_str} отсутствуют записи для камер: {', '.join(missing_records)}"
        send_notification(message, logger)
        logger.warning(message)
    else:
        logger.info("Все камеры имеют записи за проверяемый период.")

if __name__ == "__main__":
    main()