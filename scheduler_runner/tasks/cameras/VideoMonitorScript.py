"""
VideoMonitorScript.py

Скрипт для мониторинга наличия видеозаписей с камер видеонаблюдения.

Основной функционал:
- Локальная проверка: по каждому uid камеры ищет записи за последние N часов (N задаётся в конфиге) в локальных директориях.
  Для камеры может быть задан индивидуальный корень через `camera.root_key` -> `LOCAL_ROOTS`.
- Облачная проверка: по каждому uid камеры ищет записи за последние N часов (N задаётся в конфиге) в облачной директории.
- Если для какой-либо камеры не найдено нужного количества файлов ни в одном из последних N часов — формируется уведомление.
- Камера, которая выгружает записи с отставанием (Xiaomi с картой памяти догружает ночные часы днем — 144, 17–18.10.2026),
  не считается пропавшей: если в ее самую свежую папку за проверяемые часы записывались файлы — «запись с отставанием».
- Уведомления (при STATE_FILE): о новой пропавшей камере — сразу, пока не вернулась — раз в REMIND_HOURS, о вернувшихся —
  «записи снова есть»; раньше одно и то же приходило каждый час.
- Вся проверка — не дольше SCAN_TIMEOUT_SECONDS: если хранилище (облако «Диск-О:») зависло, отправляется уведомление
  «хранилище не отвечает» вместо тишины (раньше задачу молча убивал таймаут runner — 144, 05–10.10.2026).
- Уведомления отправляются через scheduler_runner.utils.notifications (Telegram).

Архитектура:
- Все параметры (пути, список камер, параметры Telegram, глубина проверки) задаются в cameras/config/scripts/videomonitor_config.py.
- Логика поиска записей за последние N часов вынесена в универсальную функцию has_recent_records.
- Для разных типов камер (UNV, Xiaomi) используется свой path_builder для построения пути к папке с записями.
- Скрипт поддерживает детализированные логи и параметр --min_files для задания минимального количества файлов.

Пример использования:
    python -m scheduler_runner.tasks.cameras.VideoMonitorScript --check_type local --detailed_logs --min_files 2

Author: anikinjura
"""
__version__ = '0.0.4'

from typing import Callable, Dict, List, Optional
import argparse
import json
import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta

from scheduler_runner.tasks.cameras.config.scripts.videomonitor_config import SCRIPT_CONFIG
from scheduler_runner.utils.filesystem import call_with_timeout
from scheduler_runner.utils.logging import configure_logger, TRACE_LEVEL
from scheduler_runner.utils.notifications import send_notification

def parse_arguments() -> argparse.Namespace:
    """
    Парсит аргументы командной строки для скрипта мониторинга видеозаписей.

    --check_type local|network   - сценарий проверки (локально или в облаке)
    --detailed_logs              - включить детализированные логи
    --min_files N                - минимальное количество файлов для успешной проверки (по умолчанию 1)
    --current-time               - фиктивное текущее время для проверки в формате 'YYYY-MM-DD HH:MM:SS'
    """
    parser = argparse.ArgumentParser(
        description="Мониторинг видеозаписей с камер",
        epilog="Пример: python -m scheduler_runner.tasks.cameras.VideoMonitorScript --check_type local --detailed_logs --min_files 2"
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
    parser.add_argument(
        "--current-time",
        help="Фиктивное текущее время для проверки в формате 'YYYY-MM-DD HH:MM:SS'"
    )
    return parser.parse_args()

def has_recent_records(
    root_dir: Path,
    uid: str,
    min_files: int,
    max_lookback_hours: int,
    path_builder: Callable[[Path, str, datetime], Path],
    logger: logging.Logger,
    camera_type: str,
    now: datetime = None
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
        now: Фиктивное текущее время для проверки (если None, используется datetime.now())

    Возвращает:
        True, если записи найдены хотя бы за один из последних max_lookback_hours часов, иначе False.
    """
    if now is None:
        current_time = datetime.now()
    else:
        current_time = now

    logger.debug(f"Проверка наличия записей для {camera_type} камеры {uid} в директории {root_dir}")
    logger.debug(f"Параметры проверки: min_files={min_files}, max_lookback_hours={max_lookback_hours}")

    for delta in range(0, max_lookback_hours):
        check_time = current_time - timedelta(hours=delta)
        path = path_builder(root_dir, uid, check_time)

        logger.debug(f"Проверяем путь для {camera_type} камеры {uid} за {delta}-й час назад ({check_time.strftime('%Y-%m-%d %H:%M:%S')}): {path}")

        if not path.exists():
            logger.debug(f"Путь не существует: {path}")
            continue

        files_count = sum(1 for _ in path.iterdir())
        logger.debug(f"Найдено файлов в {path}: {files_count} (требуется минимум {min_files})")

        # Проверяем наличие папки и достаточного количества файлов
        if path.exists() and files_count >= min_files:
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

def create_notification_logger():
    """
    Создает и настраивает логгер для микросервиса уведомлений

    Returns:
        logging.Logger: Настроенный объект логгера для уведомлений
    """
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

    connection_params = dict(SCRIPT_CONFIG.get("NOTIFICATION_CONNECTION_PARAMS") or {})
    provider = connection_params.get("NOTIFICATION_PROVIDER", "telegram")

    if not connection_params:
        notification_logger.warning("Параметры notifications transport не заданы, уведомление не отправлено")
        return False

    # Отправим уведомление
    notification_logger.info(f"Отправка уведомления через provider={provider}: {len(message)} символов")
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

CAMERA_DIRS = {
    # тип камеры: (папка типа в корне, сколько уровней папок с датой до файлов)
    "UNV": ("unv_camera", 2),           # <uid>/YYYYMMDD/HH/файлы
    "Xiaomi": ("xiaomi_camera_videos", 1),  # <uid>/YYYYMMDDHH/файлы
}
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def latest_write_time(camera_dir: Path, depth: int) -> Optional[tuple]:
    """
    Самая свежая по имени папка записей камеры (спуск по последним по имени папкам на depth уровней) и время последней
    записи файла в ней: (имя папки, datetime) или None.
    """
    folder = camera_dir
    for _ in range(depth):
        try:
            subfolders = [entry for entry in folder.iterdir() if entry.is_dir()]
        except OSError:
            return None
        if not subfolders:
            return None
        folder = max(subfolders, key=lambda entry: entry.name)
    try:
        newest = max((entry.stat().st_mtime for entry in folder.iterdir() if entry.is_file()), default=None)
    except OSError:
        return None
    if newest is None:
        return None
    return folder.relative_to(camera_dir).as_posix(), datetime.fromtimestamp(newest)


def is_recording_with_lag(root_dir: Path, uid: str, camera_type: str, max_lookback_hours: int,
                          logger: logging.Logger, now: datetime = None) -> bool:
    """Камера пишет, но с отставанием: в ее самую свежую папку файлы записывались в последние max_lookback_hours часов."""
    type_dir, depth = CAMERA_DIRS[camera_type]
    latest = latest_write_time(root_dir / type_dir / uid, depth)
    if latest is None:
        return False
    folder, written_at = latest
    if (now or datetime.now()) - written_at <= timedelta(hours=max_lookback_hours):
        logger.info(f"{camera_type} камера {uid}: записи идут с отставанием — последняя папка {folder}, "
                    f"файлы записаны в {written_at.strftime('%d.%m %H:%M')}")
        return True
    return False


def find_missing_records(cameras, scenario: str, default_root_dir: Path, local_roots, min_files: int,
                         max_lookback_hours: int, logger: logging.Logger, current_time: datetime = None) -> list:
    """Обходит все камеры и возвращает список «камера (зона)», для которых не найдено записей."""
    missing_records = []
    for area, cam_list in cameras.items():
        for cam in cam_list:
            uid = cam["uid"]
            cam_id = cam["id"]
            root_dir = default_root_dir
            if scenario == "local":
                camera_root_key = cam.get("root_key", "default")
                resolved_root = local_roots.get(camera_root_key)
                if resolved_root is None:
                    logger.warning(
                        "root_key '%s' not found for camera %s, fallback to %s",
                        camera_root_key, cam_id, default_root_dir
                    )
                else:
                    root_dir = Path(resolved_root)
            # Для UNV-камер используем unv_path_builder, для Xiaomi — xiaomi_path_builder
            if cam_id.startswith("unv"):
                camera_type, path_builder = "UNV", unv_path_builder
            elif cam_id.startswith("xiaomi"):
                camera_type, path_builder = "Xiaomi", xiaomi_path_builder
            else:
                continue
            if has_recent_records(root_dir, uid, min_files, max_lookback_hours, path_builder, logger, camera_type, current_time):
                continue
            if is_recording_with_lag(root_dir, uid, camera_type, max_lookback_hours, logger, current_time):
                continue
            missing_records.append(f"{cam_id} ({area})")
    return missing_records


def load_state(path, logger: logging.Logger) -> Dict:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as e:
        logger.warning(f"Состояние мониторинга не прочитано ({path}): {e} — начинаем заново")
        return {}


def save_state(path, state: Dict, logger: logging.Logger) -> None:
    try:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        temp = target.with_suffix(".tmp")
        temp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        temp.replace(target)
    except OSError as e:
        logger.warning(f"Состояние мониторинга не сохранено ({path}): {e}")


def plan_notifications(state: Dict, missing: List[str], now: datetime, remind_hours: float) -> Dict:
    """
    Что сообщать: {"missing": [...] или [], "reminder": bool, "recovered": [...]}.

    О новой пропавшей камере — сразу (в сообщении — все пропавшие), пока пропавшие не вернулись — напоминание раз в
    remind_hours, о вернувшихся — отдельно.
    """
    previous = state.get("missing") or {}
    new = [camera for camera in missing if camera not in previous]
    recovered = [camera for camera in previous if camera not in missing]
    last_alert = state.get("last_alert_at")
    reminder = False
    if missing and not new:
        try:
            reminder = last_alert is None or now - datetime.strptime(last_alert, TIME_FORMAT) >= timedelta(hours=remind_hours)
        except ValueError:
            reminder = True
    return {"missing": missing if (new or reminder) else [], "reminder": reminder and not new, "recovered": recovered}

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
    
    # Парсим фиктивное время, если оно задано
    current_time = None
    if args.current_time:
        try:
            # Пробуем разные форматы времени
            if 'T' in args.current_time:
                current_time = datetime.strptime(args.current_time, "%Y-%m-%dT%H:%M:%S")
            else:
                current_time = datetime.strptime(args.current_time, "%Y-%m-%d %H:%M:%S")
        except ValueError as e:
            print(f"Неверный формат времени '{args.current_time}'. Ожидается формат 'YYYY-MM-DD HH:MM:SS' или 'YYYY-MM-DDTHH:MM:SS'")
            sys.exit(1)
    
    scenario = args.check_type
    scenario_config = SCRIPT_CONFIG[scenario]
    max_lookback_hours = scenario_config.get("MAX_LOOKBACK_HOURS", 2)
    cameras = SCRIPT_CONFIG["CAMERAS"]

    # Настройка логгера с учётом сценария и детализированности
    if args.detailed_logs or scenario_config["DETAILED_LOGS"]:
        logger = configure_logger(
            user=scenario_config["USER"],
            task_name=scenario_config["TASK_NAME"],
            log_levels=[TRACE_LEVEL, logging.DEBUG],
            single_file_for_levels=False
        )
    else:
        logger = configure_logger(
            user=scenario_config["USER"],
            task_name=scenario_config["TASK_NAME"],
            detailed=args.detailed_logs if args.detailed_logs is not None else scenario_config["DETAILED_LOGS"]
        )

    if not cameras:
        logger.error("Конфигурация камер недоступна или PVZ_ID не найден.")
        sys.exit(1)

    default_root_dir = Path(scenario_config["CHECK_DIR"])
    local_roots = scenario_config.get("LOCAL_ROOTS", {"default": default_root_dir})
    check_type_str = "на компьютере" if scenario == "local" else "в облаке"
    scan_timeout = scenario_config.get("SCAN_TIMEOUT_SECONDS", 40)

    finished, missing_records = call_with_timeout(
        find_missing_records, scan_timeout,
        cameras, scenario, default_root_dir, local_roots, args.min_files, max_lookback_hours, logger, current_time,
    )
    if not finished:
        message = (
            f"🔴 ПВЗ: {SCRIPT_CONFIG.get('PVZ_ID', '-')}, не удалось проверить записи камер {check_type_str}: "
            f"хранилище {default_root_dir} не отвечает дольше {scan_timeout} с (зависло)."
        )
        if scenario == "network":
            message += " Перезапустите «Диск-О:» (или перезагрузите компьютер) и проверьте, открывается ли диск O:."
        logger.error(message)
        send_telegram_notification(message, logger)
        sys.exit(1)

    pvz_id = SCRIPT_CONFIG.get('PVZ_ID', '-')
    if missing_records:
        logger.warning(f"ПВЗ: {pvz_id}, {check_type_str} отсутствуют записи для камер: {', '.join(missing_records)}")
    else:
        logger.info("Все камеры имеют записи за проверяемый период.")

    state_file = scenario_config.get("STATE_FILE")
    if not state_file:
        # Без состояния — как раньше: уведомление на каждую проверку с пропавшими камерами
        if missing_records:
            send_telegram_notification(
                f"ПВЗ: {pvz_id}, {check_type_str} отсутствуют записи для камер: {', '.join(missing_records)}", logger)
        return

    now = current_time or datetime.now()
    state = load_state(state_file, logger)
    plan = plan_notifications(state, missing_records, now, scenario_config.get("REMIND_HOURS", 3))
    previous = state.get("missing") or {}
    new_state = {"missing": {camera: previous.get(camera, now.strftime(TIME_FORMAT)) for camera in missing_records},
                 "last_alert_at": state.get("last_alert_at") if missing_records else None}

    if plan["missing"]:
        if plan["reminder"]:
            since = lambda camera: datetime.strptime(new_state["missing"][camera], TIME_FORMAT).strftime("%d.%m %H:%M")
            details = ", ".join(f"{camera} (с {since(camera)})" for camera in plan["missing"])
            message = f"🔴 ПВЗ: {pvz_id}, {check_type_str} по-прежнему отсутствуют записи для камер: {details}"
        else:
            message = f"🔴 ПВЗ: {pvz_id}, {check_type_str} отсутствуют записи для камер: {', '.join(plan['missing'])}"
        if send_telegram_notification(message, logger):
            new_state["last_alert_at"] = now.strftime(TIME_FORMAT)
    if plan["recovered"]:
        message = f"✅ ПВЗ: {pvz_id}, {check_type_str} снова есть записи для камер: {', '.join(plan['recovered'])}"
        if not send_telegram_notification(message, logger):
            # Не ушло — оставить в состоянии, чтобы сообщить при следующей проверке
            for camera in plan["recovered"]:
                new_state["missing"].setdefault(camera, previous[camera])
    save_state(state_file, new_state, logger)

if __name__ == "__main__":
    main()
