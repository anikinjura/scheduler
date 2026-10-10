"""
CloudMonitorScript.py

Скрипт для мониторинга доступности облачного хранилища.

- Проверяет возможность записи во внешнюю директорию (cloud), каждая попытка — не дольше PROBE_TIMEOUT_SECONDS.
- Повторяет попытки проверки, если неудачно; зависшее хранилище (нет ответа за таймаут) не повторяет.
- Уведомление (ВК/Telegram): при первом сбое, затем напоминание раз в REMIND_HOURS, и когда хранилище снова доступно.
  Состояние — в STATE_FILE (reports/, вне git).

Author: anikinjura
"""
__version__ = '1.3.0'

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import time
from typing import Any, Dict, Optional

from scheduler_runner.tasks.cameras.config.scripts.cloudmonitor_config import SCRIPT_CONFIG
from scheduler_runner.utils.filesystem import call_with_timeout
from scheduler_runner.utils.logging import configure_logger, TRACE_LEVEL
from scheduler_runner.utils.notifications import send_notification

def parse_arguments() -> argparse.Namespace:
    """
    Парсит аргументы командной строки для скрипта мониторинга облачного хранилища.
    """
    parser = argparse.ArgumentParser(
        description="Мониторинг доступности облачного хранилища",
        epilog="Пример: python -m scheduler_runner.tasks.cameras.CloudMonitorScript --detailed_logs"
    )
    parser.add_argument(
        "--detailed_logs",
        action="store_true",
        default=False,
        help="Включить детализированные логи"
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=SCRIPT_CONFIG["RETRIES"],
        help="Количество попыток проверки облачного хранилища"
    )
    parser.add_argument(
        "--delay",
        type=int,
        default=SCRIPT_CONFIG["DELAY"],
        help="Задержка между попытками в секундах"
    )
    return parser.parse_args()

def test_cloud_accessibility(dest_dir: Path, logger: logging.Logger) -> tuple[bool, str]:
    """
    Тестирует доступность облачного хранилища путем создания временного файла.
    """
    try:
        if not dest_dir.exists():
            logger.warning(f"Целевая директория {dest_dir} не существует")
            return False, f"Директория {dest_dir} не существует"
        if not dest_dir.is_dir():
            logger.error(f"Путь {dest_dir} не является директорией")
            return False, f"{dest_dir} не является директорией"
        with tempfile.NamedTemporaryFile(dir=dest_dir, prefix="cloud_test_", suffix=".tmp", delete=True) as tmp:
            tmp.write(b"test")
            tmp.flush()
        logger.info("Проверка доступности облачного хранилища: Успешно")
        return True, "Успешно"
    except Exception as e:
        logger.error(f"Ошибка проверки доступности: {e}", exc_info=True)
        return False, str(e)

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

STATUS_OK = "ok"
STATUS_FAILED = "failed"
STATUS_HUNG = "hung"
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def check_cloud(dest_dir: Path, logger: logging.Logger, timeout: float) -> tuple[str, str]:
    """
    Одна проверка хранилища не дольше timeout секунд.

    Возвращает (STATUS_OK | STATUS_FAILED | STATUS_HUNG, описание).
    """
    finished, result = call_with_timeout(test_cloud_accessibility, timeout, dest_dir, logger)
    if not finished:
        message = f"хранилище не отвечает дольше {timeout:g} с (зависло)"
        logger.error("Проверка доступности облачного хранилища: %s", message)
        return STATUS_HUNG, message
    accessible, message = result
    return (STATUS_OK if accessible else STATUS_FAILED), message


def load_state(path: Path, logger: logging.Logger) -> Dict[str, Any]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as e:
        logger.warning("Состояние мониторинга не прочитано (%s): %s — начинаем заново", path, e)
        return {}


def save_state(path: Path, state: Dict[str, Any], logger: logging.Logger) -> None:
    try:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        temp = target.with_suffix(".tmp")
        temp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        temp.replace(target)
    except OSError as e:
        logger.warning("Состояние мониторинга не сохранено (%s): %s", path, e)


def _parse_time(value: Optional[str]) -> Optional[datetime]:
    try:
        return datetime.strptime(value, TIME_FORMAT) if value else None
    except ValueError:
        return None


def plan_notification(state: Dict[str, Any], success: bool, now: datetime, remind_hours: float) -> Optional[str]:
    """
    Что сообщать по итогам проверки: "failure" (новый сбой), "reminder" (сбой продолжается, пора напомнить),
    "recovery" (снова доступно) или None (сообщать нечего).
    """
    failing_since = state.get("failing_since")
    if success:
        return "recovery" if failing_since and state.get("last_alert_at") else None
    if not failing_since or not state.get("last_alert_at"):
        return "failure"
    last_alert = _parse_time(state.get("last_alert_at"))
    if last_alert is None or now - last_alert >= timedelta(hours=remind_hours):
        return "reminder"
    return None


def build_message(kind: str, pvz_id: str, dest_dir: Path, status: str, error: str, failing_since: Optional[str]) -> str:
    since = f" с {failing_since[:16]}" if failing_since else ""
    if kind == "recovery":
        return f"✅ ПВЗ: {pvz_id}, облачное хранилище снова доступно (было недоступно{since})."
    if status == STATUS_HUNG:
        advice = "Перезапустите «Диск-О:» (или перезагрузите компьютер) и проверьте, открывается ли диск O:."
    else:
        advice = "Проверьте, запущен ли «Диск-О:» и вошел ли он в учетную запись."
    head = "облачное хранилище по-прежнему недоступно" + since if kind == "reminder" else "облачное хранилище недоступно"
    return (
        f"🔴 ПВЗ: {pvz_id}, {head}: {error}. Путь: {dest_dir}. "
        f"Записи камер не попадают в облако. {advice}"
    )


def main() -> None:
    """
    Основная функция скрипта мониторинга облачного хранилища.
    """
    args = parse_arguments()
    logger = configure_logger(
        user=SCRIPT_CONFIG["USER"],
        task_name=SCRIPT_CONFIG["TASK_NAME"],
        detailed=args.detailed_logs if args.detailed_logs is not None else SCRIPT_CONFIG["DETAILED_LOGS"]
    )

    dest_dir = Path(SCRIPT_CONFIG["CHECK_DIR"])
    retries = args.retries
    delay = args.delay
    probe_timeout = SCRIPT_CONFIG.get("PROBE_TIMEOUT_SECONDS", 15)
    success = False
    status, last_error = STATUS_FAILED, "Неизвестная ошибка"

    logger.info("Начало мониторинга облачного хранилища %s", dest_dir)

    for attempt in range(1, retries + 1):
        logger.debug("Попытка %d из %d", attempt, retries)
        status, error_message = check_cloud(dest_dir, logger, probe_timeout)
        if status == STATUS_OK:
            logger.info("Облачное хранилище доступно после %d попытки(попыток)", attempt)
            success = True
            break
        last_error = error_message
        logger.debug("Попытка %d неудачна: %s", attempt, error_message)
        if status == STATUS_HUNG:
            break  # зависший диск за секунды не оживает, а время задачи ограничено таймаутом runner
        if attempt < retries:
            logger.debug("Ожидание %d секунд до следующей попытки...", delay)
            time.sleep(delay)

    if not success:
        logger.critical(
            "КРИТИЧЕСКАЯ ОШИБКА: Облачное хранилище недоступно (%s). ПВЗ: %s, Путь: %s. Последняя ошибка: %s",
            status, SCRIPT_CONFIG["PVZ_ID"], dest_dir, last_error,
        )

    state_file = SCRIPT_CONFIG.get("STATE_FILE")
    state = load_state(state_file, logger) if state_file else {}
    now = datetime.now()
    kind = plan_notification(state, success, now, SCRIPT_CONFIG.get("REMIND_HOURS", 3))
    if not success and not state.get("failing_since"):
        state["failing_since"] = now.strftime(TIME_FORMAT)
    if kind:
        message = build_message(kind, SCRIPT_CONFIG["PVZ_ID"], dest_dir, status, last_error, state.get("failing_since"))
        sent = send_telegram_notification(message, logger)
        if kind == "recovery":
            if sent:
                state = {}
        elif sent:
            state["last_alert_at"] = now.strftime(TIME_FORMAT)
    elif success:
        state = {}
    if state_file:
        save_state(state_file, state, logger)

    logger.info("Мониторинг облачного хранилища завершен. Статус: %s", "УСПЕШНО" if success else "ОШИБКА")
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
