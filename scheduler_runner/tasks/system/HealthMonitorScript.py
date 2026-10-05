"""
HealthMonitorScript.py

Ежедневный сбор состояния компьютера ПВЗ (docs/HEALTH_MONITOR_PLAN.md).

- Запускает health_probe.ps1 (только чтение): диски и SMART, тома, счетчики событий журнала System, задачи
  Планировщика Windows. PowerShell — без окна, сотрудник за компьютером ничего не видит.
- Добавляет версию кода (сравнение с удаленной веткой через git ls-remote, без fetch) и даты последних успешных
  запусков задач scheduler по их логам.
- Формирует строки листов PVZ_HEALTH (ПВЗ/день) и PVZ_DISK_HEALTH (диск/день).

- Пишет строки в листы Google-таблицы (книга KPI) пакетным upsert-ом; недостающие листы создает с заголовками.

- Оценивает тревоги (health_alerts.py) по сравнению с прошлым сбором и сообщает в ВК только изменения: новое,
  ухудшение, повтор неустраненной критической раз в неделю, устранение. Итог — в колонках status / alerts.

Лог: HEALTH_SUMMARY, HEALTH_DISK, HEALTH_ALERTS, HEALTH_UPLOAD, HEALTH_NOTIFY.

Аргументы командной строки:
    --hours <N>              окно подсчета событий журнала (по умолчанию из конфига, 24)
    --output <path>          сохранить собранное (строки и сырой результат сбора) в JSON
    --no-upload              только собрать и записать в лог, без Google-таблицы
    --spreadsheet-id <id>    писать в другую книгу (например, тестовую) вместо книги KPI
    --no-jitter              без случайной паузы перед записью (ручной запуск)
    --no-notify              не отправлять уведомление в ВК (состояние тревог при этом не меняется)
    --state-file <path>      файл состояния тревог вместо конфига (проверки)
    --detailed_logs          детальные логи (DEBUG)

Author: anikinjura
"""

__version__ = '0.0.1'

import argparse
import json
import random
import subprocess
import sys
import time
from copy import deepcopy
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from config.base_config import PVZ_ID
from scheduler_runner.tasks.system.config.scripts.health_monitor_config import SCRIPT_CONFIG
from scheduler_runner.utils.logging import configure_logger

# Флаг CreateProcess: дочерний PowerShell без консольного окна
CREATE_NO_WINDOW = 0x08000000
# Строка runner-а об успешном завершении задачи: "Задача 'ReportsProcessor' успешно завершена"
TASK_SUCCESS_MARKER = "успешно завершена"
# Поля листа PVZ_DISK_HEALTH -> номер атрибута SMART
SMART_FIELDS = {
    "smart_5_reallocated": "5",
    "smart_197_pending": "197",
    "smart_198_offline_uncorrectable": "198",
    "smart_187_reported_uncorrectable": "187",
    "smart_199_crc_errors": "199",
}
# У части дисков (Toshiba DT01ACA) сырое значение 187 упаковано: значимы младшие 16 бит (65536 означает 0)
SMART_LOW16_ATTRS = {"187"}
TASK_RESULT_FIELDS = {"operator": "task_operator_result", "camera": "task_camera_result", "system": "task_system_result"}


class ProbeError(RuntimeError):
    """health_probe.ps1 не выполнился или вернул не JSON."""


def parse_arguments():
    parser = argparse.ArgumentParser(description="Ежедневный сбор состояния компьютера ПВЗ")
    parser.add_argument("--hours", type=int, default=SCRIPT_CONFIG["EVENTS_WINDOW_HOURS"],
                        help="Окно подсчета событий журнала System, часов")
    parser.add_argument("--output", type=str, default=None, help="Сохранить собранное в JSON-файл")
    parser.add_argument("--no-upload", action="store_true", help="Не писать в Google-таблицу")
    parser.add_argument("--spreadsheet-id", type=str, default=None, help="Книга для записи вместо книги KPI")
    parser.add_argument("--no-jitter", action="store_true", help="Без случайной паузы перед записью")
    parser.add_argument("--no-notify", action="store_true", help="Не отправлять уведомление в ВК")
    parser.add_argument("--state-file", type=str, default=None, help="Файл состояния тревог вместо конфига")
    parser.add_argument("--detailed_logs", action="store_true", help="Включить детализированные логи (DEBUG)")
    return parser.parse_args()


def run_probe(probe_script: str, hours: int, task_names: Iterable[str], timeout: int) -> Dict[str, Any]:
    """Запускает health_probe.ps1 без окна и возвращает разобранный JSON."""
    command = [
        "powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
        "-File", probe_script, "-Hours", str(hours), "-TaskNames", ",".join(task_names),
    ]
    try:
        result = subprocess.run(
            command, capture_output=True, timeout=timeout,
            creationflags=CREATE_NO_WINDOW if sys.platform == "win32" else 0,
        )
    except (OSError, subprocess.TimeoutExpired) as e:
        raise ProbeError(f"health_probe.ps1 не выполнен: {e}") from e
    return parse_probe_output(result.returncode, result.stdout, result.stderr)


def parse_probe_output(returncode: int, stdout: bytes, stderr: bytes) -> Dict[str, Any]:
    text = (stdout or b"").decode("utf-8", errors="replace").lstrip("﻿")
    start = text.find("{")
    if returncode != 0 or start < 0:
        err = (stderr or b"").decode("utf-8", errors="replace").strip()
        raise ProbeError(f"health_probe.ps1: код {returncode}, нет JSON; stderr: {err[:500]}")
    try:
        return json.loads(text[start:])
    except json.JSONDecodeError as e:
        raise ProbeError(f"health_probe.ps1 вернул некорректный JSON: {e}") from e


def _git(repo_dir: str, args: List[str], timeout: int) -> Optional[str]:
    try:
        result = subprocess.run(
            ["git", *args], cwd=repo_dir, capture_output=True, text=True, timeout=timeout,
            creationflags=CREATE_NO_WINDOW if sys.platform == "win32" else 0,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    return result.stdout.strip() if result.returncode == 0 else None


def collect_code_state(repo_dir: str, branch: str, timeout: int) -> Dict[str, Any]:
    """Версия кода и совпадение с удаленной веткой (git ls-remote: только чтение, без fetch)."""
    local = _git(repo_dir, ["rev-parse", "HEAD"], timeout)
    remote_line = _git(repo_dir, ["ls-remote", "origin", f"refs/heads/{branch}"], timeout)
    remote = remote_line.split()[0] if remote_line else None
    return {
        "code_version": local[:7] if local else "",
        "main_version": remote[:7] if remote else "",
        # None — сравнить не удалось (нет сети/git); True/False — совпадает ли HEAD с удаленной веткой
        "code_up_to_date": (local == remote) if (local and remote) else None,
    }


def find_last_task_success(logs_root: str, user: str, task_name: str, lookback_days: int,
                           today: date) -> Optional[str]:
    """Время последней строки «Задача '<task>' успешно завершена» в логах задачи за lookback_days дней."""
    task_dir = Path(logs_root) / user / task_name
    marker = f"Задача '{task_name}' {TASK_SUCCESS_MARKER}"
    for offset in range(lookback_days):
        log_file = task_dir / f"{(today - timedelta(days=offset)).isoformat()}.log"
        if not log_file.exists():
            continue
        try:
            lines = log_file.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        for line in reversed(lines):
            if marker in line:
                return line[:19]  # "YYYY-MM-DD HH:MM:SS"
    return None


def collect_scheduler_state(config: Dict[str, Any], today: date) -> Dict[str, Optional[str]]:
    state = {}
    for field, tasks in config["TRACKED_TASK_LOGS"].items():
        found = [find_last_task_success(config["LOGS_ROOT"], user, name, config["TASK_LOG_LOOKBACK_DAYS"], today)
                 for user, name in tasks]
        found = [value for value in found if value]
        state[field] = max(found) if found else ""
    return state


def smart_raw(attrs: Dict[str, Any], attr_id: str) -> Optional[int]:
    attr = (attrs or {}).get(attr_id)
    if not attr or attr.get("raw") is None:
        return None
    raw = int(attr["raw"])
    return raw & 0xFFFF if attr_id in SMART_LOW16_ATTRS else raw


def disk_has_warning(disk: Dict[str, Any]) -> bool:
    return disk.get("health_status") not in (None, "", "Healthy") or disk.get("predict_failure") is True


def build_disk_rows(probe: Dict[str, Any], pvz_id: str, work_date: str) -> List[Dict[str, Any]]:
    rows = []
    for disk in probe.get("disks") or []:
        attrs = disk.get("smart") or {}
        row = {
            "work_date": work_date,
            "pvz_id": pvz_id,
            "disk_serial": disk.get("serial") or f"disk{disk.get('disk_number')}",
            "model": disk.get("model", ""),
            "media_type": disk.get("media_type", ""),
            "size_gb": disk.get("size_gb"),
            "volumes": disk.get("volumes", ""),
            "health_status": disk.get("health_status", ""),
            "predict_failure": disk.get("predict_failure"),
            "temperature_c": disk.get("temperature_c"),
            "smart_9_power_on_hours": disk.get("power_on_hours"),
        }
        for field, attr_id in SMART_FIELDS.items():
            row[field] = smart_raw(attrs, attr_id)
        rows.append(row)
    return rows


def _volume(probe: Dict[str, Any], letter: str) -> Dict[str, Any]:
    return next((v for v in probe.get("volumes") or [] if str(v.get("letter", "")).upper() == letter), {})


def build_health_row(probe: Dict[str, Any], code_state: Dict[str, Any], scheduler_state: Dict[str, Optional[str]],
                     disk_rows: List[Dict[str, Any]], pvz_id: str, work_date: str) -> Dict[str, Any]:
    system = probe.get("system") or {}
    events = probe.get("events") or {}
    c_volume, d_volume = _volume(probe, "C"), _volume(probe, "D")
    warning_disks = [d for d in probe.get("disks") or [] if disk_has_warning(d)]
    row = {
        "work_date": work_date,
        "pvz_id": pvz_id,
        "collected_at": probe.get("collected_at", ""),
        "computer_name": system.get("computer_name", ""),
        **code_state,
        "os_version": system.get("os_version", ""),
        "boot_time": system.get("boot_time", ""),
        "uptime_hours": system.get("uptime_hours"),
        "cpu": system.get("cpu", ""),
        "ram_gb": system.get("ram_gb"),
        "c_free_gb": c_volume.get("free_gb"),
        "c_free_pct": c_volume.get("free_pct"),
        "d_free_gb": d_volume.get("free_gb"),
        "d_free_pct": d_volume.get("free_pct"),
        "events_window_hours": events.get("window_hours"),
        "unexpected_shutdowns": events.get("unexpected_shutdowns"),
        "bugchecks": events.get("bugchecks"),
        "whea_errors": events.get("whea_errors"),
        "disk_errors": events.get("disk_errors"),
        "ntfs_errors": events.get("ntfs_errors"),
        "display_resets": events.get("display_resets"),
        "disks_total": len(disk_rows),
        "disks_warning": len(warning_disks),
        "worst_disk": ", ".join(f"{d.get('model', '')} [{d.get('serial', '')}]" for d in warning_disks),
        **scheduler_state,
        "probe_elevated": probe.get("elevated"),
        "probe_limited": "; ".join(probe.get("limited") or []),
        # Итог и сработавшие правила — этап 3 (тревоги)
        "status": "",
        "alerts": "",
    }
    # Состав колонок листа не должен зависеть от того, нашлись ли логи задач
    for field in SCRIPT_CONFIG["TRACKED_TASK_LOGS"]:
        row[field] = row.get(field) or ""
    for field in TASK_RESULT_FIELDS.values():
        row[field] = None
    for task in probe.get("tasks") or []:
        name = str(task.get("name", "")).lower()
        for suffix, field in TASK_RESULT_FIELDS.items():
            if name.endswith(suffix):
                row[field] = task.get("last_result") if task.get("found") else None
    return row


def format_summary(row: Dict[str, Any]) -> str:
    return (
        f"HEALTH_SUMMARY pvz={row['pvz_id']} code={row['code_version']} up_to_date={row['code_up_to_date']} "
        f"C_free={row['c_free_gb']}GB({row['c_free_pct']}%) D_free={row['d_free_gb']}GB({row['d_free_pct']}%) "
        f"disks={row['disks_total']} warning={row['disks_warning']} "
        f"shutdowns={row['unexpected_shutdowns']} whea={row['whea_errors']} disk_errors={row['disk_errors']} "
        f"ntfs={row['ntfs_errors']} display={row['display_resets']} "
        f"tasks(op/cam/sys)={row['task_operator_result']}/{row['task_camera_result']}/{row['task_system_result']} "
        f"reports_ok={row['reports_last_success']} copy_ok={row['copy_last_success']} "
        f"updater_ok={row['updater_last_success']} elevated={row['probe_elevated']}"
    )


def format_disk(row: Dict[str, Any]) -> str:
    return (
        f"HEALTH_DISK {row['disk_serial']} {row['model']} {row['media_type']} {row['size_gb']}GB vol={row['volumes']} "
        f"health={row['health_status']} predict={row['predict_failure']} t={row['temperature_c']} "
        f"poh={row['smart_9_power_on_hours']} realloc={row['smart_5_reallocated']} "
        f"pending={row['smart_197_pending']} offline_unc={row['smart_198_offline_uncorrectable']} "
        f"reported_unc={row['smart_187_reported_uncorrectable']} crc={row['smart_199_crc_errors']}"
    )


def is_configured_pvz(pvz_id: str) -> bool:
    """PVZ_ID задан: не заглушка эталона (docs/imaging, НЕ_НАСТРОЕН) и не значение по умолчанию base_config."""
    return bool(pvz_id) and pvz_id not in ("НЕ_НАСТРОЕН", "UNKNOWN")


def sheet_value(value: Any) -> Any:
    """Значение для ячейки: ISO-время -> 'YYYY-MM-DD HH:MM:SS' (Google распознает как дату-время)."""
    if isinstance(value, str) and len(value) == 19 and value[10] == "T":
        return value.replace("T", " ")
    return value


def rows_for_sheet(rows: List[Dict[str, Any]], columns: List[str]) -> List[Dict[str, Any]]:
    """Только колонки листа; None не передается (загрузчик оставляет такие ячейки пустыми)."""
    return [{name: sheet_value(row[name]) for name in columns if row.get(name) is not None} for row in rows]


def ensure_worksheets(credentials_path: str, spreadsheet_id: str, table_configs: List[Any], logger) -> None:
    """Создает недостающие листы с заголовками (загрузчик работает только с существующим листом).

    Существующий лист не меняется: несовпадение заголовка — предупреждение HEALTH_SHEET_HEADER_MISMATCH.
    """
    import gspread

    spreadsheet = gspread.service_account(filename=credentials_path).open_by_key(spreadsheet_id)
    existing = {ws.title: ws for ws in spreadsheet.worksheets()}
    for table in table_configs:
        headers = [column.name for column in table.columns]
        worksheet = existing.get(table.worksheet_name)
        if worksheet is None:
            try:
                worksheet = spreadsheet.add_worksheet(title=table.worksheet_name, rows=1000, cols=len(headers))
            except gspread.exceptions.APIError as e:
                # Другой ПВЗ создал лист одновременно (все собирают в 13:10)
                if "already exists" not in str(e).lower():
                    raise
                worksheet = spreadsheet.worksheet(table.worksheet_name)
            logger.info(f"HEALTH_SHEET_CREATED: лист {table.worksheet_name}")
        current = worksheet.row_values(1)
        if not current:
            worksheet.update(range_name="A1", values=[headers])
            worksheet.freeze(rows=1)
        elif current[:len(headers)] != headers:
            logger.warning(f"HEALTH_SHEET_HEADER_MISMATCH: лист {table.worksheet_name}, заголовок {current}")


def upload_rows(rows: List[Dict[str, Any]], table_config: Any, credentials_path: str, spreadsheet_id: str,
                logger) -> Dict[str, Any]:
    """Пакетный upsert строк (как KPI в режиме batch): ключ — unique_key_columns конфигурации листа."""
    from scheduler_runner.utils.uploader import upload_batch_data

    if not rows:
        return {"success": True, "uploaded_records": 0}
    columns = [column.name for column in table_config.columns]
    connection_params = {
        "CREDENTIALS_PATH": credentials_path,
        "SPREADSHEET_ID": spreadsheet_id,
        "WORKSHEET_NAME": table_config.worksheet_name,
        "TABLE_CONFIG": deepcopy(table_config),
        "REQUIRED_CONNECTION_PARAMS": ["CREDENTIALS_PATH", "SPREADSHEET_ID", "WORKSHEET_NAME", "TABLE_CONFIG"],
    }
    return upload_batch_data(
        data_list=rows_for_sheet(rows, columns),
        connection_params=connection_params,
        logger=logger,
        strategy="update_or_append",
        UPLOAD_MODE="batch",
    ) or {"success": False, "error": "пустой результат загрузчика"}


def upload_health(health_row: Dict[str, Any], disk_rows: List[Dict[str, Any]], sheets_config: Dict[str, Any],
                  spreadsheet_id: str, logger, jitter: bool = True) -> bool:
    if jitter and sheets_config.get("UPLOAD_START_JITTER_SECONDS"):
        time.sleep(random.uniform(0, float(sheets_config["UPLOAD_START_JITTER_SECONDS"])))
    health_table, disk_table = sheets_config["TABLES"]
    credentials = sheets_config["CREDENTIALS_PATH"]
    ensure_worksheets(credentials, spreadsheet_id, sheets_config["TABLES"], logger)
    ok = True
    for table, rows in ((health_table, [health_row]), (disk_table, disk_rows)):
        result = upload_rows(rows, table, credentials, spreadsheet_id, logger)
        success = bool(result.get("success"))
        ok = ok and success
        logger.log(20 if success else 40,
                   f"HEALTH_UPLOAD sheet={table.worksheet_name} rows={len(rows)} success={success} "
                   f"error={result.get('error', '')}")
    return ok


def load_state(path: str, logger) -> Dict[str, Any]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as e:
        # Испорченный файл: начать заново (тревоги придут как новые), но не падать
        logger.warning(f"HEALTH_STATE_UNREADABLE: {path}: {e}")
        return {}


def save_state(path: str, state: Dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_suffix(".tmp")
    temp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(target)


def send_alert_message(message: str, logger) -> Dict[str, Any]:
    """ВК — тот же чат и параметры, что у уведомлений отчетов (план, раздел 8)."""
    from scheduler_runner.tasks.reports.config.reports_paths import REPORTS_PATHS
    from scheduler_runner.utils.notifications import send_notification

    connection_params = REPORTS_PATHS.get("NOTIFICATION_CONNECTION_PARAMS") or {}
    if not connection_params:
        return {"success": False, "error": "нет параметров подключения уведомлений"}
    try:
        return send_notification(message=message, connection_params=connection_params, logger=logger) or {}
    except Exception as e:
        return {"success": False, "error": str(e)}


def collect(config: Dict[str, Any], hours: int, pvz_id: str, now: Optional[datetime] = None) -> Tuple[Dict, List, Dict]:
    now = now or datetime.now()
    probe = run_probe(config["PROBE_SCRIPT"], hours, config["SCHEDULED_TASKS"], config["PROBE_TIMEOUT_SECONDS"])
    work_date = now.strftime("%Y-%m-%d")
    disk_rows = build_disk_rows(probe, pvz_id, work_date)
    code_state = collect_code_state(config["REPO_DIR"], config["MAIN_BRANCH"], config["GIT_TIMEOUT_SECONDS"])
    scheduler_state = collect_scheduler_state(config, now.date())
    health_row = build_health_row(probe, code_state, scheduler_state, disk_rows, pvz_id, work_date)
    return health_row, disk_rows, probe


def notify_and_save(result, health_row: Dict[str, Any], state_file: str, pvz_id: str, notify: bool, logger) -> None:
    """Сообщение в ВК только при изменениях; состояние сохраняется, если сообщать нечего или отправка удалась —
    иначе изменение будет сообщено при следующем сборе."""
    from scheduler_runner.tasks.system.health_alerts import format_message

    if not result.has_news:
        logger.info("HEALTH_NOTIFY: изменений нет, уведомление не нужно")
        save_state(state_file, result.state)
        return
    message = format_message(pvz_id, health_row, result)
    if not notify:
        logger.info("HEALTH_NOTIFY пропущено (--no-notify или PVZ_ID не задан), состояние не изменено. Текст:\n"
                    + message)
        return
    sent = send_alert_message(message, logger)
    if sent.get("success"):
        logger.info("HEALTH_NOTIFY: уведомление отправлено")
        save_state(state_file, result.state)
    else:
        logger.error(f"HEALTH_NOTIFY_FAILED: {sent.get('error', sent)}")


def main() -> None:
    from scheduler_runner.tasks.system.health_alerts import evaluate

    args = parse_arguments()
    logger = configure_logger(user=SCRIPT_CONFIG["USER"], task_name=SCRIPT_CONFIG["TASK_NAME"],
                              detailed=args.detailed_logs)
    try:
        health_row, disk_rows, probe = collect(SCRIPT_CONFIG, args.hours, PVZ_ID)
    except ProbeError as e:
        logger.error(f"HEALTH_PROBE_FAILED: {e}")
        sys.exit(1)

    state_file = args.state_file or SCRIPT_CONFIG["STATE_FILE"]
    result = evaluate(health_row, disk_rows, load_state(state_file, logger), datetime.now(), SCRIPT_CONFIG["ALERTS"])
    health_row["status"], health_row["alerts"] = result.status, result.codes

    logger.info(format_summary(health_row))
    for disk_row in disk_rows:
        logger.info(format_disk(disk_row))
    logger.info(f"HEALTH_ALERTS status={result.status} codes={result.codes or '-'} new={len(result.new)} "
                f"worsened={len(result.worsened)} reminders={len(result.reminders)} resolved={len(result.resolved)}")
    if health_row["probe_limited"]:
        # Без повышенных прав нет SMART и части журналов: задаче system нужны «наивысшие права» (план, раздел 3.3)
        logger.warning(f"HEALTH_PROBE_LIMITED: {health_row['probe_limited']}")
    if args.output:
        Path(args.output).write_text(
            json.dumps({"health": health_row, "disks": disk_rows, "probe": probe}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info(f"Результат сохранен: {args.output}")

    configured = is_configured_pvz(PVZ_ID)
    uploaded = True
    if args.no_upload:
        logger.info("HEALTH_UPLOAD пропущена (--no-upload)")
    elif not configured and not args.spreadsheet_id:
        # Эталон образа / клон до after_deploy.ps1: в общую книгу не писать (явно указанную тестовую — можно)
        logger.warning(f"HEALTH_UPLOAD пропущена: PVZ_ID не задан ({PVZ_ID})")
    else:
        from scheduler_runner.tasks.system.config.health_sheets_config import HEALTH_SHEETS_CONFIG

        spreadsheet_id = args.spreadsheet_id or HEALTH_SHEETS_CONFIG["SPREADSHEET_ID"]
        try:
            uploaded = upload_health(health_row, disk_rows, HEALTH_SHEETS_CONFIG, spreadsheet_id, logger,
                                     jitter=not args.no_jitter)
        except Exception as e:
            logger.error(f"HEALTH_UPLOAD_FAILED: {e}", exc_info=True)
            uploaded = False

    # Уведомление не зависит от записи в таблицу: о проблеме диска важнее сообщить, даже если Google недоступен
    notify_and_save(result, health_row, state_file, PVZ_ID, notify=configured and not args.no_notify, logger=logger)
    if not uploaded:
        sys.exit(1)


if __name__ == "__main__":
    main()
