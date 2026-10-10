"""
health_alerts.py

Правила тревог HealthMonitorScript (docs/HEALTH_MONITOR_PLAN.md, раздел 4) и решение, о чем сообщать.

Чистая логика без сети и Windows:
    evaluate(health_row, disk_rows, previous_state, now, settings) -> AlertsResult

Сообщается только изменение: новая тревога, ухудшение (warning -> critical), повтор неустраненной критической раз в
REMINDER_DAYS дней и устранение. «Рост» SMART (переназначенные, CRC) сравнивается со снимком прошлого сбора и
срабатывает на каждый новый рост. Состояние (снимок дисков, активные тревоги, начало условий) хранится в JSON-файле
на компьютере ПВЗ — без чтений Google-таблицы и при недоступном Google.

Author: anikinjura
"""

__version__ = '0.0.1'

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

LEVEL_WARNING = "warning"
LEVEL_CRITICAL = "critical"
LEVEL_ORDER = {LEVEL_WARNING: 1, LEVEL_CRITICAL: 2}
LEVEL_TITLE = {LEVEL_WARNING: "внимание", LEVEL_CRITICAL: "критично"}

# Результаты задачи Планировщика, которые не являются ошибкой: 0 — успех, 0x41300 — готова, 0x41301 — выполняется,
# 0x41303 — еще не запускалась, 0x41325 — запуск в очереди
TASK_RESULTS_OK = {0, 0x41300, 0x41301, 0x41303, 0x41325}
# Тревоги-события: срабатывают на каждый новый рост, а не держатся как состояние
GROWTH_CODES = {"DISK_REALLOCATED_GROWTH", "DISK_CRC_GROWTH"}
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

DEFAULT_SETTINGS = {
    "LOW_SPACE_WARNING_PCT": 15,
    "LOW_SPACE_WARNING_GB": 10,
    "LOW_SPACE_CRITICAL_PCT": 5,
    "DISK_HOT_C": 50,
    "SHUTDOWNS_CRITICAL": 3,
    "CODE_OUTDATED_HOURS": 24,
    "REPORTS_STALE_HOURS": 36,
    "COPY_STALE_HOURS": 36,
    "REMINDER_DAYS": 7,
}


@dataclass
class Alert:
    code: str
    level: str
    subject: str  # что именно: диск, том, задача; часть ключа тревоги
    text: str     # что случилось, со значениями
    advice: str = ""

    @property
    def key(self) -> str:
        return f"{self.code}|{self.subject}"


@dataclass
class AlertsResult:
    alerts: List[Alert]                  # все активные сейчас
    new: List[Alert] = field(default_factory=list)
    worsened: List[Alert] = field(default_factory=list)
    reminders: List[Alert] = field(default_factory=list)
    resolved: List[Dict[str, Any]] = field(default_factory=list)  # записи прошлого состояния
    state: Dict[str, Any] = field(default_factory=dict)            # сохранить после успешного уведомления

    @property
    def status(self) -> str:
        if any(a.level == LEVEL_CRITICAL for a in self.alerts):
            return LEVEL_CRITICAL
        return LEVEL_WARNING if self.alerts else "ok"

    @property
    def codes(self) -> str:
        return ",".join(sorted({a.code for a in self.alerts}))

    @property
    def has_news(self) -> bool:
        return bool(self.new or self.worsened or self.reminders or self.resolved)


def _parse_time(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.strptime(str(value)[:19].replace("T", " "), TIME_FORMAT)
    except ValueError:
        return None


def _num(value: Any) -> str:
    """Число для текста сообщения: десятичная запятая (66,1 ГБ)."""
    return str(value).replace(".", ",") if isinstance(value, float) else str(value)


def _disk_title(disk: Dict[str, Any]) -> str:
    letters = [v.strip() for v in str(disk.get("volumes") or "").split(",") if v.strip()]
    volumes = f" ({', '.join(v + ':' for v in letters)})" if letters else ""
    return f"диск {disk.get('model', '')} [{disk.get('disk_serial', '')}]{volumes}"


def _grew(current: Optional[int], previous: Optional[int]) -> bool:
    return current is not None and previous is not None and int(current) > int(previous)


def _disk_alerts(disk_rows: List[Dict[str, Any]], previous_disks: Dict[str, Dict[str, Any]],
                 settings: Dict[str, Any]) -> List[Alert]:
    alerts = []
    for disk in disk_rows:
        serial, title = disk.get("disk_serial", ""), _disk_title(disk)
        previous = previous_disks.get(serial, {})
        if disk.get("predict_failure") is True or disk.get("health_status") not in (None, "", "Healthy"):
            alerts.append(Alert(
                "DISK_PREDICT_FAILURE", LEVEL_CRITICAL, serial,
                f"{title}: Windows/SMART сообщает о неисправности (health={disk.get('health_status')}, "
                f"переназначенных {disk.get('smart_5_reallocated')}, CRC {disk.get('smart_199_crc_errors')})",
                "заменить диск; если это системный — подготовить резервный компьютер из образа"))
        growth = [(name, field_name) for name, field_name in (
            ("переназначенных", "smart_5_reallocated"), ("ожидающих", "smart_197_pending"),
            ("неисправимых", "smart_198_offline_uncorrectable")) if _grew(disk.get(field_name), previous.get(field_name))]
        if growth:
            details = ", ".join(f"{name} {previous.get(f)} → {disk.get(f)}" for name, f in growth)
            alerts.append(Alert("DISK_REALLOCATED_GROWTH", LEVEL_CRITICAL, serial,
                                f"{title}: растет число сбойных секторов ({details})",
                                "диск деградирует — заменить"))
        if _grew(disk.get("smart_199_crc_errors"), previous.get("smart_199_crc_errors")):
            alerts.append(Alert("DISK_CRC_GROWTH", LEVEL_WARNING, serial,
                                f"{title}: ошибки передачи по SATA (CRC) {previous.get('smart_199_crc_errors')} → "
                                f"{disk.get('smart_199_crc_errors')}",
                                "заменить SATA-кабель / переключить в другой порт, проверить питание"))
        temperature = disk.get("temperature_c")
        if temperature is not None and int(temperature) > settings["DISK_HOT_C"]:
            alerts.append(Alert("DISK_HOT", LEVEL_WARNING, serial, f"{title}: температура {temperature} °C",
                                "проверить вентиляцию корпуса и пыль"))
    return alerts


def _system_alerts(row: Dict[str, Any], now: datetime, conditions: Dict[str, str],
                   settings: Dict[str, Any]) -> List[Alert]:
    alerts = []
    free_pct, free_gb = row.get("c_free_pct"), row.get("c_free_gb")
    if free_pct is not None:
        if free_pct < settings["LOW_SPACE_CRITICAL_PCT"]:
            level = LEVEL_CRITICAL
        elif free_pct < settings["LOW_SPACE_WARNING_PCT"] or (free_gb is not None and free_gb < settings["LOW_SPACE_WARNING_GB"]):
            level = LEVEL_WARNING
        else:
            level = None
        if level:
            alerts.append(Alert("LOW_SPACE_C", level, "C", f"на C: свободно {_num(free_gb)} ГБ ({_num(free_pct)}%)",
                                "освободить место на системном диске"))

    shutdowns = (row.get("unexpected_shutdowns") or 0) + (row.get("bugchecks") or 0)
    if shutdowns:
        level = LEVEL_CRITICAL if shutdowns >= settings["SHUTDOWNS_CRITICAL"] else LEVEL_WARNING
        alerts.append(Alert("UNEXPECTED_SHUTDOWN", level, "system",
                            f"аварийных перезагрузок / сбоев за {row.get('events_window_hours')} ч: {shutdowns}",
                            "зависания или пропадание питания — проверить компьютер и блок питания"))
    if row.get("whea_errors"):
        alerts.append(Alert("WHEA", LEVEL_CRITICAL, "system",
                            f"неустранимые аппаратные ошибки (WHEA): {row.get('whea_errors')}",
                            "сбой процессора, памяти или платы — готовить замену компьютера"))
    io_errors = (row.get("disk_errors") or 0) + (row.get("ntfs_errors") or 0)
    if io_errors:
        alerts.append(Alert("DISK_IO_ERRORS", LEVEL_WARNING, "system",
                            f"ошибки дисков / файловой системы за {row.get('events_window_hours')} ч: {io_errors}",
                            "смотреть SMART дисков и журнал System"))

    # Условия «дольше N часов»: время первого наблюдения хранится в состоянии
    def lasting(name: str, active: bool, hours: int) -> bool:
        if not active:
            conditions.pop(name, None)
            return False
        since = _parse_time(conditions.setdefault(name, now.strftime(TIME_FORMAT)))
        return since is not None and now - since >= timedelta(hours=hours)

    if lasting("code_outdated", row.get("code_up_to_date") is False, settings["CODE_OUTDATED_HOURS"]):
        alerts.append(Alert("CODE_OUTDATED", LEVEL_WARNING, "code",
                            f"код {row.get('code_version')} не совпадает с main {row.get('main_version')} "
                            f"дольше {settings['CODE_OUTDATED_HOURS']} ч",
                            "обновление не проходит — смотреть логи UpdaterScript"))
    reports_ok = _parse_time(row.get("reports_last_success"))
    if reports_ok is None or now - reports_ok > timedelta(hours=settings["REPORTS_STALE_HOURS"]):
        alerts.append(Alert("REPORTS_STALE", LEVEL_WARNING, "reports",
                            f"сбор отчетов не завершался успешно с {row.get('reports_last_success') or '— (нет в логах)'}",
                            "смотреть логи ReportsProcessor; вход «Оператора» в Windows и Турбо ПВЗ"))
    # Записи камер не уходят в облако: ни уведомлений CopyScript, ни CloudMonitor может не быть, если задачи зависают
    # или не запускаются (144, 05–10.10.2026) — эта проверка раз в день от них не зависит
    copy_ok = _parse_time(row.get("copy_last_success"))
    if copy_ok is None or now - copy_ok > timedelta(hours=settings["COPY_STALE_HOURS"]):
        alerts.append(Alert("COPY_STALE", LEVEL_CRITICAL, "copy",
                            f"записи камер не копировались в облако с "
                            f"{row.get('copy_last_success') or '— (нет в логах за срок их хранения)'}",
                            "смотреть логи CopyScript и «Диск-О:» (диск O:); компьютер мог не выключаться вечером"))
    for field_name in ("task_operator_result", "task_camera_result", "task_system_result"):
        result = row.get(field_name)
        if result is not None and int(result) not in TASK_RESULTS_OK:
            task = field_name.replace("task_", "").replace("_result", "")
            alerts.append(Alert("TASK_FAILED", LEVEL_WARNING, task,
                                f"задача «Задачи {task}»: результат последнего запуска {int(result):#x}",
                                f"смотреть logs\\{task}"))
    return alerts


def evaluate(health_row: Dict[str, Any], disk_rows: List[Dict[str, Any]], previous_state: Optional[Dict[str, Any]],
             now: datetime, settings: Optional[Dict[str, Any]] = None) -> AlertsResult:
    settings = {**DEFAULT_SETTINGS, **(settings or {})}
    previous_state = previous_state or {}
    previous_alerts: Dict[str, Dict[str, Any]] = dict(previous_state.get("alerts") or {})
    conditions: Dict[str, str] = dict(previous_state.get("conditions") or {})
    previous_disks = previous_state.get("disks") or {}

    alerts = _disk_alerts(disk_rows, previous_disks, settings) + _system_alerts(health_row, now, conditions, settings)
    # По одному ключу — худшая
    by_key: Dict[str, Alert] = {}
    for alert in alerts:
        if alert.key not in by_key or LEVEL_ORDER[alert.level] > LEVEL_ORDER[by_key[alert.key].level]:
            by_key[alert.key] = alert
    alerts = list(by_key.values())

    result = AlertsResult(alerts=alerts)
    stamp = now.strftime(TIME_FORMAT)
    new_state_alerts = {}
    for alert in alerts:
        before = previous_alerts.get(alert.key)
        record = {"code": alert.code, "subject": alert.subject, "level": alert.level, "text": alert.text,
                  "first_seen": (before or {}).get("first_seen", stamp), "last_notified": stamp}
        growth_event = alert.code in GROWTH_CODES
        if before is None or growth_event:
            result.new.append(alert)
        elif LEVEL_ORDER[alert.level] > LEVEL_ORDER.get(before.get("level"), 0):
            result.worsened.append(alert)
        else:
            last = _parse_time(before.get("last_notified"))
            if alert.level == LEVEL_CRITICAL and (last is None or now - last >= timedelta(days=settings["REMINDER_DAYS"])):
                result.reminders.append(alert)
            else:
                record["last_notified"] = before.get("last_notified", stamp)
        new_state_alerts[alert.key] = record
    # Рост — событие, а не состояние: без нового роста тревога просто уходит, «устранено» о ней не сообщается
    result.resolved = [record for key, record in previous_alerts.items()
                       if key not in new_state_alerts and record.get("code") not in GROWTH_CODES]
    result.state = {
        "updated_at": stamp,
        "pvz_id": health_row.get("pvz_id"),
        "alerts": new_state_alerts,
        "conditions": conditions,
        "disks": {disk.get("disk_serial"): {f: disk.get(f) for f in (
            "smart_5_reallocated", "smart_197_pending", "smart_198_offline_uncorrectable", "smart_199_crc_errors")}
            for disk in disk_rows if disk.get("disk_serial")},
    }
    return result


def format_message(pvz_id: str, health_row: Dict[str, Any], result: AlertsResult) -> str:
    icon = {"ok": "✅", LEVEL_WARNING: "⚠️", LEVEL_CRITICAL: "🔴"}[result.status]
    lines = [f"{icon} {pvz_id}: состояние компьютера — {LEVEL_TITLE.get(result.status, 'норма')}"]

    def section(title: str, alerts: List[Alert]):
        if alerts:
            lines.append(title)
            for a in sorted(alerts, key=lambda x: -LEVEL_ORDER[x.level]):
                lines.append(f"• [{LEVEL_TITLE[a.level]}] {a.text}" + (f" — {a.advice}" if a.advice else ""))

    section("Новое:", result.new)
    section("Ухудшилось:", result.worsened)
    section("Не устранено:", result.reminders)
    if result.resolved:
        lines.append("Устранено:")
        lines.extend(f"• {r.get('text', r.get('code'))}" for r in result.resolved)
    lines.append(f"C: свободно {_num(health_row.get('c_free_gb'))} ГБ ({_num(health_row.get('c_free_pct'))}%), "
                 f"дисков с проблемами: {health_row.get('disks_warning')}")
    return "\n".join(lines)
