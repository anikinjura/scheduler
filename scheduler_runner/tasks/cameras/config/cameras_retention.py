"""
cameras_retention.py
Политика сроков хранения записей камер (в днях) для CleanupScript.

Поддерживает:
- дефолтные значения по сценариям (`local`, `network`);
- переопределения по конкретному PVZ_ID.
"""
__version__ = "0.0.1"

from typing import Dict


RETENTION_DEFAULTS_DAYS: Dict[str, int] = {
    "local": 8,
    "network": 120,
}

# Пример override:
# "СОСНОВКА_10": {"local": 30, "network": 120}
RETENTION_OVERRIDES_DAYS: Dict[str, Dict[str, int]] = {
    "СОСНОВКА_10": {
        "local": 90,
        "network": 45,
    }
}


def get_retention_days(pvz_id: str, scenario: str) -> int:
    """
    Возвращает порог хранения (в днях) для сценария `local`/`network`.
    Приоритет:
    1) RETENTION_OVERRIDES_DAYS[pvz_id][scenario]
    2) RETENTION_DEFAULTS_DAYS[scenario]
    """
    if scenario not in RETENTION_DEFAULTS_DAYS:
        raise ValueError(
            f"Неизвестный сценарий retention: {scenario}. "
            f"Допустимые: {', '.join(RETENTION_DEFAULTS_DAYS.keys())}"
        )

    object_overrides = RETENTION_OVERRIDES_DAYS.get(pvz_id, {})
    return int(object_overrides.get(scenario, RETENTION_DEFAULTS_DAYS[scenario]))
