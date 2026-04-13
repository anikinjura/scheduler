"""
reports_utils.py

Общие утилиты для модуля reports.
"""
from scheduler_runner.utils.system import SystemUtils


def normalize_pvz_id(pvz_id: str | None) -> str:
    """Нормализует PVZ ID: транслитерация → strip → lower."""
    transliterated = SystemUtils.cyrillic_to_translit(str(pvz_id or ""))
    return transliterated.strip().lower()
