"""
Модуль конфигураций для реализаций парсеров отчетов

Этот модуль предоставляет доступ к конфигурационным файлам,
используемым в конкретных реализациях парсеров.
"""
from .multi_step_ozon_config import MULTI_STEP_OZON_CONFIG
from .ozon_available_pvz_config import OZON_AVAILABLE_PVZ_CONFIG

__all__ = [
    'MULTI_STEP_OZON_CONFIG',
    'OZON_AVAILABLE_PVZ_CONFIG',
]
