"""
cameras_paths.py

Определяет все специфичные для задачи cameras пути и переменные, зависящие от среды (production/test) и PVZ_ID.

Используется для централизованного хранения путей к локальным, сетевым и резервным директориям с видеозаписями.
Поддерживает кириллические имена ПВЗ с использованием транслитерации для сетевых путей.

Пример использования:
    from .cameras_paths import CAMERAS_PATHS
    local_path = CAMERAS_PATHS['CAMERAS_LOCAL']

Структура CAMERAS_PATHS:
    {
        'CAMERAS_LOCAL': Path,   # Путь к локальной директории с видеоархивом
        'CAMERAS_NETWORK': Path, # Путь к сетевой директории с видеоархивом (с транслитерацией PVZ_ID)
    }

Author: anikinjura
"""
__version__ = '0.0.2'

from pathlib import Path
import os
from config.base_config import PVZ_ID, ENV_MODE
from scheduler_runner.utils.system import SystemUtils

# Получаем безопасное имя для использования в сетевых путях (транслитерация кириллицы)
def get_safe_pvz_path_name(pvz_id):
    """Преобразует PVZ_ID в безопасное имя для использования в сетевых путях файловой системы."""
    return SystemUtils.cyrillic_to_translit(str(pvz_id))

if ENV_MODE == 'production':
    CAMERAS_LOCAL = Path('D:/camera')
    CAMERAS_NETWORK = Path('O:/cameras') / get_safe_pvz_path_name(PVZ_ID)
    TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN_PROD")               # Токен для продакшен-бота
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID_PROD")           # Чат-ID для продакшен-чата
else:
    CAMERAS_LOCAL = Path('C:/tools/scheduler/tests/TestEnvironment/D_camera')
    CAMERAS_NETWORK = Path('C:/tools/scheduler/tests/TestEnvironment/O_cameras') / get_safe_pvz_path_name(PVZ_ID)
    TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN_TEST")               # Токен для тест-бота
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID_TEST")           # Чат-ID для тест-чата

CAMERAS_PATHS = {
    'CAMERAS_LOCAL': CAMERAS_LOCAL,
    'CAMERAS_NETWORK': CAMERAS_NETWORK,
    'TELEGRAM_TOKEN': TELEGRAM_TOKEN,
    'TELEGRAM_CHAT_ID': TELEGRAM_CHAT_ID,
}