"""
cameras_paths.py

Определяет все специфичные для задачи cameras пути и переменные, зависящие от среды (production/test) и PVZ_ID.

Используется для централизованного хранения путей к локальным, сетевым и резервным директориям с видеозаписями.

Пример использования:
    from .cameras_paths import CAMERAS_PATHS
    local_path = CAMERAS_PATHS['CAMERAS_LOCAL']

Структура CAMERAS_PATHS:
    {
        'CAMERAS_LOCAL': Path,   # Путь к локальной директории с видеоархивом
        'CAMERAS_NETWORK': Path, # Путь к сетевой директории с видеоархивом
    }

Author: anikinjura
"""
__version__ = '0.0.1'

from pathlib import Path
import os
from config.base_config import PVZ_ID, ENV_MODE

if ENV_MODE == 'production':
    CAMERAS_LOCAL = Path('D:/camera')
    CAMERAS_NETWORK = Path('O:/cameras') / str(PVZ_ID)
    TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN_PROD")               # Токен для продакшен-бота
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID_PROD")           # Чат-ID для продакшен-чата    
else:
    CAMERAS_LOCAL = Path('C:/tools/scheduler/tests/TestEnvironment/D_camera')
    CAMERAS_NETWORK = Path('C:/tools/scheduler/tests/TestEnvironment/O_cameras') / str(PVZ_ID)
    TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN_TEST")               # Токен для продакшен-бота
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID_TEST")           # Чат-ID для продакшен-чата    

CAMERAS_PATHS = {
    'CAMERAS_LOCAL': CAMERAS_LOCAL,
    'CAMERAS_NETWORK': CAMERAS_NETWORK,
    'TELEGRAM_TOKEN': TELEGRAM_TOKEN,
    'TELEGRAM_CHAT_ID': TELEGRAM_CHAT_ID,
}