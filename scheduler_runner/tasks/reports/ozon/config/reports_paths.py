"""
reports_paths.py

Определяет все специфичные для задачи reports пути и переменные, зависящие от среды (production/test) и PVZ_ID.

Используется для централизованного хранения путей к директориям с отчетами, а также переменных для уведомлений.

Пример использования:
    from .reports_paths import REPORTS_PATHS
    reports_dir = REPORTS_PATHS['REPORTS_DIR']

Структура REPORTS_PATHS:
    {
        'REPORTS_DIR': Path,      # Путь к директории с отчетами
        'TELEGRAM_TOKEN': str,    # Токен для Telegram-уведомлений
        'TELEGRAM_CHAT_ID': str,  # ID чата для Telegram-уведомлений
    }

Author: anikinjura
"""
__version__ = '1.0.0'

from pathlib import Path
import os
from config.base_config import ENV_MODE

if ENV_MODE == 'production':
    REPORTS_DIR = Path('C:/tools/scheduler/reports')
    TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN_PROD")               # Токен для продакшен-бота
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID_PROD")           # Чат-ID для продакшен-чата
else:
    REPORTS_DIR = Path('C:/tools/scheduler/reports')
    TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN_TEST")               # Токен для тест-бота
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID_TEST")           # Чат-ID для тест-чата

REPORTS_PATHS = {
    'REPORTS_DIR': REPORTS_DIR,
    'TELEGRAM_TOKEN': TELEGRAM_TOKEN,
    'TELEGRAM_CHAT_ID': TELEGRAM_CHAT_ID,
}