"""
reports_paths.py

Определяет все специфичные для задачи reports пути и переменные, зависящие от среды (production/test) и PVZ_ID.

Используется для централизованного хранения путей к локальным, сетевым и резервным директориям с файлами отчетов.
Поддерживает кириллические имена ПВЗ с использованием транслитерации для сетевых путей.

Пример использования:
    from .reports_paths import REPORTS_PATHS
    reports_path = REPORTS_PATHS['REPORTS_JSON']

Структура REPORTS_PATHS:
    {
        'REPORTS_JSON': Path,      # Путь к директории с JSON-файлами отчетов
        'REPORTS_LOGS': Path,      # Путь к директории с логами отчетов
        'GOOGLE_SHEETS_CREDENTIALS': Path,  # Путь к файлу сервисного аккаунта для Google Sheets
    }

Author: anikinjura
"""
__version__ = '1.0.0'

from pathlib import Path
import os
from config.base_config import PVZ_ID, ENV_MODE, PATH_CONFIG

# Получаем BASE_DIR из PATH_CONFIG
BASE_DIR = PATH_CONFIG['BASE_DIR']
from scheduler_runner.utils.system import SystemUtils

# Получаем безопасное имя для использования в сетевых путях (транслитерация кириллицы)
def get_safe_pvz_path_name(pvz_id):
    """Преобразует PVZ_ID в безопасное имя для использования в сетевых путях файловой системы."""
    return SystemUtils.cyrillic_to_translit(str(pvz_id))

# Базовая директория для отчетов из централизованной конфигурации
BASE_REPORTS_DIR = PATH_CONFIG['REPORTS_ROOT']

mode_suffix = "PROD" if ENV_MODE == "production" else "TEST"
notification_provider = os.environ.get(f"NOTIFICATION_PROVIDER_{mode_suffix}", "telegram").lower()
vk_access_token = os.environ.get(f"VK_ACCESS_TOKEN_{mode_suffix}")
vk_peer_id = os.environ.get(f"VK_PEER_ID_{mode_suffix}")
vk_api_version = os.environ.get("VK_API_VERSION", "5.199")


def build_notification_connection_params():
    if notification_provider == "vk":
        return {
            "NOTIFICATION_PROVIDER": "vk",
            "VK_ACCESS_TOKEN": vk_access_token,
            "VK_PEER_ID": vk_peer_id,
            "VK_API_VERSION": vk_api_version,
        }

    return {
        "NOTIFICATION_PROVIDER": "telegram",
        "TELEGRAM_BOT_TOKEN": TELEGRAM_TOKEN,
        "TELEGRAM_CHAT_ID": TELEGRAM_CHAT_ID,
    }


# Формируем пути в зависимости от режима среды
if ENV_MODE == 'production':
    REPORTS_JSON = BASE_REPORTS_DIR / "json"
    GOOGLE_SHEETS_CREDENTIALS = BASE_DIR / ".env" / "gspread" / "scheduler-test-account-b62ccb681f06.json"
    TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN_PROD")               # Токен для продакшен-бота
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID_PROD")           # Чат-ID для продакшен-чата
elif ENV_MODE == 'test':
    REPORTS_JSON = BASE_REPORTS_DIR / "json"  # Для тестовой среды используем ту же директорию, что и production
    GOOGLE_SHEETS_CREDENTIALS = BASE_DIR / ".env" / "gspread" / "scheduler-test-account-b62ccb681f06.json"
    TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN_TEST")               # Токен для тест-бота
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID_TEST")           # Чат-ID для тест-чата
else:  # development
    REPORTS_JSON = BASE_REPORTS_DIR / "dev" / "json"
    GOOGLE_SHEETS_CREDENTIALS = BASE_DIR / ".env" / "gspread" / "scheduler-test-account-b62ccb681f06.json"
    TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN_DEV")               # Токен для дев-бота
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID_DEV")           # Чат-ID для дев-чата

# Создаем директории, если они не существуют
REPORTS_JSON.mkdir(parents=True, exist_ok=True)

REPORTS_PATHS = {
    'REPORTS_JSON': REPORTS_JSON,
    'GOOGLE_SHEETS_CREDENTIALS': GOOGLE_SHEETS_CREDENTIALS,
    'NOTIFICATION_PROVIDER': notification_provider,
    'TELEGRAM_TOKEN': TELEGRAM_TOKEN,
    'TELEGRAM_CHAT_ID': TELEGRAM_CHAT_ID,
    'VK_ACCESS_TOKEN': vk_access_token,
    'VK_PEER_ID': vk_peer_id,
    'VK_API_VERSION': vk_api_version,
    'NOTIFICATION_CONNECTION_PARAMS': build_notification_connection_params(),
    'FAILOVER_APPS_SCRIPT_URL': os.environ.get(
        "FAILOVER_APPS_SCRIPT_URL",
        "https://script.google.com/macros/s/AKfycbzAu2cv4PhON28JmWOM9uM4TTnI9llOPIkIhq5JqeNq_W0iHdKj2H9Fbw1Veeqng-YC0Q/exec",
    ),
    'FAILOVER_SHARED_SECRET': os.environ.get(
        "FAILOVER_SHARED_SECRET",
        "3f2e8f5b7f8d4e9a9b2c1d6e7f8a1b3c",
    ),
}

