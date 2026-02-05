"""
Изолированный микросервис загрузчика данных

Архитектура:
- Микросервис полностью изолирован и принимает все специфичные параметры извне
- Не содержит жестко закодированных значений, зависимостей от внешних конфигураций
- Использует интерфейс для взаимодействия с внешними сервисами

Интерфейс:
- upload_data(): Загрузка одиночного набора данных
- upload_batch_data(): Пакетная загрузка данных
- test_connection(): Проверка подключения

Параметры, которые принимаются извне:
- Параметры подключения (путь к учетным данным, ID таблицы и т.д.)
- Данные для загрузки
- Готовый логгер
- Дополнительные параметры загрузки (опционально)
"""

from .interface import upload_data, upload_batch_data, test_connection
from .configs.base_configs.base_uploader_config import BASE_UPLOADER_CONFIG
from .configs.base_configs.google_sheets_config import GOOGLE_SHEETS_BASE_CONFIG

__all__ = [
    'upload_data',
    'upload_batch_data',
    'test_connection',
    'BASE_UPLOADER_CONFIG',
    'GOOGLE_SHEETS_BASE_CONFIG'
]