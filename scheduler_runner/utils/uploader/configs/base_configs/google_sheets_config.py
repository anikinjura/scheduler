"""
Конфигурационный файл для базового загрузчика данных в Google Sheets

=== ОПИСАНИЕ КОНФИГУРАЦИИ ===

Этот файл содержит минимально необходимые параметры для работы с Google Sheets.
Все специфичные параметры должны передаваться извне.
Конфигурация наследует все параметры из BASE_UPLOADER_CONFIG и добавляет
только те, что необходимы для базовой работы с Google Sheets.

=== СТРУКТУРА КОНФИГУРАЦИИ ===

Параметры подключения:
- REQUIRED_CONNECTION_PARAMS: Список обязательных параметров подключения

=== ИЗМЕНЕНИЯ В ВЕРСИИ 1.0.0 ===
- Создана базовая конфигурация для загрузчика Google Sheets
- Убраны специфичные параметры, теперь все специфичные параметры передаются извне
- Оставлены только общие параметры валидации и подключения
"""

from .base_uploader_config import BASE_UPLOADER_CONFIG

# Параметры подключения
REQUIRED_CONNECTION_PARAMS = ["CREDENTIALS_PATH", "SPREADSHEET_ID", "WORKSHEET_NAME", "TABLE_CONFIG"]

# Базовая конфигурация для Google Sheets
GOOGLE_SHEETS_BASE_CONFIG = {
    **BASE_UPLOADER_CONFIG,

    # Параметры подключения
    "REQUIRED_CONNECTION_PARAMS": REQUIRED_CONNECTION_PARAMS,
}