"""
Конфигурация для BaseUploader

Содержит минимальный набор параметров для работы с загрузкой данных.
Все специфичные параметры должны передаваться извне.
Является базовой конфигурацией, от которой наследуются конфигурации дочерних классов.
Дочерние конфигурации могут переопределять значения по умолчанию, указанные в этом файле.

=== ОПИСАНИЕ ПАРАМЕТРОВ ===

Параметры загрузки:
- MAX_RETRIES: Максимальное количество попыток загрузки
- DELAY_BETWEEN_RETRIES: Задержка между попытками загрузки
- BATCH_SIZE: Размер пакета для пакетной загрузки
- DELAY_BETWEEN_REQUESTS: Задержка между запросами

Параметры валидации:
- REQUIRED_FIELDS: Список обязательных полей в данных
- REQUIRED_CONNECTION_PARAMS: Список обязательных параметров подключения

=== ИЗМЕНЕНИЯ В ВЕРСИИ 1.0.0 ===
- Создана базовая конфигурация для загрузчика данных
- Убраны специфичные параметры, теперь все специфичные параметры передаются извне
- Оставлены только общие параметры валидации и загрузки
"""

# Параметры загрузки
MAX_RETRIES = 3
DELAY_BETWEEN_RETRIES = 1.0
BATCH_SIZE = 100
DELAY_BETWEEN_REQUESTS = 0.1

# Параметры валидации
REQUIRED_FIELDS = []
REQUIRED_CONNECTION_PARAMS = []

# Базовая конфигурация для загрузчика
BASE_UPLOADER_CONFIG = {
    # Параметры загрузки
    "MAX_RETRIES": MAX_RETRIES,
    "DELAY_BETWEEN_RETRIES": DELAY_BETWEEN_RETRIES,
    "BATCH_SIZE": BATCH_SIZE,
    "DELAY_BETWEEN_REQUESTS": DELAY_BETWEEN_REQUESTS,

    # Параметры валидации
    "REQUIRED_FIELDS": REQUIRED_FIELDS,
    "REQUIRED_CONNECTION_PARAMS": REQUIRED_CONNECTION_PARAMS,
}