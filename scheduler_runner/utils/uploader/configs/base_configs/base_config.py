"""
Базовая конфигурация для Uploader

Содержит стандартные параметры для работы с системами загрузки данных.
Является базовой конфигурацией, от которой наследуются конфигурации дочерних классов.
Дочерние конфигурации могут переопределять значения по умолчанию, указанные в этом файле.

=== ОПИСАНИЕ ПАРАМЕТРОВ ===

Параметры подключения:
- CREDENTIALS_PATH: Путь к файлу учетных данных для внешней системы
- CONNECTION_TIMEOUT: Таймаут подключения в секундах
- RETRY_ATTEMPTS: Количество попыток повторного подключения при ошибке

Параметры обработки данных:
- BATCH_SIZE: Размер пакета для пакетной обработки данных
- MAX_RETRIES: Максимальное количество попыток выполнения операции
- DELAY_BETWEEN_RETRIES: Задержка между попытками в секундах

Параметры логирования:
- LOG_LEVEL: Уровень логирования (DEBUG, INFO, WARNING, ERROR)
- DETAILED_LOGS: Включить детализированные логи
- LOG_FILE_PATH: Путь к файлу логов (если используется файловое логирование)

=== ИЗМЕНЕНИЯ В ВЕРСИИ 1.0.0 ===
- Создана базовая структура конфигурации для загрузчика
- Добавлены параметры подключения и обработки данных
- Добавлены параметры логирования
"""

# Параметры подключения
CREDENTIALS_PATH = ""
CONNECTION_TIMEOUT = 30
RETRY_ATTEMPTS = 3

# Параметры обработки данных
BATCH_SIZE = 100
MAX_RETRIES = 3
DELAY_BETWEEN_RETRIES = 1

# Параметры логирования
LOG_LEVEL = "INFO"
DETAILED_LOGS = False
LOG_FILE_PATH = ""

# Базовая конфигурация
BASE_CONFIG = {
    # Параметры подключения
    "CREDENTIALS_PATH": CREDENTIALS_PATH,
    "CONNECTION_TIMEOUT": CONNECTION_TIMEOUT,
    "RETRY_ATTEMPTS": RETRY_ATTEMPTS,

    # Параметры обработки данных
    "BATCH_SIZE": BATCH_SIZE,
    "MAX_RETRIES": MAX_RETRIES,
    "DELAY_BETWEEN_RETRIES": DELAY_BETWEEN_RETRIES,

    # Параметры логирования
    "LOG_LEVEL": LOG_LEVEL,
    "DETAILED_LOGS": DETAILED_LOGS,
    "LOG_FILE_PATH": LOG_FILE_PATH,
}