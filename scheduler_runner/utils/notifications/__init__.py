"""
Микросервис уведомлений

Изоляция: Микросервис полностью изолирован и принимает все специфичные параметры извне.
Он не содержит жестко закодированных значений, зависимостей от внешних конфигураций
или специфичных настроек, привязанных к конкретной реализации.

Интерфейс:
- send_notification(): Отправка одиночного уведомления
- send_batch_notifications(): Пакетная отправка уведомлений
- test_connection(): Проверка подключения

Параметры, которые принимаются извне:
- Параметры подключения (токен, чат ID и т.д.)
- Сырое сообщение для отправки
- Готовый логгер
- Шаблоны сообщений (опционально)
- Дополнительные параметры форматирования (опционально)
"""

from .interface import send_notification, send_batch_notifications, test_connection
from .implementations.telegram_notifier import TelegramNotifier
from .configs.base_notifier_config import BASE_NOTIFIER_CONFIG
from .configs.implementations.telegram_notifier_config import TELEGRAM_NOTIFIER_CONFIG

__all__ = [
    'send_notification',
    'send_batch_notifications',
    'test_connection',
    'TelegramNotifier',
    'BASE_NOTIFIER_CONFIG',
    'TELEGRAM_NOTIFIER_CONFIG'
]