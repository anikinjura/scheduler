"""
Конфигурация для отправки уведомлений через Telegram

Архитектура:
- Конкретная конфигурация для Telegram
- Минимальный набор параметров, не зависящих от конкретной реализации
- Все специфичные параметры передаются извне
"""
__version__ = '1.0.0'

from typing import Dict, Any
from ..base_notifier_config import BASE_NOTIFIER_CONFIG

# Конфигурация для Telegram
TELEGRAM_NOTIFIER_CONFIG: Dict[str, Any] = {
    # Наследуем базовые параметры
    **BASE_NOTIFIER_CONFIG,

    # Определяем минимальные специфичные параметры
    "MAX_MESSAGE_LENGTH": 4096,  # Ограничение Telegram API
    "REQUIRED_CONNECTION_PARAMS": ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"],  # Обязательные параметры подключения
}