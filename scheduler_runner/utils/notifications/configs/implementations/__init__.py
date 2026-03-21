"""
Модуль конфигураций реализаций для микросервиса уведомлений.

Содержит конкретные конфигурации для различных способов отправки уведомлений.
"""

from .telegram_notifier_config import TELEGRAM_NOTIFIER_CONFIG
from .vk_notifier_config import VK_NOTIFIER_CONFIG

__all__ = [
    "TELEGRAM_NOTIFIER_CONFIG",
    "VK_NOTIFIER_CONFIG",
]
