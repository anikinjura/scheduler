"""
Модуль реализаций для микросервиса уведомлений.

Содержит конкретные реализации отправки уведомлений.
"""

from .telegram_notifier import TelegramNotifier
from .vk_notifier import VkNotifier

__all__ = [
    "TelegramNotifier",
    "VkNotifier",
]
