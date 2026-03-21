"""
Конфигурация для отправки уведомлений через VK.
"""
__version__ = "1.0.0"

from typing import Any, Dict

from ..base_notifier_config import BASE_NOTIFIER_CONFIG


VK_NOTIFIER_CONFIG: Dict[str, Any] = {
    **BASE_NOTIFIER_CONFIG,
    "MAX_MESSAGE_LENGTH": 4096,
    "REQUIRED_CONNECTION_PARAMS": ["VK_ACCESS_TOKEN", "VK_PEER_ID"],
    "CONNECT_TIMEOUT_SECONDS": 15,
    "SEND_TIMEOUT_SECONDS": 20,
    "SEND_RETRY_ATTEMPTS": 3,
    "SEND_RETRY_BACKOFF_SECONDS": 3,
    "VK_API_VERSION": "5.199",
}
