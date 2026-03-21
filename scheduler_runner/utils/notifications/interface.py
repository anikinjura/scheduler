"""
Интерфейс для изолированного микросервиса уведомлений.
"""
__version__ = "1.0.0"

from typing import Any, Dict, Optional, Union

from .implementations.telegram_notifier import TelegramNotifier
from .implementations.vk_notifier import VkNotifier


def _resolve_notifier(config: Dict[str, Any], logger=None):
    provider = (config.get("NOTIFICATION_PROVIDER") or "telegram").lower()
    if provider == "telegram":
        return TelegramNotifier(config=config, logger=logger)
    if provider == "vk":
        return VkNotifier(config=config, logger=logger)
    raise ValueError(f"Неподдерживаемый notification provider: {provider}")


def send_notification(
    message: Union[str, Dict[str, Any]],
    connection_params: Dict[str, str],
    logger=None,
    templates: Optional[Dict[str, str]] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Отправка уведомления через изолированный микросервис.
    """
    config = {
        **connection_params,
    }

    if templates:
        config["MESSAGE_TEMPLATES"] = templates

    config.update(kwargs)

    notifier = _resolve_notifier(config=config, logger=logger)

    if notifier.connect():
        result = notifier.send_notification(message, templates=templates)
        notifier.disconnect()
        return result
    return {"success": False, "error": "Не удалось подключиться к системе уведомлений"}


def send_batch_notifications(
    messages: list,
    connection_params: Dict[str, str],
    logger=None,
    templates: Optional[Dict[str, str]] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Пакетная отправка уведомлений через изолированный микросервис.
    """
    config = {
        **connection_params,
    }

    if templates:
        config["MESSAGE_TEMPLATES"] = templates

    config.update(kwargs)

    notifier = _resolve_notifier(config=config, logger=logger)

    if notifier.connect():
        result = notifier.send_batch_notifications(messages, templates=templates)
        notifier.disconnect()
        return result
    return {"success": False, "error": "Не удалось подключиться к системе уведомлений"}


def test_connection(connection_params: Dict[str, str], logger=None) -> Dict[str, Any]:
    """
    Проверка подключения к системе уведомлений.
    """
    config = {
        **connection_params,
    }

    notifier = _resolve_notifier(config=config, logger=logger)
    connection_params_valid = notifier._validate_connection_params()
    connection_result = connection_params_valid and notifier._establish_connection()
    notifier.disconnect()

    return {
        "success": connection_result,
        "connection_params_valid": connection_params_valid,
    }
