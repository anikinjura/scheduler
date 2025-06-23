"""
notify.py

Утилиты для отправки уведомлений (Telegram и др.) для задач и ядра планировщика.

Функции:
    - send_telegram_message(token, chat_id, message, logger=None) -> tuple[bool, dict]
        Отправляет сообщение через Telegram API.

Author: anikinjura
"""
__version__ = '0.0.1'

import requests
import logging
from typing import Optional

def send_telegram_message(token: str, chat_id: str, message: str, logger: Optional[logging.Logger] = None) -> tuple[bool, dict]:
    """
    Отправляет сообщение через Telegram API.

    Args:
        token (str): Токен бота Telegram.
        chat_id (str): ID чата для отправки сообщения.
        message (str): Текст сообщения.
        logger (logging.Logger, optional): Логгер для записи информации.

    Returns:
        tuple: (success: bool, result: dict)
    """
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': message
    }
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code != 200:
            if logger:
                logger.error("Telegram API вернул код %d: %s", response.status_code, response.text.strip())
            return False, response.json()
        return True, response.json()
    except requests.exceptions.ConnectionError as ce:
        if logger:
            logger.error(f"Ошибка соединения при отправке Telegram-сообщения: {ce}", exc_info=True)
        return False, {"error": "Connection error"}
    except requests.exceptions.Timeout as te:
        if logger:
            logger.error(f"Таймаут при отправке Telegram-сообщения: {te}", exc_info=True)
        return False, {"error": "Timeout error"}
    except requests.exceptions.RequestException as re:
        if logger:
            logger.error(f"Ошибка запроса при отправке Telegram-сообщения: {re}", exc_info=True)
        return False, {"error": "Request error"}
    except Exception as e:
        if logger:
            logger.error(f"Неизвестная ошибка при отправке Telegram-сообщения: {e}", exc_info=True)
        return False, {"error": "Unknown error"}