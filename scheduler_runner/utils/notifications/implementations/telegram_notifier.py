"""
Реализация отправки уведомлений через Telegram.
"""
__version__ = "1.0.0"

import time
from typing import Any, Dict, Union

import requests

from ..configs.implementations.telegram_notifier_config import TELEGRAM_NOTIFIER_CONFIG
from ..core.base_message_sender import BaseMessageSender
from ..core.base_notifier import BaseNotifier


class TelegramNotifier(BaseNotifier, BaseMessageSender):
    """
    Реализация отправки уведомлений через Telegram API.

    Боевой send-path не делает обязательный preflight-запрос к Telegram.
    Проверка token/chat_id выполняется локально в connect(), а сеть трогается
    только при диагностическом test_connection() или реальной отправке.
    """

    def __init__(self, config: Dict[str, Any], logger=None):
        merged_config = {**TELEGRAM_NOTIFIER_CONFIG, **(config or {})}
        BaseNotifier.__init__(self, merged_config, logger)
        BaseMessageSender.__init__(self, merged_config, logger)

        self.token = self.config.get("TELEGRAM_BOT_TOKEN") or self.config.get("TOKEN")
        self.chat_id = self.config.get("TELEGRAM_CHAT_ID") or self.config.get("CHAT_ID")
        self.connect_timeout_seconds = int(self.config.get("CONNECT_TIMEOUT_SECONDS", 15) or 15)
        self.send_timeout_seconds = int(self.config.get("SEND_TIMEOUT_SECONDS", 20) or 20)
        self.send_retry_attempts = max(1, int(self.config.get("SEND_RETRY_ATTEMPTS", 3) or 3))
        self.send_retry_backoff_seconds = max(0, int(self.config.get("SEND_RETRY_BACKOFF_SECONDS", 3) or 3))

    def _internal_send_telegram_message(self, token: str, chat_id: str, message: str) -> tuple[bool, dict]:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
        }

        last_error = {"error": "Unknown error"}
        retryable_exceptions = (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        )

        for attempt in range(1, self.send_retry_attempts + 1):
            try:
                response = requests.post(url, json=payload, timeout=self.send_timeout_seconds)
                if response.status_code == 200:
                    return True, response.json()

                try:
                    response_payload = response.json()
                except ValueError:
                    response_payload = {"error": response.text.strip()}

                if self.logger:
                    self.logger.error(
                        "Telegram API вернул код %d на попытке %d/%d: %s",
                        response.status_code,
                        attempt,
                        self.send_retry_attempts,
                        response.text.strip(),
                    )

                if response.status_code < 500 and response.status_code != 429:
                    return False, response_payload
                last_error = response_payload
            except retryable_exceptions as exc:
                last_error = {"error": str(exc)}
                if self.logger:
                    self.logger.error(
                        "Retryable ошибка при отправке Telegram-сообщения на попытке %d/%d: %s",
                        attempt,
                        self.send_retry_attempts,
                        exc,
                        exc_info=True,
                    )
            except requests.exceptions.RequestException as exc:
                if self.logger:
                    self.logger.error(f"Ошибка запроса при отправке Telegram-сообщения: {exc}", exc_info=True)
                return False, {"error": str(exc)}
            except Exception as exc:
                if self.logger:
                    self.logger.error(f"Неизвестная ошибка при отправке Telegram-сообщения: {exc}", exc_info=True)
                return False, {"error": str(exc)}

            if attempt < self.send_retry_attempts and self.send_retry_backoff_seconds > 0:
                time.sleep(self.send_retry_backoff_seconds * attempt)

        return False, last_error

    def connect(self) -> bool:
        if self.logger:
            self.logger.trace("Попали в метод TelegramNotifier.connect")
            self.logger.debug("Начало локальной валидации параметров Telegram")

        try:
            if not self.token or not self.chat_id:
                if self.logger:
                    self.logger.error("Отсутствуют необходимые параметры подключения: TOKEN или CHAT_ID")
                return False

            self.connected = True
            if self.logger:
                self.logger.info("Локальная валидация параметров Telegram пройдена")
            return True
        except Exception as exc:
            if self.logger:
                self.logger.error(f"Ошибка при подключении к Telegram API: {exc}", exc_info=True)
            return False

    def disconnect(self) -> bool:
        if self.logger:
            self.logger.trace("Попали в метод TelegramNotifier.disconnect")

        try:
            self.connected = False
            if self.logger:
                self.logger.info("Отключение от Telegram API выполнено")
            return True
        except Exception as exc:
            if self.logger:
                self.logger.error(f"Ошибка при отключении от Telegram API: {exc}", exc_info=True)
            return False

    def _send_message(self, message: Union[str, Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        if self.logger:
            self.logger.trace("Попали в метод TelegramNotifier._send_message")

        try:
            if isinstance(message, dict):
                template_name = message.get("template")
                if template_name:
                    templates = self.config.get("MESSAGE_TEMPLATES", {})
                    template = templates.get(template_name)
                    if template and isinstance(template, str):
                        data = message.get("data", message)
                        if isinstance(data, dict):
                            try:
                                text = template.format(**data)
                            except KeyError:
                                text = str(message)
                        else:
                            try:
                                text = template.format(**message)
                            except KeyError:
                                text = str(message)
                    else:
                        text = str(message)
                else:
                    message_text = message.get("text", message.get("message"))
                    text = str(message_text) if message_text else str(message)
            else:
                text = str(message)

            max_length = self.config.get("MAX_MESSAGE_LENGTH", 4096)
            if len(text) > max_length:
                text = text[: max_length - 3] + "..."
                if self.logger:
                    self.logger.warning(
                        "Сообщение было обрезано до допустимой длины: %d символов",
                        max_length,
                    )

            success, result = self._internal_send_telegram_message(
                token=self.token,
                chat_id=self.chat_id,
                message=text,
            )

            if success:
                if self.logger:
                    self.logger.info(f"Сообщение успешно отправлено в Telegram: {result}")
                return {
                    "success": True,
                    "result": result,
                    "message_id": result.get("result", {}).get("message_id"),
                }

            if self.logger:
                self.logger.error(f"Ошибка при отправке сообщения в Telegram: {result}")
            return {"success": False, "error": result, "details": result}
        except Exception as exc:
            if self.logger:
                self.logger.error(f"Ошибка при отправке сообщения в Telegram: {exc}", exc_info=True)
            return {"success": False, "error": str(exc)}

    def _perform_send(self, message: Union[str, Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        return self._send_message(message, **kwargs)

    def send_notification_with_media(self, message: str, media: Dict[str, Any] = None, **kwargs) -> Dict[str, Any]:
        return self._send_message(message, **kwargs)

    def _validate_connection_params(self) -> bool:
        return BaseMessageSender._validate_connection_params(self)

    def _establish_connection(self) -> bool:
        if self.logger:
            self.logger.trace("Попали в метод TelegramNotifier._establish_connection")

        try:
            url = f"https://api.telegram.org/bot{self.token}/getMe"
            response = requests.get(url, timeout=self.connect_timeout_seconds)

            if response.status_code == 200 and response.json().get("ok"):
                if self.logger:
                    self.logger.debug("Токен Telegram бота валиден")
                return True

            if self.logger:
                self.logger.error(f"Токен Telegram бота недействителен: {response.text}")
            return False
        except Exception as exc:
            if self.logger:
                self.logger.error(f"Ошибка при проверке токена Telegram бота: {exc}", exc_info=True)
            return False

    def _close_connection(self) -> bool:
        return True
