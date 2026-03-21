"""
Реализация отправки уведомлений через VK API.
"""
__version__ = "1.0.0"

import random
import time
from typing import Any, Dict, Union

import requests

from ..configs.implementations.vk_notifier_config import VK_NOTIFIER_CONFIG
from ..core.base_message_sender import BaseMessageSender
from ..core.base_notifier import BaseNotifier


class VkNotifier(BaseNotifier, BaseMessageSender):
    """
    Реализация отправки уведомлений через VK API.

    Production send-path не делает обязательный preflight-запрос.
    Локальная проверка token/peer_id выполняется в connect(),
    а сеть трогается только при diagnostic `test_connection()` или реальной отправке.
    """

    API_URL = "https://api.vk.com/method"

    def __init__(self, config: Dict[str, Any], logger=None):
        merged_config = {**VK_NOTIFIER_CONFIG, **(config or {})}
        BaseNotifier.__init__(self, merged_config, logger)
        BaseMessageSender.__init__(self, merged_config, logger)

        self.token = self.config.get("VK_ACCESS_TOKEN")
        self.peer_id = self.config.get("VK_PEER_ID")
        self.api_version = str(self.config.get("VK_API_VERSION") or "5.199")
        self.connect_timeout_seconds = int(self.config.get("CONNECT_TIMEOUT_SECONDS", 15) or 15)
        self.send_timeout_seconds = int(self.config.get("SEND_TIMEOUT_SECONDS", 20) or 20)
        self.send_retry_attempts = max(1, int(self.config.get("SEND_RETRY_ATTEMPTS", 3) or 3))
        self.send_retry_backoff_seconds = max(0, int(self.config.get("SEND_RETRY_BACKOFF_SECONDS", 3) or 3))

    def _vk_method(self, method: str, timeout: int, **params: Any) -> dict:
        response = requests.post(
            f"{self.API_URL}/{method}",
            data={
                **params,
                "access_token": self.token,
                "v": self.api_version,
            },
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()

    def _normalize_vk_error(self, payload: dict) -> dict:
        error = payload.get("error", {})
        return {
            "error_code": error.get("error_code"),
            "error_msg": error.get("error_msg"),
            "request_params": error.get("request_params"),
        }

    def _internal_send_vk_message(self, message: str) -> tuple[bool, dict]:
        last_error = {"error": "Unknown error"}
        retryable_exceptions = (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        )
        retryable_vk_codes = {6, 9, 10}

        for attempt in range(1, self.send_retry_attempts + 1):
            try:
                payload = self._vk_method(
                    "messages.send",
                    timeout=self.send_timeout_seconds,
                    peer_id=self.peer_id,
                    random_id=int(time.time() * 1000) + random.randint(0, 999),
                    message=message,
                )
                if "response" in payload:
                    return True, payload

                normalized_error = self._normalize_vk_error(payload)
                last_error = normalized_error

                if self.logger:
                    self.logger.error(
                        "VK API вернул ошибку на попытке %d/%d: %s",
                        attempt,
                        self.send_retry_attempts,
                        normalized_error,
                    )

                if normalized_error.get("error_code") not in retryable_vk_codes:
                    return False, normalized_error
            except retryable_exceptions as exc:
                last_error = {"error": str(exc)}
                if self.logger:
                    self.logger.error(
                        "Retryable ошибка при отправке VK-сообщения на попытке %d/%d: %s",
                        attempt,
                        self.send_retry_attempts,
                        exc,
                        exc_info=True,
                    )
            except requests.exceptions.RequestException as exc:
                if self.logger:
                    self.logger.error(f"Ошибка запроса при отправке VK-сообщения: {exc}", exc_info=True)
                return False, {"error": str(exc)}
            except Exception as exc:
                if self.logger:
                    self.logger.error(f"Неизвестная ошибка при отправке VK-сообщения: {exc}", exc_info=True)
                return False, {"error": str(exc)}

            if attempt < self.send_retry_attempts and self.send_retry_backoff_seconds > 0:
                time.sleep(self.send_retry_backoff_seconds * attempt)

        return False, last_error

    def connect(self) -> bool:
        if self.logger:
            self.logger.trace("Попали в метод VkNotifier.connect")
            self.logger.debug("Начало локальной валидации параметров VK")

        try:
            if not self.token or not self.peer_id:
                if self.logger:
                    self.logger.error("Отсутствуют необходимые параметры подключения: VK_ACCESS_TOKEN или VK_PEER_ID")
                return False

            self.connected = True
            if self.logger:
                self.logger.info("Локальная валидация параметров VK пройдена")
            return True
        except Exception as exc:
            if self.logger:
                self.logger.error(f"Ошибка при подключении к VK API: {exc}", exc_info=True)
            return False

    def disconnect(self) -> bool:
        if self.logger:
            self.logger.trace("Попали в метод VkNotifier.disconnect")

        try:
            self.connected = False
            if self.logger:
                self.logger.info("Отключение от VK API выполнено")
            return True
        except Exception as exc:
            if self.logger:
                self.logger.error(f"Ошибка при отключении от VK API: {exc}", exc_info=True)
            return False

    def _send_message(self, message: Union[str, Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        if self.logger:
            self.logger.trace("Попали в метод VkNotifier._send_message")

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

            success, result = self._internal_send_vk_message(text)

            if success:
                if self.logger:
                    self.logger.info(f"Сообщение успешно отправлено в VK: {result}")
                return {
                    "success": True,
                    "result": result,
                    "message_id": result.get("response"),
                }

            if self.logger:
                self.logger.error(f"Ошибка при отправке сообщения в VK: {result}")
            return {"success": False, "error": result, "details": result}
        except Exception as exc:
            if self.logger:
                self.logger.error(f"Ошибка при отправке сообщения в VK: {exc}", exc_info=True)
            return {"success": False, "error": str(exc)}

    def _perform_send(self, message: Union[str, Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        return self._send_message(message, **kwargs)

    def _validate_connection_params(self) -> bool:
        return BaseMessageSender._validate_connection_params(self)

    def _establish_connection(self) -> bool:
        if self.logger:
            self.logger.trace("Попали в метод VkNotifier._establish_connection")

        try:
            payload = self._vk_method("messages.getConversations", timeout=self.connect_timeout_seconds, count=1)
            if "response" in payload:
                if self.logger:
                    self.logger.debug("VK community token валиден")
                return True

            normalized_error = self._normalize_vk_error(payload)
            if self.logger:
                self.logger.error(f"VK connection check failed: {normalized_error}")
            return False
        except Exception as exc:
            if self.logger:
                self.logger.error(f"Ошибка при проверке VK community token: {exc}", exc_info=True)
            return False

    def _close_connection(self) -> bool:
        return True
