"""
Реализация отправки уведомлений через Telegram

Архитектура:
- Наследование от базовых классов BaseNotifier и BaseMessageSender
- Использование существующей функции send_telegram_message из scheduler_runner.utils.notify
- Поддержка переменных среды: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
- Поддержка форматирования сообщений
- Обработка специфичных для Telegram ошибок
"""
__version__ = '1.0.0'

import requests
from typing import Dict, Any, Union
from ..core.base_notifier import BaseNotifier
from ..core.base_message_sender import BaseMessageSender


class TelegramNotifier(BaseNotifier, BaseMessageSender):
    """
    Реализация отправки уведомлений через Telegram.

    Этот класс реализует отправку уведомлений через Telegram API,
    с внутренней реализацией отправки сообщений.
    """

    def __init__(self, config: Dict[str, Any], logger=None):
        """
        Инициализация Telegram отправителя уведомлений

        Args:
            config: Конфигурация Telegram отправителя
            logger: Объект логгера (если не передан, будет создан внутренний логгер)
        """
        # Вызываем инициализацию обоих базовых классов
        BaseNotifier.__init__(self, config, logger)
        BaseMessageSender.__init__(self, config, logger)

        if self.logger:
            self.logger.trace("Попали в метод TelegramNotifier.__init__")
            self.logger.debug(f"Инициализация TelegramNotifier с конфигурацией: {list(self.config.keys()) if self.config else 'пустая конфигурация'}")

        # Инициализируем специфичные для Telegram параметры
        # Все специфичные параметры (токен, чат ID и т.д.) передаются через конфигурацию
        self.token = self.config.get("TELEGRAM_BOT_TOKEN") or self.config.get("TOKEN")
        self.chat_id = self.config.get("TELEGRAM_CHAT_ID") or self.config.get("CHAT_ID")

    def _internal_send_telegram_message(self, token: str, chat_id: str, message: str) -> tuple[bool, dict]:
        """
        Внутренняя реализация отправки сообщения через Telegram API.

        Args:
            token (str): Токен бота Telegram.
            chat_id (str): ID чата для отправки сообщения.
            message (str): Текст сообщения.

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
                if self.logger:
                    self.logger.error("Telegram API вернул код %d: %s", response.status_code, response.text.strip())
                return False, response.json()
            return True, response.json()
        except requests.exceptions.ConnectionError as ce:
            if self.logger:
                self.logger.error(f"Ошибка соединения при отправке Telegram-сообщения: {ce}", exc_info=True)
            return False, {"error": "Connection error"}
        except requests.exceptions.Timeout as te:
            if self.logger:
                self.logger.error(f"Таймаут при отправке Telegram-сообщения: {te}", exc_info=True)
            return False, {"error": "Timeout error"}
        except requests.exceptions.RequestException as re:
            if self.logger:
                self.logger.error(f"Ошибка запроса при отправке Telegram-сообщения: {re}", exc_info=True)
            return False, {"error": "Request error"}
        except Exception as e:
            if self.logger:
                self.logger.error(f"Неизвестная ошибка при отправке Telegram-сообщения: {e}", exc_info=True)
            return False, {"error": "Unknown error"}

    def connect(self) -> bool:
        """
        Подключение к Telegram API (проверка валидности токена)

        Returns:
            bool: True если подключение успешно
        """
        if self.logger:
            self.logger.trace("Попали в метод TelegramNotifier.connect")
            self.logger.debug("Начало процесса подключения к Telegram API")

        try:
            self.logger.info("Попытка подключения к Telegram API...")

            # Проверяем наличие необходимых параметров
            if not self.token or not self.chat_id:
                self.logger.error("Отсутствуют необходимые параметры подключения: TOKEN или CHAT_ID")
                return False

            # Для Telegram API подключение означает проверку валидности токена
            # Используем внутренний метод проверки подключения
            connection_result = self._establish_connection()

            if connection_result:
                self.connected = True
                self.logger.info("Подключение к Telegram API установлено")
                if self.logger:
                    self.logger.debug(f"Состояние подключения обновлено: {self.connected}")
                return True
            else:
                self.logger.error("Не удалось установить подключение к Telegram API")
                return False

        except Exception as e:
            self.logger.error(f"Ошибка при подключении к Telegram API: {e}")
            return False

    def disconnect(self) -> bool:
        """
        Отключение от Telegram API

        Returns:
            bool: True если отключение успешно
        """
        if self.logger:
            self.logger.trace("Попали в метод TelegramNotifier.disconnect")
            self.logger.debug("Начало процесса отключения от Telegram API")

        try:
            if not self.connected:
                self.logger.warning("Попытка отключения от несуществующего подключения")
                return True

            # Для Telegram API отключение просто сбрасывает состояние подключения
            self.connected = False
            self.logger.info("Отключение от Telegram API выполнено")
            return True

        except Exception as e:
            self.logger.error(f"Ошибка при отключении от Telegram API: {e}")
            return False

    def _send_message(self, message: Union[str, Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        """
        Отправка сообщения через Telegram API

        Args:
            message: Сообщение для отправки (строка или словарь с данными)
            **kwargs: Дополнительные параметры

        Returns:
            Dict с результатами отправки
        """
        if self.logger:
            self.logger.trace("Попали в метод TelegramNotifier._send_message")
            self.logger.debug(f"Попытка отправки сообщения в Telegram: {type(message)}")
            self.logger.debug(f"Параметры: {kwargs}")

        try:
            # Подготовка текста сообщения
            if isinstance(message, dict):
                # Если сообщение - словарь, проверяем наличие шаблона
                template_name = message.get("template")

                if template_name:
                    # Если шаблон указан, применяем его к данным
                    templates = self.config.get("MESSAGE_TEMPLATES", {})
                    template = templates.get(template_name)

                    if template and isinstance(template, str):
                        # Получаем данные для подстановки в шаблон
                        data = message.get("data", message)  # Если есть вложенный словарь data, используем его

                        if isinstance(data, dict):
                            try:
                                text = template.format(**data)
                            except KeyError as e:
                                if self.logger:
                                    self.logger.warning(f"Отсутствует ключ в шаблоне: {e}, используем оригинальное сообщение")
                                # Если форматирование не удалось, используем строковое представление
                                text = str(message)
                        else:
                            # Если data не является словарем, используем его как есть
                            try:
                                text = template.format(**message)
                            except KeyError as e:
                                if self.logger:
                                    self.logger.warning(f"Отсутствует ключ в шаблоне: {e}, используем оригинальное сообщение")
                                text = str(message)
                    else:
                        # Если шаблон не найден, используем строковое представление сообщения
                        text = str(message)
                else:
                    # Если шаблон не указан, извлекаем текст напрямую
                    message_text = message.get("text", message.get("message"))
                    text = str(message_text) if message_text else str(message)
            else:
                # Если сообщение - строка, используем как есть
                text = str(message)

            # Проверяем длину сообщения
            max_length = self.config.get("MAX_MESSAGE_LENGTH", 4096)
            if len(text) > max_length:
                # Обрезаем сообщение до допустимой длины
                text = text[:max_length - 3] + "..."
                if self.logger:
                    self.logger.warning(f"Сообщение было обрезано до допустимой длины: {max_length} символов")

            # Отправляем сообщение через внутреннюю реализацию
            success, result = self._internal_send_telegram_message(
                token=self.token,
                chat_id=self.chat_id,
                message=text
            )

            if success:
                self.logger.info(f"Сообщение успешно отправлено в Telegram: {result}")
                return {"success": True, "result": result, "message_id": result.get("result", {}).get("message_id")}
            else:
                self.logger.error(f"Ошибка при отправке сообщения в Telegram: {result}")
                return {"success": False, "error": result, "details": result}

        except Exception as e:
            self.logger.error(f"Ошибка при отправке сообщения в Telegram: {e}")
            return {"success": False, "error": str(e)}

    def _perform_send(self, message: Union[str, Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        """
        Выполнение отправки сообщения через Telegram API

        Args:
            message: Сообщение для отправки
            **kwargs: Дополнительные параметры

        Returns:
            Dict с результатами отправки
        """
        if self.logger:
            self.logger.trace("Попали в метод TelegramNotifier._perform_send")
            self.logger.debug(f"Выполнение отправки сообщения в Telegram: {type(message)}")
            self.logger.debug(f"Параметры: {kwargs}")

        # Используем тот же метод, что и _send_message, так как реализация одинакова
        return self._send_message(message, **kwargs)

    def send_notification_with_media(self, message: str, media: Dict[str, Any] = None, **kwargs) -> Dict[str, Any]:
        """
        Отправка уведомления с медиа-контентом (изображения, документы и т.д.)

        Args:
            message: Текст сообщения
            media: Словарь с информацией о медиа-контенте
            **kwargs: Дополнительные параметры

        Returns:
            Dict с результатами отправки
        """
        if self.logger:
            self.logger.trace("Попали в метод TelegramNotifier.send_notification_with_media")
            self.logger.debug(f"Попытка отправки уведомления с медиа-контентом в Telegram")
            self.logger.debug(f"Медиа-контент: {media}")

        try:
            # Пока что реализуем только текстовые сообщения
            # В будущем можно добавить поддержку изображений и документов
            return self._send_message(message, **kwargs)

        except Exception as e:
            self.logger.error(f"Ошибка при отправке уведомления с медиа-контентом в Telegram: {e}")
            return {"success": False, "error": str(e)}

    def _validate_connection_params(self) -> bool:
        """
        Валидация параметров подключения к Telegram

        Returns:
            bool: True если параметры валидны
        """
        if self.logger:
            self.logger.trace("Попали в метод TelegramNotifier._validate_connection_params")

        # Проверяем, что все необходимые параметры подключения присутствуют
        # Используем базовую валидацию из BaseMessageSender
        return BaseMessageSender._validate_connection_params(self)

    def _establish_connection(self) -> bool:
        """
        Установление подключения к Telegram API

        Returns:
            bool: True если подключение установлено
        """
        if self.logger:
            self.logger.trace("Попали в метод TelegramNotifier._establish_connection")
        
        # Проверяем валидность токена через API
        try:
            # Отправляем тестовый запрос для проверки токена
            import requests
            url = f"https://api.telegram.org/bot{self.token}/getMe"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200 and response.json().get("ok"):
                if self.logger:
                    self.logger.debug("Токен Telegram бота валиден")
                return True
            else:
                if self.logger:
                    self.logger.error(f"Токен Telegram бота недействителен: {response.text}")
                return False
        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при проверке токена Telegram бота: {e}")
            return False

    def _close_connection(self) -> bool:
        """
        Закрытие подключения к Telegram API

        Returns:
            bool: True если подключение закрыто
        """
        if self.logger:
            self.logger.trace("Попали в метод TelegramNotifier._close_connection")
        
        # Для Telegram API закрытие подключения просто сбрасывает состояние
        return True