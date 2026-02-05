"""
Базовый класс для конкретных отправителей уведомлений

Архитектура:
- Универсальный класс для работы с конкретными API систем отправки уведомлений
- Поддержка расширения для других систем в будущем
- Гибкая система конфигурации через словарь
- Общие методы для работы с API
- Валидация конфигурации
- Обработка ответов от API
- Поддержка передачи внешнего логгера из доменных скриптов
"""
__version__ = '1.0.0'

import time
from abc import ABC
from typing import Dict, Any, Optional, Union
from datetime import datetime


class BaseMessageSender(ABC):
    """
    Базовый класс для конкретных отправителей уведомлений.

    Этот класс предоставляет основу для всех конкретных отправителей уведомлений,
    реализуя общую функциональность подключения, аутентификации,
    обработки сообщений и отправки.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None, logger=None):
        """
        Инициализация отправителя уведомлений с конфигурацией

        Args:
            config: Конфигурация отправителя
            logger: Объект логгера (если не передан, будет создан внутренний логгер)
        """
        # Устанавливаем конфигурацию
        self.config = config or {}

        # Устанавливаем логгер: если передан извне, используем его, иначе создаем внутренний
        if logger is not None:
            self.logger = logger
        elif 'logger' in self.config and self.config['logger'] is not None:
            self.logger = self.config['logger']
        else:
            # Если внутренний логгер не может быть создан (микросервис изолирован),
            # используем None и проверяем наличие логгера перед каждым вызовом
            try:
                from scheduler_runner.utils.logging import configure_logger
                self.logger = configure_logger(
                    user=self.config.get("USER", "system"),
                    task_name=self.config.get("TASK_NAME", "BaseMessageSender"),
                    detailed=self.config.get("DETAILED_LOGS", False)
                )
            except ImportError:
                # Если не удается импортировать configure_logger, используем None
                self.logger = None

        if self.logger:
            self.logger.trace("Попали в метод BaseMessageSender.__init__")
            self.logger.debug(f"Инициализация BaseMessageSender с конфигурацией: {list(self.config.keys()) if self.config else 'пустая конфигурация'}")

        # Инициализируем состояние подключения
        self.connected = False
        self.connection_handle = None

        # Инициализируем счетчики
        self.sent_count = 0
        self.failed_count = 0
        self.last_send_time = None

    def connect(self) -> bool:
        """
        Подключение к системе отправки уведомлений

        Returns:
            bool: True если подключение успешно
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseMessageSender.connect")
            self.logger.debug("Начало процесса подключения к системе отправки уведомлений")

        try:
            self.logger.info("Попытка подключения к системе отправки уведомлений...")

            # Проверяем наличие необходимых параметров
            if not self._validate_connection_params():
                self.logger.error("Отсутствуют необходимые параметры подключения")
                return False

            # Выполняем подключение
            connection_result = self._establish_connection()

            if connection_result:
                self.connected = True
                self.logger.info("Подключение к системе отправки уведомлений установлено")
                if self.logger:
                    self.logger.debug(f"Состояние подключения обновлено: {self.connected}")
                return True
            else:
                self.logger.error("Не удалось установить подключение к системе отправки уведомлений")
                return False

        except Exception as e:
            self.logger.error(f"Ошибка при подключении: {e}")
            return False

    def disconnect(self) -> bool:
        """
        Отключение от системы отправки уведомлений

        Returns:
            bool: True если отключение успешно
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseMessageSender.disconnect")
            self.logger.debug("Начало процесса отключения от системы отправки уведомлений")

        try:
            if not self.connected:
                self.logger.warning("Попытка отключения от несуществующего подключения")
                return True

            # Выполняем отключение
            disconnection_result = self._close_connection()

            if disconnection_result:
                self.connected = False
                self.connection_handle = None
                self.logger.info("Отключение от системы отправки уведомлений выполнено")
                return True
            else:
                self.logger.error("Не удалось выполнить отключение от системы отправки уведомлений")
                return False

        except Exception as e:
            self.logger.error(f"Ошибка при отключении: {e}")
            return False

    def _validate_connection_params(self) -> bool:
        """
        Валидация параметров подключения

        Returns:
            bool: True если параметры валидны
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseMessageSender._validate_connection_params")

        # Проверяем, что все необходимые параметры подключения присутствуют
        required_params = self.config.get("REQUIRED_CONNECTION_PARAMS", [])

        for param in required_params:
            if param not in self.config or not self.config[param]:
                if self.logger:
                    self.logger.error(f"Отсутствует необходимый параметр подключения: {param}")
                return False

        # Также проверяем, что токен и чат ID установлены (для Telegram)
        # Это делается для совместимости с текущей реализацией
        if hasattr(self, 'token') and hasattr(self, 'chat_id'):
            if not getattr(self, 'token', None):
                if self.logger:
                    self.logger.error("Отсутствует токен для подключения")
                return False

            if not getattr(self, 'chat_id', None):
                if self.logger:
                    self.logger.error("Отсутствует чат ID для подключения")
                return False

        return True

    def _establish_connection(self) -> bool:
        """
        Установление подключения к системе отправки уведомлений

        Returns:
            bool: True если подключение установлено
        """
        # Должен быть реализован в дочерних классах
        raise NotImplementedError("Метод _establish_connection должен быть реализован в дочернем классе")

    def _close_connection(self) -> bool:
        """
        Закрытие подключения к системе отправки уведомлений

        Returns:
            bool: True если подключение закрыто
        """
        # Должен быть реализован в дочерних классах
        raise NotImplementedError("Метод _close_connection должен быть реализован в дочернем классе")

    def send_message(self, message: Union[str, Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        """
        Отправка сообщения в систему уведомлений

        Args:
            message: Сообщение для отправки
            **kwargs: Дополнительные параметры

        Returns:
            Dict с результатами отправки
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseMessageSender.send_message")
            self.logger.debug(f"Попытка отправки сообщения: {type(message)}")
            self.logger.debug(f"Параметры: {kwargs}")

        if not self.connected:
            self.logger.error("Нет подключения к системе отправки уведомлений")
            return {"success": False, "error": "Нет подключения к системе отправки уведомлений"}

        try:
            self.logger.info("Начало отправки сообщения...")

            # Валидация сообщения
            validation_result = self._validate_message(message)
            if not validation_result["success"]:
                self.logger.error(f"Сообщение не прошло валидацию: {validation_result['error']}")
                if self.logger:
                    self.logger.debug(f"Сообщение, не прошедшее валидацию: {message}")
                return validation_result

            # Выполнение отправки
            send_result = self._perform_send(message, **kwargs)
            if self.logger:
                self.logger.debug(f"Результат выполнения отправки: {send_result}")

            # Обновление статистики
            if send_result["success"]:
                self.sent_count += 1
                if self.logger:
                    self.logger.debug(f"Счетчик отправленных сообщений обновлен: {self.sent_count}")
            else:
                self.failed_count += 1
                if self.logger:
                    self.logger.debug(f"Счетчик неудачных отправок обновлен: {self.failed_count}")

            self.last_send_time = datetime.now()

            self.logger.info(f"Отправка завершена: {send_result}")
            return send_result

        except Exception as e:
            self.logger.error(f"Ошибка при отправке сообщения: {e}")
            if self.logger:
                import traceback
                self.logger.debug(f"Полный стек трейса ошибки: {traceback.format_exc()}")
            return {"success": False, "error": str(e)}

    def _validate_message(self, message: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Валидация сообщения перед отправкой

        Args:
            message: Сообщение для валидации

        Returns:
            Dict с результатами валидации
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseMessageSender._validate_message")
            self.logger.debug(f"Валидация сообщения: {type(message)}")
            self.logger.debug(f"Конфигурация: {self.config}")

        # Проверяем, что сообщение не пусто
        if not message:
            if self.logger:
                self.logger.debug("Сообщение пусто")
            return {"success": False, "error": "Сообщение пусто"}

        # Проверяем, что сообщение имеет правильный формат
        if not isinstance(message, (str, dict)):
            if self.logger:
                self.logger.debug(f"Неправильный формат сообщения: {type(message)}")
            return {"success": False, "error": "Сообщение должно быть строкой или словарем"}

        # Проверяем максимальный размер сообщения
        max_length = self.config.get("MAX_MESSAGE_LENGTH", 4096)  # по умолчанию для Telegram
        if isinstance(message, str) and len(message) > max_length:
            if self.logger:
                self.logger.debug(f"Сообщение превышает максимальную длину: {len(message)} > {max_length}")
            return {"success": False, "error": f"Сообщение превышает максимальную длину: {max_length} символов"}

        # Проверяем максимальный размер для словаря (если применимо)
        if isinstance(message, dict):
            import json
            try:
                message_str = json.dumps(message)
                if len(message_str) > max_length:
                    if self.logger:
                        self.logger.debug(f"Сообщение в формате JSON превышает максимальную длину: {len(message_str)} > {max_length}")
                    return {"success": False, "error": f"Сообщение в формате JSON превышает максимальную длину: {max_length} символов"}
            except TypeError:
                if self.logger:
                    self.logger.debug("Невозможно сериализовать сообщение в JSON для проверки длины")
                return {"success": False, "error": "Невозможно сериализовать сообщение в JSON"}

        # Проверяем обязательные поля из конфигурации
        required_fields = self.config.get("REQUIRED_MESSAGE_FIELDS", [])
        if self.logger:
            self.logger.debug(f"Требуемые поля в сообщении: {required_fields}")

        if isinstance(message, dict):
            missing_fields = []

            for field in required_fields:
                if field not in message:
                    missing_fields.append(field)

            if missing_fields:
                if self.logger:
                    self.logger.debug(f"Отсутствуют обязательные поля в сообщении: {missing_fields}")
                return {"success": False, "error": f"Отсутствуют обязательные поля в сообщении: {missing_fields}"}

        # Если сообщение - словарь и содержит шаблон, проверяем, что шаблон существует
        if isinstance(message, dict) and "template" in message:
            template_name = message["template"]
            templates = self.config.get("MESSAGE_TEMPLATES", {})
            if template_name not in templates:
                if self.logger:
                    self.logger.debug(f"Шаблон '{template_name}' не найден в доступных шаблонах")
                # Не считаем это критической ошибкой, просто предупреждение
                if self.logger:
                    self.logger.warning(f"Шаблон '{template_name}' не найден, будет использовано сырое сообщение")

        if self.logger:
            self.logger.debug("Сообщение прошло валидацию успешно")

        return {"success": True}

    def _perform_send(self, message: Union[str, Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        """
        Выполнение отправки сообщения

        Args:
            message: Сообщение для отправки
            **kwargs: Дополнительные параметры

        Returns:
            Dict с результатами отправки
        """
        # Должен быть реализован в дочерних классах
        raise NotImplementedError("Метод _perform_send должен быть реализован в дочернем классе")

    def batch_send(self, messages: list, **kwargs) -> Dict[str, Any]:
        """
        Пакетная отправка сообщений

        Args:
            messages: Список сообщений для отправки
            **kwargs: Дополнительные параметры

        Returns:
            Dict с результатами пакетной отправки
        """
        if not self.connected:
            self.logger.error("Нет подключения к системе отправки уведомлений")
            return {"success": False, "error": "Нет подключения к системе отправки уведомлений"}

        if not messages:
            self.logger.warning("Список сообщений для отправки пуст")
            return {"success": True, "sent": 0, "failed": 0}

        results = {
            "success": True,
            "sent": 0,
            "failed": 0,
            "details": []
        }

        batch_size = self.config.get("BATCH_SIZE", 100)

        for i, message in enumerate(messages):
            try:
                # Выполняем отправку с учетом размера пакета
                result = self.send_message(message, **kwargs)

                if result["success"]:
                    results["sent"] += 1
                else:
                    results["failed"] += 1

                results["details"].append({
                    "index": i,
                    "message_sample": str(message)[:100],  # Обрезаем для логирования
                    "result": result
                })

                # Задержка между запросами для предотвращения ограничений API
                if i < len(messages) - 1:  # Не задерживаемся после последнего элемента
                    delay = self.config.get("DELAY_BETWEEN_REQUESTS", 0.1)
                    time.sleep(delay)

            except Exception as e:
                self.logger.error(f"Ошибка при отправке сообщения {i}: {e}")
                results["failed"] += 1
                results["details"].append({
                    "index": i,
                    "message_sample": str(message)[:100],
                    "result": {"success": False, "error": str(e)}
                })

        return results

    def get_status(self) -> Dict[str, Any]:
        """
        Получение статуса отправителя уведомлений

        Returns:
            Dict с информацией о состоянии отправителя
        """
        return {
            "connected": self.connected,
            "sent_count": self.sent_count,
            "failed_count": self.failed_count,
            "last_send_time": self.last_send_time.isoformat() if self.last_send_time else None,
            "config_summary": {
                "target_system": self.config.get("TARGET_SYSTEM"),
                "batch_size": self.config.get("BATCH_SIZE")
            }
        }

    def retry_operation(self, operation_func, max_retries: int = None, delay: float = None) -> Any:
        """
        Выполнение операции с повторными попытками

        Args:
            operation_func: Функция для выполнения
            max_retries: Максимальное количество попыток
            delay: Задержка между попытками

        Returns:
            Результат выполнения операции
        """
        if max_retries is None:
            max_retries = self.config.get("MAX_RETRIES", 3)

        if delay is None:
            delay = self.config.get("DELAY_BETWEEN_RETRIES", 1.0)

        for attempt in range(max_retries + 1):
            try:
                result = operation_func()
                if self.logger and attempt > 0:
                    self.logger.info(f"Операция выполнена успешно после {attempt} попыток")
                return result
            except Exception as e:
                if attempt < max_retries:
                    self.logger.warning(f"Попытка {attempt + 1} из {max_retries + 1} не удалась: {e}. Повтор через {delay} секунд...")
                    time.sleep(delay)
                else:
                    self.logger.error(f"Все попытки выполнения операции не удалась: {e}")
                    raise e