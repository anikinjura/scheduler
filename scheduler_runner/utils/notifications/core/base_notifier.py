"""
Базовый класс для отправки уведомлений

Архитектура:
- Универсальный класс для отправки уведомлений различными способами
- Поддержка расширения для других способов отправки в будущем
- Гибкая система конфигурации через словарь
- Поддержка различных типов уведомлений (текст, изображение, документ)
- Расширяемая архитектура для специфичных типов отправителей
- Разделение на публичные и внутренние (с префиксом _) методы
- Поддержка настраиваемых параметров через конфигурацию
- Поддержка передачи внешнего логгера из доменных скриптов
- Обработка ошибок и повторные попытки отправки
"""
__version__ = '1.0.0'

import time
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Union
from datetime import datetime


class BaseNotifier(ABC):
    """Базовый класс для отправки уведомлений различными способами"""

    def __init__(self, config: Dict[str, Any], logger=None):
        """
        Инициализация базового отправителя уведомлений

        Args:
            config: Конфигурационный словарь с параметрами для работы отправителя
            logger: Объект логгера (если не передан, будет использован внутренний логгер из config)

        Поддерживаемые параметры конфигурации:
            - USER: Пользователь, от имени которого выполняется задача (по умолчанию 'system')
            - TASK_NAME: Имя задачи для логирования (по умолчанию 'BaseNotifier')
            - DETAILED_LOGS: Флаг детализированного логирования (по умолчанию False)
            - MAX_RETRIES: Максимальное количество попыток отправки (по умолчанию 3)
            - DELAY_BETWEEN_RETRIES: Задержка между попытками отправки (по умолчанию 1.0 секунда)
            - MESSAGE_TEMPLATES: Шаблоны сообщений для различных типов уведомлений
        """
        # Сохраняем config до установки логгера
        self.config = config

        # Устанавливаем логгер: если передан извне, используем его, иначе создаем внутренний
        if logger is not None:
            self.logger = logger
        elif 'logger' in config and config['logger'] is not None:
            self.logger = config['logger']
        else:
            # Если внутренний логгер не может быть создан (микросервис изолирован),
            # используем None и проверяем наличие логгера перед каждым вызовом
            try:
                from scheduler_runner.utils.logging import configure_logger
                self.logger = configure_logger(
                    user=self.config.get("USER", "system"),
                    task_name=self.config.get("TASK_NAME", "BaseNotifier"),
                    detailed=self.config.get("DETAILED_LOGS", False)
                )
            except ImportError:
                # Если не удается импортировать configure_logger, используем None
                self.logger = None
            except Exception as e:
                print(f"Ошибка при создании логгера в BaseNotifier: {e}")
                self.logger = None

        if self.logger:
            self.logger.trace("Попали в метод BaseNotifier.__init__")

        # Инициализируем счетчики
        self.sent_count = 0
        self.failed_count = 0
        self.last_send_time = None

    # === АБСТРАКТНЫЕ МЕТОДЫ (обязательны для реализации в дочерних классах) ===

    @abstractmethod
    def connect(self) -> bool:
        """
        Метод для подключения к системе отправки уведомлений

        Returns:
            bool: True, если подключение прошло успешно
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseNotifier.connect")
        pass

    @abstractmethod
    def disconnect(self) -> bool:
        """
        Метод для отключения от системы отправки уведомлений

        Returns:
            bool: True, если отключение прошло успешно
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseNotifier.disconnect")
        pass

    @abstractmethod
    def _send_message(self, message: Union[str, Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        """
        Метод для отправки сообщения (реализуется в дочерних классах)

        Args:
            message: Сообщение для отправки (строка или словарь с данными)
            **kwargs: Дополнительные параметры

        Returns:
            Dict с результатами отправки
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseNotifier._send_message")
        pass

    # === ПУБЛИЧНЫЕ МЕТОДЫ ===

    def send_notification(self, message: Union[str, Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        """
        Отправка уведомления с обработкой ошибок и повторными попытками

        Args:
            message: Сообщение для отправки (строка или словарь с данными)
            **kwargs: Дополнительные параметры

        Returns:
            Dict с результатами отправки
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseNotifier.send_notification")

        try:
            # Если переданы шаблоны в kwargs, обновляем их в конфигурации
            if 'templates' in kwargs and kwargs['templates']:
                # Создаем копию конфига, чтобы не изменять оригинал
                temp_config = self.config.copy()
                temp_config['MESSAGE_TEMPLATES'] = kwargs['templates']
                # Временно заменяем конфиг для этой операции
                original_config = self.config
                self.config = temp_config

            # Валидация сообщения
            validation_result = self.validate_message(message)
            if not validation_result["success"]:
                if self.logger:
                    self.logger.error(f"Сообщение не прошло валидацию: {validation_result['error']}")
                # Восстанавливаем оригинальный конфиг
                if 'templates' in kwargs and kwargs['templates']:
                    self.config = original_config
                return validation_result

            # Выполняем отправку с повторными попытками
            result = self.retry_operation(
                lambda: self._send_message(message, **kwargs),
                max_retries=self.config.get("MAX_RETRIES", 3),
                delay=self.config.get("DELAY_BETWEEN_RETRIES", 1.0)
            )

            # Восстанавливаем оригинальный конфиг, если он был временно изменен
            if 'templates' in kwargs and kwargs['templates']:
                self.config = original_config

            # Обновляем статистику
            if result["success"]:
                self.sent_count += 1
                if self.logger:
                    self.logger.debug(f"Счетчик отправленных уведомлений обновлен: {self.sent_count}")
            else:
                self.failed_count += 1
                if self.logger:
                    self.logger.debug(f"Счетчик неудачных отправок обновлен: {self.failed_count}")

            self.last_send_time = datetime.now()

            if self.logger:
                self.logger.info(f"Отправка уведомления завершена: {result}")

            return result

        except Exception as e:
            # Восстанавливаем оригинальный конфиг в случае ошибки
            if 'templates' in kwargs and kwargs['templates']:
                self.config = original_config
            if self.logger:
                self.logger.error(f"Ошибка при отправке уведомления: {e}", exc_info=True)
            return {"success": False, "error": str(e)}

    def send_batch_notifications(self, messages: List[Union[str, Dict[str, Any]]], **kwargs) -> Dict[str, Any]:
        """
        Пакетная отправка уведомлений

        Args:
            messages: Список сообщений для отправки
            **kwargs: Дополнительные параметры

        Returns:
            Dict с результатами пакетной отправки
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseNotifier.send_batch_notifications")
        
        if not messages:
            if self.logger:
                self.logger.warning("Список сообщений для отправки пуст")
            return {"success": True, "sent": 0, "failed": 0}

        results = {
            "success": True,
            "sent": 0,
            "failed": 0,
            "details": []
        }

        for i, message in enumerate(messages):
            try:
                # Выполняем отправку
                result = self.send_notification(message, **kwargs)

                if result["success"]:
                    results["sent"] += 1
                else:
                    results["failed"] += 1

                results["details"].append({
                    "index": i,
                    "message_sample": str(message)[:100],  # Обрезаем для логирования
                    "result": result
                })

                # Задержка между отправками для предотвращения ограничений API
                if i < len(messages) - 1:  # Не задерживаемся после последнего элемента
                    delay = self.config.get("DELAY_BETWEEN_MESSAGES", 0.1)
                    time.sleep(delay)

            except Exception as e:
                if self.logger:
                    self.logger.error(f"Ошибка при отправке сообщения {i}: {e}")
                results["failed"] += 1
                results["details"].append({
                    "index": i,
                    "message_sample": str(message)[:100],
                    "result": {"success": False, "error": str(e)}
                })

        return results

    # === ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ===

    def validate_message(self, message: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Валидация сообщения перед отправкой

        Args:
            message: Сообщение для валидации

        Returns:
            Dict с результатами валидации
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseNotifier.validate_message")
        
        # Проверяем, что сообщение не пустое
        if not message:
            if self.logger:
                self.logger.debug("Сообщение пустое")
            return {"success": False, "error": "Сообщение пустое"}

        # Проверяем, что сообщение имеет правильный формат
        if not isinstance(message, (str, dict)):
            if self.logger:
                self.logger.debug(f"Неправильный формат сообщения: {type(message)}")
            return {"success": False, "error": "Сообщение должно быть строкой или словарем"}

        # Если это словарь, проверяем наличие обязательных полей
        if isinstance(message, dict):
            required_fields = self.config.get("REQUIRED_MESSAGE_FIELDS", [])
            if self.logger:
                self.logger.debug(f"Требуемые поля в сообщении: {required_fields}")

            missing_fields = []
            for field in required_fields:
                if field not in message:
                    missing_fields.append(field)

            if missing_fields:
                if self.logger:
                    self.logger.debug(f"Отсутствуют обязательные поля в сообщении: {missing_fields}")
                return {"success": False, "error": f"Отсутствуют обязательные поля в сообщении: {missing_fields}"}

        if self.logger:
            self.logger.debug("Сообщение прошло валидацию успешно")

        return {"success": True}

    def format_message(self, message: Union[str, Dict[str, Any]], message_type: str = "text") -> Union[str, Dict[str, Any]]:
        """
        Форматирование сообщения по шаблону

        Args:
            message: Сообщение для форматирования
            message_type: Тип сообщения (text, html, markdown и т.д.)

        Returns:
            Отформатированное сообщение
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseNotifier.format_message")

        # Если сообщение - словарь и содержит шаблон, используем его
        if isinstance(message, dict) and "template" in message:
            template_name = message["template"]
            templates = self.config.get("MESSAGE_TEMPLATES", {})
            template = templates.get(template_name)

            if template and isinstance(template, str):
                try:
                    # Подставляем данные из сообщения в шаблон
                    data = message.get("data", message)  # Если есть вложенный словарь data, используем его
                    if isinstance(data, dict):
                        formatted_message = template.format(**data)
                        if self.logger:
                            self.logger.debug(f"Сообщение отформатировано по шаблону: {template_name}")
                        return formatted_message
                    else:
                        # Если data не является словарем, используем само сообщение
                        formatted_message = template.format(**message)
                        if self.logger:
                            self.logger.debug(f"Сообщение отформатировано по шаблону: {template_name}")
                        return formatted_message
                except KeyError as e:
                    if self.logger:
                        self.logger.warning(f"Отсутствует ключ в шаблоне сообщения: {e}")
                    # Возвращаем оригинальное сообщение без форматирования
                    return message

        # Получаем шаблон для данного типа сообщения (для обратной совместимости)
        templates = self.config.get("MESSAGE_TEMPLATES", {})
        template = templates.get(message_type)

        if template and isinstance(message, dict):
            # Применяем шаблон к словарю данных
            try:
                if isinstance(template, str):
                    # Если шаблон - строка, применяем форматирование
                    formatted_message = template.format(**message)
                    if self.logger:
                        self.logger.debug(f"Сообщение отформатировано по шаблону: {message_type}")
                    return formatted_message
                else:
                    # Если шаблон - словарь, объединяем с данными
                    result = template.copy()
                    result.update(message)
                    if self.logger:
                        self.logger.debug(f"Сообщение отформатировано по шаблону: {message_type}")
                    return result
            except KeyError as e:
                if self.logger:
                    self.logger.warning(f"Отсутствует ключ в шаблоне сообщения: {e}")
                # Возвращаем оригинальное сообщение без форматирования
                return message
        else:
            # Если нет шаблона или сообщение - строка, возвращаем как есть
            if self.logger:
                self.logger.debug("Форматирование сообщения не требуется")
            return message

    def get_status(self) -> Dict[str, Any]:
        """
        Получение статуса отправителя уведомлений

        Returns:
            Dict с информацией о состоянии отправителя
        """
        return {
            "sent_count": self.sent_count,
            "failed_count": self.failed_count,
            "last_send_time": self.last_send_time.isoformat() if self.last_send_time else None,
            "config_summary": {
                "max_retries": self.config.get("MAX_RETRIES"),
                "delay_between_retries": self.config.get("DELAY_BETWEEN_RETRIES")
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
                    if self.logger:
                        self.logger.warning(f"Попытка {attempt + 1} из {max_retries + 1} не удалась: {e}. Повтор через {delay} секунд...")
                    time.sleep(delay)
                else:
                    if self.logger:
                        self.logger.error(f"Все попытки выполнения операции не удалась: {e}")
                    raise e