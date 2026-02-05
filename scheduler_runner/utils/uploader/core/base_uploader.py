"""
Базовый класс для загрузки данных в различные системы

Архитектура:
- Универсальный класс для работы с различными системами загрузки данных
- Поддержка расширения для других систем в будущем
- Гибкая система конфигурации через словарь
- Поддержка различных методов аутентификации
- Расширяемая архитектура для специфичных типов загрузчиков
- Разделение на публичные и внутренние (с префиксом _) методы
- Использование традиционных имен методов connect/disconnect для аутентификации
- Поддержка настраиваемых параметров через конфигурацию
- Поддержка использования существующих учетных данных

Изменения в версии 1.0.0:
- Создан базовый класс BaseUploader
- Добавлены методы подключения и отключения
- Добавлены методы загрузки данных
- Добавлена система конфигурации
"""

import os
import time
import json
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime

from scheduler_runner.utils.logging import configure_logger


class BaseUploader:
    """
    Базовый класс для загрузки данных в различные системы.
    
    Этот класс предоставляет основу для всех загрузчиков данных,
    реализуя общую функциональность подключения, аутентификации,
    обработки данных и загрузки.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None, logger=None):
        """
        Инициализация загрузчика с конфигурацией

        Args:
            config: Конфигурация загрузчика
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
                self.logger = configure_logger(
                    user=self.config.get("USER", "system"),
                    task_name=self.config.get("TASK_NAME", "BaseUploader"),
                    detailed=self.config.get("DETAILED_LOGS", False)
                )
            except ImportError:
                # Если не удается импортировать configure_logger, используем None
                self.logger = None

        if self.logger:
            self.logger.trace("Попали в метод BaseUploader.__init__")
            self.logger.debug(f"Инициализация BaseUploader с конфигурацией: {list(self.config.keys()) if self.config else 'пустая конфигурация'}")

        # Инициализируем состояние подключения
        self.connected = False
        self.connection_handle = None

        # Инициализируем счетчики
        self.uploaded_count = 0
        self.failed_count = 0
        self.last_upload_time = None

    def connect(self) -> bool:
        """
        Подключение к целевой системе

        Returns:
            bool: True если подключение успешно
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseUploader.connect")
            self.logger.debug("Начало процесса подключения к целевой системе")

        try:
            self.logger.info("Попытка подключения к целевой системе...")

            # Проверяем наличие необходимых параметров
            if not self._validate_connection_params():
                self.logger.error("Отсутствуют необходимые параметры подключения")
                return False

            # Выполняем подключение (реализуется в дочерних классах)
            connection_result = self._establish_connection()

            if connection_result:
                self.connected = True
                self.logger.info("Подключение к целевой системе установлено")
                if self.logger:
                    self.logger.debug(f"Состояние подключения обновлено: {self.connected}")
                return True
            else:
                self.logger.error("Не удалось установить подключение к целевой системе")
                return False

        except Exception as e:
            self.logger.error(f"Ошибка при подключении: {e}")
            return False

    def disconnect(self) -> bool:
        """
        Отключение от целевой системы

        Returns:
            bool: True если отключение успешно
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseUploader.disconnect")
            self.logger.debug("Начало процесса отключения от целевой системы")

        try:
            if not self.connected:
                self.logger.warning("Попытка отключения от несуществующего подключения")
                return True

            # Выполняем отключение (реализуется в дочерних классах)
            disconnection_result = self._close_connection()
            
            if disconnection_result:
                self.connected = False
                self.connection_handle = None
                self.logger.info("Отключение от целевой системы выполнено")
                return True
            else:
                self.logger.error("Не удалось выполнить отключение от целевой системы")
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
            self.logger.trace("Попали в метод BaseUploader._validate_connection_params")

        # Проверяем, что все необходимые параметры подключения присутствуют
        required_params = self.config.get("REQUIRED_CONNECTION_PARAMS", [])

        for param in required_params:
            if param not in self.config or not self.config[param]:
                if self.logger:
                    self.logger.error(f"Отсутствует необходимый параметр подключения: {param}")
                return False

        return True

    def _establish_connection(self) -> bool:
        """
        Установление подключения к целевой системе
        
        Returns:
            bool: True если подключение установлено
        """
        # Должен быть реализован в дочерних классах
        raise NotImplementedError("Метод _establish_connection должен быть реализован в дочернем классе")

    def _close_connection(self) -> bool:
        """
        Закрытие подключения к целевой системе
        
        Returns:
            bool: True если подключение закрыто
        """
        # Должен быть реализован в дочерних классах
        raise NotImplementedError("Метод _close_connection должен быть реализован в дочернем классе")

    def upload_data(self, data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """
        Загрузка данных в целевую систему

        Args:
            data: Данные для загрузки
            **kwargs: Дополнительные параметры

        Returns:
            Dict с результатами загрузки
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseUploader.upload_data")
            self.logger.debug(f"Попытка загрузки данных: {list(data.keys()) if isinstance(data, dict) else type(data)}")
            self.logger.debug(f"Параметры: {kwargs}")

        if not self.connected:
            self.logger.error("Нет подключения к целевой системе")
            return {"success": False, "error": "Нет подключения к целевой системе"}

        try:
            self.logger.info("Начало загрузки данных...")

            # Валидация данных
            validation_result = self._validate_data(data)
            if not validation_result["success"]:
                self.logger.error(f"Данные не прошли валидацию: {validation_result['error']}")
                if self.logger:
                    self.logger.debug(f"Данные, не прошедшие валидацию: {data}")
                return validation_result

            # Трансформация данных (если требуется и не пропущена)
            skip_transformation = kwargs.get('skip_transformation', False)
            if skip_transformation:
                self.logger.debug("Пропускаем трансформацию данных (указан флаг skip_transformation)")
                transformed_data = data
            else:
                transformed_data = self._transform_data_if_needed(data)
                if self.logger:
                    self.logger.debug(f"Данные после трансформации: {transformed_data}")

            # Выполнение загрузки
            upload_result = self._perform_upload(transformed_data, **kwargs)
            if self.logger:
                self.logger.debug(f"Результат выполнения загрузки: {upload_result}")

            # Обновление статистики
            if upload_result["success"]:
                self.uploaded_count += 1
                if self.logger:
                    self.logger.debug(f"Счетчик загруженных данных обновлен: {self.uploaded_count}")
            else:
                self.failed_count += 1
                if self.logger:
                    self.logger.debug(f"Счетчик неудачных загрузок обновлен: {self.failed_count}")

            self.last_upload_time = datetime.now()

            self.logger.info(f"Загрузка завершена: {upload_result}")
            return upload_result

        except Exception as e:
            self.logger.error(f"Ошибка при загрузке данных: {e}")
            if self.logger:
                import traceback
                self.logger.debug(f"Полный стек трейса ошибки: {traceback.format_exc()}")
            return {"success": False, "error": str(e)}

    def _validate_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Валидация данных перед загрузкой

        Args:
            data: Данные для валидации

        Returns:
            Dict с результатами валидации
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseUploader._validate_data")
            self.logger.debug(f"Валидация данных: {data}")
            self.logger.debug(f"Конфигурация: {self.config}")

        # Проверяем, что данные не пусты
        if not data:
            if self.logger:
                self.logger.debug("Данные пусты")
            return {"success": False, "error": "Данные пусты"}

        # Проверяем, что данные имеют правильный формат
        if not isinstance(data, dict):
            if self.logger:
                self.logger.debug(f"Неправильный формат данных: {type(data)}")
            return {"success": False, "error": "Данные должны быть в формате словаря"}

        # Проверяем обязательные поля из конфигурации
        required_fields = self.config.get("REQUIRED_FIELDS", [])
        if self.logger:
            self.logger.debug(f"Требуемые поля: {required_fields}")

        missing_fields = []

        for field in required_fields:
            if field not in data:
                missing_fields.append(field)

        if missing_fields:
            if self.logger:
                self.logger.debug(f"Отсутствуют обязательные поля: {missing_fields}")
            return {"success": False, "error": f"Отсутствуют обязательные поля: {missing_fields}"}

        if self.logger:
            self.logger.debug("Данные прошли валидацию успешно")

        return {"success": True}

    def _transform_data_if_needed(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Трансформация данных при необходимости

        Args:
            data: Исходные данные

        Returns:
            Трансформированные данные
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseUploader._transform_data_if_needed")
            self.logger.debug(f"Попытка трансформации данных: {list(data.keys()) if isinstance(data, dict) else type(data)}")

        # Проверяем, нужна ли трансформация
        transformer_class_name = self.config.get("TRANSFORMER_CLASS")

        if transformer_class_name:
            try:
                # Используем трансформер из внешней системы (если передан)
                # В изолированном микросервисе трансформация может быть выполнена внешними средствами
                if self.logger:
                    self.logger.warning(f"Трансформер {transformer_class_name} указан, но трансформация должна выполняться внешними средствами в изолированном микросервисе")
                return data
            except Exception as e:
                self.logger.error(f"Ошибка при трансформации данных: {e}")
                return data
        else:
            # Если трансформер не указан, возвращаем данные как есть
            if self.logger:
                self.logger.debug("Трансформация не требуется, возвращаем данные как есть")
            return data

    def _perform_upload(self, data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """
        Выполнение загрузки данных
        
        Args:
            data: Данные для загрузки
            **kwargs: Дополнительные параметры
            
        Returns:
            Dict с результатами загрузки
        """
        # Должен быть реализован в дочерних классах
        raise NotImplementedError("Метод _perform_upload должен быть реализован в дочернем классе")

    def batch_upload(self, data_list: List[Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        """
        Пакетная загрузка данных
        
        Args:
            data_list: Список данных для загрузки
            **kwargs: Дополнительные параметры
            
        Returns:
            Dict с результатами пакетной загрузки
        """
        if not self.connected:
            self.logger.error("Нет подключения к целевой системе")
            return {"success": False, "error": "Нет подключения к целевой системе"}
        
        if not data_list:
            self.logger.warning("Список данных для загрузки пуст")
            return {"success": True, "uploaded": 0, "failed": 0}
        
        results = {
            "success": True,
            "uploaded": 0,
            "failed": 0,
            "details": []
        }
        
        batch_size = self.config.get("BATCH_SIZE", 100)
        
        for i, data in enumerate(data_list):
            try:
                # Выполняем загрузку с учетом размера пакета
                result = self.upload_data(data, **kwargs)
                
                if result["success"]:
                    results["uploaded"] += 1
                else:
                    results["failed"] += 1
                
                results["details"].append({
                    "index": i,
                    "data_sample": str(data)[:100],  # Обрезаем для логирования
                    "result": result
                })
                
                # Задержка между запросами для предотвращения ограничений API
                if i < len(data_list) - 1:  # Не задерживаемся после последнего элемента
                    delay = self.config.get("DELAY_BETWEEN_REQUESTS", 0.1)
                    time.sleep(delay)
                    
            except Exception as e:
                self.logger.error(f"Ошибка при загрузке элемента {i}: {e}")
                results["failed"] += 1
                results["details"].append({
                    "index": i,
                    "data_sample": str(data)[:100],
                    "result": {"success": False, "error": str(e)}
                })
        
        return results

    def get_status(self) -> Dict[str, Any]:
        """
        Получение статуса загрузчика
        
        Returns:
            Dict с информацией о состоянии загрузчика
        """
        return {
            "connected": self.connected,
            "uploaded_count": self.uploaded_count,
            "failed_count": self.failed_count,
            "last_upload_time": self.last_upload_time.isoformat() if self.last_upload_time else None,
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
                return result
            except Exception as e:
                if attempt < max_retries:
                    self.logger.warning(f"Попытка {attempt + 1} из {max_retries + 1} не удалась: {e}. Повтор через {delay} секунд...")
                    time.sleep(delay)
                else:
                    self.logger.error(f"Все попытки выполнения операции не удалась: {e}")
                    raise e