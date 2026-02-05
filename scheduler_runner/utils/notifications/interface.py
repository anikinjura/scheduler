"""
Интерфейс для изолированного микросервиса уведомлений

Архитектура:
- Простой интерфейс для взаимодействия с микросервисом уведомлений
- Принимает все необходимые параметры извне
- Возвращает результат отправки уведомления
"""
__version__ = '1.0.0'

from typing import Dict, Any, Union, Optional
from .implementations.telegram_notifier import TelegramNotifier


def send_notification(
    message: Union[str, Dict[str, Any]],
    connection_params: Dict[str, str],
    logger=None,
    templates: Optional[Dict[str, str]] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Отправка уведомления через изолированный микросервис
    
    Args:
        message: Сообщение для отправки (строка или словарь с данными)
        connection_params: Параметры подключения (токен, чат ID и т.д.)
        logger: Объект логгера (если не передан, будет использован внутренний)
        templates: Шаблоны сообщений (если необходимы)
        **kwargs: Дополнительные параметры
    
    Returns:
        Dict с результатами отправки уведомления
    """
    # Формируем конфигурацию из переданных параметров
    config = {
        **connection_params,  # Параметры подключения (токен, чат ID и т.д.)
    }
    
    # Добавляем шаблоны, если они переданы
    if templates:
        config["MESSAGE_TEMPLATES"] = templates
    
    # Добавляем другие параметры из kwargs
    config.update(kwargs)
    
    # Создаем экземпляр уведомителя
    notifier = TelegramNotifier(config=config, logger=logger)
    
    # Подключаемся к системе уведомлений
    if notifier.connect():
        # Отправляем уведомление
        result = notifier.send_notification(message, templates=templates)
        
        # Отключаемся от системы уведомлений
        notifier.disconnect()
        
        return result
    else:
        return {"success": False, "error": "Не удалось подключиться к системе уведомлений"}


def send_batch_notifications(
    messages: list,
    connection_params: Dict[str, str],
    logger=None,
    templates: Optional[Dict[str, str]] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Пакетная отправка уведомлений через изолированный микросервис
    
    Args:
        messages: Список сообщений для отправки
        connection_params: Параметры подключения (токен, чат ID и т.д.)
        logger: Объект логгера (если не передан, будет использован внутренний)
        templates: Шаблоны сообщений (если необходимы)
        **kwargs: Дополнительные параметры
    
    Returns:
        Dict с результатами пакетной отправки уведомлений
    """
    # Формируем конфигурацию из переданных параметров
    config = {
        **connection_params,  # Параметры подключения (токен, чат ID и т.д.)
    }
    
    # Добавляем шаблоны, если они переданы
    if templates:
        config["MESSAGE_TEMPLATES"] = templates
    
    # Добавляем другие параметры из kwargs
    config.update(kwargs)
    
    # Создаем экземпляр уведомителя
    notifier = TelegramNotifier(config=config, logger=logger)
    
    # Подключаемся к системе уведомлений
    if notifier.connect():
        # Отправляем уведомления пакетно
        result = notifier.send_batch_notifications(messages, templates=templates)
        
        # Отключаемся от системы уведомлений
        notifier.disconnect()
        
        return result
    else:
        return {"success": False, "error": "Не удалось подключиться к системе уведомлений"}


def test_connection(connection_params: Dict[str, str], logger=None) -> Dict[str, Any]:
    """
    Проверка подключения к системе уведомлений
    
    Args:
        connection_params: Параметры подключения (токен, чат ID и т.д.)
        logger: Объект логгера (если не передан, будет использован внутренний)
    
    Returns:
        Dict с результатами проверки подключения
    """
    # Формируем конфигурацию из переданных параметров
    config = {
        **connection_params,  # Параметры подключения (токен, чат ID и т.д.)
    }
    
    # Создаем экземпляр уведомителя
    notifier = TelegramNotifier(config=config, logger=logger)
    
    # Проверяем подключение
    connection_result = notifier.connect()
    
    # Отключаемся от системы уведомлений
    notifier.disconnect()
    
    return {
        "success": connection_result,
        "connection_params_valid": all(
            param in connection_params 
            for param in config.get("REQUIRED_CONNECTION_PARAMS", [])
        )
    }