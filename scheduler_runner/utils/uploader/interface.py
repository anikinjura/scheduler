"""
Интерфейс для изолированного микросервиса загрузчика

Архитектура:
- Простой интерфейс для взаимодействия с микросервисом загрузчика
- Принимает все необходимые параметры извне
- Возвращает результат загрузки данных
"""
__version__ = '1.0.0'

from typing import Dict, Any, List, Optional
from .implementations.google_sheets_uploader import GoogleSheetsUploader


def upload_data(
    data: Dict[str, Any],
    connection_params: Dict[str, Any],
    logger=None,
    **kwargs
) -> Dict[str, Any]:
    """
    Загрузка данных через изолированный микросервис

    Args:
        data: Данные для загрузки
        connection_params: Параметры подключения (путь к учетным данным, ID таблицы и т.д.)
        logger: Объект логгера (если не передан, будет использован внутренний)
        **kwargs: Дополнительные параметры

    Returns:
        Dict с результатами загрузки данных
    """
    # Формируем конфигурацию из переданных параметров
    config = {
        **connection_params,  # Параметры подключения
    }

    # Добавляем другие параметры из kwargs
    config.update(kwargs)

    # Создаем экземпляр загрузчика
    uploader = GoogleSheetsUploader(config=config, logger=logger)

    # Подключаемся к системе загрузки
    if uploader.connect():
        # Загружаем данные
        result = uploader.upload_data(data)

        # Отключаемся от системы загрузки
        uploader.disconnect()

        return result
    else:
        return {"success": False, "error": "Не удалось подключиться к системе загрузки"}


def upload_batch_data(
    data_list: List[Dict[str, Any]],
    connection_params: Dict[str, Any],
    logger=None,
    **kwargs
) -> Dict[str, Any]:
    """
    Пакетная загрузка данных через изолированный микросервис

    Args:
        data_list: Список данных для загрузки
        connection_params: Параметры подключения (путь к учетным данным, ID таблицы и т.д.)
        logger: Объект логгера (если не передан, будет использован внутренний)
        **kwargs: Дополнительные параметры

    Returns:
        Dict с результатами пакетной загрузки данных
    """
    # Формируем конфигурацию из переданных параметров
    config = {
        **connection_params,  # Параметры подключения
    }

    # Добавляем другие параметры из kwargs
    config.update(kwargs)

    # Создаем экземпляр загрузчика
    uploader = GoogleSheetsUploader(config=config, logger=logger)

    # Подключаемся к системе загрузки
    if uploader.connect():
        # Загружаем данные пакетно
        result = uploader.batch_upload(data_list)

        # Отключаемся от системы загрузки
        uploader.disconnect()

        return result
    else:
        return {"success": False, "error": "Не удалось подключиться к системе загрузки"}


def test_connection(connection_params: Dict[str, Any], logger=None) -> Dict[str, Any]:
    """
    Проверка подключения к системе загрузки

    Args:
        connection_params: Параметры подключения (путь к учетным данным, ID таблицы и т.д.)
        logger: Объект логгера (если не передан, будет использован внутренний)

    Returns:
        Dict с результатами проверки подключения
    """
    # Формируем конфигурацию из переданных параметров
    config = {
        **connection_params,  # Параметры подключения
    }

    # Создаем экземпляр загрузчика
    uploader = GoogleSheetsUploader(config=config, logger=logger)

    # Проверяем подключение
    connection_result = uploader.connect()

    # Отключаемся от системы загрузки
    uploader.disconnect()

    return {
        "success": connection_result,
        "connection_params_valid": all(
            param in connection_params
            for param in config.get("REQUIRED_CONNECTION_PARAMS", [])
        )
    }