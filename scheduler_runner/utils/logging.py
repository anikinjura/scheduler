"""
logging.py

Модуль для настройки и управления логированием задач и ядра планировщика.

Возможности:
    - Создание структуры логов logs/{user}/{task_name}/YYYY-MM-DD.log
    - Ротация лог-файлов по размеру и количеству backup-файлов
    - Поддержка отдельного detailed-лога (DEBUG) при detailed=True
    - Автоматическая очистка старых логов (старше backup_count дней)
    - Кеширование логгеров для предотвращения дублирования хендлеров

Основные функции:
    - configure_logger(user, task_name=None, detailed=False, ...): возвращает сконфигурированный Logger
    - _cleanup_old_logs(log_path, days_to_keep): удаляет устаревшие логи

Пример использования:
    from scheduler_runner.utils.logging import configure_logger
    logger = configure_logger(user="operator", task_name="CopyScript", detailed=True)
    logger.info("Задача успешно выполнена")

Author: anikinjura
"""
__version__ = '0.0.1'

import logging
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, Optional

# Глобальный кеш логгеров для предотвращения дублирования хендлеров
_LOGGERS: Dict[str, logging.Logger] = {}


def configure_logger(
    user: str, 
    task_name: Optional[str] = None, 
    detailed: bool = False,
    logs_dir: str = "logs", 
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5
) -> logging.Logger:
    """
    Конфигурирует и возвращает логгер для заданного пользователя и задачи.
    
    Создает структуру папок logs/{user}/{task_name}/ и настраивает ротацию файлов.
    Основной лог-файл содержит сообщения уровня INFO и выше.
    При включенном detailed режиме создается дополнительный файл с DEBUG сообщениями.
    
    :param user: имя системного пользователя
    :param task_name: опциональный идентификатор задачи
    :param detailed: если True, добавляется отдельный DEBUG-лог файл
    :param logs_dir: базовая директория для лог-файлов
    :param max_bytes: размер файла для ротации в байтах
    :param backup_count: количество backup-файлов для хранения
    :return: сконфигурированный экземпляр Logger
    """
    # Формируем уникальное имя логгера
    logger_name = user if not task_name else f"{user}.{task_name}"
    
    # Возвращаем существующий логгер из кеша
    if logger_name in _LOGGERS:
        return _LOGGERS[logger_name]

    # Создаем новый логгер
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    
    # Создаем структуру директорий: logs/user/task_name
    log_path = Path(logs_dir) / user
    if task_name:
        log_path = log_path / task_name
    log_path.mkdir(parents=True, exist_ok=True)

    # Формат сообщений: время, уровень, [пользователь.задача], сообщение
    log_format = f"%(asctime)s %(levelname)s [{logger_name}] %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(log_format, date_format)

    # Получаем текущую дату для имени файла
    today = datetime.now().strftime("%Y-%m-%d")
    
    # Основной лог-файл (INFO и выше)
    main_log_file = log_path / f"{today}.log"
    main_handler = RotatingFileHandler(
        filename=main_log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8"
    )
    main_handler.setLevel(logging.INFO)
    main_handler.setFormatter(formatter)
    
    # Добавляем хендлеры к логгеру с проверкой на наличие хендлеров перед добавлением новых
    if not logger.handlers:
        logger.addHandler(main_handler)
        # Детальный DEBUG лог-файл (опционально)
        if detailed:
            detailed_log_file = log_path / f"{today}_detailed.log"
            debug_handler = RotatingFileHandler(
                filename=detailed_log_file,
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding="utf-8"
            )
            debug_handler.setLevel(logging.DEBUG)
            debug_handler.setFormatter(formatter)
            logger.addHandler(debug_handler)

    # Очистка старых лог-файлов (старше backup_count дней)
    _cleanup_old_logs(log_path, backup_count)

    # Сохраняем логгер в кеш
    _LOGGERS[logger_name] = logger
    return logger


def _cleanup_old_logs(log_path: Path, days_to_keep: int) -> None:
    """
    Удаляет лог-файлы старше указанного количества дней.
    
    :param log_path: путь к директории с лог-файлами
    :param days_to_keep: количество дней для хранения файлов
    """
    try:
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        for log_file in log_path.glob("*.log"):
            # Извлекаем дату из имени файла (формат YYYY-MM-DD.log или YYYY-MM-DD_detailed.log)
            try:
                date_part = log_file.stem.split('_')[0]  # Убираем _detailed если есть
                file_date = datetime.strptime(date_part, "%Y-%m-%d")
                
                if file_date < cutoff_date:
                    log_file.unlink()
            except (ValueError, IndexError):
                # Пропускаем файлы с неожиданным форматом имени
                continue
    except Exception:
        # Ошибки очистки не должны прерывать основную работу
        pass
