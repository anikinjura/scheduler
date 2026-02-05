"""
logging.py

Модуль для настройки и управления логированием задач и ядра планировщика.

Возможности:
    - Создание структуры логов logs/{user}/{task_name}/YYYY-MM-DD.log
    - Ротация лог-файлов по размеру и количеству backup-файлов
    - Поддержка отдельного detailed-лога (DEBUG) при detailed=True
    - Поддержка отдельных файлов для конкретных уровней логирования
    - Поддержка комбинаций уровней логирования
    - Поддержка кастомного уровня трассировки
    - Возможность записи нескольких уровней в один файл или в разные файлы
    - Автоматическая очистка старых логов (старше backup_count дней)
    - Кеширование логгеров для предотвращения дублирования хендлеров

Основные функции:
    - configure_logger(user, task_name=None, detailed=False, ...): возвращает сконфигурированный Logger
    - _cleanup_old_logs(log_path, days_to_keep): удаляет устаревшие логи

Пример использования:
    from scheduler_runner.utils.logging import configure_logger
    logger = configure_logger(user="operator", task_name="CopyScript", detailed=True)
    logger.info("Задача успешно выполнена")

    # Для трассировки методов:
    logger = configure_logger(user="operator", task_name="MyClass", log_levels=[TRACE_LEVEL])
    logger.trace("Попали в метод MyClass.some_method")

    # Комбинация уровней (разные файлы):
    logger = configure_logger(user="operator", task_name="MyClass", log_levels=[TRACE_LEVEL, logging.DEBUG])
    logger.trace("Сообщение трассировки")
    logger.debug("Отладочное сообщение")

    # Комбинация уровней (один файл):
    logger = configure_logger(user="operator", task_name="MyClass",
                             log_levels=[TRACE_LEVEL, logging.DEBUG], single_file_for_levels=True)
    logger.trace("Сообщение трассировки")
    logger.debug("Отладочное сообщение")

Author: anikinjura
"""
__version__ = '0.0.2'

import logging
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, List, Optional

# Добавляем пользовательский уровень логирования для трассировки
TRACE_LEVEL = 5  # Между DEBUG(10) и INFO(20)
logging.addLevelName(TRACE_LEVEL, "TRACE")

def trace(self, message, *args, **kwargs):
    # Используем _log напрямую, чтобы обойти isEnabledFor
    # isEnabledFor может не работать корректно с пользовательскими уровнями < DEBUG
    if self.level <= TRACE_LEVEL:
        self._log(TRACE_LEVEL, message, args, **kwargs)

logging.Logger.trace = trace

# Глобальный кеш логгеров и их конфигураций для предотвращения дублирования хендлеров
_LOGGERS: Dict[str, logging.Logger] = {}
_LOGGER_CONFIGS: Dict[str, dict] = {}  # Хранит параметры конфигурации для каждого логгера


def configure_logger(
    user: str,
    task_name: Optional[str] = None,
    detailed: bool = False,
    log_levels: Optional[List[int]] = None,  # Новый параметр для комбинации уровней
    single_file_for_levels: bool = False,  # Новый параметр: записывать уровни в один файл
    logs_dir: str = "logs",
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5
) -> logging.Logger:
    """
    Конфигурирует и возвращает логгер для заданного пользователя и задачи.

    Создает структуру папок logs/{user}/{task_name}/ и настраивает ротацию файлов.
    Основной лог-файл содержит сообщения уровня INFO и выше.
    При включенном detailed режиме создается дополнительный файл с DEBUG сообщениями.
    При указании log_levels создаются отдельные файлы для этих уровней логирования
    (если single_file_for_levels=False) или один файл для всех уровней (если single_file_for_levels=True).

    Уровень логгера автоматически устанавливается на минимальный из используемых уровней,
    чтобы обеспечить корректную работу стандартных методов логирования (debug, info, warning и т.д.).

    :param user: имя системного пользователя
    :param task_name: опциональный идентификатор задачи
    :param detailed: если True, добавляется отдельный DEBUG-лог файл (обратная совместимость)
    :param log_levels: список уровней, для которых создаются файлы (по умолчанию None)
    :param single_file_for_levels: если True, все указанные уровни записываются в один файл (по умолчанию False)
    :param logs_dir: базовая директория для лог-файлов (по умолчанию "logs")
    :param max_bytes: размер файла для ротации в байтах (по умолчанию 10MB)
    :param backup_count: количество backup-файлов для хранения (по умолчанию 5)
    :return: сконфигурированный экземпляр Logger
    """
    # Формируем уникальное имя логгера
    logger_name = user if not task_name else f"{user}.{task_name}"

    # Проверяем, есть ли уже закешированный логгер с такой же конфигурацией
    current_config = {
        'detailed': detailed,
        'log_levels': log_levels,
        'single_file_for_levels': single_file_for_levels
    }

    cached_config = _LOGGER_CONFIGS.get(logger_name)
    if logger_name in _LOGGERS and cached_config == current_config:
        return _LOGGERS[logger_name]

    # Создаем новый логгер или обновляем существующий
    logger = logging.getLogger(logger_name)

    # Удаляем старые хендлеры, если конфигурация изменилась
    if logger.handlers:
        for handler in logger.handlers[:]:  # используем срез, чтобы избежать изменения списка во время итерации
            logger.removeHandler(handler)
            handler.close()  # закрываем хендлер, чтобы освободить файлы

    # Устанавливаем уровень логгера на минимальный из указанных уровней или TRACE_LEVEL, чтобы пропускать все уровни
    min_level = TRACE_LEVEL  # начальный минимальный уровень
    if log_levels:
        min_level = min(min_level, min(log_levels))
    if detailed:
        min_level = min(min_level, logging.DEBUG)
    logger.setLevel(min_level)

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

    logger.addHandler(main_handler)

    # Детальный DEBUG лог-файл (опционально, для обратной совместимости)
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

    # Отдельные файлы для указанных уровней логирования
    if log_levels:
        if single_file_for_levels:
            # Создаем один файл для всех указанных уровней
            levels_names = [logging.getLevelName(level).lower() for level in log_levels]
            combined_filename = "_".join(levels_names)
            level_log_file = log_path / f"{today}_{combined_filename}.log"
            level_handler = RotatingFileHandler(
                filename=level_log_file,
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding="utf-8"
            )
            # Устанавливаем уровень хендлера в NOTSET, чтобы он получал все сообщения
            # Фильтр будет определять, какие сообщения записывать
            level_handler.setLevel(logging.NOTSET)

            # Создаем фильтр, чтобы хендлер принимал только сообщения из указанного списка уровней
            class LevelsFilter:
                def __init__(self, levels):
                    self.levels = set(levels)

                def filter(self, record):
                    return record.levelno in self.levels

            level_handler.addFilter(LevelsFilter(log_levels))
            level_handler.setFormatter(formatter)
            logger.addHandler(level_handler)
        else:
            # Создаем отдельные файлы для каждого уровня
            for level in log_levels:
                level_name = logging.getLevelName(level).lower()
                level_log_file = log_path / f"{today}_{level_name}.log"
                level_handler = RotatingFileHandler(
                    filename=level_log_file,
                    maxBytes=max_bytes,
                    backupCount=backup_count,
                    encoding="utf-8"
                )
                # Устанавливаем уровень хендлера в NOTSET, чтобы он получал все сообщения
                # Фильтр будет определять, какие сообщения записывать
                level_handler.setLevel(logging.NOTSET)

                # Создаем фильтр, чтобы хендлер принимал только сообщения указанного уровня
                class LevelFilter:
                    def __init__(self, level):
                        self.level = level

                    def filter(self, record):
                        return record.levelno == self.level

                level_handler.addFilter(LevelFilter(level))
                level_handler.setFormatter(formatter)
                logger.addHandler(level_handler)

    # Очистка старых лог-файлов (старше backup_count дней)
    _cleanup_old_logs(log_path, backup_count)

    # Сохраняем логгер и его конфигурацию в кеш
    _LOGGERS[logger_name] = logger
    _LOGGER_CONFIGS[logger_name] = current_config
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
            # Извлекаем дату из имени файла (формат YYYY-MM-DD.log, YYYY-MM-DD_detailed.log или YYYY-MM-DD_{level}.log)
            try:
                date_part = log_file.stem.split('_')[0]  # Убираем суффикс уровня если есть
                file_date = datetime.strptime(date_part, "%Y-%m-%d")

                if file_date < cutoff_date:
                    log_file.unlink()
            except (ValueError, IndexError):
                # Пропускаем файлы с неожиданным форматом имени
                continue
    except Exception:
        # Ошибки очистки не должны прерывать основную работу
        pass
