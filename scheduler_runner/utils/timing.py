"""
timing.py

Модуль содержит утилиту для проверки, должна ли задача запускаться в текущий момент времени
(логика "окна" запуска для разных типов расписания).

Функции:
    - should_run_now(task: dict, now: datetime) -> bool
        Проверяет, нужно ли запускать задачу в данный момент.
        Поддерживает типы расписания:
            - 'hourly': задача запускается каждый раз при вызове (опционально в пределах time_window)
            - 'daily': задача запускается, если now.hour == scheduled_hour (минуты игнорируются)
            - 'once': зарезервировано для будущей реализации

Пример использования:
    from scheduler_runner.utils.timing import should_run_now
    from datetime import datetime

    task = {'schedule': 'daily', 'time': '12:30'}
    if should_run_now(task, datetime.now()):
        print("Запускаем задачу!")

Raises:
    ValueError: если тип расписания не поддерживается или формат времени некорректен.

Author: anikinjura
"""
__version__ = '0.0.2'

from datetime import datetime
from typing import Dict, Any


def _parse_time_window(time_window: str) -> tuple:
    """
    Парсит строку временного окна в формате 'HH:MM-HH:MM'.

    :param time_window: строка вида '09:00-21:00'
    :return: кортеж (start_hour, start_minute, end_hour, end_minute)
    :raises ValueError: при неверном формате
    """
    try:
        start_str, end_str = time_window.split('-')
        start_hour, start_minute = map(int, start_str.split(':'))
        end_hour, end_minute = map(int, end_str.split(':'))

        # Валидация диапазона
        if not (0 <= start_hour <= 23 and 0 <= start_minute <= 59):
            raise ValueError(f"Время начала вне допустимого диапазона: {start_str}")
        if not (0 <= end_hour <= 23 and 0 <= end_minute <= 59):
            raise ValueError(f"Время окончания вне допустимого диапазона: {end_str}")

        return start_hour, start_minute, end_hour, end_minute
    except ValueError as e:
        if "неверном формате" in str(e) or "недопустимого диапазона" in str(e):
            raise
        raise ValueError(
            f"Неверный формат time_window '{time_window}': ожидается 'HH:MM-HH:MM'"
        ) from e


def _is_within_time_window(now: datetime, time_window: str) -> bool:
    """
    Проверяет, попадает ли текущее время в заданное временное окно.

    :param now: текущее время datetime
    :param time_window: строка вида '09:00-21:00'
    :return: True, если время в пределах окна
    :raises ValueError: при неверном формате time_window
    """
    start_hour, start_minute, end_hour, end_minute = _parse_time_window(time_window)

    current_minutes = now.hour * 60 + now.minute
    start_minutes = start_hour * 60 + start_minute
    end_minutes = end_hour * 60 + end_minute

    return start_minutes <= current_minutes < end_minutes


def should_run_now(task: Dict[str, Any], now: datetime) -> bool:
    """
    Определяет, должна ли задача выполняться в указанное время.

    Поддерживаемые типы расписания:
    - 'hourly': выполняется каждый раз при вызове (опционально в пределах time_window)
    - 'daily': выполняется если now.hour == scheduled_hour (т.е. в любое время в течение часа)
    - 'once': зарезервировано для будущей реализации

    :param task: словарь с ключами 'schedule', опционально 'time' и 'time_window'
    :param now: текущее время datetime
    :return: True, если задача должна выполняться сейчас
    :raises ValueError: при неподдерживаемом типе расписания или неверном формате времени
    """
    schedule_type = task.get('schedule')
    time_window = task.get('time_window')

    if schedule_type == 'hourly':
        # Если задан time_window, проверяем попадание в него
        if time_window:
            return _is_within_time_window(now, time_window)
        # Выполняется каждый раз при вызове (без ограничений)
        return True

    elif schedule_type == 'daily':
        # Выполняется если now.hour == scheduled_hour (т.е. в любое время в течение часа)
        time_str = task.get('time')
        if not time_str:
            raise ValueError("Ежедневное расписание требует параметр 'time' в формате 'HH:MM'")
        
        try:
            hour, minute = map(int, time_str.split(':'))
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError(f"Время вне допустимого диапазона: {time_str}")
        except (ValueError, AttributeError) as e:
            raise ValueError(f"Неверный формат времени '{time_str}': ожидается 'HH:MM'") from e
        
        # Выполняется в указанное время (с "окном" по часу - в текущем часе)
        return now.hour == hour
    
    elif schedule_type == 'once':
        # Зарезервировано для будущей реализации
        return False
    
    else:
        raise ValueError(f"Неподдерживаемый тип расписания: {schedule_type}")
