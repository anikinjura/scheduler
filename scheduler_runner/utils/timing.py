"""
timing.py

Модуль содержит утилиту для проверки, должна ли задача запускаться в текущий момент времени
(логика "окна" запуска для разных типов расписания).

Функции:
    - should_run_now(task: dict, now: datetime) -> bool
        Проверяет, нужно ли запускать задачу в данный момент.
        Поддерживает типы расписания:
            - 'hourly': задача запускается каждый раз при вызове
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
__version__ = '0.0.1'

from datetime import datetime
from typing import Dict, Any

def should_run_now(task: Dict[str, Any], now: datetime) -> bool:
    """
    Определяет, должна ли задача выполняться в указанное время (с "окном" по часу).
    
    Поддерживаемые типы расписания:
    - 'hourly': выполняется каждый раз при вызове
    - 'daily': выполняется если now.hour == scheduled_hour (т.е. в любое время в течение часа)
    - 'once': зарезервировано для будущей реализации
    
    :param task: словарь с ключами 'schedule' и опционально 'time'
    :param now: текущее время datetime
    :return: True, если задача должна выполняться сейчас
    :raises ValueError: при неподдерживаемом типе расписания или неверном формате времени
    """
    schedule_type = task.get('schedule')
    
    if schedule_type == 'hourly':
        # Выполняется каждый раз при вызове
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
