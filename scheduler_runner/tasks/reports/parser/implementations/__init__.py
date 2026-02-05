"""
Модуль реализаций парсеров отчетов

Этот модуль предоставляет доступ к различным реализациям парсеров,
включая примеры использования базовых классов.

В версии 3.0.0 были внесены изменения в базовые классы:
- Метод select_option_from_dropdown переименован в _select_option_from_dropdown
- Метод set_element_value теперь использует _select_option_from_dropdown для работы с выпадающими списками
"""
from .multi_step_ozon_parser import MultiStepOzonParser

__all__ = [
    'MultiStepOzonParser',
]