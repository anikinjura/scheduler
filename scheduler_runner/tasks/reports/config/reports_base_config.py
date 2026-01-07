"""
base_config.py

Базовый класс конфигураций для унификации архитектуры (упрощенная версия для MVP).
В итерации 2 можно добавить JSON Schema, YAML и расширенную валидацию.

Author: anikinjura
Version: 3.0.0 (MVP)
"""

import json
from dataclasses import dataclass, asdict, fields
from typing import Dict, Any, Optional
from enum import Enum


class ConfigError(Exception):
    """Базовое исключение для ошибок конфигурации."""
    pass


@dataclass
class BaseConfig:
    """
    Базовый класс для всех конфигураций в проекте (упрощенная версия).

    В итерации 2 будет добавлено:
    - JSON Schema валидация
    - YAML сериализация
    - Расширенная валидация типов
    - Поддержка вложенных конфигураций
    """

    def to_dict(self) -> Dict[str, Any]:
        """
        Преобразует конфигурацию в словарь (базовая реализация).

        Returns:
            Словарь с данными конфигурации
        """
        result = {}
        for field in fields(self):
            value = getattr(self, field.name)

            # Простая обработка Enum
            if isinstance(value, Enum):
                result[field.name] = value.value
            # Пока не обрабатываем вложенные BaseConfig сложным образом
            elif hasattr(value, 'to_dict'):
                result[field.name] = value.to_dict()
            else:
                result[field.name] = value

        return result

    def to_json(self, indent: int = 2) -> str:
        """
        Сериализует конфигурацию в JSON.

        Args:
            indent: отступ для форматирования

        Returns:
            JSON-строка с конфигурацией
        """
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False, default=str)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BaseConfig':
        """
        Создает конфигурацию из словаря (базовая реализация).

        Args:
            data: словарь с данными конфигурации

        Returns:
            Экземпляр конфигурации
        """
        # Фильтруем поля, которые есть в классе
        field_names = {f.name for f in fields(cls)}
        filtered_data = {k: v for k, v in data.items() if k in field_names}

        # Обрабатываем Enum поля
        processed_data = {}
        for field in fields(cls):
            if field.name in filtered_data:
                value = filtered_data[field.name]

                # Если поле является Enum, преобразуем значение
                if isinstance(field.type, type) and issubclass(field.type, Enum):
                    # Простой Enum
                    if isinstance(value, str):
                        try:
                            processed_data[field.name] = field.type(value)
                        except ValueError:
                            # Если не удается преобразовать, используем значение как есть
                            processed_data[field.name] = value
                    else:
                        processed_data[field.name] = value
                else:
                    processed_data[field.name] = value

        # Создаем экземпляр
        return cls(**processed_data)

    @classmethod
    def from_json(cls, json_str: str) -> 'BaseConfig':
        """
        Создает конфигурацию из JSON строки.

        Args:
            json_str: JSON-строка с конфигурацией

        Returns:
            Экземпляр конфигурации
        """
        data = json.loads(json_str)
        return cls.from_dict(data)

    def __repr__(self) -> str:
        """
        Удобное представление конфигурации.
        """
        class_name = self.__class__.__name__
        fields_str = ", ".join(f"{f.name}={getattr(self, f.name)!r}" for f in fields(self)[:3])
        if len(fields(self)) > 3:
            fields_str += ", ..."

        return f"{class_name}({fields_str})"