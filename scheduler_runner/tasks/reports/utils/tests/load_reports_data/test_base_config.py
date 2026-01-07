"""
test_base_config.py

Тесты для BaseConfig.
"""

import json
from dataclasses import dataclass
from enum import Enum
from scheduler_runner.tasks.reports.config.reports_base_config import BaseConfig


class TestEnum(Enum):
    VALUE1 = "value1"
    VALUE2 = "value2"


@dataclass
class TestConfig(BaseConfig):
    """Тестовая конфигурация."""
    name: str
    value: int
    enabled: bool = True
    enum_value: str = "value1"  # используем строку вместо Enum для простоты
    optional_field: str = None


def test_base_config_creation():
    """Тест создания конфигурации."""
    config = TestConfig(
        name="test",
        value=42,
        enabled=True,
        enum_value="value2"
    )

    assert config.name == "test"
    assert config.value == 42
    assert config.enabled is True
    assert config.enum_value == "value2"
    assert config.optional_field is None


def test_base_config_to_dict():
    """Тест преобразования в словарь."""
    config = TestConfig(
        name="test",
        value=42,
        enabled=True,
        enum_value="value2",
        optional_field="optional"
    )

    result = config.to_dict()

    expected = {
        'name': 'test',
        'value': 42,
        'enabled': True,
        'enum_value': 'value2',
        'optional_field': 'optional'
    }

    assert result == expected


def test_base_config_to_json():
    """Тест сериализации в JSON."""
    config = TestConfig(
        name="test_json",
        value=100
    )
    
    json_str = config.to_json()
    parsed = json.loads(json_str)
    
    expected = {
        'name': 'test_json',
        'value': 100,
        'enabled': True,
        'enum_value': 'value1',
        'optional_field': None
    }
    
    assert parsed == expected


def test_base_config_from_dict():
    """Тест создания из словаря."""
    data = {
        'name': 'from_dict',
        'value': 200,
        'enabled': False,
        'enum_value': 'value2',
        'optional_field': 'test'
    }

    config = TestConfig.from_dict(data)

    assert config.name == 'from_dict'
    assert config.value == 200
    assert config.enabled is False
    assert config.enum_value == 'value2'
    assert config.optional_field == 'test'


def test_base_config_from_json():
    """Тест создания из JSON."""
    json_str = json.dumps({
        'name': 'from_json',
        'value': 300,
        'enabled': True,
        'enum_value': 'value1'
    })

    config = TestConfig.from_json(json_str)

    assert config.name == 'from_json'
    assert config.value == 300
    assert config.enabled is True
    assert config.enum_value == 'value1'


def test_base_config_repr():
    """Тест строкового представления."""
    config = TestConfig(name="repr_test", value=999)
    repr_str = repr(config)
    
    assert "TestConfig" in repr_str
    assert "name='repr_test'" in repr_str
    assert "value=999" in repr_str


def test_base_config_optional_fields():
    """Тест опциональных полей."""
    config = TestConfig(name="optional_test", value=123)
    
    # Проверяем, что опциональное поле не заполнено
    assert config.optional_field is None
    
    # Проверяем преобразование в словарь
    result = config.to_dict()
    assert result['optional_field'] is None