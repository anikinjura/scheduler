"""
test_telegram_notification_script_basic.py

Базовые тесты для скрипта TelegramNotificationScript.py.
Проверяет основную функциональность скрипта.
"""

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest

# Добавляем путь к проекту для импорта
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from scheduler_runner.tasks.reports.Telegram_KPI_NotificationScript import (
    parse_arguments,
    format_notification_message,
    _format_for_google_sheets,
    validate_report_data,
    get_report_summary
)


def test_parse_arguments_defaults():
    """Тест парсинга аргументов с значениями по умолчанию."""
    with patch('sys.argv', ['script_name']):  # без аргументов
        args = parse_arguments()
        
        # Проверяем, что аргументы существуют
        assert hasattr(args, 'report_date')
        assert hasattr(args, 'detailed_logs')
        assert hasattr(args, 'pvz_id')
        
        # detailed_logs по умолчанию False
        assert args.detailed_logs is False


def test_parse_arguments_with_values():
    """Тест парсинга аргументов с переданными значениями."""
    test_argv = [
        'script_name',
        '--report_date', '2026-01-05',
        '--detailed_logs',
        '--pvz_id', 'TEST_PVZ'
    ]
    
    with patch('sys.argv', test_argv):
        args = parse_arguments()
        
        assert args.report_date == '2026-01-05'
        assert args.detailed_logs is True
        assert args.pvz_id == 'TEST_PVZ'


def test_format_notification_message():
    """Тест форматирования сообщения для Telegram."""
    test_data = {
        '_report_date': '2026-01-05',
        'pvz_info': 'TEST_PVZ',
        'issued_packages': 100,
        'direct_flow_count': 50,
        'return_flow_data': {'total_items_count': 10}
    }
    
    message = format_notification_message(test_data)
    
    # Проверяем, что сообщение содержит основные элементы
    assert '2026-01-05' in message
    assert 'TEST_PVZ' in message
    assert '100' in message
    assert '50' in message
    assert '10' in message


def test_format_notification_message_with_empty_data():
    """Тест форматирования сообщения с пустыми данными."""
    empty_data = {}
    
    message = format_notification_message(empty_data)
    
    # Даже с пустыми данными должно быть сформировано сообщение
    assert isinstance(message, str)
    assert len(message) > 0


def test_format_for_google_sheets():
    """Тест форматирования данных для Google Sheets совместимости."""
    test_data = {
        'pvz_info': 'Test PVZ',
        'issued_packages': 150,
        'direct_flow_data': {'total_items_count': 75},
        'return_flow_data': {'total_items_count': 25}
    }
    
    formatted = _format_for_google_sheets(test_data, '2026-01-05', 'Test PVZ')
    
    # Проверяем, что формат соответствует ожидаемой структуре
    expected_fields = ['id', 'Дата', 'ПВЗ', 'Количество выдач', 'Прямой поток', 'Возвратный поток']
    for field in expected_fields:
        assert field in formatted
    
    # Проверяем конкретные значения
    assert formatted['ПВЗ'] == 'Test PVZ'
    assert formatted['Количество выдач'] == 150
    assert formatted['Прямой поток'] == 75
    assert formatted['Возвратный поток'] == 25


def test_validate_report_data():
    """Тест валидации данных для уведомлений."""
    valid_data = {
        'Дата': '05.01.2026',
        'ПВЗ': 'Test PVZ',
        'Количество выдач': 100,
        'Прямой поток': 50,
        'Возвратный поток': 10
    }
    
    is_valid = validate_report_data(valid_data)
    assert is_valid is True


def test_validate_report_data_invalid():
    """Тест валидации некорректных данных."""
    invalid_data = {
        'Неправильная_дата': '05.01.2026',  # нет обязательного поля 'Дата'
        'ПВЗ': 'Test PVZ'
    }
    
    is_valid = validate_report_data(invalid_data)
    assert is_valid is False


def test_get_report_summary():
    """Тест получения сводки по отчету."""
    test_data = {
        'Дата': '05.01.2026',
        'ПВЗ': 'Test PVZ',
        'Количество выдач': 100,
        'Прямой поток': 50,
        'Возвратный поток': 10,
        '_report_date': '2026-01-05',
        '_pvz_id': 'Test PVZ'
    }
    
    summary = get_report_summary(test_data)
    
    # Проверяем, что сводка содержит основные элементы
    assert 'report_date' in summary
    assert 'pvz_id' in summary
    assert 'total_fields' in summary
    assert summary['report_date'] == '05.01.2026'
    assert summary['pvz_id'] == 'Test PVZ'


if __name__ == "__main__":
    print("Тестирование TelegramNotificationScript...")
    
    test_parse_arguments_defaults()
    print("✓ test_parse_arguments_defaults")
    
    test_parse_arguments_with_values()
    print("✓ test_parse_arguments_with_values")
    
    test_format_notification_message()
    print("✓ test_format_notification_message")
    
    test_format_notification_message_with_empty_data()
    print("✓ test_format_notification_message_with_empty_data")
    
    test_format_for_google_sheets()
    print("✓ test_format_for_google_sheets")
    
    test_validate_report_data()
    print("✓ test_validate_report_data")
    
    test_validate_report_data_invalid()
    print("✓ test_validate_report_data_invalid")
    
    test_get_report_summary()
    print("✓ test_get_report_summary")
    
    print("\nВсе базовые тесты пройдены успешно! 🎉")