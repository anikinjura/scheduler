"""
test_telegram_notification_script.py

Тесты для скрипта TelegramNotificationScript.py.
Проверяет функциональность нового скрипта уведомлений в Telegram.
"""

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest

# Добавляем путь к проекту для импорта
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from scheduler_runner.tasks.reports.Telegram_KPI_NotificationScript import (
    load_reports_data,
    format_notification_message,
    parse_arguments
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
        'Дата': '05.01.2026',
        'ПВЗ': 'TEST_PVZ',
        'Количество выдач': 100,
        'Прямой поток': 50,
        'Возвратный поток': 10
    }
    
    message = format_notification_message(test_data)
    
    # Проверяем, что сообщение содержит основные элементы
    assert '05.01.2026' in message
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


def test_load_reports_data_integration():
    """Тест интеграции загрузки данных."""
    from scheduler_runner.tasks.reports.config.scripts.TelegramNotificationScript_config import SCRIPT_CONFIG
    
    with patch('scheduler_runner.tasks.reports.utils.load_reports_data.load_reports_data') as mock_load:
        # Мокаем возвращаемые данные
        mock_data = {
            'issued_packages': 150,
            'direct_flow_count': 75,
            'return_flow_count': 25,
            'pvz_info': 'Test PVZ',
            '_report_date': '2026-01-05'
        }
        mock_load.return_value = mock_data
        
        # Загружаем данные через новую архитектуру
        result = load_reports_data(
            report_date='2026-01-05',
            pvz_id='Test PVZ',
            config=SCRIPT_CONFIG["REPORT_CONFIGS"]
        )
        
        # Проверяем, что вызов был выполнен
        mock_load.assert_called_once()
        
        assert result == mock_data


def test_main_function_with_mocked_dependencies():
    """Тест основной функции main с замоканными зависимостями."""
    test_argv = [
        'script_name',
        '--report_date', '2026-01-05',
        '--pvz_id', 'TEST_PVZ',
        '--detailed_logs'
    ]
    
    with patch('sys.argv', test_argv), \
         patch('scheduler_runner.tasks.reports.TelegramNotificationScript.load_reports_data') as mock_load_data, \
         patch('scheduler_runner.tasks.reports.TelegramNotificationScript.format_notification_message') as mock_format, \
         patch('scheduler_runner.utils.notify.send_telegram_message') as mock_send_telegram, \
         patch('scheduler_runner.tasks.reports.config.scripts.TelegramNotificationScript_config.SCRIPT_CONFIG') as mock_config:
        
        # Настройка моков
        mock_config.__getitem__.side_effect = lambda key: {
            "TELEGRAM_BOT_TOKEN": "test_token",
            "TELEGRAM_CHAT_ID": "test_chat_id",
            "REPORT_CONFIGS": [],
            "USER": "system",
            "TASK_NAME": "TelegramNotificationScript",
            "DETAILED_LOGS": False,
        }[key]
        
        mock_load_data.return_value = {
            'issued_packages': 100,
            'pvz_info': 'TEST_PVZ',
            '_report_date': '2026-01-05'
        }
        
        mock_format.return_value = "Тестовое уведомление для TEST_PVZ за 05.01.2026"
        
        mock_send_telegram.return_value = (True, {"ok": True, "result": {"message_id": 123}})
        
        # Импортируем и вызываем main (через exec, чтобы избежать проблем с циклическими импортами)
        from scheduler_runner.tasks.reports.TelegramNotificationScript import main
        
        # Вызываем main
        main()
        
        # Проверяем, что все функции были вызваны
        mock_load_data.assert_called_once()
        mock_format.assert_called_once()
        mock_send_telegram.assert_called_once()


def test_format_for_google_sheets_compatibility():
    """Тест форматирования данных для совместимости с Google Sheets структурой."""
    from scheduler_runner.tasks.reports.TelegramNotificationScript import _format_for_google_sheets
    
    test_data = {
        'issued_packages': 200,
        'direct_flow_data': {'total_items_count': 50},
        'return_flow_data': {'total_items_count': 15},
        'pvz_info': 'Test PVZ',
        '_report_date': '2026-01-05'
    }
    
    formatted = _format_for_google_sheets(test_data, '2026-01-05', 'Test PVZ')
    
    # Проверяем, что формат соответствует ожидаемой структуре
    expected_fields = ['Дата', 'ПВЗ', 'Количество выдач', 'Прямой поток', 'Возвратный поток']
    for field in expected_fields:
        assert field in formatted
    
    # Проверяем конкретные значения
    assert formatted['Количество выдач'] == 200
    assert formatted['Прямой поток'] == 50
    assert formatted['Возвратный поток'] == 15
    assert formatted['ПВЗ'] == 'Test PVZ'


def test_validate_report_data():
    """Тест валидации данных для уведомлений."""
    from scheduler_runner.tasks.reports.TelegramNotificationScript import validate_report_data
    
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
    from scheduler_runner.tasks.reports.TelegramNotificationScript import validate_report_data
    
    invalid_data = {
        'Неправильная_дата': '05.01.2026',  # нет обязательного поля 'Дата'
        'ПВЗ': 'Test PVZ'
    }
    
    is_valid = validate_report_data(invalid_data)
    assert is_valid is False


def test_get_report_summary():
    """Тест получения сводки по отчету."""
    from scheduler_runner.tasks.reports.TelegramNotificationScript import get_report_summary
    
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


def test_load_reports_data_with_real_config():
    """Тест загрузки данных с реальной конфигурацией (частично моканная)."""
    from scheduler_runner.tasks.reports.config.scripts.TelegramNotificationScript_config import REPORT_CONFIGS
    
    with patch('scheduler_runner.tasks.reports.utils.load_reports_data.load_reports_data') as mock_load:
        expected_data = {
            'issued_packages': 250,
            'pvz_info': 'Real Test PVZ',
            '_report_date': '2026-01-05'
        }
        mock_load.return_value = expected_data
        
        # Используем реальную конфигурацию
        result = load_reports_data(
            report_date='2026-01-05',
            pvz_id='Real Test PVZ',
            config=REPORT_CONFIGS
        )
        
        # Проверяем, что вызов был с правильными параметрами
        mock_load.assert_called_once_with(
            report_date='2026-01-05',
            pvz_id='Real Test PVZ',
            config=REPORT_CONFIGS
        )
        
        assert result == expected_data


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
    
    test_load_reports_data_integration()
    print("✓ test_load_reports_data_integration")
    
    test_format_for_google_sheets_compatibility()
    print("✓ test_format_for_google_sheets_compatibility")
    
    test_validate_report_data()
    print("✓ test_validate_report_data")
    
    test_validate_report_data_invalid()
    print("✓ test_validate_report_data_invalid")
    
    test_get_report_summary()
    print("✓ test_get_report_summary")
    
    test_load_reports_data_with_real_config()
    print("✓ test_load_reports_data_with_real_config")
    
    print("\nВсе тесты пройдены успешно! 🎉")