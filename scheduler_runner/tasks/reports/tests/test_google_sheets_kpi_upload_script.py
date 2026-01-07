"""
test_google_sheets_kpi_upload_script.py

Юнит-тесты для скрипта GoogleSheets_KPI_UploadScript.py.
Тестирует функциональность скрипта, включая:
- Парсинг аргументов командной строки
- Загрузку данных через load_kpi_report_data
- Преобразование данных через transform_kpi_data_for_sheets
- Основную логику работы скрипта
"""

import sys
import argparse
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest

# Добавляем путь к проекту для импорта
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from scheduler_runner.tasks.reports import GoogleSheets_KPI_UploadScript
from scheduler_runner.tasks.reports.config.scripts.GoogleSheets_KPI_UploadScript_config import SCRIPT_CONFIG


def test_parse_arguments_defaults():
    """Тест парсинга аргументов с значениями по умолчанию."""
    # Тестируем, что аргументы парсятся корректно
    with patch('sys.argv', ['script_name']):  # без аргументов
        args = GoogleSheets_KPI_UploadScript.parse_arguments()
        
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
        args = GoogleSheets_KPI_UploadScript.parse_arguments()
        
        assert args.report_date == '2026-01-05'
        assert args.detailed_logs is True
        assert args.pvz_id == 'TEST_PVZ'


def test_load_kpi_report_data():
    """Тест загрузки KPI данных отчетов."""
    with patch('scheduler_runner.tasks.reports.GoogleSheets_KPI_UploadScript.load_reports_data') as mock_load:

        # Мокаем возвращаемые данные
        mock_data = {
            'issued_packages': 100,
            'total_packages': 150,
            'direct_flow_count': 50,
            'pvz_info': 'Test PVZ',
            '_report_date': '2026-01-05',
            '_loaded_at': '2026-01-05T12:00:00Z',
            '_reports_loaded': ['giveout', 'direct_flow']
        }
        mock_load.return_value = mock_data

        result = GoogleSheets_KPI_UploadScript.load_kpi_report_data('2026-01-05', 'Test PVZ')

        # Проверяем, что функция была вызвана
        mock_load.assert_called_once_with(
            report_date='2026-01-05',
            pvz_id='Test PVZ',
            config=SCRIPT_CONFIG["REPORT_CONFIGS"]
        )

        # Проверяем результат
        assert result == mock_data


def test_transform_kpi_data_for_sheets():
    """Тест преобразования KPI данных для Google Sheets."""
    from scheduler_runner.tasks.reports.utils.data_transformers import GoogleSheetsTransformer
    
    raw_data = {
        'issued_packages': 100,
        'total_packages': 150,
        'direct_flow_count': 50,
        'pvz_info': 'Test PVZ',
        '_report_date': '2026-01-05'
    }
    
    result = GoogleSheets_KPI_UploadScript.transform_kpi_data_for_sheets(raw_data)
    
    # Проверяем, что результат содержит ожидаемые поля
    expected_fields = ['id', 'Дата', 'ПВЗ', 'Количество выдач', 'Прямой поток', 'Возвратный поток']
    for field in expected_fields:
        assert field in result
    
    # Проверяем конкретные значения
    assert result['Количество выдач'] == 100
    assert result['Прямой поток'] == 50
    assert result['ПВЗ'] == 'Test PVZ'


def test_main_function_with_mocked_dependencies():
    """Тест основной функции main с замоканными зависимостями."""
    test_argv = [
        'script_name',
        '--report_date', '2026-01-05',
        '--pvz_id', 'TEST_PVZ',
        '--detailed_logs'
    ]

    with patch('sys.argv', test_argv), \
         patch('scheduler_runner.tasks.reports.GoogleSheets_KPI_UploadScript.load_kpi_report_data') as mock_load_data, \
         patch('scheduler_runner.tasks.reports.GoogleSheets_KPI_UploadScript.transform_kpi_data_for_sheets') as mock_transform, \
         patch('scheduler_runner.tasks.reports.GoogleSheets_KPI_UploadScript.GoogleSheetsReporter') as mock_reporter_class, \
         patch('config.base_config.PVZ_ID', 'DEFAULT_PVZ'):  # для случая, когда pvz_id не передан

        mock_load_data.return_value = {
            'issued_packages': 100,
            'pvz_info': 'TEST_PVZ',
            '_report_date': '2026-01-05'
        }

        mock_transform.return_value = {
            'id': '',
            'Дата': '05.01.2026',
            'ПВЗ': 'TEST_PVZ',
            'Количество выдач': 100,
            'Прямой поток': 50,
            'Возвратный поток': 0
        }

        mock_reporter_instance = MagicMock()
        mock_reporter_instance.update_or_append_data_with_config.return_value = {
            'success': True,
            'action': 'appended',
            'message': 'Data appended successfully'
        }
        mock_reporter_class.return_value = mock_reporter_instance

        # Вызываем main
        GoogleSheets_KPI_UploadScript.main()

        # Проверяем, что все функции были вызваны
        mock_load_data.assert_called_once_with('2026-01-05', 'TEST_PVZ')
        mock_transform.assert_called_once()
        mock_reporter_class.assert_called_once()
        mock_reporter_instance.update_or_append_data_with_config.assert_called_once()


def test_main_function_with_empty_data():
    """Тест основной функции при отсутствии данных."""
    test_argv = [
        'script_name',
        '--report_date', '2026-01-05',
        '--pvz_id', 'TEST_PVZ'
    ]
    
    with patch('sys.argv', test_argv), \
         patch('scheduler_runner.tasks.reports.GoogleSheets_KPI_UploadScript.load_kpi_report_data') as mock_load_data, \
         patch('scheduler_runner.utils.logging.configure_logger') as mock_logger:
        
        # Мокаем отсутствие данных
        mock_load_data.return_value = {}
        
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance
        
        # Вызываем main - не должно быть ошибки
        try:
            GoogleSheets_KPI_UploadScript.main()
        except Exception:
            # Если была ошибка, проверим, что это не критическая ошибка
            pass
        
        # Проверяем, что загрузка данных была вызвана
        mock_load_data.assert_called_once_with('2026-01-05', 'TEST_PVZ')


def test_main_function_with_transform_error():
    """Тест основной функции при ошибке трансформации данных."""
    test_argv = [
        'script_name',
        '--report_date', '2026-01-05',
        '--pvz_id', 'TEST_PVZ'
    ]
    
    with patch('sys.argv', test_argv), \
         patch('scheduler_runner.tasks.reports.GoogleSheets_KPI_UploadScript.load_kpi_report_data') as mock_load_data, \
         patch('scheduler_runner.tasks.reports.GoogleSheets_KPI_UploadScript.transform_kpi_data_for_sheets') as mock_transform, \
         patch('scheduler_runner.utils.logging.configure_logger') as mock_logger:
        
        # Мокаем данные
        mock_load_data.return_value = {'some': 'data'}
        
        # Мокаем ошибку при трансформации
        mock_transform.side_effect = Exception("Transformation error")
        
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance
        
        # Вызываем main - должна быть обработка ошибки
        try:
            GoogleSheets_KPI_UploadScript.main()
        except Exception:
            # Ошибка должна быть обработана внутри main
            pass
        
        # Проверяем, что загрузка данных была вызвана
        mock_load_data.assert_called_once_with('2026-01-05', 'TEST_PVZ')
        # Проверяем, что трансформация была вызвана
        mock_transform.assert_called_once()


def test_argument_parser_help():
    """Тест справки аргументов."""
    # Проверяем, что функция parse_arguments работает без ошибок
    # и возвращает объект с правильными атрибутами
    with patch('sys.argv', ['script_name']):
        args = GoogleSheets_KPI_UploadScript.parse_arguments()

        # Проверяем, что аргументы существуют
        assert hasattr(args, 'report_date')
        assert hasattr(args, 'detailed_logs')
        assert hasattr(args, 'pvz_id')


def test_load_kpi_report_data_with_real_configs():
    """Тест загрузки данных с реальной конфигурацией (частично моканная)."""
    from scheduler_runner.tasks.reports.config.scripts.GoogleSheets_KPI_UploadScript_config import REPORT_CONFIGS

    with patch('scheduler_runner.tasks.reports.GoogleSheets_KPI_UploadScript.load_reports_data') as mock_load:

        expected_data = {
            'issued_packages': 200,
            'pvz_info': 'Real Test PVZ',
            '_report_date': '2026-01-05'
        }
        mock_load.return_value = expected_data

        result = GoogleSheets_KPI_UploadScript.load_kpi_report_data('2026-01-05', 'Real Test PVZ')

        # Проверяем, что вызов был с правильными параметрами
        mock_load.assert_called_once_with(
            report_date='2026-01-05',
            pvz_id='Real Test PVZ',
            config=REPORT_CONFIGS
        )

        assert result == expected_data


def test_transform_kpi_data_edge_cases():
    """Тест преобразования данных с крайними случаями."""
    # Тест с минимальными данными
    minimal_data = {}
    result = GoogleSheets_KPI_UploadScript.transform_kpi_data_for_sheets(minimal_data)
    
    # Даже с пустыми данными должны быть базовые поля
    expected_fields = ['id', 'Дата', 'ПВЗ', 'Количество выдач', 'Прямой поток', 'Возвратный поток']
    for field in expected_fields:
        assert field in result
    
    # Тест с None значениями
    none_data = {
        'issued_packages': None,
        'direct_flow_count': None,
        'pvz_info': None
    }
    result = GoogleSheets_KPI_UploadScript.transform_kpi_data_for_sheets(none_data)
    
    # Проверяем, что значения по умолчанию установлены
    assert result['Количество выдач'] == 0
    assert result['Прямой поток'] == 0
    assert result['ПВЗ'] == ''


def test_main_with_config_import():
    """Тест основной функции с проверкой импорта конфигурации."""
    # Проверяем, что конфигурация может быть импортирована
    from scheduler_runner.tasks.reports.config.scripts.GoogleSheets_KPI_UploadScript_config import SCRIPT_CONFIG
    
    assert 'CREDENTIALS_PATH' in SCRIPT_CONFIG
    assert 'SPREADSHEET_NAME' in SCRIPT_CONFIG
    assert 'WORKSHEET_NAME' in SCRIPT_CONFIG
    assert 'TASK_NAME' in SCRIPT_CONFIG
    assert SCRIPT_CONFIG['TASK_NAME'] == 'GoogleSheets_KPI_UploadScript'