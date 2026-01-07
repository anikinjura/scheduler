"""
test_load_reports_data.py

Тесты для основной логики load_reports_data.
"""

import json
from pathlib import Path
from unittest.mock import patch, MagicMock
from scheduler_runner.tasks.reports.utils.load_reports_data import (
    load_reports_data,
    load_single_report,
    merge_reports_data,
    get_default_config,
    ReportConfig,
    MergeStrategy
)


def test_get_default_config():
    """Тест получения конфигурации по умолчанию."""
    config = get_default_config()
    
    assert len(config) == 3  # giveout, direct_flow, carriages
    
    # Проверяем типы отчетов
    report_types = [c.report_type for c in config]
    assert 'giveout' in report_types
    assert 'direct_flow' in report_types
    assert 'carriages' in report_types


def test_load_single_report_success():
    """Тест загрузки одного отчета."""
    config = ReportConfig(
        report_type='giveout',
        file_pattern='ozon_giveout_report_{pvz_id}_{date}.json',
        fields_mapping={'issued_packages': 'issued_packages'}
    )

    with patch('scheduler_runner.tasks.reports.utils.load_reports_data.find_report_file') as mock_find, \
         patch('scheduler_runner.tasks.reports.utils.load_reports_data.load_json_file') as mock_load:

        # Мокаем поиск файла
        mock_find.return_value = Path("fake_file.json")

        # Мокаем загрузку данных
        mock_load.return_value = {
            'issued_packages': 100,
            'pvz_info': 'Test PVZ'
        }

        result = load_single_report(config, "2026-01-05", "testpvz")

        assert result is not None
        assert result['_report_type'] == 'giveout'
        assert result['_report_date'] == '2026-01-05'
        assert result['_pvz_id'] == 'testpvz'
        assert result['issued_packages'] == 100


def test_load_single_report_not_found():
    """Тест загрузки несуществующего отчета."""
    config = ReportConfig(
        report_type='nonexistent',
        file_pattern='nonexistent_{date}.json'
    )
    
    result = load_single_report(config, "2026-01-05", "testpvz")
    
    assert result is None


def test_load_single_report_disabled():
    """Тест загрузки отключенного отчета."""
    config = ReportConfig(
        report_type='disabled',
        file_pattern='any_pattern.json',
        enabled=False
    )
    
    result = load_single_report(config, "2026-01-05", "testpvz")
    
    assert result is None


def test_merge_reports_data():
    """Тест объединения данных отчетов."""
    reports = [
        {'a': 1, 'b': 2, '_report_type': 'type1'},
        {'b': 3, 'c': 4, '_report_type': 'type2'},  # b будет перезаписано (последнее значение)
        {'c': 5, 'd': 6, '_report_type': 'type3'}   # c будет перезаписано
    ]
    
    result = merge_reports_data(reports)
    
    expected = {
        'a': 1,
        'b': 3,  # последнее значение
        'c': 5,  # последнее значение
        'd': 6,
        '_report_type': 'type3'  # последнее значение
    }
    
    assert result == expected


def test_merge_reports_data_empty():
    """Тест объединения пустого списка."""
    result = merge_reports_data([])
    
    assert result == {}


def test_load_reports_data_with_mocked_files():
    """Тест загрузки данных с замоканными файлами."""
    with patch('scheduler_runner.tasks.reports.utils.load_reports_data.find_report_file') as mock_find, \
         patch('scheduler_runner.tasks.reports.utils.load_reports_data.load_json_file') as mock_load:
        
        # Мокаем поиск файла
        mock_find.return_value = Path("fake_file.json")
        
        # Мокаем загрузку данных
        mock_load.return_value = {
            'issued_packages': 100,
            'pvz_info': 'Test PVZ'
        }
        
        result = load_reports_data(
            report_date="2026-01-05",
            pvz_id="Test PVZ"
        )
        
        assert result is not None
        assert result['issued_packages'] == 100
        assert result['pvz_info'] == 'Test PVZ'
        assert result['_report_date'] == '2026-01-05'
        assert result['_pvz_id'] == 'Test PVZ'


def test_load_reports_data_no_files_found():
    """Тест загрузки данных когда файлы не найдены."""
    with patch('scheduler_runner.tasks.reports.utils.load_reports_data.find_report_file') as mock_find:
        # Мокаем поиск файла возвращающим None
        mock_find.return_value = None

        result = load_reports_data(
            report_date="2026-01-05",
            pvz_id="Test PVZ"
        )

        # Должно вернуться пустое значение, но не должно быть ошибки
        assert result is not None
        # Даже если файлы не найдены, все равно должна быть метаинформация
        assert '_loaded_at' in result
        assert '_reports_loaded' in result


def test_load_reports_data_invalid_date():
    """Тест загрузки с невалидной датой."""
    try:
        result = load_reports_data(
            report_date="invalid-date",
            pvz_id="Test PVZ"
        )
        assert False, "Должна быть выброшена ошибка ValueError"
    except ValueError:
        pass  # Ожидаем ValueError


def test_load_reports_data_custom_config():
    """Тест загрузки с кастомной конфигурацией."""
    custom_config = [
        ReportConfig(
            report_type='custom',
            file_pattern='custom_{date}.json',
            fields_mapping={'custom_field': 'renamed_field'}
        )
    ]
    
    with patch('scheduler_runner.tasks.reports.utils.load_reports_data.find_report_file') as mock_find, \
         patch('scheduler_runner.tasks.reports.utils.load_reports_data.load_json_file') as mock_load:
        
        mock_find.return_value = Path("custom_file.json")
        mock_load.return_value = {
            'custom_field': 'custom_value',
            'other_field': 'other_value'
        }
        
        result = load_reports_data(
            report_date="2026-01-05",
            pvz_id="Test PVZ",
            config=custom_config
        )
        
        assert result is not None
        # Проверяем, что поле было переименовано
        assert 'renamed_field' in result
        assert result['renamed_field'] == 'custom_value'


def test_load_reports_data_empty_config():
    """Тест загрузки с пустой конфигурацией."""
    result = load_reports_data(
        report_date="2026-01-05",
        pvz_id="Test PVZ",
        config=[]
    )

    assert '_loaded_at' in result
    assert '_reports_loaded' in result
    assert result['_reports_loaded'] == []


def test_load_single_report_with_mapping():
    """Тест загрузки отчета с маппингом полей."""
    config = ReportConfig(
        report_type='test',
        file_pattern='test_{date}.json',
        fields_mapping={
            'old_name': 'new_name',
            'count': 'total_count'
        }
    )
    
    with patch('scheduler_runner.tasks.reports.utils.load_reports_data.find_report_file') as mock_find, \
         patch('scheduler_runner.tasks.reports.utils.load_reports_data.load_json_file') as mock_load:
        
        mock_find.return_value = Path("test_file.json")
        mock_load.return_value = {
            'old_name': 'value1',
            'count': 42,
            'unchanged': 'value2'
        }
        
        result = load_single_report(config, "2026-01-05", "Test PVZ")
        
        assert result is not None
        assert result['new_name'] == 'value1'  # переименовано
        assert result['total_count'] == 42     # переименовано
        assert result['unchanged'] == 'value2' # без изменений
        assert 'old_name' not in result        # старое имя не должно быть
        assert 'count' not in result          # старое имя не должно быть


def test_load_reports_data_default_date():
    """Тест загрузки с датой по умолчанию."""
    # Тестируем, что если дата не указана, используется текущая
    with patch('scheduler_runner.tasks.reports.utils.load_reports_data.datetime') as mock_datetime, \
         patch('scheduler_runner.tasks.reports.utils.load_reports_data.find_report_file') as mock_find, \
         patch('scheduler_runner.tasks.reports.utils.load_reports_data.load_json_file') as mock_load:
        
        # Мокаем текущую дату
        mock_now = MagicMock()
        mock_now.strftime.return_value = '2026-01-06'
        mock_datetime.now.return_value = mock_now
        mock_datetime.strptime = lambda x, y: True  # для валидации формата
        
        mock_find.return_value = Path("test_file.json")
        mock_load.return_value = {'test': 'data'}
        
        result = load_reports_data(pvz_id="Test PVZ")
        
        # Проверяем, что использовалась дата по умолчанию
        assert result['_report_date'] == '2026-01-06'