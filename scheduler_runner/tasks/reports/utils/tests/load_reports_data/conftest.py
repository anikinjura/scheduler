"""
conftest.py

Общие фикстуры для тестов load_reports_data.
"""

import pytest
from pathlib import Path
import tempfile
import json
from datetime import datetime
from unittest.mock import Mock, patch


@pytest.fixture
def temp_report_dir():
    """Создает временный каталог для тестовых файлов отчетов."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def sample_report_data():
    """Образцы данных отчетов для тестов."""
    return {
        'giveout': {
            'issued_packages': 100,
            'total_packages': 150,
            'pvz_info': 'Москва, ул. Примерная, 1',
            'marketplace': 'ОЗОН'
        },
        'direct_flow': {
            'total_items_count': 50,
            'pvz_info': 'Москва, ул. Примерная, 1',
            'marketplace': 'ОЗОН'
        },
        'carriages': {
            'direct_flow': {
                'total_items_count': 50,
                'status': 'completed'
            },
            'return_flow': {
                'total_items_count': 25,
                'status': 'pending'
            },
            'pvz_info': 'Москва, ул. Примерная, 1',
            'marketplace': 'ОЗОН'
        }
    }


@pytest.fixture
def create_test_files(temp_report_dir, sample_report_data):
    """Создает тестовые JSON файлы отчетов."""
    files_created = []
    
    # Создаем файл отчета по выдаче
    giveout_file = temp_report_dir / "ozon_giveout_report_testpvz_20260105.json"
    with open(giveout_file, 'w', encoding='utf-8') as f:
        json.dump(sample_report_data['giveout'], f, ensure_ascii=False, indent=2)
    files_created.append(giveout_file)
    
    # Создаем файл отчета по селлерским отправлениям
    direct_flow_file = temp_report_dir / "ozon_direct_flow_report_testpvz_20260105.json"
    with open(direct_flow_file, 'w', encoding='utf-8') as f:
        json.dump(sample_report_data['direct_flow'], f, ensure_ascii=False, indent=2)
    files_created.append(direct_flow_file)
    
    # Создаем файл отчета по перевозкам
    carriages_file = temp_report_dir / "ozon_carriages_report_20260105.json"
    with open(carriages_file, 'w', encoding='utf-8') as f:
        json.dump(sample_report_data['carriages'], f, ensure_ascii=False, indent=2)
    files_created.append(carriages_file)
    
    return {
        'directory': temp_report_dir,
        'files': files_created,
        'giveout_file': giveout_file,
        'direct_flow_file': direct_flow_file,
        'carriages_file': carriages_file
    }


@pytest.fixture
def mock_reports_paths(create_test_files):
    """Мок для REPORTS_PATHS."""
    with patch('scheduler_runner.tasks.reports.utils.load_reports_data.REPORTS_PATHS') as mock_paths:
        mock_paths.__getitem__.return_value = create_test_files['directory']
        yield mock_paths


@pytest.fixture
def mock_file_utils():
    """Мок для file_utils функций."""
    with patch('scheduler_runner.tasks.reports.utils.load_reports_data.find_report_file') as mock_find, \
         patch('scheduler_runner.tasks.reports.utils.load_reports_data.load_json_file') as mock_load:
        yield {
            'find_report_file': mock_find,
            'load_json_file': mock_load
        }