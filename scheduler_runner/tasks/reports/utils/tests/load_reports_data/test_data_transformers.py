"""
test_data_transformers.py

Тесты для трансформеров данных.
"""

from scheduler_runner.tasks.reports.utils.data_transformers import (
    GoogleSheetsTransformer
)


def test_google_sheets_transformer_basic():
    """Тест базового преобразования для Google Sheets."""
    transformer = GoogleSheetsTransformer()
    
    raw_data = {
        'issued_packages': 100,
        'total_packages': 150,
        'direct_flow_count': 50,
        'pvz_info': 'Москва ПВЗ',
        '_report_date': '2026-01-05'
    }
    
    result = transformer.transform(raw_data)
    
    expected = {
        'id': '',  # будет заполнен формулой
        'Дата': '05.01.2026',  # формат даты изменен
        'ПВЗ': 'Москва ПВЗ',
        'Количество выдач': 100,
        'Прямой поток': 50,
        'Возвратный поток': 0  # не найдено в данных
    }
    
    assert result == expected


def test_google_sheets_transformer_with_direct_flow_data():
    """Тест преобразования с данными прямого потока."""
    transformer = GoogleSheetsTransformer()
    
    raw_data = {
        'direct_flow_data': {
            'total_items_count': 75
        },
        'pvz_info': 'СПб ПВЗ',
        '_report_date': '2026-02-15'
    }
    
    result = transformer.transform(raw_data)
    
    assert result['Прямой поток'] == 75
    assert result['Дата'] == '15.02.2026'
    assert result['ПВЗ'] == 'СПб ПВЗ'


def test_google_sheets_transformer_with_return_flow_data():
    """Тест преобразования с данными возвратного потока."""
    transformer = GoogleSheetsTransformer()
    
    raw_data = {
        'return_flow_data': {
            'total_items_count': 30
        },
        '_report_date': '2026-03-20'
    }
    
    result = transformer.transform(raw_data)
    
    assert result['Возвратный поток'] == 30


def test_google_sheets_transformer_missing_fields():
    """Тест преобразования с отсутствующими полями."""
    transformer = GoogleSheetsTransformer()
    
    raw_data = {
        '_report_date': '2026-01-10'
    }
    
    result = transformer.transform(raw_data)
    
    expected = {
        'id': '',
        'Дата': '10.01.2026',
        'ПВЗ': '',  # пустая строка если pvz_info нет
        'Количество выдач': 0,  # 0 если issued_packages нет
        'Прямой поток': 0,      # 0 если direct_flow нет
        'Возвратный поток': 0   # 0 если return_flow нет
    }
    
    assert result == expected


def test_google_sheets_transformer_prefer_issued_packages():
    """Тест приоритета issued_packages над total_packages."""
    transformer = GoogleSheetsTransformer()
    
    raw_data = {
        'issued_packages': 200,
        'total_packages': 300,  # должен быть проигнорирован
        '_report_date': '2026-01-05'
    }
    
    result = transformer.transform(raw_data)
    
    assert result['Количество выдач'] == 200  # issued_packages имеет приоритет


def test_google_sheets_transformer_fallback_to_total_packages():
    """Тест fallback на total_packages если issued_packages нет."""
    transformer = GoogleSheetsTransformer()
    
    raw_data = {
        'total_packages': 250,  # должен быть использован
        '_report_date': '2026-01-05'
    }
    
    result = transformer.transform(raw_data)
    
    assert result['Количество выдач'] == 250


def test_format_date_valid():
    """Тест форматирования валидной даты."""
    transformer = GoogleSheetsTransformer()
    
    result = transformer._format_date('2026-01-15')
    
    assert result == '15.01.2026'


def test_format_date_invalid():
    """Тест форматирования невалидной даты."""
    transformer = GoogleSheetsTransformer()
    
    result = transformer._format_date('invalid-date')
    
    assert result == 'invalid-date'  # возвращается как есть


def test_format_date_empty():
    """Тест форматирования пустой даты."""
    transformer = GoogleSheetsTransformer()
    
    result = transformer._format_date('')
    
    assert result == ''


def test_format_date_none():
    """Тест форматирования None."""
    transformer = GoogleSheetsTransformer()
    
    result = transformer._format_date(None)
    
    assert result == ''


def test_extract_direct_flow_with_count():
    """Тест извлечения прямого потока с direct_flow_count."""
    transformer = GoogleSheetsTransformer()
    
    raw_data = {
        'direct_flow_count': 60
    }
    
    result = transformer._extract_direct_flow(raw_data)
    
    assert result == 60


def test_extract_direct_flow_with_data():
    """Тест извлечения прямого потока с direct_flow_data."""
    transformer = GoogleSheetsTransformer()
    
    raw_data = {
        'direct_flow_data': {
            'total_items_count': 45
        }
    }
    
    result = transformer._extract_direct_flow(raw_data)
    
    assert result == 45


def test_extract_direct_flow_prefer_count():
    """Тест приоритета direct_flow_count над direct_flow_data."""
    transformer = GoogleSheetsTransformer()
    
    raw_data = {
        'direct_flow_count': 80,
        'direct_flow_data': {
            'total_items_count': 40  # должен быть проигнорирован
        }
    }
    
    result = transformer._extract_direct_flow(raw_data)
    
    assert result == 80  # direct_flow_count имеет приоритет


def test_extract_direct_flow_fallback_to_data():
    """Тест fallback на direct_flow_data если direct_flow_count нет."""
    transformer = GoogleSheetsTransformer()
    
    raw_data = {
        'direct_flow_data': {
            'total_items_count': 35
        }
    }
    
    result = transformer._extract_direct_flow(raw_data)
    
    assert result == 35


def test_extract_direct_flow_no_data():
    """Тест извлечения прямого потока без данных."""
    transformer = GoogleSheetsTransformer()
    
    raw_data = {}
    
    result = transformer._extract_direct_flow(raw_data)
    
    assert result == 0


def test_extract_return_flow_with_data():
    """Тест извлечения возвратного потока."""
    transformer = GoogleSheetsTransformer()
    
    raw_data = {
        'return_flow_data': {
            'total_items_count': 20
        }
    }
    
    result = transformer._extract_return_flow(raw_data)
    
    assert result == 20


def test_extract_return_flow_no_data():
    """Тест извлечения возвратного потока без данных."""
    transformer = GoogleSheetsTransformer()
    
    raw_data = {}
    
    result = transformer._extract_return_flow(raw_data)
    
    assert result == 0


def test_extract_return_flow_invalid_structure():
    """Тест извлечения возвратного потока с невалидной структурой."""
    transformer = GoogleSheetsTransformer()
    
    raw_data = {
        'return_flow_data': 'not_a_dict'  # не словарь
    }
    
    result = transformer._extract_return_flow(raw_data)
    
    assert result == 0


def test_google_sheets_transformer_complete_data():
    """Тест преобразования полных данных."""
    transformer = GoogleSheetsTransformer()
    
    raw_data = {
        'issued_packages': 120,
        'total_packages': 180,
        'direct_flow_count': 65,
        'direct_flow_data': {
            'total_items_count': 40
        },
        'return_flow_data': {
            'total_items_count': 25
        },
        'pvz_info': 'Екатеринбург ПВЗ',
        '_report_date': '2026-04-10',
        '_pvz_id': 'ekat_pvz'
    }
    
    result = transformer.transform(raw_data)
    
    # issued_packages имеет приоритет над total_packages
    assert result['Количество выдач'] == 120
    
    # direct_flow_count имеет приоритет над direct_flow_data
    assert result['Прямой поток'] == 65
    
    # return_flow_data используется как есть
    assert result['Возвратный поток'] == 25
    
    # дата форматируется правильно
    assert result['Дата'] == '10.04.2026'
    
    # ПВЗ берется из pvz_info
    assert result['ПВЗ'] == 'Екатеринбург ПВЗ'
    
    # id остается пустым
    assert result['id'] == ''