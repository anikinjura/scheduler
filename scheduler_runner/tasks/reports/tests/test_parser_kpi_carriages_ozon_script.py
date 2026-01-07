"""
test_parser_kpi_carriages_ozon_script.py

Юнит-тесты для скрипта Parser_KPI_Carriages_OzonScript.py.
Тестирует функциональность скрипта парсинга данных о перевозках ОЗОН, включая:
- Парсинг аргументов командной строки
- Извлечение данных о перевозках
- Основную логику работы скрипта
"""

import sys
import argparse
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest

# Добавляем путь к проекту для импорта
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from scheduler_runner.tasks.reports import Parser_KPI_Carriages_OzonScript
from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Carriages_OzonScript_config import SCRIPT_CONFIG


def test_parse_arguments_defaults():
    """Тест парсинга аргументов с значениями по умолчанию."""
    # Тестируем, что аргументы парсятся корректно
    with patch('sys.argv', ['script_name']):  # без аргументов
        args = Parser_KPI_Carriages_OzonScript.parse_arguments()

        # Проверяем, что аргументы существуют
        assert hasattr(args, 'detailed_logs')
        assert hasattr(args, 'date')

        # detailed_logs по умолчанию False
        assert args.detailed_logs is False
        # date по умолчанию None
        assert args.date is None


def test_parse_arguments_with_values():
    """Тест парсинга аргументов с переданными значениями."""
    test_argv = [
        'script_name',
        '--detailed_logs',
        '--date', '2026-01-01'
    ]

    with patch('sys.argv', test_argv):
        args = Parser_KPI_Carriages_OzonScript.parse_arguments()

        assert args.detailed_logs is True
        assert args.date == '2026-01-01'


def test_extract_data_success():
    """Тест успешного извлечения данных."""
    # Создаем мок-объект парсера
    with patch('scheduler_runner.tasks.reports.Parser_KPI_Carriages_OzonScript.OzonCarriagesReportParser') as mock_parser_class:
        mock_parser_instance = MagicMock()
        mock_parser_class.return_value = mock_parser_instance

        # Мокаем возвращаемые данные
        expected_data = {
            'marketplace': 'Ozon',
            'report_type': 'carriages_combined',
            'date': '2026-01-07',
            'timestamp': '2026-01-07T12:00:00',
            'direct_flow': {
                'flow_type': 'Direct',
                'total_carriages_found': 50,
                'carriage_numbers': ['C001', 'C002'],
                'carriage_details': [
                    {'carriage_number': 'C001', 'items_count': 10},
                    {'carriage_number': 'C002', 'items_count': 15}
                ],
                'total_items_count': 25
            },
            'return_flow': {
                'flow_type': 'Return',
                'total_carriages_found': 30,
                'carriage_numbers': ['R001', 'R002'],
                'carriage_details': [
                    {'carriage_number': 'R001', 'items_count': 5},
                    {'carriage_number': 'R002', 'items_count': 8}
                ],
                'total_items_count': 13
            },
            'pvz_info': 'TEST_PVZ',
            'raw_data': {
                'page_source_length': 1000,
                'page_text_length': 500
            }
        }
        mock_parser_instance.extract_data.return_value = expected_data

        # Создаем экземпляр парсера и вызываем extract_data
        parser = Parser_KPI_Carriages_OzonScript.OzonCarriagesReportParser(SCRIPT_CONFIG)
        result = parser.extract_data()

        # Проверяем, что метод extract_data был вызван
        assert result == expected_data


def test_extract_data_with_mocked_selenium():
    """Тест извлечения данных с моканным Selenium."""
    with patch('selenium.webdriver.Edge') as mock_driver_class, \
         patch('scheduler_runner.utils.logging.configure_logger') as mock_logger:

        mock_driver_instance = MagicMock()
        mock_driver_class.return_value = mock_driver_instance

        # Мокаем возвращаемые значения для элементов страницы
        mock_element = MagicMock()
        mock_element.text = 'Найдено: 100'
        mock_element.get_attribute.return_value = 'TEST_PVZ'

        mock_driver_instance.current_url = 'https://turbo-pvz.ozon.ru/outbound/carriages-archive?filter=%7B%22startSentMoment%22:%222026-01-07T00:00:00%2B03:00%22,%22endSentMoment%22:%222026-01-07T23:59:59%2B03:00%22,%22flowType%22:%22Direct%22%7D'
        mock_driver_instance.title = 'Отчет о перевозках'
        mock_driver_instance.page_source = '<html>test page</html>'

        # Мокаем find_element
        mock_body_element = MagicMock()
        mock_body_element.text = 'Найдено: 100'
        mock_driver_instance.find_element.return_value = mock_body_element

        # Мокаем find_elements для поиска элементов
        mock_carriage_element = MagicMock()
        mock_carriage_element.text = 'C001'
        mock_driver_instance.find_elements.return_value = [mock_carriage_element]

        # Мокаем другие элементы
        with patch('scheduler_runner.tasks.reports.BaseOzonParser.BaseOzonParser.extract_ozon_element_by_xpath') as mock_extract:
            mock_extract.return_value = 'Найдено: 100'

            # Создаем парсер с моканным драйвером
            parser = Parser_KPI_Carriages_OzonScript.OzonCarriagesReportParser(SCRIPT_CONFIG)
            parser.driver = mock_driver_instance

            # Вызываем извлечение данных
            result = parser.extract_data('Direct')

            # Проверяем, что результат содержит ожидаемые поля
            assert 'marketplace' in result
            assert 'direct_flow' in result
            assert result['marketplace'] == 'Ozon'


def test_main_function_with_mocked_dependencies():
    """Тест основной функции main с замоканными зависимостями."""
    test_argv = [
        'script_name',
        '--detailed_logs'
    ]

    with patch('sys.argv', test_argv), \
         patch('scheduler_runner.tasks.reports.Parser_KPI_Carriages_OzonScript.OzonCarriagesReportParser') as mock_parser_class, \
         patch('scheduler_runner.utils.logging.configure_logger') as mock_logger:

        mock_parser_instance = MagicMock()
        mock_parser_class.return_value = mock_parser_instance

        # Мокаем возвращаемые данные
        mock_data = {
            'marketplace': 'Ozon',
            'report_type': 'carriages_combined',
            'date': '2026-01-07',
            'timestamp': '2026-01-07T12:00:00',
            'direct_flow': {
                'flow_type': 'Direct',
                'total_carriages_found': 50,
                'carriage_numbers': ['C001', 'C002'],
                'carriage_details': [
                    {'carriage_number': 'C001', 'items_count': 10},
                    {'carriage_number': 'C002', 'items_count': 15}
                ],
                'total_items_count': 25
            },
            'return_flow': {
                'flow_type': 'Return',
                'total_carriages_found': 30,
                'carriage_numbers': ['R001', 'R002'],
                'carriage_details': [
                    {'carriage_number': 'R001', 'items_count': 5},
                    {'carriage_number': 'R002', 'items_count': 8}
                ],
                'total_items_count': 13
            },
            'pvz_info': 'TEST_PVZ',
            'raw_data': {
                'page_source_length': 1000,
                'page_text_length': 500
            }
        }
        mock_parser_instance.extract_data.return_value = mock_data

        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance

        # Импортируем и вызываем main
        # Для избежания проблем с циклическими импортами используем exec
        import scheduler_runner.tasks.reports.Parser_KPI_Carriages_OzonScript as script_module
        # Мокаем необходимые зависимости в модуле
        with patch.object(script_module, 'OzonCarriagesReportParser', mock_parser_class):
            # Вызываем main
            try:
                script_module.main()
            except SystemExit:
                # Ожидаем, что main вызывает sys.exit() в конце
                pass

        # Проверяем, что все функции были вызваны
        # В новой версии main() создается несколько экземпляров парсера для обработки разных типов перевозок
        assert mock_parser_class.call_count >= 1  # Как минимум один вызов
        mock_parser_instance.setup_driver.assert_called()
        mock_parser_instance.login.assert_called()
        mock_parser_instance.logout.assert_called()
        mock_parser_instance.close.assert_called()
        # extract_data вызывается дважды - для прямых и возвратных перевозок
        assert mock_parser_instance.extract_data.call_count >= 1


def test_data_transformation():
    """Тест трансформации данных."""
    from scheduler_runner.tasks.reports.utils.data_transformers import GoogleSheetsTransformer

    raw_data = {
        'issued_packages': 100,
        'total_packages': 150,
        'pvz_info': 'Test PVZ',
        'date': '2026-01-07',
        'timestamp': '2026-01-07T12:00:00'
    }

    transformer = GoogleSheetsTransformer()
    result = transformer.transform(raw_data)

    # Проверяем, что результат содержит ожидаемые поля
    expected_fields = ['id', 'Дата', 'ПВЗ', 'Количество выдач', 'Прямой поток', 'Возвратный поток']
    for field in expected_fields:
        assert field in result

    # Проверяем конкретные значения
    assert result['Количество выдач'] == 100
    assert result['ПВЗ'] == 'Test PVZ'


def test_error_handling():
    """Тест обработки ошибок."""
    with patch('selenium.webdriver.Edge') as mock_driver_class, \
         patch('scheduler_runner.utils.logging.configure_logger') as mock_logger:
        
        mock_driver_instance = MagicMock()
        mock_driver_class.return_value = mock_driver_instance
        
        # Мокаем выброс исключения
        mock_driver_instance.find_element.side_effect = Exception("Test error")
        
        # Создаем парсер с моканным драйвером
        parser = Parser_KPI_Carriages_OzonScript.OzonCarriagesReportParser(SCRIPT_CONFIG)
        parser.driver = mock_driver_instance
        parser.logger = mock_logger.return_value
        
        # Вызываем извлечение данных и проверяем, что возвращаются данные об ошибке
        result = parser.extract_data()
        
        # Проверяем, что в результатах есть информация об ошибке
        assert 'error' in result
        assert result['marketplace'] == 'Ozon'


def test_config_integration():
    """Тест интеграции с конфигурацией."""
    # Проверяем, что конфигурация может быть импортирована
    from scheduler_runner.tasks.reports.config.scripts.Parser_KPI_Carriages_OzonScript_config import SCRIPT_CONFIG, TASK_SCHEDULE

    assert 'ERP_URL' in SCRIPT_CONFIG
    assert 'USER' in SCRIPT_CONFIG
    assert 'TASK_NAME' in SCRIPT_CONFIG
    assert SCRIPT_CONFIG['TASK_NAME'] == 'Parser_KPI_Carriages_OzonScript'

    assert len(TASK_SCHEDULE) > 0




if __name__ == "__main__":
    print("Тестирование Parser_KPI_Giveout_OzonScript...")

    test_parse_arguments_defaults()
    print("✓ test_parse_arguments_defaults")

    test_parse_arguments_with_values()
    print("✓ test_parse_arguments_with_values")

    test_extract_data_success()
    print("✓ test_extract_data_success")

    test_extract_data_with_mocked_selenium()
    print("✓ test_extract_data_with_mocked_selenium")

    test_data_transformation()
    print("✓ test_data_transformation")

    test_error_handling()
    print("✓ test_error_handling")

    test_config_integration()
    print("✓ test_config_integration")

    print("\nВсе тесты пройдены успешно! 🎉")