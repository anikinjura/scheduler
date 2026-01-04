"""
Тестовый скрипт для проверки OzonCarriagesReportScript.py

Этот скрипт проверяет, что основной скрипт может быть импортирован и запущен без ошибок.
"""
import sys
from pathlib import Path

# Добавляем путь к корню проекта для импорта модулей
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_import():
    """Тестируем импорт основного модуля"""
    try:
        from scheduler_runner.tasks.reports.OzonCarriagesReportScript import OzonCarriagesReportParser
        from scheduler_runner.tasks.reports.config.scripts.OzonCarriagesReportScript_config import SCRIPT_CONFIG
        from scheduler_runner.utils.logging import configure_logger
        print("+ Импорт модулей прошел успешно")
        return True
    except Exception as e:
        print(f"- Ошибка импорта: {e}")
        return False

def test_logger():
    """Тестируем создание логгера"""
    try:
        from scheduler_runner.utils.logging import configure_logger
        logger = configure_logger(
            user="test_user",
            task_name="test_task",
            detailed=True
        )
        logger.info("Тестовое сообщение логгера")
        print("+ Создание логгера прошло успешно")
        return True
    except Exception as e:
        print(f"- Ошибка создания логгера: {e}")
        return False

def test_parser_creation():
    """Тестируем создание экземпляра парсера с логгером"""
    try:
        from scheduler_runner.tasks.reports.OzonCarriagesReportScript import OzonCarriagesReportParser
        from scheduler_runner.tasks.reports.config.scripts.OzonCarriagesReportScript_config import SCRIPT_CONFIG
        from scheduler_runner.utils.logging import configure_logger

        logger = configure_logger(
            user="test_user",
            task_name="test_task",
            detailed=True
        )

        # Создаем экземпляр парсера с логгером (как в обновленной версии)
        parser = OzonCarriagesReportParser(SCRIPT_CONFIG, logger)
        print("+ Создание экземпляра парсера с логгером прошло успешно")
        return True
    except Exception as e:
        print(f"- Ошибка создания экземпляра парсера: {e}")
        return False

def test_parser_creation_without_logger():
    """Тестируем создание экземпляра парсера без логгера (для обратной совместимости)"""
    try:
        from scheduler_runner.tasks.reports.OzonCarriagesReportScript import OzonCarriagesReportParser
        from scheduler_runner.tasks.reports.config.scripts.OzonCarriagesReportScript_config import SCRIPT_CONFIG

        # Создаем экземпляр парсера без логгера (для проверки обратной совместимости)
        parser = OzonCarriagesReportParser(SCRIPT_CONFIG)
        print("+ Создание экземпляра парсера без логгера прошло успешно")
        return True
    except Exception as e:
        print(f"- Ошибка создания экземпляра парсера без логгера: {e}")
        return False

def test_script_execution():
    """Тестируем выполнение основного скрипта с имитацией работы"""
    try:
        from scheduler_runner.tasks.reports.OzonCarriagesReportScript import OzonCarriagesReportParser
        from scheduler_runner.tasks.reports.config.scripts.OzonCarriagesReportScript_config import SCRIPT_CONFIG
        from scheduler_runner.utils.logging import configure_logger

        logger = configure_logger(
            user="test_user",
            task_name="test_task",
            detailed=True
        )

        # Создаем экземпляр парсера
        parser = OzonCarriagesReportParser(SCRIPT_CONFIG, logger)

        # Проверяем, что методы существуют и могут быть вызваны (без реального выполнения)
        # (мы не будем вызывать setup_driver, т.к. это потребует реальный браузер)

        # Проверим, что методы существуют
        assert hasattr(parser, 'login'), "Метод login отсутствует"
        assert hasattr(parser, 'extract_data'), "Метод extract_data отсутствует"
        assert hasattr(parser, 'process_flow_type'), "Метод process_flow_type отсутствует"
        assert hasattr(parser, 'logout'), "Метод logout отсутствует"

        print("+ Проверка наличия методов выполнена успешно")

        # Проверим, что конфигурация доступна
        assert hasattr(parser, 'config'), "Конфигурация недоступна"
        assert 'ERP_URL' in parser.config, "ERP_URL отсутствует в конфигурации"

        print("+ Проверка конфигурации выполнена успешно")

        return True
    except Exception as e:
        print(f"- Ошибка выполнения скрипта: {e}")
        import traceback
        print(f"  Трассировка стека: {traceback.format_exc()}")
        return False

def test_main_function():
    """Тестируем вызов основной функции main() с мокированием"""
    try:
        import sys
        from unittest.mock import patch, MagicMock

        # Мокируем setup_driver, чтобы избежать запуска реального браузера
        with patch('scheduler_runner.tasks.reports.OzonCarriagesReportScript.OzonCarriagesReportParser.setup_driver') as mock_setup_driver, \
             patch('scheduler_runner.tasks.reports.OzonCarriagesReportScript.OzonCarriagesReportParser.login') as mock_login, \
             patch('scheduler_runner.tasks.reports.OzonCarriagesReportScript.OzonCarriagesReportParser.extract_data') as mock_extract_data, \
             patch('scheduler_runner.tasks.reports.OzonCarriagesReportScript.OzonCarriagesReportParser.logout') as mock_logout, \
             patch('scheduler_runner.tasks.reports.OzonCarriagesReportScript.OzonCarriagesReportParser.close') as mock_close:

            # Подготовим возвращаемые значения для моков
            mock_extract_data.return_value = {
                'marketplace': 'Ozon',
                'report_type': 'carriages_test',
                'date': '2023-01-01',
                'timestamp': '2023-01-01T00:00:00',
                'flow_type': 'Test',
                'test_flow': {
                    'total_carriages_found': 0,
                    'carriage_numbers': [],
                    'carriage_details': [],
                    'total_items_count': 0
                },
                'pvz_info': 'TEST_PVZ'
            }

            # Мокируем аргументы командной строки
            with patch.object(sys, 'argv', ['test', '--detailed_logs']):
                # Импортируем и вызываем parse_arguments, чтобы проверить, что они работают
                from scheduler_runner.tasks.reports.OzonCarriagesReportScript import parse_arguments
                args = parse_arguments()
                # Проверяем, что аргументы парсятся корректно
                print(f"+ Аргументы командной строки успешно спарсены: detailed_logs={args.detailed_logs}")

                # Вызываем main функцию в try-except блоке, чтобы перехватить SystemExit
                try:
                    from scheduler_runner.tasks.reports.OzonCarriagesReportScript import main
                    main()
                except SystemExit:
                    # Это нормально для main функции, если она завершает работу
                    pass  # Просто продолжаем выполнение теста

                # Проверяем, что моки были вызваны (после выполнения main)
                if mock_setup_driver.called:
                    print("+ setup_driver был вызван")
                else:
                    print("- setup_driver не был вызван")

                if mock_login.called:
                    print("+ login был вызван")
                else:
                    print("- login не был вызван")

                if mock_extract_data.called:
                    print("+ extract_data был вызван")
                else:
                    print("- extract_data не был вызван")

                if mock_logout.called:
                    print("+ logout был вызван")
                else:
                    print("- logout не был вызван")

                if mock_close.called:
                    print("+ close был вызван")
                else:
                    print("- close не был вызван")

                # Возвращаем True, если хотя бы основные моки были вызваны
                success = mock_setup_driver.called and mock_login.called and mock_close.called
                if success:
                    print("+ Проверка вызова основных методов выполнена успешно")
                    return True
                else:
                    print("- Не все основные методы были вызваны")
                    return False
    except Exception as e:
        print(f"- Ошибка тестирования main функции: {e}")
        import traceback
        print(f"  Трассировка стека: {traceback.format_exc()}")
        return False

def main():
    """Основная функция тестирования"""
    print("Запуск тестирования OzonCarriagesReportScript...")
    print("="*50)

    tests = [
        test_import,
        test_logger,
        test_parser_creation,
        test_parser_creation_without_logger,
        test_script_execution,
        test_main_function
    ]

    results = []
    for test in tests:
        result = test()
        results.append(result)
        print()

    print("="*50)
    print(f"Результаты: {sum(results)}/{len(results)} тестов пройдено успешно")

    if all(results):
        print("+ Все тесты пройдены успешно! Основной скрипт работает корректно.")
        return True
    else:
        print("- Некоторые тесты не пройдены. Проверьте ошибки выше.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)