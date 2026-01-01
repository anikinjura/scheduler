"""
test_google_sheets.py

Тестовый скрипт для проверки подключения к Google Sheets.
"""
__version__ = '0.0.1'

import sys
from pathlib import Path

# Добавляем путь к проекту
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_google_sheets_connection():
    """Тест подключения к Google Sheets"""
    try:
        from scheduler_runner.tasks.reports.google_sheets.GoogleSheetsReporter import GoogleSheetsReporter
        from scheduler_runner.tasks.reports.google_sheets.config.google_sheets_config import GOOGLE_SHEETS_CONFIG
        
        print("Попытка подключения к Google Sheets...")
        
        # Создаем экземпляр GoogleSheetsReporter
        reporter = GoogleSheetsReporter(
            credentials_path=GOOGLE_SHEETS_CONFIG["CREDENTIALS_PATH"],
            spreadsheet_name=GOOGLE_SHEETS_CONFIG["SPREADSHEET_NAME"],
            worksheet_name=GOOGLE_SHEETS_CONFIG["WORKSHEET_NAME"]
        )
        
        print("Подключение успешно установлено!")
        
        # Получаем заголовки
        headers = reporter.get_headers()
        print(f"Заголовки столбцов: {headers}")
        
        # Получаем последнюю строку с данными
        last_row = reporter.get_last_row_with_data()
        print(f"Последняя строка с данными: {last_row}")
        
        return True
        
    except ImportError as e:
        print(f"Ошибка импорта: {e}")
        return False
    except FileNotFoundError:
        print("Файл с ключами сервисного аккаунта не найден")
        print(f"Ожидаемый путь: {GOOGLE_SHEETS_CONFIG['CREDENTIALS_PATH']}")
        return False
    except Exception as e:
        print(f"Ошибка подключения к Google Sheets: {e}")
        return False

if __name__ == "__main__":
    success = test_google_sheets_connection()
    if success:
        print("Тест подключения к Google Sheets прошел успешно!")
    else:
        print("Тест подключения к Google Sheets завершился с ошибкой.")