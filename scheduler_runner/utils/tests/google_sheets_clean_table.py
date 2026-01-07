"""
Скрипт для очистки Google-таблицы от неполных записей
"""
import sys
from pathlib import Path

# Добавляем корень проекта в sys.path для корректного импорта
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))  # Поднимаемся на 3 уровня вверх до корня проекта

from scheduler_runner.utils.google_sheets import GoogleSheetsReporter
from scheduler_runner.tasks.reports.config.scripts.GoogleSheetsUploadScript_config import (
    GOOGLE_CREDENTIALS_PATH, 
    SPREADSHEET_NAME, 
    WORKSHEET_NAME
)


def clean_table():
    """Очищает таблицу от неполных записей"""
    print("=== Очистка Google-таблицы от неполных записей ===")
    
    # Подключаемся к таблице
    print("Подключение к Google-таблице...")
    try:
        reporter = GoogleSheetsReporter(
            credentials_path=GOOGLE_CREDENTIALS_PATH,
            spreadsheet_name=SPREADSHEET_NAME,
            worksheet_name=WORKSHEET_NAME
        )
        print("Подключение успешно")
    except Exception as e:
        print(f"Ошибка подключения: {e}")
        return
    
    # Получаем заголовки
    try:
        headers = reporter.worksheet.row_values(1)
        expected_columns = len(headers)
        print(f"Заголовки: {headers}")
        print(f"Ожидаемое количество колонок: {expected_columns}")
    except Exception as e:
        print(f"Ошибка чтения заголовков: {e}")
        return
    
    # Получаем последнюю строку с данными
    try:
        last_row = reporter.get_last_row_with_data()
        print(f"Последняя строка с данными: {last_row}")
    except Exception as e:
        print(f"Ошибка получения последней строки: {e}")
        return
    
    # Находим неполные строки и очищаем их
    incomplete_rows = []
    
    for row_num in range(2, last_row + 1):  # Начинаем с 2, пропускаем заголовки
        try:
            row_values = reporter.worksheet.row_values(row_num)
            if len(row_values) < expected_columns:
                incomplete_rows.append((row_num, row_values))
                print(f"Найдена неполная строка {row_num}: {row_values} (колонок: {len(row_values)})")
        except Exception as e:
            print(f"Ошибка чтения строки {row_num}: {e}")
    
    if incomplete_rows:
        print(f"\nНайдено {len(incomplete_rows)} неполных строк")
        
        # Очищаем неполные строки (устанавливаем пустые значения для всех колонок)
        for row_num, row_values in incomplete_rows:
            try:
                # Создаем пустые значения для всех колонок
                empty_values = [["" for _ in range(expected_columns)]]
                range_name = f"A{row_num}:{chr(64 + expected_columns)}{row_num}"  # A1 notation for the row
                
                reporter.worksheet.update(
                    values=empty_values,
                    range_name=range_name
                )
                print(f"Строка {row_num} очищена")
            except Exception as e:
                print(f"Ошибка очистки строки {row_num}: {e}")
        
        print(f"\nОчистка завершена. Неполные строки очищены.")
    else:
        print("Неполных строк не найдено.")
    
    # Проверяем результат
    print("\n=== Проверка результата ===")
    last_row_after = reporter.get_last_row_with_data()
    print(f"Последняя строка с данными после очистки: {last_row_after}")
    
    # Показываем несколько последних строк
    start_row = max(2, last_row_after - 5)  # Читаем последние 5 строк или с 2-й строки
    for row_num in range(start_row, last_row_after + 1):
        try:
            row_values = reporter.worksheet.row_values(row_num)
            if row_values:  # Показываем только непустые строки
                print(f"Строка {row_num}: {row_values}")
        except Exception as e:
            print(f"Ошибка чтения строки {row_num}: {e}")


def main():
    clean_table()


if __name__ == "__main__":
    main()