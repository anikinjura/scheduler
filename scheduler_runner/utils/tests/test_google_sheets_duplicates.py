"""
Скрипт для тестирования возврата всех дубликатов
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta
import time

# Добавляем корень проекта в sys.path для корректного импорта
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))  # Поднимаемся на 3 уровня вверх до корня проекта

from scheduler_runner.utils.google_sheets import GoogleSheetsReporter
from scheduler_runner.tasks.reports.config.scripts.GoogleSheetsUploadScript_config import (
    GOOGLE_CREDENTIALS_PATH,
    SPREADSHEET_NAME,
    WORKSHEET_NAME
)
from scheduler_runner.utils.google_sheets import (
    ColumnType,
    ColumnDefinition,
    TableConfig
)


def test_get_all_duplicates():
    """Тестирует возврат всех дубликатов"""
    print("=== Тестирование возврата всех дубликатов ===")

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

    # Подготовим тестовые данные
    print("Подготовка тестовых данных...")
    
    # Используем уникальную дату и ПВЗ
    unique_date = "30.12.2026"  # Дата, которой нет в существующих записях
    unique_pvz = "DUPLICATE_TEST_PVZ"
    
    test_data = {
        "Дата": unique_date,
        "ПВЗ": unique_pvz,
        "Количество выдач": "5555",
        "Прямой поток": "DUPLICATE_SELLER",
        "Возвратный поток": "555"
    }

    print(f"Тестовые данные для добавления: {test_data}")

    # Создаем конфигурацию таблицы
    table_config = TableConfig(
        worksheet_name=WORKSHEET_NAME,
        columns=[
            ColumnDefinition(
                name="id",
                column_type=ColumnType.FORMULA,
                formula_template="=B{row}&C{row}",  # Простая формула как в оригинальной системе
                unique_key=False
            ),
            ColumnDefinition(
                name="Дата",
                column_type=ColumnType.DATA,
                required=True,
                unique_key=True
            ),
            ColumnDefinition(
                name="ПВЗ",
                column_type=ColumnType.DATA,
                required=True,
                unique_key=True
            ),
            ColumnDefinition(
                name="Количество выдач",
                column_type=ColumnType.DATA,
                unique_key=False
            ),
            ColumnDefinition(
                name="Прямой поток",
                column_type=ColumnType.DATA,
                unique_key=False
            ),
            ColumnDefinition(
                name="Возвратный поток",
                column_type=ColumnType.DATA,
                unique_key=False
            )
        ],
        id_column="id",
        unique_key_columns=["Дата", "ПВЗ"],
        id_formula_template="=B{row}&C{row}"
    )

    # Добавим несколько строк с одинаковыми уникальными ключами
    print("\n=== Добавление строк с одинаковыми ключами ===")
    added_rows = []
    for i in range(3):
        try:
            result = reporter.update_or_append_data_with_config(
                data=test_data,
                config=table_config,
                strategy="append_only"
            )
            print(f"Результат добавления {i+1}: {result}")
            
            if result.get('success'):
                added_row_number = result.get('row_number')
                added_rows.append(added_row_number)
                print(f"Строка {i+1} добавлена в строку {added_row_number}")
            else:
                print(f"Ошибка при добавлении строки {i+1}")
                
        except Exception as e:
            print(f"Ошибка при добавлении данных {i+1}: {e}")
            import traceback
            print(f"Полный стек трейс: {traceback.format_exc()}")
            return

        time.sleep(1)  # Небольшая задержка между добавлениями

    # Теперь тестируем метод get_rows_by_unique_keys с возвратом всех строк
    print("\n=== Тестирование метода get_rows_by_unique_keys (все строки) ===")
    try:
        search_data = {
            "Дата": test_data["Дата"],
            "ПВЗ": test_data["ПВЗ"]
        }
        
        print(f"Данные для поиска: {search_data}")
        
        all_rows = reporter.get_rows_by_unique_keys(
            unique_key_values=search_data,
            config=table_config,
            first_only=False  # хотим все строки
        )
        
        if all_rows:
            print(f"SUCCESS: Найдено {len(all_rows)} строк:")
            for i, row in enumerate(all_rows):
                print(f"  Строка {i+1}: номер {row.get('_row_number')}, Дата={row.get('Дата')}, ПВЗ={row.get('ПВЗ')}, Количество выдач={row.get('Количество выдач')}")
            
            # Проверим, что все найденные строки имеют правильные ключи
            all_correct = all(
                str(row.get('Дата')) == test_data["Дата"] and row.get('ПВЗ') == test_data["ПВЗ"]
                for row in all_rows
            )
            if all_correct:
                print("SUCCESS: Все найденные строки имеют правильные ключи!")
            else:
                print("ERROR: Не все найденные строки имеют правильные ключи!")
        else:
            print("ERROR: Строки не найдены методом get_rows_by_unique_keys")
            
    except Exception as e:
        print(f"Ошибка при поиске всех строк: {e}")
        import traceback
        print(f"Полный стек трейс: {traceback.format_exc()}")

    # Теперь тестируем метод get_rows_by_unique_keys с возвратом только первой строки
    print("\n=== Тестирование метода get_rows_by_unique_keys (только первая строка) ===")
    try:
        first_row = reporter.get_rows_by_unique_keys(
            unique_key_values=search_data,
            config=table_config,
            first_only=True  # хотим только первую строку
        )
        
        if first_row:
            print(f"SUCCESS: Найдена первая строка: номер {first_row.get('_row_number')}, Дата={first_row.get('Дата')}, ПВЗ={first_row.get('ПВЗ')}")
        else:
            print("ERROR: Первая строка не найдена")
            
    except Exception as e:
        print(f"Ошибка при поиске первой строки: {e}")
        import traceback
        print(f"Полный стек трейс: {traceback.format_exc()}")

    # Теперь тестируем метод get_rows_by_unique_keys с выбросом исключения при дубликатах
    print("\n=== Тестирование метода get_rows_by_unique_keys (ошибка при дубликатах) ===")
    try:
        rows_with_error = reporter.get_rows_by_unique_keys(
            unique_key_values=search_data,
            config=table_config,
            first_only=False,  # хотим все строки
            raise_on_duplicate=True  # бросить исключение при дубликатах
        )
        
        print("ERROR: Ожидалось исключение при дубликатах, но его не произошло")
            
    except ValueError as e:
        print(f"SUCCESS: Получено ожидаемое исключение при дубликатах: {e}")
    except Exception as e:
        print(f"ERROR: Получено неожиданное исключение: {e}")
        import traceback
        print(f"Полный стек трейс: {traceback.format_exc()}")

    print("\nТестирование возврата всех дубликатов завершено")


def main():
    test_get_all_duplicates()


if __name__ == "__main__":
    main()