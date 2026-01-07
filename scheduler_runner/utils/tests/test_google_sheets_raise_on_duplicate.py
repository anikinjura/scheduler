"""
Скрипт для тестирования поведения при дубликатах
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


def test_raise_on_duplicate():
    """Тестирует поведение при дубликатах с raise_on_duplicate=True"""
    print("=== Тестирование поведения при дубликатах ===")

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
    unique_date = "31.12.2026"  # Дата, которой нет в существующих записях
    unique_pvz = "RAISE_TEST_PVZ"
    
    test_data = {
        "Дата": unique_date,
        "ПВЗ": unique_pvz,
        "Количество выдач": "4444",
        "Прямой поток": "RAISE_SELLER",
        "Возвратный поток": "444"
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

    # Добавим две строки с одинаковыми уникальными ключами
    print("\n=== Добавление строк с одинаковыми ключами ===")
    for i in range(2):
        try:
            result = reporter.update_or_append_data_with_config(
                data=test_data,
                config=table_config,
                strategy="append_only"
            )
            print(f"Результат добавления {i+1}: {result}")
            
        except Exception as e:
            print(f"Ошибка при добавлении данных {i+1}: {e}")
            import traceback
            print(f"Полный стек трейс: {traceback.format_exc()}")
            return

        time.sleep(1)  # Небольшая задержка между добавлениями

    # Теперь тестируем метод get_rows_by_unique_keys с выбросом исключения при дубликатах
    print("\n=== Тестирование метода с raise_on_duplicate=True ===")
    try:
        search_data = {
            "Дата": test_data["Дата"],
            "ПВЗ": test_data["ПВЗ"]
        }
        
        print(f"Данные для поиска: {search_data}")
        
        rows_with_error = reporter.get_rows_by_unique_keys(
            unique_key_values=search_data,
            config=table_config,
            first_only=False,  # хотим все строки
            raise_on_duplicate=True  # бросить исключение при дубликатах
        )
        
        print("ERROR: Ожидалось исключение при дубликатах, но его не произошло")
        print(f"Полученные строки: {rows_with_error}")
            
    except ValueError as e:
        print(f"SUCCESS: Получено ожидаемое исключение при дубликатах: {e}")
    except Exception as e:
        print(f"ERROR: Получено неожиданное исключение: {e}")
        import traceback
        print(f"Полный стек трейс: {traceback.format_exc()}")

    # Теперь тестируем метод get_rows_by_unique_keys без выброса исключения
    print("\n=== Тестирование метода без выброса исключения ===")
    try:
        rows_normal = reporter.get_rows_by_unique_keys(
            unique_key_values=search_data,
            config=table_config,
            first_only=False,  # хотим все строки
            raise_on_duplicate=False  # не бросать исключение
        )
        
        if rows_normal:
            print(f"SUCCESS: Найдено {len(rows_normal)} строк без исключения:")
            for i, row in enumerate(rows_normal):
                print(f"  Строка {i+1}: номер {row.get('_row_number')}")
        else:
            print("ERROR: Строки не найдены")
            
    except Exception as e:
        print(f"ERROR: Произошло исключение: {e}")
        import traceback
        print(f"Полный стек трейс: {traceback.format_exc()}")

    print("\nТестирование поведения при дубликатах завершено")


def main():
    test_raise_on_duplicate()


if __name__ == "__main__":
    main()