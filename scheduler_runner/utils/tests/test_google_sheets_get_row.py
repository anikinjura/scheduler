"""
Скрипт для тестирования метода get_row_by_unique_keys
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta

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


def test_get_row_by_unique_keys():
    """Тестирует чтение строки по уникальным ключам"""
    print("=== Тестирование метода get_row_by_unique_keys ===")

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

    # Подготовим тестовые данные для поиска
    print("Подготовка тестовых данных для поиска...")
    
    # Используем дату и ПВЗ, которые должны быть в таблице
    base_date = datetime.now()
    search_data = {
        "Дата": (base_date + timedelta(days=300)).strftime("%d.%m.%Y"),  # Используем ту же дату, что и в предыдущем тесте
        "ПВЗ": "SIMPLE_FORMULA_PVZ"
    }

    print(f"Данные для поиска: {search_data}")

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

    # Тестируем метод get_row_by_unique_keys
    print("\n=== Тестирование чтения строки по уникальным ключам ===")
    try:
        row_data = reporter.get_row_by_unique_keys(
            unique_key_values=search_data,
            config=table_config
        )
        
        if row_data:
            print(f"Найдена строка: {row_data}")
            print(f"Номер строки: {row_data.get('_row_number')}")
            print(f"Дата: {row_data.get('Дата')}")
            print(f"ПВЗ: {row_data.get('ПВЗ')}")
            print(f"Количество выдач: {row_data.get('Количество выдач')}")
        else:
            print("Строка не найдена")
            
    except Exception as e:
        print(f"Ошибка при чтении данных: {e}")
        import traceback
        print(f"Полный стек трейс: {traceback.format_exc()}")

    print("\nТестирование завершено")


def main():
    test_get_row_by_unique_keys()


if __name__ == "__main__":
    main()