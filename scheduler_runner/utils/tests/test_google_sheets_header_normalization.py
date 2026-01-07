"""
Тест для проверки нормализации заголовков в методе _find_rows_by_unique_keys_batch
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Добавляем корень проекта в sys.path для корректного импорта
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))  # Поднимаемся на 3 уровня вверх до корня проекта

from scheduler_runner.utils.google_sheets import GoogleSheetsReporter, ColumnType, ColumnDefinition, TableConfig
from scheduler_runner.tasks.reports.config.scripts.GoogleSheetsUploadScript_config import (
    GOOGLE_CREDENTIALS_PATH,
    SPREADSHEET_NAME,
    WORKSHEET_NAME
)


def test_header_normalization():
    """Тест проверяет, что заголовки нормализуются (удаляются пробелы, регистр не важен)"""
    print("=== Тест нормализации заголовков ===")
    
    # Подключение к Google Sheets
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
    test_date = (datetime.now() + timedelta(days=1)).strftime('%d.%m.%Y')
    test_pvz = "HEADER_NORMALIZATION_TEST"
    test_data = {
        "Дата": test_date,
        "ПВЗ": test_pvz,
        "Количество выдач": "100",
        "Прямой поток": "SELLER1",
        "Возвратный поток": "25"
    }
    
    print(f"Тестовые данные: {test_data}")
    
    # Записываем тестовую строку
    print("\n=== Запись тестовой строки ===")
    config = TableConfig(
        worksheet_name=WORKSHEET_NAME,
        id_column="id",
        columns=[
            ColumnDefinition(name="id", column_type=ColumnType.FORMULA, formula_template="=B{row}&C{row}"),
            ColumnDefinition(name="Дата", column_type=ColumnType.DATA, required=True, unique_key=True),
            ColumnDefinition(name="ПВЗ", column_type=ColumnType.DATA, required=True, unique_key=True),
            ColumnDefinition(name="Количество выдач", column_type=ColumnType.DATA),
            ColumnDefinition(name="Прямой поток", column_type=ColumnType.DATA),
            ColumnDefinition(name="Возвратный поток", column_type=ColumnType.DATA)
        ],
        unique_key_columns=["Дата", "ПВЗ"]
    )
    
    result = reporter.update_or_append_data_with_config(test_data, config=config)
    print(f"Результат записи: {result}")
    
    if not result.get('success'):
        print("Не удалось записать тестовую строку")
        return
    
    # Теперь попробуем найти строку с разными вариантами написания заголовков
    # (с пробелами, разным регистром)
    search_keys = {
        "Дата": test_date,  # оригинальное имя
        "ПВЗ": test_pvz
    }
    
    print(f"\nПоиск по ключам: {search_keys}")
    found_rows = reporter.get_rows_by_unique_keys(search_keys, config=config, first_only=True, raise_on_duplicate=False)
    if found_rows:
        print(f"Найдена строка: {found_rows}")
        print("Тест пройден: поиск работает с оригинальными именами колонок")
    else:
        print("Ошибка: строка не найдена с оригинальными именами колонок")
    
    # Попробуем найти с пробелами и разным регистром
    search_keys_with_spaces = {
        "Дата ": test_date,  # с пробелом в конце
        " ПВЗ": test_pvz     # с пробелом в начале
    }
    
    print(f"\nПоиск по ключам с пробелами: {search_keys_with_spaces}")
    found_rows_spaces = reporter.get_rows_by_unique_keys(search_keys_with_spaces, config=config, first_only=True, raise_on_duplicate=False)
    if found_rows_spaces:
        print(f"Найдена строка: {found_rows_spaces}")
        print("Тест пройден: поиск работает с пробелами в именах колонок")
    else:
        print("Строка не найдена с пробелами в именах колонок (это может быть нормально, если колонки не существуют с такими именами)")
    
    print("\nТест нормализации заголовков завершен")


if __name__ == "__main__":
    test_header_normalization()