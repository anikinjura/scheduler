"""
Тест для проверки, что метод get_row_by_id ищет только в ID-колонке
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


def test_id_search_only_in_id_column():
    """Тест проверяет, что get_row_by_id ищет только в ID-колонке, а не в других колонках"""
    print("=== Тест поиска по ID только в ID-колонке ===")
    
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

    # Подготовка конфигурации таблицы
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
    
    # Подготовим тестовые данные
    test_date = (datetime.now() + timedelta(days=1)).strftime('%d.%m.%Y')
    test_pvz = "TEST_ID_SEARCH_PVZ"
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
    result = reporter.update_or_append_data_with_config(test_data, config=config)
    print(f"Результат записи: {result}")
    
    if not result.get('success'):
        print("Не удалось записать тестовую строку")
        return
    
    # Получаем ID новой строки
    new_row_number = result.get('row_number')
    if not new_row_number:
        print("Не удалось получить номер новой строки")
        return
    
    # Читаем строку, чтобы получить ID
    row_data = reporter._get_row_by_number(new_row_number, config)
    if not row_data:
        print("Не удалось прочитать новую строку")
        return
    
    actual_id = row_data.get('id')
    print(f"Фактический ID строки: {actual_id}")
    
    # Теперь проверим, что get_row_by_id находит только в ID-колонке
    # Для этого создадим ситуацию, где значение, которое мы ищем, может быть в другой колонке
    found_row = reporter.get_row_by_id(actual_id, config=config)
    if found_row:
        print(f"Найдена строка по ID: {found_row}")
        print(f"Номер строки: {found_row.get('_row_number')}")
        print("Тест пройден: метод get_row_by_id нашел строку по ID")
    else:
        print("Ошибка: метод get_row_by_id не нашел строку по ID")
    
    # Проверим, что если мы попытаемся найти значение, которое есть в другой колонке, но не в ID-колонке,
    # то строка не будет найдена в ID-колонке
    pvz_value = test_data["ПВЗ"]
    print(f"\nПопытка найти значение '{pvz_value}' (которое есть в колонке ПВЗ) в ID-колонке:")
    found_row_by_pvz = reporter.get_row_by_id(pvz_value, config=config)
    if found_row_by_pvz:
        print(f"Ошибка: метод нашел значение '{pvz_value}' в ID-колонке, хотя оно там не должно быть")
        print(f"Найденная строка: {found_row_by_pvz}")
    else:
        print(f"Тест пройден: метод не нашел значение '{pvz_value}' в ID-колонке (как и ожидалось)")
    
    print("\nТест завершен")


if __name__ == "__main__":
    test_id_search_only_in_id_column()