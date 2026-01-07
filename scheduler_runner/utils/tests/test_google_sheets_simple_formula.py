"""
Скрипт для тестирования простой формулы в Id колонке
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


def test_simple_formula():
    """Тестирует запись с простой формулой в Id колонке"""
    print("=== Тестирование простой формулы в Id колонке ===")
    
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
    
    # Начинаем с текущей даты и добавляем по одному дню для каждой записи
    base_date = datetime.now()
    test_data = {
        "Дата": (base_date + timedelta(days=300)).strftime("%d.%m.%Y"),
        "ПВЗ": "SIMPLE_FORMULA_PVZ",
        "Количество выдач": "5000",
        "Прямой поток": "SIMPLE_SELLER",
        "Возвратный поток": "250"
    }
    
    print(f"Тестовые данные: {test_data}")
    
    # Записываем данные в таблицу с простой формулой
    print("\n=== Запись тестовых данных с простой формулой ===")
    try:
        # Создаем конфигурацию таблицы с простой формулой
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
        
        result = reporter.update_or_append_data_with_config(
            data=test_data,
            config=table_config,
            strategy="append_only"
        )
        print(f"Результат записи: {result}")
    except Exception as e:
        print(f"Ошибка при записи данных: {e}")
        import traceback
        print(f"Полный стек трейс: {traceback.format_exc()}")
    
    print("\nТестирование завершено")


def main():
    test_simple_formula()


if __name__ == "__main__":
    main()