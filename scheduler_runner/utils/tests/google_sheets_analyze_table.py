"""
Скрипт для анализа данных в Google-таблице
Позволяет анализировать результаты записи данных в таблицу
"""
import sys
from pathlib import Path
from typing import List, Dict, Any

# Добавляем корень проекта в sys.path для корректного импорта
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))  # Поднимаемся на 3 уровня вверх до корня проекта

from scheduler_runner.utils.google_sheets import GoogleSheetsReporter
from scheduler_runner.tasks.reports.config.scripts.GoogleSheetsUploadScript_config import (
    GOOGLE_CREDENTIALS_PATH,
    SPREADSHEET_NAME,
    WORKSHEET_NAME
)


def analyze_table_data():
    """Анализирует данные в Google-таблице"""
    print("=== Анализ данных в Google-таблице ===")
    
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
        print(f"Заголовки: {headers}")
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
    
    # Читаем несколько последних строк для анализа
    print("\n=== Анализ последних строк ===")
    start_row = max(1, last_row - 10)  # Читаем последние 10 строк или с первой строки
    for row_num in range(start_row, last_row + 1):
        try:
            row_values = reporter.worksheet.row_values(row_num)
            if row_values:  # Показываем только непустые строки
                print(f"Строка {row_num}: {row_values}")
                
                # Проверим значение в ячейке A (Id колонка) отдельно
                cell_a_value = reporter.worksheet.cell(row_num, 1).value
                print(f"  -> Значение в A{row_num}: {repr(cell_a_value)}")
                
                # Проверим, есть ли все необходимые данные
                if len(row_values) >= 6:
                    id_val, date_val, pvz_val, quantity_val, direct_flow_val, return_flow_val = row_values[:6]
                    print(f"  -> Данные: Id={id_val}, Дата={date_val}, ПВЗ={pvz_val}, Количество={quantity_val}, Прямой={direct_flow_val}, Возвратный={return_flow_val}")
                else:
                    print(f"  -> Неполные данные: {len(row_values)} значений из 6 ожидаемых")
        except Exception as e:
            print(f"Ошибка чтения строки {row_num}: {e}")
    
    # Анализируем уникальные даты и ПВЗ
    print("\n=== Анализ уникальных значений ===")
    unique_dates = set()
    unique_pvz = set()
    
    for row_num in range(2, last_row + 1):  # Начинаем с 2, пропускаем заголовки
        try:
            row_values = reporter.worksheet.row_values(row_num)
            if len(row_values) >= 3:  # Должно быть как минимум Дата и ПВЗ
                date_val = row_values[1]  # Вторая колонка - Дата
                pvz_val = row_values[2]  # Третья колонка - ПВЗ
                
                if date_val:
                    unique_dates.add(date_val)
                if pvz_val:
                    unique_pvz.add(pvz_val)
        except Exception:
            continue
    
    print(f"Уникальные даты: {sorted(unique_dates)}")
    print(f"Уникальные ПВЗ: {sorted(unique_pvz)}")
    
    # Проверим, есть ли дубликаты по уникальным ключам (Дата + ПВЗ)
    print("\n=== Проверка дубликатов ===")
    key_counts = {}
    duplicates_found = False
    
    for row_num in range(2, last_row + 1):
        try:
            row_values = reporter.worksheet.row_values(row_num)
            if len(row_values) >= 3:
                date_val = row_values[1]
                pvz_val = row_values[2]
                
                key = (date_val, pvz_val)
                if key in key_counts:
                    key_counts[key].append(row_num)
                    duplicates_found = True
                else:
                    key_counts[key] = [row_num]
        except Exception:
            continue
    
    if duplicates_found:
        print("Найдены дубликаты по уникальным ключам (Дата + ПВЗ):")
        for key, rows in key_counts.items():
            if len(rows) > 1:
                print(f"  {key} -> строки {rows}")
    else:
        print("Дубликаты по уникальным ключам не найдены")
    
    # Статистика по количеству значений в строках
    print("\n=== Статистика по количеству значений в строках ===")
    value_counts = {}
    
    for row_num in range(2, last_row + 1):
        try:
            row_values = reporter.worksheet.row_values(row_num)
            count = len(row_values)
            if count in value_counts:
                value_counts[count] += 1
            else:
                value_counts[count] = 1
        except Exception:
            continue
    
    for count, num_rows in sorted(value_counts.items()):
        print(f"  {count} значений: {num_rows} строк")
    
    print(f"\nАнализ завершен. Последняя строка с данными: {last_row}")


def main():
    analyze_table_data()


if __name__ == "__main__":
    main()