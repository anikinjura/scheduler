#!/usr/bin/env python3
"""
test_integration_components.py

Интеграционный тест для проверки работы всех компонентов вместе.
Проверяет, что все компоненты новой архитектуры работают совместно.
"""

import sys
from pathlib import Path

# Добавляем путь к проекту для импорта
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from scheduler_runner.tasks.reports.utils import (
    load_reports_data,
    ReportConfig,
    GoogleSheetsTransformer,
    find_report_file
)
from scheduler_runner.tasks.reports.config.reports_paths import REPORTS_PATHS


def test_integration_components():
    """
    Интеграционный тест для проверки работы всех компонентов вместе.

    Проверяет:
    1. Создание конфигурации
    2. Загрузку данных (даже без файлов)
    3. Преобразование данных для Google Sheets
    4. Работу утилит для файлов
    """
    print("=" * 70)
    print("ИНТЕГРАЦИОННЫЙ ТЕСТ: Проверка работы всех компонентов вместе")
    print("=" * 70)

    # Тест 1: Создание конфигурации
    print("\n1. [ИНТЕГРАЦИЯ] Создание конфигурации:")
    config = ReportConfig(
        report_type='test',
        file_pattern='test_report_{date}.json',
        required=False
    )
    print(f"   [OK] Создана конфигурация: {config}")
    print(f"   [OK] Тип отчета: {config.report_type}")
    print(f"   [OK] Шаблон файла: {config.file_pattern}")

    # Тест 2: Загрузка данных (без реальных файлов)
    print("\n2. [ИНТЕГРАЦИЯ] Загрузка данных (ожидается отсутствие файлов):")
    try:
        data = load_reports_data(
            report_date="2026-01-05",
            pvz_id="Тестовый ПВЗ"
        )
        print(f"   [OK] Загрузка выполнена: {bool(data)}")
        print(f"   [OK] Метаинформация присутствует: {'_loaded_at' in data}")
        print(f"   [OK] Ключи данных: {list(data.keys()) if data else 'Нет данных'}")
    except Exception as e:
        print(f"   [ERROR] Ошибка при загрузке: {e}")
        assert False, f"Ошибка при загрузке данных: {e}"

    # Тест 3: Трансформер данных
    print("\n3. [ИНТЕГРАЦИЯ] Преобразование данных для Google Sheets:")
    test_data = {
        'issued_packages': 100,
        'total_packages': 150,
        'direct_flow_count': 50,
        'pvz_info': 'Тестовый ПВЗ',
        '_report_date': '2026-01-05'
    }

    transformer = GoogleSheetsTransformer()
    sheets_data = transformer.transform(test_data)

    print(f"   [OK] Трансформация выполнена")
    print(f"   [OK] Данные для Google Sheets: {sheets_data}")

    # Проверим, что все ключи присутствуют
    required_keys = ['id', 'Дата', 'ПВЗ', 'Количество выдач', 'Прямой поток', 'Возвратный поток']
    has_all_keys = all(key in sheets_data for key in required_keys)
    print(f"   [OK] Все ключи присутствуют: {has_all_keys}")
    assert has_all_keys, "Не все ключи присутствуют в трансформированных данных"

    # Тест 4: Поиск файла (тестирование утилиты)
    print("\n4. [ИНТЕГРАЦИЯ] Работа утилит для файлов:")
    report_dir = REPORTS_PATHS["REPORTS_JSON"]
    print(f"   [OK] Директория отчетов: {report_dir}")

    # Попробуем найти несуществующий файл
    file_path = find_report_file(
        pattern_template="nonexistent_{date}.json",
        directory=report_dir,
        date="2026-01-05"
    )
    print(f"   [OK] Поиск несуществующего файла: {file_path is None}")

    print("\n" + "=" * 70)
    print("[OK] ИНТЕГРАЦИОННЫЙ ТЕСТ ЗАВЕРШЕН УСПЕШНО!")
    print("[OK] Все компоненты новой архитектуры работают совместно")
    print("[OK] Интеграция между слоями работает корректно")
    print("=" * 70)

    # Убедимся, что все основные компоненты работают
    assert config is not None
    assert data is not None
    assert sheets_data is not None
    assert file_path is None  # потому что файл не должен быть найден


def test_end_to_end_workflow():
    """
    Тест полного рабочего процесса от загрузки до трансформации.
    """
    print("\n" + "=" * 70)
    print("ТЕСТ ПОЛНОГО РАБОЧЕГО ПРОЦЕССА")
    print("=" * 70)

    # Имитация полного процесса
    print("\n1. Загрузка данных отчетов...")
    raw_data = load_reports_data(
        report_date="2026-01-05",
        pvz_id="Тестовый ПВЗ"
    )
    print(f"   [OK] Данные загружены: {bool(raw_data)}")

    print("\n2. Преобразование для Google Sheets...")
    transformer = GoogleSheetsTransformer()
    sheets_data = transformer.transform(raw_data)
    print(f"   [OK] Данные преобразованы: {bool(sheets_data)}")

    print("\n3. Проверка структуры данных для Google Sheets...")
    expected_fields = ['id', 'Дата', 'ПВЗ', 'Количество выдач', 'Прямой поток', 'Возвратный поток']
    has_expected_fields = all(field in sheets_data for field in expected_fields)
    print(f"   [OK] Структура корректна: {has_expected_fields}")

    print("\n" + "=" * 70)
    print("[OK] ТЕСТ ПОЛНОГО РАБОЧЕГО ПРОЦЕССА ЗАВЕРШЕН")
    print("=" * 70)

    # Проверки для pytest
    assert raw_data is not None
    assert sheets_data is not None
    assert has_expected_fields, "Структура данных для Google Sheets некорректна"


if __name__ == "__main__":
    try:
        test_integration_components()
        test_end_to_end_workflow()
        print("\n[OK] ВСЕ ИНТЕГРАЦИОННЫЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО!")
    except Exception as e:
        print(f"\n[ERROR] ОШИБКА В ИНТЕГРАЦИОННЫХ ТЕСТАХ: {e}")
        sys.exit(1)