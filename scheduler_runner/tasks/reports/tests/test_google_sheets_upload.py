"""
Тест для проверки обновленного GoogleSheetsUploadScript с новой структурой данных
"""
import sys
from pathlib import Path

# Добавляем путь к корню проекта для импорта модулей
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

def test_format_report_data_with_new_headers():
    """Тестируем форматирование данных с новыми заголовками"""
    try:
        from scheduler_runner.tasks.reports.GoogleSheetsUploadScript import format_report_data_for_sheets
        from scheduler_runner.tasks.reports.config.scripts.GoogleSheetsUploadScript_config import SCRIPT_CONFIG
        
        print("Тестируем форматирование данных с новыми заголовками...")
        
        # Создаем тестовые данные
        test_data = {
            'date': '2026-01-04',
            'pvz_info': 'СОСНОВКА_10',
            'marketplace': 'Ozon',
            'giveout_report': {
                'issued_packages': 150
            },
            'carriages_report': {
                'report_type': 'carriages_combined',
                'direct_flow': {
                    'total_items_count': 100,
                    'total_carriages_found': 5
                },
                'return_flow': {
                    'total_items_count': 20,
                    'total_carriages_found': 2
                }
            }
        }
        
        formatted_data = format_report_data_for_sheets(test_data, 'СОСНОВКА_10')
        
        print(f"Отформатированные данные: {formatted_data}")
        
        # Проверяем, что все необходимые поля из новой структуры присутствуют
        required_fields = SCRIPT_CONFIG['REQUIRED_HEADERS']
        print(f"Требуемые поля: {required_fields}")
        
        all_present = True
        for field in required_fields:
            if field not in formatted_data:
                print(f"- Поле '{field}' отсутствует в отформатированных данных")
                all_present = False
            else:
                print(f"+ Поле '{field}' присутствует: {formatted_data[field]}")
        
        if all_present:
            print("+ Все требуемые поля присутствуют в правильном формате")
            return True
        else:
            print("- Не все требуемые поля присутствуют")
            return False
    except Exception as e:
        print(f"- Ошибка при тестировании форматирования данных: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_validation_compatibility():
    """Тестируем совместимость с валидацией данных"""
    try:
        from scheduler_runner.tasks.reports.config.scripts.GoogleSheetsUploadScript_config import SCRIPT_CONFIG
        
        print("\nТестируем совместимость с валидацией...")
        
        # Создаем тестовые отформатированные данные
        test_formatted_data = {
            "id": "",
            "Дата": "04.01.2026",
            "ПВЗ": "СОСНОВКА_10",
            "Количество выдач": 150,
            "Прямой поток": 100,
            "Возвратный поток": 20
        }
        
        required_headers = SCRIPT_CONFIG['REQUIRED_HEADERS']
        print(f"Требуемые заголовки: {required_headers}")
        
        # Проверяем, что все требуемые заголовки присутствуют в данных
        validation_passed = all(header in test_formatted_data for header in required_headers)
        
        if validation_passed:
            print("+ Валидация структуры данных прошла успешно")
            return True
        else:
            print("- Валидация структуры данных не пройдена")
            missing = [h for h in required_headers if h not in test_formatted_data]
            print(f"  Отсутствующие поля: {missing}")
            return False
    except Exception as e:
        print(f"- Ошибка при тестировании валидации: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Основная функция тестирования"""
    print("Тестирование обновленного GoogleSheetsUploadScript с новой структурой")
    print("="*70)
    
    tests = [
        test_format_report_data_with_new_headers,
        test_validation_compatibility
    ]
    
    results = []
    for test in tests:
        result = test()
        results.append(result)
    
    print("="*70)
    print(f"Результаты: {sum(results)}/{len(results)} тестов пройдено успешно")
    
    if all(results):
        print("+ Все тесты пройдены успешно!")
        print("  GoogleSheetsUploadScript готов к работе с новой структурой данных.")
        return True
    else:
        print("- Некоторые тесты не пройдены.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)