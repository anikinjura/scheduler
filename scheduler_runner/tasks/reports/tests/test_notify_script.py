"""
Простой тест для проверки обновленного NotifyScript
"""
import sys
from pathlib import Path

# Добавляем путь к корню проекта для импорта модулей
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_load_report_data():
    """Тестируем загрузку данных отчетов"""
    try:
        from scheduler_runner.tasks.reports.utils.reports_utils import load_combined_report_data
        from config.base_config import PVZ_ID
        
        print("Тестируем загрузку данных отчетов...")
        
        # Загружаем данные для сегодняшней даты
        report_data = load_combined_report_data(None, PVZ_ID)
        
        print(f"Дата отчета: {report_data.get('date')}")
        print(f"ПВЗ: {report_data.get('pvz_info')}")
        print(f"Маркетплейс: {report_data.get('marketplace')}")
        
        # Проверяем наличие данных из разных отчетов
        giveout_report = report_data.get('giveout_report', {})
        direct_flow_report = report_data.get('direct_flow_report', {})
        carriages_report = report_data.get('carriages_report', {})
        
        print(f"Данные из отчета по выдаче: {'ДА' if giveout_report else 'НЕТ'}")
        print(f"Данные из отчета по селлерским отправлениям: {'ДА' if direct_flow_report else 'НЕТ'}")
        print(f"Данные из отчета по перевозкам: {'ДА' if carriages_report else 'НЕТ'}")
        
        if carriages_report:
            print(f"  Тип отчета: {carriages_report.get('report_type', 'Неизвестен')}")
            print(f"  Дата: {carriages_report.get('date', 'Неизвестна')}")
            
            direct_flow = carriages_report.get('direct_flow', {})
            if direct_flow:
                print(f"  Прямые перевозки: {direct_flow.get('total_items_count', 0)} отправлений, {direct_flow.get('total_carriages_found', 0)} перевозок")
            
            return_flow = carriages_report.get('return_flow', {})
            if return_flow:
                print(f"  Возвратные перевозки: {return_flow.get('total_items_count', 0)} отправлений, {return_flow.get('total_carriages_found', 0)} перевозок")
        
        print("+ Загрузка данных отчетов работает корректно")
        return True
    except Exception as e:
        print(f"- Ошибка при загрузке данных отчетов: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_format_notification_message():
    """Тестируем форматирование сообщения без вывода"""
    try:
        from scheduler_runner.tasks.reports.NotifyScript import format_notification_message
        
        print("\nТестируем форматирование сообщения...")
        
        # Создаем тестовые данные
        test_data = {
            'date': '2026-01-04',
            'pvz_info': 'СОСНОВКА_10',
            'marketplace': 'Ozon',
            'giveout_report': {
                'issued_packages': 150
            },
            'direct_flow_report': {
                'total_items_count': 50,
                'total_carriages_found': 3
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
        
        message = format_notification_message(test_data)
        print(f"Сообщение успешно сформировано (длина: {len(message)} символов)")
        
        print("+ Форматирование сообщения работает корректно")
        return True
    except Exception as e:
        print(f"- Ошибка при форматировании сообщения: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Основная функция тестирования"""
    print("Тестирование обновленного NotifyScript")
    print("="*50)
    
    tests = [
        test_load_report_data,
        test_format_notification_message
    ]
    
    results = []
    for test in tests:
        result = test()
        results.append(result)
    
    print("="*50)
    print(f"Результаты: {sum(results)}/{len(results)} тестов пройдено успешно")
    
    if all(results):
        print("+ Все тесты пройдены успешно!")
        return True
    else:
        print("- Некоторые тесты не пройдены.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)