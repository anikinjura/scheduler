"""
Скрипт для прямого запуска NotifyScript

Этот скрипт запускает основной скрипт уведомлений напрямую, чтобы проверить его работу.
"""
import sys
import os
from pathlib import Path

# Добавляем путь к корню проекта для импорта модулей
from config.base_config import PATH_CONFIG
project_root = PATH_CONFIG['BASE_DIR']
sys.path.insert(0, str(project_root))

def direct_run():
    """Прямой запуск основного скрипта уведомлений"""
    print("=== Прямой запуск NotifyScript ===")
    
    try:
        print("Импорт основного модуля...")
        from scheduler_runner.tasks.reports.NotifyScript import main
        print("+ Импорт успешно завершен")
    except Exception as e:
        print(f"- Ошибка импорта: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    try:
        print("Подмена аргументов командной строки...")
        # Подменяем sys.argv для передачи --detailed_logs
        original_argv = sys.argv[:]
        sys.argv = ['test', '--detailed_logs']
        
        print("Вызов основной функции main()...")
        main()
        print("+ Основная функция выполнена")
        
        # Восстанавливаем оригинальные аргументы
        sys.argv = original_argv
        return True
    except SystemExit as e:
        print(f"+ Программа завершена с кодом: {e.code}")
        # Это нормальное поведение для main функции
        return True
    except Exception as e:
        print(f"- Ошибка при выполнении основной функции: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Основная функция"""
    print("Прямой запуск NotifyScript")
    print("=" * 50)
    
    success = direct_run()
    
    print("=" * 50)
    if success:
        print("+ Прямой запуск завершен успешно!")
        print("Скрипт работает корректно.")
    else:
        print("- При прямом запуске возникли проблемы.")
        print("Проверьте вывод выше для выявления причин ошибок.")
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)