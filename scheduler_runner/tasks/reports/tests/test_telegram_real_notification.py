#!/usr/bin/env python3
"""
test_telegram_notification_real.py

Тест реальной отправки уведомления в Telegram через новый скрипт.
"""

import sys
import os
from pathlib import Path

# Добавляем путь к проекту
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from scheduler_runner.tasks.reports.Telegram_KPI_NotificationScript import main
from scheduler_runner.tasks.reports.config.scripts.Telegram_KPI_NotificationScript_config import SCRIPT_CONFIG

def test_real_telegram_notification():
    """Тест реальной отправки уведомления в Telegram."""
    print("=" * 70)
    print("ТЕСТ РЕАЛЬНОЙ ОТПРАВКИ УВЕДОМЛЕНИЯ В TELEGRAM")
    print("=" * 70)
    
    # Проверяем, что переменные окружения установлены
    print("\n1. Проверка переменных окружения:")
    telegram_token = os.environ.get("TELEGRAM_TOKEN_DEV")
    telegram_chat_id = os.environ.get("TELEGRAM_CHAT_ID_DEV")
    
    print(f"   TELEGRAM_TOKEN_DEV: {'установлен' if telegram_token else 'НЕ установлен'}")
    print(f"   TELEGRAM_CHAT_ID_DEV: {'установлен' if telegram_chat_id else 'НЕ установлен'}")
    
    if not telegram_token or not telegram_chat_id:
        print("   [ERROR] Необходимо установить переменные окружения TELEGRAM_TOKEN_DEV и TELEGRAM_CHAT_ID_DEV")
        return False
    
    # Проверяем, что конфигурация содержит правильные значения
    print(f"\n2. Проверка конфигурации скрипта:")
    print(f"   TELEGRAM_BOT_TOKEN: {'установлен' if SCRIPT_CONFIG.get('TELEGRAM_BOT_TOKEN') else 'НЕ установлен'}")
    print(f"   TELEGRAM_CHAT_ID: {'установлен' if SCRIPT_CONFIG.get('TELEGRAM_CHAT_ID') else 'НЕ установлен'}")
    
    if not SCRIPT_CONFIG.get('TELEGRAM_BOT_TOKEN') or not SCRIPT_CONFIG.get('TELEGRAM_CHAT_ID'):
        print("   [ERROR] Конфигурация скрипта не содержит токен или ID чата")
        return False
    
    print(f"   Токен: {SCRIPT_CONFIG['TELEGRAM_BOT_TOKEN'][:10]}..." if len(SCRIPT_CONFIG['TELEGRAM_BOT_TOKEN']) > 10 else f"   Токен: {SCRIPT_CONFIG['TELEGRAM_BOT_TOKEN']}")
    print(f"   Чат ID: {SCRIPT_CONFIG['TELEGRAM_CHAT_ID']}")
    
    # Проверяем наличие тестовых файлов отчетов
    print(f"\n3. Проверка наличия тестовых файлов отчетов:")
    from scheduler_runner.tasks.reports.config.reports_paths import REPORTS_PATHS
    reports_dir = REPORTS_PATHS["REPORTS_JSON"]
    print(f"   Директория отчетов: {reports_dir}")
    
    # Ищем файлы отчетов
    import glob
    giveout_files = list(reports_dir.glob("ozon_giveout_report_*.json"))
    carriage_files = list(reports_dir.glob("ozon_carriages_report_*.json"))
    
    print(f"   Файлы отчетов по выдаче: {len(giveout_files)}")
    print(f"   Файлы отчетов по перевозкам: {len(carriage_files)}")
    
    if not giveout_files and not carriage_files:
        print("   [WARNING] Нет файлов отчетов для теста")
    else:
        print("   [OK] Файлы отчетов доступны для теста")
    
    # Подготовим аргументы для теста
    print(f"\n4. Подготовка к запуску скрипта:")
    print(f"   Запуск будет выполнен с тестовыми параметрами")
    print(f"   Дата отчета: 2026-01-06 (у нас есть файлы с этой датой)")
    print(f"   ПВЗ: SOSNOVKA_10 (у нас есть файлы с этим ПВЗ)")
    
    # Устанавливаем аргументы для теста
    import sys
    original_argv = sys.argv.copy()
    
    try:
        # Подготовим аргументы для запуска
        sys.argv = [
            'test_telegram_notification_real.py',
            '--report_date', '2026-01-06',
            '--pvz_id', 'SOSNOVKA_10',
            '--detailed_logs'
        ]
        
        print(f"\n5. Запуск скрипта с аргументами: {' '.join(sys.argv[1:])}")
        
        # Вызываем основную функцию
        main()
        
        print(f"\n[OK] СКРИПТ ВЫПОЛНЕН УСПЕШНО!")
        print(f"[OK] Уведомление должно быть отправлено в Telegram")

        return True

    except Exception as e:
        print(f"\n[ERROR] ОШИБКА ПРИ ВЫПОЛНЕНИИ СКРИПТА: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Восстанавливаем оригинальные аргументы
        sys.argv = original_argv


if __name__ == "__main__":
    success = test_real_telegram_notification()
    
    if success:
        print("\n" + "=" * 70)
        print("ТЕСТ РЕАЛЬНОЙ ОТПРАВКИ В TELEGRAM ПРОЙДЕН УСПЕШНО! 🎉")
        print("Проверьте свой Telegram-чат на наличие нового сообщения.")
        print("=" * 70)
    else:
        print("\n" + "=" * 70)
        print("ТЕСТ РЕАЛЬНОЙ ОТПРАВКИ В TELEGRAM НЕ УДАЛСЯ")
        print("Проверьте настройки переменных окружения и файлы отчетов.")
        print("=" * 70)