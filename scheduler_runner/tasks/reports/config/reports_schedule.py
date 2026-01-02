"""
reports_schedule.py

Расписание задач для модуля отчетов.
Автоматически собирает расписания всех скриптов из config/scripts/*_config.py

Author: anikinjura
"""
__version__ = '1.0.0'

import os
import importlib.util
from pathlib import Path

# Путь к директории с конфигами скриптов
SCRIPTS_CONFIG_DIR = Path(__file__).parent / "scripts"

# Список задач
TASK_SCHEDULE = []

# Автоматически импортируем расписания из всех файлов конфигов скриптов
for config_file in SCRIPTS_CONFIG_DIR.glob("*_config.py"):
    if config_file.name == "__init__.py":
        continue
    
    # Формируем имя модуля
    module_name = f"reports_config_{config_file.stem}"
    
    try:
        # Загружаем модуль динамически
        spec = importlib.util.spec_from_file_location(module_name, config_file)
        config_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(config_module)
        
        # Импортируем TASK_SCHEDULE из модуля, если он существует
        if hasattr(config_module, 'TASK_SCHEDULE'):
            TASK_SCHEDULE.extend(config_module.TASK_SCHEDULE)
    except Exception as e:
        print(f"Ошибка при импорте конфигурации {config_file}: {e}")
        continue

# Выводим количество загруженных задач
print(f"Загружено {len(TASK_SCHEDULE)} задач из конфигураций отчетов")