"""
base_config.py

Базовый конфигурационный файл проекта.

Содержит:
    - Определение основных директорий проекта (BASE_DIR, LOGS_DIR, SCHEDULER_RUNNER_DIR, TASKS_DIR)
    - Загрузку идентификатора объекта (PVZ_ID) и режима среды (ENV_MODE) из pvz_config.ini
    - Глобальный словарь PATH_CONFIG с основными путями для ядра и задач
    - Валидацию наличия ключевых директорий

Пример использования:
    from config.base_config import PVZ_ID, ENV_MODE, PATH_CONFIG

    print(PATH_CONFIG['TASKS_ROOT'])
    print(PVZ_ID, ENV_MODE)

Структура PATH_CONFIG:
    {
        'BASE_DIR': Path,           # Корень проекта
        'SCHEDULER_ROOT': Path,     # Директория scheduler_runner
        'LOGS_ROOT': Path,          # Директория логов
        'TASKS_ROOT': Path,         # Директория с задачами
    }

pvz_config.ini должен содержать секцию [DEFAULT] с параметрами:
    PVZ_ID = <int>
    ENV_MODE = production | test

Если ENV_MODE не определён или невалиден — выполнение прерывается с ошибкой.

При отсутствии важных директорий выводится предупреждение.

Author: anikinjura
"""
__version__ = '0.0.1'

import os
import sys
import configparser
from pathlib import Path

# 1. Базовые директории проекта
BASE_DIR = Path(__file__).parent.parent     # Путь к корневой директории проекта
LOGS_DIR = BASE_DIR / 'logs'
SCHEDULER_RUNNER_DIR = BASE_DIR / 'scheduler_runner'
TASKS_DIR = SCHEDULER_RUNNER_DIR / 'tasks'

# 2. Загрузка PVZ_ID и ENV_MODE из pvz_config.ini
PVZ_CONFIG_FILE = BASE_DIR / 'pvz_config.ini'
config = configparser.ConfigParser()
config.read(PVZ_CONFIG_FILE)
PVZ_ID = config.getint('DEFAULT', 'PVZ_ID', fallback=0)
ENV_MODE = config.get('DEFAULT', 'ENV_MODE', fallback='production').lower()
if ENV_MODE not in ('production', 'test'):
    sys.exit(f"[ERROR] ENV_MODE должны быть 'production' или 'test', получено: {ENV_MODE}")

# 3. Общие переменные для всего проекта
PATH_CONFIG = {
    'BASE_DIR': BASE_DIR,
    'SCHEDULER_ROOT': SCHEDULER_RUNNER_DIR,
    'LOGS_ROOT': LOGS_DIR,
    'TASKS_ROOT': TASKS_DIR,
}

# 3. Вспомогательная валидация: предупреждаем, если важноe
for key, path in PATH_CONFIG.items():
    if isinstance(path, Path) and key.endswith(('ROOT')):
        if not path.exists():
            print(f"[WARNING] Путь для {key} не определен: {path}")
