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


def _strip_env_value(raw_value: str) -> str:
    value = raw_value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1]
    return value


def load_env_file(env_file: Path) -> dict[str, str]:
    """
    Загружает простой `.env`-файл формата KEY=VALUE без внешних зависимостей.

    Поведение специально консервативное:
    - пропускает пустые строки и комментарии;
    - не затирает уже существующие переменные окружения;
    - поддерживает опциональный префикс `export `;
    - возвращает словарь реально загруженных значений.
    """
    loaded: dict[str, str] = {}

    if not env_file.exists():
        return loaded

    with env_file.open("r", encoding="utf-8") as fh:
        for lineno, raw_line in enumerate(fh, start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue

            if line.startswith("export "):
                line = line[len("export ") :].strip()

            if "=" not in line:
                print(f"[WARNING] Некорректная строка в .env ({env_file}:{lineno}): {raw_line.rstrip()}")
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            if not key:
                print(f"[WARNING] Пустой ключ в .env ({env_file}:{lineno})")
                continue

            if key in os.environ:
                continue

            normalized_value = _strip_env_value(value)
            os.environ[key] = normalized_value
            loaded[key] = normalized_value

    return loaded

# 1. Базовые директории проекта
BASE_DIR = Path(__file__).parent.parent     # Путь к корневой директории проекта
LOGS_DIR = BASE_DIR / 'logs'
SCHEDULER_RUNNER_DIR = BASE_DIR / 'scheduler_runner'
TASKS_DIR = SCHEDULER_RUNNER_DIR / 'tasks'

# Ранняя загрузка `.env` перед чтением остальной конфигурации проекта.
# Путь можно временно переопределить через SCHEDULER_ENV_FILE.
DEFAULT_ENV_FILE = BASE_DIR / ".env" / "secrets.env"
ENV_FILE = Path(os.environ.get("SCHEDULER_ENV_FILE", str(DEFAULT_ENV_FILE)))
LOADED_ENV_VARS = load_env_file(ENV_FILE)

# 2. Загрузка PVZ_ID и ENV_MODE из pvz_config.ini
# Ищем pvz_config.ini вне проекта, по умолчанию: C:\tools\pvz_config.ini
# Можно переопределить путь через переменную окружения PVZ_CONFIG_PATH
DEFAULT_PVZ_CONFIG_PATH = r'C:\tools\pvz_config.ini'
PVZ_CONFIG_FILE = Path(os.environ.get('PVZ_CONFIG_PATH', DEFAULT_PVZ_CONFIG_PATH))

if not PVZ_CONFIG_FILE.exists():
    sys.exit(f"[ERROR] Не найден файл конфигурации PVZ: {PVZ_CONFIG_FILE}")

config = configparser.ConfigParser()
config.read(PVZ_CONFIG_FILE, encoding='utf-8')
PVZ_ID = config.get('DEFAULT', 'PVZ_ID', fallback='UNKNOWN')
ENV_MODE = config.get('DEFAULT', 'ENV_MODE', fallback='production').lower()
if ENV_MODE not in ('production', 'test'):
    sys.exit(f"[ERROR] ENV_MODE должны быть 'production' или 'test', получено: {ENV_MODE}")

# 3. Общие переменные для всего проекта
REPORTS_DIR = BASE_DIR / 'reports'  # Директория для отчетов в корне проекта
PATH_CONFIG = {
    'BASE_DIR': BASE_DIR,
    'SCHEDULER_ROOT': SCHEDULER_RUNNER_DIR,
    'LOGS_ROOT': LOGS_DIR,
    'TASKS_ROOT': TASKS_DIR,
    'REPORTS_ROOT': REPORTS_DIR,
}

# 4. Вспомогательная валидация: предупреждаем, если важное отсутствует
for key, path in PATH_CONFIG.items():
    if isinstance(path, Path) and key.endswith(('ROOT')):
        if not path.exists():
            print(f"[WARNING] Путь для {key} не определен: {path}")
