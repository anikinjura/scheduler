import importlib
import pkgutil
from typing import List, Dict

from config.base_config import PATH_CONFIG

def get_scripts_dir(domain: str):
    """Возвращает путь к директории с конфигами скриптов для указанного домена."""
    return PATH_CONFIG['TASKS_ROOT'] / domain / "config" / "scripts"

def get_module_name(domain: str, module_info):
    """Возвращает полное имя модуля для импорта скрипта задачи."""
    return f"scheduler_runner.tasks.{domain}.config.scripts.{module_info.name}"

def collect_task_schedule(domain: str) -> List[Dict]:
    """
    Универсально собирает расписания всех скриптов задачи <domain> из config/scripts/*_config.py.
    Возвращает список задач для ядра планировщика.
    """
    task_schedule = []
    scripts_dir = get_scripts_dir(domain)
    for module_info in pkgutil.iter_modules([str(scripts_dir)]):
        if not module_info.name.endswith("_config"):
            continue
        module_name = get_module_name(domain, module_info)
        try:
            mod = importlib.import_module(module_name)
            schedule = getattr(mod, "SCHEDULE", None)
            if schedule and isinstance(schedule, list):
                task_schedule.extend(schedule)
        except Exception as e:
            print(f"[WARNING] Не удалось импортировать {module_name}: {e}")
    return task_schedule