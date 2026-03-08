"""
copy_config.py
"""
__version__ = '0.0.2'

from scheduler_runner.tasks.cameras.config.cameras_paths import CAMERAS_PATHS

MODULE_PATH = "scheduler_runner.tasks.cameras.CopyScript"

SCRIPT_CONFIG = {
    "INPUT_DIR": CAMERAS_PATHS["CAMERAS_LOCAL"],
    "INPUT_DIRS": list(CAMERAS_PATHS.get("LOCAL_ROOTS", {"default": CAMERAS_PATHS["CAMERAS_LOCAL"]}).values()),
    "OUTPUT_DIR": CAMERAS_PATHS["CAMERAS_NETWORK"],
    "MAX_AGE_DAYS": 3,
    "ON_CONFLICT": "skip",
    "DETAILED_LOGS": False,
    "USER": "operator",
    "TASK_NAME": "CopyScript",
    "SHUTDOWN_ENABLED": True,
    "SHUTDOWN_IF_NO_FILES": False,
}

SCHEDULE = [{
    "name": SCRIPT_CONFIG["TASK_NAME"],
    "module": MODULE_PATH,
    "args": ["--shutdown", "1"],
    "schedule": "daily",
    "time": "22:30",
    "user": SCRIPT_CONFIG["USER"],
    "no_timeout_control": False,
    "timeout": 21600,
}]
