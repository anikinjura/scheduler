"""
cameras_paths.py
Централизованные пути поддомена cameras.
Поддерживает объектные overrides, несколько локальных корней (`LOCAL_ROOTS`)
и отдельный target для архива/съемного диска (`CAMERAS_NETWORK`).
"""
__version__ = '0.0.3'

import os
from pathlib import Path

from config.base_config import ENV_MODE, PVZ_ID
from scheduler_runner.utils.system import SystemUtils


def get_safe_pvz_path_name(pvz_id: str) -> str:
    """Convert PVZ_ID to filesystem-safe transliterated value for network paths."""
    return SystemUtils.cyrillic_to_translit(str(pvz_id))


if ENV_MODE == "production":
    default_local = Path("D:/camera")
    default_network = Path("O:/cameras") / get_safe_pvz_path_name(PVZ_ID)
    telegram_token = os.environ.get("TELEGRAM_TOKEN_PROD")
    telegram_chat_id = os.environ.get("TELEGRAM_CHAT_ID_PROD")
else:
    default_local = Path("C:/tools/scheduler/tests/TestEnvironment/D_camera")
    default_network = Path("C:/tools/scheduler/tests/TestEnvironment/O_cameras") / get_safe_pvz_path_name(PVZ_ID)
    telegram_token = os.environ.get("TELEGRAM_TOKEN_TEST")
    telegram_chat_id = os.environ.get("TELEGRAM_CHAT_ID_TEST")


# Per-PVZ path overrides.
# LOCAL_ROOTS keys are referenced by camera.root_key in cameras_list.py.
OBJECT_PATH_OVERRIDES = {
    "СОСНОВКА_10": {
        "production": {
            "LOCAL_ROOTS": {
                "local_1": Path("D:/camera"),
                "local_2": Path("E:/camera"),
                "local_3": Path("F:/camera"),
            },
            "CAMERAS_NETWORK": Path("R:/cameras") / get_safe_pvz_path_name(PVZ_ID),
        },
        "test": {
            "LOCAL_ROOTS": {
                "local_1": Path("C:/tools/scheduler/tests/TestEnvironment/D_camera"),
                "local_2": Path("C:/tools/scheduler/tests/TestEnvironment/E_camera"),
                "local_3": Path("C:/tools/scheduler/tests/TestEnvironment/F_camera"),
            },
            "CAMERAS_NETWORK": Path("C:/tools/scheduler/tests/TestEnvironment/REMOVABLE_cameras") / get_safe_pvz_path_name(PVZ_ID),
        },
    }
}

mode_key = "production" if ENV_MODE == "production" else "test"
overrides = OBJECT_PATH_OVERRIDES.get(PVZ_ID, {}).get(mode_key, {})

LOCAL_ROOTS = overrides.get("LOCAL_ROOTS", {"default": default_local})
CAMERAS_LOCAL = LOCAL_ROOTS.get("default", next(iter(LOCAL_ROOTS.values())))
CAMERAS_NETWORK = Path(overrides.get("CAMERAS_NETWORK", default_network))
TELEGRAM_TOKEN = telegram_token
TELEGRAM_CHAT_ID = telegram_chat_id

CAMERAS_PATHS = {
    "CAMERAS_LOCAL": CAMERAS_LOCAL,
    "LOCAL_ROOTS": LOCAL_ROOTS,
    "CAMERAS_NETWORK": CAMERAS_NETWORK,
    "TELEGRAM_TOKEN": TELEGRAM_TOKEN,
    "TELEGRAM_CHAT_ID": TELEGRAM_CHAT_ID,
}
