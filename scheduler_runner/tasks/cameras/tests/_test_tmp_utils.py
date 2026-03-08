from pathlib import Path
import shutil
import uuid

BASE = Path('C:/tools/scheduler/tests/TestEnvironment/.tmp_pytests')
BASE.mkdir(parents=True, exist_ok=True)


def make_temp_dir(prefix: str) -> Path:
    path = BASE / f"{prefix}_{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=False)
    return path


def cleanup_dir(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)
