import importlib
import sys
import uuid
from pathlib import Path


def _make_local_temp_dir(name: str) -> Path:
    path = Path("C:/tools/scheduler/.tmp") / f"{name}_{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=False)
    return path


def _cleanup_local_temp_dir(path: Path) -> None:
    if not path.exists():
        return
    for child in sorted(path.rglob("*"), reverse=True):
        if child.is_file():
            child.unlink()
        else:
            child.rmdir()
    path.rmdir()


def _reload_base_config():
    sys.modules.pop("config.base_config", None)
    return importlib.import_module("config.base_config")


def test_base_config_loads_env_file_without_overwriting_existing_env(monkeypatch):
    tmp_dir = _make_local_temp_dir("base_config_env")
    try:
        pvz_config = tmp_dir / "pvz_config.ini"
        pvz_config.write_text("[DEFAULT]\nPVZ_ID=ЧЕБОКСАРЫ_144\nENV_MODE=test\n", encoding="utf-8")

        env_file = tmp_dir / "secrets.env"
        env_file.write_text(
            "TELEGRAM_TOKEN_TEST=token-from-dotenv\n"
            "TELEGRAM_CHAT_ID_TEST=chat-from-dotenv\n"
            "VK_API_VERSION=5.199\n",
            encoding="utf-8",
        )

        monkeypatch.setenv("PVZ_CONFIG_PATH", str(pvz_config))
        monkeypatch.setenv("SCHEDULER_ENV_FILE", str(env_file))
        monkeypatch.delenv("TELEGRAM_TOKEN_TEST", raising=False)
        monkeypatch.delenv("TELEGRAM_CHAT_ID_TEST", raising=False)
        monkeypatch.delenv("VK_API_VERSION", raising=False)

        module = _reload_base_config()

        assert module.ENV_FILE == env_file
        assert module.PVZ_ID == "ЧЕБОКСАРЫ_144"
        assert module.ENV_MODE == "test"
        assert module.LOADED_ENV_VARS["TELEGRAM_TOKEN_TEST"] == "token-from-dotenv"
        assert module.LOADED_ENV_VARS["TELEGRAM_CHAT_ID_TEST"] == "chat-from-dotenv"
        assert module.LOADED_ENV_VARS["VK_API_VERSION"] == "5.199"
    finally:
        _cleanup_local_temp_dir(tmp_dir)


def test_base_config_preserves_existing_environment_values(monkeypatch):
    tmp_dir = _make_local_temp_dir("base_config_fallback")
    try:
        pvz_config = tmp_dir / "pvz_config.ini"
        pvz_config.write_text("[DEFAULT]\nPVZ_ID=ЧЕБОКСАРЫ_143\nENV_MODE=production\n", encoding="utf-8")

        env_file = tmp_dir / "secrets.env"
        env_file.write_text("TELEGRAM_TOKEN_PROD=token-from-dotenv\n", encoding="utf-8")

        monkeypatch.setenv("PVZ_CONFIG_PATH", str(pvz_config))
        monkeypatch.setenv("SCHEDULER_ENV_FILE", str(env_file))
        monkeypatch.setenv("TELEGRAM_TOKEN_PROD", "token-from-process-env")

        module = _reload_base_config()

        assert module.PVZ_ID == "ЧЕБОКСАРЫ_143"
        assert module.ENV_MODE == "production"
        assert "TELEGRAM_TOKEN_PROD" not in module.LOADED_ENV_VARS
    finally:
        _cleanup_local_temp_dir(tmp_dir)
