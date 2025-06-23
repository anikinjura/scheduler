import sys
import pytest
from datetime import datetime

import scheduler_runner.tasks.cameras.VideoMonitorScript as vms

@pytest.fixture
def fake_logger():
    class Logger:
        def __init__(self):
            self.messages = []
        def info(self, msg, *a, **k): self.messages.append(("info", msg))
        def warning(self, msg, *a, **k): self.messages.append(("warning", msg))
        def error(self, msg, *a, **k): self.messages.append(("error", msg))
    return Logger()

def make_cam(uid, cam_id):
    return {"uid": uid, "id": cam_id}

def test_has_recent_records_found(tmp_path, fake_logger):
    # Создаём структуру для UNV камеры: .../unv_camera/uid/YYYYMMDD/HH
    now = datetime.now()
    uid = "cam1"
    root = tmp_path
    date = now.strftime("%Y%m%d")
    hour = now.strftime("%H")
    rec_dir = root / "unv_camera" / uid / date / hour
    rec_dir.mkdir(parents=True)
    (rec_dir / "file1.mp4").write_text("data")
    assert vms.has_recent_records(
        root, uid, min_files=1, max_lookback_hours=2,
        path_builder=vms.unv_path_builder, logger=fake_logger, camera_type="UNV"
    )

def test_has_recent_records_not_found(tmp_path, fake_logger):
    # Нет ни одной папки с файлами
    now = datetime.now()
    uid = "cam2"
    root = tmp_path
    assert not vms.has_recent_records(
        root, uid, min_files=1, max_lookback_hours=2,
        path_builder=vms.unv_path_builder, logger=fake_logger, camera_type="UNV"
    )

def test_main_all_cameras_ok(monkeypatch, tmp_path):
    # Подготовка: одна UNV и одна Xiaomi камера, у обеих есть записи
    now = datetime.now()
    uid1, uid2 = "unv1", "xiaomi1"
    date = now.strftime("%Y%m%d")
    hour = now.strftime("%H")
    dt = now.strftime("%Y%m%d%H")
    # UNV
    unv_dir = tmp_path / "unv_camera" / uid1 / date / hour
    unv_dir.mkdir(parents=True)
    (unv_dir / "f.mp4").write_text("x")
    # Xiaomi
    xiaomi_dir = tmp_path / "xiaomi_camera_videos" / uid2 / dt
    xiaomi_dir.mkdir(parents=True)
    (xiaomi_dir / "f.mp4").write_text("x")

    # Переопределяем SCRIPT_CONFIG для теста
    test_config = {
        "CAMERAS": {"test": [make_cam(uid1, "unv1"), make_cam(uid2, "xiaomi1")]},
        "TOKEN": "t", "CHAT_ID": "c",
        "local": {
            "CHECK_DIR": str(tmp_path),
            "MAX_LOOKBACK_HOURS": 2,
            "DETAILED_LOGS": False,
            "USER": "test",
            "TASK_NAME": "VideoMonitorScript_local"
        }
    }
    monkeypatch.setattr(vms, "SCRIPT_CONFIG", test_config)

    # Мокаем send_telegram_message чтобы не отправлять настоящие уведомления
    monkeypatch.setattr(vms, "send_telegram_message", lambda token, chat_id, msg, logger: (True, "ok"))

    # Мокаем configure_logger чтобы не писать в файл
    class DummyLogger:
        def info(self, *a, **k): pass
        def warning(self, *a, **k): pass
        def error(self, *a, **k): pass
    monkeypatch.setattr(vms, "configure_logger", lambda **kwargs: DummyLogger())

    # Мокаем sys.argv
    monkeypatch.setattr(sys, "argv", ["VideoMonitorScript.py", "--check_type", "local"])

    # Должно пройти без ошибок и не вызвать send_telegram_message с предупреждением
    vms.main()

def test_main_missing_records(monkeypatch, tmp_path):
    # Камера без записей, должен быть вызов send_telegram_message
    uid1 = "unv2"
    test_config = {
        "CAMERAS": {"test": [make_cam(uid1, "unv2")]},
        "TOKEN": "t", "CHAT_ID": "c",
        "local": {
            "CHECK_DIR": str(tmp_path),
            "MAX_LOOKBACK_HOURS": 2,
            "DETAILED_LOGS": False,
            "USER": "test",
            "TASK_NAME": "VideoMonitorScript_local"
        }
    }
    monkeypatch.setattr(vms, "SCRIPT_CONFIG", test_config)
    called = {}
    def fake_send(token, chat_id, msg, logger):
        called["sent"] = msg
        return True, "ok"
    monkeypatch.setattr(vms, "send_telegram_message", fake_send)
    monkeypatch.setattr(vms, "configure_logger", lambda **kwargs: type("L", (), {"info": lambda *a, **k: None, "warning": lambda *a, **k: None, "error": lambda *a, **k: None})())
    monkeypatch.setattr(sys, "argv", ["VideoMonitorScript.py", "--check_type", "local"])
    vms.main()
    assert "отсутствуют записи" in called["sent"]

def test_main_no_cameras(monkeypatch):
    # Нет камер — должен быть sys.exit(1)
    test_config = {
        "CAMERAS": {},
        "TOKEN": "t", "CHAT_ID": "c",
        "local": {
            "CHECK_DIR": ".",
            "MAX_LOOKBACK_HOURS": 2,
            "DETAILED_LOGS": False,
            "USER": "test",
            "TASK_NAME": "VideoMonitorScript_local"
        }
    }
    monkeypatch.setattr(vms, "SCRIPT_CONFIG", test_config)
    monkeypatch.setattr(vms, "configure_logger", lambda **kwargs: type("L", (), {"info": lambda *a, **k: None, "warning": lambda *a, **k: None, "error": lambda *a, **k: None})())
    monkeypatch.setattr(sys, "argv", ["VideoMonitorScript.py", "--check_type", "local"])
    with pytest.raises(SystemExit) as e:
        vms.main()
    assert e.value.code == 1