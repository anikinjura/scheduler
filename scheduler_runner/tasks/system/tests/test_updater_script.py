import sys
import pytest

import scheduler_runner.tasks.system.UpdaterScript as updater

class DummyLogger:
    def __init__(self):
        self.messages = []
    def info(self, msg, *a, **k): self.messages.append(("info", msg))
    def warning(self, msg, *a, **k): self.messages.append(("warning", msg))
    def error(self, msg, *a, **k): self.messages.append(("error", msg))

@pytest.fixture
def fake_logger():
    return DummyLogger()

def test_get_local_commit_success(monkeypatch, tmp_path):
    def fake_run(*a, **k):
        class R: stdout = "abc123\n"
        return R()
    monkeypatch.setattr(updater.subprocess, "run", fake_run)
    assert updater.get_local_commit(tmp_path, "main") == "abc123"

def test_get_local_commit_fail(monkeypatch, tmp_path):
    def fake_run(*a, **k): raise updater.subprocess.CalledProcessError(1, "git")
    monkeypatch.setattr(updater.subprocess, "run", fake_run)
    assert updater.get_local_commit(tmp_path, "main") is None

def test_get_remote_commit_success(monkeypatch, tmp_path):
    def fake_run(*a, **k):
        class R: stdout = "def456\trefs/heads/main\n"
        return R()
    monkeypatch.setattr(updater.subprocess, "run", fake_run)
    assert updater.get_remote_commit(tmp_path, "main") == "def456"

def test_get_remote_commit_fail(monkeypatch, tmp_path):
    def fake_run(*a, **k): raise updater.subprocess.CalledProcessError(1, "git")
    monkeypatch.setattr(updater.subprocess, "run", fake_run)
    assert updater.get_remote_commit(tmp_path, "main") is None

def test_pull_updates_success(monkeypatch, fake_logger, tmp_path):
    def fake_run(*a, **k):
        class R:
            returncode = 0
            stdout = "OK"
            stderr = ""
        return R()
    monkeypatch.setattr(updater.subprocess, "run", fake_run)
    assert updater.pull_updates(tmp_path, "main", fake_logger)

def test_pull_updates_fail(monkeypatch, fake_logger, tmp_path):
    def fake_run(*a, **k):
        class R:
            returncode = 1
            stdout = ""
            stderr = "fail"
        return R()
    monkeypatch.setattr(updater.subprocess, "run", fake_run)
    assert not updater.pull_updates(tmp_path, "main", fake_logger)

def test_main_no_git(monkeypatch, tmp_path):
    # .git отсутствует
    config = dict(updater.SCRIPT_CONFIG)
    config["REPO_DIR"] = str(tmp_path)
    monkeypatch.setattr(updater, "SCRIPT_CONFIG", config)
    monkeypatch.setattr(updater, "configure_logger", lambda **kwargs: DummyLogger())
    monkeypatch.setattr(sys, "argv", ["UpdaterScript.py"])
    with pytest.raises(SystemExit) as e:
        updater.main()
    assert e.value.code == 2

def test_main_no_commits(monkeypatch, tmp_path):
    # .git есть, но нет коммитов
    (tmp_path / ".git").mkdir()
    config = dict(updater.SCRIPT_CONFIG)
    config["REPO_DIR"] = str(tmp_path)
    monkeypatch.setattr(updater, "SCRIPT_CONFIG", config)
    monkeypatch.setattr(updater, "configure_logger", lambda **kwargs: DummyLogger())
    monkeypatch.setattr(updater, "get_local_commit", lambda *a, **k: None)
    monkeypatch.setattr(updater, "get_remote_commit", lambda *a, **k: None)
    monkeypatch.setattr(sys, "argv", ["UpdaterScript.py"])
    with pytest.raises(SystemExit) as e:
        updater.main()
    assert e.value.code == 3

def test_main_no_updates(monkeypatch, tmp_path):
    # .git есть, коммиты совпадают
    (tmp_path / ".git").mkdir()
    config = dict(updater.SCRIPT_CONFIG)
    config["REPO_DIR"] = str(tmp_path)
    monkeypatch.setattr(updater, "SCRIPT_CONFIG", config)
    monkeypatch.setattr(updater, "configure_logger", lambda **kwargs: DummyLogger())
    monkeypatch.setattr(updater, "get_local_commit", lambda *a, **k: "abc")
    monkeypatch.setattr(updater, "get_remote_commit", lambda *a, **k: "abc")
    monkeypatch.setattr(sys, "argv", ["UpdaterScript.py"])
    with pytest.raises(SystemExit) as e:
        updater.main()
    assert e.value.code == 0

def test_main_updates_and_pull_success(monkeypatch, tmp_path):
    # .git есть, коммиты разные, pull успешен
    (tmp_path / ".git").mkdir()
    config = dict(updater.SCRIPT_CONFIG)
    config["REPO_DIR"] = str(tmp_path)
    monkeypatch.setattr(updater, "SCRIPT_CONFIG", config)
    monkeypatch.setattr(updater, "configure_logger", lambda **kwargs: DummyLogger())
    monkeypatch.setattr(updater, "get_local_commit", lambda *a, **k: "abc")
    monkeypatch.setattr(updater, "get_remote_commit", lambda *a, **k: "def")
    monkeypatch.setattr(updater, "pull_updates", lambda *a, **k: True)
    monkeypatch.setattr(sys, "argv", ["UpdaterScript.py"])
    monkeypatch.setattr(updater.os.path, "getmtime", lambda x: 1)
    with pytest.raises(SystemExit) as e:
        updater.main()
    assert e.value.code == 0

def test_main_updates_and_pull_fail(monkeypatch, tmp_path):
    # .git есть, коммиты разные, pull неудачен
    (tmp_path / ".git").mkdir()
    config = dict(updater.SCRIPT_CONFIG)
    config["REPO_DIR"] = str(tmp_path)
    monkeypatch.setattr(updater, "SCRIPT_CONFIG", config)
    monkeypatch.setattr(updater, "configure_logger", lambda **kwargs: DummyLogger())
    monkeypatch.setattr(updater, "get_local_commit", lambda *a, **k: "abc")
    monkeypatch.setattr(updater, "get_remote_commit", lambda *a, **k: "def")
    monkeypatch.setattr(updater, "pull_updates", lambda *a, **k: False)
    monkeypatch.setattr(sys, "argv", ["UpdaterScript.py"])
    with pytest.raises(SystemExit) as e:
        updater.main()
    assert e.value.code == 4