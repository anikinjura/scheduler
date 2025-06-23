import sys
import pytest
from pathlib import Path

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

def test_ensure_origin_adds_origin(monkeypatch, fake_logger, tmp_path):
    (tmp_path / ".git").mkdir()  # Эмулируем наличие git-репозитория
    calls = {}
    def fake_run(cmd, cwd, capture_output, text, **kwargs):
        # print("FAKE_RUN:", cmd)
        if cmd[:3] == ["git", "remote", "get-url"]:
            class GetUrlResult:
                returncode = 1
                stdout = ""
                stderr = ""
            return GetUrlResult()
        if cmd[:3] == ["git", "remote", "add"]:
            calls["add"] = True
            class AddOriginResult:
                returncode = 0
                stdout = ""
                stderr = ""
            return AddOriginResult()
        # Любая другая команда — успешно
        class Dummy:
            returncode = 0
            stdout = ""
            stderr = ""
        return Dummy()
    monkeypatch.setattr(updater.subprocess, "run", fake_run)
    updater.ensure_origin(tmp_path, "https://github.com/anikinjura/scheduler.git", fake_logger)
    assert "add" in calls

def test_ensure_origin_wrong_url(monkeypatch, fake_logger, tmp_path):
    # origin настроен, но url отличается
    def fake_run(cmd, cwd, capture_output, text, **kwargs):
        if cmd[:4] == ["git", "remote", "get-url", "origin"]:
            class R: returncode = 0; stdout = "https://other/repo.git\n"; stderr = ""
            return R()
        class Dummy: returncode = 0; stdout = ""; stderr = ""
        return Dummy()
    monkeypatch.setattr(updater.subprocess, "run", fake_run)
    updater.ensure_origin(tmp_path, "https://github.com/anikinjura/scheduler.git", fake_logger)
    assert any("origin уже настроен" in msg for level, msg in fake_logger.messages if level == "warning")

def test_ensure_origin_add_fail(monkeypatch, fake_logger, tmp_path):
    (tmp_path / ".git").mkdir()
    def fake_run(cmd, cwd, capture_output, text, **kwargs):
        # print("FAKE_RUN:", cmd)
        if cmd[:4] == ["git", "remote", "get-url", "origin"]:
            class GetUrlResult:
                returncode = 1
                stdout = ""
                stderr = ""
            return GetUrlResult()
        if cmd[:4] == ["git", "remote", "add", "origin", "https://github.com/anikinjura/scheduler.git"]:
            class AddOriginResult:
                returncode = 1
                stdout = ""
                stderr = "fail"
            return AddOriginResult()
        class Dummy:
            returncode = 1
            stdout = ""
            stderr = ""
        return Dummy()
    monkeypatch.setattr(updater.subprocess, "run", fake_run)
    with pytest.raises(SystemExit) as e:
        updater.ensure_origin(tmp_path, "https://github.com/anikinjura/scheduler.git", fake_logger)
    assert e.value.code == 5

def test_ensure_origin_exception(monkeypatch, fake_logger, tmp_path):
    # Неожиданное исключение
    def fake_run(*a, **k): raise Exception("boom")
    monkeypatch.setattr(updater.subprocess, "run", fake_run)
    with pytest.raises(SystemExit) as e:
        updater.ensure_origin(tmp_path, "https://github.com/anikinjura/scheduler.git", fake_logger)
    assert e.value.code == 6

def test_main_calls_ensure_origin(monkeypatch, tmp_path):
    called = {}
    def fake_ensure_origin(repo_dir, repo_url, logger):
        called["called"] = (repo_dir, repo_url)
    monkeypatch.setattr(updater, "ensure_origin", fake_ensure_origin)
    (tmp_path / ".git").mkdir()
    config = updater.SCRIPT_CONFIG.copy()
    config["REPO_DIR"] = str(tmp_path)
    monkeypatch.setattr(updater, "SCRIPT_CONFIG", config)
    monkeypatch.setattr(updater, "configure_logger", lambda **kwargs: DummyLogger())
    monkeypatch.setattr(updater, "get_local_commit", lambda *a, **k: "abc")
    monkeypatch.setattr(updater, "get_remote_commit", lambda *a, **k: "abc")
    monkeypatch.setattr(sys, "argv", ["UpdaterScript.py"])
    with pytest.raises(SystemExit):
        updater.main()
    assert "called" in called

def test_main_no_git(monkeypatch, tmp_path):
    # .git отсутствует
    config = dict(updater.SCRIPT_CONFIG)
    config["REPO_DIR"] = str(tmp_path)
    monkeypatch.setattr(updater, "SCRIPT_CONFIG", config)
    monkeypatch.setattr(updater, "configure_logger", lambda **kwargs: DummyLogger())
    # ensure_origin не должен падать
    monkeypatch.setattr(updater, "ensure_origin", lambda *a, **k: None)
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
    monkeypatch.setattr(updater, "ensure_origin", lambda *a, **k: None)
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
    monkeypatch.setattr(updater, "ensure_origin", lambda *a, **k: None)
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
    monkeypatch.setattr(updater, "ensure_origin", lambda *a, **k: None)
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
    monkeypatch.setattr(updater, "ensure_origin", lambda *a, **k: None)
    monkeypatch.setattr(updater, "get_local_commit", lambda *a, **k: "abc")
    monkeypatch.setattr(updater, "get_remote_commit", lambda *a, **k: "def")
    monkeypatch.setattr(updater, "pull_updates", lambda *a, **k: False)
    monkeypatch.setattr(sys, "argv", ["UpdaterScript.py"])
    with pytest.raises(SystemExit) as e:
        updater.main()
    assert e.value.code == 4