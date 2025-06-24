import sys
import pytest

import scheduler_runner.tasks.system.UpdaterScript as updater

class DummyLogger:
    """
    Фейковый логгер для тестов.
    Сохраняет все сообщения в self.messages для последующей проверки.
    """
    def __init__(self):
        self.messages = []
    def info(self, msg, *a, **k): self.messages.append(("info", msg))
    def warning(self, msg, *a, **k): self.messages.append(("warning", msg))
    def error(self, msg, *a, **k): self.messages.append(("error", msg))
    def debug(self, msg, *a, **k): self.messages.append(("debug", msg))


@pytest.fixture
def fake_logger():
    """Фикстура для передачи фейкового логгера в тесты."""
    return DummyLogger()

def test_get_local_commit_success(monkeypatch, tmp_path):
    """
    Тестирует успешное получение локального коммита.
    Проверяет, что функция get_local_commit возвращает правильный хеш коммита,
    если subprocess.run возвращает stdout с хешем.
    Код тестируемой функции:
        result = subprocess.run(
            ["git", "rev-parse", branch],
            cwd=repo_dir,
            capture_output=True, text=True, check=True
        )
        return result.stdout.strip()
    """
    def fake_run(*a, **k):
        class R: stdout = "abc123\n"
        return R()
    monkeypatch.setattr(updater.subprocess, "run", fake_run)
    assert updater.get_local_commit(tmp_path, "main") == "abc123"

def test_get_local_commit_fail(monkeypatch, tmp_path):
    """
    Тестирует обработку ошибки при получении локального коммита.
    Проверяет, что функция возвращает None при исключении CalledProcessError.
    """
    def fake_run(*a, **k): raise updater.subprocess.CalledProcessError(1, "git")
    monkeypatch.setattr(updater.subprocess, "run", fake_run)
    assert updater.get_local_commit(tmp_path, "main") is None

def test_get_remote_commit_success(monkeypatch, tmp_path):
    """
    Тестирует успешное получение удалённого коммита.
    Проверяет, что функция get_remote_commit возвращает правильный хеш коммита,
    если subprocess.run возвращает stdout с хешем.
    Код тестируемой функции:
        result = subprocess.run(
            ["git", "ls-remote", "origin", branch],
            ...
        )
        return result.stdout.split()[0] if result.stdout else None
    """
    def fake_run(*a, **k):
        class R: stdout = "def456\trefs/heads/main\n"
        return R()
    monkeypatch.setattr(updater.subprocess, "run", fake_run)
    assert updater.get_remote_commit(tmp_path, "main") == "def456"

def test_get_remote_commit_fail(monkeypatch, tmp_path):
    """
    Тестирует обработку ошибки при получении удалённого коммита.
    Проверяет, что функция возвращает None при исключении CalledProcessError.
    """
    def fake_run(*a, **k): raise updater.subprocess.CalledProcessError(1, "git")
    monkeypatch.setattr(updater.subprocess, "run", fake_run)
    assert updater.get_remote_commit(tmp_path, "main") is None

def test_pull_updates_success(monkeypatch, fake_logger, tmp_path):
    """
    Тестирует успешный git pull.
    Проверяет, что функция pull_updates возвращает True при успешном pull.
    Код тестируемой функции:
        result = subprocess.run(
            ["git", "pull", "origin", branch],
            ...
        )
        logger.debug(f"git pull stdout:\n{result.stdout}")
        if result.returncode != 0:
            logger.error(f"git pull stderr:\n{result.stderr}")
            return False
        return True
    """
    def fake_run(*a, **k):
        class R:
            returncode = 0
            stdout = "OK"
            stderr = ""
        return R()
    monkeypatch.setattr(updater.subprocess, "run", fake_run)
    assert updater.pull_updates(tmp_path, "main", fake_logger)

def test_pull_updates_fail(monkeypatch, fake_logger, tmp_path):
    """
    Тестирует неудачный git pull.
    Проверяет, что функция pull_updates возвращает False при ошибке pull.
    """
    def fake_run(*a, **k):
        class R:
            returncode = 1
            stdout = ""
            stderr = "fail"
        return R()
    monkeypatch.setattr(updater.subprocess, "run", fake_run)
    assert not updater.pull_updates(tmp_path, "main", fake_logger)

def test_ensure_origin_adds_origin(monkeypatch, fake_logger, tmp_path):
    """
    Тестирует добавление origin, если он не настроен.
    Проверяет, что вызывается команда git remote add и функция не падает.
    Код тестируемой функции:
        if result.returncode != 0:
            logger.info(f"origin не найден, настраиваем на {repo_url}")
            add_result = subprocess.run(
                ["git", "remote", "add", "origin", repo_url],
                ...
            )
            logger.debug(f"git remote add stdout:\n{add_result.stdout}")
            if add_result.returncode != 0:
                logger.error(...)
                sys.exit(5)
    """
    (tmp_path / ".git").mkdir()  # Эмулируем наличие git-репозитория
    calls = {}
    def fake_run(cmd, cwd, capture_output, text, **kwargs):
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
        class Dummy:
            returncode = 0
            stdout = ""
            stderr = ""
        return Dummy()
    monkeypatch.setattr(updater.subprocess, "run", fake_run)
    updater.ensure_origin(tmp_path, "https://github.com/anikinjura/scheduler.git", fake_logger)
    assert "add" in calls

def test_ensure_origin_wrong_url(monkeypatch, fake_logger, tmp_path):
    """
    Тестирует ситуацию, когда origin уже настроен, но url отличается.
    Проверяет, что логируется предупреждение.
    Код тестируемой функции:
        else:
            current_url = result.stdout.strip()
            logger.debug(f"origin get-url stdout: {current_url}")
            if current_url != repo_url:
                logger.warning(f"origin уже настроен на {current_url}, а не на {repo_url}")
    """
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
    """
    Тестирует ошибку при добавлении origin.
    Проверяет, что функция завершает работу с кодом 5.
    """
    (tmp_path / ".git").mkdir()
    def fake_run(cmd, cwd, capture_output, text, **kwargs):
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
    """
    Тестирует обработку неожиданного исключения в ensure_origin.
    Проверяет, что функция завершает работу с кодом 6.
    """
    def fake_run(*a, **k): raise Exception("boom")
    monkeypatch.setattr(updater.subprocess, "run", fake_run)
    with pytest.raises(SystemExit) as e:
        updater.ensure_origin(tmp_path, "https://github.com/anikinjura/scheduler.git", fake_logger)
    assert e.value.code == 6

def test_main_calls_ensure_origin(monkeypatch, tmp_path):
    """
    Тестирует, что main вызывает ensure_origin с правильными аргументами.
    Проверяет, что ensure_origin был вызван.
    Код тестируемой функции:
        ensure_origin(repo_dir, SCRIPT_CONFIG["REPO_URL"], logger)
    """
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
    """
    Тестирует завершение main с кодом 2, если .git отсутствует.
    Код тестируемой функции:
        if not (repo_dir / ".git").exists():
            logger.error("В текущей директории не найден git-репозиторий!")
            sys.exit(2)
    """
    config = dict(updater.SCRIPT_CONFIG)
    config["REPO_DIR"] = str(tmp_path)
    monkeypatch.setattr(updater, "SCRIPT_CONFIG", config)
    monkeypatch.setattr(updater, "configure_logger", lambda **kwargs: DummyLogger())
    monkeypatch.setattr(updater, "ensure_origin", lambda *a, **k: None)
    monkeypatch.setattr(sys, "argv", ["UpdaterScript.py"])
    with pytest.raises(SystemExit) as e:
        updater.main()
    assert e.value.code == 2

def test_main_no_commits(monkeypatch, tmp_path):
    """
    Тестирует завершение main с кодом 3, если не удалось получить коммиты.
    Код тестируемой функции:
        if not local_commit or not remote_commit:
            logger.error("Не удалось получить информацию о коммитах. Проверьте git и подключение к origin.")
            sys.exit(3)
    """
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
    """
    Тестирует завершение main с кодом 0, если обновлений нет (коммиты совпадают).
    Код тестируемой функции:
        if local_commit != remote_commit:
            ...
        else:
            logger.info("Обновлений не найдено.")
            sys.exit(0)
    """
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
    """
    Тестирует успешное обновление: коммиты разные, pull успешен.
    Проверяет, что main завершает работу с кодом 0.
    Код тестируемой функции:
        if local_commit != remote_commit:
            ...
            success = pull_updates(repo_dir, branch, logger)
            if success:
                logger.info("Обновление завершено успешно.")
                ...
                sys.exit(0)
    """
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
    """
    Тестирует неудачное обновление: коммиты разные, pull неудачен.
    Проверяет, что main завершает работу с кодом 4.
    Код тестируемой функции:
        if local_commit != remote_commit:
            ...
            success = pull_updates(repo_dir, branch, logger)
            if not success:
                logger.error("Ошибка при обновлении.")
                sys.exit(4)
    """
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

def test_main_restart_on_script_update(monkeypatch, tmp_path):
    """
    Тестирует логику перезапуска скрипта после обновления самого себя.
    Проверяет, что os.execv вызывается с правильными аргументами, если mtime скрипта и sys.argv[0] различаются.
    Код тестируемой функции:
        if os.path.getmtime(script_path) != os.path.getmtime(sys.argv[0]):
            logger.info("Скрипт обновился, перезапуск...")
            os.execv(sys.executable, [sys.executable] + sys.argv)
    """
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
    # Эмулируем, что скрипт обновился (mtime отличается)
    script_path = updater.Path(updater.__file__).resolve()
    def fake_getmtime(path):
        if str(path) == str(script_path):
            return 1
        if str(path) == "UpdaterScript.py":
            return 2
        return 0
    monkeypatch.setattr(updater.os.path, "getmtime", fake_getmtime)
    called = {}
    def fake_execv(exe, argv):
        called["execv"] = (exe, argv)
        raise SystemExit(0)  # чтобы не продолжать выполнение
    monkeypatch.setattr(updater.os, "execv", fake_execv)
    with pytest.raises(SystemExit):
        updater.main()
    assert "execv" in called
    assert called["execv"][0] == sys.executable
    assert called["execv"][1][0] == sys.executable
    assert "UpdaterScript.py" in called["execv"][1]