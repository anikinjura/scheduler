"""
Модуль тестов для утилит scheduler_runner/utils:

Проверяем три подсистемы:

1. timing.should_run_now — логика расписаний (hourly, daily с указанием времени, once, ошибки формата и неподдерживаемые виды).
2. logging.configure_logger — корректное создание логгеров и файлов логов, отсутствие дублирования хендлеров.
3. subprocess.run_subprocess — запуск внешнего скрипта через subprocess.Popen, таймауты, очистка lock-файла.
4. schedule_config.get_task_env — фильтрация None-значений из env.

В тестах применяются:
- pytest для ассертов и обработки исключений
- tempfile и tmp_path для изолированной работы с файловой системой
- monkeypatch для подмены внешних зависимостей (subprocess, _is_process_running)
- mock-логгер из logging_utils для проверки поведения

Каждый тест содержит докстринг, описывающий:
- что именно проверяется,
- какие шаги из кода under test затрагиваются,
- ожидаемое поведение.

Также в теле тестов есть поясняющие комментарии.
"""
import pytest
import tempfile
import os
from datetime import datetime
from pathlib import Path
from unittest import mock

from scheduler_runner.utils import timing, subprocess as subprocess_utils, logging as logging_utils
from scheduler_runner.schedule_config import DEFAULT_TASK_ENV, get_task_env

# --- Tests for timing.should_run_now ---

def test_should_run_now_hourly():
    """
    Задача с schedule='hourly' должна всегда запускаться:
    Логика из timing.py:
    ```python
    if task['schedule'] == 'hourly':
        return True
    ```
    """
    task = {'schedule': 'hourly'}
    now = datetime.now()
    assert timing.should_run_now(task, now) is True


def test_should_run_now_daily_exact_hour():
    """
    Задача с schedule='daily' и "окном" совпадением часа запуска:
    Должна вернуть True, когда текущий час == часу задачи.

    Код под тестом в timing.py:
    ```python
    elif schedule_type == 'daily':
        time_str = task.get('time')
        try:
            hour, minute = map(int, time_str.split(':'))
        return now.hour == hour
    ```
    """
    now = datetime(2025, 1, 1, 12, 35)
    task = {'schedule': 'daily', 'time': '12:30'}
    assert timing.should_run_now(task, now) is True


def test_should_run_now_daily_wrong_hour():
    """
    Для schedule='daily' и неправильного времени должен вернуть False.
    """
    now = datetime(2025, 1, 1, 12, 35)
    task = {'schedule': 'daily', 'time': '13:00'}
    assert timing.should_run_now(task, now) is False


def test_should_run_now_daily_invalid_format():
    """
    Если формат времени невалидный ('25:00'), должна возникнуть ValueError:
    Разбор строки через `map(int, task['time'].split(':'))` бросит ValueError.
    """
    now = datetime.now()
    task = {'schedule': 'daily', 'time': '25:00'}
    with pytest.raises(ValueError):
        timing.should_run_now(task, now)


def test_should_run_now_daily_missing_time():
    """
    Если schedule='daily', но ключ 'time' отсутствует, требуется ValueError.
    """
    now = datetime.now()
    task = {'schedule': 'daily'}
    with pytest.raises(ValueError):
        timing.should_run_now(task, now)


def test_should_run_now_unsupported_schedule():
    """
    Неподдерживаемый schedule ('weekly') должен вызвать ValueError.
    Код:
    ```python
    else:
        raise ValueError(f"Unsupported schedule: {schedule}")
    ```
    """
    now = datetime.now()
    task = {'schedule': 'weekly'}
    with pytest.raises(ValueError):
        timing.should_run_now(task, now)


def test_should_run_now_once():
    """
    schedule='once' всегда возвращает False по логике:
    ```python
    if schedule=='once':
        return False
    ```
    """
    now = datetime.now()
    task = {'schedule': 'once'}
    assert timing.should_run_now(task, now) is False


# --- Tests for logging.configure_logger ---

def test_configure_logger_creates_files(tmp_path):
    """
    Проверяем, что configure_logger создает два файла:
    - YYYY-MM-DD.log (INFO+)
    - YYYY-MM-DD_detailed.log (DEBUG)
    И что записи действительно пишутся в них.
    """
    # Создаем логгер с детальной записью в tmp_path
    logger = logging_utils.configure_logger(
        user="testuser", task_name="testtask", detailed=True, logs_dir=str(tmp_path)
    )
    # Запишем сообщения разного уровня
    logger.info("Test info")
    logger.debug("Test debug")

    today = datetime.now().strftime("%Y-%m-%d")
    main_log = tmp_path / "testuser" / "testtask" / f"{today}.log"
    detailed_log = tmp_path / "testuser" / "testtask" / f"{today}_detailed.log"

    # Оба файла должны быть созданы
    assert main_log.exists(), f"Main log not found: {main_log}"
    assert detailed_log.exists(), f"Detailed log not found: {detailed_log}"

    # Проверяем содержимое файлов
    with open(main_log, encoding="utf-8") as f:
        content = f.read()
        assert "Test info" in content
        assert "Test debug" not in content  # debug не в основном логе
    with open(detailed_log, encoding="utf-8") as f:
        content = f.read()
        assert "Test debug" in content


def test_configure_logger_no_duplicate_handlers(tmp_path):
    """
    При повторном вызове configure_logger для одного user/task не должно множиться handlers.
    """
    logger1 = logging_utils.configure_logger("user", "task", logs_dir=str(tmp_path))
    handler_count1 = len(logger1.handlers)
    logger2 = logging_utils.configure_logger("user", "task", logs_dir=str(tmp_path))
    handler_count2 = len(logger2.handlers)

    # Оба логгера — один и тот же объект
    assert logger1 is logger2
    # Количество handler-ов не изменилось
    assert handler_count1 == handler_count2


# --- Tests for subprocess.run_subprocess ---

def test_run_subprocess_success(monkeypatch, tmp_path):
    """
    Проверяем успешный запуск:
    - Создаем тестовый Python-скрипт в tmp_path
    - Мокаем subprocess.Popen, чтобы вернуть DummyProc с returncode=0
    - run_subprocess должен вернуть True
    """
    # Генерим простой скрипт
    script_path = tmp_path / "test_script.py"
    script_path.write_text("print('hello')\n")
    env = {"PWD": str(tmp_path)}
    logger = logging_utils.configure_logger("user", "subproc", logs_dir=str(tmp_path))

    # DummyProc emulates subprocess.Popen
    class DummyProc:
        def __init__(self):
            self.pid = 12345
            self.returncode = 0
        def communicate(self, timeout=None):
            return ("stdout", "stderr")
        def kill(self): pass

    # Подменяем subprocess.Popen
    monkeypatch.setattr(subprocess_utils, "subprocess", __import__("subprocess"))
    monkeypatch.setattr(subprocess_utils.subprocess, "Popen", lambda *a, **k: DummyProc())

    result = subprocess_utils.run_subprocess(
        script_name="test_script", args=[], env=env,
        logger=logger, timeout=5, working_dir=str(tmp_path)
    )
    assert result is True


def test_run_subprocess_timeout(monkeypatch, tmp_path):
    """
    Если communicate бросает TimeoutExpired, run_subprocess должен ловить и возвращать False.
    """
    env = {"PWD": str(tmp_path)}
    logger = logging_utils.configure_logger("user", "subproc", logs_dir=str(tmp_path))

    # Удаляем .last_run-файл для этого сценария (идемпотентность)
    lock_dir = Path(tempfile.gettempdir())
    lock_file = lock_dir / "user_subproc_test_script.lock"
    last_run_file = lock_file.with_suffix('.last_run')
    if last_run_file.exists():
        last_run_file.unlink()

    class DummyProc:
        def __init__(self):
            self.pid = 12345
            self.returncode = 1
        def communicate(self, timeout=None):
            # эмулируем таймаут: если timeout None, подставляем 1.0.
            raise subprocess_utils.subprocess.TimeoutExpired(
                "x",
                # timeout может быть None, но класс TimeoutExpired из модуля subprocess требует, чтобы timeout был числом (float).
                timeout if timeout is not None else 1.0
            )
        def kill(self): pass

    monkeypatch.setattr(subprocess_utils, "subprocess", __import__("subprocess"))
    monkeypatch.setattr(subprocess_utils.subprocess, "Popen", lambda *a, **k: DummyProc())

    result = subprocess_utils.run_subprocess(
        script_name="test_script", args=[], env=env,
        logger=logger, timeout=1, working_dir=str(tmp_path)
    )
    assert result is False


def test_run_subprocess_lockfile_cleanup(tmp_path, monkeypatch):
    """
    Проверяем очистку lock-файла:
    - Создаем lock-файл в tempdir по шаблону user_task_script.lock
    - _is_process_running возвращает False, значит старый PID неактуален
    - После выполнения run_subprocess lock-файл должен быть удалён
    """
    env = {"PWD": str(tmp_path)}
    logger = logging_utils.configure_logger("user", "subproc", logs_dir=str(tmp_path))

    # Подготовка lock-файла
    lock_dir = Path(tempfile.gettempdir())
    lock_file = lock_dir / "user_subproc_test_script.lock"
    last_run_file = lock_file.with_suffix('.last_run')
    lock_file.write_text("999999")
    # Удаляем .last_run-файл, если он есть (чтобы не мешал идемпотентности)   
    if last_run_file.exists():
        last_run_file.unlink()

    # Мокаем проверку существования процесса: False -> file will be cleaned
    monkeypatch.setattr(subprocess_utils, "_is_process_running", lambda pid: False)

    class DummyProc:
        def __init__(self): self.pid = 12345; self.returncode = 0
        def communicate(self, timeout=None): return ("ok", "")
        def kill(self): pass

    monkeypatch.setattr(subprocess_utils, "subprocess", __import__("subprocess"))
    monkeypatch.setattr(subprocess_utils.subprocess, "Popen", lambda *a, **k: DummyProc())

    # Запуск и проверка результатов
    result = subprocess_utils.run_subprocess(
        script_name="test_script", args=[], env=env,
        logger=logger, timeout=5, working_dir=str(tmp_path)
    )
    assert result is True
    assert not lock_file.exists(), "Lock-файл не был удалён"  

def test_run_subprocess_no_timeout_control(monkeypatch, tmp_path):
    """
    Проверяем режим 'fire-and-forget' (no_timeout_control=True):
    - subprocess.Popen должен быть вызван с флагами DETACHED_PROCESS
    - process.communicate не должен вызываться
    - функция должна вернуть True
    """
    env = {"PWD": str(tmp_path)}
    logger = logging_utils.configure_logger("user", "subproc_no_timeout", logs_dir=str(tmp_path))

    mock_popen = mock.Mock()
    # Мокаем Popen, чтобы проверить аргументы вызова
    monkeypatch.setattr(subprocess_utils.subprocess, "Popen", mock_popen)

    result = subprocess_utils.run_subprocess(
        script_name="test_script", args=[], env=env,
        logger=logger, no_timeout_control=True
    )

    assert result is True
    mock_popen.assert_called_once()
    call_kwargs = mock_popen.call_args.kwargs
    
    # Проверяем, что процесс запускается как отсоединенный
    expected_flags = subprocess_utils.subprocess.DETACHED_PROCESS | subprocess_utils.subprocess.CREATE_NEW_PROCESS_GROUP
    assert call_kwargs.get('creationflags') == expected_flags
    assert call_kwargs.get('stdout') == subprocess_utils.subprocess.DEVNULL
    
    # communicate не должен был вызываться
    assert mock_popen.return_value.communicate.call_count == 0


def test_run_subprocess_idempotency(monkeypatch, tmp_path):
    """
    Проверяем, что задача не запускается повторно в одном окне (идемпотентность).
    """
    env = {"PWD": str(tmp_path)}
    logger = logging_utils.configure_logger("user", "subproc", logs_dir=str(tmp_path))
    # DummyProc для успешного запуска
    class DummyProc:
        def __init__(self): self.pid = 12345; self.returncode = 0
        def communicate(self, timeout=None): return ("ok", "")
        def kill(self): pass
    monkeypatch.setattr(subprocess_utils, "subprocess", __import__("subprocess"))
    monkeypatch.setattr(subprocess_utils.subprocess, "Popen", lambda *a, **k: DummyProc())
    # Первый запуск — должен быть True
    result1 = subprocess_utils.run_subprocess(
        script_name="test_script", args=[], env=env,
        logger=logger, timeout=5, working_dir=str(tmp_path),
        schedule_type='daily', window='2025-06-14_12'
    )
    # Второй запуск в том же окне — должен быть True, но процесс не стартует (идемпотентность)
    result2 = subprocess_utils.run_subprocess(
        script_name="test_script", args=[], env=env,
        logger=logger, timeout=5, working_dir=str(tmp_path),
        schedule_type='daily', window='2025-06-14_12'
    )
    assert result1 is True
    assert result2 is True


# --- Tests for schedule_config.get_task_env ---

def test_get_task_env_filters_none():
    """
    scheduler_runner.schedule_config:get_task_env должен исключать переменные, значения которых None:

    Код под тестом:
    ```python
        for key, value in task_env.items():
            # добавляем в env только если значение в task_env не None
            if value is not None:
                # Преобразуем значение в строку (требование для subprocess)
                env[key] = str(value)
    ```
    Удостоверяемся в том, что get_task_env возвращает:
    - Ключ "A" есть и его значение — строка "1".
    - Ключ "B" не попал (т. к. его значение было None).
    - Ключ "C" есть и его значение — строка "2".
    Т.е. проверяем именно логику фильтрации None и приведения типов в get_task_env, 
    игнорируем все остальные (валидные) переменные, которые функция тоже может возвращать (например из config.base_config).
    """
    task = {"env": {"A": "1", "B": None, "C": 2}}
    env = get_task_env(task)
    assert env["A"] == "1"
    assert "B" not in env
    assert env["C"] == "2"

def test_get_task_env_default_none_cleanup(monkeypatch):
    """
    Проверяем, что None-значения из DEFAULT_TASK_ENV тоже удаляются:

    Код под тестом:
    ```python
        # Если в DEFAULT_TASK_ENV или задаче остались ключи с None, удаляем их тоже:
        filtered_env = {k: v for k, v in env.items() if v is not None}
    ```
    - Подмешиваем в DEFAULT_TASK_ENV ключ 'X' со значением None
    - task['env'] = {} (нет специфичных переменных)
    - Вызов get_task_env не должен возвращать 'X'
    """

    # Сохраним исходный DEFAULT_TASK_ENV, чтобы вернуть его после теста
    original = DEFAULT_TASK_ENV.copy()
    try:
        # Вставим ключ X со значением None
        DEFAULT_TASK_ENV['X'] = None  # type: ignore

        # Вызов функции без специфичных переменных
        env = get_task_env({'env': {}})

        # Проверяем, что X не попал в итоговый словарь
        assert 'X' not in env, "Ключ 'X' со значением None должен быть удалён из DEFAULT_TASK_ENV"
    finally:
        # Восстанавливаем оригинальный DEFAULT_TASK_ENV
        DEFAULT_TASK_ENV.clear()
        DEFAULT_TASK_ENV.update(original)    