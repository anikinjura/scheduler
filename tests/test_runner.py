"""
Модуль тестов для runner.py

Здесь проверяется поведение основных функций:
- filter_tasks  — фильтрация списка задач по пользователю и имени задачи (без учёта регистра)
- execute_task  — логика запуска одной задачи: проверка расписания, запуск subprocess, обработка исключений и логирование
- main          — целевое выполнение всех задач: парсинг аргументов, фильтрация, последовательный запуск, сбор кодов выхода

Используем pytest и unittest.mock для подмены зависимостей:
- monkeypatch заменяет глобальные переменные и функции в runner
- mock.Mock() имитирует логгер для проверки вызовов logger.info, logger.error, logger.exception

Вставки кода из runner.py приведены в докстрингах для наглядности.
"""
import pytest
from pathlib import Path
from unittest import mock
import sys

# Добавляем корень проекта в sys.path для импортов (до установки пакета)
sys.path.insert(0, str(Path(__file__).parent.parent))

import scheduler_runner.runner as runner


@pytest.fixture
def dummy_schedule(monkeypatch):
    """
    Подменяет глобальный SCHEDULE и вспомогательные функции:
    - SCHEDULE содержит две задачи: TaskA (hourly) и TaskB (daily 12:00)
    - get_task_env возвращает task['env']
    - should_run_now: TaskA всегда True, TaskB всегда False (если force_run=False)
    - run_subprocess: эмулирует успешный запуск для module='tasks.dummy'
    """
    tasks = [
        {
            "name": "TaskA",
            "user": "Operator",
            "module": "tasks.dummy",
            "schedule": "hourly",
            "args": [],
            "timeout": 10,
            "env": {"A": "1"}
        },
        {
            "name": "TaskB",
            "user": "operator",
            "module": "tasks.dummy",
            "schedule": "daily",
            "time": "12:00",
            "args": [],
            "timeout": 10,
            "env": {"B": "2"}
        }
    ]
    # Подмена SCHEDULE в модуле runner
    monkeypatch.setattr(runner, "SCHEDULE", tasks)
    # get_task_env просто возвращает env из задачи
    monkeypatch.setattr(runner, "get_task_env", lambda task: task.get("env", {}))
    # should_run_now: позволяет TaskA, блокирует TaskB
    monkeypatch.setattr(runner, "should_run_now", lambda task, now: task["name"] == "TaskA")
    # run_subprocess: True для tasks.dummy, False для остальных
    monkeypatch.setattr(runner, "run_subprocess", lambda **kwargs: kwargs["script_name"] == "tasks.dummy")
    return tasks


def test_filter_tasks_case_insensitive(dummy_schedule):
    """
    Проверяем runner.filter_tasks:
    - фильтрация по полю 'user' без учёта регистра
    - опциональная фильтрация по имени задачи (task_name)

    В runner.py:
    ```python
    def filter_tasks(all_tasks, user, task_name=None):
        user_lower = user.lower()
        user_tasks = [t for t in all_tasks if t['user'].lower() == user_lower]
        if task_name:
            name_lower = task_name.lower()
            user_tasks = [t for t in user_tasks if t['name'].lower() == name_lower]
        return user_tasks
    ```
    """
    # оба пользователя 'Operator' и 'OPERATOR' должны найти две задачи
    tasks = runner.filter_tasks(runner.SCHEDULE, "operator")
    assert len(tasks) == 2
    tasks = runner.filter_tasks(runner.SCHEDULE, "OPERATOR")
    assert len(tasks) == 2
    
    # фильтрация по имени задачи 'TaskA'
    tasks = runner.filter_tasks(runner.SCHEDULE, "operator", "taska")
    assert len(tasks) == 1
    assert tasks[0]["name"] == "TaskA"


def test_filter_tasks_no_match(dummy_schedule):
    """
    Случаи, когда нет совпадений:
    - несуществующий пользователь
    - несуществующее имя задачи
    """
    # нет пользователя 'nouser'
    tasks = runner.filter_tasks(runner.SCHEDULE, "nouser")
    assert tasks == []
    # есть пользователь, но нет Task 'notask'
    tasks = runner.filter_tasks(runner.SCHEDULE, "operator", "notask")
    assert tasks == []


def test_execute_task_success(dummy_schedule):
    """
    Проверяем execute_task для успешного сценария:
    - should_run_now -> True
    - run_subprocess -> True

    В runner.py:
    ```python
    if should_run:
        logger.info(f"Старт задачи '{task_name}' (force_run={force_run})")
    ...
    success = run_subprocess(...)
    if success:
        logger.info(f"Задача '{task_name}' успешно завершена")
        return True
    ```
    """
    logger = mock.Mock()
    task = runner.SCHEDULE[0]  # TaskA
    result = runner.execute_task(task, logger, force_run=False)
    assert result is True
    # проверяем, что старт и успех залогированы
    logger.info.assert_any_call("Старт задачи 'TaskA' (force_run=False)")
    logger.info.assert_any_call("Задача 'TaskA' успешно завершена")


def test_execute_task_fail(dummy_schedule):
    """
    Сценарий неуспеха: run_subprocess возвращает False
    Ожидаем:
    - logger.error с сообщением об ошибке
    - функция возвращает False
    """
    logger = mock.Mock()
    # override run_subprocess to always False
    runner.run_subprocess = lambda **kwargs: False
    task = runner.SCHEDULE[0]
    result = runner.execute_task(task, logger, force_run=False)
    assert result is False
    logger.error.assert_any_call("Задача 'TaskA' завершилась с ошибкой")


def test_execute_task_should_run_now_false(dummy_schedule):
    """
    Когда задача не должна запускаться по расписанию:
    - should_run_now возвращает False
    Ожидаем:
    - return False
    - лог: "Задача 'TaskB' не должна запускаться сейчас по расписанию"
    """
    logger = mock.Mock()
    runner.should_run_now = lambda task, now: False
    task = runner.SCHEDULE[1]  # TaskB
    result = runner.execute_task(task, logger, force_run=False)
    assert result is False
    logger.info.assert_any_call("Задача 'TaskB' не должна запускаться сейчас по расписанию")


def test_execute_task_exception(dummy_schedule):
    """
    Исключение при запуске subprocess:
    - run_subprocess бросает исключение
    Ожидаем:
    - logger.exception вызван один раз
    - return False
    """
    logger = mock.Mock()
    runner.run_subprocess = lambda **kwargs: (_ for _ in ()).throw(RuntimeError("fail"))
    task = runner.SCHEDULE[0]
    result = runner.execute_task(task, logger, force_run=False)
    assert result is False
    assert logger.exception.call_count == 1


def test_execute_task_no_timeout_control(dummy_schedule):
    """
    Проверяем, что флаг no_timeout_control из конфига задачи
    корректно передается в run_subprocess.
    """
    logger = mock.Mock()
    task = {
        "name": "NoTimeoutTask",
        "user": "operator",
        "module": "tasks.dummy",
        "schedule": "daily",
        "time": "10:00",
        "no_timeout_control": True
    }

    with mock.patch('scheduler_runner.runner.run_subprocess') as mock_run_subprocess:
        mock_run_subprocess.return_value = True
        runner.execute_task(task, logger, force_run=True)

        # Проверяем, что run_subprocess был вызван с no_timeout_control=True
        mock_run_subprocess.assert_called_once()
        call_kwargs = mock_run_subprocess.call_args.kwargs
        assert call_kwargs.get('no_timeout_control') is True


def test_main_no_tasks(dummy_schedule):
    """
    Если после фильтрации нет задач:
    - runner.filter_tasks возвращает []
    - main должен вызвать SystemExit(2)
    """
    runner.filter_tasks = lambda *args, **kwargs: []
    runner.parse_arguments = lambda: mock.Mock(user="nouser", task=None, detailed=False)
    runner.configure_logger = lambda *args, **kwargs: mock.Mock()
    with pytest.raises(SystemExit) as e:
        runner.main()
    assert e.value.code == 2


def test_main_success(dummy_schedule):
    """
    Успешный сценарий main():
    - после фильтрации одна задача
    - execute_task возвращает True
    - main завершится SystemExit(0)
    """
    runner.filter_tasks = lambda *args, **kwargs: [runner.SCHEDULE[0]]
    runner.parse_arguments = lambda: mock.Mock(user="operator", task=None, detailed=False)
    logger = mock.Mock()
    runner.configure_logger = lambda *args, **kwargs: logger
    runner.execute_task = lambda *args, **kwargs: True
    with pytest.raises(SystemExit) as e:
        runner.main()
    assert e.value.code == 0


def test_main_fail(dummy_schedule):
    """
    Если execute_task возвращает False хотя бы для одной задачи:
    - main завершится SystemExit(1)
    """
    runner.filter_tasks = lambda *args, **kwargs: [runner.SCHEDULE[0]]
    runner.parse_arguments = lambda: mock.Mock(user="operator", task=None, detailed=False)
    logger = mock.Mock()
    runner.configure_logger = lambda *args, **kwargs: logger
    runner.execute_task = lambda *args, **kwargs: False
    with pytest.raises(SystemExit) as e:
        runner.main()
    assert e.value.code == 1


def test_main_exception(dummy_schedule):
    """
    Если внутри main() происходит непредвиденное исключение:
    - main должен перехватить его и вызвать SystemExit(3)
    """
    runner.filter_tasks = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("oops"))
    runner.parse_arguments = lambda: mock.Mock(user="operator", task=None, detailed=False)
    logger = mock.Mock()
    runner.configure_logger = lambda *args, **kwargs: logger
    with pytest.raises(SystemExit) as e:
        runner.main()
    assert e.value.code == 3

def test_sort_tasks_by_time():
    """
    Проверяем сортировку задач по общему времени (часы и минуты).
    Задачи без корректного time (None, отсутствует, некорректный формат) идут первыми.
    Остальные — по возрастанию времени (часы*60+минуты).
    """
    tasks = [
        {'name': 'A', 'time': '21:35'},
        {'name': 'B', 'time': '21:10'},
        {'name': 'C', 'time': '21:05'},
        {'name': 'D', 'time': '21:00'},
        {'name': 'E', 'time': '21:59'},
        {'name': 'F', 'time': None},
        {'name': 'G'},  # нет time
        {'name': 'H', 'time': 'bad:format'},
    ]
    sorted_tasks = runner.sort_tasks_by_time(tasks)
    # Ожидаемый порядок: F (None), G (нет time), H (bad:format), D (21:00), C (21:05), B (21:10), A (21:35), E (21:59)
    expected_order = ['F', 'G', 'H', 'D', 'C', 'B', 'A', 'E']
    assert [t['name'] for t in sorted_tasks] == expected_order