"""
subprocess.py

Модуль для безопасного запуска подпроцессов (скриптов задач) с поддержкой:
    - Защиты от параллельного запуска одной и той же задачи (lock-файл)
    - Идемпотентности: не запускать задачу повторно в одном "окне" (например, в одном часу)
    - Логирования stdout/stderr и событий выполнения
    - Таймаута выполнения и корректного завершения процесса

Основные функции:
    - run_subprocess(...): запускает Python-модуль как подпроцесс, возвращает True/False по результату
    - _is_process_running(pid): проверяет, активен ли процесс по PID

Пример использования:
    from scheduler_runner.utils.subprocess import run_subprocess
    result = run_subprocess(
        script_name="tasks.cameras.CopyScript",
        args=["--shutdown", "30"],
        env={"PWD": "C:/work"},
        logger=my_logger,
        timeout=300,
        working_dir="C:/work",
        schedule_type="daily",
        window="2025-06-14_21"
    )

Особенности:
    - Lock-файл и .last_run-файл создаются в temp для защиты от повторного запуска
    - ENV переменные автоматически объединяются с os.environ
    - Логируются все этапы запуска и завершения процесса

Author: anikinjura
"""
__version__ = '0.0.1'

import json
import os
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from logging import Logger
from typing import List, Dict, Optional

def _get_last_run_path(lock_file: Path) -> Path:
    return lock_file.with_suffix('.last_run')

def _was_run_in_this_window(lock_file: Path, window: str) -> bool:
    last_run_path = _get_last_run_path(lock_file)
    if last_run_path.exists():
        try:
            with last_run_path.open('r') as f:
                data = json.load(f)
            return data.get('window') == window
        except Exception:
            return False
    return False

def _mark_run_in_window(lock_file: Path, window: str):
    last_run_path = _get_last_run_path(lock_file)
    with last_run_path.open('w') as f:
        json.dump({'window': window, 'ts': datetime.now().isoformat()}, f)


def run_subprocess(
    script_name: str, 
    args: List[str], 
    env: Dict[str, str], 
    logger: Logger, 
    timeout: int = 60,
    working_dir: Optional[str] = None,
    schedule_type: Optional[str] = None,
    window: Optional[str] = None,
    no_timeout_control: bool = False
) -> bool:
    """
    Запускает Python модуль как подпроцесс с защитой от двойного запуска.
    
    Использует lock-файл для предотвращения одновременного выполнения одной задачи.
    Запускает процесс через pythonw для фонового выполнения без консоли.
    Логирует stdout и stderr на уровне DEBUG.
    
    :param script_name: имя модуля без .py (например, 'tasks.MyTask')
    :param args: список аргументов командной строки
    :param env: словарь переменных окружения для добавления
    :param logger: экземпляр Logger для записи событий
    :param timeout: таймаут выполнения в секундах
    :param working_dir: рабочая директория для процесса
    :param no_timeout_control: если True, запускает процесс и не ждет его завершения
    :return: True если код выхода 0, False в остальных случаях или при ошибке
    """
    # Создаем lock-файл для предотвращения параллельного выполнения
    lock_dir = Path(tempfile.gettempdir())
    # Безопасное имя для lock-файла (заменяем точки на подчеркивания)
    safe_logger_name = logger.name.replace('.', '_').replace('/', '_')
    lock_file = lock_dir / f"{safe_logger_name}_{script_name.replace('.', '_')}.lock"

    # Проверяем, не выполняется ли уже эта задача
    if lock_file.exists():
        try:
            with lock_file.open('r') as f:
                pid_str = f.read().strip()
                if pid_str.isdigit():
                    pid = int(pid_str)
                    if _is_process_running(pid):
                        logger.warning(f"Задача уже выполняется с PID {pid}, пропускаем")
                        return False
        except (OSError, ValueError):
            # Если не можем прочитать PID или файл поврежден, продолжаем
            pass

    # Определяем окно запуска (например, YYYY-MM-DD_HH для daily/hourly)
    if not window:
        now = datetime.now()
        if schedule_type == 'daily':
            window = now.strftime('%Y-%m-%d_%H')
        elif schedule_type == 'hourly':
            window = now.strftime('%Y-%m-%d_%H')
        else:
            window = now.strftime('%Y-%m-%d_%H-%M')

    # Проверяем идемпотентность
    if _was_run_in_this_window(lock_file, window):
        logger.info(f"Задача уже запускалась в окне {window}, повторный запуск не требуется")
        return True    

    # Подготавливаем команду для запуска
    # Используем полный путь к исполняемому файлу Python из текущего окружения
    # для обеспечения корректного импорта модулей
    import sys
    python_executable = sys.executable.replace('python.exe', 'pythonw.exe')  # Фоновое выполнение без консоли
    command = [python_executable, '-m', script_name] + args
    
    # Объединяем переменные окружения
    process_env = {**os.environ, **env} # TODO: возможно нужно process_env = {**os.environ, **{k: str(v) for k, v in env.items() if v is not None}}
    
    # Определяем корень проекта (где лежит scheduler_runner)
    project_root = str(Path(__file__).parent.parent.parent)
    cwd = project_root

    process = None
    try:
        # Запускаем процесс
        logger.info(f"Запуск подпроцесса: {' '.join(command)} (cwd={cwd})")
        
        if no_timeout_control:
            logger.info("Запуск в режиме 'fire-and-forget' (без контроля таймаута)")
            subprocess.Popen(
                command,
                cwd=cwd,
                env=process_env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
            )
            _mark_run_in_window(lock_file, window)
            return True

        process = subprocess.Popen(
            command,
            cwd=cwd,
            env=process_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Записываем PID в lock-файл
        try:
            with lock_file.open('w') as f:
                f.write(str(process.pid))
            logger.info(f"Запущен подпроцесс PID={process.pid}, команда: {' '.join(command)}")
        except OSError as e:
            logger.warning(f"Не удалось создать lock-файл: {e}")
        
        # Ожидаем завершения с таймаутом
        try:
            stdout, stderr = process.communicate(timeout=timeout)
            
            # Логируем вывод процесса
            if stdout and stdout.strip():
                for line in stdout.strip().split('\n'):
                    logger.debug(f"STDOUT: {line}")
            
            if stderr and stderr.strip():
                for line in stderr.strip().split('\n'):
                    logger.debug(f"STDERR: {line}")
                    
        except subprocess.TimeoutExpired:
            logger.error(f"Подпроцесс превысил таймаут {timeout}с, принудительное завершение")
            process.kill()
            try:
                process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                pass
            return False
        
        # Проверяем код выхода
        if process.returncode != 0:
            logger.error(f"Подпроцесс завершился с кодом {process.returncode}")
            return False
        
        logger.info("Подпроцесс успешно завершен")
        
        # Отмечаем успешный запуск в этом окне
        _mark_run_in_window(lock_file, window)        
        
        return True
        
    except Exception as e:
        logger.exception(f"Ошибка при запуске подпроцесса: {e}")
        return False
        
    finally:
        # Очищаем lock-файл
        try:
            if lock_file.exists() and not no_timeout_control:
                lock_file.unlink()
        except OSError:
            pass


def _is_process_running(pid: int) -> bool:
    """
    Проверяет, выполняется ли процесс с указанным PID.
    
    Работает на Windows и Unix-подобных системах.
    
    :param pid: идентификатор процесса
    :return: True если процесс активен, False иначе
    """
    try:
        # Используем psutil для более надежной проверки процесса на Windows
        import psutil
        return psutil.pid_exists(pid)
    except ImportError:
        # Если psutil не установлен, используем os.kill с обработкой ошибок
        try:
            os.kill(pid, 0)
            return True
        except (OSError, ProcessLookupError, SystemError):
            return False
