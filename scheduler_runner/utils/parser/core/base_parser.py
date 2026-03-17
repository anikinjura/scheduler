"""
Базовый класс для парсинга данных из веб-систем

Архитектура:
- Универсальный класс для работы с веб-браузером Edge через Selenium
- Поддержка расширения для других браузеров в будущем
- Гибкая система конфигурации через словарь
- Поддержка различных методов аутентификации
- Расширяемая архитектура для специфичных типов парсеров
- Разделение на публичные и внутренние (с префиксом _) методы
- Использование традиционных имен методов login/logout для аутентификации
- Поддержка настраиваемых параметров через конфигурацию
- Поддержка использования существующей пользовательской сессии

Изменения в версии 0.0.1:
- Метод select_option_from_dropdown переименован в _select_option_from_dropdown
- Метод set_element_value теперь использует _select_option_from_dropdown для работы с выпадающими списками
- _select_option_from_dropdown модифицирован для поддержки работы с уже найденным элементом
"""
__version__ = '0.0.1'

import subprocess
import time
import re
import os
import json
import tempfile
import platform
import shutil
import psutil
from pathlib import Path
from abc import ABC, abstractmethod
from selenium import webdriver
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from datetime import datetime
from typing import Dict, Any, Union, Optional
from scheduler_runner.utils.logging import ensure_logger_artifacts_dir


class BaseParser(ABC):
    """Базовый класс для парсинга данных из веб-систем"""

    def __init__(self, config: Dict[str, Any], logger=None):
        """
        Инициализация базового парсера

        Args:
            config: Конфигурационный словарь с параметрами для работы парсера
            logger: Объект логгера (если не передан, будет использован внутренний логгер из config)

        Поддерживаемые параметры конфигурации:
            - DEFAULT_TIMEOUT: Таймаут ожидания элементов (по умолчанию 60)
            - ELEMENT_CLICK_TIMEOUT: Таймаут ожидания кликабельности элемента (по умолчанию 10)
            - ELEMENT_WAIT_TIMEOUT: Таймаут ожидания появления элемента (по умолчанию 10)
            - BROWSER_EXECUTABLE: Имя исполняемого файла браузера (по умолчанию 'msedge.exe')
            - BROWSER_USER_DATA_PATH_TEMPLATE: Шаблон пути к данным браузера (по умолчанию 'C:/Users/{username}/AppData/Local/Microsoft/Edge/User Data')
            - PROCESS_TERMINATION_SLEEP: Время ожидания после завершения процессов (по умолчанию 2)
            - DROPDOWN_OPEN_DELAY: Задержка после открытия выпадающего списка (по умолчанию 2)
            - PAGE_UPDATE_DELAY: Задержка после обновления страницы (по умолчанию 2)
            - PAGE_LOAD_DELAY: Задержка после загрузки страницы (по умолчанию 3)
            - EDGE_USER_DATA_DIR: Путь к пользовательским данным Edge
            - HEADLESS: Режим headless для браузера
            - table_configs: Словарь с конфигурациями для извлечения данных из таблиц.
                             Структура: {
                               'table_identifier': {
                                 'table_selector': 'xpath_to_table',
                                 'table_type': 'standard|dynamic',
                                 'table_columns': [
                                   {'name': 'col_name', 'selector': 'xpath_to_cell', 'regex': 'optional_regex'}
                                 ]
                               }
                             }
        """
        # Сохраняем config до установки логгера
        self.config = config

        # Устанавливаем логгер: если передан извне, используем его, иначе создаем внутренний
        if logger is not None:
            self.logger = logger
        elif 'logger' in config and config['logger'] is not None:
            self.logger = config['logger']
        else:
            # Создаем внутренний логгер, если ни внешний, ни из конфига не переданы
            try:
                from scheduler_runner.utils.logging import configure_logger
                self.logger = configure_logger(
                    user=self.config.get("USER", "system"),
                    task_name=self.config.get("TASK_NAME", "BaseParser"),
                    detailed=self.config.get("DETAILED_LOGS", False)
                )
            except Exception as e:
                print(f"Ошибка при создании логгера в BaseParser: {e}")
                self.logger = None

        if self.logger:
            self.logger.trace("Попали в метод BaseParser.__init__")

        self.driver = None
        self._startup_environment_logged = False

    # === АБСТРАКТНЫЕ МЕТОДЫ (обязательны для реализации в дочерних классах) ===

    @abstractmethod
    def login(self) -> bool:
        """
        Метод для входа в систему

        Returns:
            bool: True, если вход прошел успешно
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseParser.login")
        pass

    @abstractmethod
    def navigate_to_target(self) -> bool:
        """
        Метод для навигации к целевой странице

        Returns:
            bool: True, если навигация прошла успешно
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseParser.navigate_to_target")
        pass

    def extract_data(self) -> Dict[str, Any]:
        """
        Метод для извлечения данных из системы

        Returns:
            Dict[str, Any]: Извлеченные данные
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseParser.extract_data")
        pass

    @abstractmethod
    def logout(self) -> bool:
        """
        Метод для выхода из системы

        Returns:
            bool: True, если выход прошел успешно
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseParser.logout")
        pass

    # === МЕТОДЫ УПРАВЛЕНИЯ БРАУЗЕРОМ (текущая реализация для Edge, с возможностью расширения) ===

    def setup_browser(self, browser_config: Optional[Dict[str, Any]] = None) -> bool:
        """
        Настройка веб-браузера (Edge)

        Метод настраивает браузер Edge с использованием параметров из конфигурации.
        Особое внимание уделяется использованию существующего профиля пользователя:
        - Используется параметр --user-data-dir для указания директории с пользовательскими данными
        - Это позволяет использовать сохраненную сессию, cookies, настройки и авторизацию пользователя
        - Если путь к пользовательским данным не указан, используется путь к данным текущего пользователя
        - Также устанавливаются другие параметры, такие как размер окна, режим headless и т.д.

        Args:
            browser_config: Конфигурация браузера

        Returns:
            bool: True, если браузер успешно настроен
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseParser.setup_browser")
        # Получаем конфигурацию браузера
        config = browser_config or self.config.get('browser_config', {})

        if self.logger:
            self.logger.debug(f"Конфигурация браузера: {config}")
        
        # Завершаем все процессы браузера перед запуском
        self._terminate_browser_processes()

        # Получаем путь к пользовательским данным браузера
        user_data_dir = config.get('user_data_dir', self.config.get('EDGE_USER_DATA_DIR'))
        if not user_data_dir or user_data_dir == "":
            user_data_dir = self._get_default_browser_user_data_dir()
            
        if self.logger:
            self.logger.debug(f"Путь к пользовательским данным браузера: {user_data_dir}")

        # Логируем окружение старта браузера один раз за запуск парсера.
        if not self._startup_environment_logged:
            self._log_startup_environment(user_data_dir=user_data_dir, config=config)
            self._startup_environment_logged = True
            
        # Проверяем, существует ли директория с пользовательскими данными
        if not os.path.exists(user_data_dir):
            if self.logger:
                self.logger.error(f"Директория пользовательских данных браузера не существует: {user_data_dir}")
            return False

        # Создаем экземпляр драйвера Edge с повторными попытками
        max_retries = 3
        retry_delay = 3  # секунды между попытками
        self._ensure_selenium_manager_environment()

        primary_success, crash_signature_detected = self._start_edge_driver_with_retries(
            config=config,
            user_data_dir=user_data_dir,
            max_retries=max_retries,
            retry_delay=retry_delay,
            phase='primary'
        )
        if primary_success:
            return True

        requested_headless = config.get('headless', self.config.get('HEADLESS', False))
        if requested_headless and crash_signature_detected:
            if self.logger:
                self.logger.warning(
                    "BROWSER_FALLBACK_TRIGGERED: "
                    "обнаружена сигнатура падения headless старта, активируем аварийный обход headless=False"
                )

            fallback_config = dict(config)
            fallback_config['headless'] = False

            # Перед fallback принудительно очищаем остатки предыдущих попыток.
            self._terminate_browser_processes()
            fallback_success, _ = self._start_edge_driver_with_retries(
                config=fallback_config,
                user_data_dir=user_data_dir,
                max_retries=max_retries,
                retry_delay=retry_delay,
                phase='fallback'
            )
            if fallback_success:
                if self.logger:
                    self.logger.warning("BROWSER_FALLBACK_SUCCESS: браузер запущен в режиме headless=False")
                return True
            if self.logger:
                self.logger.error("BROWSER_FALLBACK_FAILED: аварийный обход headless=False не помог")

        return False

    def _start_edge_driver_with_retries(
        self,
        config: Dict[str, Any],
        user_data_dir: str,
        max_retries: int,
        retry_delay: int,
        phase: str
    ) -> tuple[bool, bool]:
        """
        Запускает Edge-драйвер с ретраями.

        Returns:
            tuple[bool, bool]:
                [0] success: успешно ли поднят драйвер,
                [1] crash_signature_detected: обнаружена ли сигнатура startup crash.
        """
        crash_signature_detected = False
        options = self._build_edge_options(config=config, user_data_dir=user_data_dir)

        for attempt in range(1, max_retries + 1):
            try:
                if self.logger:
                    if attempt == 1:
                        self.logger.debug(f"Попытка создания экземпляра драйвера Edge (phase={phase})...")
                    else:
                        self.logger.debug(
                            f"Повторная попытка #{attempt} создания экземпляра драйвера Edge (phase={phase})..."
                        )

                self._log_attempt_runtime_context(
                    attempt=attempt,
                    max_retries=max_retries,
                    user_data_dir=user_data_dir,
                    config=config,
                    options=options
                )

                self.driver = webdriver.Edge(options=options)
                if self.logger:
                    self.logger.debug(f"Экземпляр драйвера Edge успешно создан (phase={phase})")
                    if self.driver.session_id:
                        self.logger.debug(f"ID сессии драйвера: {self.driver.session_id[:10]}...")

                timeout = config.get('timeout', self.config.get('DEFAULT_TIMEOUT', 60))
                self.driver.implicitly_wait(timeout)
                if self.logger:
                    self.logger.debug(f"Установлен таймаут ожидания элементов: {timeout} секунд")
                return True, crash_signature_detected

            except Exception as e:
                if self.logger:
                    self.logger.error(
                        f"Ошибка при настройке браузера Edge (phase={phase}, попытка {attempt}/{max_retries}): {e}"
                    )
                    self.logger.error(f"Тип ошибки: {type(e).__name__}")
                    self._log_known_startup_crash_signature(error=e, attempt=attempt, max_retries=max_retries)
                    self._log_post_failed_attempt_state(user_data_dir=user_data_dir, attempt=attempt)

                is_signature = self._is_startup_crash_signature(e)
                if is_signature:
                    crash_signature_detected = True

                if attempt == max_retries:
                    if self.logger:
                        self.logger.error(
                            f"Параметры запуска браузера (phase={phase}): "
                            f"--user-data-dir={user_data_dir}, "
                            f"headless={config.get('headless', self.config.get('HEADLESS', False))}"
                        )
                        self._log_user_data_dir_diagnostics(user_data_dir=user_data_dir)
                    return False, crash_signature_detected

                if self.logger:
                    self.logger.info(f"Ожидание {retry_delay} секунд перед повторной попыткой...")

                try:
                    self._cleanup_lock_files(user_data_dir)
                except Exception as cleanup_error:
                    if self.logger:
                        self.logger.debug(f"Очистка Lock-файлов не удалась: {cleanup_error}")
                time.sleep(retry_delay)

        return False, crash_signature_detected

    def _build_edge_options(self, config: Dict[str, Any], user_data_dir: str) -> EdgeOptions:
        """Создает и возвращает EdgeOptions для запуска браузера."""
        options = EdgeOptions()
        browser_binary = self._resolve_edge_binary_location()
        if browser_binary:
            options.binary_location = browser_binary
        options.add_argument(f"--user-data-dir={user_data_dir}")
        options.add_argument("--profile-directory=Default")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        if config.get('headless', self.config.get('HEADLESS', False)):
            options.add_argument("--headless")
            if self.logger:
                self.logger.debug("Включен headless режим браузера")

        window_size = config.get('window_size', [1920, 1080])
        if window_size and len(window_size) >= 2:
            width, height = window_size[0], window_size[1]
            options.add_argument(f"--window-size={width},{height}")
            if self.logger:
                self.logger.debug(f"Установлен размер окна браузера: {width}x{height}")
        return options

    def _ensure_selenium_manager_environment(self) -> None:
        """Настраивает writable cache path для Selenium Manager в локальном окружении."""
        if os.environ.get("SE_CACHE_PATH"):
            return

        cache_root = os.path.join(tempfile.gettempdir(), "selenium-cache")
        try:
            os.makedirs(cache_root, exist_ok=True)
            os.environ["SE_CACHE_PATH"] = cache_root
            if self.logger:
                self.logger.debug(f"Установлен SE_CACHE_PATH: {cache_root}")
        except Exception as e:
            if self.logger:
                self.logger.debug(f"Не удалось установить SE_CACHE_PATH: {e}")

    def _resolve_edge_binary_location(self) -> Optional[str]:
        """Ищет установленный msedge.exe в типовых Windows paths."""
        configured_path = self.config.get("BROWSER_BINARY")
        if configured_path and os.path.exists(configured_path):
            return configured_path

        candidates = [
            r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
            r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
        ]
        for candidate in candidates:
            if os.path.exists(candidate):
                return candidate
        return None

    def _log_user_data_dir_diagnostics(self, user_data_dir: str) -> None:
        """Логирует детали по директории профиля браузера."""
        if not self.logger:
            return
        try:
            if os.path.exists(user_data_dir):
                if os.access(user_data_dir, os.R_OK):
                    self.logger.debug(f"Директория {user_data_dir} доступна для чтения")
                else:
                    self.logger.error(f"Директория {user_data_dir} НЕ доступна для чтения")

                lock_file_path = os.path.join(user_data_dir, "Default", "Lock")
                if os.path.exists(lock_file_path):
                    self.logger.error(
                        f"Файл блокировки существует: {lock_file_path}. Возможно, браузер уже запущен."
                    )
                    try:
                        os.remove(lock_file_path)
                        self.logger.info(f"Lock-файл удален: {lock_file_path}")
                    except Exception as lock_error:
                        self.logger.warning(f"Не удалось удалить Lock-файл: {lock_error}")

                local_state_path = os.path.join(user_data_dir, "Local State")
                if os.path.exists(local_state_path):
                    try:
                        with open(local_state_path, 'r', encoding='utf-8') as f:
                            local_state = json.load(f)
                            if 'profile' in local_state and 'info_cache' in local_state['profile']:
                                active_profiles = local_state['profile']['info_cache']
                                self.logger.debug(f"Найдено профилей в Local State: {len(active_profiles)}")
                    except Exception as json_error:
                        self.logger.debug(f"Не удалось прочитать Local State: {json_error}")
            else:
                self.logger.error(f"Директория пользовательских данных {user_data_dir} не существует")
        except Exception as dir_check_error:
            self.logger.error(f"Ошибка при проверке директории пользовательских данных: {dir_check_error}")

    def _log_startup_environment(self, user_data_dir: str, config: Dict[str, Any]) -> None:
        """Логирует общий контекст окружения для диагностики сбоев старта браузера."""
        if not self.logger:
            return

        current_user = self._safe_get_current_user()
        edge_version = self._get_command_output("msedge.exe --version")
        edgedriver_version = self._get_command_output("msedgedriver.exe --version")
        temp_dir = os.environ.get("TEMP", "")
        local_app_data = os.environ.get("LOCALAPPDATA", "")
        diagnostics = {
            "platform": platform.platform(),
            "python_version": platform.python_version(),
            "current_user": current_user,
            "headless": config.get('headless', self.config.get('HEADLESS', False)),
            "user_data_dir": user_data_dir,
            "temp_dir": temp_dir,
            "local_app_data": local_app_data,
            "user_data_exists": os.path.exists(user_data_dir),
            "temp_exists": os.path.exists(temp_dir) if temp_dir else False,
            "local_app_data_exists": os.path.exists(local_app_data) if local_app_data else False,
            "user_data_disk_free_mb": self._get_disk_free_mb(user_data_dir),
            "temp_disk_free_mb": self._get_disk_free_mb(temp_dir) if temp_dir else None,
            "edge_version": edge_version,
            "msedgedriver_version": edgedriver_version,
        }
        self.logger.debug(
            f"ENV_BROWSER_STARTUP_CONTEXT: {json.dumps(diagnostics, ensure_ascii=False, default=str)}"
        )

    def _log_attempt_runtime_context(
        self,
        attempt: int,
        max_retries: int,
        user_data_dir: str,
        config: Dict[str, Any],
        options: EdgeOptions
    ) -> None:
        """Логирует контекст конкретной попытки создания драйвера."""
        if not self.logger:
            return

        default_lock_path = os.path.join(user_data_dir, "Default", "Lock")
        local_state_path = os.path.join(user_data_dir, "Local State")
        runtime_context = {
            "attempt": attempt,
            "max_retries": max_retries,
            "headless": config.get('headless', self.config.get('HEADLESS', False)),
            "window_size": config.get('window_size', [1920, 1080]),
            "user_data_dir": user_data_dir,
            "lock_exists_before_start": os.path.exists(default_lock_path),
            "local_state_exists": os.path.exists(local_state_path),
            "options_arguments": list(getattr(options, "arguments", [])),
            "options_experimental": getattr(options, "experimental_options", {}),
        }
        self.logger.debug(
            f"BROWSER_START_ATTEMPT_CONTEXT: {json.dumps(runtime_context, ensure_ascii=False, default=str)}"
        )

    def _log_post_failed_attempt_state(self, user_data_dir: str, attempt: int) -> None:
        """Логирует состояние окружения сразу после неудачной попытки старта драйвера."""
        if not self.logger:
            return

        lock_path = os.path.join(user_data_dir, "Default", "Lock")
        local_state_path = os.path.join(user_data_dir, "Local State")
        post_state = {
            "attempt": attempt,
            "lock_exists_after_failure": os.path.exists(lock_path),
            "local_state_exists_after_failure": os.path.exists(local_state_path),
            "local_state_mtime": self._get_file_mtime(local_state_path),
            "msedge_process_count_after_failure": self._get_process_count("msedge.exe"),
        }
        self.logger.debug(
            f"BROWSER_POST_FAILURE_STATE: {json.dumps(post_state, ensure_ascii=False, default=str)}"
        )

    def _log_known_startup_crash_signature(self, error: Exception, attempt: int, max_retries: int) -> None:
        """Логирует маркер известного инцидента падения старта браузера."""
        if not self.logger:
            return

        message = str(error)
        if self._is_startup_crash_signature(error):
            self.logger.error(
                "BROWSER_STARTUP_CRASH_SIGNATURE: "
                f"attempt={attempt}/{max_retries}; message={message}"
            )

    @staticmethod
    def _is_startup_crash_signature(error: Exception) -> bool:
        """Проверяет, содержит ли ошибка сигнатуру падения старта браузера."""
        message = str(error)
        crash_tokens = (
            "DevToolsActivePort file doesn't exist",
            "Microsoft Edge failed to start: crashed",
            "session not created",
        )
        return any(token in message for token in crash_tokens)

    @staticmethod
    def _get_file_mtime(path: str) -> Optional[str]:
        """Возвращает mtime файла в строковом формате ISO, если файл существует."""
        try:
            if not path or not os.path.exists(path):
                return None
            return datetime.fromtimestamp(os.path.getmtime(path)).isoformat()
        except Exception:
            return None

    @staticmethod
    def _get_disk_free_mb(path: str) -> Optional[int]:
        """Возвращает объем свободного места в MB для диска, где расположен путь."""
        try:
            if not path:
                return None
            target = path
            if not os.path.exists(target):
                target = os.path.dirname(target) or target
            usage = shutil.disk_usage(target)
            return int(usage.free / (1024 * 1024))
        except Exception:
            return None

    @staticmethod
    def _get_command_output(command: str) -> str:
        """Запускает shell-команду и возвращает stdout либо сообщение об ошибке."""
        try:
            result = subprocess.run(command, capture_output=True, text=True, shell=True, timeout=5)
            if result.returncode == 0 and result.stdout:
                return result.stdout.strip()
            if result.stderr:
                return f"error: {result.stderr.strip()}"
            return f"error: returncode={result.returncode}"
        except Exception as e:
            return f"error: {type(e).__name__}: {e}"

    @staticmethod
    def _get_process_count(process_name: str) -> Optional[int]:
        """Возвращает количество процессов по имени через tasklist."""
        try:
            result = subprocess.run(
                ["tasklist", "/FI", f"IMAGENAME eq {process_name}"],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode != 0:
                return None
            lines = [line for line in result.stdout.splitlines() if process_name.lower() in line.lower()]
            return len(lines)
        except Exception:
            return None

    def _safe_get_current_user(self) -> str:
        """Безопасное получение текущего пользователя с fallback в env."""
        try:
            return self._get_current_user()
        except Exception:
            return os.environ.get("USERNAME", "unknown")

    def close_browser(self):
        """Закрытие браузера и освобождение ресурсов"""
        if self.logger:
            self.logger.trace("Попали в метод BaseParser.close_browser")
        if self.driver:
            if self.logger:
                self.logger.debug("Закрытие браузера")
                # Проверим, активна ли сессия перед закрытием
                try:
                    if self.driver.session_id:
                        self.logger.debug(f"Сессия драйвера перед закрытием: {self.driver.session_id[:10]}...")
                    else:
                        self.logger.debug("Сессия драйвера неактивна перед закрытием")
                except Exception as e:
                    self.logger.debug(f"Не удалось проверить сессию драйвера: {e}")
                    
            self.driver.quit()
            self.driver = None
            
            if self.logger:
                self.logger.debug("Браузер успешно закрыт")
        else:
            if self.logger:
                self.logger.debug("Драйвер не инициализирован, закрывать нечего")

    def dump_debug_artifacts(self, label: str) -> dict:
        """Сохраняет screenshot и HTML текущей страницы для отладки runtime-сбоев."""
        if not self.driver:
            return {}

        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        safe_label = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(label or "debug")).strip("_") or "debug"
        if self.logger:
            artifacts_dir = ensure_logger_artifacts_dir(self.logger, artifacts_subdir="artifacts")
        else:
            artifacts_dir = Path(".tmp") / "parser_debug_artifacts"
            artifacts_dir.mkdir(parents=True, exist_ok=True)

        screenshot_path = artifacts_dir / f"{timestamp}_{safe_label}.png"
        html_path = artifacts_dir / f"{timestamp}_{safe_label}.html"
        meta_path = artifacts_dir / f"{timestamp}_{safe_label}.json"

        payload = {
            "url": None,
            "title": None,
            "screenshot_path": str(screenshot_path),
            "html_path": str(html_path),
        }

        try:
            payload["url"] = self.driver.current_url
            payload["title"] = self.driver.title
        except Exception:
            pass

        try:
            self.driver.save_screenshot(str(screenshot_path))
        except Exception as e:
            payload["screenshot_error"] = str(e)

        try:
            html_path.write_text(self.driver.page_source, encoding="utf-8")
        except Exception as e:
            payload["html_error"] = str(e)

        try:
            meta_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

        if self.logger:
            self.logger.info(f"DEBUG_ARTIFACTS_SAVED: {json.dumps(payload, ensure_ascii=False)}")

        return payload

    # === УНИВЕРСАЛЬНЫЕ МЕТОДЫ РАБОТЫ С ЭЛЕМЕНТАМИ ===

    def get_element_value(self,
                         selector: str,
                         element_type: str = 'input',
                         attribute: Optional[str] = None,
                         pattern: Optional[str] = None) -> str:
        """
        Получение значения элемента

        Args:
            selector: Селектор элемента (XPath, CSS)
            element_type: Тип элемента ('input', 'textarea', 'select', 'div', etc.)
            attribute: Имя атрибута для извлечения (если None, извлекается текст или значение)
            pattern: Регулярное выражение для извлечения части текста (опционально)

        Returns:
            str: Значение элемента или пустая строка
        """
        if self.logger:
            self.logger.trace(f"Попали в метод BaseParser.get_element_value с селектором: {selector}")
        try:
            # Добавляем задержку перед поиском элемента, чтобы дать странице время загрузиться
            import time
            search_delay = self.config.get('ELEMENT_SEARCH_DELAY', 1)
            if search_delay > 0:
                if self.logger:
                    self.logger.debug(f"Задержка перед поиском элемента: {search_delay} секунд")
                time.sleep(search_delay)

            if self.logger:
                self.logger.debug(f"Поиск элемента по селектору: {selector}")
            element = self.driver.find_element(By.XPATH, selector)
            if self.logger:
                self.logger.debug(f"Элемент найден: {element.tag_name}, текст: '{element.text[:50]}...'")

            if attribute:
                # Получаем значение атрибута
                value = element.get_attribute(attribute)
                if self.logger:
                    self.logger.debug(f"Значение атрибута '{attribute}': {value}")
            elif element_type in ['input', 'textarea', 'select']:
                # Для полей ввода получаем значение атрибута 'value'
                value = element.get_attribute('value') or element.text
                if self.logger:
                    self.logger.debug(f"Значение элемента типа {element_type}: {value}")
            elif element_type in ['button', 'div', 'span', 'label', 'p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                # Для других элементов получаем текст
                value = element.text
                if self.logger:
                    self.logger.debug(f"Текст элемента типа {element_type}: {value[:100]}...")
            elif element_type in ['checkbox', 'radio']:
                # Для чекбоксов и радио-кнопок делегируем получение состояния вспомогательному методу
                value = self._get_checkbox_state(element)
                if self.logger:
                    self.logger.debug(f"Состояние чекбокса/радио: {value}")
            else:
                # По умолчанию получаем текст элемента
                value = element.text
                if self.logger:
                    self.logger.debug(f"Текст элемента по умолчанию: {value[:100]}...")

            result = value.strip() if value else ""

            # Применяем регулярное выражение, если оно задано
            if pattern and result:
                import re
                if self.logger:
                    self.logger.debug(f"Применяем регулярное выражение: {pattern}")
                matches = re.findall(pattern, result)
                if self.logger:
                    self.logger.debug(f"Найденные совпадения: {matches}")
                if matches:
                    result = matches[0]  # Берем первое совпадение
                    if self.logger:
                        self.logger.debug(f"После применения регулярного выражения: '{result}'")

            if self.logger:
                self.logger.debug(f"Возвращаемое значение: '{result}'")
            return result
        except Exception as e:
            if self.logger:
                self.logger.debug(f"Ошибка при получении значения элемента {selector}: {e}")
            if self.logger:
                self.logger.warning(f"Не удалось получить значение элемента {selector}: {e}")
                import traceback
                self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
                
                # Дополнительно проверим, доступен ли драйвер и сессия
                if hasattr(self, 'driver') and self.driver:
                    try:
                        if self.driver.session_id:
                            self.logger.debug(f"Сессия драйвера активна: {self.driver.session_id[:10]}...")
                        else:
                            self.logger.error("Сессия драйвера неактивна")
                            
                        self.logger.debug(f"Текущий URL: {self.driver.current_url}")
                        self.logger.debug(f"Заголовок страницы: {self.driver.title}")
                    except Exception as driver_check_error:
                        self.logger.error(f"Ошибка при проверке состояния драйвера: {driver_check_error}")
                else:
                    self.logger.error("Драйвер не инициализирован или недоступен")
                    
            return ""

    def set_element_value(self,
                         selector: str,
                         value: str,
                         element_type: str = 'input',
                         clear_before_set: bool = True,
                         **kwargs) -> bool:
        """
        Установка значения элемента

        Args:
            selector: Селектор элемента (XPath, CSS)
            value: Значение для установки
            element_type: Тип элемента ('input', 'textarea', 'dropdown', 'checkbox', etc.)
            clear_before_set: Очищать ли поле перед установкой значения
            **kwargs: Дополнительные аргументы для работы с выпадающими списками и другими элементами

        Returns:
            bool: True, если значение успешно установлено

        Note:
            Для элементов типа 'dropdown' вызывает метод _select_option_from_dropdown(),
            передавая ему найденный элемент и значение для установки.
            Для элементов типа 'checkbox' и 'radio' вызывает метод _set_checkbox_state().
        """
        if self.logger:
            self.logger.trace(f"Попали в метод BaseParser.set_element_value с селектором: {selector} и значением: {value}")
        try:
            if self.logger:
                self.logger.debug(f"Поиск элемента по селектору: {selector}")
            element = self.driver.find_element(By.XPATH, selector)
            if self.logger:
                self.logger.debug(f"Элемент найден: {element.tag_name}, текущее значение: '{element.get_attribute('value') or element.text[:50]}...'")

            if element_type == 'dropdown':
                # Для выпадающих списков делегируем работу вспомогательному методу
                # Если передан option_selector, используем режим с селекторами, иначе режим с элементом
                if 'option_selector' in kwargs:
                    # Используем режим с селекторами, так как нужен специфичный option_selector
                    dropdown_selector = selector
                    option_selector = kwargs.pop('option_selector')
                    if self.logger:
                        self.logger.debug(f"Вызов _select_option_from_dropdown с селекторами: dropdown={dropdown_selector}, option={option_selector}, value={value}")
                    return self._select_option_from_dropdown(
                        dropdown_selector=dropdown_selector,
                        option_selector=option_selector,
                        option_value=value,
                        **kwargs
                    )
                else:
                    # Используем режим с элементом
                    if self.logger:
                        self.logger.debug(f"Вызов _select_option_from_dropdown с элементом и значением: {value}")
                    return self._select_option_from_dropdown(element=element, option_value=value, **kwargs)
            elif element_type in ['checkbox', 'radio']:
                # Для чекбоксов и радио-кнопок делегируем работу вспомогательному методу
                target_state = value.lower() == 'true'
                if self.logger:
                    self.logger.debug(f"Установка состояния чекбокса/радио в {target_state}")
                return self._set_checkbox_state(element=element, target_state=target_state)
            else:
                # Для обычных полей ввода
                if self.logger:
                    self.logger.debug(f"Установка значения для элемента типа {element_type}, очистка перед установкой: {clear_before_set}")
                if clear_before_set:
                    element.clear()
                element.send_keys(value)

                # Проверяем, что значение действительно установлено
                current_value = element.get_attribute('value') or element.text
                result = str(current_value).strip() == str(value).strip()
                if self.logger:
                    self.logger.debug(f"Проверка результата: ожидаемое='{value}', фактическое='{current_value}', результат={result}")
                return result

        except Exception as e:
            if self.logger:
                self.logger.warning(f"Не удалось установить значение элемента {selector}: {e}")
                import traceback
                self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
                
                # Дополнительно проверим, доступен ли драйвер и сессия
                if hasattr(self, 'driver') and self.driver:
                    try:
                        if self.driver.session_id:
                            self.logger.debug(f"Сессия драйвера активна: {self.driver.session_id[:10]}...")
                        else:
                            self.logger.error("Сессия драйвера неактивна")
                            
                        self.logger.debug(f"Текущий URL: {self.driver.current_url}")
                        self.logger.debug(f"Заголовок страницы: {self.driver.title}")
                    except Exception as driver_check_error:
                        self.logger.error(f"Ошибка при проверке состояния драйвера: {driver_check_error}")
                else:
                    self.logger.error("Драйвер не инициализирован или недоступен")
                    
            return False

    def _click_element(self, selector: str, wait_for_clickable: bool = True, timeout: Optional[int] = None) -> bool:
        """
        Клик по элементу

        Args:
            selector: Селектор элемента (XPath, CSS)
            wait_for_clickable: Ждать ли, пока элемент станет кликабельным
            timeout: Время ожидания (если не указано, используется значение из конфига)

        Returns:
            bool: True, если клик прошел успешно
        """
        if self.logger:
            self.logger.trace(f"Попали в метод BaseParser._click_element с селектором: {selector}")
        # Получаем таймаут из параметра, конфига или используем значение по умолчанию
        wait_timeout = timeout or self.config.get('ELEMENT_CLICK_TIMEOUT', 10)

        if self.logger:
            self.logger.debug(f"Попытка клика по элементу: {selector}, ожидание кликабельности: {wait_for_clickable}, таймаут: {wait_timeout}")

        try:
            if wait_for_clickable:
                if self.logger:
                    self.logger.debug(f"Ожидание кликабельности элемента в течение {wait_timeout} секунд")
                element = WebDriverWait(self.driver, wait_timeout).until(
                    EC.element_to_be_clickable((By.XPATH, selector))
                )
            else:
                if self.logger:
                    self.logger.debug(f"Поиск элемента без ожидания кликабельности")
                element = self.driver.find_element(By.XPATH, selector)

            if self.logger:
                self.logger.debug(f"Элемент найден, выполнение клика")
            element.click()
            if self.logger:
                self.logger.debug(f"Клик выполнен успешно")
            return True
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Не удалось кликнуть по элементу {selector}: {e}")
                import traceback
                self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
                
                # Дополнительно проверим, доступен ли драйвер и сессия
                if hasattr(self, 'driver') and self.driver:
                    try:
                        if self.driver.session_id:
                            self.logger.debug(f"Сессия драйвера активна: {self.driver.session_id[:10]}...")
                        else:
                            self.logger.error("Сессия драйвера неактивна")
                            
                        self.logger.debug(f"Текущий URL: {self.driver.current_url}")
                        self.logger.debug(f"Заголовок страницы: {self.driver.title}")
                    except Exception as driver_check_error:
                        self.logger.error(f"Ошибка при проверке состояния драйвера: {driver_check_error}")
                else:
                    self.logger.error("Драйвер не инициализирован или недоступен")
                    
            return False


    # === ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ===

    def _cleanup_lock_files(self, user_data_dir: str) -> None:
        """
        Удаляет Lock-файлы браузера после завершения процессов.
        
        Lock-файлы создаются браузером при запуске и должны удаляться при корректном закрытии.
        Если процесс завершен принудительно, Lock-файлы могут остаться и блокировать новый запуск.
        
        Args:
            user_data_dir: Путь к директории пользовательских данных браузера
        
        Note:
            Удаление Lock-файлов безопасно и не влияет на сохраненные данные сессии (cookies, логины).
            Lock-файл — это временный индикатор активности процесса.
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseParser._cleanup_lock_files")
        
        try:
            # Основные Lock-файлы, которые могут блокировать запуск
            lock_files = [
                os.path.join(user_data_dir, "Default", "Lock"),
                os.path.join(user_data_dir, "Default", "LOCK"),
                os.path.join(user_data_dir, "Default", "SingletonLock"),
                os.path.join(user_data_dir, "SingletonLock"),
            ]
            
            removed_count = 0
            for lock_file in lock_files:
                if os.path.exists(lock_file):
                    try:
                        os.remove(lock_file)
                        if self.logger:
                            self.logger.debug(f"Удален Lock-файл: {lock_file}")
                        removed_count += 1
                    except PermissionError:
                        # Файл заблокирован другим процессом - это нормально, значит браузер еще работает
                        if self.logger:
                            self.logger.debug(f"Не удалось удалить Lock-файл (заблокирован): {lock_file}")
                    except Exception as e:
                        if self.logger:
                            self.logger.warning(f"Ошибка при удалении Lock-файла {lock_file}: {e}")
            
            if self.logger:
                if removed_count > 0:
                    self.logger.debug(f"Очищено Lock-файлов: {removed_count}")
                else:
                    self.logger.debug("Lock-файлы не обнаружены")
                    
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Ошибка при очистке Lock-файлов: {e}")

    def _terminate_browser_processes(self):
        """Очищает driver-процессы и lock-файлы, не трогая пользовательский Edge по умолчанию."""
        if self.logger:
            self.logger.trace("Попали в метод BaseParser._terminate_browser_processes")
        try:
            browser_executable = self.config.get('BROWSER_EXECUTABLE', 'msedge.exe')
            driver_executables = self.config.get('BROWSER_DRIVER_EXECUTABLES', ['msedgedriver.exe'])
            if isinstance(driver_executables, str):
                driver_executables = [driver_executables]

            force_kill_browser = self.config.get('FORCE_TERMINATE_BROWSER_PROCESSES', False)
            process_names_to_kill = list(driver_executables)
            if force_kill_browser:
                process_names_to_kill.append(browser_executable)

            if self.logger:
                self.logger.debug(
                    "Попытка завершения процессов parser bootstrap: "
                    f"drivers={driver_executables}, "
                    f"force_kill_browser={force_kill_browser}, "
                    f"browser={browser_executable}"
                )

            any_process_killed = False
            for process_name in process_names_to_kill:
                matching_processes = []
                for proc in psutil.process_iter(['pid', 'name']):
                    proc_name = (proc.info.get('name') or '').lower()
                    if process_name.lower() in proc_name:
                        matching_processes.append(proc.info)

                if matching_processes:
                    if self.logger:
                        self.logger.debug(f"Найдено запущенных процессов {process_name}: {len(matching_processes)}")
                        for proc_info in matching_processes:
                            self.logger.debug(f"  PID: {proc_info['pid']}, Name: {proc_info['name']}")
                else:
                    if self.logger:
                        self.logger.debug(f"Не найдено запущенных процессов {process_name}")
                    continue

                result = subprocess.run(
                    ["taskkill", "/f", "/im", process_name],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )

                if result.returncode != 0:
                    if self.logger:
                        self.logger.debug(f"Команда taskkill для {process_name} завершилась с кодом {result.returncode}")
                        if result.stderr:
                            self.logger.debug(f"Сообщение об ошибке: {result.stderr.strip()}")
                else:
                    any_process_killed = True
                    if self.logger:
                        self.logger.debug(f"Процессы {process_name} успешно завершены")

            if any_process_killed:
                sleep_time = self.config.get('PROCESS_TERMINATION_SLEEP', 2)
                if self.logger:
                    self.logger.debug(f"Ожидание завершения процессов: {sleep_time} секунд")
                time.sleep(sleep_time)

            # Очищаем Lock-файлы после завершения процессов
            user_data_dir = self.config.get('EDGE_USER_DATA_DIR', '')
            if not user_data_dir:
                user_data_dir = self._get_default_browser_user_data_dir()
            self._cleanup_lock_files(user_data_dir)
            
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Ошибка при завершении bootstrap-процессов браузера: {e}")

    def _get_default_browser_user_data_dir(self, username: Optional[str] = None) -> str:
        """
        Возвращает путь к пользовательским данным браузера для указанного пользователя.

        Args:
            username: Имя пользователя (если None, используется текущий пользователь)

        Returns:
            str: Путь к пользовательским данным браузера
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseParser._get_default_browser_user_data_dir")
        resolved_path = self._resolve_existing_edge_user_data_dir()
        if resolved_path:
            return resolved_path

        if username is None:
            username = self._safe_get_current_user()

        # Получаем путь к данным браузера из конфига или используем значение по умолчанию
        default_path_template = self.config.get('BROWSER_USER_DATA_PATH_TEMPLATE',
                                               "C:/Users/{username}/AppData/Local/Microsoft/Edge/User Data")
        return default_path_template.format(username=username)

    def _resolve_existing_edge_user_data_dir(self) -> Optional[str]:
        """Ищет существующую директорию профиля Edge через переменные окружения Windows."""
        local_app_data = os.environ.get("LOCALAPPDATA", "").strip()
        if local_app_data:
            candidate = os.path.join(local_app_data, "Microsoft", "Edge", "User Data")
            if os.path.exists(candidate):
                return candidate

        user_profile = os.environ.get("USERPROFILE", "").strip()
        if user_profile:
            candidate = os.path.join(user_profile, "AppData", "Local", "Microsoft", "Edge", "User Data")
            if os.path.exists(candidate):
                return candidate

        return None

    def _get_current_user(self) -> str:
        """
        Возвращает имя текущего пользователя системы.

        Returns:
            str: Имя текущего пользователя
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseParser._get_current_user")
        return (
            os.environ.get("USERNAME")
            or os.environ.get("USER")
            or os.getlogin()
        )



    def _get_checkbox_state(self, element) -> str:
        """
        Получает состояние чекбокса или радио-кнопки

        Args:
            element: WebElement чекбокса или радио-кнопки

        Returns:
            str: Состояние элемента ('True' - отмечен, 'False' - не отмечен)
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseParser._get_checkbox_state")
        try:
            # Получаем состояние элемента
            state = element.is_selected()
            if self.logger:
                self.logger.debug(f"Состояние чекбокса: {state}")
            return str(state)

        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при получении состояния чекбокса: {e}")
                import traceback
                self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
            return "False"

    def _set_checkbox_state(self, element, target_state: bool) -> bool:
        """
        Устанавливает состояние чекбокса или радио-кнопки

        Args:
            element: WebElement чекбокса или радио-кнопки
            target_state: Целевое состояние (True - отмечен, False - не отмечен)

        Returns:
            bool: True, если состояние успешно установлено
        """
        if self.logger:
            self.logger.trace(f"Попали в метод BaseParser._set_checkbox_state с целевым состоянием: {target_state}")
        try:
            # Получаем текущее состояние элемента
            current_state = element.is_selected()
            if self.logger:
                self.logger.debug(f"Текущее состояние чекбокса: {current_state}, целевое состояние: {target_state}")

            # Если текущее состояние не соответствует целевому, кликаем по элементу
            if current_state != target_state:
                if self.logger:
                    self.logger.debug("Текущее состояние не соответствует целевому, кликаем по элементу")
                element.click()

            # Проверяем, что состояние действительно стало целевым
            final_state = element.is_selected()
            result = final_state == target_state
            if self.logger:
                self.logger.debug(f"Конечное состояние: {final_state}, результат: {result}")
            return result

        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при установке состояния чекбокса: {e}")
                import traceback
                self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
            return False

    def _select_option_from_dropdown(self,
                                   dropdown_selector: str = None,
                                   option_selector: str = None,
                                   option_value: str = None,
                                   element = None,
                                   return_url: Optional[str] = None,
                                   expected_url_pattern: Optional[str] = None,
                                   exact_match: bool = False,
                                   text_attribute: Optional[str] = None,
                                   value_attribute: str = 'value') -> bool:
        """
        Универсальный метод для выбора опции в выпадающем списке

        Args:
            dropdown_selector: XPath или CSS-селектор для выпадающего списка (опционально, если передан element)
            option_selector: XPath или CSS-селектор для элементов опций (опционально, если передан element)
            option_value: Целевое значение для выбора
            element: Готовый элемент выпадающего списка (опционально, если переданы селекторы)
            return_url: URL для возврата, если произошел переход на другую странице
            expected_url_pattern: Паттерн URL для проверки, остались ли на целевой странице
            exact_match: Флаг, указывающий на необходимость точного соответствия
            text_attribute: Атрибут для получения текста (например, 'textContent')
            value_attribute: Атрибут для получения значения (например, 'value' или 'data-value')

        Returns:
            bool: True, если опция была успешно выбрана

        Note:
            Метод поддерживает два режима работы:
            1. Поиск элементов по селекторам (старый режим) - используется, когда element=None
            2. Работа с уже найденным элементом (новый режим) - используется, когда передан element
               В этом случае применяется стандартный подход через selenium.webdriver.support.ui.Select
        """
        if self.logger:
            self.logger.trace(f"Попали в метод BaseParser._select_option_from_dropdown с значением: {option_value}")
        try:
            # Если передан готовый элемент, используем его, иначе находим по селектору
            if element is not None:
                if self.logger:
                    self.logger.debug(f"Работа с переданным элементом выпадающего списка, значение: {option_value}")
                dropdown_element = element
            else:
                if self.logger:
                    self.logger.debug(f"Поиск выпадающего списка по селектору: {dropdown_selector}")
                # Кликаем по выпадающему списку, чтобы открыть опции
                click_timeout = self.config.get('ELEMENT_CLICK_TIMEOUT', 10)
                self._click_element(dropdown_selector, timeout=click_timeout)

                # Ждем появление опций
                time.sleep(self.config.get('DROPDOWN_OPEN_DELAY', 2))

                if self.logger:
                    self.logger.debug(f"Поиск опций по селектору: {option_selector}")
                
                # === ДЕТАЛЬНАЯ ДИАГНОСТИКА DROPDOWN ===
                if self.logger:
                    self.logger.debug("=== ДИАГНОСТИКА DROPDOWN ===")
                    # Проверка: открыт ли dropdown
                    try:
                        # Проверка состояния dropdown элемента
                        dropdown_elem = self.driver.find_element(By.XPATH, dropdown_selector)
                        dropdown_aria = dropdown_elem.get_attribute('aria-expanded')
                        dropdown_class = dropdown_elem.get_attribute('class')
                        self.logger.debug(f"Dropdown элемент: aria-expanded='{dropdown_aria}', class='{dropdown_class[:80] if dropdown_class else None}...'")
                    except Exception as check_err:
                        self.logger.debug(f"Не удалось проверить dropdown: {check_err}")
                
                # Пытаемся найти все доступные опции в выпадающем списке
                all_option_elements = self.driver.find_elements(By.XPATH, option_selector)

                if self.logger:
                    self.logger.debug(f"Найдено опций: {len(all_option_elements)}")
                    
                    # === ДИАГНОСТИКА ВСЕХ НАЙДЕННЫХ ОПЦИЙ ===
                    self.logger.debug("=== СПИСОК ВСЕХ ДОСТУПНЫХ ОПЦИЙ ===")
                    for i, opt in enumerate(all_option_elements[:20]):  # Ограничим 20 для читаемости
                        try:
                            opt_text = opt.text.strip() if opt.text else '(нет текста)'
                            opt_id = opt.get_attribute('id')
                            opt_class = opt.get_attribute('class')
                            opt_aria = opt.get_attribute('aria-selected')
                            self.logger.debug(f"  Опция #{i+1}: текст='{opt_text[:60]}...', id='{opt_id}', aria-selected='{opt_aria}'")
                            if opt_class:
                                self.logger.debug(f"    class='{opt_class[:80]}...'")
                        except Exception as opt_err:
                            self.logger.debug(f"  Опция #{i+1}: ошибка диагностики - {opt_err}")
                    
                    if len(all_option_elements) == 0:
                        # Попытка найти опции альтернативными селекторами
                        self.logger.debug("Попытка найти опции альтернативными селекторами...")
                        alt_selectors = [
                            "//div[contains(@class, 'ozi__dropdown-item')]",
                            "//div[contains(@class, 'dropdown-item')]",
                            "//div[contains(@class, 'option')]",
                            "//li[contains(@class, 'option')]",
                        ]
                        for alt_sel in alt_selectors:
                            try:
                                alt_opts = self.driver.find_elements(By.XPATH, alt_sel)
                                if alt_opts:
                                    self.logger.debug(f"  Альтернативный селектор '{alt_sel}' нашел {len(alt_opts)} опций")
                            except Exception as alt_err:
                                pass

                # Ищем конкретно нужный пункт среди доступных опций
                target_option = None
                for option_element in all_option_elements:
                    # Проверяем текст элемента
                    if text_attribute:
                        option_text = option_element.get_attribute(text_attribute) or option_element.text
                    else:
                        option_text = option_element.text.strip()

                    # Проверяем значение атрибута
                    option_attr_value = option_element.get_attribute(value_attribute) if value_attribute else None

                    if self.logger:
                        self.logger.debug(f"Проверка опции: текст='{option_text}', атрибут='{option_attr_value}', искомое='{option_value}'")

                    if exact_match:
                        if option_value == option_text or (option_attr_value and option_value == option_attr_value):
                            target_option = option_element
                            if self.logger:
                                self.logger.debug(f"Найдена точная опция: {option_text}")
                            break
                    else:
                        if option_value in option_text or (option_attr_value and option_value in option_attr_value):
                            target_option = option_element
                            if self.logger:
                                self.logger.debug(f"Найдена частичная опция: {option_text}")
                            break

                if target_option:
                    if self.logger:
                        self.logger.debug(f"Выбрана опция: {target_option.text if target_option.text else 'без текста'}")
                    # Используем ActionChains для более надежного клика
                    actions = ActionChains(self.driver)
                    actions.move_to_element(target_option).click().perform()

                    # Ждем обновления страницы
                    time.sleep(self.config.get('PAGE_UPDATE_DELAY', 2))

                    # Если нужно проверить URL и вернуться при необходимости
                    if return_url and expected_url_pattern:
                        # Проверяем, остались ли мы на нужной странице
                        current_url = self.driver.current_url

                        if self.logger:
                            self.logger.debug(f"Проверка URL: текущий={current_url}, ожидаемый паттерн={expected_url_pattern}, возврат={return_url}")

                        # Если мы не на целевой странице, возвращаемся туда
                        if expected_url_pattern not in current_url:
                            if self.logger:
                                self.logger.debug(f"URL не соответствует ожидаемому, возвращаемся к: {return_url}")
                            self.driver.get(return_url)
                            # Ждем загрузки страницы
                            time.sleep(self.config.get('PAGE_LOAD_DELAY', 3))

                    return True
                else:
                    if self.logger:
                        self.logger.warning(f"❌ НЕ НАЙДЕНА опция для значения '{option_value}'")
                        self.logger.warning(f"Всего найдено опций: {len(all_option_elements)}")
                        
                        # Сохранение скриншота при проблеме с dropdown
                        try:
                            from datetime import datetime
                            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                            screenshot_path = f"./logs/reports_domain/Parser/error_dropdown_{timestamp}.png"
                            self.driver.save_screenshot(screenshot_path)
                            self.logger.error(f"📸 Скриншот dropdown сохранен: {screenshot_path}")
                        except Exception as screenshot_err:
                            self.logger.error(f"Не удалось сохранить скриншот dropdown: {screenshot_err}")
                        
                        # Выведем все доступные опции для диагностики
                        available_options = []
                        for opt in all_option_elements:
                            text = opt.text.strip()
                            attr_val = opt.get_attribute(value_attribute) if value_attribute else None
                            available_options.append(f"text='{text}', attr='{attr_val}'")
                        if available_options:
                            self.logger.debug(f"Доступные опции: {available_options}")
                    return False

            # Если был передан готовый элемент, используем стандартный способ выбора опции
            if element is not None:
                if self.logger:
                    self.logger.debug(f"Использование стандартного метода Select для элемента, значение: {option_value}")
                from selenium.webdriver.support.ui import Select
                select = Select(dropdown_element)

                # Пытаемся выбрать по значению, тексту или индексу
                try:
                    select.select_by_value(option_value)
                    if self.logger:
                        self.logger.debug(f"Выбрана опция по значению: {option_value}")
                except:
                    try:
                        select.select_by_visible_text(option_value)
                        if self.logger:
                            self.logger.debug(f"Выбрана опция по видимому тексту: {option_value}")
                    except:
                        try:
                            select.select_by_index(int(option_value))
                            if self.logger:
                                self.logger.debug(f"Выбрана опция по индексу: {int(option_value)}")
                        except:
                            if self.logger:
                                self.logger.error(f"Не удалось выбрать опцию ни по значению, ни по тексту, ни по индексу: {option_value}")
                            return False
                return True

        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при установке значения в выпадающем списке: {e}")
                import traceback
                self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
                
                # Дополнительно проверим, доступен ли драйвер и сессия
                if hasattr(self, 'driver') and self.driver:
                    try:
                        if self.driver.session_id:
                            self.logger.debug(f"Сессия драйвера активна: {self.driver.session_id[:10]}...")
                        else:
                            self.logger.error("Сессия драйвера неактивна")
                            
                        self.logger.debug(f"Текущий URL: {self.driver.current_url}")
                        self.logger.debug(f"Заголовок страницы: {self.driver.title}")
                    except Exception as driver_check_error:
                        self.logger.error(f"Ошибка при проверке состояния драйвера: {driver_check_error}")
                else:
                    self.logger.error("Драйвер не инициализирован или недоступен")
                    
            return False

    def extract_table_data(self, table_config_key: str = None, table_config: dict = None) -> list:
        """
        Извлечение данных из HTML-таблицы.

        Args:
            table_config_key: Ключ в конфигурации (self.config['table_configs']), по которому находится конфигурация таблицы.
                              Если указан, используется конфигурация из self.config['table_configs'][table_config_key].
            table_config: Явно переданная конфигурация таблицы. Если указан, используется вместо поиска по ключу.
                          Приоритет выше, чем у table_config_key.

        Returns:
            list: Список словарей, где каждый словарь представляет строку таблицы с ключами, соответствующими
                  именам колонок из конфигурации. Возвращает пустой список в случае ошибки или отсутствия данных.
        """
        if self.logger:
            self.logger.trace(f"Попали в метод BaseParser.extract_table_data с ключом: {table_config_key}")
        # Определяем, какую конфигурацию использовать
        config_to_use = None
        if table_config:
            config_to_use = table_config
        elif table_config_key and isinstance(self.config, dict):
            # Ищем table_config_key внутри self.config['table_configs']
            table_configs = self.config.get('table_configs', {})
            if table_config_key in table_configs:
                config_to_use = table_configs[table_config_key]
            else:
                if self.logger:
                    self.logger.error(f"Конфигурация таблицы не найдена для ключа '{table_config_key}' в 'table_configs'.")
                return []
        else:
            if self.logger:
                self.logger.error(f"Конфигурация таблицы не найдена. "
                                  f"Проверьте table_config_key='{table_config_key}' или передайте table_config.")
            return []

        if self.logger:
            self.logger.debug(f"Используемая конфигурация таблицы: {config_to_use}")

        # Извлекаем параметры из конфигурации
        table_selector = config_to_use.get("table_selector", "")
        columns_config = config_to_use.get("table_columns", [])
        table_type = config_to_use.get("table_type", "standard") # По умолчанию стандартная таблица

        if self.logger:
            self.logger.debug(f"Селектор таблицы: {table_selector}, тип таблицы: {table_type}, количество колонок: {len(columns_config) if isinstance(columns_config, list) else 0}")

        if not table_selector or not isinstance(columns_config, list):
            if self.logger:
                self.logger.error(f"Некорректная конфигурация таблицы по ключу '{table_config_key}'. "
                                  f"Проверьте 'table_selector' и 'table_columns'.")
            return []

        try:
            # Находим таблицу
            if self.logger:
                self.logger.debug(f"Поиск таблицы по селектору: {table_selector}")
            table_element = self.driver.find_element(By.XPATH, table_selector)
            if not table_element:
                if self.logger:
                    self.logger.warning(f"Таблица не найдена по селектору: {table_selector}")
                return []

            # Определяем тип таблицы и применяем соответствующую логику
            if table_type == 'standard':
                if self.logger:
                    self.logger.debug(f"Обработка стандартной таблицы")
                return self._extract_standard_table_data(table_element, columns_config)
            elif table_type == 'dynamic':
                # Здесь может быть логика для динамических таблиц (прокрутка, ожидание загрузки)
                # Пока используем стандартную как fallback
                if self.logger:
                     self.logger.info(f"Обработка динамической таблицы как стандартной. "
                                      f"Расширьте логику для полной поддержки динамических таблиц.")
                return self._extract_standard_table_data(table_element, columns_config)
            else:
                # Неизвестный тип, используем стандартную как fallback
                if self.logger:
                    self.logger.warning(f"Неизвестный тип таблицы '{table_type}', используем стандартную логику.")
                return self._extract_standard_table_data(table_element, columns_config)

        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при извлечении данных из таблицы (ключ: '{table_config_key}'): {e}")
                import traceback
                self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
                
                # Дополнительно проверим, доступен ли драйвер и сессия
                if hasattr(self, 'driver') and self.driver:
                    try:
                        if self.driver.session_id:
                            self.logger.debug(f"Сессия драйвера активна: {self.driver.session_id[:10]}...")
                        else:
                            self.logger.error("Сессия драйвера неактивна")
                            
                        self.logger.debug(f"Текущий URL: {self.driver.current_url}")
                        self.logger.debug(f"Заголовок страницы: {self.driver.title}")
                    except Exception as driver_check_error:
                        self.logger.error(f"Ошибка при проверке состояния драйвера: {driver_check_error}")
                else:
                    self.logger.error("Драйвер не инициализирован или недоступен")
                    
            return [] # Возвращаем пустой список в случае ошибки

    def _extract_standard_table_data(self, table_element, columns_config: list) -> list:
        """
        Внутренний метод для извлечения данных из стандартной HTML-таблицы (thead/tbody).
        Использует Selenium для поиска элементов.

        Args:
            table_element: WebElement объект таблицы.
            columns_config: Список словарей с конфигурацией колонок.

        Returns:
            list: Список словарей с данными строк.
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseParser._extract_standard_table_data")
        rows_data = []

        if self.logger:
            self.logger.debug(f"Начало извлечения данных из таблицы, количество колонок в конфигурации: {len(columns_config)}")

        # Находим все строки tbody
        try:
            if self.logger:
                self.logger.debug("Поиск строк в tbody")
            rows = table_element.find_elements(By.XPATH, ".//tbody/tr")
            if self.logger:
                self.logger.debug(f"Найдено {len(rows)} строк в tbody")
        except Exception:
            # Если tbody нет, пробуем найти все tr внутри table
            try:
                if self.logger:
                    self.logger.debug("Поиск строк в table (без tbody)")
                rows = table_element.find_elements(By.XPATH, ".//tr[not(./th)]") # Исключаем строки с th, если они есть
                if self.logger:
                    self.logger.debug(f"Найдено {len(rows)} строк в table")
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"Не удалось найти строки таблицы (tbody/tr): {e}")
                return [] # Возвращаем пустой список, если строки не найдены

        if self.logger:
            self.logger.debug(f"Обработка {len(rows)} строк таблицы")

        for i, row in enumerate(rows):
            if self.logger:
                self.logger.debug(f"Обработка строки {i+1}")
            row_data = {}
            # Проходим по каждой колонке в конфигурации
            for col_config in columns_config:
                col_name = col_config.get('name', f'column_{len(row_data)}') # Генерируем имя, если не задано
                cell_selector = col_config.get('selector')
                regex_pattern = col_config.get('regex')

                if self.logger:
                    self.logger.debug(f"Обработка колонки '{col_name}', селектор: {cell_selector}")

                if not cell_selector:
                    if self.logger:
                        self.logger.warning(f"Для колонки '{col_name}' не задан 'selector'. Пропускаем.")
                    row_data[col_name] = ""
                    continue

                try:
                    # Находим ячейку в строке
                    if self.logger:
                        self.logger.debug(f"Поиск ячейки по селектору: {cell_selector}")
                    cell_element = row.find_element(By.XPATH, cell_selector)

                    # Извлекаем текст
                    cell_text = cell_element.text.strip()

                    # Если текст пуст, пробуем получить его из атрибута textContent
                    if not cell_text:
                        cell_text_content = cell_element.get_attribute("textContent")
                        if cell_text_content:
                            cell_text = cell_text_content.strip()

                    # Применяем регулярное выражение, если задано
                    if regex_pattern:
                        import re
                        if self.logger:
                            self.logger.debug(f"Применение регулярного выражения: {regex_pattern}")
                        matches = re.findall(regex_pattern, cell_text)
                        if matches:
                            cell_text = matches[0]  # Берем первое совпадение
                            if self.logger:
                                self.logger.debug(f"Результат после применения регулярного выражения: {cell_text}")
                        else:
                            cell_text = "" # Или оставить исходный текст, если совпадений нет
                            if self.logger:
                                self.logger.debug("Совпадений по регулярному выражению не найдено")

                    if self.logger:
                        self.logger.debug(f"Извлеченный текст для колонки '{col_name}': '{cell_text}'")
                    row_data[col_name] = cell_text
                except Exception as e:
                    if self.logger:
                        self.logger.warning(f"Не удалось извлечь данные для колонки '{col_name}' в строке {i}: {e}")
                        import traceback
                        self.logger.error(f"Полный стек трейса: {traceback.format_exc()}")
                    row_data[col_name] = "" # Устанавливаем пустую строку в случае ошибки

            rows_data.append(row_data)

        if self.logger:
            self.logger.debug(f"Извлечение данных завершено, всего строк: {len(rows_data)}")
        return rows_data

    # === МЕТОД ЗАПУСКА ПАРСЕРА ===

    def run_parser(self) -> Dict[str, Any]:
        """
        Метод запуска парсера, определяющий последовательность вызова абстрактных методов

        Returns:
            Dict[str, Any]: Извлеченные данные
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseParser.run_parser")
        try:
            # 1. Настройка браузера
            if not self.setup_browser():
                raise Exception("Не удалось настроить браузер")

            # 2. Вход в систему
            if not self.login():
                raise Exception("Не удалось выполнить вход в систему")

            # 3. Навигация к целевой странице
            if not self.navigate_to_target():
                raise Exception("Не удалось выполнить навигацию к целевой странице")

            # 4. Извлечение данных
            data = self.extract_data()

            # 5. Выход из системы
            if not self.logout():
                if self.logger:
                    self.logger.warning("Не удалось корректно выйти из системы")

            return data

        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при выполнении парсинга: {e}")
            raise
        finally:
            # 6. Закрытие браузера
            self.close_browser()
