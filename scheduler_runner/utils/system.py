"""
system.py

Системные утилиты для выполнения операций, связанных с управлением системой.

Возможности:
    - Выключение компьютера (shutdown) с учётом платформы (Windows, Linux, macOS)
    - Возможность расширения: перезагрузка, выход из системы и др.

Функции и классы:
    - SystemUtils.shutdown_computer(logger: logging.Logger, force: bool = False) -> None
        Выключает компьютер, вызывая соответствующую команду для текущей ОС.
        Логирует все этапы и ошибки.

Пример использования:
    from scheduler_runner.utils.system import SystemUtils
    SystemUtils.shutdown_computer(logger)

Author: anikinjura
"""
__version__ = '0.0.1'

import subprocess
import logging
import sys

class SystemUtils:
    """
    Системные утилиты для выполнения операций, связанных с управлением системой.
    """

    @staticmethod
    def shutdown_computer(logger: logging.Logger, force: bool = False) -> None:
        """
        Выключает компьютер, вызывая соответствующую команду для текущей платформы.

        Args:
            logger (logging.Logger): Логгер для записи информации.
            force (bool): Если True, принудительное завершение работы (опционально).

        Returns:
            None

        Логика:
            - Windows: shutdown /s /t 0
            - Linux/Unix/macOS: shutdown -h now (может требовать sudo)
            - Если недостаточно прав или команда не поддерживается — логируется ошибка.
        """
        try:
            if sys.platform.startswith("win"):
                command = ["shutdown", "/s", "/t", "0"]
            elif sys.platform.startswith("linux") or sys.platform.startswith("darwin"):
                command = ["shutdown", "-h", "now"]
            else:
                raise NotImplementedError("Shutdown не поддерживается для данной платформы.")

            logger.info("Инициировано выключение компьютера")
            subprocess.run(command, check=True)

        except NotImplementedError as nie:
            logger.critical(f"Неподдерживаемая платформа: {nie}")
        except subprocess.CalledProcessError as cpe:
            logger.critical(f"Ошибка выполнения команды выключения: {cpe}", exc_info=True)
        except Exception as e:
            logger.critical(f"Ошибка при попытке выключения компьютера: {e}", exc_info=True)