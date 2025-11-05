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
    def shutdown_computer(logger: logging.Logger, force: bool = False) -> bool:
        """
        Выключает компьютер, вызывая соответствующую команду для текущей платформы.
        
        Args:
            logger (logging.Logger): Логгер для записи информации.
            force (bool): Если True, принудительное завершение работы (опционально).

        Returns:
            bool: True если команда выключения успешно отправлена, иначе False

        Логика:
            - Windows: shutdown /s /t 60 (60 секунд для завершения логирования)
            - Linux/Unix/macOS: shutdown -h +1 (через 1 минуту, возможно с sudo)
            - Если недостаточно прав или команда не поддерживается — логируется ошибка.
        """
        try:
            if sys.platform.startswith("win"):
                command = ["shutdown", "/s", "/t", "60"]  # 60 секунд вместо 0
                if force:
                    command.append("/f")  # Принудительное завершение приложений
            elif sys.platform.startswith("linux") or sys.platform.startswith("darwin"):
                command = ["sudo", "shutdown", "-h", "1"]  # Через 1 минуту, с правами администратора
            else:
                logger.error(f"Платформа не поддерживается: {sys.platform}")
                return False

            logger.info(f"Отправлена команда выключения: {' '.join(command)}")
            result = subprocess.run(command, check=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info("Команда выключения успешно отправлена")
                return True
            else:
                logger.error(f"Команда выключения завершилась с кодом: {result.returncode}")
                if result.stderr:
                    logger.error(f"Сообщение об ошибке: {result.stderr}")
                return False

        except FileNotFoundError:
            logger.error("Команда выключения не найдена. Убедитесь, что утилита shutdown доступна.")
            return False
        except subprocess.CalledProcessError as e:
            logger.error(f"Ошибка выполнения команды выключения: {e}")
            if hasattr(e, 'stderr'):
                logger.error(f"Сообщение об ошибке: {e.stderr}")
            return False
        except Exception as e:
            logger.error(f"Непредвиденная ошибка при попытке выключения компьютера: {e}")
            return False