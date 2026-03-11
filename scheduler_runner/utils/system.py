"""
system.py

Системные утилиты для выполнения операций, связанных с управлением системой.

Возможности:
    - Выключение компьютера (shutdown) с учётом платформы (Windows, Linux, macOS)
    - Возможность расширения: перезагрузка, выход из системы и др.
    - Транслитерация кириллических строк для использования в путях файловой системы
    - Обратная конвертация транслита в кириллицу

Функции и классы:
    - SystemUtils.shutdown_computer(logger: logging.Logger, force: bool = False) -> None
        Выключает компьютер, вызывая соответствующую команду для текущей ОС.
        Логирует все этапы и ошибки.
    - SystemUtils.cyrillic_to_translit(text: str) -> str
        Конвертирует кириллический текст в транслит для использования в путях файловой системы.
    - SystemUtils.translit_to_cyrillic(text: str, uppercase_first: bool = False) -> str
        Конвертирует транслитерированный текст обратно в кириллицу.

Пример использования:
    from scheduler_runner.utils.system import SystemUtils
    SystemUtils.shutdown_computer(logger)
    safe_path_name = SystemUtils.cyrillic_to_translit("ЧЕБОКСАРЫ_182")
    # Обратная конвертация: SystemUtils.translit_to_cyrillic("cheboksary_182") → "Чебоксары_182"

Author: anikinjura
"""
__version__ = '0.0.3'

import subprocess
import logging
import sys

class SystemUtils:
    """
    Системные утилиты для выполнения операций, связанных с управлением системой.
    """

    # Словарь транслитерации (кириллица → транслит)
    CYRILLIC_TO_TRANSLIT_MAP = {
        'А': 'A', 'Б': 'B', 'В': 'V', 'Г': 'G', 'Д': 'D', 'Е': 'E', 'Ё': 'E',
        'Ж': 'Zh', 'З': 'Z', 'И': 'I', 'Й': 'Y', 'К': 'K', 'Л': 'L', 'М': 'M',
        'Н': 'N', 'О': 'O', 'П': 'P', 'Р': 'R', 'С': 'S', 'Т': 'T', 'У': 'U',
        'Ф': 'F', 'Х': 'Kh', 'Ц': 'Ts', 'Ч': 'Ch', 'Ш': 'Sh', 'Щ': 'Shch',
        'Ъ': '', 'Ы': 'Y', 'Ь': '', 'Э': 'E', 'Ю': 'Yu', 'Я': 'Ya',
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'e',
        'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
        'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
        'ф': 'f', 'х': 'kh', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'shch',
        'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya'
    }

    # Обратный словарь (транслит → кириллица) - для основных однозначных соответствий
    TRANSLIT_TO_CYRILLIC_MAP = {v: k for k, v in CYRILLIC_TO_TRANSLIT_MAP.items() if v}

    @staticmethod
    def cyrillic_to_translit(text: str) -> str:
        """
        Конвертирует кириллический текст в транслит для использования в путях файловой системы.

        Args:
            text (str): Текст на кириллице для конвертации

        Returns:
            str: Транслитерированный текст
        """
        result = ""
        for char in text:
            if char in SystemUtils.CYRILLIC_TO_TRANSLIT_MAP:
                result += SystemUtils.CYRILLIC_TO_TRANSLIT_MAP[char]
            else:
                result += char  # Оставляем символы, не входящие в кириллицу, без изменений (например, цифры и подчеркивания)
        return result

    @staticmethod
    def translit_to_cyrillic(text: str, uppercase_first: bool = False) -> str:
        """
        Конвертирует транслитерированный текст обратно в кириллицу.

        Примечание: Транслитерация неоднозначна (например, 'kh' может быть 'х' или 'кх'),
        поэтому функция использует жадный алгоритм для поиска наиболее длинных совпадений.

        Args:
            text (str): Транслитерированный текст для конвертации
            uppercase_first (bool): Если True, первая буква результата будет заглавной

        Returns:
            str: Текст в кириллице (с возможными неоднозначностями)
        """
        result = ""
        i = 0
        while i < len(text):
            # Пробуем найти максимально длинное совпадение (сначала 4 символа, потом 3, 2, 1)
            found = False
            for length in range(4, 0, -1):
                if i + length <= len(text):
                    substring = text[i:i+length]
                    # Пробуем точное совпадение
                    if substring in SystemUtils.TRANSLIT_TO_CYRILLIC_MAP:
                        result += SystemUtils.TRANSLIT_TO_CYRILLIC_MAP[substring]
                        i += length
                        found = True
                        break
                    # Пробуем совпадение без учёта регистра
                    substring_lower = substring.lower()
                    if substring_lower in SystemUtils.TRANSLIT_TO_CYRILLIC_MAP:
                        cyrillic_char = SystemUtils.TRANSLIT_TO_CYRILLIC_MAP[substring_lower]
                        # Сохраняем регистр первой буквы
                        if substring[0].isupper():
                            result += cyrillic_char.upper()
                        else:
                            result += cyrillic_char
                        i += length
                        found = True
                        break
            
            if not found:
                # Оставляем символ как есть (цифры, подчеркивания и т.д.)
                result += text[i]
                i += 1
        
        # Если нужно, делаем первую букву заглавной
        if uppercase_first and result:
            result = result[0].upper() + result[1:] if len(result) > 1 else result.upper()
        
        return result

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