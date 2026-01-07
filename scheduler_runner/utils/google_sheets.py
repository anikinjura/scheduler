"""
google_sheets.py

Универсальный модуль для работы с Google-таблицами с использованием системы конфигураций.
Предоставляет классы и функции для подключения к Google-таблице,
чтения, записи и обновления данных с использованием конфигурации таблицы.

Основные возможности:
    - Универсальный метод update_or_append_data_with_config для обновления/добавления данных
    - Поиск строк по ID с использованием get_row_by_id (только в ID-колонке)
    - Поиск строк по уникальным ключам с использованием batch-запросов (get_rows_by_unique_keys)
    - Поддержка формул в колонках
    - Валидация данных по конфигурации
    - Обработка дубликатов
    - Нормализация заголовков (регистронезависимый поиск, удаление пробелов)

Основные входные данные:
    1. Конфигурация таблицы (TableConfig):
       - Определяет структуру таблицы (имена колонок, типы, уникальные ключи)
       - Создается в вызывающем коде как объект Python

    2. Данные для записи (Dict[str, Any]):
       - Словарь с данными для записи в таблицу
       - Ключи соответствуют именам колонок из конфигурации
       - Значения - строковые представления данных

Пример использования:
    # Создание конфигурации таблицы
    config = TableConfig(
        worksheet_name="Лист1",
        id_column="id",
        columns=[
            ColumnDefinition(name="id", column_type=ColumnType.FORMULA, formula_template="=B{row}&C{row}"),
            ColumnDefinition(name="Дата", column_type=ColumnType.DATA, required=True, unique_key=True),
            ColumnDefinition(name="ПВЗ", column_type=ColumnType.DATA, required=True, unique_key=True),
            ColumnDefinition(name="Количество выдач", column_type=ColumnType.DATA)
        ],
        unique_key_columns=["Дата", "ПВЗ"]
    )

    # Подготовка данных для записи
    data = {
        "Дата": "01.01.2026",
        "ПВЗ": "TEST_PVZ",
        "Количество выдач": "1000"
    }

    # Запись данных в таблицу
    reporter = GoogleSheetsReporter(
        credentials_path="path/to/credentials.json",
        spreadsheet_name="table_id_or_name",
        table_config=config
    )
    result = reporter.update_or_append_data_with_config(data, config=config)

    # Поиск строки по ID
    row = reporter.get_row_by_id("46028TEST_PVZ", config=config)

    # Поиск строк по уникальным ключам
    rows = reporter.get_rows_by_unique_keys({"Дата": "01.01.2026", "ПВЗ": "TEST_PVZ"}, config=config)

Author: anikinjura
"""
__version__ = '0.0.3'

import gspread
from google.oauth2.service_account import Credentials
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable, TypeVar, Union
from dataclasses import dataclass, field
from enum import Enum, auto
import time
import random
from functools import wraps

from scheduler_runner.utils.logging import configure_logger


class ColumnType(Enum):
    """
    Типы колонок в таблице Google Sheets.

    Используется для семантического разделения колонок:
    - DATA: Простые данные, вводимые пользователем или из отчетов
    - FORMULA: Колонки с формулами Google Sheets
    - CALCULATED: Вычисляемые значения на стороне Python-скрипта
    - IGNORE: Колонки, которые игнорируются при операциях записи
    """
    DATA = auto()        # Простые данные
    FORMULA = auto()     # Формулы Google Sheets
    CALCULATED = auto()  # Вычисляемые значения (на стороне скрипта)
    IGNORE = auto()      # Колонки, которые игнорируем при записи


@dataclass
class ColumnDefinition:
    """
    Определение отдельной колонки таблицы.

    Атрибуты:
        name (str): Имя колонки, как оно отображается в заголовке таблицы
        column_type (ColumnType): Тип колонки (по умолчанию DATA)
        required (bool): Обязательна ли колонка для валидации (по умолчанию False)
        formula_template (Optional[str]): Шаблон формулы для колонок типа FORMULA
        unique_key (bool): Является ли колонка частью уникального ключа записи
        data_key (Optional[str]): Ключ в данных, если отличается от имени колонки
        column_letter (Optional[str]): Буква колонки (A, B, C) - вычисляется автоматически
    """
    name: str
    column_type: ColumnType = ColumnType.DATA
    required: bool = False
    formula_template: Optional[str] = None  # Шаблон формулы, например: "=B{row}&C{row}"
    unique_key: bool = False  # Является ли частью уникального ключа
    data_key: Optional[str] = None  # Ключ в данных, если отличается от имени колонки
    column_letter: Optional[str] = None  # Буква колонки (A, B, C) - вычисляем позже

    def __post_init__(self):
        """
        Валидация после инициализации.

        Проверяет:
        1. Для колонок типа FORMULA должен быть указан formula_template
        2. Имя колонки не должно быть пустым
        """
        if not self.name:
            raise ValueError("Имя колонки не может быть пустым")

        if self.column_type == ColumnType.FORMULA and not self.formula_template:
            raise ValueError(
                f"Для колонки '{self.name}' типа FORMULA необходимо указать formula_template"
            )

        # Нормализуем имя колонки (убираем лишние пробелы)
        self.name = self.name.strip()


@dataclass
class TableConfig:
    """
    Конфигурация структуры таблицы Google Sheets.

    Атрибуты:
        worksheet_name (str): Имя листа в таблице
        columns (List[ColumnDefinition]): Список определений колонок
        id_column (str): Имя колонки, содержащей идентификатор записи (по умолчанию "id")
        unique_key_columns (Optional[List[str]]): Список колонок, формирующих уникальный ключ
        id_formula_template (Optional[str]): Шаблон формулы для вычисления ID
        header_row (int): Номер строки с заголовками (по умолчанию 1)

    Внутренние атрибуты:
        _column_index_map (Dict[str, int]): Маппинг имени колонки -> индекс (начинается с 1)
        _letter_index_map (Dict[str, str]): Маппинг имени колонки -> буква колонки (A, B, C)
    """
    worksheet_name: str
    columns: List[ColumnDefinition]
    id_column: str = "id"
    unique_key_columns: Optional[List[str]] = None
    id_formula_template: Optional[str] = None  # Шаблон формулы для Id
    header_row: int = 1  # Строка с заголовками (обычно 1)

    # Внутренние поля (не инициализируются пользователем)
    _column_index_map: Dict[str, int] = field(default_factory=dict, init=False)
    _letter_index_map: Dict[str, str] = field(default_factory=dict, init=False)

    def __post_init__(self):
        """
        Инициализация после создания объекта.

        Выполняет:
        1. Автоматическое определение unique_key_columns, если не заданы явно
        2. Проверку уникальности имен колонок
        3. Проверку наличия id_column в списке колонок
        """
        # Проверяем уникальность имен колонок
        column_names = [col.name for col in self.columns]
        if len(column_names) != len(set(column_names)):
            duplicates = [name for name in column_names if column_names.count(name) > 1]
            raise ValueError(f"Обнаружены дублирующиеся имена колонок: {duplicates}")

        # Автоматически определяем unique_key_columns, если не заданы
        if self.unique_key_columns is None:
            self.unique_key_columns = [col.name for col in self.columns if col.unique_key]

        # Проверяем, что id_column существует в таблице
        if self.id_column not in column_names:
            raise ValueError(f"Колонка id_column='{self.id_column}' не найдена в списке колонок")

        # Проверяем, что все unique_key_columns существуют
        for key in self.unique_key_columns:
            if key not in column_names:
                raise ValueError(f"Колонка уникального ключа '{key}' не найдена в списке колонок")

        # Проверяем id_formula_template, если указан
        if self.id_formula_template and self.id_column:
            id_col_def = self.get_column(self.id_column)
            if id_col_def and id_col_def.column_type != ColumnType.FORMULA:
                raise ValueError(
                    f"Колонка '{self.id_column}' должна быть типа FORMULA "
                    f"если указан id_formula_template"
                )

    @property
    def column_names(self) -> List[str]:
        """Возвращает список всех имен колонок."""
        return [col.name for col in self.columns]

    @property
    def required_headers(self) -> List[str]:
        """Возвращает список обязательных заголовков."""
        return [col.name for col in self.columns if col.required]

    @property
    def data_columns(self) -> List[ColumnDefinition]:
        """Возвращает колонки с данными (DATA и CALCULATED)."""
        return [col for col in self.columns if col.column_type in (ColumnType.DATA, ColumnType.CALCULATED)]

    @property
    def formula_columns(self) -> List[ColumnDefinition]:
        """Возвращает колонки с формулами."""
        return [col for col in self.columns if col.column_type == ColumnType.FORMULA]

    def get_column(self, name: str) -> Optional[ColumnDefinition]:
        """Возвращает определение колонки по имени."""
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def is_unique_key(self, column_name: str) -> bool:
        """Проверяет, является ли колонка частью уникального ключа."""
        return column_name in (self.unique_key_columns or [])

    def get_column_index(self, column_name: str) -> Optional[int]:
        """Возвращает индекс колонки (начиная с 1) по имени."""
        return self._column_index_map.get(column_name)

    def get_column_letter(self, column_name: str) -> Optional[str]:
        """Возвращает букву колонки (A, B, C) по имени."""
        return self._letter_index_map.get(column_name)

    @classmethod
    def from_headers(cls, headers: List[str], worksheet_name: str = "Лист1", id_column: str = "id") -> 'TableConfig':
        """
        Создает базовую конфигурацию из заголовков таблицы.

        Args:
            headers: Список заголовков из таблицы
            worksheet_name: Имя листа
            id_column: Имя колонки ID

        Returns:
            TableConfig: Базовая конфигурация
        """
        columns = []
        for header in headers:
            if header == id_column:
                # Предполагаем, что ID колонка - это формула
                columns.append(ColumnDefinition(
                    name=header,
                    column_type=ColumnType.FORMULA,
                    formula_template=f"=B{{row}}&C{{row}}",  # Пример простой формулы
                    unique_key=False
                ))
            else:
                # Все остальные колонки как DATA
                columns.append(ColumnDefinition(
                    name=header,
                    column_type=ColumnType.DATA,
                    required=False,
                    unique_key=True if header in ["Дата", "ПВЗ"] else False  # Пример уникальных ключей
                ))

        return cls(
            worksheet_name=worksheet_name,
            columns=columns,
            id_column=id_column
        )

    def build_column_indexes(self, headers: List[str]):
        """
        Строит индексы колонок на основе реальных заголовков таблицы.

        Args:
            headers: Список заголовков из первой строки таблицы
        """
        self._column_index_map.clear()
        self._letter_index_map.clear()

        # Создаем маппинг имя колонки -> индекс
        for i, header in enumerate(headers):
            if header in self.column_names:
                self._column_index_map[header] = i + 1  # Индексы начинаются с 1
                self._letter_index_map[header] = _index_to_column_letter(i + 1)

        # Обновляем буквы колонок в определениях
        for col in self.columns:
            if col.name in self._letter_index_map:
                col.column_letter = self._letter_index_map[col.name]


def _index_to_column_letter(index: int) -> str:
    """
    Преобразует числовой индекс колонки в буквенное обозначение (A, B, C...).

    Args:
        index: Числовой индекс колонки (начиная с 1)

    Returns:
        Буквенное обозначение колонки
    """
    if index <= 0:
        raise ValueError("Индекс колонки должен быть положительным числом")

    result = ""
    while index > 0:
        index -= 1
        result = chr(index % 26 + ord('A')) + result
        index //= 26

    return result


def _letter_to_index(letter: str) -> int:
    """Преобразует букву колонки в числовой индекс."""
    result = 0
    for char in letter:
        result = result * 26 + (ord(char.upper()) - ord('A') + 1)
    return result




def create_kpi_table_config() -> TableConfig:
    """
    Создает конфигурацию для таблицы KPI с формулами.

    Returns:
        TableConfig: Конфигурация таблицы KPI
    """
    return TableConfig(
        worksheet_name="KPI",
        columns=[
            ColumnDefinition(
                name="id",
                column_type=ColumnType.FORMULA,
                formula_template="=B{row}&C{row}",
                unique_key=False
            ),
            ColumnDefinition(
                name="Дата",
                column_type=ColumnType.DATA,
                required=True,
                unique_key=True
            ),
            ColumnDefinition(
                name="ПВЗ",
                column_type=ColumnType.DATA,
                required=True,
                unique_key=True
            ),
            ColumnDefinition(
                name="Количество выдач",
                column_type=ColumnType.DATA,
                unique_key=False
            ),
            ColumnDefinition(
                name="Прямой поток",
                column_type=ColumnType.DATA,
                unique_key=False
            ),
            ColumnDefinition(
                name="Возвратный поток",
                column_type=ColumnType.DATA,
                unique_key=False
            )
        ],
        id_column="id",
        unique_key_columns=["Дата", "ПВЗ"],
        id_formula_template="=B{row}&C{row}"
    )


def create_basic_table_config(worksheet_name: str = "Лист1",
                              id_column: str = "id",
                              unique_keys: List[str] = None) -> TableConfig:
    """
    Создает базовую конфигурацию таблицы.

    Args:
        worksheet_name: Имя листа
        id_column: Имя колонки ID
        unique_keys: Список уникальных ключей

    Returns:
        TableConfig: Базовая конфигурация
    """
    if unique_keys is None:
        unique_keys = ["Дата", "ПВЗ"]

    return TableConfig(
        worksheet_name=worksheet_name,
        columns=[
            ColumnDefinition(
                name=id_column,
                column_type=ColumnType.FORMULA,
                formula_template="=B{row}&C{row}",
                unique_key=False
            ),
            ColumnDefinition(
                name="Дата",
                column_type=ColumnType.DATA,
                required=True,
                unique_key=True
            ),
            ColumnDefinition(
                name="ПВЗ",
                column_type=ColumnType.DATA,
                required=True,
                unique_key=True
            )
        ],
        id_column=id_column,
        unique_key_columns=unique_keys
    )


def retry_on_api_error(max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 10.0):
    """
    Декоратор для retry-механизма при ошибках API Google Sheets.

    Args:
        max_retries: Максимальное количество попыток
        base_delay: Базовая задержка в секундах
        max_delay: Максимальная задержка в секундах
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Получаем логгер из экземпляра класса, если возможно
            logger = None
            if args and hasattr(args[0], 'logger'):
                logger = args[0].logger

            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except gspread.exceptions.APIError as e:
                    last_exception = e
                    if attempt < max_retries:
                        # Exponential backoff с jitter
                        delay = min(base_delay * (2 ** attempt), max_delay)
                        jitter = random.uniform(0, 0.1 * delay)
                        actual_delay = delay + jitter

                        if logger:
                            logger.warning(f"API ошибка: {e}. Повторная попытка {attempt + 1}/{max_retries} через {actual_delay:.2f} секунд...")
                        else:
                            print(f"API ошибка: {e}. Повторная попытка {attempt + 1}/{max_retries} через {actual_delay:.2f} секунд...")
                        time.sleep(actual_delay)
                    else:
                        if logger:
                            logger.error(f"Все попытки исчерпаны. Последняя ошибка: {e}")
                        else:
                            print(f"Все попытки исчерпаны. Последняя ошибка: {e}")
                        raise last_exception
                except Exception as e:
                    # Не API ошибки не retry-им
                    raise e

            return None
        return wrapper
    return decorator


class GoogleSheetsReporter:
    """
    Класс для работы с Google-таблицами с использованием системы конфигураций.
    Предоставляет методы для подключения к Google-таблице, определения последней строки с данными,
    добавления новых строк, обновления существующих записей и валидации структуры данных.
    """

    def __init__(self, credentials_path: str, spreadsheet_name: str,
                 worksheet_name: Optional[str] = None,
                 table_config: Optional[TableConfig] = None):
        """
        Инициализация подключения к Google-таблице с поддержкой конфигурации.

        Args:
            credentials_path (str): путь к файлу учетных данных
            spreadsheet_name (str): ID или имя таблицы
            worksheet_name (Optional[str]): имя листа (если не указано в table_config)
            table_config (Optional[TableConfig]): конфигурация структуры таблицы
        """
        self.credentials_path = Path(credentials_path)
        self.spreadsheet_name = spreadsheet_name
        self.table_config = table_config

        # Определяем имя рабочего листа
        if table_config:
            self.worksheet_name = table_config.worksheet_name
        elif worksheet_name:
            self.worksheet_name = worksheet_name
        else:
            self.worksheet_name = "Лист1"  # значение по умолчанию

        # Настройка логгера
        self.logger = configure_logger(
            user="system",
            task_name="GoogleSheetsReporter",
            detailed=True
        )

        # Настройка аутентификации
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]

        credentials = Credentials.from_service_account_file(
            self.credentials_path,
            scopes=scope
        )

        # Подключение к Google Sheets
        self.gc = gspread.authorize(credentials)

        # Открытие таблицы
        if len(self.spreadsheet_name) > 20:  # Предполагаем, что это ID
            self.spreadsheet = self.gc.open_by_key(self.spreadsheet_name)
        else:
            self.spreadsheet = self.gc.open(self.spreadsheet_name)

        # Открытие рабочего листа
        self.worksheet = self.spreadsheet.worksheet(self.worksheet_name)

        # Синхронизация структуры таблицы с конфигурацией
        self._sync_table_structure()

        self.logger.info(f"Подключено к таблице: {self.spreadsheet_name}, лист: {self.worksheet_name}")
        if self.table_config:
            self.logger.info(f"Используется конфигурация таблицы с {len(self.table_config.columns)} колонками")

    @retry_on_api_error(max_retries=3, base_delay=1.0, max_delay=10.0)
    def _sync_table_structure(self) -> None:
        """
        Синхронизирует конфигурацию таблицы с реальной структурой Google Sheets.

        Выполняет:
        1. Загрузку заголовков из таблицы
        2. Построение индексов колонок в конфигурации
        3. Создание конфигурации, если она не была предоставлена

        Raises:
            gspread.exceptions.APIError: если не удалось получить доступ к таблице
            ValueError: если структура таблицы не соответствует конфигурации
        """
        try:
            # Загружаем заголовки из первой строки таблицы
            headers = self.worksheet.row_values(1)

            if not headers:
                self.logger.warning("Таблица пуста или не содержит заголовков")
                headers = []

            if self.table_config:
                # Синхронизация существующей конфигурации
                self.logger.debug(f"Синхронизация конфигурации с заголовками: {headers}")

                # Строим индексы колонок на основе реальных заголовков
                self.table_config.build_column_indexes(headers)
                self.logger.info(f"Конфигурация синхронизирована, найдено {len(self.table_config._column_index_map)} колонок")

                # Проверяем соответствие структуры
                if not self._validate_table_structure():
                    error_msg = "Структура таблицы не соответствует конфигурации"
                    self.logger.error(error_msg)
                    raise ValueError(error_msg)

            else:
                # Создаем базовую конфигурацию из заголовков
                self.logger.info("Создание базовой конфигурации из заголовков таблицы")
                self.table_config = TableConfig.from_headers(
                    headers=headers,
                    worksheet_name=self.worksheet_name
                )
                # ДОБАВИТЬ эту строку:
                self.table_config.build_column_indexes(headers)
                self.logger.info(f"Базовая конфигурация создана с {len(self.table_config.columns)} колонками")

        except gspread.exceptions.WorksheetNotFound:
            self.logger.error(f"Лист '{self.worksheet_name}' не найден в таблице")
            raise
        except Exception as e:
            self.logger.error(f"Ошибка синхронизации структуры таблицы: {e}")
            raise

    def _validate_table_structure(self) -> bool:
        """
        Проверяет соответствие структуры таблицы конфигурации.

        Returns:
            bool: True если структура корректна
        """
        if not self.table_config:
            return False

        try:
            # Получаем заголовки из таблицы
            headers = self.worksheet.row_values(1)

            # Проверяем, что все обязательные колонки из конфигурации присутствуют в таблице
            missing_columns = []
            for col_def in self.table_config.columns:
                if col_def.required and col_def.name not in headers:
                    missing_columns.append(col_def.name)

            if missing_columns:
                self.logger.error(f"Отсутствуют обязательные колонки в таблице: {missing_columns}")
                return False

            # Проверяем, что все колонки из конфигурации могут быть найдены в таблице
            for col_def in self.table_config.columns:
                if col_def.name in headers:
                    continue  # Колонка найдена
                else:
                    # Проверяем, может быть это колонка, которая не обязательна
                    self.logger.warning(f"Колонка '{col_def.name}' из конфигурации не найдена в таблице")

            self.logger.info("Структура таблицы соответствует конфигурации")
            return True

        except Exception as e:
            self.logger.error(f"Ошибка валидации структуры таблицы: {e}")
            return False

    def validate_data_structure(self, data: Dict, required_headers: Optional[List[str]] = None) -> bool:
        """
        Проверяет структуру данных на соответствие требованиям.

        DEPRECATED: Используйте update_or_append_data_with_config, который автоматически
        вызывает _validate_data_for_config, или используйте валидацию вручную.

        Args:
            data: Данные для проверки
            required_headers: Список обязательных заголовков (если None, использует конфигурацию)

        Returns:
            bool: True если данные корректны
        """
        # Перенаправляем на унифицированный метод валидации
        if self.table_config:
            return self._validate_data_for_config(data, self.table_config)
        else:
            # Если конфигурации нет, используем старую логику
            try:
                if not data:
                    self.logger.error("Данные пусты")
                    return False

                # Определяем обязательные заголовки
                if required_headers is None and self.table_config:
                    required_headers = self.table_config.required_headers
                elif required_headers is None:
                    required_headers = []

                # Проверяем наличие обязательных полей
                missing_fields = []
                for header in required_headers:
                    if header not in data:
                        missing_fields.append(header)

                if missing_fields:
                    self.logger.error(f"Отсутствуют обязательные поля в данных: {missing_fields}")
                    return False

                # Проверяем, что все значения являются строками или числами
                for key, value in data.items():
                    if not isinstance(value, (str, int, float, type(None))):
                        self.logger.warning(f"Поле '{key}' имеет неожиданный тип: {type(value)}")

                self.logger.debug(f"Данные прошли валидацию: {list(data.keys())}")
                return True

            except Exception as e:
                self.logger.error(f"Ошибка валидации данных: {e}")
                return False

    def get_table_headers(self) -> List[str]:
        """
        Возвращает заголовки таблицы.

        Returns:
            List[str]: Список заголовков
        """
        try:
            headers = self.worksheet.row_values(1)
            return headers
        except Exception as e:
            self.logger.error(f"Ошибка получения заголовков: {e}")
            return []

    def get_last_row_with_data(self, column_index: Optional[int] = None,
                               start_row: int = 2) -> int:
        """
        Определяет последнюю строку с данными в таблице.

        Args:
            column_index: Индекс колонки для проверки (по умолчанию использует первую колонку)
            start_row: Начальная строка для поиска (по умолчанию 2, пропуская заголовки)

        Returns:
            int: Номер последней строки с данными
        """
        try:
            if column_index is None:
                # Используем первую колонку для определения последней строки
                column_values = self.worksheet.col_values(1)
            else:
                column_values = self.worksheet.col_values(column_index)

            # Безопасная проверка пустых строк
            for row_num in range(len(column_values), start_row - 1, -1):
                if row_num - 1 < len(column_values):
                    cell_value = column_values[row_num - 1]
                    if cell_value and str(cell_value).strip():
                        return row_num

            # Если не нашли данных, возвращаем последнюю строку перед заголовками
            return start_row - 1 if start_row > 1 else 1

        except Exception as e:
            self.logger.error(f"Ошибка определения последней строки: {e}")
            return 1

    def get_column_letter_by_name(self, column_name: str) -> Optional[str]:
        """
        Возвращает букву колонки по имени.

        Args:
            column_name: Имя колонки

        Returns:
            Буква колонки (A, B, C) или None если не найдена
        """
        if self.table_config:
            return self.table_config.get_column_letter(column_name)
        return None

    def get_column_index_by_name(self, column_name: str) -> Optional[int]:
        """
        Возвращает индекс колонки по имени.

        Args:
            column_name: Имя колонки

        Returns:
            Индекс колонки (начиная с 1) или None если не найдена
        """
        if self.table_config:
            return self.table_config.get_column_index(column_name)
        return None

    def update_or_append_data_with_config(self,
                                         data: Dict[str, Any],
                                         config: Optional[TableConfig] = None,
                                         strategy: str = "update_or_append",
                                         formula_row_placeholder: str = "{row}") -> Dict[str, Any]:
        """
        Универсальный метод для обновления или добавления данных с использованием конфигурации.

        Args:
            data: Словарь с данными для записи
            config: Конфигурация таблицы (если None, используется self.table_config)
            strategy: Стратегия поведения:
                - "update_or_append": обновить если существует, иначе добавить (по умолчанию)
                - "append_only": всегда добавлять как новую строку
                - "update_only": обновлять только существующие строки
            formula_row_placeholder: Плейсхолдер для номера строки в формулах

        Returns:
            Dict с результатами операции:
            {
                "success": bool,
                "row_number": int,
                "action": str,  # "updated", "appended", "skipped", "error"
                "message": str,
                "data": Dict[str, Any]  # записанные данные
            }
        """
        # Начало операции
        operation_start = datetime.now()

        # Определяем конфигурацию для использования
        use_config = config or self.table_config
        if not use_config:
            return self._create_result(
                success=False,
                action="error",
                message="Не указана конфигурация таблицы",
                data=data
            )

        try:
            # 1. Подготовка данных
            prepared_data = self._prepare_data_for_table(data, use_config)

            # 2. Валидация данных
            if not self._validate_data_for_config(prepared_data, use_config):
                return self._create_result(
                    success=False,
                    action="error",
                    message="Данные не прошли валидацию",
                    data=prepared_data
                )

            # 3. Поиск существующей строки (если нужно)
            existing_row = None
            if strategy in ["update_or_append", "update_only"] and use_config.unique_key_columns:
                # используем batch-версию, ищем по unique_key_columns из конфигурации
                search_keys = {k: prepared_data.get(k) for k in (use_config.unique_key_columns or []) if prepared_data.get(k) is not None}
                existing = self.get_rows_by_unique_keys(search_keys, config=use_config, first_only=True, raise_on_duplicate=False)
                # existing может быть dict или None
                existing_row = existing.get("_row_number") if existing else None

            # 4. Определение действия
            action_result = self._determine_action(
                existing_row=existing_row,
                strategy=strategy
            )

            # 5. Выполнение действия
            if action_result["action"] == "update" and existing_row:
                result = self._update_existing_row(
                    row_number=existing_row,
                    data=prepared_data,
                    config=use_config,
                    formula_row_placeholder=formula_row_placeholder
                )
            elif action_result["action"] == "append":
                result = self._append_new_row(
                    data=prepared_data,
                    config=use_config,
                    formula_row_placeholder=formula_row_placeholder
                )
            elif action_result["action"] == "skip":
                result = self._create_result(
                    success=True,
                    action="skipped",
                    message="Стратегия указывает пропустить операцию",
                    data=prepared_data
                )
            else:
                result = self._create_result(
                    success=False,
                    action="error",
                    message="Неизвестное действие",
                    data=prepared_data
                )

            # 6. Логирование результата
            operation_duration = (datetime.now() - operation_start).total_seconds()
            self.logger.info(f"Операция завершена: {result['action']} за {operation_duration:.2f}с")

            return result

        except Exception as e:
            self.logger.error(f"Критическая ошибка в update_or_append_data_with_config: {e}")
            return self._create_result(
                success=False,
                action="error",
                message=f"Критическая ошибка: {str(e)}",
                data=data
            )

    def _prepare_data_for_table(self, data: Dict[str, Any], config: TableConfig) -> Dict[str, Any]:
        """
        Подготавливает данные для записи в таблицу.

        Args:
            data: Исходные данные
            config: Конфигурация таблицы

        Returns:
            Подготовленные данные
        """
        prepared_data = {}

        for col_def in config.columns:
            # Пропускаем формульные колонки - они заполняются формулами
            if col_def.column_type == ColumnType.FORMULA:
                continue

            # Определяем ключ в данных
            data_key = col_def.data_key or col_def.name

            if data_key in data and data[data_key] is not None:
                # Сохраняем значение как есть (Google Sheets сам определит тип)
                prepared_data[col_def.name] = data[data_key]
            elif col_def.required:
                # Для обязательных колонок устанавливаем пустое значение
                prepared_data[col_def.name] = ""
            # Необязательные колонки без данных не включаем в результат

        return prepared_data

    def _validate_data_for_config(self, data: Dict[str, Any], config: TableConfig) -> bool:
        """
        Валидирует данные для указанной конфигурации.

        Args:
            data: Данные для валидации
            config: Конфигурация таблицы

        Returns:
            bool: True если данные корректны
        """
        try:
            # Проверяем обязательные поля
            for col_def in config.columns:
                if col_def.required:
                    # Проверяем, что ключ есть и значение не None и не пустая строка
                    if col_def.name not in data or data[col_def.name] is None:
                        self.logger.error(f"Обязательное поле '{col_def.name}' отсутствует или None")
                        return False
                    # Дополнительная проверка: если значение - пустая строка, это может быть ошибкой
                    elif isinstance(data[col_def.name], str) and data[col_def.name].strip() == "":
                        self.logger.error(f"Обязательное поле '{col_def.name}' содержит пустое значение")
                        return False

            # Проверяем уникальные ключи
            for key in config.unique_key_columns or []:
                if key not in data or data[key] is None:
                    self.logger.error(f"Поле уникального ключа '{key}' отсутствует или None")
                    return False
                # Проверяем, что уникальные ключи не пустые
                elif isinstance(data[key], str) and data[key].strip() == "":
                    self.logger.error(f"Поле уникального ключа '{key}' содержит пустое значение")
                    return False

            return True

        except Exception as e:
            self.logger.error(f"Ошибка валидации данных для конфигурации: {e}")
            return False


    def get_rows_by_unique_keys(
        self,
        unique_key_values: Dict[str, Any],
        config: Optional[TableConfig] = None,
        return_raw: bool = False,
        first_only: bool = True,
        raise_on_duplicate: bool = False
    ) -> Union[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Находит и возвращает строки по уникальным ключам с использованием batch_get.

        Args:
            unique_key_values: Словарь с значениями уникальных ключей.
            config: Конфигурация таблицы (если None, используется self.table_config)
            return_raw: Если True, возвращает сырые значения без преобразования типов
            first_only: Если True, возвращает только первую найденную строку, иначе список всех
            raise_on_duplicate: Если True, бросает исключение при наличии дубликатов

        Returns:
            Dict[str, Any] с данными строки, список строк или None если строка не найдена
        """
        if not unique_key_values:
            self.logger.error("Не указаны значения уникальных ключей для поиска")
            return [] if not first_only else None

        use_config = config or self.table_config
        if not use_config:
            self.logger.error("Не указана конфигурация таблицы")
            return [] if not first_only else None

        # Фильтруем переданные ключи, оставляем только те, что есть в данных (не обязательно уникальные)
        filtered_unique_key_values = {}
        for key, value in unique_key_values.items():
            filtered_unique_key_values[key] = value

        if not filtered_unique_key_values:
            self.logger.error("Нет ключей для поиска")
            return [] if not first_only else None

        try:
            # Находим строки по ключам с использованием batch_get
            row_numbers = self._find_rows_by_unique_keys_batch(filtered_unique_key_values, use_config, strict_mode=False)

            if not row_numbers:
                self.logger.debug(f"Строки не найдены по ключам: {filtered_unique_key_values}")
                return [] if not first_only else None

            # Проверяем дубликаты
            if len(row_numbers) > 1:
                self.logger.warning(f"Найдено несколько строк по ключам {filtered_unique_key_values}: {row_numbers}")
                if raise_on_duplicate:
                    raise ValueError(f"Найдено несколько строк по ключам {filtered_unique_key_values}: {row_numbers}")

            # Читаем данные строк
            results = []
            for row_num in row_numbers:
                row_data = self._get_row_by_number(row_num, use_config, return_raw=return_raw)
                if row_data:
                    row_data["_row_number"] = row_num
                    results.append(row_data)

            if not results:
                self.logger.warning(f"Найденные строки {row_numbers}, но не удалось прочитать данные")
                return [] if not first_only else None

            self.logger.info(f"Найдено {len(results)} строк по ключам: {filtered_unique_key_values}")

            if first_only:
                result = results[0]
                self.logger.info(f"Возвращена первая строка: строка {result['_row_number']}, ключи: {filtered_unique_key_values}")
                return result
            else:
                return results
        except ValueError:
            # Не перехватываем ValueError, особенно при дубликатах
            raise
        except Exception as e:
            self.logger.exception(f"Ошибка при поиске строк по ключам {filtered_unique_key_values}: {e}")
            return [] if not first_only else None

    def get_row_by_unique_keys(
        self,
        unique_key_values: Dict[str, Any],
        config: Optional[TableConfig] = None,
        return_raw: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Находит и возвращает строку по уникальным ключам (обертка над get_rows_by_unique_keys).

        Args:
            unique_key_values: Словарь с значениями уникальных ключей.
            config: Конфигурация таблицы (если None, используется self.table_config)
            return_raw: Если True, возвращает сырые значения без преобразования типов

        Returns:
            Dict[str, Any] с данными строки или None если строка не найдена
        """
        return self.get_rows_by_unique_keys(
            unique_key_values=unique_key_values,
            config=config,
            return_raw=return_raw,
            first_only=True,
            raise_on_duplicate=False
        )

    def _prepare_value_for_search(self, value: Any) -> str:
        """
        Подготавливает значение для поиска в таблице — нормализует формат дат/чисел/строк.
        """
        if value is None:
            return ""

        # datetime -> "DD.MM.YYYY"
        if isinstance(value, datetime):
            return value.strftime("%d.%m.%Y")

        # float без лишних нулей
        if isinstance(value, float):
            if value.is_integer():
                return str(int(value))
            return str(value).rstrip('0').rstrip('.')

        if isinstance(value, int):
            return str(value)

        s = str(value).strip()
        if s.endswith(" 00:00:00"):
            s = s.replace(" 00:00:00", "")
        return s

    def _get_row_by_number(
        self,
        row_number: int,
        config: TableConfig,
        return_raw: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Читает данные строки по номеру и возвращает словарь header->value.
        Если return_raw=True — возвращает значения, как есть в таблице.
        """
        try:
            headers = self.worksheet.row_values(1)
            if not headers:
                self.logger.error("Таблица не содержит заголовков")
                return None

            if row_number < 1:
                self.logger.error(f"Некорректный номер строки: {row_number}")
                return None

            row_values = self.worksheet.row_values(row_number)
            # Дополняем до длины заголовков
            if len(row_values) < len(headers):
                row_values.extend([""] * (len(headers) - len(row_values)))

            row_data: Dict[str, Any] = {}
            for idx, header in enumerate(headers):
                header_name = header.strip()
                raw_value = row_values[idx] if idx < len(row_values) else ""

                if return_raw:
                    row_data[header_name] = raw_value
                    continue

                col_def = config.get_column(header_name)
                if col_def and col_def.column_type == ColumnType.DATA:
                    row_data[header_name] = self._convert_value_by_type(raw_value, col_def)
                else:
                    # Формулы и игнорируемые колонки — возвращаем как есть
                    row_data[header_name] = raw_value

            row_data["_row_number"] = row_number
            return row_data
        except Exception as e:
            self.logger.exception(f"Ошибка чтения строки {row_number}: {e}")
            return None

    def _convert_value_by_type(self, value: str, col_def: ColumnDefinition) -> Any:
        """
        Попытка привести строку к наиболее подходящему типу: int, float, datetime или оставить строкой.
        """
        if value is None:
            return None
        if not isinstance(value, str):
            return value

        v = value.strip()
        if v == "":
            return ""

        # Целые числа
        if v.isdigit() or (v.startswith('-') and v[1:].isdigit()):
            try:
                return int(v)
            except Exception:
                pass

        # Вещественные числа (учёт запятых)
        norm = v.replace(',', '.')
        if norm.replace('.', '', 1).replace('-', '', 1).isdigit():
            try:
                return float(norm)
            except Exception:
                pass

        # Даты: пробуем несколько форматов
        date_formats = [
            "%d.%m.%Y", "%d/%m/%Y", "%Y-%m-%d",
            "%d.%m.%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"
        ]
        for fmt in date_formats:
            try:
                return datetime.strptime(v, fmt)
            except Exception:
                continue

        return v

    def _normalize_for_comparison(self, value: Any) -> str:
        """
        Нормализует значение для сравнения. Используется, если потребуется сравнение
        в логике поиска (вместо прямого str == str).
        """
        if value is None:
            return ""
        s = str(value).strip()
        if s.endswith(" 00:00:00"):
            s = s.replace(" 00:00:00", "")
        return s

    def get_row_by_id(self, id_value: Any, config: Optional[TableConfig] = None, return_raw: bool = False) -> Optional[Dict[str, Any]]:
        """
        Находит и возвращает строку по значению ID.

        Args:
            id_value: Значение ID для поиска (обычно результат формулы в колонке id)
            config: Конфигурация таблицы (если None, используется self.table_config)
            return_raw: Если True, возвращает сырые значения без преобразования типов

        Returns:
            Dict[str, Any] с данными строки или None если строка не найдена
        """
        use_config = config or self.table_config
        if not use_config or not getattr(use_config, "id_column", None):
            self.logger.debug("Нет id_column в конфигурации")
            return None

        id_idx = use_config.get_column_index(use_config.id_column)
        if not id_idx:
            self.logger.debug("id_column не найден в синхронизированных индексах (build_column_indexes)")
            return None

        try:
            cell = self.worksheet.find(str(id_value), in_column=id_idx)
            if not cell:
                self.logger.debug(f"Ячейка с ID '{id_value}' не найдена")
                return None

            row_data = self._get_row_by_number(cell.row, use_config, return_raw=return_raw)
            if row_data:
                self.logger.info(f"Найдена строка по ID {id_value}: строка {cell.row}")
            return row_data
        except gspread.exceptions.CellNotFound:
            return None
        except Exception as e:
            self.logger.exception(f"Ошибка при поиске строки по ID {id_value}: {e}")
            return None

    def _find_rows_by_unique_keys_batch(self, data: Dict[str, Any], config: TableConfig, strict_mode: bool = False) -> List[int]:
        """
        Находит строки по уникальным ключам с использованием batch_get для лучшей производительности.

        Args:
            data: Данные для поиска (только те ключи, которые нужно проверить)
            config: Конфигурация таблицы
            strict_mode: Если True, бросает исключение при отсутствии обязательных колонок

        Returns:
            Список номеров строк, соответствующих критериям
        """
        try:
            if not data:
                self.logger.warning("Не переданы данные для поиска")
                return []

            # Получаем заголовки из первой строки и нормализуем их
            raw_headers = self.worksheet.row_values(1)
            headers = [h.strip() for h in raw_headers]
            # Создаем маппинг нормализованных заголовков для поиска
            normalized_header_to_idx = {h.lower(): i+1 for i, h in enumerate(headers)}

            # Проверяем, что все ключи из данных существуют в заголовках (с нормализацией)
            key_col_idxs = {}
            missing_columns = []
            for k in data.keys():
                normalized_key = k.lower().strip()
                if normalized_key in normalized_header_to_idx:
                    key_col_idxs[k] = normalized_header_to_idx[normalized_key]  # Сохраняем оригинальный ключ, но ищем по нормализованному
                else:
                    missing_columns.append(k)

            # Если есть отсутствующие колонки
            if missing_columns:
                if strict_mode:
                    # В строгом режиме бросаем исключение
                    raise ValueError(f"Обязательные колонки не найдены в заголовках: {missing_columns}. Доступные колонки: {list(normalized_header_to_idx.keys())}")
                else:
                    # В нестрогом режиме логируем и возвращаем пустой список
                    self.logger.warning(f"Колонки '{missing_columns}' не найдены в заголовках (нормализованные заголовки: {list(normalized_header_to_idx.keys())})")
                    return []

            # Определяем максимальное количество строк для чтения
            # Используем get_last_row_with_data() для оптимизации, но добавляем буфер для свежих данных
            last_data_row = self.get_last_row_with_data()
            max_row = min(self.worksheet.row_count, last_data_row + 10)  # добавляем буфер в 10 строк для свежих данных

            # Формируем диапазоны для batch_get
            ranges = []
            key_order = []
            for key, col_idx in key_col_idxs.items():
                col_letter = _index_to_column_letter(col_idx)
                ranges.append(f"{col_letter}2:{col_letter}{max_row}")
                key_order.append(key)

            # Выполняем batch_get запрос
            try:
                batch_values = self.worksheet.batch_get(ranges)  # list of ValueRange objects
            except Exception as e:
                self.logger.exception("batch_get error: %s", e)
                return []

            # Обрабатываем полученные значения
            cols_values = {}
            for key, vals in zip(key_order, batch_values):
                # vals - это ValueRange объект, который можно итерировать
                # ValueRange содержит данные в формате [[value1], [value2], ...]
                flat = []
                for item in vals:
                    if isinstance(item, list) and len(item) > 0:
                        flat.append(item[0])
                    else:
                        flat.append(item if not isinstance(item, list) else "")
                cols_values[key] = flat

            # Подготавливаем ожидаемые нормализованные значения
            expected = {k: self._normalize_for_comparison(self._prepare_value_for_search(data.get(k, ""))) for k in key_order}

            # Сканируем строки
            matches = []
            max_len = max((len(v) for v in cols_values.values()), default=0)
            for i in range(max_len):
                ok = True
                for k in key_order:
                    actual = cols_values[k][i] if i < len(cols_values[k]) else ""
                    if self._normalize_for_comparison(actual) != expected[k]:
                        ok = False
                        break
                if ok:
                    matches.append(i + 2)  # +2 потому что i начинается с 0, а строки начинаются с 2 (пропускаем заголовки)
            return matches
        except Exception as e:
            self.logger.error(f"Ошибка поиска строк по ключам (batch): {e}")
            return []

    def _determine_action(self, existing_row: Optional[int],
                         strategy: str) -> Dict[str, Any]:
        """
        Определяет действие на основе существующей строки и стратегии.

        Args:
            existing_row: Номер существующей строки или None
            strategy: Стратегия поведения

        Returns:
            Словарь с информацией о действии
        """
        if strategy == "append_only":
            return {"action": "append"}
        elif strategy == "update_only":
            if existing_row:
                return {"action": "update"}
            else:
                return {"action": "skip"}
        elif strategy == "update_or_append":
            if existing_row:
                return {"action": "update"}
            else:
                return {"action": "append"}
        else:
            self.logger.error(f"Неизвестная стратегия: {strategy}")
            return {"action": "error"}

    @retry_on_api_error(max_retries=3, base_delay=1.0, max_delay=10.0)
    def _update_existing_row(self, row_number: int,
                           data: Dict[str, Any],
                           config: TableConfig,
                           formula_row_placeholder: str) -> Dict[str, Any]:
        """
        Обновляет существующую строку.

        Args:
            row_number: Номер строки для обновления
            data: Данные для обновления
            config: Конфигурация таблицы
            formula_row_placeholder: Плейсхолдер для номера строки

        Returns:
            Результат операции
        """
        try:
            # Получаем заголовки
            headers = self.worksheet.row_values(1)

            # Подготавливаем значения для обновления
            values = self._prepare_row_values(data, config, row_number, formula_row_placeholder)

            # Обновляем строку
            start_col = config.get_column_letter(headers[0]) or "A"
            end_col = config.get_column_letter(headers[-1]) or _index_to_column_letter(len(headers))
            range_str = f"{start_col}{row_number}:{end_col}{row_number}"

            self.worksheet.update(range_str, [values], value_input_option='USER_ENTERED')

            result = self._create_result(
                success=True,
                action="updated",
                message=f"Строка {row_number} обновлена",
                data=data,
                row_number=row_number
            )

            self.logger.info(f"Строка {row_number} успешно обновлена")
            return result

        except Exception as e:
            self.logger.error(f"Ошибка обновления строки {row_number}: {e}")
            return self._create_result(
                success=False,
                action="error",
                message=f"Ошибка обновления строки: {str(e)}",
                data=data
            )

    def _prepare_row_values(self, data: Dict[str, Any], config: TableConfig,
                           row_number: int, formula_row_placeholder: str) -> List[Any]:
        """
        Подготавливает значения для строки с учетом формул и конфигурации колонок.

        Этот метод унифицирует логику подготовки значений для использования
        в методах _update_existing_row и _append_new_row, устраняя дублирование кода.

        Args:
            data: Данные для подготовки
            config: Конфигурация таблицы
            row_number: Номер строки для формул
            formula_row_placeholder: Плейсхолдер для номера строки

        Returns:
            Список значений для строки
        """
        headers = self.worksheet.row_values(1)
        values = []

        for header in headers:
            col_def = config.get_column(header)
            if not col_def:
                values.append("")
                continue

            if col_def.column_type == ColumnType.FORMULA and col_def.formula_template:
                # Заменяем плейсхолдер в формуле
                formula = col_def.formula_template.replace(
                    formula_row_placeholder,
                    str(row_number)
                )
                values.append(formula)
            elif col_def.name in data:
                values.append(data[col_def.name])
            else:
                values.append("")

        return values

    @retry_on_api_error(max_retries=3, base_delay=1.0, max_delay=10.0)
    def _append_new_row(self, data: Dict[str, Any],
                       config: TableConfig,
                       formula_row_placeholder: str) -> Dict[str, Any]:
        """
        Добавляет новую строку с данными одним запросом.

        Args:
            data: Данные для добавления
            config: Конфигурация таблицы
            formula_row_placeholder: Плейсхолдер для номера строки

        Returns:
            Результат операции
        """
        try:
            last_row = self.get_last_row_with_data()
            new_row_num = last_row + 1

            # Подготавливаем значения с формулами
            values = self._prepare_row_values(data, config, new_row_num, formula_row_placeholder)

            # ОДИН запрос к API
            self.worksheet.append_rows(
                values=[values],
                value_input_option='USER_ENTERED'
            )

            # Логирование
            self.logger.info(f"Новая строка {new_row_num} добавлена")

            return self._create_result(
                success=True,
                action="appended",
                message=f"Строка {new_row_num} добавлена",
                data=data,
                row_number=new_row_num
            )

        except Exception as e:
            self.logger.error(f"Ошибка добавления новой строки: {e}")
            return self._create_result(
                success=False,
                action="error",
                message=f"Ошибка добавления строки: {str(e)}",
                data=data
            )


    def _create_result(self, success: bool, action: str, message: str,
                      data: Dict[str, Any], row_number: Optional[int] = None) -> Dict[str, Any]:
        """
        Создает стандартный результат операции.

        Args:
            success: Успешность операции
            action: Тип действия
            message: Сообщение
            data: Данные
            row_number: Номер строки (если применимо)

        Returns:
            Стандартный результат операции
        """
        result = {
            "success": success,
            "action": action,
            "message": message,
            "data": data,
            "timestamp": datetime.now().isoformat()
        }

        if row_number is not None:
            result["row_number"] = row_number

        return result