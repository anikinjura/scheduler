"""
Структуры данных для изолированного микросервиса загрузчика Google Sheets

Этот файл содержит структуры данных, необходимые для работы с Google Sheets:
- Перечисления типов колонок
- Классы определения колонок и конфигурации таблицы
- Вспомогательные функции для работы с индексами колонок
"""
__version__ = '1.0.0'

from enum import Enum, auto
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from pathlib import Path


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