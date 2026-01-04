"""
google_sheets.py

Универсальный модуль для работы с Google-таблицами.
Предоставляет классы и функции для подключения к Google-таблице,
чтения, записи и обновления данных.

Функции:
    - GoogleSheetsReporter: класс для работы с Google-таблицами
    - connect_to_spreadsheet: функция подключения к таблице
    - read_data_from_sheet: функция чтения данных
    - write_data_to_sheet: функция записи данных

Author: anikinjura
"""
__version__ = '0.0.1'

import gspread
from google.oauth2.service_account import Credentials
from pathlib import Path
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum, auto

from scheduler_runner.utils.logging import configure_logger


class ColumnType(Enum):
    """
    Типы колонок в таблице Google Sheets.

    Используется для семантического разделения колонок:
    - ID: Идентификатор записи (часто формула)
    - DATA: Простые данные, вводимые пользователем или из отчетов
    - FORMULA: Колонки с формулами Google Sheets
    - CALCULATED: Вычисляемые значения на стороне Python-скрипта
    - IGNORE: Колонки, которые игнорируются при операциях записи
    """
    ID = auto()          # Идентификатор (часто формула)
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

    def build_column_indexes(self, headers: List[str]):
        """
        Строит индексы колонок на основе реальных заголовков таблицы.

        Args:
            headers: Список заголовков из первой строки таблицы

        Raises:
            ValueError: Если какие-то обязательные колонки отсутствуют в таблице
        """
        self._column_index_map.clear()
        self._letter_index_map.clear()

        # Проверяем наличие обязательных колонок
        missing_required = []
        for col_def in self.columns:
            if col_def.required and col_def.name not in headers:
                missing_required.append(col_def.name)

        if missing_required:
            raise ValueError(
                f"В таблице отсутствуют обязательные колонки: {missing_required}. "
                f"Найдены колонки: {headers}"
            )

        # Строим маппинги
        for idx, header in enumerate(headers, start=1):
            col_def = self.get_column(header)
            if col_def:
                self._column_index_map[header] = idx
                letter = self._index_to_column_letter(idx)
                self._letter_index_map[header] = letter
                col_def.column_letter = letter

        # Логируем результат
        if len(self._column_index_map) < len(self.columns):
            configured_columns = set(self.column_names)
            found_columns = set(headers)
            missing = configured_columns - found_columns
            extra = found_columns - configured_columns

            if missing:
                print(f"Предупреждение: В таблице отсутствуют настроенные колонки: {missing}")
            if extra:
                print(f"Предупреждение: В таблице найдены лишние колонки: {extra}")

    @staticmethod
    def _index_to_column_letter(index: int) -> str:
        """
        Преобразует индекс колонки в буквенное обозначение.

        Args:
            index: Номер колонки (начинается с 1)

        Returns:
            Буквенное обозначение колонки (A, B, ..., Z, AA, AB, ...)

        Examples:
            1 -> 'A'
            26 -> 'Z'
            27 -> 'AA'
            28 -> 'AB'
        """
        letter = ''
        while index > 0:
            index, remainder = divmod(index - 1, 26)
            letter = chr(65 + remainder) + letter
        return letter

    @staticmethod
    def from_headers(headers: List[str], worksheet_name: str = "Лист1", id_column: str = "id") -> 'TableConfig':
        """
        Создает базовую конфигурацию из списка заголовков.

        Args:
            headers: Список заголовков из таблицы
            worksheet_name: Имя листа
            id_column: Имя колонки идентификатора (должно быть в списке headers)

        Returns:
            TableConfig с колонками типа DATA
        """
        columns = [
            ColumnDefinition(name=header, column_type=ColumnType.DATA)
            for header in headers
        ]
        # Если id_column не в списке headers, используем первый заголовок или оставляем пустым
        actual_id_column = id_column if id_column in headers else headers[0] if headers else "id"
        return TableConfig(
            worksheet_name=worksheet_name,
            columns=columns,
            id_column=actual_id_column
        )


def create_kpi_table_config() -> TableConfig:
    """
    Создает предварительно настроенную конфигурацию для таблицы KPI.

    Соответствует структуре:
        id, Дата, ПВЗ, Количество выдач, Прямой поток, Возвратный поток

    Returns:
        TableConfig для таблицы KPI
    """
    return TableConfig(
        worksheet_name="KPI",
        columns=[
            ColumnDefinition(
                name="id",
                column_type=ColumnType.FORMULA,
                formula_template="=B{row}&C{row}"
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
                column_type=ColumnType.DATA
            ),
            ColumnDefinition(
                name="Прямой поток",
                column_type=ColumnType.DATA
            ),
            ColumnDefinition(
                name="Возвратный поток",
                column_type=ColumnType.DATA
            ),
        ],
        id_column="id",
        id_formula_template="=B{row}&C{row}"
    )


def create_basic_table_config(worksheet_name: str = "Лист1",
                             id_column: str = "id",
                             unique_keys: Optional[List[str]] = None) -> TableConfig:
    """
    Создает базовую конфигурацию таблицы.

    Args:
        worksheet_name: Имя листа
        id_column: Имя колонки идентификатора
        unique_keys: Список колонок уникального ключа

    Returns:
        Базовая TableConfig
    """
    return TableConfig(
        worksheet_name=worksheet_name,
        columns=[],
        id_column=id_column,
        unique_key_columns=unique_keys
    )


if __name__ == "__main__":
    """
    Тестирование классов конфигурации.
    Запустить: python -m scheduler_runner.utils.google_sheets
    """

    print("=" * 60)
    print("Тестирование классов конфигурации таблицы")
    print("=" * 60)

    # Тест 1: Создание конфигурации KPI
    print("\n1. Тест конфигурации KPI:")
    kpi_config = create_kpi_table_config()
    print(f"   Лист: {kpi_config.worksheet_name}")
    print(f"   Колонки: {kpi_config.column_names}")
    print(f"   Уникальные ключи: {kpi_config.unique_key_columns}")
    print(f"   Обязательные: {kpi_config.required_headers}")

    # Тест 2: Построение индексов
    print("\n2. Тест построения индексов:")
    headers = ["id", "Дата", "ПВЗ", "Количество выдач", "Прямой поток", "Возвратный поток"]
    kpi_config.build_column_indexes(headers)

    for col_name in kpi_config.column_names:
        idx = kpi_config.get_column_index(col_name)
        letter = kpi_config.get_column_letter(col_name)
        print(f"   {col_name}: индекс={idx}, буква={letter}")

    # Тест 3: Конвертация индекса в букву
    print("\n3. Тест конвертации индекса в букву:")
    test_indices = [1, 2, 26, 27, 28, 52, 53]
    for idx in test_indices:
        letter = TableConfig._index_to_column_letter(idx)
        print(f"   Индекс {idx:3} -> '{letter}'")

    # Тест 4: Создание из заголовков
    print("\n4. Тест создания конфигурации из заголовков:")
    custom_headers = ["OrderID", "Customer", "Amount", "Status"]
    # Используем метод from_headers, который теперь корректно обрабатывает id_column
    custom_config = TableConfig.from_headers(custom_headers, "Orders", id_column="OrderID")
    print(f"   Лист: {custom_config.worksheet_name}")
    print(f"   Колонки: {custom_config.column_names}")
    print(f"   Уникальные ключи: {custom_config.unique_key_columns}")

    print("\n" + "=" * 60)
    print("Все тесты пройдены успешно!")
    print("=" * 60)


class GoogleSheetsReporter:
    """
    Класс для работы с Google-таблицами.
    Предоставляет методы для подключения к Google-таблице, определения последней строки с данными,
    добавления новых строк, обновления существующих записей и валидации структуры данных.
    """
    
    def __init__(self, credentials_path: str, spreadsheet_name: str, worksheet_name: str = "Лист1"):
        """
        Инициализация подключения к Google-таблице.

        Args:
            credentials_path (str): путь к файлу учетных данных
            spreadsheet_name (str): ID или имя таблицы
            worksheet_name (str): имя листа (по умолчанию "Лист1")
        """
        self.credentials_path = Path(credentials_path)
        self.spreadsheet_name = spreadsheet_name
        self.worksheet_name = worksheet_name
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
        
        # Открытие таблицы и листа
        self.spreadsheet = self.gc.open_by_key(self.spreadsheet_name) if len(self.spreadsheet_name) > 20 else self.gc.open(self.spreadsheet_name)
        self.worksheet = self.spreadsheet.worksheet(self.worksheet_name)
        
        self.logger.info(f"Подключено к таблице: {self.spreadsheet_name}, лист: {self.worksheet_name}")
    
    def validate_data_structure(self, data: Dict, required_headers: List[str]) -> bool:
        """
        Проверяет, что структура данных соответствует требованиям таблицы.

        Args:
            data: словарь с данными для проверки
            required_headers: список обязательных заголовков

        Returns:
            bool: True, если структура данных корректна
        """
        try:
            for header in required_headers:
                if header not in data:
                    self.logger.error(f"Отсутствует обязательное поле: {header}")
                    return False
            return True
        except Exception as e:
            self.logger.error(f"Ошибка при валидации структуры данных: {e}")
            return False

    def get_last_row_with_data(self, column_index: int = 1) -> int:
        """
        Определяет последнюю строку с данными в указанном столбце.

        Args:
            column_index (int): индекс столбца для проверки (по умолчанию 1 - столбец A)

        Returns:
            int: номер последней строки с данными
        """
        try:
            # Получаем все значения в столбце
            col_values = self.worksheet.col_values(column_index)
            # Возвращаем длину списка, что соответствует последней строке с данными
            return len(col_values)
        except Exception as e:
            self.logger.error(f"Ошибка при получении последней строки: {e}")
            return 1  # Возвращаем первую строку как безопасное значение
    
    def append_row_data_with_row_number(self, data: List) -> int:
        """
        Добавляет данные в следующую строку после последней заполненной и возвращает номер строки.

        Args:
            data: список значений для записи в строку

        Returns:
            int: номер добавленной строки, или 0 при ошибке
        """
        try:
            # Получаем текущую последнюю строку
            current_last_row = self.get_last_row_with_data()

            # Определяем целевую строку
            target_row = current_last_row + 1

            # Проверяем, пустая ли эта строка
            try:
                # Проверяем, есть ли данные в целевой строке
                row_values = self.worksheet.row_values(target_row)
                if not any(row_values):
                    # Строка пустая, можно использовать
                    range_name = f'A{target_row}:{chr(64+len(data))}{target_row}'
                    self.worksheet.update(range_name, [data], value_input_option='USER_ENTERED')
                    self.logger.info(f"Данные успешно добавлены в пустую строку {target_row}")
                else:
                    # Строка содержит данные, используем append_rows для добавления новой строки
                    target_row = current_last_row + 1
                    # Добавляем пустую строку для данных
                    self.worksheet.append_rows([data], value_input_option='USER_ENTERED')
                    # Получаем новую последнюю строку
                    new_last_row = self.get_last_row_with_data()
                    if new_last_row > current_last_row:
                        target_row = new_last_row
                        self.logger.info(f"Данные успешно добавлены в новую строку {target_row}")
                    else:
                        self.logger.error("Не удалось добавить новую строку")
                        return 0
            except Exception as e:
                # Если возникла ошибка при проверке строки, используем append_rows
                self.logger.warning(f"Ошибка при проверке строки {target_row}: {e}, используем append_rows")
                self.worksheet.append_rows([data], value_input_option='USER_ENTERED')
                new_last_row = self.get_last_row_with_data()
                if new_last_row > current_last_row:
                    target_row = new_last_row
                else:
                    self.logger.error("Не удалось добавить новую строку через append_rows")
                    return 0

            return target_row
        except Exception as e:
            self.logger.error(f"Ошибка при добавлении строки: {e}")
            import traceback
            self.logger.error(f"Полный стек трейс: {traceback.format_exc()}")
            return 0
    
    def update_or_append_data(self, data: Dict, date_key: str = "Дата", pvz_key: str = "ПВЗ") -> bool:
        """
        Обновляет данные, если запись с такой датой и ПВЗ уже существует, или добавляет новую.
        Использует Id столбец (A) для определения уникальности записи.
        Предотвращает дублирование данных при одновременной записи из разных ПВЗ.

        Args:
            data: словарь с данными для записи
            date_key: ключ в словаре, содержащий дату для поиска дубликатов (по умолчанию "Дата")
            pvz_key: ключ в словаре, содержащий ПВЗ для поиска дубликатов (по умолчанию "ПВЗ")

        Returns:
            bool: True при успешной записи
        """
        try:
            # Получаем дату и ПВЗ из данных
            date_value = data.get(date_key)
            pvz_value = data.get(pvz_key)

            self.logger.debug(f"update_or_append_data: date_value='{date_value}', pvz_value='{pvz_value}'")

            if date_value and pvz_value:
                # Ищем запись с такой датой и ПВЗ (вместо поиска по Id, ищем по дате и ПВЗ)
                try:
                    # Сначала ищем в столбце даты
                    date_matches = self.worksheet.findall(date_value)

                    # Среди найденных ищем совпадение по ПВЗ
                    for date_cell in date_matches:
                        pvz_cell_value = self.worksheet.cell(date_cell.row, 3).value  # ПВЗ в 3-м столбце
                        if pvz_cell_value == pvz_value:
                            # Нашли совпадение, обновляем строку
                            row_num = date_cell.row
                            self.logger.debug(f"update_or_append_data: найдено совпадение в строке {row_num}")

                            # Проверяем, существует ли строка физически
                            try:
                                # Проверяем, есть ли хотя бы одно значение в строке
                                row_values = self.worksheet.row_values(row_num)
                                if not any(row_values):
                                    # Строка физически удалена, нужно добавить новую
                                    self.logger.warning(f"Строка {row_num} физически удалена, добавляем новую")
                                    break

                                # Обновляем строку новыми значениями, но оставляем Id столбец с формулой
                                # Получаем заголовки из первой строки, чтобы определить порядок колонок
                                headers = self.worksheet.row_values(1) if self.worksheet.row_count >= 1 else []

                                # Подготавливаем новые значения, но оставляем Id столбец (A) без изменений
                                updated_values = [row_values[0] if len(row_values) > 0 else ""]  # Id столбец - оставляем как есть

                                # Для каждой колонки (кроме Id) получаем соответствующее значение из данных
                                for i in range(1, len(headers)):  # начиная с 2-го столбца (B), т.к. A - Id
                                    if i < len(row_values):
                                        current_value = row_values[i]
                                    else:
                                        current_value = ""

                                    # Если есть заголовок и он есть в данных, используем значение из данных
                                    if i < len(headers) and headers[i] in data:
                                        updated_values.append(data[headers[i]])
                                    else:
                                        # Оставляем текущее значение, если поле не найдено в новых данных
                                        updated_values.append(current_value)

                                # Обновляем диапазон
                                self.worksheet.update(f'A{row_num}:{chr(64+len(updated_values))}{row_num}',
                                                      [updated_values],
                                                      value_input_option='USER_ENTERED')

                                expected_id = f"{date_value}{pvz_value}"
                                self.logger.info(f"Данные за {date_value} для ПВЗ {pvz_value} обновлены в строке {row_num} (Id: {expected_id})")
                                return True

                            except Exception as e:
                                self.logger.warning(f"Ошибка при обновлении строки {row_num}: {e}, строка, вероятно, удалена")
                                break
                except Exception as e:
                    self.logger.error(f"Ошибка при поиске существующей записи: {e}")

                # Если не найдено совпадение или строка была удалена, добавляем новую строку с формулой в Id столбце
                self.logger.debug(f"update_or_append_data: запись с датой '{date_value}' и ПВЗ '{pvz_value}' не найдена, добавляем новую строку")

                # Подготовим данные для добавления, но сначала добавим строку с пустым Id
                # Затем установим формулу в Id столбце
                current_last_row = self.get_last_row_with_data()

                # Получаем заголовки из первой строки, чтобы определить порядок колонок
                headers = self.worksheet.row_values(1) if self.worksheet.row_count >= 1 else []

                # Подготовим данные в правильном порядке в соответствии с заголовками таблицы
                values = [""]
                for header in headers[1:]:  # начиная со второго столбца (первый - Id)
                    if header in data:
                        value = data[header]
                        # Проверяем тип данных и конвертируем при необходимости
                        if isinstance(value, str) and value.isdigit():
                            value = int(value)
                        elif isinstance(value, (int, float)):
                            pass  # уже число
                        else:
                            value = value if value is not None else ""
                        values.append(value)
                    else:
                        values.append("")  # если поле не найдено в данных, добавляем пустое значение

                row_num = self.append_row_data_with_row_number(values)
                if row_num > 0:
                    self.logger.debug(f"update_or_append_data: строка добавлена в строку {row_num}")
                    # Теперь установим формулу в Id столбце для этой строки
                    formula = f"=B{row_num}&C{row_num}"  # формула: дата + ПВЗ для этой строки
                    # Используем update с правильным value_input_option для корректной записи формулы
                    self.worksheet.update(values=[[formula]], range_name=f'A{row_num}', value_input_option='USER_ENTERED')
                    self.logger.info(f"Формула Id установлена в строке {row_num}: {formula}")
                    return True
                else:
                    self.logger.error("Не удалось добавить строку")
                    return False
            else:
                self.logger.debug(f"update_or_append_data: дата или ПВЗ не указаны, добавляем новую строку")
                # Если дата или ПВЗ не указаны, просто добавляем новую строку
                # Получаем заголовки из первой строки
                headers = self.worksheet.row_values(1) if self.worksheet.row_count >= 1 else []

                # Подготовим данные в правильном порядке в соответствии с заголовками таблицы
                values = [""]
                for header in headers[1:]:  # начиная со второго столбца (первый - Id)
                    if header in data:
                        value = data[header]
                        # Проверяем тип данных и конвертируем при необходимости
                        if isinstance(value, str) and value.isdigit():
                            value = int(value)
                        elif isinstance(value, (int, float)):
                            pass  # уже число
                        else:
                            value = value if value is not None else ""
                        values.append(value)
                    else:
                        values.append("")  # если поле не найдено в данных, добавляем пустое значение

                result = self.append_row_data_with_row_number(values)
                return result != 0
        except Exception as e:
            self.logger.error(f"Ошибка при обновлении/добавлении данных: {e}")
            import traceback
            self.logger.error(f"Полный стек трейс: {traceback.format_exc()}")
            return False

    def append_rows_data(self, data: List[List]) -> bool:
        """
        Добавляет несколько строк данных в конец таблицы.

        Args:
            data: список списков значений для записи

        Returns:
            bool: True при успешной записи
        """
        try:
            if not data:
                return True

            # Добавляем строки с помощью append_rows
            self.worksheet.append_rows(data, value_input_option='USER_ENTERED')

            self.logger.info(f"Успешно добавлено {len(data)} строк")
            return True
        except Exception as e:
            self.logger.error(f"Ошибка при добавлении строк: {e}")
            return False


def connect_to_spreadsheet(credentials_path: str, spreadsheet_name: str, worksheet_name: str = "Лист1") -> GoogleSheetsReporter:
    """
    Функция подключения к Google-таблице.

    Args:
        credentials_path (str): путь к файлу учетных данных
        spreadsheet_name (str): ID или имя таблицы
        worksheet_name (str): имя листа (по умолчанию "Лист1")

    Returns:
        GoogleSheetsReporter: экземпляр класса для работы с таблицей
    """
    return GoogleSheetsReporter(credentials_path, spreadsheet_name, worksheet_name)


def read_data_from_sheet(reporter: GoogleSheetsReporter, start_row: int = 1, end_row: int = None) -> List[List[str]]:
    """
    Функция чтения данных из листа.

    Args:
        reporter (GoogleSheetsReporter): экземпляр класса для работы с таблицей
        start_row (int): начальная строка для чтения
        end_row (int): конечная строка для чтения (если None, читает до конца)

    Returns:
        List[List[str]]: список строк с данными
    """
    try:
        if end_row is None:
            all_values = reporter.worksheet.get_all_values()
            return all_values[start_row-1:]
        else:
            range_name = f'A{start_row}:Z{end_row}'  # предполагаем максимум 26 столбцов
            values = reporter.worksheet.get(range_name)
            return values
    except Exception as e:
        reporter.logger.error(f"Ошибка при чтении данных из листа: {e}")
        return []


def write_data_to_sheet(reporter: GoogleSheetsReporter, data: List[List[Any]], start_row: int = None) -> bool:
    """
    Функция записи данных в лист.

    Args:
        reporter (GoogleSheetsReporter): экземпляр класса для работы с таблицей
        data (List[List[Any]]): данные для записи
        start_row (int): строка начала записи (если None, добавляет в конец)

    Returns:
        bool: True при успешной записи
    """
    try:
        if start_row is None:
            # Если строка не указана, добавляем в конец
            last_row = reporter.get_last_row_with_data()
            start_row = last_row + 1
        
        # Определяем диапазон для записи
        num_rows = len(data)
        num_cols = max(len(row) for row in data) if data else 0
        end_col = chr(64 + num_cols)  # Преобразуем число в букву столбца (A=1, B=2, ..., Z=26)
        
        range_name = f'A{start_row}:{end_col}{start_row + num_rows - 1}'
        
        # Записываем данные
        reporter.worksheet.update(range_name, data, value_input_option='USER_ENTERED')
        
        reporter.logger.info(f"Данные успешно записаны в диапазон {range_name}")
        return True
    except Exception as e:
        reporter.logger.error(f"Ошибка при записи данных в лист: {e}")
        return False