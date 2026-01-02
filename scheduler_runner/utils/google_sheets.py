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

from scheduler_runner.utils.logging import configure_logger


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
                                # Получаем текущие значения строки
                                current_row_values = self.worksheet.row_values(row_num)
                                # Подготавливаем новые значения, но оставляем Id столбец (A) без изменений
                                updated_values = [current_row_values[0] if len(current_row_values) > 0 else ""]  # Id столбец - оставляем как есть
                                for key in ["Дата", "ПВЗ", "Количество выдач", "Селлер (FBS)", "Обработано возвратов"]:
                                    updated_values.append(data.get(key, ""))

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

                # Подготовим данные с пустым Id, сохраняя типы данных
                # Дата - строка
                date_value = data.get("Дата", "")
                # ПВЗ - строка
                pvz_value = data.get("ПВЗ", "")
                # Количество выдач - число (если возможно)
                giveout_value = data.get("Количество выдач", 0)
                if isinstance(giveout_value, str) and giveout_value.isdigit():
                    giveout_value = int(giveout_value)
                elif isinstance(giveout_value, (int, float)):
                    pass  # уже число
                else:
                    giveout_value = 0  # по умолчанию

                # Селлер (FBS) - строка
                seller_value = data.get("Селлер (FBS)", "")
                # Обработано возвратов - число (если возможно)
                returns_value = data.get("Обработано возвратов", 0)
                if isinstance(returns_value, str) and returns_value.isdigit():
                    returns_value = int(returns_value)
                elif isinstance(returns_value, (int, float)):
                    pass  # уже число
                else:
                    returns_value = 0  # по умолчанию

                # Подготовим данные с пустым Id
                values = ["", date_value, pvz_value, giveout_value, seller_value, returns_value]

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
                values = [data.get(key, "") for key in data.keys()]
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