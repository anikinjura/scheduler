"""
GoogleSheetsReporter.py

Класс для работы с Google-таблицами.
Предоставляет методы для подключения к Google-таблице, определения последней строки с данными,
добавления новых строк, обновления существующих записей и валидации структуры данных.

Функции:
- Подключение к Google-таблице через Service Account по ID таблицы
- Определение последней строки с данными в указанном столбце
- Добавление новой строки с данными
- Обновление существующих записей при необходимости
- Получение заголовков столбцов
- Валидация структуры данных

Author: anikinjura
"""
__version__ = '0.0.1'

import gspread
from google.oauth2.service_account import Credentials
from pathlib import Path
import json
from datetime import datetime
from typing import Dict, List, Optional

from scheduler_runner.utils.logging import configure_logger


class GoogleSheetsReporter:
    """
    Класс для работы с Google-таблицами.
    
    Предоставляет методы для:
    - Подключения к Google-таблице
    - Определения последней строки с данными
    - Добавления новых строк с данными отчета
    - Обновления существующих записей
    """
    
    def __init__(self, credentials_path: str, spreadsheet_name: str, worksheet_name: str = "Лист1"):
        """
        Инициализация клиента Google Sheets.
        
        Args:
            credentials_path: путь к JSON-файлу с ключами сервисного аккаунта
            spreadsheet_name: имя Google-таблицы
            worksheet_name: имя листа в таблице (по умолчанию "Лист1")
        """
        self.credentials_path = Path(credentials_path)
        self.spreadsheet_name = spreadsheet_name
        self.worksheet_name = worksheet_name
        
        # Настройка аутентификации
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = Credentials.from_service_account_file(
            str(self.credentials_path), 
            scopes=scopes
        )
        self.client = gspread.authorize(credentials)
        
        # Открытие таблицы и листа
        # Если spreadsheet_name - это ID таблицы (содержит только латинские буквы, цифры и специальные символы),
        # используем open_by_key, иначе - open
        if len(self.spreadsheet_name) > 10 and all(c.isalnum() or c in '_- ' for c in self.spreadsheet_name.replace('_', '').replace('-', '')):
            # Предполагаем, что это ID таблицы, если строка достаточно длинная и содержит допустимые символы ID
            self.spreadsheet = self.client.open_by_key(self.spreadsheet_name)
        else:
            # Используем обычное открытие по названию
            self.spreadsheet = self.client.open(self.spreadsheet_name)
        self.worksheet = self.spreadsheet.worksheet(self.worksheet_name)
        
        # Настройка логгера
        self.logger = configure_logger(
            user='system',
            task_name='GoogleSheetsReporter',
            detailed=False
        )
        
        self.logger.info(f"Подключено к таблице: {self.spreadsheet_name}, лист: {self.worksheet_name}")
    
    def get_last_row_with_data(self, date_column_index: int = 1) -> int:
        """
        Получает номер последней строки с данными в указанном столбце.

        Args:
            date_column_index: индекс столбца с датами (1-индексированный)

        Returns:
            int: номер последней строки с данными
        """
        try:
            # Получаем все значения в столбце
            column_values = self.worksheet.col_values(date_column_index)
            # Находим последнюю непустую строку
            for i in range(len(column_values), 0, -1):
                if column_values[i-1].strip():  # проверяем, что строка не пустая
                    return i
            # Если все строки пустые, возвращаем 0
            return 0
        except Exception as e:
            self.logger.error(f"Ошибка при получении последней строки: {e}")
            return 1  # Возвращаем первую строку как безопасное значение
    
    def append_row_data(self, data: List, start_row: int = None) -> bool:
        """
        Добавляет данные в следующую строку после последней заполненной.

        Args:
            data: список значений для записи в строку
            start_row: номер строки для записи (если None, определяется автоматически)

        Returns:
            bool: True при успешной записи
        """
        try:
            if start_row is None:
                # Используем append_row для добавления в конец таблицы, а не insert_row
                self.worksheet.append_row(data)
                # Получаем номер последней строки после добавления
                last_row = self.get_last_row_with_data()
                self.logger.info(f"Данные успешно добавлены в строку {last_row}")
            else:
                # Если указан конкретный номер строки, используем insert_row (осторожно!)
                self.worksheet.insert_row(data, start_row)
                self.logger.info(f"Данные успешно добавлены в строку {start_row}")
            return True
        except Exception as e:
            self.logger.error(f"Ошибка при добавлении строки: {e}")
            return False

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
            target_row = current_last_row + 1

            # Создаем диапазон для обновления новой строки
            # Обновляем всю строку целиком
            range_name = f'A{target_row}:{chr(64+len(data))}{target_row}'

            # Обновляем строку с правильным value_input_option
            self.worksheet.update(range_name, [data], value_input_option='USER_ENTERED')

            self.logger.info(f"Данные успешно добавлены в строку {target_row}")
            return target_row
        except Exception as e:
            self.logger.error(f"Ошибка при добавлении строки: {e}")
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
                            # Обновляем всю строку новыми значениями
                            values = [data.get(key, "") for key in data.keys()]
                            self.worksheet.update(f'A{row_num}:{chr(64+len(values))}{row_num}', [values])
                            expected_id = f"{date_value}{pvz_value}"
                            self.logger.info(f"Данные за {date_value} для ПВЗ {pvz_value} обновлены в строке {row_num} (Id: {expected_id})")
                            return True
                except Exception as e:
                    self.logger.error(f"Ошибка при поиске существующей записи: {e}")
                # Если не найдено совпадение, добавляем новую строку с формулой в Id столбце
                self.logger.debug(f"update_or_append_data: запись с датой '{date_value}' и ПВЗ '{pvz_value}' не найдена, добавляем новую строку")
                # Подготовим данные для добавления, но сначала добавим строку с пустым Id
                # Затем установим формулу в Id столбце
                current_last_row = self.get_last_row_with_data()
                target_row = current_last_row + 1

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
                result = self.append_row_data(values)
                self.logger.debug(f"update_or_append_data: результат append_row_data: {result}")
                return result
        except Exception as e:
            self.logger.error(f"Ошибка при обновлении/добавлении данных: {e}")
            import traceback
            self.logger.error(f"Полный стек трейс: {traceback.format_exc()}")
            return False
    
    def get_headers(self) -> List[str]:
        """
        Получает заголовки столбцов из первой строки таблицы.
        
        Returns:
            List[str]: список заголовков столбцов
        """
        try:
            headers = self.worksheet.row_values(1)
            return headers
        except Exception as e:
            self.logger.error(f"Ошибка при получении заголовков: {e}")
            return []
    
    def validate_data_structure(self, data: Dict, required_headers: List[str]) -> bool:
        """
        Проверяет, что структура данных соответствует структуре таблицы.
        
        Args:
            data: словарь с данными для проверки
            required_headers: список обязательных заголовков столбцов
            
        Returns:
            bool: True если структура данных корректна
        """
        try:
            data_keys = set(data.keys())
            required_keys = set(required_headers)
            
            # Проверяем, что все обязательные поля присутствуют
            missing_keys = required_keys - data_keys
            if missing_keys:
                self.logger.warning(f"Отсутствуют обязательные поля: {missing_keys}")
                return False
            
            return True
        except Exception as e:
            self.logger.error(f"Ошибка при проверке структуры данных: {e}")
            return False