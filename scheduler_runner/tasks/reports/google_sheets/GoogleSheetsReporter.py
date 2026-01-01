"""
GoogleSheetsReporter.py

Модуль для автоматической отправки данных отчетов ОЗОН в Google-таблицу.

Функции:
- Подключение к Google-таблице через Service Account
- Определение последней строки с данными
- Добавление новой строки с данными отчета
- Обновление существующих записей при необходимости

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
            # Получаем все значения в столбце даты
            date_column = self.worksheet.col_values(date_column_index)
            # Возвращаем длину списка, что соответствует последней строке с данными
            return len(date_column)
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
                last_row = self.get_last_row_with_data()
                start_row = last_row + 1
            
            # Записываем данные в следующую строку
            self.worksheet.insert_row(data, start_row)
            self.logger.info(f"Данные успешно добавлены в строку {start_row}")
            return True
        except Exception as e:
            self.logger.error(f"Ошибка при добавлении строки: {e}")
            return False
    
    def update_or_append_data(self, data: Dict, date_key: str = "date") -> bool:
        """
        Обновляет данные, если запись с такой датой уже существует, или добавляет новую.
        
        Args:
            data: словарь с данными для записи
            date_key: ключ в словаре, содержащий дату для поиска дубликатов
            
        Returns:
            bool: True при успешной записи
        """
        try:
            # Проверяем, есть ли уже запись с такой датой
            date_value = data.get(date_key)
            if date_value:
                try:
                    # Ищем ячейку с указанной датой в первом столбце
                    date_cell = self.worksheet.find(date_value)
                    # Если нашли, обновляем строку
                    row_num = date_cell.row
                    # Обновляем всю строку новыми значениями
                    values = [data.get(key, "") for key in data.keys()]
                    self.worksheet.update(f'A{row_num}:{chr(64+len(values))}{row_num}', [values])
                    self.logger.info(f"Данные за {date_value} обновлены в строке {row_num}")
                    return True
                except gspread.exceptions.CellNotFound:
                    # Если не нашли, добавляем новую строку
                    values = [data.get(key, "") for key in data.keys()]
                    return self.append_row_data(values)
            else:
                # Если дата не указана, просто добавляем новую строку
                values = [data.get(key, "") for key in data.keys()]
                return self.append_row_data(values)
        except Exception as e:
            self.logger.error(f"Ошибка при обновлении/добавлении данных: {e}")
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