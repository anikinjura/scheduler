"""
google_sheets_config.py

Конфигурационные параметры для подключения к Google Sheets.

Author: anikinjura
"""
__version__ = '0.0.1'

from pathlib import Path

# Путь к файлу с ключами сервисного аккаунта
GOOGLE_CREDENTIALS_PATH = "C:/tools/gspread/service_account.json"

# Имя Google-таблицы
SPREADSHEET_NAME = "Отчеты ОЗОН по ПВЗ"

# Имя листа в таблице
WORKSHEET_NAME = "Данные"

# Структура данных для валидации
REQUIRED_HEADERS = [
    "Дата",
    "ПВЗ",
    "Количество выданных",
    "Процент выполнения",
    "Комментарии"
]

# Конфигурация для задачи
GOOGLE_SHEETS_CONFIG = {
    "CREDENTIALS_PATH": GOOGLE_CREDENTIALS_PATH,
    "SPREADSHEET_NAME": SPREADSHEET_NAME,
    "WORKSHEET_NAME": WORKSHEET_NAME,
    "REQUIRED_HEADERS": REQUIRED_HEADERS,
    "USER": "system",
    "TASK_NAME": "GoogleSheetsReporter",
    "DETAILED_LOGS": False
}