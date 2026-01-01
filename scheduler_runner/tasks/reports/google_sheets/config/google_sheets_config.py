"""
google_sheets_config.py

Конфигурационные параметры для подключения к Google Sheets.

Author: anikinjura
"""
__version__ = '0.0.1'

from pathlib import Path

# Путь к файлу с ключами сервисного аккаунта
GOOGLE_CREDENTIALS_PATH = ".env/gspread/delta-pagoda-483016-n8-52088e23e06d.json"

# ID Google-таблицы (из URL: https://docs.google.com/spreadsheets/d/[ID]/edit)
SPREADSHEET_NAME = "1D9msGQtGV67ExJBDYlcMhyWVKrV690iSThd2iW361P8"

# Имя листа в таблице
WORKSHEET_NAME = "KPI"

# Структура данных для валидации (реальные заголовки из листа KPI)
REQUIRED_HEADERS = [
    "id",
    "Дата",
    "ПВЗ",
    "Количество выдач",
    "Селлер (FBS)",
    "Обработано возвратов"
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