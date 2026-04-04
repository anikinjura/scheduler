"""
kpi_google_sheets_config.py

Конфигурация для загрузки KPI данных в Google Sheets.
Используется в центральном процессоре поддомена для получения параметров подключения к Google Sheets.

Author: anikinjura
"""

from scheduler_runner.utils.uploader.core.providers.google_sheets.google_sheets_data_models import TableConfig, ColumnDefinition, ColumnType

# Конфигурация структуры таблицы для KPI отчетов
TABLE_CONFIG = TableConfig(
    worksheet_name="KPI",
    id_column="id",
    columns=[
        ColumnDefinition(name="id", column_type=ColumnType.FORMULA, formula_template="=B{row}&C{row}"),
        ColumnDefinition(
            name="Дата",
            column_type=ColumnType.DATA,
            required=True,
            unique_key=True,
            coverage_filter=True,
            coverage_filter_type="date_range",
            date_input_format="YYYY-MM-DD",
            date_output_format="DD.MM.YYYY"
        ),
        ColumnDefinition(
            name="ПВЗ",
            column_type=ColumnType.DATA,
            required=True,
            unique_key=True,
            coverage_filter=True,
            coverage_filter_type="list",
            normalization="strip_lower_str"
        ),
        ColumnDefinition(name="Количество выдач", column_type=ColumnType.DATA),
        ColumnDefinition(name="Прямой поток", column_type=ColumnType.DATA),
        ColumnDefinition(name="Возвратный поток", column_type=ColumnType.DATA),
        ColumnDefinition(name="timestamp", column_type=ColumnType.DATA),
        ColumnDefinition(
            name="Сумма за Количество выдач",
            column_type=ColumnType.FORMULA,
            formula_template='=GET_REWARD("Количество выдач";D{row};$B{row};KPI_REWARD_RULES_RANGE)'
        ),
        ColumnDefinition(
            name="Сумма за Прямой поток",
            column_type=ColumnType.FORMULA,
            formula_template='=GET_REWARD("Прямой поток";E{row};$B{row};KPI_REWARD_RULES_RANGE)'
        ),
        ColumnDefinition(
            name="Сумма за Возвратный поток",
            column_type=ColumnType.FORMULA,
            formula_template='=GET_REWARD("Возвратный поток";F{row};$B{row};KPI_REWARD_RULES_RANGE)'
        ),
    ],
    unique_key_columns=["Дата", "ПВЗ"]
)

# ID Google-таблицы (из URL: https://docs.google.com/spreadsheets/d/[ID]/edit)
SPREADSHEET_ID = "1uuaxb0omdb28sFDysiMSTUUMwzPfte4hDDBZ0W9zzH8"

# Имя листа в таблице
WORKSHEET_NAME = "KPI"

# Полная конфигурация
KPI_GOOGLE_SHEETS_CONFIG = {
    "TABLE_CONFIG": TABLE_CONFIG,
    "SPREADSHEET_ID": SPREADSHEET_ID,
    "WORKSHEET_NAME": WORKSHEET_NAME,
    "REQUIRED_CONNECTION_PARAMS": ["CREDENTIALS_PATH", "SPREADSHEET_ID", "WORKSHEET_NAME", "TABLE_CONFIG"]
}
