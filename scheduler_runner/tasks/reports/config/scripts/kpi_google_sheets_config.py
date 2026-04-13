"""
kpi_google_sheets_config.py

Конфигурация для загрузки KPI данных в Google Sheets.
Используется в центральном процессоре поддомена для получения параметров подключения к Google Sheets.

Author: anikinjura
"""

from config.base_config import ENV_MODE
from scheduler_runner.utils.uploader.core.providers.google_sheets.google_sheets_data_models import TableConfig, ColumnDefinition, ColumnType

# Конфигурация структуры таблицы для KPI отчетов
TABLE_CONFIG = TableConfig(
    worksheet_name="KPI",
    id_column="id",
    columns=[
        ColumnDefinition(name="id", column_type=ColumnType.FORMULA, formula_template="=B{row}&C{row}"),
        ColumnDefinition(
            name="work_date",
            column_type=ColumnType.DATA,
            required=True,
            unique_key=True,
            coverage_filter=True,
            coverage_filter_type="date_range",
            date_input_format="YYYY-MM-DD",
            date_output_format="DD.MM.YYYY"
        ),
        ColumnDefinition(
            name="object_name",
            column_type=ColumnType.DATA,
            required=True,
            unique_key=True,
            coverage_filter=True,
            coverage_filter_type="list",
            normalization="strip_lower_str"
        ),
        ColumnDefinition(name="issued_packages", column_type=ColumnType.DATA),
        ColumnDefinition(name="direct_flow", column_type=ColumnType.DATA),
        ColumnDefinition(name="return_flow", column_type=ColumnType.DATA),
        ColumnDefinition(
            name="reward_issued_packages",
            column_type=ColumnType.FORMULA,
            formula_template='=GET_REWARD("issued_packages";D{row};$B{row};KPI_REWARD_RULES_RANGE)'
        ),
        ColumnDefinition(
            name="reward_direct_flow",
            column_type=ColumnType.FORMULA,
            formula_template='=GET_REWARD("direct_flow";E{row};$B{row};KPI_REWARD_RULES_RANGE)'
        ),
        ColumnDefinition(
            name="reward_return_flow",
            column_type=ColumnType.FORMULA,
            formula_template='=GET_REWARD("return_flow";F{row};$B{row};KPI_REWARD_RULES_RANGE)'
        ),
        ColumnDefinition(
            name="total_reward",
            column_type=ColumnType.FORMULA,
            formula_template="=SUM(G{row}:I{row})"
        ),
        ColumnDefinition(name="timestamp", column_type=ColumnType.DATA),
    ],
    unique_key_columns=["work_date", "object_name"]
)

# ID Google-таблицы (разделение по ENV_MODE)
SPREADSHEET_ID_PROD = "1uuaxb0omdb28sFDysiMSTUUMwzPfte4hDDBZ0W9zzH8"
SPREADSHEET_ID_TEST = "1n6Tsa4LmSoVDcRRFj6QKvnU4NfIpq1h9_MRwnyuiKW4"

SPREADSHEET_ID = SPREADSHEET_ID_TEST if ENV_MODE == "test" else SPREADSHEET_ID_PROD

# Имя листа в таблице
WORKSHEET_NAME = "KPI"

# Полная конфигурация
KPI_GOOGLE_SHEETS_CONFIG = {
    "TABLE_CONFIG": TABLE_CONFIG,
    "SPREADSHEET_ID": SPREADSHEET_ID,
    "WORKSHEET_NAME": WORKSHEET_NAME,
    "REQUIRED_CONNECTION_PARAMS": ["CREDENTIALS_PATH", "SPREADSHEET_ID", "WORKSHEET_NAME", "TABLE_CONFIG"]
}

