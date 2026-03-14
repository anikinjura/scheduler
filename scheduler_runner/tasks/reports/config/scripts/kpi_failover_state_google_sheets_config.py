"""
kpi_failover_state_google_sheets_config.py

Конфигурация worksheet `KPI_FAILOVER_STATE` для coordination/failover state.
Используется orchestration-слоем reports и не смешивается с KPI data sheet.
"""

from scheduler_runner.utils.uploader.core.providers.google_sheets.google_sheets_data_models import (
    ColumnDefinition,
    ColumnType,
    TableConfig,
)
from scheduler_runner.tasks.reports.config.scripts.kpi_google_sheets_config import SPREADSHEET_ID


FAILOVER_STATE_TABLE_CONFIG = TableConfig(
    worksheet_name="KPI_FAILOVER_STATE",
    id_column="request_id",
    columns=[
        ColumnDefinition(
            name="request_id",
            column_type=ColumnType.FORMULA,
            formula_template="=B{row}&C{row}",
        ),
        ColumnDefinition(
            name="Дата",
            column_type=ColumnType.DATA,
            required=True,
            unique_key=True,
            coverage_filter=True,
            coverage_filter_type="date_range",
            date_input_format="YYYY-MM-DD",
            date_output_format="DD.MM.YYYY",
        ),
        ColumnDefinition(
            name="target_pvz",
            column_type=ColumnType.DATA,
            required=True,
            unique_key=True,
            coverage_filter=True,
            coverage_filter_type="list",
            normalization="strip_lower_str",
        ),
        ColumnDefinition(
            name="owner_pvz",
            column_type=ColumnType.DATA,
            required=True,
            normalization="strip_lower_str",
        ),
        ColumnDefinition(name="status", column_type=ColumnType.DATA, required=True),
        ColumnDefinition(name="claimed_by", column_type=ColumnType.DATA, normalization="strip_lower_str"),
        ColumnDefinition(name="claim_expires_at", column_type=ColumnType.DATA),
        ColumnDefinition(name="attempt_no", column_type=ColumnType.DATA),
        ColumnDefinition(name="source_run_id", column_type=ColumnType.DATA),
        ColumnDefinition(name="last_error", column_type=ColumnType.DATA),
        ColumnDefinition(name="updated_at", column_type=ColumnType.DATA, required=True),
    ],
    unique_key_columns=["Дата", "target_pvz"],
)


KPI_FAILOVER_STATE_GOOGLE_SHEETS_CONFIG = {
    "TABLE_CONFIG": FAILOVER_STATE_TABLE_CONFIG,
    "SPREADSHEET_ID": SPREADSHEET_ID,
    "WORKSHEET_NAME": "KPI_FAILOVER_STATE",
    "REQUIRED_CONNECTION_PARAMS": ["CREDENTIALS_PATH", "SPREADSHEET_ID", "WORKSHEET_NAME", "TABLE_CONFIG"],
}
