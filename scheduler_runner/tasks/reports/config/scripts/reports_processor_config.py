"""
reports_processor_config.py

Конфиг для reports_processor задачи reports.

Author: anikinjura
"""
__version__ = '0.0.2'

MODULE_PATH = "scheduler_runner.tasks.reports.reports_processor"

BACKFILL_CONFIG = {
    "default_days": 7,
    "default_parser_api": "legacy",
    "max_missing_dates_per_run": 7,
    "strict_headers": True,
    "max_scan_rows": 5000,
    "max_expected_keys": 1000,
}

SCHEDULE = [
    {
        "name": "ReportsProcessor",
        "module": MODULE_PATH,
        "args": [],
        "schedule": "daily",
        "time": "21:10",
        "user": "operator",
        "timeout": 360,
    },
]
