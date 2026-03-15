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
    "enable_failover_coordination": False,
    "failover_claim_ttl_minutes": 15,
    "failover_max_claims_per_run": 3,
    "failover_claim_backend": "apps_script",
    "failover_apps_script_timeout_seconds": 15,
}

FAILOVER_POLICY_CONFIG = {
    "enabled": True,
    "priority_map": {
        "ЧЕБОКСАРЫ_143": ["ЧЕБОКСАРЫ_144"],
        "ЧЕБОКСАРЫ_182": ["ЧЕБОКСАРЫ_144"],
        "ЧЕБОКСАРЫ_144": ["ЧЕБОКСАРЫ_182", "ЧЕБОКСАРЫ_143"],
        "СОСНОВКА_10": ["ЧЕБОКСАРЫ_144"],
        "ЧЕБОКСАРЫ_340": [],
    },
    "default_rank_delay_minutes": 10,
    "max_attempts_per_date": 3,
    "max_claims_per_run": 3,
    "allow_unlisted_fallback": False,
}

SCHEDULE = [
    {
        "name": "ReportsProcessor",
        "module": MODULE_PATH,
        "args": [],
        "schedule": "daily",
        "time": "21:10",
        "user": "operator",
        "timeout": 900,
    },
]
