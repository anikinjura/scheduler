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
    "enable_failover_coordination": True,
    "failover_claim_ttl_minutes": 30,  # больше времени восстановления (браузер + до 3 дат), иначе дату перехватит второй помощник
    "failover_max_claims_per_run": 3,
    "failover_claim_backend": "apps_script",
    "failover_apps_script_timeout_seconds": 30,  # вызов 4–7 с, холодный старт скрипта и ожидание LockService — дольше 15 с
    # Молчащие ПВЗ (не запускались, строк состояния нет): помощник ищет их пропуски в KPI (до вчерашнего дня) и захватывает
    # с созданием строки — нужен Apps Script версии 2 (create_if_missing). Помощник ранга r > 1 ждет еще (r-1)*lag дней.
    "failover_detect_silent_owners": True,  # 01.10.2026: Apps Script версии 2 развернут, create_if_missing проверен
    "failover_silent_rank_lag_days": 1,
    "owner_state_sync_max_attempts": 3,
    "owner_state_sync_base_delay_seconds": 2.0,
    "owner_state_sync_max_delay_seconds": 8.0,
    "owner_state_sync_jitter_seconds": 1.0,
    # Квота Google Sheets API общая для всех ПВЗ (один сервисный аккаунт) и считается поминутно:
    # ПВЗ стартуют одновременно, поэтому загрузка разносится случайной паузой, а 429 повторяется не раньше чем через минуту
    "upload_start_jitter_seconds": 120,
    "google_sheets_quota_retry_delay_seconds": 65,
    # Загрузка KPI: "row" — построчный upsert (~10 запросов на строку), "batch" — пакетный (1 чтение и до 2 записей
    # на пакет + подключение). kpi_upload_mode — режим для всех ПВЗ (новые ПВЗ получают его автоматически);
    # kpi_upload_mode_by_pvz — только временные исключения на время обкатки, после нее меняется kpi_upload_mode.
    # 01.10.2026: "batch" для всех ПВЗ после обкатки на ЧЕБОКСАРЫ_144 (с 30.09.2026, 4 чтения / 2 записи на пакет)
    "kpi_upload_mode": "batch",
    "kpi_upload_mode_by_pvz": {},  # временные исключения, например откат одного ПВЗ на "row"
}

FAILOVER_POLICY_CONFIG = {
    "enabled": True,
    "selection_mode": "priority_map_legacy",
    "priority_map": {
        "ЧЕБОКСАРЫ_143": ["ЧЕБОКСАРЫ_144"],
        "ЧЕБОКСАРЫ_182": ["ЧЕБОКСАРЫ_144"],
        "ЧЕБОКСАРЫ_144": ["ЧЕБОКСАРЫ_182", "ЧЕБОКСАРЫ_143"],
        # Пункт закрыт с 12.09.2026: никто не помогает (пустой список — явный отказ; без ключа строки пункта были бы
        # доступны любому помощнику). Открытие пункта — вернуть ["ЧЕБОКСАРЫ_144"].
        "СОСНОВКА_10": [],
        "ЧЕБОКСАРЫ_340": [],
    },
    "capability_map": {
        "ЧЕБОКСАРЫ_143": ["ЧЕБОКСАРЫ_144"],
        "ЧЕБОКСАРЫ_182": ["ЧЕБОКСАРЫ_144"],
        "ЧЕБОКСАРЫ_144": ["ЧЕБОКСАРЫ_182", "ЧЕБОКСАРЫ_143"],
        "СОСНОВКА_10": ["ЧЕБОКСАРЫ_144"],
        "ЧЕБОКСАРЫ_340": [],
    },
    "helper_bias": {},
    "dry_run_capability_ranked": True,
    "default_rank_delay_minutes": 10,
    "max_attempts_per_date": 3,
    "candidate_window_days": 7,  # строки KPI_FAILOVER_STATE старше окна не восстанавливаются (как окно backfill владельца)
    "max_claims_per_run": 3,
    "allow_unlisted_fallback": False,
    "prefer_lower_load_helpers": False,
}

SCHEDULE = [
    {
        "name": "ReportsProcessor",
        "module": MODULE_PATH,
        "args": [],
        "schedule": "daily",
        "time": "21:10",
        "user": "operator",
        "timeout": 2700,  # парсинг 7 дат ~15 мин + паузы при квоте Google Sheets (429) — с запасом до следующего часа
    },
]

