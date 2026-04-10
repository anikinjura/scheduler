"""
failover_state.py — backward-compatible re-export layer.

Этот модуль:
1. Ре-экспортирует protocol + helpers из failover_state_protocol
2. Ре-экспортирует Google Sheets implementation из google_sheets_store
3. Предоставляет default store instance для существующего кода

Для нового кода рекомендуется:
- Импортировать FailoverStateStore из failover_state_protocol
- Передавать конкретную реализацию через dependency injection
"""
from __future__ import annotations

# ── Protocol + status constants + helpers ──
from scheduler_runner.tasks.reports.storage.failover_state_protocol import (
    STATUS_CLAIM_EXPIRED,
    STATUS_FAILOVER_CLAIMED,
    STATUS_FAILOVER_FAILED,
    STATUS_FAILOVER_SUCCESS,
    STATUS_OWNER_FAILED,
    STATUS_OWNER_PENDING,
    STATUS_OWNER_SUCCESS,
    TERMINAL_STATUSES,
    FAILOVER_STATE_UPSERT_KEY_COLUMNS,
    FailoverStateStore,
    build_failover_request_id,
    build_failover_state_record,
    is_claim_active_stateless,
)

# ── Google Sheets implementation ──
from scheduler_runner.tasks.reports.storage.google_sheets_store import (
    create_failover_state_logger,
    failover_state_connection,
    get_failover_state,
    get_failover_state_rows_by_keys,
    is_claim_active,
    list_candidate_failover_rows_fast,
    list_failover_state_rows,
    mark_failover_state,
    try_claim_failover,
    upsert_failover_state,
    upsert_failover_state_records,
    get_failover_claim_backend,
    get_failover_apps_script_config,
    try_claim_failover_via_apps_script,
    try_claim_failover_via_sheets,
    verify_claim_ownership,
    prepare_failover_state_connection_params,
    format_sheet_date,
    format_sheet_timestamp,
    parse_sheet_timestamp,
)

# ── Default Google Sheets store instance ──
_default_store = None


def get_default_store() -> FailoverStateStore:
    """Получить default store (Google Sheets singleton)."""
    global _default_store
    if _default_store is None:
        from scheduler_runner.tasks.reports.storage.google_sheets_failover_store import GoogleSheetsFailoverStore
        _default_store = GoogleSheetsFailoverStore()
    return _default_store


def reset_default_store():
    """Сбросить default store (для тестов)."""
    global _default_store
    _default_store = None


def set_default_store(store: FailoverStateStore) -> None:
    """Установить кастомный default store (для DI / тестов)."""
    global _default_store
    _default_store = store

