"""
storage/ — Storage abstraction layer for KPI_FAILOVER_STATE.

This package contains the storage protocol, concrete implementations,
and backward-compatible re-export layer.

Structure:
    failover_state_protocol.py  — ABC interface (FailoverStateStore)
    failover_state.py           — backward-compatible re-export + DI
    google_sheets_store.py      — Google Sheets implementation functions
    google_sheets_failover_store.py — GoogleSheetsFailoverStore adapter class

To use the protocol:
    from . import FailoverStateStore

To use the default Google Sheets store:
    from . import get_default_store
"""

# ── Protocol + helpers (always available) ──
from .failover_state_protocol import (
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

# ── Backward-compatible re-export (Google Sheets) ──
from .failover_state import (
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
    get_default_store as _fs_get_default_store,
    reset_default_store,
    set_default_store as _fs_set_default_store,
)

# ── DI helpers ──
def set_default_store(store: FailoverStateStore) -> None:
    """Установить default storage для всех модулей."""
    from ..owner_state_sync import set_default_store as _oss_set_default_store
    _oss_set_default_store(store)
    _fs_set_default_store(store)


def get_default_store() -> FailoverStateStore:
    """Получить default storage."""
    return _fs_get_default_store()

