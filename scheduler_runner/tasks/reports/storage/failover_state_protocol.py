"""
failover_state_protocol.py — абстрактный интерфейс storage layer для KPI_FAILOVER_STATE.

Этот модуль определяет контракт, которую должна реализовать любая
storage backend (Google Sheets, PostgreSQL, SQLite, и т.д.).

Цель: замена хранилища НЕ должна требовать изменения бизнес-логики
(failover_policy.py, reports_summary.py, owner_state_sync.py, failover_orchestration.py).
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional


# ──────────────────────────────────────────────
# Константы статусов (не зависят от storage)
# ──────────────────────────────────────────────

STATUS_OWNER_PENDING = "owner_pending"
STATUS_OWNER_SUCCESS = "owner_success"
STATUS_OWNER_FAILED = "owner_failed"
STATUS_FAILOVER_CLAIMED = "failover_claimed"
STATUS_FAILOVER_SUCCESS = "failover_success"
STATUS_FAILOVER_FAILED = "failover_failed"
STATUS_CLAIM_EXPIRED = "claim_expired"

TERMINAL_STATUSES = {STATUS_OWNER_SUCCESS, STATUS_FAILOVER_SUCCESS}
FAILOVER_STATE_UPSERT_KEY_COLUMNS = ["Дата", "target_pvz"]


# ──────────────────────────────────────────────
# Protocol: абстрактный storage layer
# ──────────────────────────────────────────────

class FailoverStateStore(ABC):
    """
    Абстрактный интерфейс для работы с KPI_FAILOVER_STATE.

    Любая реализация (Google Sheets, PostgreSQL, etc.) должна
    реализовать все abstract-методы.

    Contract:
    - Ключ записи: (execution_date: str YYYY-MM-DD, target_pvz: str)
    - Все даты возвращаются/принимаются в формате YYYY-MM-DD
    - record dict содержит все поля строки состояния
    """

    # ── Read operations ──

    @abstractmethod
    def get_row(self, execution_date: str, target_pvz: str) -> Optional[Dict[str, Any]]:
        """Получить одну строку по уникальному ключу (Дата, target_pvz)."""
        ...

    @abstractmethod
    def get_rows_by_keys(
        self,
        keys: List[Dict[str, str]],
    ) -> Dict[tuple, Dict[str, Any]]:
        """
        Batch-read строк по списку ключей.

        Args:
            keys: [{"Дата": "2026-04-01", "target_pvz": "PVZ1"}, ...]

        Returns:
            Dict[(normalized_date, normalized_target_pvz) -> row_dict]
        """
        ...

    @abstractmethod
    def list_rows(
        self,
        statuses: Optional[List[str]] = None,
        target_pvz: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Получить строки с фильтрацией по статусам и target_pvz."""
        ...

    @abstractmethod
    def list_candidate_rows(
        self,
        statuses: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Получить candidate rows для failover scan.

        Должно использовать batch-оптимизацию (не get_all_records).
        """
        ...

    # ── Write operations ──

    @abstractmethod
    def upsert_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Upsert одной записи (update если существует, иначе append).

        Returns:
            {"success": bool, "action": "updated"|"appended"|"error", ...}
        """
        ...

    @abstractmethod
    def upsert_records(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Batch upsert множества записей.

        Returns:
            {
                "success": bool,
                "results": [upsert_result, ...],
                "diagnostics": {
                    "prefetch_keys_count": int,
                    "prefetch_matches_count": int,
                    "updated_count": int,
                    "appended_count": int,
                }
            }
        """
        ...

    @abstractmethod
    def mark_state(
        self,
        execution_date: str,
        target_pvz: str,
        owner_pvz: str,
        status: str,
        source_run_id: str = "",
        last_error: str = "",
        claimed_by: str = "",
        claim_expires_at: str = "",
        attempt_no: int = 0,
    ) -> Dict[str, Any]:
        """
        Обновить статус строки (mark).

        Returns:
            {"success": bool, "row_number": int, ...}
        """
        ...

    # ── Claim operations ──

    @abstractmethod
    def is_claim_active(
        self,
        state_row: Optional[Dict[str, Any]],
        now: Optional[datetime] = None,
    ) -> bool:
        """Проверить, активен ли claim (не истёк TTL)."""
        ...

    @abstractmethod
    def try_claim(
        self,
        execution_date: str,
        target_pvz: str,
        owner_pvz: str,
        claimer_pvz: str,
        ttl_minutes: int,
        source_run_id: str = "",
    ) -> Dict[str, Any]:
        """
        Атомарно claim-ить строку.

        Returns:
            {
                "claimed": bool,
                "reason": str | None,
                "remote_payload": dict | None,  # для Apps Script backend
            }
        """
        ...

    # ── Utility ──

    @abstractmethod
    def get_store_type(self) -> str:
        """Вернуть тип хранилища: 'google_sheets', 'postgresql', 'sqlite', etc."""
        ...


# ──────────────────────────────────────────────
# Helpers, не зависящие от storage
# ──────────────────────────────────────────────

def build_failover_request_id(execution_date: str, target_pvz: str) -> str:
    """Создать уникальный request_id для строки failover state."""
    return f"{execution_date}|{target_pvz}"


def build_failover_state_record(
    execution_date: str,
    target_pvz: str,
    owner_pvz: str,
    status: str,
    source_run_id: str = "",
    last_error: str = "",
    claimed_by: str = "",
    claim_expires_at: str = "",
    attempt_no: int = 0,
    updated_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Создать record для upsert в failover state."""
    from datetime import datetime
    return {
        "request_id": build_failover_request_id(execution_date, target_pvz),
        "Дата": execution_date,
        "target_pvz": target_pvz,
        "owner_pvz": owner_pvz,
        "status": status,
        "claimed_by": claimed_by,
        "claim_expires_at": claim_expires_at,
        "attempt_no": attempt_no,
        "source_run_id": source_run_id,
        "last_error": last_error,
        "updated_at": (updated_at or datetime.now()).strftime("%d.%m.%Y %H:%M:%S"),
    }


def is_claim_active_stateless(
    state_row: Optional[Dict[str, Any]],
    now: Optional[datetime] = None,
) -> bool:
    """
    Проверить активен ли claim без обращения к storage.

    Эта функция может использоваться любой storage backend
    для проверки claim expiry.
    """
    if not state_row:
        return False

    claimed_by = str(state_row.get("claimed_by", "") or "").strip()
    if not claimed_by:
        return False

    claim_expires_at = state_row.get("claim_expires_at", "")
    if not claim_expires_at:
        return False

    now = now or datetime.now()
    try:
        expiry_time = datetime.strptime(str(claim_expires_at), "%d.%m.%Y %H:%M:%S")
        return now < expiry_time
    except (ValueError, TypeError):
        return False

