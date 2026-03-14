from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List

from scheduler_runner.tasks.reports.config.scripts.reports_processor_config import FAILOVER_POLICY_CONFIG
from scheduler_runner.tasks.reports.failover_state import (
    STATUS_FAILOVER_SUCCESS,
    STATUS_OWNER_SUCCESS,
    parse_sheet_timestamp,
)
from scheduler_runner.utils.system import SystemUtils


TERMINAL_STATUSES = {STATUS_OWNER_SUCCESS, STATUS_FAILOVER_SUCCESS}


def normalize_pvz_id(pvz_id: str | None) -> str:
    transliterated = SystemUtils.cyrillic_to_translit(str(pvz_id or ""))
    return transliterated.strip().lower()


def get_priority_map() -> Dict[str, List[str]]:
    raw_map = FAILOVER_POLICY_CONFIG.get("priority_map", {}) or {}
    normalized_map: Dict[str, List[str]] = {}
    for target_pvz, priority_list in raw_map.items():
        normalized_map[normalize_pvz_id(target_pvz)] = [str(item).strip() for item in (priority_list or []) if str(item).strip()]
    return normalized_map


def get_priority_list(target_pvz: str) -> List[str]:
    return get_priority_map().get(normalize_pvz_id(target_pvz), [])


def has_explicit_priority_rule(target_pvz: str) -> bool:
    return normalize_pvz_id(target_pvz) in get_priority_map()


def get_current_rank(target_pvz: str, claimer_pvz: str) -> int | None:
    priority_list = get_priority_list(target_pvz)
    normalized_claimer_pvz = normalize_pvz_id(claimer_pvz)
    for index, pvz_id in enumerate(priority_list, start=1):
        if normalize_pvz_id(pvz_id) == normalized_claimer_pvz:
            return index
    return None


def get_reference_timestamp(state_row: Dict[str, Any] | None) -> datetime | None:
    if not state_row:
        return None
    return parse_sheet_timestamp(state_row.get("updated_at"))


def get_eligible_time(state_row: Dict[str, Any], rank: int, now: datetime | None = None) -> datetime | None:
    reference_ts = get_reference_timestamp(state_row)
    if not reference_ts:
        return None
    delay_minutes = max(int(FAILOVER_POLICY_CONFIG.get("default_rank_delay_minutes", 10) or 10), 0)
    return reference_ts + timedelta(minutes=max(rank - 1, 0) * delay_minutes)


def can_attempt_failover_claim(
    *,
    state_row: Dict[str, Any],
    configured_pvz_id: str,
    available_pvz: Iterable[str],
    now: datetime | None = None,
) -> Dict[str, Any]:
    current_time = now or datetime.now()
    target_pvz = state_row.get("target_pvz", "")
    normalized_target_pvz = normalize_pvz_id(target_pvz)
    normalized_configured_pvz_id = normalize_pvz_id(configured_pvz_id)
    normalized_available_pvz = {normalize_pvz_id(pvz_id) for pvz_id in (available_pvz or [])}
    status = state_row.get("status")

    if not target_pvz:
        return {"eligible": False, "reason": "missing_target_pvz"}
    if status in TERMINAL_STATUSES:
        return {"eligible": False, "reason": "terminal_status"}
    if normalized_target_pvz == normalized_configured_pvz_id:
        return {"eligible": False, "reason": "own_target_pvz"}
    if normalized_target_pvz not in normalized_available_pvz:
        return {"eligible": False, "reason": "not_accessible"}

    attempt_no = int(state_row.get("attempt_no") or 0)
    max_attempts = int(FAILOVER_POLICY_CONFIG.get("max_attempts_per_date", 3) or 3)
    if max_attempts > 0 and attempt_no >= max_attempts:
        return {"eligible": False, "reason": "max_attempts_reached"}

    priority_list = get_priority_list(target_pvz)
    has_explicit_rule = has_explicit_priority_rule(target_pvz)
    rank = get_current_rank(target_pvz, configured_pvz_id)
    allow_unlisted_fallback = bool(FAILOVER_POLICY_CONFIG.get("allow_unlisted_fallback", False))

    if has_explicit_rule and rank is None and not allow_unlisted_fallback:
        return {"eligible": False, "reason": "not_in_priority"}

    if rank is not None:
        eligible_time = get_eligible_time(state_row, rank=rank, now=current_time)
        if eligible_time and current_time < eligible_time:
            return {
                "eligible": False,
                "reason": "rank_delay",
                "rank": rank,
                "eligible_time": eligible_time,
            }

    return {
        "eligible": True,
        "reason": "eligible",
        "rank": rank,
        "priority_list": priority_list,
    }


def filter_claimable_rows_by_policy(
    *,
    rows: Iterable[Dict[str, Any]],
    configured_pvz_id: str,
    available_pvz: Iterable[str],
    max_claims: int | None = None,
    now: datetime | None = None,
) -> List[Dict[str, Any]]:
    eligible_rows = []
    for row in rows or []:
        decision = can_attempt_failover_claim(
            state_row=row,
            configured_pvz_id=configured_pvz_id,
            available_pvz=available_pvz,
            now=now,
        )
        if decision.get("eligible", False):
            eligible_rows.append(row)

    eligible_rows.sort(key=lambda row: (row.get("Дата", ""), row.get("target_pvz", "")))
    if max_claims:
        eligible_rows = eligible_rows[:max_claims]
    return eligible_rows
