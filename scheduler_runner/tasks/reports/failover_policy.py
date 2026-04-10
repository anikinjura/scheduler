from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Tuple

from scheduler_runner.tasks.reports.config.scripts.reports_processor_config import FAILOVER_POLICY_CONFIG
from scheduler_runner.tasks.reports.storage.failover_state import (
    STATUS_FAILOVER_SUCCESS,
    STATUS_OWNER_SUCCESS,
    parse_sheet_timestamp,
)
from scheduler_runner.utils.system import SystemUtils


TERMINAL_STATUSES = {STATUS_OWNER_SUCCESS, STATUS_FAILOVER_SUCCESS}
SELECTION_MODE_PRIORITY_MAP_LEGACY = "priority_map_legacy"
SELECTION_MODE_CAPABILITY_RANKED = "capability_ranked"


def normalize_pvz_id(pvz_id: str | None) -> str:
    transliterated = SystemUtils.cyrillic_to_translit(str(pvz_id or ""))
    return transliterated.strip().lower()


def get_selection_mode() -> str:
    raw_mode = str(FAILOVER_POLICY_CONFIG.get("selection_mode", SELECTION_MODE_PRIORITY_MAP_LEGACY) or "")
    normalized_mode = raw_mode.strip().lower()
    if normalized_mode in {SELECTION_MODE_PRIORITY_MAP_LEGACY, SELECTION_MODE_CAPABILITY_RANKED}:
        return normalized_mode
    return SELECTION_MODE_PRIORITY_MAP_LEGACY


def get_priority_map() -> Dict[str, List[str]]:
    raw_map = FAILOVER_POLICY_CONFIG.get("priority_map", {}) or {}
    normalized_map: Dict[str, List[str]] = {}
    for target_pvz, priority_list in raw_map.items():
        normalized_map[normalize_pvz_id(target_pvz)] = [str(item).strip() for item in (priority_list or []) if str(item).strip()]
    return normalized_map


def get_capability_map() -> Dict[str, List[str]]:
    raw_map = FAILOVER_POLICY_CONFIG.get("capability_map", {}) or {}
    normalized_map: Dict[str, List[str]] = {}
    for helper_pvz, target_list in raw_map.items():
        normalized_map[normalize_pvz_id(helper_pvz)] = [str(item).strip() for item in (target_list or []) if str(item).strip()]
    return normalized_map


def get_helper_bias_map() -> Dict[str, int]:
    raw_map = FAILOVER_POLICY_CONFIG.get("helper_bias", {}) or {}
    normalized_map: Dict[str, int] = {}
    for helper_pvz, bias in raw_map.items():
        normalized_map[normalize_pvz_id(helper_pvz)] = int(bias or 0)
    return normalized_map


def get_priority_list(target_pvz: str) -> List[str]:
    return get_priority_map().get(normalize_pvz_id(target_pvz), [])


def has_explicit_priority_rule(target_pvz: str) -> bool:
    return normalize_pvz_id(target_pvz) in get_priority_map()


def helper_has_any_capabilities(helper_pvz: str) -> bool:
    return bool(get_capability_map().get(normalize_pvz_id(helper_pvz), []))


def get_capability_targets_for_helper(helper_pvz: str) -> List[str]:
    return get_capability_map().get(normalize_pvz_id(helper_pvz), [])


def get_helper_candidates_for_target(target_pvz: str) -> List[str]:
    normalized_target_pvz = normalize_pvz_id(target_pvz)
    candidates: List[str] = []
    for helper_pvz, target_list in get_capability_map().items():
        normalized_targets = {normalize_pvz_id(item) for item in target_list}
        if normalized_target_pvz in normalized_targets:
            candidates.append(helper_pvz)
    return sorted(candidates)


def get_live_accessible_helper_candidates(target_pvz: str, available_pvz: Iterable[str]) -> List[str]:
    normalized_target_pvz = normalize_pvz_id(target_pvz)
    normalized_available_pvz = {normalize_pvz_id(pvz_id) for pvz_id in (available_pvz or [])}
    if normalized_target_pvz not in normalized_available_pvz:
        return []
    return get_helper_candidates_for_target(target_pvz)


def build_helper_rank_tuple(helper_pvz: str) -> Tuple[int, str]:
    helper_bias = get_helper_bias_map()
    normalized_helper_pvz = normalize_pvz_id(helper_pvz)
    return (int(helper_bias.get(normalized_helper_pvz, 0)), normalized_helper_pvz)


def select_preferred_helper_for_target(target_pvz: str, available_pvz: Iterable[str]) -> str | None:
    candidates = get_live_accessible_helper_candidates(target_pvz, available_pvz)
    if not candidates:
        return None
    return min(candidates, key=build_helper_rank_tuple)


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


def can_attempt_failover_claim_legacy(
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


def can_attempt_failover_claim_capability_ranked(
    *,
    state_row: Dict[str, Any],
    configured_pvz_id: str,
    available_pvz: Iterable[str],
    now: datetime | None = None,
) -> Dict[str, Any]:
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

    helper_candidates = get_helper_candidates_for_target(target_pvz)
    if not helper_candidates:
        return {"eligible": False, "reason": "no_eligible_helpers"}

    preferred_helper = select_preferred_helper_for_target(target_pvz, available_pvz)
    if not preferred_helper:
        return {
            "eligible": False,
            "reason": "no_accessible_helper_candidates",
            "helper_candidates": helper_candidates,
        }

    if normalized_configured_pvz_id != normalize_pvz_id(preferred_helper):
        return {
            "eligible": False,
            "reason": "not_preferred_helper",
            "helper_candidates": helper_candidates,
            "preferred_helper": preferred_helper,
        }

    return {
        "eligible": True,
        "reason": "eligible",
        "helper_candidates": helper_candidates,
        "preferred_helper": preferred_helper,
    }


def can_attempt_failover_claim(
    *,
    state_row: Dict[str, Any],
    configured_pvz_id: str,
    available_pvz: Iterable[str],
    now: datetime | None = None,
) -> Dict[str, Any]:
    if get_selection_mode() == SELECTION_MODE_CAPABILITY_RANKED:
        return can_attempt_failover_claim_capability_ranked(
            state_row=state_row,
            configured_pvz_id=configured_pvz_id,
            available_pvz=available_pvz,
            now=now,
        )

    return can_attempt_failover_claim_legacy(
        state_row=state_row,
        configured_pvz_id=configured_pvz_id,
        available_pvz=available_pvz,
        now=now,
    )


def evaluate_claimable_rows_by_policy(
    *,
    rows: Iterable[Dict[str, Any]],
    configured_pvz_id: str,
    available_pvz: Iterable[str],
    max_claims: int | None = None,
    now: datetime | None = None,
) -> Dict[str, Any]:
    decisions = []
    eligible_rows = []
    for row in rows or []:
        decision = can_attempt_failover_claim(
            state_row=row,
            configured_pvz_id=configured_pvz_id,
            available_pvz=available_pvz,
            now=now,
        )
        decision_item = {
            "execution_date": row.get("Дата", ""),
            "target_pvz": row.get("target_pvz", ""),
            "status": row.get("status"),
            "eligible": bool(decision.get("eligible", False)),
            "reason": decision.get("reason", ""),
        }
        if "rank" in decision:
            decision_item["rank"] = decision.get("rank")
        if "priority_list" in decision:
            decision_item["priority_list"] = decision.get("priority_list")
        if "helper_candidates" in decision:
            decision_item["helper_candidates"] = decision.get("helper_candidates")
        if "preferred_helper" in decision:
            decision_item["preferred_helper"] = decision.get("preferred_helper")
        if "eligible_time" in decision:
            decision_item["eligible_time"] = decision.get("eligible_time")
        decisions.append(decision_item)
        if decision_item["eligible"]:
            eligible_rows.append(row)

    eligible_rows.sort(key=lambda row: (row.get("Дата", ""), row.get("target_pvz", "")))
    selected_rows = eligible_rows[:max_claims] if max_claims else eligible_rows
    selected_keys = {(row.get("Дата", ""), row.get("target_pvz", "")) for row in selected_rows}
    for decision_item in decisions:
        decision_item["selected_for_claim"] = (
            decision_item["eligible"]
            and (decision_item["execution_date"], decision_item["target_pvz"]) in selected_keys
        )

    rejected_reasons = {}
    for decision_item in decisions:
        if decision_item["eligible"]:
            continue
        reason = decision_item.get("reason", "unknown")
        rejected_reasons[reason] = int(rejected_reasons.get(reason, 0) or 0) + 1

    return {
        "mode": get_selection_mode(),
        "decisions": decisions,
        "eligible_rows": eligible_rows,
        "selected_rows": selected_rows,
        "eligible_count": len(eligible_rows),
        "selected_count": len(selected_rows),
        "rejected_count": max(len(decisions) - len(eligible_rows), 0),
        "rejected_reasons": rejected_reasons,
    }


def filter_claimable_rows_by_policy(
    *,
    rows: Iterable[Dict[str, Any]],
    configured_pvz_id: str,
    available_pvz: Iterable[str],
    max_claims: int | None = None,
    now: datetime | None = None,
) -> List[Dict[str, Any]]:
    evaluation = evaluate_claimable_rows_by_policy(
        rows=rows,
        configured_pvz_id=configured_pvz_id,
        available_pvz=available_pvz,
        max_claims=max_claims,
        now=now,
    )
    return evaluation["selected_rows"]

