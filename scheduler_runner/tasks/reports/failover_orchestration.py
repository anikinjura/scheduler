"""
failover_orchestration.py

Orchestration всего failover-phase: candidate evaluation, claim path, recovery execution.

Извлечено из reports_processor.py (Phase 3 — high-risk extraction).
"""
from datetime import datetime

from config.base_config import PVZ_ID
from scheduler_runner.tasks.reports.config.scripts.reports_processor_config import BACKFILL_CONFIG, FAILOVER_POLICY_CONFIG
from scheduler_runner.tasks.reports.storage.failover_state import (
    STATUS_CLAIM_EXPIRED,
    STATUS_FAILOVER_FAILED,
    STATUS_FAILOVER_SUCCESS,
    STATUS_OWNER_FAILED,
    create_failover_state_logger,
    failover_state_connection,
    list_candidate_failover_rows_fast,
    mark_failover_state,
    try_claim_failover,
)
from scheduler_runner.tasks.reports.failover_policy import (
    evaluate_claimable_rows_by_policy,
    filter_claimable_rows_by_policy,
    get_capability_targets_for_helper,
    get_priority_list,
    get_selection_mode,
    has_explicit_priority_rule,
)
from scheduler_runner.utils.logging import configure_logger
from scheduler_runner.utils.parser import (
    build_jobs_for_pvz,
    invoke_parser_for_pvz,
    create_parser_logger,
)

# Извлечённые helpers из reports_summary (Phase 1.1)
from .reports_summary import (
    extract_batch_failures,
    build_filtered_batch_result,
    is_failover_candidate_scan_retryable_error,
)

# Извлечённые helpers из reports_upload (Phase 1.3)
from .reports_upload import (
    create_uploader_logger,
    detect_missing_report_dates,
    run_upload_batch_microservice,
)

# Извлечённые helpers из reports_scope (Phase 2.1)
from .reports_scope import (
    discover_available_pvz_scope,
    normalize_pvz_id,
    build_parser_definition,
    build_jobs_from_missing_dates_by_pvz,
    group_jobs_by_pvz,
)

# Извлечённые helpers из owner_state_sync (Phase 2.2)
from .owner_state_sync import (
    sync_owner_failover_state_from_batch_result,
)


# ──────────────────────────────────────────────
# Candidate collection & policy evaluation
# ──────────────────────────────────────────────

def collect_claimable_failover_rows(
    *,
    accessible_pvz_ids,
    configured_pvz_id=PVZ_ID,
    max_claims=None,
    logger=None,
    uploader=None,
    return_evaluation=False,
):
    """Собирает claimable failover rows и оценивает их по policy."""
    logger = logger or create_failover_state_logger()
    rows = list_candidate_failover_rows_fast(
        statuses=[STATUS_OWNER_FAILED, STATUS_CLAIM_EXPIRED, STATUS_FAILOVER_FAILED],
        logger=logger,
        uploader=uploader,
    )
    if not FAILOVER_POLICY_CONFIG.get("enabled", True):
        filtered_rows = []
        decisions = []
        normalized_accessible = {normalize_pvz_id(pvz_id) for pvz_id in (accessible_pvz_ids or [])}
        normalized_configured_pvz_id = normalize_pvz_id(configured_pvz_id)
        for row in rows:
            target_pvz = row.get("target_pvz")
            if not target_pvz:
                continue
            normalized_target_pvz = normalize_pvz_id(target_pvz)
            decision_item = {
                "execution_date": row.get("Дата", ""),
                "target_pvz": target_pvz,
                "status": row.get("status"),
                "eligible": False,
                "reason": "",
            }
            if normalized_target_pvz == normalized_configured_pvz_id:
                decision_item["reason"] = "own_target_pvz"
                decisions.append(decision_item)
                continue
            if normalized_target_pvz not in normalized_accessible:
                decision_item["reason"] = "not_accessible"
                decisions.append(decision_item)
                continue
            decision_item["eligible"] = True
            decision_item["reason"] = "eligible"
            decisions.append(decision_item)
            filtered_rows.append(row)
        filtered_rows.sort(key=lambda row: (row.get("Дата", ""), row.get("target_pvz", "")))
        selected_rows = filtered_rows[:max_claims] if max_claims else filtered_rows
        selected_keys = {(row.get("Дата", ""), row.get("target_pvz", "")) for row in selected_rows}
        for decision_item in decisions:
            decision_item["selected_for_claim"] = (
                (decision_item.get("execution_date"), decision_item.get("target_pvz")) in selected_keys
            )
        evaluation = {
            "mode": "policy_disabled",
            "total_candidates": len(rows),
            "eligible_count": len(filtered_rows),
            "selected_count": len(selected_rows),
            "rejected_count": len([d for d in decisions if not d["eligible"]]),
            "rejected_reasons": {},
            "selected_rows": selected_rows,
            "decisions": decisions,
        }
        return evaluation if return_evaluation else selected_rows

    if not rows:
        evaluation = {
            "mode": get_selection_mode(),
            "total_candidates": 0,
            "eligible_count": 0,
            "selected_count": 0,
            "rejected_count": 0,
            "rejected_reasons": {},
            "selected_rows": [],
            "decisions": [],
        }
        return evaluation if return_evaluation else []

    filtered_rows, decisions = filter_claimable_rows_by_policy(
        rows=rows,
        configured_pvz_id=configured_pvz_id,
        accessible_pvz_ids=accessible_pvz_ids,
    )
    filtered_rows.sort(key=lambda row: (row.get("Дата", ""), row.get("target_pvz", "")))
    selected_rows = filtered_rows[:max_claims] if max_claims else filtered_rows
    selected_keys = {(row.get("Дата", ""), row.get("target_pvz", "")) for row in selected_rows}
    for decision_item in decisions:
        decision_item["selected_for_claim"] = (
            (decision_item.get("execution_date"), decision_item.get("target_pvz")) in selected_keys
        )
    evaluation = evaluate_claimable_rows_by_policy(
        rows=rows,
        filtered_rows=filtered_rows,
        selected_rows=selected_rows,
        decisions=decisions,
    )
    return evaluation if return_evaluation else selected_rows


def normalize_claimable_failover_evaluation(candidate_evaluation) -> dict:
    """Нормализует evaluation в единый формат."""
    if candidate_evaluation is None:
        return {
            "mode": "unknown",
            "total_candidates": 0,
            "eligible_count": 0,
            "selected_count": 0,
            "rejected_count": 0,
            "rejected_reasons": {},
            "selected_rows": [],
            "decisions": [],
        }
    return {
        "mode": candidate_evaluation.get("mode", "unknown"),
        "total_candidates": candidate_evaluation.get("total_candidates", 0),
        "eligible_count": candidate_evaluation.get("eligible_count", 0),
        "selected_count": candidate_evaluation.get("selected_count", 0),
        "rejected_count": candidate_evaluation.get("rejected_count", 0),
        "rejected_reasons": candidate_evaluation.get("rejected_reasons", {}),
        "selected_rows": candidate_evaluation.get("selected_rows", []),
        "decisions": candidate_evaluation.get("decisions", []),
    }


# ──────────────────────────────────────────────
# Scan decision helpers
# ──────────────────────────────────────────────

def should_scan_failover_candidates(
    *,
    configured_pvz_id=PVZ_ID,
    accessible_pvz_ids=None,
):
    if not FAILOVER_POLICY_CONFIG.get("enabled", True):
        return {"should_scan": True, "reason": "policy_disabled"}

    if get_selection_mode() == "capability_ranked":
        return should_scan_failover_candidates_capability_ranked(
            configured_pvz_id=configured_pvz_id,
            accessible_pvz_ids=accessible_pvz_ids,
        )

    return should_scan_failover_candidates_legacy(
        configured_pvz_id=configured_pvz_id,
        accessible_pvz_ids=accessible_pvz_ids,
    )


def should_scan_failover_candidates_legacy(
    *,
    configured_pvz_id=PVZ_ID,
    accessible_pvz_ids=None,
):
    if not has_explicit_priority_rule(configured_pvz_id):
        return {"should_scan": True, "reason": "no_explicit_rule"}

    priority_list = get_priority_list(configured_pvz_id)
    if not priority_list:
        return {"should_scan": False, "reason": "empty_priority_list"}

    normalized_accessible = {normalize_pvz_id(pvz_id) for pvz_id in (accessible_pvz_ids or [])}
    normalized_priority = {normalize_pvz_id(pvz_id) for pvz_id in priority_list}
    if normalized_accessible & normalized_priority:
        return {"should_scan": True, "reason": "accessible_priority_candidates"}

    return {"should_scan": False, "reason": "priority_candidates_not_accessible"}


def should_scan_failover_candidates_capability_ranked(
    *,
    configured_pvz_id=PVZ_ID,
    accessible_pvz_ids=None,
):
    capability_targets = get_capability_targets_for_helper(configured_pvz_id)
    if not capability_targets:
        return {"should_scan": False, "reason": "empty_capability_list"}

    normalized_accessible = {normalize_pvz_id(pvz_id) for pvz_id in (accessible_pvz_ids or [])}
    normalized_accessible.discard(normalize_pvz_id(configured_pvz_id))
    normalized_targets = {normalize_pvz_id(pvz_id) for pvz_id in capability_targets}
    if normalized_accessible & normalized_targets:
        return {"should_scan": True, "reason": "accessible_capability_targets"}

    return {"should_scan": False, "reason": "capability_targets_not_accessible"}


def collect_failover_scan_decisions(
    *,
    configured_pvz_id=PVZ_ID,
    accessible_pvz_ids=None,
):
    active_decision = should_scan_failover_candidates(
        configured_pvz_id=configured_pvz_id,
        accessible_pvz_ids=accessible_pvz_ids,
    )
    decisions = {
        "active_mode": get_selection_mode(),
        "active": active_decision,
    }
    if FAILOVER_POLICY_CONFIG.get("dry_run_capability_ranked", False):
        decisions["dry_run_capability_ranked"] = should_scan_failover_candidates_capability_ranked(
            configured_pvz_id=configured_pvz_id,
            accessible_pvz_ids=accessible_pvz_ids,
        )
    return decisions


# ──────────────────────────────────────────────
# Claim & recovery
# ──────────────────────────────────────────────

def claim_failover_rows(
    *,
    candidate_rows,
    claimer_pvz,
    ttl_minutes,
    source_run_id="",
    logger=None,
    uploader=None,
):
    """Пытается claim-ить candidate rows через Apps Script LockService."""
    logger = logger or create_failover_state_logger()
    claimed_rows = []
    for row in candidate_rows or []:
        claim_result = try_claim_failover(
            execution_date=row.get("Дата"),
            target_pvz=row.get("target_pvz"),
            owner_pvz=row.get("owner_pvz") or row.get("target_pvz"),
            claimer_pvz=claimer_pvz,
            ttl_minutes=ttl_minutes,
            source_run_id=source_run_id,
            logger=logger,
            uploader=uploader,
        )
        if claim_result.get("claimed", False):
            claimed_rows.append(row)
    return claimed_rows


def run_claimed_failover_backfill(
    *,
    claimed_rows,
    parser_api,
    parser_logger=None,
    failover_logger=None,
    claimer_pvz=PVZ_ID,
    source_run_id="",
):
    """Выполняет recovery parse + upload для claimed rows."""
    from scheduler_runner.tasks.reports.storage.failover_state import mark_failover_state

    parser_logger = parser_logger or create_parser_logger()
    failover_logger = failover_logger or create_failover_state_logger()
    claimed_dates_by_pvz = {}
    owner_by_key = {}
    for row in claimed_rows or []:
        target_pvz = row["target_pvz"]
        execution_date = row["Дата"]
        claimed_dates_by_pvz.setdefault(target_pvz, []).append(execution_date)
        owner_by_key[(target_pvz, execution_date)] = row.get("owner_pvz") or target_pvz

    execution_results = {}
    uploaded_records_total = 0
    recovered_dates_total = 0
    failed_recovery_dates_total = 0
    for target_pvz, execution_dates in claimed_dates_by_pvz.items():
        unique_dates = sorted(set(execution_dates))
        jobs = build_jobs_for_pvz(pvz_id=target_pvz, execution_dates=unique_dates)
        batch_result = invoke_parser_for_pvz(
            parser_api=parser_api,
            jobs=jobs,
            logger=parser_logger,
        )
        coverage_result = detect_missing_report_dates(
            date_from=min(unique_dates),
            date_to=max(unique_dates),
            logger=create_uploader_logger(),
            max_missing_dates=len(unique_dates),
            pvz_id=target_pvz,
        )
        recoverable_dates = set(unique_dates)
        if coverage_result.get("success", False):
            recoverable_dates = set(coverage_result.get("missing_dates", [])) & set(unique_dates)
        filtered_batch_result = build_filtered_batch_result(batch_result, recoverable_dates)
        upload_result = (
            run_upload_batch_microservice(filtered_batch_result)
            if recoverable_dates
            else {"success": True, "uploaded_records": 0, "skipped_as_already_covered": True}
        )
        failed_by_date = extract_batch_failures(batch_result)
        uploaded_records_total += int(upload_result.get("uploaded_records", 0) or 0)
        recovered_dates_total += len(recoverable_dates)
        failed_recovery_dates_total += len(failed_by_date)

        for execution_date in unique_dates:
            common_kwargs = {
                "execution_date": execution_date,
                "target_pvz": target_pvz,
                "owner_pvz": owner_by_key[(target_pvz, execution_date)],
                "claimed_by": claimer_pvz,
                "source_run_id": source_run_id,
                "logger": failover_logger,
            }
            if execution_date in failed_by_date:
                mark_failover_state(
                    status=STATUS_FAILOVER_FAILED,
                    last_error=failed_by_date[execution_date],
                    **common_kwargs,
                )
            elif execution_date not in recoverable_dates:
                mark_failover_state(
                    status=STATUS_FAILOVER_SUCCESS,
                    last_error="skipped_upload_already_covered",
                    **common_kwargs,
                )
            else:
                mark_failover_state(
                    status=STATUS_FAILOVER_SUCCESS,
                    **common_kwargs,
                )

        execution_results[target_pvz] = {
            "execution_dates": unique_dates,
            "batch_result": batch_result,
            "filtered_batch_result": filtered_batch_result,
            "coverage_result": coverage_result,
            "recoverable_dates": sorted(recoverable_dates),
            "upload_result": upload_result,
        }

    return {
        "results_by_pvz": execution_results,
        "recovered_pvz_count": len(execution_results),
        "recovered_dates_count": recovered_dates_total,
        "failed_recovery_dates_count": failed_recovery_dates_total,
        "uploaded_records": uploaded_records_total,
    }


# ──────────────────────────────────────────────
# Main failover coordination pass
# ──────────────────────────────────────────────

def run_failover_coordination_pass(
    *,
    configured_pvz_id=PVZ_ID,
    parser_api,
    parser_logger=None,
    processor_logger=None,
    source_run_id="",
):
    """Главный entrypoint failover coordination.

    Поток:
    1. discover_available_pvz_scope
    2. collect_failover_scan_decisions
    3. Если should_scan → collect_claimable_failover_rows
    4. claim_failover_rows
    5. run_claimed_failover_backfill
    """
    processor_logger = processor_logger or configure_logger(user="reports_domain", task_name="Processor")
    failover_logger = create_failover_state_logger()
    discovery_scope = discover_available_pvz_scope(
        configured_pvz_id=configured_pvz_id,
        logger=processor_logger,
        parser_logger=parser_logger,
    )
    result = {
        "attempted": True,
        "discovery_result": discovery_scope.get("discovery_result", {}),
        "available_pvz": discovery_scope.get("available_pvz", []),
        "scan_policy_mode": get_selection_mode(),
        "scan_decisions": {},
        "candidate_scan": {
            "attempted": False,
            "success": None,
            "error": "",
        },
        "candidate_rows": [],
        "candidate_rows_count": 0,
        "candidate_policy_evaluation": {},
        "claimed_rows": [],
        "claimed_rows_count": 0,
        "results_by_pvz": {},
        "recovered_pvz_count": 0,
        "recovered_dates_count": 0,
        "failed_recovery_dates_count": 0,
        "uploaded_records": 0,
    }
    scan_decisions = collect_failover_scan_decisions(
        configured_pvz_id=configured_pvz_id,
        accessible_pvz_ids=discovery_scope.get("available_pvz", []),
    )
    result["scan_decisions"] = scan_decisions
    scan_decision = scan_decisions.get("active", {})
    if "dry_run_capability_ranked" in scan_decisions:
        processor_logger.info(
            f"Failover coordination dry-run: capability_ranked decision={scan_decisions['dry_run_capability_ranked']}"
        )
    if not scan_decision.get("should_scan", True):
        processor_logger.info(
            "Failover coordination: candidate scan skipped, "
            f"mode={result['scan_policy_mode']}, reason={scan_decision.get('reason', 'unknown')}"
        )
        return result

    result["candidate_scan"]["attempted"] = True
    try:
        with failover_state_connection(logger=failover_logger) as failover_uploader:
            candidate_evaluation = collect_claimable_failover_rows(
                accessible_pvz_ids=discovery_scope.get("available_pvz", []),
                configured_pvz_id=configured_pvz_id,
                max_claims=BACKFILL_CONFIG.get("failover_max_claims_per_run"),
                logger=failover_logger,
                uploader=failover_uploader,
                return_evaluation=True,
            )
            candidate_evaluation = normalize_claimable_failover_evaluation(candidate_evaluation)
            result["candidate_policy_evaluation"] = candidate_evaluation
            result["candidate_rows"] = candidate_evaluation.get("selected_rows", [])
            result["candidate_rows_count"] = len(result["candidate_rows"])
            result["candidate_scan"]["success"] = True
            processor_logger.info(
                "Failover coordination arbitration: "
                f"mode={candidate_evaluation.get('mode')}, "
                f"eligible={candidate_evaluation.get('eligible_count', 0)}, "
                f"selected={candidate_evaluation.get('selected_count', 0)}, "
                f"rejected={candidate_evaluation.get('rejected_count', 0)}, "
                f"rejected_reasons={candidate_evaluation.get('rejected_reasons', {})}"
            )
            if not result["candidate_rows"]:
                processor_logger.info("Failover coordination: claimable colleague rows not found")
                return result

            claimed_rows = claim_failover_rows(
                candidate_rows=result["candidate_rows"],
                claimer_pvz=configured_pvz_id,
                ttl_minutes=BACKFILL_CONFIG.get("failover_claim_ttl_minutes", 15),
                source_run_id=source_run_id,
                logger=failover_logger,
                uploader=failover_uploader,
            )
            result["claimed_rows"] = claimed_rows
            result["claimed_rows_count"] = len(claimed_rows)
    except Exception as exc:
        if not is_failover_candidate_scan_retryable_error(exc):
            raise
        result["candidate_scan"]["success"] = False
        result["candidate_scan"]["error"] = str(exc)
        processor_logger.warning(
            f"Failover coordination degraded: candidate scan failed, skip claim phase. error={exc}"
        )
        return result
    if not result["claimed_rows"]:
        processor_logger.info("Failover coordination: claimable rows were scanned but claim was not acquired")
        return result

    processor_logger.info(
        f"Failover coordination: claimed_rows={len(result['claimed_rows'])}, targets={sorted(set(row['target_pvz'] for row in result['claimed_rows']))}"
    )
    execution_result = run_claimed_failover_backfill(
        claimed_rows=result["claimed_rows"],
        parser_api=parser_api,
        parser_logger=parser_logger,
        failover_logger=failover_logger,
        claimer_pvz=configured_pvz_id,
        source_run_id=source_run_id,
    )
    result.update(execution_result)
    return result

