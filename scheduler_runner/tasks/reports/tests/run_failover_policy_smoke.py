#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timedelta

from config.base_config import PVZ_ID
from scheduler_runner.tasks.reports.config.scripts.reports_processor_config import (
    BACKFILL_CONFIG,
    FAILOVER_POLICY_CONFIG,
)
from scheduler_runner.tasks.reports.failover_policy import can_attempt_failover_claim, filter_claimable_rows_by_policy
from scheduler_runner.tasks.reports.storage.failover_state import (
    STATUS_CLAIM_EXPIRED,
    STATUS_OWNER_FAILED,
    STATUS_OWNER_SUCCESS,
    build_failover_state_record,
    create_failover_state_logger,
    failover_state_connection,
    get_failover_state,
    try_claim_failover,
    upsert_failover_state,
)

def build_arg_parser():
    parser = argparse.ArgumentParser(description="Controlled synthetic smoke for policy-aware failover on KPI_FAILOVER_STATE")
    parser.add_argument("--claimer_pvz", default=PVZ_ID, help="Current object PVZ id that simulates failover claimant")
    parser.add_argument(
        "--claim_backend",
        choices=["apps_script", "sheets"],
        default=None,
        help="Override claim backend for synthetic smoke",
    )
    parser.add_argument(
        "--accessible_pvz",
        action="append",
        default=None,
        help="Accessible PVZ scope for the synthetic claimant. Can be repeated.",
    )
    parser.add_argument("--ttl_minutes", type=int, default=15, help="Claim TTL in minutes")
    parser.add_argument("--source_run_id", default="smoke-policy-run", help="Run id written into claimed rows")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON result")
    return parser


def build_synthetic_rows(now: datetime):
    max_attempts = int(FAILOVER_POLICY_CONFIG.get("max_attempts_per_date", 3) or 3)
    stale_ts = now - timedelta(minutes=45)
    fresh_ts = now - timedelta(minutes=2)
    return [
        build_failover_state_record(
            execution_date="2099-12-21",
            target_pvz="ЧЕБОКСАРЫ_143",
            owner_pvz="ЧЕБОКСАРЫ_143",
            status=STATUS_OWNER_FAILED,
            source_run_id="smoke-policy-seed",
            last_error="synthetic_owner_failed_primary_143",
            updated_at=stale_ts,
        ),
        build_failover_state_record(
            execution_date="2099-12-22",
            target_pvz="ЧЕБОКСАРЫ_182",
            owner_pvz="ЧЕБОКСАРЫ_182",
            status=STATUS_CLAIM_EXPIRED,
            source_run_id="smoke-policy-seed",
            last_error="synthetic_claim_expired_182",
            updated_at=stale_ts,
        ),
        build_failover_state_record(
            execution_date="2099-12-23",
            target_pvz="ЧЕБОКСАРЫ_340",
            owner_pvz="ЧЕБОКСАРЫ_340",
            status=STATUS_OWNER_FAILED,
            source_run_id="smoke-policy-seed",
            last_error="synthetic_owner_failed_isolated_340",
            updated_at=stale_ts,
        ),
        build_failover_state_record(
            execution_date="2099-12-24",
            target_pvz="ЧЕБОКСАРЫ_144",
            owner_pvz="ЧЕБОКСАРЫ_144",
            status=STATUS_OWNER_FAILED,
            source_run_id="smoke-policy-seed",
            last_error="synthetic_owner_failed_own_target",
            updated_at=stale_ts,
        ),
        build_failover_state_record(
            execution_date="2099-12-25",
            target_pvz="ЧЕБОКСАРЫ_143",
            owner_pvz="ЧЕБОКСАРЫ_143",
            status=STATUS_OWNER_FAILED,
            attempt_no=max_attempts,
            source_run_id="smoke-policy-seed",
            last_error="synthetic_owner_failed_max_attempts",
            updated_at=stale_ts,
        ),
        build_failover_state_record(
            execution_date="2099-12-26",
            target_pvz="ЧЕБОКСАРЫ_182",
            owner_pvz="ЧЕБОКСАРЫ_182",
            status=STATUS_OWNER_SUCCESS,
            source_run_id="smoke-policy-seed",
            last_error="",
            updated_at=fresh_ts,
        ),
    ]


def retry_operation(fn, *args, attempts=3, delay_seconds=2, **kwargs):
    last_exc = None
    for attempt_index in range(attempts):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:  # pragma: no cover - smoke resiliency path
            last_exc = exc
            if attempt_index >= attempts - 1:
                raise
            time.sleep(delay_seconds)
    raise last_exc


def normalize_execution_date(value: str) -> str:
    raw_value = str(value or "").strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(raw_value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return raw_value


def get_state_with_uploader(uploader, execution_date: str, target_pvz: str):
    return uploader.sheets_reporter.get_row_by_unique_keys(
        unique_key_values={
            "Дата": normalize_execution_date(execution_date),
            "target_pvz": target_pvz,
        },
        config=uploader.table_config,
        return_raw=True,
    )


def upsert_with_uploader(uploader, record):
    return uploader._perform_upload(record, strategy="update_or_append")


def seed_rows_and_collect_decisions(*, synthetic_rows, claimer_pvz, accessible_pvz_ids, now, logger):
    seeded_rows = []
    decisions = []
    seeded_state_rows = []
    with failover_state_connection(logger=logger) as uploader:
        for row in synthetic_rows:
            seed_result = upsert_with_uploader(uploader, row)
            state_after_seed = get_state_with_uploader(
                uploader,
                execution_date=row["Дата"],
                target_pvz=row["target_pvz"],
            )
            decision = can_attempt_failover_claim(
                state_row=state_after_seed or row,
                configured_pvz_id=claimer_pvz,
                available_pvz=accessible_pvz_ids,
                now=now,
            )
            seeded_rows.append(
                {
                    "execution_date": row["Дата"],
                    "target_pvz": row["target_pvz"],
                    "status": row["status"],
                    "seed_result": seed_result,
                    "state_after_seed": state_after_seed,
                }
            )
            if state_after_seed:
                seeded_state_rows.append(state_after_seed)
            decisions.append(
                {
                    "execution_date": row["Дата"],
                    "target_pvz": row["target_pvz"],
                    "status": (state_after_seed or row).get("status"),
                    "decision": decision,
                }
            )
    return seeded_rows, decisions, seeded_state_rows


def main():
    parser = build_arg_parser()
    args = parser.parse_args()
    logger = create_failover_state_logger()

    if args.claim_backend:
        BACKFILL_CONFIG["failover_claim_backend"] = args.claim_backend

    accessible_pvz_ids = args.accessible_pvz or [
        "ЧЕБОКСАРЫ_143",
        "ЧЕБОКСАРЫ_182",
        "ЧЕБОКСАРЫ_144",
        "СОСНОВКА_10",
        "ЧЕБОКСАРЫ_340",
    ]
    now = datetime.now()
    synthetic_rows = build_synthetic_rows(now)

    seeded_rows, decisions, seeded_state_rows = retry_operation(
        seed_rows_and_collect_decisions,
        synthetic_rows=synthetic_rows,
        claimer_pvz=args.claimer_pvz,
        accessible_pvz_ids=accessible_pvz_ids,
        now=now,
        logger=logger,
    )

    candidate_rows = filter_claimable_rows_by_policy(
        rows=seeded_state_rows,
        configured_pvz_id=args.claimer_pvz,
        available_pvz=accessible_pvz_ids,
        max_claims=int(FAILOVER_POLICY_CONFIG.get("max_claims_per_run", 3) or 3),
    )

    claimed_results = []
    for row in candidate_rows:
        claim_result = try_claim_failover(
            execution_date=normalize_execution_date(row["Дата"]),
            target_pvz=row["target_pvz"],
            owner_pvz=row.get("owner_pvz") or row["target_pvz"],
            claimer_pvz=args.claimer_pvz,
            ttl_minutes=args.ttl_minutes,
            source_run_id=args.source_run_id,
            logger=logger,
        )
        claimed_results.append(
            {
                "execution_date": row["Дата"],
                "target_pvz": row["target_pvz"],
                "claim_result": claim_result,
                "state_after_claim": None,
            }
        )

    if claimed_results:
        try:
            with failover_state_connection(logger=logger) as uploader:
                for claim_item in claimed_results:
                    claim_item["state_after_claim"] = get_state_with_uploader(
                        uploader,
                        execution_date=normalize_execution_date(claim_item["execution_date"]),
                        target_pvz=claim_item["target_pvz"],
                    )
        except Exception as exc:  # pragma: no cover - smoke diagnostic path
            for claim_item in claimed_results:
                claim_item["state_after_claim_error"] = str(exc)
                if claim_item["state_after_claim"] is None:
                    claim_item["state_after_claim"] = claim_item["claim_result"].get("state")

    result = {
        "success": bool(claimed_results) and all(item["claim_result"].get("success", False) and item["claim_result"].get("claimed", False) for item in claimed_results),
        "claimer_pvz": args.claimer_pvz,
        "claim_backend": BACKFILL_CONFIG.get("failover_claim_backend"),
        "accessible_pvz_ids": accessible_pvz_ids,
        "selection_mode": FAILOVER_POLICY_CONFIG.get("selection_mode"),
        "policy_priority_map": FAILOVER_POLICY_CONFIG.get("priority_map", {}),
        "policy_capability_map": FAILOVER_POLICY_CONFIG.get("capability_map", {}),
        "seeded_rows": seeded_rows,
        "decisions_before_claim": decisions,
        "candidate_rows": [
            {
                "execution_date": row["Дата"],
                "target_pvz": row["target_pvz"],
                "status": row.get("status"),
            }
            for row in candidate_rows
        ],
        "claimed_results": claimed_results,
    }

    if args.pretty:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(result, ensure_ascii=False))

    if claimed_results and result["success"]:
        return 0
    if not candidate_rows:
        return 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

