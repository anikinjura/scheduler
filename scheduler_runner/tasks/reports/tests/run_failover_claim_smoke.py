#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys

from config.base_config import PVZ_ID
from scheduler_runner.tasks.reports.config.scripts.reports_processor_config import BACKFILL_CONFIG
from scheduler_runner.tasks.reports.failover_state import (
    STATUS_OWNER_FAILED,
    create_failover_state_logger,
    get_failover_state,
    mark_failover_state,
    try_claim_failover,
)


def build_arg_parser():
    parser = argparse.ArgumentParser(description="Manual smoke for failover claim via KPI_FAILOVER_STATE")
    parser.add_argument("--execution_date", default="2099-12-31", help="Synthetic date for safe smoke row")
    parser.add_argument("--target_pvz", default="SMOKE_FAILOVER_TARGET", help="Synthetic target PVZ")
    parser.add_argument("--owner_pvz", default="SMOKE_FAILOVER_TARGET", help="Owner PVZ for synthetic row")
    parser.add_argument("--claimer_pvz", default=PVZ_ID, help="Claimer PVZ")
    parser.add_argument("--claim_backend", choices=["apps_script", "sheets"], default=None, help="Override claim backend")
    parser.add_argument("--ttl_minutes", type=int, default=15, help="Claim TTL in minutes")
    parser.add_argument("--source_run_id", default="smoke-claim-run", help="Run id written into claim row")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON result")
    return parser


def main():
    parser = build_arg_parser()
    args = parser.parse_args()
    logger = create_failover_state_logger()

    if args.claim_backend:
        BACKFILL_CONFIG["failover_claim_backend"] = args.claim_backend

    seed_result = mark_failover_state(
        execution_date=args.execution_date,
        target_pvz=args.target_pvz,
        owner_pvz=args.owner_pvz,
        status=STATUS_OWNER_FAILED,
        source_run_id="smoke-seed",
        last_error="smoke_seed_owner_failed",
        logger=logger,
    )
    state_before = get_failover_state(
        execution_date=args.execution_date,
        target_pvz=args.target_pvz,
        logger=logger,
    )
    claim_result = try_claim_failover(
        execution_date=args.execution_date,
        target_pvz=args.target_pvz,
        owner_pvz=args.owner_pvz,
        claimer_pvz=args.claimer_pvz,
        ttl_minutes=args.ttl_minutes,
        source_run_id=args.source_run_id,
        logger=logger,
    )
    state_after = get_failover_state(
        execution_date=args.execution_date,
        target_pvz=args.target_pvz,
        logger=logger,
    )

    result = {
        "success": bool(claim_result.get("success", False)),
        "claim_backend": BACKFILL_CONFIG.get("failover_claim_backend"),
        "execution_date": args.execution_date,
        "target_pvz": args.target_pvz,
        "owner_pvz": args.owner_pvz,
        "claimer_pvz": args.claimer_pvz,
        "seed_result": seed_result,
        "state_before": state_before,
        "claim_result": claim_result,
        "state_after": state_after,
    }

    if args.pretty:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(result, ensure_ascii=False))

    if not result["success"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
