#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[4]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config.base_config import PVZ_ID
from ..storage.failover_state import (
    STATUS_OWNER_FAILED,
    STATUS_OWNER_SUCCESS,
    build_failover_state_record,
    create_failover_state_logger,
    get_failover_state,
    upsert_failover_state_records,
)


def build_arg_parser():
    parser = argparse.ArgumentParser(description="Synthetic smoke for bulk upsert into KPI_FAILOVER_STATE")
    parser.add_argument("--execution_date", default="2099-12-29", help="Safe synthetic date")
    parser.add_argument("--target_prefix", default="SMOKE_UPSERT", help="Prefix for synthetic target_object_name ids")
    parser.add_argument("--owner_object_name", default=PVZ_ID, help="Owner object_name to write into synthetic rows")
    parser.add_argument("--source_run_id", default="smoke-upsert-run", help="Base run id for smoke rows")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON result")
    return parser


def build_seed_records(args):
    return [
        build_failover_state_record(
            execution_date=args.execution_date,
            target_object_name=f"{args.target_prefix}_A",
            owner_object_name=args.owner_object_name,
            status=STATUS_OWNER_FAILED,
            source_run_id=f"{args.source_run_id}-seed-1",
            last_error="smoke_seed_owner_failed_a",
            updated_at=datetime.now(),
        ),
        build_failover_state_record(
            execution_date=args.execution_date,
            target_object_name=f"{args.target_prefix}_B",
            owner_object_name=args.owner_object_name,
            status=STATUS_OWNER_FAILED,
            source_run_id=f"{args.source_run_id}-seed-2",
            last_error="smoke_seed_owner_failed_b",
            updated_at=datetime.now(),
        ),
    ]


def build_update_records(args):
    return [
        build_failover_state_record(
            execution_date=args.execution_date,
            target_object_name=f"{args.target_prefix}_A",
            owner_object_name=args.owner_object_name,
            status=STATUS_OWNER_SUCCESS,
            source_run_id=f"{args.source_run_id}-update-1",
            last_error="",
            updated_at=datetime.now(),
        ),
        build_failover_state_record(
            execution_date=args.execution_date,
            target_object_name=f"{args.target_prefix}_B",
            owner_object_name=args.owner_object_name,
            status=STATUS_OWNER_SUCCESS,
            source_run_id=f"{args.source_run_id}-update-2",
            last_error="",
            updated_at=datetime.now(),
        ),
    ]


def load_states(args, logger):
    rows = []
    for suffix in ("A", "B"):
        target_object_name = f"{args.target_prefix}_{suffix}"
        rows.append(
            get_failover_state(
                execution_date=args.execution_date,
                target_object_name=target_object_name,
                logger=logger,
            )
        )
    return rows


def main():
    parser = build_arg_parser()
    args = parser.parse_args()
    logger = create_failover_state_logger()

    seed_records = build_seed_records(args)
    update_records = build_update_records(args)

    seed_result = upsert_failover_state_records(seed_records, logger=logger)
    states_after_seed = load_states(args, logger)

    update_result = upsert_failover_state_records(update_records, logger=logger)
    states_after_update = load_states(args, logger)

    success = bool(seed_result.get("success")) and bool(update_result.get("success")) and all(
        state
        and state.get("status") == STATUS_OWNER_SUCCESS
        and state.get("source_run_id", "").startswith(f"{args.source_run_id}-update")
        for state in states_after_update
    )

    result = {
        "success": success,
        "execution_date": args.execution_date,
        "owner_object_name": args.owner_object_name,
        "target_prefix": args.target_prefix,
        "seed_result": seed_result,
        "states_after_seed": states_after_seed,
        "update_result": update_result,
        "states_after_update": states_after_update,
    }

    if args.pretty:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(result, ensure_ascii=False))

    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())

