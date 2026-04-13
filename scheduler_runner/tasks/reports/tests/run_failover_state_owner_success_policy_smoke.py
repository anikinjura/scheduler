#!/usr/bin/env python3
"""
Synthetic smoke для owner_success suppression policy через refactored модули.

Импортирует sync_owner_failover_state_from_batch_result из refactored_modules.owner_state_sync
вместо боевого reports_processor.py.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from ..storage.failover_state import (  # noqa: E402
    STATUS_OWNER_FAILED,
    STATUS_OWNER_SUCCESS,
    build_failover_state_record,
    create_failover_state_logger,
    get_failover_state,
    upsert_failover_state_records,
)
from ..owner_state_sync import sync_owner_failover_state_from_batch_result  # noqa: E402


def build_arg_parser():
    parser = argparse.ArgumentParser(description="Synthetic smoke for owner_success suppression policy (refactored)")
    parser.add_argument("--target_prefix", default="SMOKE_OWNER_POLICY", help="Prefix for synthetic owner/target ids")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON result")
    return parser


def make_batch_success(execution_date: str):
    return {
        "results_by_date": {
            execution_date: {"success": True, "data": {}},
        }
    }


def get_state(execution_date: str, target_object_name: str, logger):
    return get_failover_state(execution_date=execution_date, target_object_name=target_object_name, logger=logger)


def main():
    args = build_arg_parser().parse_args()
    logger = create_failover_state_logger()
    now = datetime.now()

    healthy_date = "2099-12-26"
    incident_date = "2099-12-27"
    duplicate_success_date = "2099-12-28"

    healthy_owner = f"{args.target_prefix}_HEALTHY"
    incident_owner = f"{args.target_prefix}_INCIDENT"
    duplicate_success_owner = f"{args.target_prefix}_SUCCESS"

    seed_records = [
        build_failover_state_record(
            execution_date=incident_date,
            target_object_name=incident_owner,
            owner_object_name=incident_owner,
            status=STATUS_OWNER_FAILED,
            source_run_id="smoke-owner-policy-seed-failed",
            last_error="synthetic_owner_failed",
            updated_at=now,
        ),
        build_failover_state_record(
            execution_date=duplicate_success_date,
            target_object_name=duplicate_success_owner,
            owner_object_name=duplicate_success_owner,
            status=STATUS_OWNER_SUCCESS,
            source_run_id="smoke-owner-policy-seed-success",
            last_error="",
            updated_at=now,
        ),
    ]

    seed_result = upsert_failover_state_records(seed_records, logger=logger)

    healthy_result = sync_owner_failover_state_from_batch_result(
        owner_object_name=healthy_owner,
        missing_dates=[healthy_date],
        batch_result=make_batch_success(healthy_date),
        upload_result={"success": True, "uploaded_records": 1},
        logger=logger,
        source_run_id="smoke-owner-policy-healthy",
    )
    incident_result = sync_owner_failover_state_from_batch_result(
        owner_object_name=incident_owner,
        missing_dates=[incident_date],
        batch_result=make_batch_success(incident_date),
        upload_result={"success": True, "uploaded_records": 1},
        logger=logger,
        source_run_id="smoke-owner-policy-incident",
    )
    duplicate_success_result = sync_owner_failover_state_from_batch_result(
        owner_object_name=duplicate_success_owner,
        missing_dates=[duplicate_success_date],
        batch_result=make_batch_success(duplicate_success_date),
        upload_result={"success": True, "uploaded_records": 1},
        logger=logger,
        source_run_id="smoke-owner-policy-duplicate-success",
    )

    healthy_state = get_state(healthy_date, healthy_owner, logger)
    incident_state = get_state(incident_date, incident_owner, logger)
    duplicate_success_state = get_state(duplicate_success_date, duplicate_success_owner, logger)

    success = (
        seed_result.get("success", False)
        and healthy_result.get("persisted_rows_count") == 0
        and healthy_state is None
        and incident_result.get("persisted_rows_count") == 1
        and incident_state is not None
        and incident_state.get("status") == STATUS_OWNER_SUCCESS
        and incident_state.get("source_run_id") == "smoke-owner-policy-incident"
        and duplicate_success_result.get("persisted_rows_count") == 0
        and duplicate_success_state is not None
        and duplicate_success_state.get("status") == STATUS_OWNER_SUCCESS
        and duplicate_success_state.get("source_run_id") == "smoke-owner-policy-seed-success"
    )

    result = {
        "success": success,
        "seed_result": seed_result,
        "healthy_new_success": {
            "owner_object_name": healthy_owner,
            "execution_date": healthy_date,
            "sync_result": healthy_result,
            "state_after": healthy_state,
        },
        "incident_related_success": {
            "owner_object_name": incident_owner,
            "execution_date": incident_date,
            "sync_result": incident_result,
            "state_after": incident_state,
        },
        "duplicate_success": {
            "owner_object_name": duplicate_success_owner,
            "execution_date": duplicate_success_date,
            "sync_result": duplicate_success_result,
            "state_after": duplicate_success_state,
        },
    }

    if args.pretty:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(result, ensure_ascii=False))

    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())

