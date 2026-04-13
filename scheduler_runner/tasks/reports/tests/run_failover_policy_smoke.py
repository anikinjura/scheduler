#!/usr/bin/env python3
"""
E2E smoke для проверки failover policy evaluation и claim на реальных данных KPI_FAILOVER_STATE.

Использует ТОТ ЖЕ путь, что и production-код:
- upsert_failover_state_records   — batch-seed строк (как owner_state_sync)
- get_failover_state_rows_by_keys — batch read-back (как owner_state_sync)
- collect_claimable_failover_rows  — policy evaluation (как reports_processor)
- try_claim_failover               — atomic claim (как claim_failover_rows)

Никакой кастомной логики — только импорты из production-модулей.

Этот тест должен падать/проходить в зависимости от:
- доступности Google Sheets (KPI_FAILOVER_STATE)
- корректности policy evaluation logic
- (опционально) доступности claim backend (Sheets или Apps Script)

Если production-код меняет способ policy evaluation или claim,
этот тест должен упасть — это сигнализирует о необходимости обновления смоки.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta

from config.base_config import PVZ_ID

# Production-модули — те же пути что и reports_processor.py
from ..failover_orchestration import collect_claimable_failover_rows
from ..storage.failover_state import (
    build_failover_state_record,
    create_failover_state_logger,
    get_failover_state_rows_by_keys,
    try_claim_failover,
    upsert_failover_state_records,
)


def build_arg_parser():
    parser = argparse.ArgumentParser(
        description="Failover policy evaluation smoke через production-пути"
    )
    parser.add_argument(
        "--claimer_pvz",
        default=PVZ_ID,
        help="Current object PVZ id that simulates failover claimant",
    )
    parser.add_argument(
        "--accessible_pvz",
        action="append",
        default=None,
        help="Accessible PVZ scope for the synthetic claimant. Can be repeated.",
    )
    parser.add_argument(
        "--claim_backend",
        choices=["sheets", "skip"],
        default="skip",
        help="Claim backend: 'sheets' — claim via Sheets, 'skip' — только policy evaluation",
    )
    parser.add_argument("--pretty", action="store_true", help="Печатать результат в человекочитаемом виде")
    return parser


def build_synthetic_records(now: datetime):
    """Строит тестовые записи для проверки policy evaluation.

    Сценарии:
    - owner_failed (ЧЕБОКСАРЫ_143) — eligible для claim
    - claim_expired (ЧЕБОКСАРЫ_182) — eligible для claim
    - owner_failed изолированного PVZ (ЧЕБОКСАРЫ_340) — rejected (no helpers)
    - own_target (ЧЕБОКСАРЫ_144) — rejected (own target)
    - max_attempts (ЧЕБОКСАРЫ_143, attempt=3) — rejected
    - owner_success (ЧЕБОКСАРЫ_182) — rejected (terminal status)
    """
    max_attempts = 3
    stale_ts = now - timedelta(minutes=45)
    fresh_ts = now - timedelta(minutes=2)
    return [
        build_failover_state_record(
            execution_date="2099-12-21",
            target_object_name="ЧЕБОКСАРЫ_143",
            owner_object_name="ЧЕБОКСАРЫ_143",
            status="owner_failed",
            source_run_id="smoke-policy-seed",
            last_error="synthetic_owner_failed_primary_143",
            updated_at=stale_ts,
        ),
        build_failover_state_record(
            execution_date="2099-12-22",
            target_object_name="ЧЕБОКСАРЫ_182",
            owner_object_name="ЧЕБОКСАРЫ_182",
            status="claim_expired",
            source_run_id="smoke-policy-seed",
            last_error="synthetic_claim_expired_182",
            updated_at=stale_ts,
        ),
        build_failover_state_record(
            execution_date="2099-12-23",
            target_object_name="ЧЕБОКСАРЫ_340",
            owner_object_name="ЧЕБОКСАРЫ_340",
            status="owner_failed",
            source_run_id="smoke-policy-seed",
            last_error="synthetic_owner_failed_isolated_340",
            updated_at=stale_ts,
        ),
        build_failover_state_record(
            execution_date="2099-12-24",
            target_object_name="ЧЕБОКСАРЫ_144",
            owner_object_name="ЧЕБОКСАРЫ_144",
            status="owner_failed",
            source_run_id="smoke-policy-seed",
            last_error="synthetic_owner_failed_own_target",
            updated_at=stale_ts,
        ),
        build_failover_state_record(
            execution_date="2099-12-25",
            target_object_name="ЧЕБОКСАРЫ_143",
            owner_object_name="ЧЕБОКСАРЫ_143",
            status="owner_failed",
            attempt_no=max_attempts,
            source_run_id="smoke-policy-seed",
            last_error="synthetic_owner_failed_max_attempts",
            updated_at=stale_ts,
        ),
        build_failover_state_record(
            execution_date="2099-12-26",
            target_object_name="ЧЕБОКСАРЫ_182",
            owner_object_name="ЧЕБОКСАРЫ_182",
            status="owner_success",
            source_run_id="smoke-policy-seed",
            last_error="",
            updated_at=fresh_ts,
        ),
    ]


def seed_records_production_batch(logger, records):
    """Seed через production batch upsert — тот же путь что и owner_state_sync."""
    upsert_result = upsert_failover_state_records(records, logger=logger)

    # Batch read-back через production get_rows_by_keys — тот же путь что и owner_state_sync
    keys = [
        {"work_date": rec["work_date"], "target_object_name": rec["target_object_name"]}
        for rec in records
    ]
    rows_by_key = get_failover_state_rows_by_keys(keys=keys, logger=logger)

    seeded = []
    for rec in records:
        row_key = (rec["work_date"], rec["target_object_name"])
        state_after = rows_by_key.get(row_key)
        seeded.append({
            "execution_date": rec["work_date"],
            "target_object_name": rec["target_object_name"],
            "status": rec["status"],
            "seed_result": None,
            "state_after_seed": state_after,
        })

    return seeded, upsert_result


def main():
    parser = build_arg_parser()
    args = parser.parse_args()
    logger = create_failover_state_logger()

    accessible_pvz_ids = args.accessible_pvz or [
        "ЧЕБОКСАРЫ_143",
        "ЧЕБОКСАРЫ_182",
        "ЧЕБОКСАРЫ_144",
        "СОСНОВКА_10",
        "ЧЕБОКСАРЫ_340",
    ]

    now = datetime.now()
    synthetic_records = build_synthetic_records(now)

    result = {
        "success": True,
        "claimer_pvz": args.claimer_pvz,
        "claim_backend": args.claim_backend,
        "accessible_pvz_ids": accessible_pvz_ids,
    }

    # ── Seed: production batch upsert ──
    try:
        seeded_rows, batch_upsert_result = seed_records_production_batch(
            logger, synthetic_records
        )
        result["batch_upsert_diagnostics"] = batch_upsert_result.get("diagnostics", {})
    except Exception as exc:
        result["success"] = False
        result["seed_error"] = str(exc)
        _print_result(result, args.pretty)
        return 1

    result["seeded_rows"] = seeded_rows

    # ── Policy evaluation: production collect_claimable_failover_rows ──
    try:
        evaluation = collect_claimable_failover_rows(
            available_pvz_ids=accessible_pvz_ids,
            configured_pvz_id=args.claimer_pvz,
            max_claims=3,
            logger=logger,
            return_evaluation=True,
        )
    except Exception as exc:
        result["success"] = False
        result["policy_evaluation_error"] = str(exc)
        _print_result(result, args.pretty)
        return 1

    result["policy_evaluation"] = evaluation

    # ── Claim: production try_claim_failover ──
    candidate_rows = evaluation.get("selected_rows", [])
    result["candidate_rows"] = [
        {
            "execution_date": row.get("work_date", ""),
            "target_object_name": row.get("target_object_name", ""),
            "status": row.get("status", ""),
        }
        for row in candidate_rows
    ]

    claimed_results = []
    if args.claim_backend == "sheets" and candidate_rows:
        for row in candidate_rows:
            try:
                claim_result = try_claim_failover(
                    execution_date=row["work_date"],
                    target_object_name=row["target_object_name"],
                    owner_object_name=row.get("owner_object_name") or row["target_object_name"],
                    claimer_pvz=args.claimer_pvz,
                    ttl_minutes=15,
                    source_run_id="smoke-policy-run",
                    logger=logger,
                )
                claimed_results.append({
                    "execution_date": row["work_date"],
                    "target_object_name": row["target_object_name"],
                    "claim_result": claim_result,
                })
            except Exception as exc:
                claimed_results.append({
                    "execution_date": row["work_date"],
                    "target_object_name": row["target_object_name"],
                    "claim_error": str(exc),
                })

        result["claimed_results"] = claimed_results
        result["success"] = result["success"] and all(
            item.get("claim_result", {}).get("claimed", False)
            for item in claimed_results
            if "claim_error" not in item
        )
    else:
        result["claim_skipped"] = True

    _print_result(result, args.pretty)

    if not result["success"]:
        return 1
    if not candidate_rows:
        return 2
    return 0


def _print_result(result, pretty):
    if pretty:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    raise SystemExit(main())
