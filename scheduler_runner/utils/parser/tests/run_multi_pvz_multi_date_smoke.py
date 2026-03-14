#!/usr/bin/env python3
"""
Manual e2e grouped smoke entrypoint for parser runtime.

Runs the canonical parser invocation flow for multiple PVZ and multiple execution dates
without involving reports orchestration, uploader, or notifications.
"""

import argparse
import json
import sys

from scheduler_runner.utils.parser import build_jobs_for_pvz, invoke_parser_for_grouped_jobs
from scheduler_runner.utils.parser.parser_invocation import create_parser_logger


def parse_args():
    parser = argparse.ArgumentParser(
        description="Manual multi-PVZ multi-date smoke for scheduler_runner.utils.parser",
    )
    parser.add_argument(
        "--pvz",
        action="append",
        default=None,
        help="PVZ identifier; pass multiple times for grouped mode",
    )
    parser.add_argument(
        "--execution_date",
        action="append",
        default=None,
        help="Execution date in YYYY-MM-DD format; pass multiple times for grouped mode",
    )
    parser.add_argument(
        "--pvz_dates",
        action="append",
        default=None,
        help="Per-PVZ mapping in format 'PVZ=YYYY-MM-DD,YYYY-MM-DD'; pass multiple times",
    )
    parser.add_argument(
        "--parser_api",
        choices=["legacy", "new"],
        default="legacy",
        help="Parser invocation mode. Default: legacy",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output",
    )
    return parser.parse_args()


def parse_pvz_dates_arg(raw_value):
    normalized_value = str(raw_value or "").strip()
    if "=" not in normalized_value:
        raise ValueError(f"Invalid --pvz_dates value '{raw_value}'. Expected format: PVZ=YYYY-MM-DD,YYYY-MM-DD")

    pvz_id, raw_dates = normalized_value.split("=", 1)
    pvz_id = pvz_id.strip()
    execution_dates = [date.strip() for date in raw_dates.split(",") if date.strip()]
    if not pvz_id or not execution_dates:
        raise ValueError(f"Invalid --pvz_dates value '{raw_value}'. PVZ and dates are required")
    return pvz_id, execution_dates


def build_grouped_jobs(grouped_execution_dates):
    grouped_jobs = {}
    for pvz_id, execution_dates in grouped_execution_dates.items():
        grouped_jobs[pvz_id] = build_jobs_for_pvz(
            pvz_id=pvz_id,
            execution_dates=execution_dates,
        )
    return grouped_jobs


def main():
    args = parse_args()
    logger = create_parser_logger()

    grouped_execution_dates = {}
    if args.pvz_dates:
        for raw_mapping in args.pvz_dates:
            pvz_id, execution_dates = parse_pvz_dates_arg(raw_mapping)
            grouped_execution_dates[pvz_id] = execution_dates
    else:
        pvz_ids = [pvz_id for pvz_id in (args.pvz or []) if pvz_id]
        execution_dates = [date for date in (args.execution_date or []) if date]
        if not pvz_ids or not execution_dates:
            raise ValueError("Use either --pvz_dates mapping or a combination of --pvz and --execution_date")
        grouped_execution_dates = {pvz_id: execution_dates[:] for pvz_id in pvz_ids}

    pvz_ids = list(grouped_execution_dates.keys())
    all_execution_dates = sorted({date for dates in grouped_execution_dates.values() for date in dates})
    grouped_jobs = build_grouped_jobs(grouped_execution_dates=grouped_execution_dates)

    try:
        results_by_pvz = invoke_parser_for_grouped_jobs(
            grouped_jobs=grouped_jobs,
            pvz_ids=pvz_ids,
            parser_api=args.parser_api,
            logger=logger,
        )
    except Exception as exc:
        error_payload = {
            "success": False,
            "pvz_ids": pvz_ids,
            "execution_dates": grouped_execution_dates,
            "parser_api": args.parser_api,
            "error": str(exc),
        }
        print(json.dumps(error_payload, ensure_ascii=False, indent=2 if args.pretty else None))
        return 1

    successful_pvz = [pvz_id for pvz_id, result in results_by_pvz.items() if result.get("success", False)]
    failed_pvz = [pvz_id for pvz_id, result in results_by_pvz.items() if not result.get("success", False)]
    payload = {
        "success": not failed_pvz,
        "mode": "grouped_batch",
        "total_pvz": len(pvz_ids),
        "total_dates": len(all_execution_dates),
        "requested_execution_dates_by_pvz": grouped_execution_dates,
        "successful_pvz": successful_pvz,
        "failed_pvz": failed_pvz,
        "results_by_pvz": results_by_pvz,
    }

    print(json.dumps(payload, ensure_ascii=False, indent=2 if args.pretty else None))
    return 0 if payload["success"] else 1


if __name__ == "__main__":
    sys.exit(main())
