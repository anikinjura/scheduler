#!/usr/bin/env python3
"""
Manual e2e batch smoke entrypoint for parser runtime.

Runs the canonical parser invocation flow for exactly one PVZ and multiple execution dates
without involving reports orchestration, uploader, or notifications.
"""

import argparse
import json
import sys

from scheduler_runner.utils.parser import invoke_parser_for_pvz
from scheduler_runner.utils.parser.parser_invocation import create_parser_logger


def parse_args():
    parser = argparse.ArgumentParser(
        description="Manual single-PVZ batch smoke for scheduler_runner.utils.parser",
    )
    parser.add_argument(
        "--pvz",
        required=True,
        help="PVZ identifier to inject into parser runtime config",
    )
    parser.add_argument(
        "--execution_date",
        action="append",
        required=True,
        help="Execution date in YYYY-MM-DD format; pass multiple times for batch mode",
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


def main():
    args = parse_args()
    logger = create_parser_logger()
    execution_dates = [date for date in args.execution_date if date]

    try:
        result = invoke_parser_for_pvz(
            parser_api=args.parser_api,
            pvz_id=args.pvz,
            execution_dates=execution_dates,
            logger=logger,
        )
    except Exception as exc:
        error_payload = {
            "success": False,
            "pvz_id": args.pvz,
            "execution_dates": execution_dates,
            "parser_api": args.parser_api,
            "error": str(exc),
        }
        print(json.dumps(error_payload, ensure_ascii=False, indent=2 if args.pretty else None))
        return 1

    print(json.dumps(result, ensure_ascii=False, indent=2 if args.pretty else None))
    return 0 if result.get("success", False) else 1


if __name__ == "__main__":
    sys.exit(main())
