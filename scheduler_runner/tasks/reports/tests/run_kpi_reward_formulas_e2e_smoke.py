#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime

from gspread.utils import DateTimeOption, ValueRenderOption
from config.base_config import ENV_MODE, PVZ_ID
from scheduler_runner.tasks.reports.reports_processor import (
    create_uploader_logger,
    prepare_connection_params,
)
from scheduler_runner.utils.uploader import (
    test_connection as test_upload_connection,
    upload_batch_data,
)
from scheduler_runner.utils.uploader.implementations.google_sheets_uploader import GoogleSheetsUploader


def build_arg_parser():
    parser = argparse.ArgumentParser(
        description="E2E smoke для проверки автоподстановки KPI reward-формул в Google Sheets"
    )
    parser.add_argument("--execution_date", default="2099-12-30", help="Synthetic execution date in YYYY-MM-DD")
    parser.add_argument("--pvz", default=PVZ_ID, help="PVZ id for the smoke row")
    parser.add_argument("--issued", type=int, default=317, help="Значение для 'Количество выдач'")
    parser.add_argument("--direct", type=int, default=120, help="Значение для 'Прямой поток'")
    parser.add_argument("--return", dest="return_flow", type=int, default=105, help="Значение для 'Возвратный поток'")
    parser.add_argument("--read_attempts", type=int, default=5, help="Сколько раз перечитывать строку после upload")
    parser.add_argument("--read_delay_seconds", type=float, default=2.0, help="Пауза между перечитываниями")
    parser.add_argument(
        "--require_nonzero",
        action="store_true",
        help="Считать smoke failed, если хотя бы одно reward-значение пустое/ноль",
    )
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON result")
    return parser


def build_smoke_record(args) -> dict:
    execution_dt = datetime.strptime(args.execution_date, "%Y-%m-%d")
    return {
        "Дата": execution_dt.strftime("%d.%m.%Y"),
        "ПВЗ": args.pvz,
        "Количество выдач": args.issued,
        "Прямой поток": args.direct,
        "Возвратный поток": args.return_flow,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def read_row_by_unique_keys(connection_params: dict, execution_date: str, pvz: str, logger):
    uploader = GoogleSheetsUploader(config=connection_params, logger=logger)
    if not uploader.connect():
        return None, "Не удалось подключиться к Google Sheets для чтения строки"

    try:
        row = uploader.sheets_reporter.get_row_by_unique_keys(
            unique_key_values={
                "Дата": execution_date,
                "ПВЗ": pvz,
            },
            config=uploader.table_config,
            return_raw=True,
        )
        return row, None
    finally:
        uploader.disconnect()


def build_row_a1_range(row_number: int, headers: list[str]) -> str:
    last_column_letter = chr(ord("A") + len(headers) - 1)
    return f"A{row_number}:{last_column_letter}{row_number}"


def map_headers_to_values(headers: list[str], row_values: list) -> dict:
    normalized_values = list(row_values or [])
    if len(normalized_values) < len(headers):
        normalized_values.extend([""] * (len(headers) - len(normalized_values)))
    return {
        header: normalized_values[index] if index < len(normalized_values) else ""
        for index, header in enumerate(headers)
    }


def looks_like_datetime_format(value) -> bool:
    raw = str(value or "").strip()
    return bool(raw) and ":" in raw and "." in raw


def read_row_render_diagnostics(connection_params: dict, row_number: int, logger):
    uploader = GoogleSheetsUploader(config=connection_params, logger=logger)
    if not uploader.connect():
        return None, "Не удалось подключиться к Google Sheets для чтения diagnostics строки"

    try:
        headers = uploader.sheets_reporter.worksheet.row_values(1)
        row_range = build_row_a1_range(row_number, headers)
        worksheet = uploader.sheets_reporter.worksheet

        formatted = worksheet.get(
            row_range,
            value_render_option=ValueRenderOption.formatted,
            date_time_render_option=DateTimeOption.formatted_string,
        )
        unformatted = worksheet.get(
            row_range,
            value_render_option=ValueRenderOption.unformatted,
            date_time_render_option=DateTimeOption.serial_number,
        )
        formulas = worksheet.get(
            row_range,
            value_render_option=ValueRenderOption.formula,
            date_time_render_option=DateTimeOption.formatted_string,
        )

        formatted_row = map_headers_to_values(headers, formatted[0] if formatted else [])
        unformatted_row = map_headers_to_values(headers, unformatted[0] if unformatted else [])
        formula_row = map_headers_to_values(headers, formulas[0] if formulas else [])

        reward_headers = [
            "Сумма за Количество выдач",
            "Сумма за Прямой поток",
            "Сумма за Возвратный поток",
            "Итого вознаграждение",
        ]
        reward_diagnostics = {}
        for header in reward_headers:
            formatted_value = formatted_row.get(header, "")
            unformatted_value = unformatted_row.get(header, "")
            formula_value = formula_row.get(header, "")
            reward_diagnostics[header] = {
                "formatted": formatted_value,
                "unformatted": unformatted_value,
                "formula": formula_value,
                "format_warning": (
                    "formatted value looks like datetime while formula/unformatted value is present"
                    if looks_like_datetime_format(formatted_value) and str(formula_value).startswith("=")
                    else None
                ),
            }

        return {
            "row_range": row_range,
            "formatted_row": formatted_row,
            "unformatted_row": unformatted_row,
            "formula_row": formula_row,
            "reward_diagnostics": reward_diagnostics,
        }, None
    finally:
        uploader.disconnect()


def extract_reward_snapshot(row: dict | None) -> dict:
    row = row or {}
    return {
        "Сумма за Количество выдач": row.get("Сумма за Количество выдач", ""),
        "Сумма за Прямой поток": row.get("Сумма за Прямой поток", ""),
        "Сумма за Возвратный поток": row.get("Сумма за Возвратный поток", ""),
        "Итого вознаграждение": row.get("Итого вознаграждение", ""),
    }


def reward_fields_ready(snapshot: dict) -> bool:
    return all(str(value).strip() != "" for value in snapshot.values())


def reward_fields_nonzero(snapshot: dict) -> bool:
    for value in snapshot.values():
        raw = str(value).strip()
        if raw in {"", "0", "0.0"}:
            return False
    return True


def main():
    parser = build_arg_parser()
    args = parser.parse_args()
    logger = create_uploader_logger()

    connection_params = prepare_connection_params()
    smoke_record = build_smoke_record(args)
    connection_check = test_upload_connection(connection_params, logger=logger)

    result = {
        "success": False,
        "env_mode": ENV_MODE,
        "pvz_id": PVZ_ID,
        "execution_date": args.execution_date,
        "smoke_record": smoke_record,
        "connection_check": connection_check,
        "upload_result": None,
        "read_attempts_used": 0,
        "read_row": None,
        "reward_snapshot": {},
        "row_render_diagnostics": None,
        "require_nonzero": args.require_nonzero,
    }

    if not connection_check.get("success", False):
        result["error"] = "Не удалось подключиться к Google Sheets"
        return print_result_and_exit(result, args.pretty, exit_code=1)

    upload_result = upload_batch_data(
        data_list=[smoke_record],
        connection_params=connection_params,
        logger=logger,
        strategy="update_or_append",
    )
    result["upload_result"] = upload_result

    if not upload_result.get("success", False):
        result["error"] = upload_result.get("error") or "Upload failed"
        return print_result_and_exit(result, args.pretty, exit_code=1)

    last_row = None
    last_snapshot = {}
    last_error = None
    for attempt in range(1, max(args.read_attempts, 1) + 1):
        row, read_error = read_row_by_unique_keys(
            connection_params=connection_params,
            execution_date=smoke_record["Дата"],
            pvz=args.pvz,
            logger=logger,
        )
        last_row = row
        last_error = read_error
        last_snapshot = extract_reward_snapshot(row)
        result["read_attempts_used"] = attempt

        if row and reward_fields_ready(last_snapshot):
            break
        if attempt < max(args.read_attempts, 1):
            time.sleep(max(args.read_delay_seconds, 0))

    result["read_row"] = last_row
    result["reward_snapshot"] = last_snapshot

    if last_error:
        result["error"] = last_error
        return print_result_and_exit(result, args.pretty, exit_code=1)

    if not last_row:
        result["error"] = "Не удалось перечитать строку KPI после upload"
        return print_result_and_exit(result, args.pretty, exit_code=1)

    render_diagnostics, diagnostics_error = read_row_render_diagnostics(
        connection_params=connection_params,
        row_number=last_row["_row_number"],
        logger=logger,
    )
    result["row_render_diagnostics"] = render_diagnostics
    if diagnostics_error:
        result["render_diagnostics_error"] = diagnostics_error

    if not reward_fields_ready(last_snapshot):
        result["error"] = "Reward-колонки остались пустыми после upload"
        return print_result_and_exit(result, args.pretty, exit_code=1)

    if args.require_nonzero and not reward_fields_nonzero(last_snapshot):
        result["error"] = "Не все reward-значения оказались ненулевыми"
        return print_result_and_exit(result, args.pretty, exit_code=1)

    result["success"] = True
    return print_result_and_exit(result, args.pretty, exit_code=0)


def print_result_and_exit(result: dict, pretty: bool, exit_code: int):
    if pretty:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    else:
        print(json.dumps(result, ensure_ascii=False, default=str))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
