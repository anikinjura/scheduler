#!/usr/bin/env python3
"""
Smoke пакетной загрузки KPI (режим UPLOAD_MODE="batch") на ТЕСТОВОЙ таблице (SPREADSHEET_ID_TEST, лист KPI).

Синтетические строки: объекты SMOKE_BATCH_*, даты декабря 2099 года. Боевая таблица не используется.

Проверки:
  1. пакет из 7 новых строк: 4 чтения (подключение 3 + batch_get) и 2 записи, формулы с верными номерами строк;
  2. тот же пакет с другими значениями: 7 обновлений, 4 чтения и 1 запись, timestamp не изменился, дубликатов нет;
  3. два процесса одновременно (разные объекты): строки не перезаписали друг друга, формулы соответствуют строкам;
  4. удаление синтетических строк.

Запуск из корня проекта:
    python -m scheduler_runner.tasks.reports.tests.run_kpi_batch_upload_smoke
"""
import argparse
import json
import subprocess
import sys
from copy import deepcopy

import gspread

from scheduler_runner.tasks.reports.config.reports_paths import REPORTS_PATHS
from scheduler_runner.tasks.reports.config.scripts.kpi_google_sheets_config import KPI_GOOGLE_SHEETS_CONFIG, SPREADSHEET_ID_TEST
from scheduler_runner.utils.uploader import diff_google_sheets_request_stats, get_google_sheets_request_stats, upload_batch_data

SMOKE_PREFIX = "SMOKE_BATCH_"


def connection_params():
    return {
        "CREDENTIALS_PATH": str(REPORTS_PATHS["GOOGLE_SHEETS_CREDENTIALS"]),
        "SPREADSHEET_ID": SPREADSHEET_ID_TEST,
        "WORKSHEET_NAME": "KPI",
        "TABLE_CONFIG": deepcopy(KPI_GOOGLE_SHEETS_CONFIG["TABLE_CONFIG"]),
        "REQUIRED_CONNECTION_PARAMS": ["CREDENTIALS_PATH", "SPREADSHEET_ID", "WORKSHEET_NAME", "TABLE_CONFIG"],
    }


def build_records(object_name, issued_base, timestamp):
    return [
        {"work_date": f"{day:02d}.12.2099", "object_name": object_name, "issued_packages": issued_base + day,
         "direct_flow": day, "return_flow": 1, "timestamp": timestamp}
        for day in range(1, 8)
    ]


def upload(records):
    before = get_google_sheets_request_stats()
    result = upload_batch_data(data_list=records, connection_params=connection_params(), UPLOAD_MODE="batch")
    return result, diff_google_sheets_request_stats(before)


def smoke_rows(worksheet, render="FORMULA"):
    """
    Строки SMOKE_BATCH_*: {номер строки: значения}. render="FORMULA" — формулы (даты числами),
    render="FORMATTED_VALUE" — значения так, как их видит пользователь (для проверки timestamp).
    """
    values = worksheet.get_all_values(value_render_option=render)
    return {i + 1: row for i, row in enumerate(values) if i > 0 and len(row) > 2 and str(row[2]).startswith(SMOKE_PREFIX)}


def check(condition, message, failures):
    print(("  OK   " if condition else "  FAIL ") + message)
    if not condition:
        failures.append(message)


def run_child(object_name):
    """Режим дочернего процесса для проверки параллельной записи."""
    result, stats = upload(build_records(object_name, 1000, "2099-12-31 00:00:00"))
    print(json.dumps({"success": result.get("success"), "stats": stats}))


def main():
    cli = argparse.ArgumentParser()
    cli.add_argument("--child", help=argparse.SUPPRESS)
    cli.add_argument("--keep", action="store_true", help="не удалять синтетические строки")
    args = cli.parse_args()
    if args.child:
        return run_child(args.child)

    worksheet = gspread.service_account(filename=str(REPORTS_PATHS["GOOGLE_SHEETS_CREDENTIALS"])).open_by_key(SPREADSHEET_ID_TEST).worksheet("KPI")
    failures = []
    try:
        print("1. Пакет из 7 новых строк")
        result, stats = upload(build_records(f"{SMOKE_PREFIX}A", 100, "2099-12-01 10:00:00"))
        print(f"     stats={stats} diagnostics={result.get('diagnostics')}")
        check(result.get("success") and result.get("uploaded") == 7, "7 строк загружено", failures)
        check(stats["reads"] == 4 and stats["writes"] == 2, f"4 чтения и 2 записи (факт: {stats['reads']}/{stats['writes']})", failures)
        rows = smoke_rows(worksheet)
        check(len(rows) == 7, f"в листе 7 строк объекта (факт: {len(rows)})", failures)
        check(all(r[0] == f"=B{n}&C{n}" and r[9] == f"=SUM(G{n}:I{n})" for n, r in rows.items()), "формулы id и total_reward с номером своей строки", failures)
        first_timestamps = [r[10] for r in smoke_rows(worksheet, "FORMATTED_VALUE").values()]

        print("2. Повтор того же пакета с другими значениями")
        result, stats = upload(build_records(f"{SMOKE_PREFIX}A", 500, "2099-12-02 10:00:00"))
        print(f"     stats={stats} diagnostics={result.get('diagnostics')}")
        check(result.get("diagnostics", {}).get("updated") == 7, "7 обновлений", failures)
        check(stats["reads"] == 4 and stats["writes"] == 1, f"4 чтения и 1 запись (факт: {stats['reads']}/{stats['writes']})", failures)
        rows = smoke_rows(worksheet)
        check(len(rows) == 7, f"дубликатов нет: по-прежнему 7 строк (факт: {len(rows)})", failures)
        check(len({r[10] for r in smoke_rows(worksheet, "FORMATTED_VALUE").values()} - set(first_timestamps)) == 0,
              "timestamp сохранен (как после шага 1)", failures)
        check(all(str(r[3]).startswith("5") for r in rows.values()), "значения обновлены", failures)

        print("3. Два процесса одновременно")
        children = [subprocess.Popen([sys.executable, "-m", "scheduler_runner.tasks.reports.tests.run_kpi_batch_upload_smoke",
                                      "--child", f"{SMOKE_PREFIX}{name}"], stdout=subprocess.PIPE, text=True, encoding="utf-8")
                    for name in ("B", "C")]
        outputs = [child.communicate(timeout=600)[0] for child in children]
        for output in outputs:
            print(f"     {output.strip().splitlines()[-1] if output.strip() else '(нет вывода)'}")
        rows = smoke_rows(worksheet)
        by_object = {}
        for n, r in rows.items():
            by_object.setdefault(r[2], []).append(n)
        check(len(by_object.get(f"{SMOKE_PREFIX}B", [])) == 7 and len(by_object.get(f"{SMOKE_PREFIX}C", [])) == 7,
              "у каждого процесса ровно 7 строк", failures)
        check(len(rows) == 21, f"всего 21 строка (факт: {len(rows)})", failures)
        check(all(r[0] == f"=B{n}&C{n}" for n, r in rows.items()), "формулы всех строк указывают на свою строку", failures)
    finally:
        if not args.keep:
            rows = smoke_rows(worksheet)
            if rows:
                requests = [{"deleteDimension": {"range": {"sheetId": worksheet.id, "dimension": "ROWS",
                                                           "startIndex": n - 1, "endIndex": n}}}
                            for n in sorted(rows, reverse=True)]
                worksheet.spreadsheet.batch_update({"requests": requests})
            print(f"4. Удалено синтетических строк: {len(rows)}")

    print("\nИТОГ:", "OK" if not failures else f"ОШИБКИ: {failures}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
