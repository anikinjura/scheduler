"""
Последние даты в листе KPI по каждому ПВЗ (только чтение Google Sheets).

Показывает, с какой даты перестали поступать данные, есть ли пропуски и дубликаты (ПВЗ + дата) — первый шаг,
чтобы понять, поломка локальная (один ПВЗ) или общая (все ПВЗ с одной даты).

Запуск из корня проекта:
    python scheduler_runner/utils/parser/docs/debug/kpi_last_dates.py [--days 30] [--show-from 2026-06-20]
"""
import argparse
import collections
import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))  # корень проекта

import gspread  # noqa: E402

from scheduler_runner.tasks.reports.config.reports_paths import REPORTS_PATHS  # noqa: E402
from scheduler_runner.tasks.reports.config.scripts.kpi_google_sheets_config import SPREADSHEET_ID  # noqa: E402

TEST_DATE_YEAR = 2099  # smoke-тесты пишут строки с датами 2099 года


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--days", type=int, default=30, help="окно поиска пропусков перед последней датой")
    parser.add_argument("--show-from", help="вывести значения KPI начиная с даты YYYY-MM-DD")
    args = parser.parse_args()

    client = gspread.service_account(filename=str(REPORTS_PATHS["GOOGLE_SHEETS_CREDENTIALS"]))
    rows = client.open_by_key(SPREADSHEET_ID).worksheet("KPI").get_all_values()
    header = rows[0]
    col = {name: header.index(name) for name in ("work_date", "object_name", "issued_packages", "direct_flow", "return_flow")}

    by_object = collections.defaultdict(dict)
    duplicates = collections.defaultdict(list)  # (ПВЗ, дата) -> номера строк, если ключ встречается больше одного раза
    for row_number, row in enumerate(rows[1:], start=2):
        try:
            work_date = dt.datetime.strptime(row[col["work_date"]].strip(), "%d.%m.%Y").date()
        except (ValueError, IndexError):
            continue
        if work_date.year < TEST_DATE_YEAR:
            obj = row[col["object_name"]]
            if work_date in by_object[obj]:
                duplicates[(obj, work_date)].append(row_number)
            by_object[obj][work_date] = row

    for obj, dates in sorted(by_object.items()):
        last = max(dates)
        missing = [last - dt.timedelta(days=i) for i in range(1, args.days + 1) if last - dt.timedelta(days=i) not in dates]
        dup_count = sum(1 for (o, _), extra in duplicates.items() if o == obj for _ in extra)
        print(f"{obj:18} строк={len(dates):4}  первая={min(dates)}  последняя={last}  пропусков за {args.days} дн. до нее={len(missing)}"
              f"  дубликатов={dup_count}")
        if args.show_from:
            since = dt.date.fromisoformat(args.show_from)
            for day in sorted(d for d in dates if d >= since):
                r = dates[day]
                print(f"    {day}  выдано={r[col['issued_packages']]:>5}  прямой={r[col['direct_flow']]:>5}  возврат={r[col['return_flow']]:>5}")

    if duplicates:
        print("\nДУБЛИКАТЫ (ПВЗ + дата встречаются несколько раз; номера лишних строк):")
        for (obj, day), extra in sorted(duplicates.items()):
            print(f"    {obj} {day}: строки {extra}")


if __name__ == "__main__":
    main()
