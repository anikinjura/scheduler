"""
Тесты построения листа ДАШБОРД (health_dashboard.py) — без сети: формулы, подсчет линий, запросы оформления.
"""
from datetime import date

from scheduler_runner.tasks.system import health_dashboard as hd

TODAY = date(2026, 10, 18)
SERIAL = lambda d: (d - hd.SHEETS_EPOCH).days  # noqa: E731


def test_col_letter():
    assert [hd.col_letter(i) for i in (1, 26, 27, 36, 40, 200)] == ["A", "Z", "AA", "AJ", "AN", "GR"]


def test_count_series_only_recent_rows():
    health = [[SERIAL(date(2026, 10, 17)), "ЧЕБОКСАРЫ_144"], [SERIAL(date(2026, 10, 18)), "ЧЕБОКСАРЫ_144"],
              [SERIAL(date(2026, 10, 18)), "ЧЕБОКСАРЫ_182"], [SERIAL(date(2026, 8, 1)), "СОСНОВКА_10"], []]
    disks = [[SERIAL(date(2026, 10, 18)), "ЧЕБОКСАРЫ_144", "9VMKZ90X"],
             [SERIAL(date(2026, 10, 6)), "ЧЕБОКСАРЫ_144", "9VMGFAQH"],
             [SERIAL(date(2026, 10, 18)), "ЧЕБОКСАРЫ_143", "9VMKZ90X"]]
    assert hd.count_series(health, disks, TODAY) == {"pvz": 2, "disk": 3}


def test_formulas_use_ru_locale_syntax():
    summary = hd.latest_rows_formula("PVZ_HEALTH", "AJ", [2], [2, 1, 3])
    assert summary.startswith("=IFERROR(SORT(CHOOSECOLS(SORTN(SORT(FILTER(PVZ_HEALTH!A2:AJ; ")
    assert "9^9; 2; 2; TRUE); 2; 1; 3)" in summary and "," not in summary
    disks = hd.last_collection_formula("PVZ_DISK_HEALTH", "P", [2, 3, 7], sort_cols=[1, 3])
    assert "MAXIFS(PVZ_DISK_HEALTH!A2:A; PVZ_DISK_HEALTH!B2:B; p)" in disks and "; 1; TRUE; 3; TRUE)" in disks
    pivot = hd.CHARTS[3][2]
    assert "{PVZ_DISK_HEALTH!A2:A\\ RIGHT(" in pivot and "pivot Col2" in pivot


def test_values_and_requests_cover_all_charts():
    values = hd.dashboard_values()
    data_cells = [v["range"] for v in values if v["range"].startswith(f"'{hd.DATA_SHEET}'")]
    assert data_cells == [f"'{hd.DATA_SHEET}'!{hd.col_letter(i * hd.PIVOT_BLOCK_WIDTH + 1)}1"
                          for i in range(len(hd.CHARTS))]
    chart = hd.chart_request(1, 2, 3, "t", "LINE", 4)["addChart"]["chart"]
    assert len(chart["spec"]["basicChart"]["series"]) == 4
    domain = chart["spec"]["basicChart"]["domains"][0]["domain"]["sourceRange"]["sources"][0]
    assert domain["sheetId"] == 2 and domain["startColumnIndex"] == 3 * hd.PIVOT_BLOCK_WIDTH
    rules = hd.conditional_rules(1)
    assert all(r["addConditionalFormatRule"]["index"] == i for i, r in enumerate(rules))
    assert any('="critical"' in r["addConditionalFormatRule"]["rule"]["booleanRule"]["condition"]["values"][0]
               ["userEnteredValue"] for r in rules)
