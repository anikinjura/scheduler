"""
health_dashboard.py

Лист «ДАШБОРД» в книге мониторинга (HEALTH_MONITOR_PLAN.md, раздел 6, вариант без Looker Studio): сводка по ПВЗ и
дискам с цветовой подсветкой и графики за 30 дней. Все значения — формулы поверх PVZ_HEALTH / PVZ_DISK_HEALTH, лист
обновляется сам после каждого сбора (13:10); данные для графиков — на скрытом листе «ДАШБОРД_ДАННЫЕ».

Запускается вручную с компьютера администратора и пересоздает оба листа (идемпотентно). Перезапускать, когда появится
новый ПВЗ или диск: число линий на графиках фиксируется при построении.

    python -m scheduler_runner.tasks.system.health_dashboard
    python -m scheduler_runner.tasks.system.health_dashboard --spreadsheet-id <ID>   # другая книга

Формулы — в синтаксисе русской локали книги (аргументы через «;», колонки массива через «\\»): API разбирает их так же,
как ввод в интерфейсе.

Author: anikinjura
"""
__version__ = '0.0.1'

import argparse
from datetime import date, timedelta
from typing import Any, Dict, List, Sequence

DASHBOARD_SHEET = "ДАШБОРД"
DATA_SHEET = "ДАШБОРД_ДАННЫЕ"
HEALTH_SHEET = "PVZ_HEALTH"
DISK_SHEET = "PVZ_DISK_HEALTH"
CHART_DAYS = 30
SHEETS_EPOCH = date(1899, 12, 30)

# Сводка: (заголовок, номер колонки PVZ_HEALTH с 1, формат)
SUMMARY_COLUMNS = [
    ("ПВЗ", 2, None), ("Дата сбора", 1, "dd.mm.yyyy"), ("Статус", 3, None), ("Тревоги", 4, None),
    ("C: свободно, ГБ", 5, "0.0"), ("C: свободно, %", 6, "0.0"), ("D: свободно, %", 8, "0.0"),
    ("Худший диск", 11, None), ("Аварийные перезагрузки", 12, "0"), ("WHEA", 14, "0"),
    ("Ошибки дисков", 15, "0"), ("Ошибки NTFS", 16, "0"), ("Версия кода", 19, None), ("Код актуален", 21, None),
    ("Последний KPI", 22, "dd.mm hh:mm"), ("Последнее копирование", 23, "dd.mm hh:mm"),
    ("Последнее обновление", 24, "dd.mm hh:mm"), ("Аптайм, ч", 33, "0.0"), ("Админ-права", 34, None),
]
DISK_COLUMNS = [
    ("ПВЗ", 2, None), ("Серийный номер", 3, None), ("Модель", 4, None), ("Тома", 7, None),
    ("Состояние", 8, None), ("Предсказание отказа", 9, None), ("Переназначенные (5)", 10, "0"),
    ("Ожидающие (197)", 11, "0"), ("Неисправимые (198)", 12, "0"), ("Ошибки чтения (187)", 13, "0"),
    ("CRC (199)", 14, "0"), ("Температура, °C", 15, "0"), ("Наработка, ч", 16, "0"),
]

TITLE_ROW, NOTE_ROW = 1, 2
SUMMARY_LABEL_ROW, SUMMARY_HEADER_ROW, SUMMARY_ROWS = 4, 5, 12        # до 12 ПВЗ
DISK_LABEL_ROW = SUMMARY_HEADER_ROW + SUMMARY_ROWS + 2                # 19
DISK_HEADER_ROW, DISK_ROWS = DISK_LABEL_ROW + 1, 30                   # до 30 дисков
CHARTS_LABEL_ROW = DISK_HEADER_ROW + DISK_ROWS + 2                    # 52
CHART_ROWS_STEP, CHART_WIDTH, CHART_HEIGHT = 20, 820, 360
COLUMN_WIDTH = 118
PIVOT_ROWS = CHART_DAYS + 5

COLOR_OK = {"red": 0.85, "green": 0.94, "blue": 0.83}
COLOR_WARNING = {"red": 1.0, "green": 0.95, "blue": 0.70}
COLOR_CRITICAL = {"red": 0.96, "green": 0.78, "blue": 0.76}
COLOR_HEADER = {"red": 0.90, "green": 0.92, "blue": 0.95}


def col_letter(index: int) -> str:
    """1 -> A, 27 -> AA."""
    letters = ""
    while index:
        index, rest = divmod(index - 1, 26)
        letters = chr(65 + rest) + letters
    return letters


def latest_rows_formula(sheet: str, last_col: str, key_cols: Sequence[int], columns: Sequence[int]) -> str:
    """Последняя по дате строка на каждый ключ (ПВЗ), выбранные колонки, по ПВЗ."""
    data = f"{sheet}!A2:{last_col}"
    condition = f'{sheet}!A2:A<>""'
    keys = "; ".join(f"{key}; TRUE" for key in key_cols)
    picked = "; ".join(str(c) for c in columns)
    return (f'=IFERROR(SORT(CHOOSECOLS(SORTN(SORT(FILTER({data}; {condition}); 1; FALSE); 9^9; 2; {keys}); '
            f'{picked}); 1; TRUE); "нет данных")')


def last_collection_formula(sheet: str, last_col: str, columns: Sequence[int], sort_cols: Sequence[int]) -> str:
    """Строки последнего сбора каждого ПВЗ (диски, снятые с ПВЗ или замененные, в таблицу не попадают)."""
    dates, pvz = f"{sheet}!A2:A", f"{sheet}!B2:B"
    latest = f"MAP({dates}; {pvz}; LAMBDA(d; p; d=MAXIFS({dates}; {pvz}; p)))"
    picked = "; ".join(str(c) for c in columns)
    order = "; ".join(f"{c}; TRUE" for c in sort_cols)
    return (f'=IFERROR(SORT(CHOOSECOLS(FILTER({sheet}!A2:{last_col}; {dates}<>""; {latest}); {picked}); {order}); '
            f'"нет данных")')


def pivot_formula(series_label: str, value: str, sheet: str) -> str:
    """Дата x серия (ПВЗ или диск) за CHART_DAYS дней: QUERY pivot для графика."""
    return (f'=IFERROR(QUERY(FILTER({{{sheet}!A2:A\\ {series_label}\\ {value}}}; {sheet}!A2:A>=TODAY()-{CHART_DAYS}); '
            f'"select Col1, max(Col3) where Col1 is not null group by Col1 pivot Col2 label Col1 \'Дата\'"; 0); '
            f'"нет данных")')


DISK_LABEL = f'RIGHT({DISK_SHEET}!B2:B; 3)&" · "&{DISK_SHEET}!D2:D&" "&{DISK_SHEET}!C2:C'
PVZ_LABEL = f"{HEALTH_SHEET}!B2:B"

# Графики: (название, тип, формула данных, серия — ПВЗ или диски)
CHARTS = [
    ("Свободно на C:, %", "LINE", pivot_formula(PVZ_LABEL, f"{HEALTH_SHEET}!F2:F", HEALTH_SHEET), "pvz"),
    ("Аварийные перезагрузки за сутки", "COLUMN",
     pivot_formula(PVZ_LABEL, f"{HEALTH_SHEET}!L2:L+{HEALTH_SHEET}!M2:M", HEALTH_SHEET), "pvz"),
    ("Ошибки дисков и NTFS за сутки", "COLUMN",
     pivot_formula(PVZ_LABEL, f"{HEALTH_SHEET}!O2:O+{HEALTH_SHEET}!P2:P", HEALTH_SHEET), "pvz"),
    ("Переназначенные сектора (SMART 5) по дискам", "LINE",
     pivot_formula(DISK_LABEL, f"{DISK_SHEET}!J2:J", DISK_SHEET), "disk"),
    ("Температура дисков, °C", "LINE", pivot_formula(DISK_LABEL, f"{DISK_SHEET}!O2:O", DISK_SHEET), "disk"),
]
PIVOT_BLOCK_WIDTH = 40  # колонок на блок данных графика на скрытом листе


def count_series(health_rows: List[List[Any]], disk_rows: List[List[Any]], today: date) -> Dict[str, int]:
    """Сколько линий будет на графиках: ПВЗ и дисков с данными за CHART_DAYS дней (значения UNFORMATTED)."""
    since = (today - timedelta(days=CHART_DAYS) - SHEETS_EPOCH).days

    def recent(rows, key):
        return {key(row) for row in rows if row and isinstance(row[0], (int, float)) and row[0] >= since}

    return {
        "pvz": len(recent(health_rows, lambda row: row[1])),
        "disk": len(recent(disk_rows, lambda row: (row[1], row[2]))),
    }


def _grid(sheet_id, row1, row2, col1, col2):
    """Диапазон: строки/колонки с 1, включительно."""
    return {"sheetId": sheet_id, "startRowIndex": row1 - 1, "endRowIndex": row2,
            "startColumnIndex": col1 - 1, "endColumnIndex": col2}


def _rule_formula(sheet_id, row1, row2, col1, col2, formula, color, index):
    return {"addConditionalFormatRule": {"index": index, "rule": {
        "ranges": [_grid(sheet_id, row1, row2, col1, col2)],
        "booleanRule": {"condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": formula}]},
                        "format": {"backgroundColor": color}}}}}


def _summary_col(title: str) -> int:
    return next(i for i, (name, _, _) in enumerate(SUMMARY_COLUMNS, start=1) if name == title)


def _disk_col(title: str) -> int:
    return next(i for i, (name, _, _) in enumerate(DISK_COLUMNS, start=1) if name == title)


def conditional_rules(sheet_id: int) -> List[Dict]:
    """Подсветка: статус, устаревшие данные, место, сбои, диски. Формулы — относительно первой строки данных."""
    r1, r2 = SUMMARY_HEADER_ROW + 1, SUMMARY_HEADER_ROW + SUMMARY_ROWS
    d1, d2 = DISK_HEADER_ROW + 1, DISK_HEADER_ROW + DISK_ROWS

    def cell(col_index, row):
        return f"${col_letter(col_index)}{row}"

    s = _summary_col
    status = cell(s("Статус"), r1)
    rules = [
        (r1, r2, s("Статус"), s("Статус"), f'={status}="ok"', COLOR_OK),
        (r1, r2, s("Статус"), s("Статус"), f'={status}="warning"', COLOR_WARNING),
        (r1, r2, s("Статус"), s("Статус"), f'={status}="critical"', COLOR_CRITICAL),
        (r1, r2, s("Дата сбора"), s("Дата сбора"),
         f'=AND(ISNUMBER({cell(s("Дата сбора"), r1)}); {cell(s("Дата сбора"), r1)}<TODAY()-1)', COLOR_CRITICAL),
        (r1, r2, s("C: свободно, %"), s("C: свободно, %"),
         f'=AND(ISNUMBER({cell(s("C: свободно, %"), r1)}); {cell(s("C: свободно, %"), r1)}<5)', COLOR_CRITICAL),
        (r1, r2, s("C: свободно, %"), s("C: свободно, %"),
         f'=AND(ISNUMBER({cell(s("C: свободно, %"), r1)}); {cell(s("C: свободно, %"), r1)}<15)', COLOR_WARNING),
        (r1, r2, s("Худший диск"), s("Худший диск"), f'=LEN({cell(s("Худший диск"), r1)})>0', COLOR_CRITICAL),
        (r1, r2, s("Код актуален"), s("Код актуален"), f'={cell(s("Код актуален"), r1)}=FALSE', COLOR_WARNING),
        (r1, r2, s("Админ-права"), s("Админ-права"), f'={cell(s("Админ-права"), r1)}=FALSE', COLOR_WARNING),
    ]
    for title in ("Аварийные перезагрузки", "WHEA", "Ошибки дисков", "Ошибки NTFS"):
        ref = cell(s(title), r1)
        rules.append((r1, r2, s(title), s(title), f"=AND(ISNUMBER({ref}); {ref}>0)", COLOR_CRITICAL))
    for title in ("Последний KPI", "Последнее копирование"):
        ref = cell(s(title), r1)
        rules.append((r1, r2, s(title), s(title), f'=OR(AND(ISNUMBER({ref}); {ref}<NOW()-1,5); '
                                                  f'AND(LEN({cell(s("ПВЗ"), r1)})>0; {ref}=""))', COLOR_CRITICAL))
    d = _disk_col
    rules += [
        (d1, d2, d("Состояние"), d("Состояние"),
         f'=AND(LEN({cell(d("Состояние"), d1)})>0; {cell(d("Состояние"), d1)}<>"Healthy")', COLOR_CRITICAL),
        (d1, d2, d("Предсказание отказа"), d("Предсказание отказа"),
         f'={cell(d("Предсказание отказа"), d1)}=TRUE', COLOR_CRITICAL),
        (d1, d2, d("Температура, °C"), d("Температура, °C"),
         f'=AND(ISNUMBER({cell(d("Температура, °C"), d1)}); {cell(d("Температура, °C"), d1)}>50)', COLOR_WARNING),
    ]
    for title in ("Переназначенные (5)", "Ожидающие (197)", "Неисправимые (198)", "Ошибки чтения (187)", "CRC (199)"):
        ref = cell(d(title), d1)
        rules.append((d1, d2, d(title), d(title), f"=AND(ISNUMBER({ref}); {ref}>0)", COLOR_WARNING))
    return [_rule_formula(sheet_id, *rule, index=i) for i, rule in enumerate(rules)]


def chart_request(dashboard_id: int, data_id: int, index: int, title: str, chart_type: str, series: int) -> Dict:
    col = index * PIVOT_BLOCK_WIDTH + 1
    row = CHARTS_LABEL_ROW + 1 + (index // 2) * CHART_ROWS_STEP
    anchor_col = 1 if index % 2 == 0 else 8
    return {"addChart": {"chart": {
        "spec": {
            "title": title,
            "basicChart": {
                "chartType": chart_type,
                "legendPosition": "RIGHT_LEGEND",
                "headerCount": 1,
                "axis": [{"position": "BOTTOM_AXIS", "title": "Дата"}, {"position": "LEFT_AXIS"}],
                "domains": [{"domain": {"sourceRange": {"sources": [
                    _grid(data_id, 1, PIVOT_ROWS, col, col)]}}}],
                "series": [{"series": {"sourceRange": {"sources": [
                    _grid(data_id, 1, PIVOT_ROWS, col + i, col + i)]}}, "targetAxis": "LEFT_AXIS"}
                    for i in range(1, series + 1)],
            },
        },
        "position": {"overlayPosition": {
            "anchorCell": {"sheetId": dashboard_id, "rowIndex": row - 1, "columnIndex": anchor_col - 1},
            "widthPixels": CHART_WIDTH, "heightPixels": CHART_HEIGHT}},
    }}}


def dashboard_values() -> List[Dict]:
    """Тексты и формулы листа ДАШБОРД (value ranges для values.batchUpdate)."""
    summary_last = col_letter(len(SUMMARY_COLUMNS))
    disk_last = col_letter(len(DISK_COLUMNS))
    return [
        {"range": f"'{DASHBOARD_SHEET}'!A{TITLE_ROW}", "values": [["Техническое состояние ПВЗ"]]},
        {"range": f"'{DASHBOARD_SHEET}'!A{NOTE_ROW}", "values": [[
            "Обновляется само после сбора (13:10): формулы поверх PVZ_HEALTH и PVZ_DISK_HEALTH. Цвет: зеленый — норма, "
            "желтый — внимание, красный — проблема (подробности — в столбце «Тревоги» и в ВК). Обновлено: "]]},
        {"range": f"'{DASHBOARD_SHEET}'!L{NOTE_ROW}", "values": [[f"=MAX({HEALTH_SHEET}!AJ2:AJ)"]]},
        {"range": f"'{DASHBOARD_SHEET}'!A{SUMMARY_LABEL_ROW}", "values": [["Сводка по ПВЗ — последний сбор"]]},
        {"range": f"'{DASHBOARD_SHEET}'!A{SUMMARY_HEADER_ROW}:{summary_last}{SUMMARY_HEADER_ROW}",
         "values": [[name for name, _, _ in SUMMARY_COLUMNS]]},
        {"range": f"'{DASHBOARD_SHEET}'!A{SUMMARY_HEADER_ROW + 1}",
         "values": [[latest_rows_formula(HEALTH_SHEET, "AJ", [2], [c for _, c, _ in SUMMARY_COLUMNS])]]},
        {"range": f"'{DASHBOARD_SHEET}'!A{DISK_LABEL_ROW}",
         "values": [["Диски — последний сбор каждого ПВЗ (замененные диски — только на графиках, с номером ПВЗ)"]]},
        {"range": f"'{DASHBOARD_SHEET}'!A{DISK_HEADER_ROW}:{disk_last}{DISK_HEADER_ROW}",
         "values": [[name for name, _, _ in DISK_COLUMNS]]},
        {"range": f"'{DASHBOARD_SHEET}'!A{DISK_HEADER_ROW + 1}",
         "values": [[last_collection_formula(DISK_SHEET, "P", [c for _, c, _ in DISK_COLUMNS], sort_cols=[1, 4])]]},
        {"range": f"'{DASHBOARD_SHEET}'!A{CHARTS_LABEL_ROW}", "values": [[f"Графики за {CHART_DAYS} дней"]]},
    ] + [
        {"range": f"'{DATA_SHEET}'!{col_letter(i * PIVOT_BLOCK_WIDTH + 1)}1", "values": [[formula]]}
        for i, (_, _, formula, _) in enumerate(CHARTS)
    ]


def format_requests(dashboard_id: int, data_id: int) -> List[Dict]:
    def number_format(row1, row2, col, pattern):
        number_type = "DATE_TIME" if "dd" in pattern else "NUMBER"
        return {"repeatCell": {"range": _grid(dashboard_id, row1, row2, col, col),
                               "cell": {"userEnteredFormat": {"numberFormat": {"type": number_type, "pattern": pattern}}},
                               "fields": "userEnteredFormat.numberFormat"}}

    def bold(row, col1, col2, size=None, background=None):
        fmt = {"textFormat": {"bold": True, **({"fontSize": size} if size else {})}}
        fields = "userEnteredFormat.textFormat"
        if background:
            fmt["backgroundColor"] = background
            fmt["wrapStrategy"] = "WRAP"
            fmt["verticalAlignment"] = "MIDDLE"
            fields += ",userEnteredFormat.backgroundColor,userEnteredFormat.wrapStrategy,userEnteredFormat.verticalAlignment"
        return {"repeatCell": {"range": _grid(dashboard_id, row, row, col1, col2),
                               "cell": {"userEnteredFormat": fmt}, "fields": fields}}

    requests = [
        {"updateDimensionProperties": {"range": {"sheetId": dashboard_id, "dimension": "COLUMNS",
                                                 "startIndex": 0, "endIndex": 26},
                                       "properties": {"pixelSize": COLUMN_WIDTH}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": dashboard_id, "dimension": "ROWS",
                                                 "startIndex": SUMMARY_HEADER_ROW - 1, "endIndex": SUMMARY_HEADER_ROW},
                                       "properties": {"pixelSize": 42}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": dashboard_id, "dimension": "ROWS",
                                                 "startIndex": DISK_HEADER_ROW - 1, "endIndex": DISK_HEADER_ROW},
                                       "properties": {"pixelSize": 42}, "fields": "pixelSize"}},
        bold(TITLE_ROW, 1, 1, size=16),
        bold(SUMMARY_LABEL_ROW, 1, 1, size=12),
        bold(DISK_LABEL_ROW, 1, 1, size=12),
        bold(CHARTS_LABEL_ROW, 1, 1, size=12),
        bold(SUMMARY_HEADER_ROW, 1, len(SUMMARY_COLUMNS), background=COLOR_HEADER),
        bold(DISK_HEADER_ROW, 1, len(DISK_COLUMNS), background=COLOR_HEADER),
        number_format(NOTE_ROW, NOTE_ROW, 12, "dd.mm.yyyy hh:mm"),
    ]
    for i, (_, _, pattern) in enumerate(SUMMARY_COLUMNS, start=1):
        if pattern:
            requests.append(number_format(SUMMARY_HEADER_ROW + 1, SUMMARY_HEADER_ROW + SUMMARY_ROWS, i, pattern))
    for i, (_, _, pattern) in enumerate(DISK_COLUMNS, start=1):
        if pattern:
            requests.append(number_format(DISK_HEADER_ROW + 1, DISK_HEADER_ROW + DISK_ROWS, i, pattern))
    # Скрытый лист: формат даты — только у колонки дат каждого блока, значения — автоматический формат
    for i in range(len(CHARTS)):
        first = i * PIVOT_BLOCK_WIDTH + 1
        requests.append({"repeatCell": {"range": _grid(data_id, 2, PIVOT_ROWS, first, first),
                                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE",
                                                                                        "pattern": "dd.mm"}}},
                                        "fields": "userEnteredFormat.numberFormat"}})
    return requests


def build(spreadsheet, today: date = None) -> Dict[str, int]:
    """Пересоздает ДАШБОРД и ДАШБОРД_ДАННЫЕ. Возвращает число линий на графиках."""
    today = today or date.today()
    health_rows = spreadsheet.worksheet(HEALTH_SHEET).get("A2:B", value_render_option="UNFORMATTED_VALUE")
    disk_rows = spreadsheet.worksheet(DISK_SHEET).get("A2:C", value_render_option="UNFORMATTED_VALUE")
    series = count_series(health_rows, disk_rows, today)

    existing = {ws.title: ws for ws in spreadsheet.worksheets()}
    for title in (DASHBOARD_SHEET, DATA_SHEET):
        if title in existing:
            spreadsheet.del_worksheet(existing[title])
    dashboard = spreadsheet.add_worksheet(DASHBOARD_SHEET, rows=CHARTS_LABEL_ROW + 3 * CHART_ROWS_STEP + 5, cols=26)
    data = spreadsheet.add_worksheet(DATA_SHEET, rows=PIVOT_ROWS + 5, cols=len(CHARTS) * PIVOT_BLOCK_WIDTH)

    spreadsheet.values_batch_update({"valueInputOption": "USER_ENTERED", "data": dashboard_values()})
    requests = format_requests(dashboard.id, data.id) + conditional_rules(dashboard.id)
    requests += [chart_request(dashboard.id, data.id, i, title, chart_type, series[kind])
                 for i, (title, chart_type, _, kind) in enumerate(CHARTS)]
    requests.append({"updateSheetProperties": {"properties": {"sheetId": data.id, "hidden": True}, "fields": "hidden"}})
    requests.append({"updateSheetProperties": {"properties": {"sheetId": dashboard.id,
                                                              "gridProperties": {"hideGridlines": True}},
                                               "fields": "gridProperties.hideGridlines"}})
    spreadsheet.batch_update({"requests": requests})
    return series


def main() -> None:
    import gspread
    from scheduler_runner.tasks.system.config.health_sheets_config import HEALTH_SHEETS_CONFIG

    parser = argparse.ArgumentParser(description="Построить лист ДАШБОРД в книге мониторинга")
    parser.add_argument("--spreadsheet-id", default=HEALTH_SHEETS_CONFIG["SPREADSHEET_ID"])
    args = parser.parse_args()

    client = gspread.service_account(filename=HEALTH_SHEETS_CONFIG["CREDENTIALS_PATH"])
    spreadsheet = client.open_by_key(args.spreadsheet_id)
    series = build(spreadsheet)
    print(f"Лист {DASHBOARD_SHEET} построен: ПВЗ на графиках — {series['pvz']}, дисков — {series['disk']}")


if __name__ == "__main__":
    main()
