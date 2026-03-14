# Implementations

В `implementations/` лежат готовые concrete parser classes.

## MultiStepOzonParser

Файл: `implementations/multi_step_ozon_parser.py`

Назначение:
- основной parser KPI/report summary.

Методы:
- `__init__(config, args=None, logger=None)`
- `get_report_type() -> str`
- `extract_report_data() -> dict`
- `login() -> bool`
- `logout() -> bool`
- `main(logger=None)`

Особенности:
- использует сохраненную browser session;
- поддерживает multi-step extraction contract;
- рассчитан на single-date, batch и job-based execution через facade.

## OzonAvailablePvzParser

Файл: `implementations/ozon_available_pvz_parser.py`

Назначение:
- discovery доступных ПВЗ для текущей Ozon account/session.

Методы:
- `get_report_type() -> str`
- `extract_report_data() -> dict`
- `login() -> bool`
- `logout() -> bool`
- `run_discovery(save_to_file=False, output_format='json') -> dict`

Output contract:
- `success`
- `mode`
- `configured_pvz_id`
- `current_pvz`
- `available_pvz_count`
- `available_pvz`
- `source_url`
- `extraction_timestamp`

Этот implementation не должен менять report summary contract основного parser-а.
