# OzonReportParser API

`OzonReportParser` добавляет Ozon-specific поведение поверх `BaseReportParser`.

## Публичные методы

- `__init__(config, args=None, logger=None)`
- `get_current_pvz() -> str`
  - читает текущий выбранный PVZ из UI и кеширует последнее валидное значение.
- `set_pvz(target_pvz) -> bool`
  - переключает UI на нужный PVZ.
- `ensure_correct_pvz() -> bool`
  - гарантирует, что parser работает в ожидаемом PVZ context.
- `navigate_to_target() -> bool`
  - переходит на Ozon target page и проверяет корректность PVZ context.
- `extract_report_data() -> dict`
  - базовый Ozon-oriented placeholder extraction.
- `collect_available_pvz() -> list[str]`
  - раскрывает dropdown ПВЗ и собирает все доступные для текущей учетной записи объекты.

## PVZ discovery helpers

- `_remember_current_pvz(pvz_value) -> str`
- `_get_cached_pvz() -> str`
- `_get_pvz_selectors() -> dict`
- `_get_pvz_dropdown_candidates() -> list[str]`
- `_get_pvz_option_item_candidates() -> list[str]`
- `_get_pvz_option_label_candidates() -> list[str]`
- `_open_pvz_dropdown() -> bool`
- `_collect_pvz_dropdown_elements()`
- `_extract_pvz_option_label(option_element) -> str`

Эти методы образуют Ozon-specific слой выбора ПВЗ и reuse-ятся как report parser-ом, так и discovery parser-ом.

## Overlay handling

- `_check_and_close_overlay() -> bool`
- `_click_close_button_candidates(selectors) -> bool`
- `_is_overlay_present(selector, timeout=5) -> bool`
- `_is_backdrop_active() -> bool`
- `_click_close_button(selector) -> bool`

Текущая модель overlay handling:
- сначала проверяется dialog/backdrop state;
- затем пробуются `close_button_candidates` из config;
- если overlay не мешает, parser продолжает работу без принудительного dismiss.

## Timestamp helper

- `_get_current_timestamp() -> str`

Используется для consistent metadata в report/discovery output.

## Что важно

- `OzonReportParser` не должен содержать orchestration policy.
- Внешний consumer решает, когда вызывать parser, на каких PVZ и с каким retry/fallback поведением.
