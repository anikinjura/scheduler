# OzonReportParser API

`OzonReportParser` добавляет Ozon-specific поведение поверх `BaseReportParser`.

## Публичные методы

- `__init__(config, args=None, logger=None)`
- `get_current_pvz() -> str`
  - читает текущий выбранный PVZ из UI и кеширует последнее валидное значение.
- `set_pvz(target_pvz) -> bool`
  - переключает UI на нужный PVZ.
- `ensure_correct_pvz() -> bool`
  - гарантирует, что parser работает в ожидаемом PVZ context. С `DATA_SOURCE_MODE` ≠ `dom` — по ответу API
    `stores/current` без проверки окон, пока ПВЗ правильный; окна и клик — только при переключении (этап 3).
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

## Селекторы Ozon UI

Селекторы задаются в `configs/base_configs/ozon_report_config.py` (ПВЗ, таблицы, оверлей) и
`configs/implementations/multi_step_ozon_config.py` (счетчик «Найдено: N» на шагах).

Классы Ozon UI имеют вид `ozi__<компонент>__<элемент>__<hash>`. Хэш меняется при каждой пересборке фронтенда
(так в версии 3.11.x сломались счетчик `caption-medium__v6V9R` → `__SCm2O` и список ПВЗ), поэтому селекторы
сопоставляют только префикс класса без хэша, при необходимости уточняя его текстом:

```text
//div[contains(@class, 'ozi__text-view__caption-medium__') and contains(normalize-space(.), 'Найдено')]
```

Как проверить селекторы на живой странице — в [DEBUG_GUIDE.md](../DEBUG_GUIDE.md).

## Что важно

- `OzonReportParser` не должен содержать orchestration policy.
- Внешний consumer решает, когда вызывать parser, на каких PVZ и с каким retry/fallback поведением.
