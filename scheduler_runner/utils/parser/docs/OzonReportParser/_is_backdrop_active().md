# Метод `_is_backdrop_active()`

## Версия
**0.0.2**

## Описание
Метод `_is_backdrop_active()` реализует логику проверки наличия активного backdrop (полупрозрачного фона оверлея) на странице. Backdrop может оставаться активным даже после того, как оверлей скрыт, что блокирует клики по элементам страницы.

## Сигнатура
```python
def _is_backdrop_active(self) -> bool
```

## Возвращаемое значение
- `True` — если активный backdrop обнаружен
- `False` — если backdrop не активен или не найден

## Конфигурация
Метод использует параметры из конфигурации `overlay_config`:

```python
"overlay_config": {
    "backdrop_selectors": [  # Селекторы для проверки активного backdrop
        "//div[contains(@class, 'ozi__backdrop__backdrop__')]",
        "//div[contains(@class, 'ozi__backdrop')]",
        "//div[contains(@class, 'backdrop')]",
        "//div[contains(@class, 'modal-backdrop')]"
    ]
}
```

## Алгоритм работы

1. **Получение селекторов**: Извлекает `backdrop_selectors` из конфигурации `overlay_config`
2. **Проверка наличия селекторов**: Если селекторы не указаны, возвращает `False`
3. **Поочерёдная проверка**: Для каждого селектора из списка:
   - Использует `WebDriverWait` с коротким таймаутом (1 секунда) для поиска элементов
   - Проверяет, виден ли найденный элемент (`is_displayed()`)
   - Если элемент виден — возвращает `True`
4. **Если ни один селектор не сработал**: Возвращает `False`

## Производительность

**Критично важно:** Метод использует короткий таймаут (1 секунда на селектор) вместо `implicit_wait` (20 секунд).

**Почему это важно:**
- 4 селектора × 20 сек (implicit_wait) = **80 секунд на одну проверку**
- 4 селектора × 1 сек (WebDriverWait) = **4 секунды на одну проверку**
- Экономия: **~380 секунд за цикл парсинга** (при 5+ проверках)

## Логирование

Метод ведёт подробное логирование:
- `TRACE` — вход в метод
- `DEBUG` — количество селекторов для проверки, информация о найденном backdrop (class, style)
- `WARNING` — если селекторы backdrop не указаны в конфигурации
- `DEBUG` — "Backdrop не активен" если ни один селектор не сработал

## Пример использования

```python
# В методе _is_overlay_present()
if self._is_backdrop_active():
    if self.logger:
        self.logger.info("Backdrop активен, ожидаем завершения анимации")
    return True

# В методе _check_and_close_overlay()
# После попытки закрытия оверлея проверяем backdrop
if self._is_backdrop_active():
    if self.logger:
        self.logger.warning("Backdrop всё ещё активен после закрытия оверлея")
```

## Зависимости
- `overlay_config.backdrop_selectors` — список селекторов для проверки
- `WebDriverWait` — для ожидания элементов с таймаутом
- `EC.presence_of_all_elements_located` — для проверки наличия элементов

## Изменения в версии 0.0.2
- Добавлен короткий таймаут (1 сек) для проверки backdrop
- Использован `WebDriverWait` вместо `find_elements()` для независимости от `implicit_wait`
- Добавлена диагностика style и class для найденных элементов backdrop

## Пример диагностики

```
[10:32:30] TRACE Попали в метод OzonReportParser._is_backdrop_active
[10:32:30] DEBUG Проверка backdrop: 4 селекторов из конфигурации
[10:32:31] DEBUG Backdrop найден: selector='//div[contains(@class, 'ozi__backdrop__backdrop__')]...', class='ozi__backdrop__backdrop__xyz123...'
[10:32:31] DEBUG   style='position: fixed; z-index: 1000; ...'
```
