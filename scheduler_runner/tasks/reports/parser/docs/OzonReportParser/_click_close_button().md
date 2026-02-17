# Метод `_click_close_button()`

## Версия
**0.0.2**

## Описание
Метод `_click_close_button()` выполняет клик по кнопке закрытия оверлея (модального окна). Метод ожидает кликабельности элемента перед выполнением клика.

## Сигнатура
```python
def _click_close_button(self, selector: str) -> bool
```

## Параметры
- **selector** (`str`): XPath или CSS селектор для поиска кнопки закрытия

## Возвращаемое значение
- `True` — если клик успешно выполнен
- `False` — если не удалось найти кнопку или выполнить клик

## Алгоритм работы

1. **Ожидание кликабельности**: Использует `WebDriverWait` для ожидания состояния `element_to_be_clickable`
2. **Выполнение клика**: Вызывает `.click()` на найденном элементе
3. **Задержка**: Делает паузу 0.5 секунды для завершения анимации закрытия
4. **Возврат результата**:
   - `True` — клик выполнен успешно
   - `False` — ошибка при поиске или клике

## Логирование

Метод ведёт логирование:
- `TRACE` — вход в метод с селектором
- `DEBUG` — поиск кнопки, обнаружение кликабельного элемента
- `INFO` — успешный клик по кнопке
- `WARNING` — ошибка при клике

## Пример использования

```python
# Клик по кнопке закрытия оверлея
close_button_selector = "//button[contains(@class, 'ozi__window__closeIcon__-pkPv')]"
if self._click_close_button(close_button_selector):
    if self.logger:
        self.logger.info("Оверлей закрыт")
else:
    if self.logger:
        self.logger.warning("Не удалось закрыть оверлей")
```

## Зависимости
- `selenium.webdriver.support.ui.WebDriverWait`
- `selenium.webdriver.support.expected_conditions`
- `selenium.webdriver.common.by.By`

## Изменения в версии 0.0.2
- Метод добавлен для поддержки функциональности закрытия оверлеев
