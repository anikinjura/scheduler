# Метод `_click_element()`

## Версия
**0.0.1**

## Описание
Метод `_click_element()` выполняет клик по элементу на веб-странице. Метод поддерживает ожидание кликабельности элемента и настраиваемый таймаут.

## Сигнатура
```python
def _click_element(
    self,
    selector: str,
    wait_for_clickable: bool = True,
    timeout: Optional[int] = None
) -> bool
```

## Параметры
- **selector** (`str`): Селектор элемента (XPath, CSS) для клика
- **wait_for_clickable** (`bool`, optional): Флаг ожидания кликабельности элемента. По умолчанию True
- **timeout** (`Optional[int]`, optional): Таймаут ожидания в секундах. Если None, используется таймаут из конфигурации

## Возвращаемое значение
- **bool**: True, если клик выполнен успешно

## Используемые параметры конфигурации
- **ELEMENT_CLICK_TIMEOUT** (`int`): Таймаут ожидания кликабельности элемента (по умолчанию 10 секунд)
- **DEFAULT_TIMEOUT** (`int`): Таймаут ожидания элемента (по умолчанию 60 секунд)

## Примеры использования

### Клик по элементу с ожиданием кликабельности
```python
success = parser._click_element("//button[@id='submit-btn']")
```

### Клик по элементу без ожидания кликабельности
```python
success = parser._click_element("//div[@class='menu-item']", wait_for_clickable=False)
```

### Клик с пользовательским таймаутом
```python
success = parser._click_element("//input[@type='submit']", timeout=15)
```

## Особенности
- Метод может ждать, пока элемент станет кликабельным
- Поддерживает настраиваемый таймаут ожидания
- Использует WebDriverWait и expected_conditions для ожидания элемента
- В случае ошибки возвращает False
- Является внутренним методом и используется другими методами класса