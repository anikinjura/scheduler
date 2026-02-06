# Метод `_select_option_from_dropdown()`

## Версия
**0.0.1**

## Описание
Метод `_select_option_from_dropdown()` используется для выбора опции в выпадающем списке. Метод поддерживает два режима работы: через селекторы и через уже найденный WebElement. Также поддерживает выбор по точному совпадению текста или частичному совпадению, и может обрабатывать переходы между страницами после выбора опции.

## Сигнатура
```python
def _select_option_from_dropdown(
    self,
    dropdown_selector: str = None,
    option_selector: str = None,
    option_value: str = None,
    element = None,
    return_url: Optional[str] = None,
    expected_url_pattern: Optional[str] = None,
    exact_match: bool = False,
    text_attribute: Optional[str] = None,
    value_attribute: str = 'value'
) -> bool
```

## Параметры
- **dropdown_selector** (`str`, optional): XPath или CSS-селектор для выпадающего списка. Необходим, если не передан параметр `element`.
- **option_selector** (`str`, optional): XPath или CSS-селектор для элементов опций внутри выпадающего списка. Необходим в режиме работы через селекторы.
- **option_value** (`str`, optional): Целевое значение для выбора (текст или значение атрибута опции).
- **element** (`Optional[WebElement]`, optional): Уже найденный WebElement выпадающего списка. Если передан, используется вместо поиска по селектору.
- **return_url** (`Optional[str]`, optional): URL для возврата, если произошел переход на другую страницу после выбора опции.
- **expected_url_pattern** (`Optional[str]`, optional): Паттерн URL для проверки, остались ли на целевой странице после выбора опции.
- **exact_match** (`bool`, optional): Флаг, указывающий на необходимость точного соответствия при поиске опции. По умолчанию False.
- **text_attribute** (`Optional[str]`, optional): Атрибут для получения текста опции (например, 'textContent'). По умолчанию используется текст элемента.
- **value_attribute** (`str`, optional): Атрибут для получения значения опции (например, 'value' или 'data-value'). По умолчанию 'value'.

## Возвращаемое значение
- **bool**: True, если опция успешно выбрана

## Примеры использования

### Выбор опции по селекторам
```python
success = parser._select_option_from_dropdown(
    dropdown_selector="//select[@id='dropdown']",
    option_selector="//option",
    option_value="Option 1"
)
```

### Выбор опции с использованием уже найденного элемента
```python
dropdown_element = parser.driver.find_element(By.ID, "dropdown")
success = parser._select_option_from_dropdown(
    element=dropdown_element,
    option_value="Option 2"
)
```

### Выбор опции с точным соответствием
```python
success = parser._select_option_from_dropdown(
    dropdown_selector="//select[@name='category']",
    option_selector="//option",
    option_value="Electronics",
    exact_match=True
)
```

### Выбор опции с обработкой перехода на другую страницу
```python
success = parser._select_option_from_dropdown(
    dropdown_selector="//select[@id='page-selector']",
    option_selector="//option",
    option_value="Next Page",
    return_url="https://example.com/current-page",
    expected_url_pattern="different-page"
)
```

## Особенности
- Метод поддерживает два режима работы: через селекторы и через уже найденный WebElement
- В режиме через WebElement используется стандартный подход через selenium.webdriver.support.ui.Select
- В режиме через селекторы метод сначала кликает по выпадающему списку для его открытия
- Затем ищет опцию по тексту или значению атрибута, сначала пытаясь найти точное совпадение (если exact_match=True)
- Использует ActionChains для кликов по опциям
- Поддерживает задержки для обеспечения стабильности взаимодействия
- Может обрабатывать переходы между страницами после выбора опции
- В случае ошибки возвращает False