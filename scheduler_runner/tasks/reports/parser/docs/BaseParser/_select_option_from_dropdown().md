# Метод `_select_option_from_dropdown()`

## Версия
**0.0.1**

## Описание
Метод `_select_option_from_dropdown()` используется для выбора опции в выпадающем списке. Метод поддерживает выбор по точному совпадению текста или частичному совпадению, а также может работать с уже найденным WebElement.

## Сигнатура
```python
def _select_option_from_dropdown(
    self,
    selector: str,
    option_text: str,
    option_selector: Optional[str] = None,
    element: Optional[WebElement] = None
) -> bool
```

## Параметры
- **selector** (`str`): Селектор выпадающего списка (XPath, CSS)
- **option_text** (`str`): Текст опции для выбора
- **option_selector** (`Optional[str]`, optional): Селектор для поиска опций внутри выпадающего списка. Если не указан, используется селектор по умолчанию.
- **element** (`Optional[WebElement]`, optional): Уже найденный WebElement выпадающего списка. Если передан, используется вместо поиска по селектору.

## Возвращаемое значение
- **bool**: True, если опция успешно выбрана

## Примеры использования

### Выбор опции по селектору
```python
success = parser._select_option_from_dropdown(
    "//select[@id='dropdown']",
    "Option 1"
)
```

### Выбор опции с использованием специфичного селектора для опций
```python
success = parser._select_option_from_dropdown(
    "//div[@class='custom-dropdown']",
    "Custom Option",
    option_selector="//div[@class='dropdown-option']"
)
```

### Выбор опции из уже найденного элемента
```python
dropdown_element = parser.driver.find_element(By.ID, "dropdown")
success = parser._select_option_from_dropdown(
    "",
    "Option 2",
    element=dropdown_element
)
```

## Особенности
- Метод сначала кликает по выпадающему списку для его открытия
- Затем ищет опцию по тексту, сначала пытаясь найти точное совпадение
- Если точное совпадение не найдено, ищет частичное совпадение
- Использует ActionChains для кликов по опциям
- Поддерживает задержки для обеспечения стабильности взаимодействия
- В случае ошибки возвращает False