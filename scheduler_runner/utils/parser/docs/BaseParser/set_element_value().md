# Метод `set_element_value()`

## Версия
**0.0.1**

## Описание
Метод `set_element_value()` используется для установки значения элемента на веб-странице. Метод поддерживает различные типы элементов, включая input, textarea, select (выпадающие списки), checkbox и radio кнопки.

## Сигнатура
```python
def set_element_value(
    self,
    selector: str,
    value: str,
    element_type: str = 'input',
    option_selector: Optional[str] = None
) -> bool
```

## Параметры
- **selector** (`str`): Селектор элемента (XPath, CSS)
- **value** (`str`): Значение для установки
- **element_type** (`str`, optional): Тип элемента ('input', 'textarea', 'select', 'checkbox', 'radio', 'dropdown'). По умолчанию 'input'.
- **option_selector** (`Optional[str]`, optional): Селектор для поиска опции в выпадающем списке. Используется только для элементов типа 'dropdown'.

## Возвращаемое значение
- **bool**: True, если значение успешно установлено

## Примеры использования

### Установка значения в input-элемент
```python
success = parser.set_element_value("//input[@name='username']", "my_username")
```

### Установка значения в выпадающий список
```python
success = parser.set_element_value(
    "//select[@id='country']",
    "Russia",
    element_type='dropdown',
    option_selector="//option[@value='RU']"
)
```

### Установка состояния чекбокса
```python
success = parser.set_element_value(
    "//input[@type='checkbox']",
    "true",  # или "false" для снятия галочки
    element_type='checkbox'
)
```

## Особенности
- Метод автоматически определяет, как устанавливать значение в зависимости от типа элемента
- Для выпадающих списков ('dropdown') метод сначала кликает по элементу, затем ищет опцию по тексту и кликает по ней
- Для чекбоксов и радио-кнопок метод устанавливает состояние в зависимости от переданного значения
- В случае ошибки возвращает False
- Поддерживает задержки перед взаимодействием с элементами для обеспечения стабильности