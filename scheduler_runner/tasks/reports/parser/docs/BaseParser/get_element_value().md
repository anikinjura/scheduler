# get_element_value()

## Версия
**0.0.1**

## Описание

Метод `get_element_value()` используется для получения значения элемента на веб-странице. Метод поддерживает различные типы элементов и может применять регулярные выражения к извлеченному значению.

## Сигнатура

```python
def get_element_value(
    self,
    selector: str,
    element_type: str = 'input',
    attribute: Optional[str] = None,
    pattern: Optional[str] = None
) -> str
```

## Параметры

- **selector** (`str`): Селектор элемента (XPath, CSS)
- **element_type** (`str`, optional): Тип элемента ('input', 'textarea', 'select', 'div', 'button', 'span', 'label', 'p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'checkbox', 'radio'). По умолчанию 'input'.
- **attribute** (`Optional[str]`, optional): Имя атрибута для извлечения (если None, извлекается текст или значение). По умолчанию None.
- **pattern** (`Optional[str]`, optional): Регулярное выражение для извлечения части текста. По умолчанию None.

## Возвращаемое значение

- **str**: Значение элемента или пустая строка

## Примеры использования

### Извлечение текста из div-элемента

```python
value = parser.get_element_value("//div[@id='my-div']")
```

### Извлечение значения из input-элемента

```python
value = parser.get_element_value("//input[@name='username']", element_type='input')
```

### Извлечение значения атрибута

```python
href_value = parser.get_element_value("//a[@class='link']", attribute='href')
```

### Извлечение значения с применением регулярного выражения

```python
# Извлечение числа из строки "Количество: 123 шт."
value = parser.get_element_value("//span[@class='count']", pattern=r'\d+')
```

### Извлечение значения из чекбокса

```python
state = parser.get_element_value("//input[@type='checkbox']", element_type='checkbox')
```

## Особенности

- Метод автоматически определяет, как извлекать значение в зависимости от типа элемента
- Поддерживает извлечение значений из различных типов элементов (input, textarea, select, div, button, checkbox, radio и др.)
- Может применять регулярные выражения к извлеченному значению для получения нужной части текста
- В случае ошибки возвращает пустую строку
- Поддерживает задержку перед поиском элемента (настраивается через конфигурацию `ELEMENT_SEARCH_DELAY`)