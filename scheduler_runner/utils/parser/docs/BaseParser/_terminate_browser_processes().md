# Метод `_terminate_browser_processes()`

## Версия
**0.0.2**

## Описание
Метод `_terminate_browser_processes()` вызывается в начале `setup_browser()` (и перед fallback-фазой). Он завершает
оставшиеся процессы `msedgedriver.exe` и, если включен `FORCE_TERMINATE_BROWSER_PROCESSES`, Edge текущего пользователя,
после чего удаляет lock-файлы runtime-профиля.

Edge пользователя закрывается, чтобы его профиль можно было консистентно скопировать в snapshot: пока Edge запущен,
файл `Network/Cookies` заблокирован. Закрытие выполняется в два этапа:
1. `_close_browser_gracefully()` — `taskkill /im msedge.exe` без `/f` и ожидание до `BROWSER_GRACEFUL_CLOSE_TIMEOUT`
   секунд. Штатно закрытый Edge сбрасывает cookies на диск; после принудительного завершения на диске может остаться
   уже использованный токен обновления Ozon, и сессия будет отозвана.
2. `taskkill /f /im msedge.exe` — для процессов, которые не закрылись (в том числе фоновых процессов Edge без окон).

Без прав администратора процессы других пользователей Windows не завершаются («Отказано в доступе») — это ожидаемо.

## Сигнатура
```python
def _terminate_browser_processes(self)
```

## Возвращаемое значение
- **None**: Метод не возвращает значения

## Используемые параметры конфигурации
- **BROWSER_EXECUTABLE** (`str`): Имя исполняемого файла браузера (по умолчанию 'msedge.exe')
- **BROWSER_DRIVER_EXECUTABLES** (`list[str]`): Процессы драйвера, завершаемые всегда (по умолчанию `['msedgedriver.exe']`)
- **FORCE_TERMINATE_BROWSER_PROCESSES** (`bool`): Закрывать ли Edge пользователя
- **BROWSER_GRACEFUL_CLOSE_TIMEOUT** (`int`): Ожидание штатного закрытия Edge, секунды (по умолчанию 10)
- **PROCESS_TERMINATION_SLEEP** (`int`): Время ожидания после принудительного завершения процессов (по умолчанию 2 секунды)

## Связанные helper-методы
- `_find_own_processes(process_name)` - процессы с указанным именем, принадлежащие текущему пользователю
- `_wait_for_own_processes_exit(process_name, timeout)` - ожидание завершения этих процессов
- `_close_browser_gracefully(browser_executable)` - штатное закрытие Edge
- `_cleanup_lock_files(user_data_dir, profile_directory)` - удаление lock-файлов

## Пример использования
```python
parser._terminate_browser_processes()
```
