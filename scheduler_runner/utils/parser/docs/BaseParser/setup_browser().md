# Метод `setup_browser()`

## Версия
**0.0.3**

## Описание
Метод `setup_browser()` настраивает и запускает Edge WebDriver с диагностикой startup-сбоев и аварийным обходом.

Ключевая логика:
- Подготовка окружения: закрытие Edge пользователя (сначала штатно, затем принудительно), определение `user_data_dir`
  и `profile_directory` по режиму профиля.
- В режиме `snapshot` — копирование профиля пользователя в отдельный каталог (см. ниже). Если копия не удалась
  (например, `Cookies` заблокированы запущенным Edge), метод возвращает `False`.
- Основной запуск (`phase=primary`) с ретраями.
- Если запрошен `headless=True` и обнаружена известная сигнатура startup-падения, выполняется аварийный обход:
  - переключение на `headless=False`;
  - повторный запуск (`phase=fallback`) с теми же ретраями.

## Режимы профиля Edge (`EDGE_PROFILE_MODE`)

| Режим | `user-data-dir` | Сессия Ozon | Примечание |
|---|---|---|---|
| `snapshot` (по умолчанию) | копия профиля пользователя, `%LOCALAPPDATA%/scheduler/EdgeParserSnapshot/User Data` | сессия пользователя | рабочий режим |
| `default` | профиль пользователя по умолчанию | сессия пользователя | Edge запрещает на нем remote debugging: `DevToolsActivePort file doesn't exist` |
| `dedicated` | отдельный профиль `ParserProfile` | нужен собственный вход | не подходит для Ozon с двухфакторной авторизацией |

Явно заданный `user_data_dir` / `EDGE_USER_DATA_DIR` имеет приоритет над режимом и отключает snapshot.

### Жизненный цикл snapshot

1. `_terminate_browser_processes()` — штатное закрытие Edge текущего пользователя (`taskkill` без `/f`, чтобы cookies
   сохранились на диск), затем принудительное.
2. `_create_edge_profile_snapshot()` — копирование `Local State` и профиля без каталогов из `EDGE_SNAPSHOT_EXCLUDE`
   (кэши) и lock-файлов. Cookies расшифровываются в копии, так как ключ в `Local State` привязан к пользователю Windows.
3. Работа парсера с копией.
4. `close_browser()` → `_sync_edge_snapshot_back()` — возврат хранилищ сессии (`EDGE_SNAPSHOT_SYNC_BACK`) в профиль
   пользователя. Нужен потому, что Ozon выдает новый токен при каждом использовании сессии и отзывает сессию целиком
   при повторном предъявлении старого. Пропускается, если Edge пользователя запущен или его cookies изменились
   за время работы парсера.
5. `_remove_edge_profile_snapshot()` — удаление копии (в ней рабочие cookies).

## Сигнатура
```python
def setup_browser(self, browser_config: Optional[Dict[str, Any]] = None) -> bool
```

## Параметры
- **browser_config** (`Optional[Dict[str, Any]]`): переопределение параметров браузера поверх `self.config['browser_config']`.

## Возвращаемое значение
- **bool**: `True`, если браузер успешно поднят (на primary или fallback фазе), иначе `False`.

## Поддерживаемые параметры конфигурации
- `EDGE_PROFILE_MODE` - режим профиля: `snapshot` | `default` | `dedicated`.
- `EDGE_SNAPSHOT_USER_DATA_DIR` - каталог копии профиля (пусто - `%LOCALAPPDATA%/scheduler/EdgeParserSnapshot/User Data`).
- `EDGE_SNAPSHOT_EXCLUDE` - папки профиля, не копируемые в snapshot.
- `EDGE_SNAPSHOT_SYNC_BACK` - хранилища сессии, возвращаемые в профиль после работы.
- `BROWSER_GRACEFUL_CLOSE_TIMEOUT` - секунды ожидания штатного закрытия Edge.
- `FORCE_TERMINATE_BROWSER_PROCESSES` - закрывать Edge пользователя перед стартом (нужно для консистентной копии).
- `user_data_dir` / `EDGE_USER_DATA_DIR` - явный путь к профилю Edge (отключает snapshot).
- `headless` / `HEADLESS` - режим headless.
- `window_size` - размер окна.

## Перехват ответов API
Если `DATA_SOURCE_MODE` не `dom`, после успешного старта драйвера (primary или fallback) вызывается
`_install_api_capture()`: перехватчик `fetch`/XHR регистрируется через CDP `Page.addScriptToEvaluateOnNewDocument`
до первой навигации и доступен как `self.api_capture`. Сбой установки не прерывает запуск браузера.

## Диагностические маркеры в логах
- `API_CAPTURE_INSTALLED` / `API_CAPTURE_INSTALL_FAILED` - установка перехватчика ответов API.
- `EDGE_SNAPSHOT` - профиль скопирован (время, пути) или ошибка копирования.
- `EDGE_SNAPSHOT_SYNC` / `EDGE_SNAPSHOT_SYNC_SKIPPED` / `EDGE_SNAPSHOT_SYNC_FAILED` - обратная запись сессии.
- `ENV_BROWSER_STARTUP_CONTEXT` - снимок окружения перед запуском.
- `BROWSER_START_ATTEMPT_CONTEXT` - контекст конкретной попытки старта.
- `BROWSER_STARTUP_CRASH_SIGNATURE` - совпадение с известной сигнатурой падения.
- `BROWSER_POST_FAILURE_STATE` - состояние после неуспешной попытки.
- `BROWSER_FALLBACK_TRIGGERED` - активирован аварийный обход (`headless=False`).
- `BROWSER_FALLBACK_SUCCESS` / `BROWSER_FALLBACK_FAILED` - результат fallback-фазы.

## Внутренние helper-методы
- `_start_edge_driver_with_retries(...)`
- `_install_api_capture()`
- `_build_edge_options(...)`
- `_is_snapshot_profile_mode(config)`
- `_get_snapshot_user_data_dir()`
- `_create_edge_profile_snapshot(snapshot_user_data_dir, profile_directory)`
- `_log_startup_environment(...)`
- `_log_attempt_runtime_context(...)`
- `_log_post_failed_attempt_state(...)`
- `_is_startup_crash_signature(...)`
- `_log_user_data_dir_diagnostics(...)`

## Пример
```python
success = parser.setup_browser()
if not success:
    raise RuntimeError("Не удалось запустить браузер")
```
