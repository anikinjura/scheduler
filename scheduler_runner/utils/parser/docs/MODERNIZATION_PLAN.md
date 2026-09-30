# План модернизации парсера: получение данных из API Турбо ПВЗ

Статус: **этапы 1–2 выполнены (30.09.2026), этапы 3–6 — план**. Составлен 29.09.2026 по итогам инцидента 03.07–29.09.2026 и разведки API.

## 1. Зачем

Сейчас парсер читает числа с отрисованной страницы: текст «Найдено: N» в `div` с классом
`ozi__text-view__caption-medium__<hash>`, номера перевозок из таблицы, текущий ПВЗ из поля `input___v-0-0`.
Разметка и CSS-классы Ozon UI меняются при каждой пересборке фронтенда (в версии 3.11.x сменились хэши классов
и разметка списка ПВЗ — парсер перестал работать, см. [DEBUG_GUIDE.md](DEBUG_GUIDE.md)).

Турбо ПВЗ — SPA: все числа приходят на страницу JSON-ответами внутреннего API, и только потом отрисовываются.
Формат ответов меняется значительно реже вёрстки. Цель модернизации — брать данные из этих ответов,
оставив UI только для того, что без него не сделать (переключение ПВЗ), и сохранить текущий способ как
переключаемый режим.

## 2. Результаты разведки API (29.09.2026)

Разведка выполнена перехватом `fetch`/XHR в браузере парсера (snapshot-профиль) на страницах, которые открывает
текущий парсер, за 28.09.2026 для ЧЕБОКСАРЫ_144. Эталонные значения (DOM-парсер): выдано 508, прямой поток 89, возврат 24.

| Шаг парсера | Сейчас (DOM) | API: запрос → поле | Проверено |
|---|---|---|---|
| Выдано (`giveout`) | «Найдено: N» | `GET /api2/reports/give_out/logV2?startDate=…&endDate=…&operationTypes=GiveoutAll&take=50&skip=0` → `logV2.totalCount` | 508 ✅ |
| Перевозки за день (`direct_flow` / `return_flow`, таблица) | таблица `ozi__table__table__…`, колонка `_carriageNumber_` | `GET /api2/reports/CarriageReport/GetCarriages?searchString=&startSentMoment=…&endSentMoment=…&flowType=Direct\|Return&skip=0&take=20` → `carriages[].carriageId`, `totalCount` | 141445739 / 141443498 ✅ |
| Отправления в перевозке (вложенный шаг) | «Найдено: N» на странице перевозки | `GET /api2/reports/CarriageReport/GetCarriageArticles?carriageId=…&articleLabel=&articleState=Took&articleType=ArticlePosting&skip=0&take=20` → `totalCount` | 89 / 24 ✅ |
| Текущий ПВЗ | поле `input___v-0-0` | `GET /api2/stores/current` → `store.name` (`store.id`) | ✅ |
| Доступные ПВЗ | раскрыть список, прочитать подписи | `GET /api2/stores/list` → `stores[].name` (`id`) | 4 ПВЗ ✅ |
| Переключение ПВЗ | клик в списке | `POST /api2/stores/select-v2` `{storeId, userId, userName, authAuditParameters}` → `{token, refreshToken}` | ✅ |

Наблюдения:
- Параметры запросов совпадают с фильтрами, которые парсер уже подставляет в URL страниц (`filter={...}`).
- ПВЗ в запросах данных **не передается**: сервер хранит текущий ПВЗ в сессии (`stores/current`).
- Запросы идут с заголовками `authorization`, `x-o3-fp` (отпечаток устройства), `x-o3-app-name`, `x-o3-app-version`.
  Повторить их вне страницы без воспроизведения этих заголовков нельзя.
- `select-v2` выпускает **новую пару токенов**; ее сохраняет фронтенд. Если в сессии устаревший токен обновления,
  сервер отказывает и отзывает сессию (так проявлялся разрыв сессии при переключении ПВЗ).
- В `authAuditParameters` Ozon фиксирует user agent (`HeadlessChrome/…`): в аудите видно, что работал автоматизированный браузер.
- `stores/current` запрашивается при загрузке каждой страницы; `stores/list` — не на всех страницах
  (есть на странице перевозки и после переключения ПВЗ, нет на странице выдачи).
- `GetCarriages` отдает не более `take=20` перевозок, общее количество — в `totalCount`. DOM-парсер сейчас тоже
  обрабатывает только первую страницу таблицы.

## 3. Выбранный подход (вариант А): чтение ответов, которые получает страница

Браузер, snapshot-профиль и сессия остаются как есть. Парсер открывает те же URL, что и сейчас, но значение берет
не из DOM, а из JSON-ответа соответствующего запроса. Для этого до загрузки страницы в нее встраивается
перехватчик ответов (`Page.addScriptToEvaluateOnNewDocument` через CDP, как при разведке).

Почему не вызывать API самим (вариант Б): потребовалось бы воспроизводить `authorization`/`x-o3-fp` и самостоятельно
сохранять новые токены после `select-v2` — ошибка ведет к отзыву сессии. Вариант Б рассматривается только как
возможная оптимизация после стабилизации варианта А (см. раздел 8).

Переключение ПВЗ **остается через UI** (клик в списке): токены после `select-v2` сохраняет фронтенд.
API используется для проверки результата и для чтения списка ПВЗ.

## 4. Переключатель режимов в конфиге

Новый параметр в `configs/base_configs/ozon_report_config.py` (наследуется всеми конфигами Ozon):

```python
"DATA_SOURCE_MODE": "dom",  # dom | shadow | api_with_dom_fallback | api
```

| Режим | Откуда берется результат | Что еще происходит | Назначение |
|---|---|---|---|
| `dom` (по умолчанию) | DOM, как сейчас | перехватчик не устанавливается | текущее поведение без изменений |
| `shadow` | DOM | дополнительно считается значение из API, расхождения пишутся в лог (`API_SHADOW_MISMATCH`) | безопасная обкатка на проде |
| `api_with_dom_fallback` | API | если запрос/поле не найдены — предупреждение `API_EXTRACTION_FALLBACK` и значение из DOM | переходный период |
| `api` | API | при отсутствии запроса/поля — явная ошибка шага, **без значения по умолчанию** | целевой режим |

Параметр можно переопределить при вызове (аргумент `--data_source` в smoke-скриптах; при необходимости — через
`parser_invocation`, аналогично `apply_headless_override_to_parser_config`).

Отдельный параметр для ПВЗ не вводится: чтение текущего ПВЗ и списка ПВЗ следует тому же режиму.

## 5. Изменения по файлам

### 5.1 Конфиги

**`configs/base_configs/ozon_report_config.py`**
- `DATA_SOURCE_MODE` (см. выше).
- `api_capture`: `{"wait_timeout": 15, "poll_interval": 0.5, "max_records": 500}`.
- `api_endpoints` для ПВЗ:
  ```python
  "api_endpoints": {
      "current_store": {"path": "/api2/stores/current", "value_path": "store.name"},
      "store_list": {"path": "/api2/stores/list", "items_path": "stores", "name_field": "name"},
      "select_store": {"path": "/api2/stores/select-v2"},
  }
  ```

**`configs/implementations/multi_step_ozon_config.py`** — в каждый шаг добавляется блок `api_extraction`
рядом с существующими `data_extraction` / `table_processing` / `nested_processing` (они остаются для DOM-режима):

```python
"giveout": {
    ...,                                   # текущие параметры без изменений
    "api_extraction": {
        "type": "value",
        "request": {"path": "/api2/reports/give_out/logV2", "query_contains": {"operationTypes": "GiveoutAll"}},
        "value_path": "logV2.totalCount",
        "post_processing": {"convert_to": "int"},   # без default_value
    },
},
"direct_flow": {
    ...,
    "api_extraction": {
        "type": "list_nested",
        "list_request": {"path": "/api2/reports/CarriageReport/GetCarriages", "query_contains": {"flowType": "Direct"}},
        "items_path": "carriages",
        "id_field": "carriageId",
        "total_path": "totalCount",
        "nested_request": {"path": "/api2/reports/CarriageReport/GetCarriageArticles",
                           "query_contains": {"carriageId": "{identifier}", "articleState": "Took"}},
        "nested_value_path": "totalCount",
        "aggregation": {"method": "sum", "target_field": "total_carriages"},
    },
},
# return_flow — аналогично с flowType=Return
```

`query_contains` защищает от чтения чужого или устаревшего ответа: запрос засчитывается, только если его
query-параметры содержат указанные значения (в том числе дату выполнения и id перевозки).

### 5.2 Новый модуль `core/api_response_capture.py`

Класс `ApiResponseCapture` — универсальный перехват ответов страницы, без знания об Ozon:
- `install(driver)` — `Page.addScriptToEvaluateOnNewDocument` с JS-перехватчиком `fetch`/`XMLHttpRequest`;
  записи хранятся в `window.__apiCapture` текущего документа (полная навигация создает новый документ — буфер
  очищается естественным образом).
- `records(driver)` — прочитать записи (`url`, `method`, `status`, `body`, `t`).
- `wait_for(driver, path, query_contains=None, timeout=...)` — дождаться завершенного ответа на запрос с путем `path`
  и подходящими query-параметрами; вернуть разобранный JSON или выбросить `ApiResponseNotFound`.
- `resolve_path(data, "logV2.totalCount")` / `"carriages[].carriageId"` — извлечение значения по пути;
  `ApiFieldNotFound`, если поля нет.

Требования безопасности:
- заголовки запросов (в том числе `authorization`) **не сохраняются и не логируются**;
- тела ответов в лог не пишутся; в лог — только путь запроса, статус и извлеченное значение;
- размер хранимого тела ограничен, число записей — `max_records`.

### 5.3 `core/base_parser.py`

- `_install_api_capture()` — вызывается в `setup_browser()` после успешного старта драйвера, если
  `DATA_SOURCE_MODE != "dom"`. Логирует `API_CAPTURE_INSTALLED`.
- Больше изменений нет: `BaseParser` остается без знания о бизнес-смысле отчетов.

### 5.4 `core/base_report_parser.py`

- `_get_data_source_mode()` — чтение режима с валидацией.
- `_execute_single_step()` — после навигации выбор источника:
  - `dom` → текущие `_handle_simple_extraction` / `_handle_table_extraction` / `_handle_table_nested_extraction`;
  - `api` / `api_with_dom_fallback` / `shadow` → новый `_handle_api_extraction(step_config)`, с fallback/сравнением по режиму.
- `_handle_api_extraction(step_config)`:
  - `type: "value"` — `wait_for` + `resolve_path` + `_apply_post_processing`;
  - `type: "list_nested"` — список id из `list_request`; для каждого id навигация на страницу перевозки
    (тот же `base_url_template`, что в DOM-режиме) и `wait_for(nested_request)`; агрегация через существующий
    `_aggregate_nested_results`, чтобы результат шага имел ту же структуру (`total_carriages`, `details[]`).
- Если `totalCount` списка больше числа полученных элементов — предупреждение `API_LIST_TRUNCATED`
  (в режиме `api` — ошибка шага), до реализации постраничной загрузки (раздел 8).
- Результат шага сохраняет текущий формат, добавляется `__STEP_DATA_SOURCE__`: `dom` | `api` | `dom_fallback`.
- Режим `shadow`: значение из DOM возвращается как результат, API-значение считается отдельно и сравнивается;
  расхождение — `API_SHADOW_MISMATCH step=… dom=… api=…`, совпадение — `API_SHADOW_MATCH`.
- `_calculate_run_status()` — ошибка извлечения из API считается ошибкой шага (как сейчас исключение в DOM-режиме).

### 5.5 `core/ozon_report_parser.py`

- `get_current_pvz()` — в API-режимах сначала `stores/current` из перехваченных ответов текущей страницы
  (запрашивается при каждой загрузке), fallback — текущее чтение `input___v-0-0`.
- `collect_available_pvz()` — в API-режимах `stores/list` (если на странице его не было — открыть список ПВЗ,
  что вызывает запрос, либо перейти на страницу, где он запрашивается); fallback — чтение подписей.
- `set_pvz()` / `ensure_correct_pvz()` — переключение по-прежнему кликом в списке; в API-режимах результат
  проверяется по ответу `select-v2` и последующему `stores/current`:
  - `select-v2` со статусом ≠ 200 → явная ошибка `PVZ_SWITCH_REJECTED` (признак отозванной сессии) вместо
    текущей картины «ПВЗ не изменился, затем редирект на /login».

### 5.6 Реализации и точки входа

- `implementations/multi_step_ozon_parser.py`, `ozon_available_pvz_parser.py` — изменений логики не требуется,
  режим приходит из конфига.
- `tests/run_single_date_smoke.py`, `run_available_pvz_discovery_smoke.py` — аргумент `--data_source {dom,shadow,api_with_dom_fallback,api}`.
- `parser_invocation.py` — при необходимости `apply_data_source_override_to_parser_config(config, mode)`.

### 5.7 Тесты

- `core/tests/test_api_response_capture.py` (новый): разбор путей, `query_contains`, таймаут ожидания, ограничения
  размера, отсутствие заголовков в записях. Драйвер — mock с `execute_script`, ответы — синтетические фикстуры
  (не реальные ответы Ozon: в них персональные данные).
- `core/tests/test_base_report_parser.py`: выбор источника по режиму, `value` и `list_nested`, fallback, shadow-сравнение,
  ошибка без значения по умолчанию, `API_LIST_TRUNCATED`, неизменность формата результата.
- `core/tests/test_ozon_report_parser.py`: текущий ПВЗ и список ПВЗ из API, `PVZ_SWITCH_REJECTED`.
- Режим `dom` проверяется существующими тестами — они должны проходить без изменений.

### 5.8 Документация (в том же коммите, что и код)

- `docs/README.md` — режимы источника данных.
- `docs/BaseReportParser/README.md`, `docs/OzonReportParser/README.md` — новые методы и поведение по режимам.
- `docs/DEBUG_GUIDE.md` — маркеры `API_*`, как найти нужный запрос при изменении API (повторить разведку).
- Этот документ — статус этапов.

## 6. Этапы и порядок внедрения

| Этап | Содержание | Режим на проде | Критерий завершения |
|---|---|---|---|
| 1 ✅ | `ApiResponseCapture`, установка в `setup_browser`, параметр `DATA_SOURCE_MODE`, тесты | `dom` | все тесты проходят, поведение в `dom` не изменилось; живая проверка — раздел 10 |
| 2 ✅ | `api_extraction` в шагах, `_handle_api_extraction`, режимы `shadow`/`api_with_dom_fallback`/`api`, smoke `--data_source` | `dom` | smoke за 28.09 для 144 в `api` совпадает с DOM (508/89/24) — раздел 11 |
| 3 | ПВЗ через API: текущий, список, проверка переключения | `dom` | smoke с переключением 143 ↔ 144 и discovery в `api` |
| 4 | Обкатка: `shadow` на 144 не менее 7 вечерних запусков | `shadow` | нет `API_SHADOW_MISMATCH` |
| 5 | Переход: `api_with_dom_fallback`, затем `api`; распространение на остальные ПВЗ через `UpdaterScript` | `api` | 7 запусков без `API_EXTRACTION_FALLBACK` |
| 6 | (опционально) удаление DOM-селекторов шагов данных | `api` | решение принимается отдельно |

Каждый этап — отдельная ветка и коммит с документацией; до этапа 4 прод работает в `dom` без изменения поведения.

## 7. Риски и как они закрываются

| Риск | Мера |
|---|---|
| Ozon изменит путь или поля API | явная ошибка с именем запроса/поля вместо тихого 0; `api_with_dom_fallback` как страховка; разведку повторить скриптом из раздела 9 |
| Ответ получен до установки перехватчика | перехватчик ставится через `addScriptToEvaluateOnNewDocument` сразу после старта драйвера, до первой навигации |
| Прочитан устаревший ответ другой страницы/даты | буфер привязан к документу; `query_contains` проверяет дату и id перевозки |
| Утечка токенов или персональных данных в логи | заголовки не сохраняются, тела не логируются, фикстуры тестов синтетические |
| Больше 20 перевозок за день | `API_LIST_TRUNCATED` (предупреждение/ошибка); постраничная загрузка — раздел 8 |
| Самостоятельные вызовы API ломают сессию | не делаются: переключение ПВЗ через UI, токены сохраняет фронтенд |
| Детектирование автоматизации по аудиту (`HeadlessChrome`) | вне рамок плана; учитывать при решениях о режиме headless |

## 8. Вне рамок первой итерации

- **Постраничная загрузка перевозок** (больше `take=20`): через UI-пагинацию или повтор запроса с `skip` из страницы.
- **Вариант Б** — вызовы API напрямую из страницы (`fetch` с заголовками фронтенда) без отрисовки экранов:
  быстрее, но сильнее зависит от внутренней механики авторизации Ozon.
- **Мониторинг**: уведомление, если за вчера нет строки в KPI или ReportsProcessor завершился с ошибкой;
  запись stderr упавшего подпроцесса на уровне ERROR. Не зависит от этого плана и может делаться параллельно.

## 9. Как повторить разведку

Скрипт разведки перехватывает все `fetch`/XHR на открываемых страницах через ту же инфраструктуру парсера
(snapshot-профиль с обратной записью сессии) и ищет в ответах эталонные значения, полученные DOM-парсером.
После этапа 1 он заменяется встроенным режимом: `DATA_SOURCE_MODE = "shadow"` плюс отладочный вывод всех
перехваченных путей (`API_CAPTURE_DUMP` на уровне DEBUG, без тел и заголовков).

Правила при ручной разведке:
- сохранять захват только во временную папку и удалять после анализа — в ответах `select-v2` действующие токены,
  в списках выдачи — логины сотрудников и номера заказов;
- не логировать значения заголовков.

## 10. Результаты этапа 1 (30.09.2026)

- `core/api_response_capture.py`: `ApiResponseCapture` (`install`, `records`, `wait_for`, `debug_dump`), `resolve_path`,
  исключения `ApiResponseNotFound`, `ApiResponseError`, `ApiFieldNotFound`. Перехватчик хранит в `window.__apiCapture`
  URL, метод, статус и тело ответа (без заголовков); ограничения `max_records` и `max_body_chars`.
- `DATA_SOURCE_MODE` и `api_capture` в `configs/base_configs/ozon_report_config.py` (по умолчанию `dom`).
- `BaseParser.setup_browser()` ставит перехватчик после старта драйвера, если режим не `dom` (`API_CAPTURE_INSTALLED`;
  при сбое — `API_CAPTURE_INSTALL_FAILED`, браузер продолжает работу, `api_capture = None`).
- Живая проверка в режиме `shadow` для ЧЕБОКСАРЫ_144 за 28.09.2026 — значения из API совпали с DOM-парсером:
  `logV2.totalCount` = 508, `stores/current` → ЧЕБОКСАРЫ_144, `carriages[].carriageId` = [141445739],
  `GetCarriageArticles.totalCount` = 89. Ожидание ответа: ~15 с на первой странице сессии, ~4 с на следующих.
- Отличие от плана: перехватчик хранит записи в `window.__apiCapture` документа, а не в `sessionStorage`, как скрипт
  разведки. Для шагов парсера этого достаточно: ответ читается на той же странице, что его запросила.

## 11. Результаты этапа 2 (30.09.2026)

Реализовано в `core/base_report_parser.py`:
- `_extract_step_result` — выбор источника по `DATA_SOURCE_MODE`; результат шага дополнен `__STEP_DATA_SOURCE__`
  (`dom` | `api` | `dom_fallback`), структура результата та же, что у DOM (число или `{total_carriages, details[]}`);
- `_handle_api_extraction` (`value`) и `_handle_api_list_nested` (`list_nested`: список id из одного ответа, переход на
  страницу каждой перевозки по тем же шаблонам `nested_processing`, значение из ответа страницы);
- `_wait_api_response` — при отсутствии ответа одна перезагрузка страницы и повторное ожидание (`API_RETRY_RELOAD`);
- без значений по умолчанию: нет запроса, поля или страницы перевозки — ошибка шага (в DOM-режиме недоступная
  перевозка молча пропускается, и сумма занижается);
- `shadow`: результат из разметки, сравнение с API (`API_SHADOW_MATCH` / `API_SHADOW_MISMATCH` / `API_SHADOW_ERROR`);
  после API-извлечения со вложенными переходами — возврат на страницу шага;
- `api_extraction` для шагов `giveout`, `direct_flow`, `return_flow` в `configs/implementations/multi_step_ozon_config.py`;
  `api_capture.wait_timeout` увеличен до 30 с; `tests/run_single_date_smoke.py --data_source`.

Живая проверка (ЧЕБОКСАРЫ_144, 28.09.2026):
- `--data_source api`: 508 / 89 / 24, все шаги из API, запуск 1 мин 39 с (DOM — ~2 мин);
- `--data_source shadow` при нестабильной сети («Ваше интернет соединение нестабильно»): первая страница не получила
  данные ни в API за 15 с, ни в разметке — **DOM-режим записал выдачу = 0** (пустой счетчик превратился в 0 через
  `default_value`), API-режим сообщил бы об ошибке. Третий шаг — `API_SHADOW_MATCH` (24). По итогам — ожидание 30 с и
  перезагрузка страницы при отсутствии ответа.

### Даты с ошибкой шага и «тихие нули» DOM (решено 30.09.2026)

Проблема: дата с ошибкой одного из шагов (`__RUN_STATUS__ = partial`) выгружалась с пустой ячейкой, после чего
coverage-check больше ее не собирал; DOM-режим превращал пустой счетчик в 0 и молча пропускал недоступные перевозки.

Решение:
- `FAIL_DATE_ON_STEP_ERROR: True` (`multi_step_ozon_config.py`): дата с ошибкой шага — ошибка даты
  (`PARTIAL_DATE_REJECTED`), не выгружается и собирается повторно следующим запуском;
- DOM, перевозки — `nested_processing.strict: True` и `data_extraction.required: True`: таблица не найдена
  (`DOM_TABLE_ERROR`), страница перевозки не открылась (`DOM_NESTED_PAGE_FAILED`), счетчик пуст (`DOM_VALUE_NOT_FOUND`) —
  ошибка шага. Пустая таблица (0 перевозок) — честный 0: на странице без перевозок таблица есть, в ней 0 строк;
- DOM, выдача — **без** `required`: при 0 выдач Ozon не показывает счетчик «Найдено» вовсе (проверено на дате без данных:
  DOM — пусто, API — `logV2.totalCount = 0`), поэтому в разметке настоящий 0 неотличим от незагрузившейся страницы.
  Строгая проверка отклоняла бы каждый день без выдач навсегда. Надежное решение для выдачи — режим `api`;
- `AUTH_REQUIRED` при переходе на страницу перевозки больше не глотается в DOM-режиме (раньше перевозка пропускалась).
