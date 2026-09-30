# Инструменты диагностики парсера

Скрипты, с помощью которых разбирался инцидент 03.07–29.09.2026 (см. [DEBUG_GUIDE.md](../DEBUG_GUIDE.md),
раздел «Ход диагностики»). Все инструменты **только читают** данные, кроме отмеченных: они закрывают Edge
текущего пользователя, как это делает сам парсер.

Python-скрипты запускаются **из корня проекта** (`C:\tools\scheduler`) тем интерпретатором, который использует задача
(`C:\Program Files\Python313\python.exe`).

## Порядок диагностики (от дешевого к дорогому)

| # | Вопрос | Инструмент | Закрывает Edge |
|---|---|---|---|
| 1 | Задача вообще запускает Python-код? Хватает пакетов и секретов? | `check_task_environment.cmd` | нет |
| 2 | Поломка у одного ПВЗ или у всех, с какой даты? | `kpi_last_dates.py` | нет |
| 3 | Совпадают ли селекторы парсера с текущей разметкой Турбо ПВЗ? | `browser_*.js` в консоли Edge | нет |
| 4 | Парсер собирает данные на живом Ozon (без выгрузки)? | `tests/run_single_date_smoke.py` | **да** |
| 5 | Edge не стартует (`DevToolsActivePort file doesn't exist`)? | `check_edge_remote_debugging.ps1` | нет, но требует закрытого Edge |
| 6 | Где данные в API (для перехода на API, см. [MODERNIZATION_PLAN.md](../MODERNIZATION_PLAN.md))? | `api_recon.py` | **да** |

## Окружение задачи

### `check_task_environment.cmd`
Запускать **в сеансе пользователя задачи** (у операторской задачи — пользователь `Оператор`): у каждого пользователя
Windows свой PATH и свои user site-packages. Двойной клик или `cmd /c check_task_environment.cmd`.
Результат: `C:\Users\Public\reports_diag.txt` — какой Python запускает `pythonw`, где его пакеты, импортируются ли
зависимости и `reports_processor`, на месте ли `.env\secrets.env` и ключ Google.

Если задача завершается с `Подпроцесс завершился с кодом 1` без лога процессора — начинать отсюда: stderr подпроцесса
пишется в лог только на уровне DEBUG.

## Данные в Google Sheets

### `kpi_last_dates.py`
```powershell
python scheduler_runner\utils\parser\docs\debug\kpi_last_dates.py
python scheduler_runner\utils\parser\docs\debug\kpi_last_dates.py --show-from 2026-09-20
```
Последняя дата, пропуски и дубликаты (ПВЗ + дата) в листе `KPI` по каждому ПВЗ; `--show-from` выводит значения. Строки smoke-тестов (2099 год)
не учитываются. Одна дата обрыва у всех ПВЗ указывает на внешнюю причину (Ozon, Edge), а не на конкретную машину.

## Разметка Турбо ПВЗ (консоль Edge)

Открыть страницу в обычном Edge под учетной записью с доступом к ПВЗ, обновить `Ctrl+F5`, F12 → Console, вставить
содержимое скрипта, Enter. Если Edge не дает вставить — ввести `allow pasting`. Результат копируется в буфер обмена
(вставить в Блокнот). Скрипты ничего не нажимают.

| Скрипт | Где запускать | Что показывает |
|---|---|---|
| `browser_selectors_probe.js` | выдача, архив перевозок, страница перевозки | `OK`/`MISS` для каждого селектора парсера; актуальные классы по префиксу без хэша; элементы с текстом «Найдено»; input и таблицы страницы |
| `browser_counter_probe.js` | выдача, страница перевозки **с фильтром** `articleState=Took` | разметка счетчика «Найдено: N» (класс и точный текст) |
| `browser_overlay_probe.js` | любая страница, **пока открыто** окно «Новости» | видит ли парсер окно и фон, какую кнопку закрытия он нажмет, перекрывает ли что-то поле ПВЗ |
| `browser_pvz_dropdown_probe.js` | любая страница с полем ПВЗ | разметка открытого списка ПВЗ. После Enter есть 5 секунд, чтобы **открыть список мышкой** (при фокусе в консоли список закрывается) |

`SEL`/`CFG` внутри скриптов — копии селекторов из `configs/`: при изменении конфигов обновлять и здесь.

## Браузер и сессия

### `tests/run_single_date_smoke.py` (не в этой папке)
```powershell
python -m scheduler_runner.utils.parser.tests.run_single_date_smoke --pvz ЧЕБОКСАРЫ_144 --execution_date 2026-09-28 --pretty
```
Полный цикл парсера за одну дату без выгрузки в Google Sheets и уведомлений. Лог: `logs/reports_domain/Parser/`
(`*_debug.log` — навигация, селекторы, `EDGE_SNAPSHOT*`). Два запуска подряд проверяют, что сессия Ozon переживает
работу парсера (обратная запись `EDGE_SNAPSHOT_SYNC`).

### `check_edge_remote_debugging.ps1`
```powershell
powershell -ExecutionPolicy Bypass -File scheduler_runner\utils\parser\docs\debug\check_edge_remote_debugging.ps1
```
Запускает Edge в headless с `--remote-debugging-port=0` на профиле по умолчанию и на пустом каталоге и сообщает,
открылся ли порт DevTools. Ожидаемо: `default : DevTools НЕ доступен` («requires a non-default data directory»),
`separate : DevTools OK`. Требует закрытого Edge текущего пользователя (включая фоновые процессы без окон).

## API Турбо ПВЗ

### `api_recon.py`
```powershell
python scheduler_runner\utils\parser\docs\debug\api_recon.py pages --pvz ЧЕБОКСАРЫ_144 --date 2026-09-28 --expect 508 89 24 141445739 141443498 --carriage 141445739 141443498
python scheduler_runner\utils\parser\docs\debug\api_recon.py switch --pvz ЧЕБОКСАРЫ_144 --switch-to ЧЕБОКСАРЫ_143
```
Поднимает браузер через инфраструктуру парсера (snapshot + обратная запись сессии), встраивает перехватчик
`fetch`/XHR и печатает запросы к API с формой ответа; ответы, где встретились значения `--expect`, помечаются `<<<`
с путем в JSON. `switch` переключает ПВЗ через UI и обратно, показывая запросы переключения.

Эталонные значения берутся из DOM-парсера (smoke выше) за ту же дату; id перевозок — из его результата (`details[].identifier`).

**Захват содержит действующие токены сессии и персональные данные** — пишется во временную папку и удаляется
после вывода сводки (`--keep` оставляет файл; удалить вручную после анализа). Заголовки запросов не сохраняются.
