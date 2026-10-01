# Инфраструктура Reports-домена

Полная инструкция по подготовке Google Sheets + Apps Script для работы `scheduler_runner/tasks/reports`.

---

## 1. Google Spreadsheet

### 1.1 Создание таблицы

1. Открой https://sheets.google.com → **Создать таблицу**
2. Дай название, например: `Scheduler KPI`
3. Скопируй **SPREADSHEET_ID** из URL:
   ```
   https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit
   ```

### 1.2 Создание и настройка сервисного аккаунта GCP

Для доступа к Google Sheets нужен сервисный аккаунт Google Cloud Platform (GCP).
Ниже — пошаговая инструкция от создания проекта до предоставления доступа к таблице.

---

#### Шаг 1: Создание проекта в Google Cloud Console

1. Открой https://console.cloud.google.com
2. Если у тебя ещё нет проекта:
   - В верхней панели нажми на название текущего проекта (или **Select a project**)
   - Нажми **New Project** (справа вверху)
   - Введи название, например: `scheduler-kpi-access`
   - Нажми **Create**
   - Дождись создания и переключись на этот проект (выбери из списка)

#### Шаг 2: Активация API

Для работы с Google Sheets нужны два API:

1. Открой https://console.cloud.google.com/apis/library (или **APIs & Services** → **Library**)
2. Найди и активируй:
   - **Google Sheets API** → нажми **Enable**
   - **Google Drive API** → нажми **Enable** (нужен для работы gspread)

> **Примечание:** Google Sheets и Drive API бесплатны для большинства сценариев. Квоты по умолчанию достаточны для работы scheduler.

#### Шаг 3: Создание сервисного аккаунта

1. Открой https://console.cloud.google.com/iam-admin/serviceaccounts (или **IAM & Admin** → **Service Accounts**)
2. Нажми **+ Create Service Account**
3. Заполни форму:
   - **Service account name**: `scheduler-sheets-access`
   - **Description**: `Access to KPI Google Sheets for scheduler`
   - Нажми **Create and Continue**
4. Назначь роль:
   - **Role**: выбери **Basic** → **Editor** (или **Project** → **Editor**)
   - Нажми **Continue**
5. Нажми **Done**

#### Шаг 4: Генерация JSON key файла

1. В списке сервисных аккаунтов найди `scheduler-sheets-access@...iam.gserviceaccount.com`
2. Нажми на него → вкладка **Keys**
3. Нажми **Add Key** → **Create new key**
4. Выбери тип: **JSON**
5. Нажми **Create**
6. JSON-файл автоматически скачается на компьютер (имя вида `your-project-12345-abc123.json`)

#### Шаг 5: Структура JSON key файла

Скачанный файл выглядит примерно так:

```json
{
  "type": "service_account",
  "project_id": "scheduler-kpi-access",
  "private_key_id": "a1b2c3d4e5f6...",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEv...\n-----END PRIVATE KEY-----\n",
  "client_email": "scheduler-sheets-access@scheduler-kpi-access.iam.gserviceaccount.com",
  "client_id": "123456789012345678901",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/..."
}
```

**Важные поля:**
- `client_email` — email сервисного аккаунта. Его нужно добавить в настройки доступа Google-таблицы
- `private_key` — закрытый ключ. Никому не передавай этот файл

#### Шаг 6: Сохранение JSON-файла

1. Переименуй скачанный файл в `scheduler-test-account-b62ccb681f06.json` (или используй своё имя)
2. Положи его по пути:
   ```
   C:\tools\scheduler\.env\gspread\scheduler-test-account-b62ccb681f06.json
   ```
3. Убедись что директория `.env\gspread\` существует (создай если нет)

#### Шаг 7: Предоставление доступа к Google-таблице

1. Открой JSON-файл и скопируй значение поля `client_email`, например:
   ```
   scheduler-sheets-access@scheduler-kpi-access.iam.gserviceaccount.com
   ```
2. Открой Google-таблицу → **Share** (кнопка в правом верхнем углу) → **Share with others**
3. В поле **Add people and groups** вставь `client_email`
4. Выбери роль: **Editor** (Редактор)
5. Нажми **Send** (или **Share**)

> **Важно:** Нужно проделать этот шаг для **каждой** Google-таблицы (production и test), к которой должен иметь доступ scheduler.

#### Шаг 8: Проверка доступа

Запусти простую проверку из проекта:

```powershell
.venv\Scripts\python.exe -c "
from scheduler_runner.tasks.reports.config.scripts.kpi_google_sheets_config import KPI_GOOGLE_SHEETS_CONFIG
from scheduler_runner.tasks.reports.config.reports_paths import REPORTS_PATHS
import gspread

gc = gspread.service_account(filename=str(REPORTS_PATHS['GOOGLE_SHEETS_CREDENTIALS']))
spreadsheet = gc.open_by_key(KPI_GOOGLE_SHEETS_CONFIG['SPREADSHEET_ID'])
print(f'Connected to: {spreadsheet.title}')
print(f'Sheets: {[ws.title for ws in spreadsheet.worksheets()]}')
"
```

Ожидаемый вывод:
```
Connected to: Scheduler KPI (TEST)
Sheets: ['KPI', 'KPI_FAILOVER_STATE', 'KPI_REWARD_RULES']
```

Если видишь `Access denied` или `403` — проверь что email сервисного аккаунта добавлен в настройки доступа таблицы с ролью **Editor**.

---

### 1.3 Troubleshooting

| Ошибка | Причина | Решение |
|---|---|---|
| `Не удалось подключиться к KPI_FAILOVER_STATE` | Нет доступа к таблице | Добавь `client_email` в Share → Editor |
| `403 PERMISSION_DENIED` | Файл credentials не найден или битый | Проверь путь `.env/gspread/...` и содержимое JSON |
| `invalid_client: Service account user not found` | Сервисный аккаунт удалён | Пересоздай сервисный аккаунт (Шаг 3-4) |
| `File not found` | SPREADSHEET_ID неверный | Проверь ID в URL таблицы |

---

## 2. Лист "KPI"

Основной лист для загрузки KPI-данных парсером.

### 2.1 Структура колонок (строка 1 — заголовки)

| A | B | C | D | E | F | G | H | I | J | K |
|---|---|---|---|---|---|---|---|---|---|---|
| id | work_date | object_name | issued_packages | direct_flow | return_flow | reward_issued_packages | reward_direct_flow | reward_return_flow | total_reward | timestamp |

### 2.2 Формулы

**Столбец A (id)** — строка 2:
```
=B2&C2
```
Протяни вниз на весь лист.

**Столбец G (reward_issued_packages)** — строка 2:
```
=GET_REWARD("issued_packages";D2;B2;KPI_REWARD_RULES_RANGE)
```

**Столбец H (reward_direct_flow)** — строка 2:
```
=GET_REWARD("direct_flow";E2;B2;KPI_REWARD_RULES_RANGE)
```

**Столбец I (reward_return_flow)** — строка 2:
```
=GET_REWARD("return_flow";F2;B2;KPI_REWARD_RULES_RANGE)
```

**Столбец J (total_reward)** — строка 2:
```
=SUM(G2:I2)
```
Протяни формулы G–J вниз на весь лист.

### 2.3 Форматирование

- **Столбец B (work_date)** — формат `ДД.ММ.ГГГГ`
- **Столбец K (timestamp)** — формат `ДД.ММ.ГГГГ ЧЧ:ММ:СС`

---

## 3. Лист "KPI_FAILOVER_STATE"

Лист координации failover-механизма. Используется для:
- отслеживания `owner_failed` дат, нуждающихся в recovery
- атомарного claim через Apps Script LockService
- tracking `failover_claimed`, `failover_success`, `failover_failed`

### 3.1 Структура колонок (строка 1 — заголовки)

| A | B | C | D | E | F | G | H | I | J | K | L |
|---|---|---|---|---|---|---|---|---|---|---|---|
| request_id | work_date | target_object_name | owner_object_name | status | claimed_by | claim_expires_at | last_error | attempt_no | source_run_id | updated_at | timestamp |

### 3.2 Уникальный ключ

Пара **work_date + target_object_name** — уникальный ключ для upsert-операций.

### 3.3 Допустимые значения status

| Статус | Описание |
|---|---|
| `owner_pending` | Ожидание результата owner-run |
| `owner_success` | Owner успешно закрыл дату (terminal) |
| `owner_failed` | Owner не смог закрыть дату |
| `failover_claimed` | Коллега взял claim |
| `failover_success` | Коллега успешно восстановил (terminal) |
| `failover_failed` | Коллега не смог восстановить |
| `claim_expired` | Claim истёк по TTL |

### 3.4 Форматирование

- **Столбец B (work_date)** — формат `ДД.ММ.ГГГГ`
- **Столбец G (claim_expires_at)** — формат `ДД.ММ.ГГГГ ЧЧ:ММ:СС`
- **Столбец K (updated_at)** — формат `ДД.ММ.ГГГГ ЧЧ:ММ:СС`

---

## 4. Лист-справочник "KPI_REWARD_RULES"

Справочник правил расчёта вознаграждений. Используется кастомной функцией `GET_REWARD()` (Apps Script) в формулах листа KPI.

### 4.1 Структура (строка 1 — заголовки)

| A | B | C | D | E |
|---|---|---|---|---|
| id | kpi_type | param_value | reward_amount | effective_date |

### 4.2 Пример данных

| id | kpi_type | param_value | reward_amount | effective_date |
|---|---|---|---|---|
| 1 | issued_packages | 0 | 50 | 01.01.2026 |
| 2 | issued_packages | 100 | 100 | 01.01.2026 |
| 3 | issued_packages | 200 | 200 | 01.01.2026 |
| 4 | direct_flow | 0 | 50 | 01.01.2026 |
| 5 | direct_flow | 50 | 100 | 01.01.2026 |
| 6 | return_flow | 0 | 50 | 01.01.2026 |
| 7 | return_flow | 30 | 100 | 01.01.2026 |
| ... | ... | ... | ... | ... |

> **Как работает GET_REWARD():** Функция ищет правила для указанного `kpiType` с `effective_date` ≤ даты отчёта. Среди найденных выбирает правило с максимальной `effective_date`, а затем — строку с наибольшим `param_value`, которое ≤ переданного значения. Возвращает соответствующую `reward_amount`. Если правило не найдено — возвращает `0`.

### 4.3 Named Range

1. Выдели диапазон данных **без заголовка**: `KPI_REWARD_RULES!B2:E999`
2. **Данные** → **Настроить именованные диапазоны** → **Добавить диапазон**
3. Имя: `KPI_REWARD_RULES_RANGE`
4. Нажми **Готово**

> **Важно:** диапазон должен включать столбцы B–E (kpi_type, param_value, reward_amount, effective_date). Столбец A (id) не включается — он используется только для внутренней нумерации.

---

## 5. Apps Script проект "Claim"

В одном проекте Apps Script размещаются **два файла**:
1. `failover_apps_script_try_claim.gs` — атомарный claim failover-строк
2. `CustomFormulas.gs` — кастомная функция `GET_REWARD()` для листа KPI

### 5.1 Создание проекта

1. Открой Google Sheet → **Расширения** → **Apps Script**
2. Назови проект, например: **Claim**

### 5.2 Файл 1: `failover_apps_script_try_claim.gs` (Failover Claim)

Создай файл с именем `failover_apps_script_try_claim.gs` и вставь **целиком** код из репозитория:
`scheduler_runner/tasks/reports/storage/INFRASTRUCTURE_failover_apps_script_try_claim.gs`
(копия здесь не приводится, чтобы не расходилась с файлом).

Действие `try_claim_failover`:
- строка (`work_date`, `target_object_name`) есть → захват, если она не завершена и не захвачена с действующим сроком;
- строки нет → `row_not_found`, а с `create_if_missing: true` (версия 2, 01.10.2026) — строка добавляется сразу
  захваченной (`created_and_claimed`, `last_error=owner_silent`): так помощник берет дату ПВЗ, который не запускался.

### 5.3 Файл 2: `CustomFormulas.gs` (Reward Formulas)

Создай файл с именем `CustomFormulas.gs` и вставь код кастомной функции `GET_REWARD()`:

```javascript
/**
 * Возвращает сумму вознаграждения по типу KPI, значению параметра и дате.
 *
 * @param {string} kpiType      Тип параметра KPI.
 * @param {number} paramValue   Значение параметра.
 * @param {Date}   date         Дата.
 * @param {Range|Array} data    Диапазон или массив данных (столбцы A,B,C,D).
 * @return {number|string}      Сумма вознаграждения или 0.
 * @customfunction
 */
function GET_REWARD(kpiType, paramValue, date, data) {
  let values;
  if (data && typeof data.getValues === 'function') {
    values = data.getValues();
  } else if (Array.isArray(data)) {
    values = data;
  } else {
    return "Ошибка: последний аргумент должен быть диапазоном (например, B2:E100)";
  }

  const queryDate = new Date(date);
  let lastValidDate = null;
  let rowsForLastDate = [];

  for (let i = 0; i < values.length; i++) {
    const type = values[i][0];
    const rowDate = new Date(values[i][3]);

    if (type === kpiType && rowDate <= queryDate) {
      if (lastValidDate === null || rowDate > lastValidDate) {
        lastValidDate = rowDate;
        rowsForLastDate = [];
      }
      if (rowDate.getTime() === lastValidDate.getTime()) {
        rowsForLastDate.push({
          param: values[i][1],
          reward: values[i][2]
        });
      }
    }
  }

  if (rowsForLastDate.length === 0) return 0;

  let bestMatch = null;
  for (let i = 0; i < rowsForLastDate.length; i++) {
    const p = rowsForLastDate[i].param;
    if (p <= paramValue) {
      if (bestMatch === null || p > bestMatch.param) {
        bestMatch = {
          param: p,
          reward: rowsForLastDate[i].reward
        };
      }
    }
  }

  return bestMatch ? bestMatch.reward : 0;
}
```

### 5.4 Настройки проекта → Свойства скрипта

1. В Apps Script: **Настройки проекта** (шестерёнка слева) → прокрути вниз до **Свойства скрипта**
2. Нажми **Добавить свойство скрипта**
3. Ключ: `FAILOVER_SHARED_SECRET`
4. Значение: сгенерируй случайную строку, например `3f2e8f5b7f8d4e9a9b2c1d6e7f8a1b3c`
5. Нажми **Сохранить свойства скрипта**

### 5.5 Развёртывание (Deployment)

1. В Apps Script: синяя кноп **Начать развертывание** → **Новое развертывание**
2. Тип: **Веб-приложение**
3. Описание: `Claim v1`
4. Выполнять от имени: **Меня**
5. У кого есть доступ: **Все** (обязательно — иначе Python не сможет POST-ить)
6. Нажми **Начать развертывание**
7. Скопируй **URL веб-приложения** (заканчивается на `/exec`)

**Обновление кода** (например, переход на версию 2): вставь новый код в файл → **Развертывание** →
**Управление развертываниями** → карандаш у текущего развертывания → **Версия: Новая версия** → **Развернуть**.
URL не меняется, `secrets.env` на ПВЗ трогать не нужно. «Новое развертывание» вместо этого создаст **новый** URL.

---

## 6. Переменные окружения

Файл: `.env\secrets.env`

```ini
# VK notifications (production provider)
VK_ACCESS_TOKEN_PROD=<токен_из_VK_App>
VK_PEER_ID_PROD=<peer_id_чата>
VK_ACCESS_TOKEN_TEST=<токен_из_VK_App>
VK_PEER_ID_TEST=<peer_id_чата>
VK_API_VERSION=5.199
NOTIFICATION_PROVIDER_PROD=vk
NOTIFICATION_PROVIDER_TEST=vk

# Telegram (заблокирован в РФ, оставлен для полноты)
TELEGRAM_TOKEN_PROD=
TELEGRAM_CHAT_ID_PROD=
TELEGRAM_TOKEN_TEST=
TELEGRAM_CHAT_ID_TEST=

# Apps Script failover claim
FAILOVER_APPS_SCRIPT_URL=https://script.google.com/macros/s/{DEPLOYMENT_ID}/exec
FAILOVER_SHARED_SECRET=3f2e8f5b7f8d4e9a9b2c1d6e7f8a1b3c
```

> **Важно:** `FAILOVER_SHARED_SECRET` здесь должен **точно совпадать** со значением в свойствах скрипта Apps Script.

---

## 7. Конфигурация в коде

### 7.1 `kpi_google_sheets_config.py`

```python
SPREADSHEET_ID = "{SPREADSHEET_ID_из_URL}"
```

### 7.2 `reports_paths.py`

Проверь путь к credentials:
```python
GOOGLE_SHEETS_CREDENTIALS = BASE_DIR / ".env" / "gspread" / "scheduler-test-account-b62ccb681f06.json"
```
Убедись что файл существует по этому пути.

---

## 8. Проверка

### 8.1 Unit-тесты

```powershell
.venv\Scripts\python.exe -m pytest scheduler_runner/tasks/reports/tests/ -q
```

### 8.2 Smoke-тесты

```powershell
# VK notification
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.tests.run_vk_notification_e2e_smoke --pretty

# Failover claim через Apps Script
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.tests.run_failover_claim_smoke --claim_backend apps_script --pretty

# KPI reward formulas
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.tests.run_kpi_reward_formulas_e2e_smoke --pretty

# Failover state upsert
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.tests.run_failover_state_upsert_smoke --pretty

# Owner success suppression
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.tests.run_failover_state_owner_success_policy_smoke --pretty
```

---

## Чек-лист

- [ ] Проект GCP создан, Google Sheets API и Google Drive API активированы
- [ ] Сервисный аккаунт создан с ролью Editor
- [ ] JSON key файл сгенерирован и сохранён в `.env/gspread/scheduler-test-account-b62ccb681f06.json`
- [ ] `client_email` добавлен в Share → Editor для каждой Google-таблицы (production + test)
- [ ] Проверка доступа прошла успешно (Шаг 8)
- [ ] Google Spreadsheet создана, SPREADSHEET_ID записан
- [ ] Лист "KPI" с 11 колонками (Latin snake_case) и формулами (G–J вызывают `GET_REWARD()`)
- [ ] Лист "KPI_FAILOVER_STATE" с 12 колонками (Latin snake_case, `request_id` первым)
- [ ] Лист "KPI_REWARD_RULES" с 5 колонками (id, kpi_type, param_value, reward_amount, effective_date)
- [ ] Named Range `KPI_REWARD_RULES_RANGE` = `KPI_REWARD_RULES!B2:E999`
- [ ] Сервисный аккаунт добавлен в Editors таблицы
- [ ] Apps Script проект создан с двумя файлами:
  - `failover_apps_script_try_claim.gs` (claim через LockService, столбцы `work_date`, `target_object_name`)
  - `CustomFormulas.gs` (функция `GET_REWARD()`)
- [ ] `FAILOVER_SHARED_SECRET` добавлен в свойствах скрипта
- [ ] Web App развёрнут с доступом **Все**
- [ ] `.env\secrets.env` заполнен (FAILOVER_APPS_SCRIPT_URL, FAILOVER_SHARED_SECRET, VK-credentials)
- [ ] `SPREADSHEET_ID` в `kpi_google_sheets_config.py` обновлён
- [ ] Все unit-тесты проходят
- [ ] Smoke-тесты проходят

---

## Автоматическая настройка тестовой таблицы

Для быстрого создания тестовой таблицы с правильной структурой используй:

```
scheduler_runner/tasks/reports/storage/INFRASTRUCTURE_TEST_SHEET_SETUP.gs
```

Этот Apps Script автоматически создаёт все 3 листа с Latin snake_case заголовками, формулами и именованным диапазоном. После запуска нужно только добавить `CustomFormulas.gs` и `failover_apps_script_try_claim.gs` и развернуть как Web App.
