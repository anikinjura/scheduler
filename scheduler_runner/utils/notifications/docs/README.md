# Изолированный Микросервис Уведомлений

## Общее Описание

`scheduler_runner/utils/notifications` — это изолированный transport-layer для отправки уведомлений.

Ключевой принцип:

- микросервис принимает все provider-specific параметры извне;
- микросервис не читает `.env`, `ENV_MODE`, доменные `*_paths.py` и другие внешние config sources напрямую;
- внешний слой проекта поднимает secrets и формирует `connection_params`;
- notifications-layer только выбирает backend и выполняет отправку.

Это позволяет использовать один и тот же transport contract в разных доменах и с разными каналами доставки.

---

## Текущий Baseline

Сейчас микросервис поддерживает два transport backend'а:

- `TelegramNotifier`
- `VkNotifier`

Выбор backend выполняется через:

- `NOTIFICATION_PROVIDER=telegram`
- `NOTIFICATION_PROVIDER=vk`

Если `NOTIFICATION_PROVIDER` не задан, по умолчанию используется `telegram`.

---

## Архитектура

### 1. Public Interface

Внешние entrypoints:

- `send_notification()` — отправка одиночного уведомления
- `send_batch_notifications()` — пакетная отправка
- `test_connection()` — диагностическая проверка подключения

Эти функции находятся в:

- `scheduler_runner/utils/notifications/interface.py`

### 2. Базовые Слои

- `BaseNotifier`
  - общая retry wrapper semantics
  - batch/send orchestration
  - базовая валидация и formatting contract

- `BaseMessageSender`
  - lifecycle подключения
  - validation connection params
  - connected state

### 3. Реальные Backend'ы

- `TelegramNotifier`
  - `scheduler_runner/utils/notifications/implementations/telegram_notifier.py`

- `VkNotifier`
  - `scheduler_runner/utils/notifications/implementations/vk_notifier.py`

### 4. Конфигурации Реализаций

- `BASE_NOTIFIER_CONFIG`
- `TELEGRAM_NOTIFIER_CONFIG`
- `VK_NOTIFIER_CONFIG`

---

## Принцип Изоляции

Это важно не нарушать.

Notifications microservice:

- не знает, где лежит `.env`;
- не знает, какой сейчас `ENV_MODE`;
- не знает, как именно домены собирают credentials;
- не должен сам ходить в доменные config files.

Он получает уже готовые параметры, например:

```python
connection_params = {
    "NOTIFICATION_PROVIDER": "vk",
    "VK_ACCESS_TOKEN": "...",
    "VK_PEER_ID": "2000000001",
    "VK_API_VERSION": "5.199",
}
```

Или:

```python
connection_params = {
    "TELEGRAM_BOT_TOKEN": "...",
    "TELEGRAM_CHAT_ID": "...",
}
```

---

## Поддерживаемые Параметры Подключения

### Telegram

Минимально:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

Опционально:

- `NOTIFICATION_PROVIDER=telegram`
- `CONNECT_TIMEOUT_SECONDS`
- `SEND_TIMEOUT_SECONDS`
- `SEND_RETRY_ATTEMPTS`
- `SEND_RETRY_BACKOFF_SECONDS`

### VK

Минимально:

- `NOTIFICATION_PROVIDER=vk`
- `VK_ACCESS_TOKEN`
- `VK_PEER_ID`

Опционально:

- `VK_API_VERSION`
- `CONNECT_TIMEOUT_SECONDS`
- `SEND_TIMEOUT_SECONDS`
- `SEND_RETRY_ATTEMPTS`
- `SEND_RETRY_BACKOFF_SECONDS`

---

## Сообщение Для Отправки

Поддерживаются:

- обычная строка;
- словарь с данными;
- template-based message через:
  - `template`
  - `data`

Пример:

```python
templates = {
    "alarm": "ПВЗ {pvz_id}: {status} в {timestamp}"
}

message = {
    "template": "alarm",
    "data": {
        "pvz_id": "ЧЕБОКСАРЫ_144",
        "status": "недоступен Telegram, отправка через VK",
        "timestamp": "2026-03-21 19:00:00",
    }
}
```

---

## Использование

### 1. Простая Telegram-Отправка

```python
from scheduler_runner.utils.notifications import send_notification

connection_params = {
    "TELEGRAM_BOT_TOKEN": "your_token",
    "TELEGRAM_CHAT_ID": "your_chat_id",
}

result = send_notification(
    message="Привет, мир!",
    connection_params=connection_params,
)
```

### 2. Простая VK-Отправка

```python
from scheduler_runner.utils.notifications import send_notification

connection_params = {
    "NOTIFICATION_PROVIDER": "vk",
    "VK_ACCESS_TOKEN": "your_vk_community_token",
    "VK_PEER_ID": "2000000001",
    "VK_API_VERSION": "5.199",
}

result = send_notification(
    message="VK alarm message",
    connection_params=connection_params,
)
```

### 3. Отправка С Template

```python
from scheduler_runner.utils.notifications import send_notification

templates = {
    "welcome": "Добро пожаловать, {name}! Ваш ID: {user_id}"
}

message = {
    "template": "welcome",
    "data": {
        "name": "Иван",
        "user_id": "12345"
    }
}

connection_params = {
    "TELEGRAM_BOT_TOKEN": "your_token",
    "TELEGRAM_CHAT_ID": "your_chat_id",
}

result = send_notification(
    message=message,
    connection_params=connection_params,
    templates=templates,
)
```

### 4. Проверка Подключения

```python
from scheduler_runner.utils.notifications import test_connection

result = test_connection({
    "NOTIFICATION_PROVIDER": "vk",
    "VK_ACCESS_TOKEN": "your_vk_community_token",
    "VK_PEER_ID": "2000000001",
    "VK_API_VERSION": "5.199",
})
```

Важно:

- `test_connection()` — это diagnostic capability;
- production send-path не обязан делать отдельный preflight перед отправкой.

---

## Runtime Semantics

### Telegram

- production send-path не делает обязательный `getMe` preflight;
- локальная валидация token/chat_id выполняется в `connect()`;
- retry/backoff применяется вокруг реальной отправки.

### VK

- production send-path не делает обязательный отдельный preflight;
- локальная валидация token/peer_id выполняется в `connect()`;
- реальная отправка идет через `messages.send`;
- diagnostic `test_connection()` использует VK-specific connection check отдельно.

---

## Batch Semantics

`send_batch_notifications()` поддерживается для обоих backend'ов через общий contract.

На текущем этапе batch path:

- не использует специальный VK batch API;
- выполняет последовательные single-send операции через общий notifier lifecycle.

Это осознанный baseline на correctness/reliability, а не на transport-level optimization.

---

## Что Должен Делать Внешний Слой Проекта

Доменные скрипты или config layers должны:

1. поднять secrets из внешнего источника;
2. выбрать provider;
3. собрать `connection_params`;
4. передать их в notifications microservice.

Например:

- `config/base_config.py` может загружать `.env\\secrets.env`;
- `reports_paths.py` и `cameras_paths.py` могут выбирать test/prod credentials;
- но сами `scheduler_runner/utils/notifications/*` этого делать не должны.

---

## Smoke / Verification Entry Points

Текущие полезные entrypoints:

- reports Telegram smoke:
  - `scheduler_runner/tasks/reports/tests/run_notification_e2e_smoke.py`

- opening Telegram smoke:
  - `scheduler_runner/tasks/cameras/tests/run_opening_notification_e2e_smoke.py`

- temporary raw VK feasibility smoke:
  - `scheduler_runner/tasks/cameras/tests/run_vk_notification_e2e_smoke.py`

- production-style VK smoke через общий notifications interface:
  - `scheduler_runner/utils/notifications/tests/run_vk_notification_interface_smoke.py`

---

## Преимущества Текущей Архитектуры

1. Изоляция transport-layer от внешней конфигурации.
2. Один public contract для Telegram и VK.
3. Расширяемость под новые backend'ы.
4. Отдельная diagnostic capability через `test_connection()`.
5. Возможность rollout новых transport'ов без переписывания доменной бизнес-логики.
