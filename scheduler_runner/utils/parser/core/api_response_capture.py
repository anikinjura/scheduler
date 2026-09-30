"""
Перехват JSON-ответов, которые получает страница (fetch / XMLHttpRequest).

Универсальный модуль без знания об Ozon: парсер открывает страницу как обычно, а значение берет не из DOM,
а из ответа API, который страница сама запросила. См. docs/MODERNIZATION_PLAN.md.

Безопасность:
- заголовки запросов (в том числе authorization) не сохраняются;
- тела ответов хранятся только в памяти страницы, в лог не пишутся — в лог только путь, статус и значение.
"""
from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlsplit


class ApiCaptureError(Exception):
    """Базовая ошибка перехвата API."""


class ApiResponseNotFound(ApiCaptureError):
    """Страница не сделала ожидаемый запрос за отведенное время."""


class ApiResponseError(ApiCaptureError):
    """Ожидаемый запрос завершился ошибкой (статус не 2xx) или ответ — не JSON."""


class ApiFieldNotFound(ApiCaptureError):
    """В ответе нет поля по указанному пути."""


# Перехватчик ставится до загрузки документа (CDP Page.addScriptToEvaluateOnNewDocument).
# Буфер — window.__apiCapture текущего документа: полная навигация создает новый документ и пустой буфер.
HOOK_TEMPLATE = r"""
(() => {
  if (window.__apiCaptureInstalled) return;
  window.__apiCaptureInstalled = true;
  window.__apiCapture = [];
  const MAX_BODY = %(max_body)d, MAX_RECORDS = %(max_records)d;
  const push = (rec) => { const a = window.__apiCapture; a.push(rec); if (a.length > MAX_RECORDS) a.shift(); };
  const originalFetch = window.fetch;
  window.fetch = async function(input, init) {
    const url = typeof input === 'string' ? input : (input && input.url) || String(input);
    const rec = {url: url, method: ((init && init.method) || (input && input.method) || 'GET').toUpperCase(),
                 status: null, body: null, t: Date.now()};
    push(rec);
    const response = await originalFetch.apply(this, arguments);
    try { rec.status = response.status; rec.body = (await response.clone().text()).slice(0, MAX_BODY); }
    catch (e) { rec.status = rec.status || -1; rec.error = String(e); }
    return response;
  };
  const P = XMLHttpRequest.prototype, open = P.open, send = P.send;
  P.open = function(method, url) {
    this.__apiRec = {url: String(url), method: String(method || 'GET').toUpperCase(), status: null, body: null, t: Date.now()};
    return open.apply(this, arguments);
  };
  P.send = function() {
    const rec = this.__apiRec;
    if (rec) {
      push(rec);
      this.addEventListener('loadend', () => {
        rec.status = this.status;
        try {
          const text = (this.responseType === '' || this.responseType === 'text') ? this.responseText : JSON.stringify(this.response);
          rec.body = (text || '').slice(0, MAX_BODY);
        } catch (e) { rec.error = String(e); }
      });
    }
    return send.apply(this, arguments);
  };
})();
"""


class ApiResponseCapture:
    """Установка перехватчика в браузер и чтение перехваченных ответов."""

    def __init__(self, wait_timeout: float = 15.0, poll_interval: float = 0.5,
                 max_records: int = 500, max_body_chars: int = 2_000_000, logger=None):
        self.wait_timeout = wait_timeout
        self.poll_interval = poll_interval
        self.max_records = max_records
        self.max_body_chars = max_body_chars
        self.logger = logger

    @classmethod
    def from_config(cls, config: Optional[Dict[str, Any]], logger=None) -> "ApiResponseCapture":
        config = config or {}
        return cls(
            wait_timeout=float(config.get("wait_timeout", 15)),
            poll_interval=float(config.get("poll_interval", 0.5)),
            max_records=int(config.get("max_records", 500)),
            max_body_chars=int(config.get("max_body_chars", 2_000_000)),
            logger=logger,
        )

    @property
    def hook_source(self) -> str:
        return HOOK_TEMPLATE % {"max_body": self.max_body_chars, "max_records": self.max_records}

    def install(self, driver) -> None:
        """Ставит перехватчик для всех следующих документов вкладки (до первой навигации)."""
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": self.hook_source})
        if self.logger:
            self.logger.info("API_CAPTURE_INSTALLED")

    @staticmethod
    def records(driver) -> List[Dict[str, Any]]:
        """Записи текущего документа: url, method, status, body, t."""
        return driver.execute_script("return window.__apiCapture || []") or []

    @staticmethod
    def matches(record: Dict[str, Any], path: str, query_contains: Optional[Dict[str, Any]] = None) -> bool:
        """Путь запроса оканчивается на path и query содержит все указанные значения (сравнение строк)."""
        parts = urlsplit(record.get("url", ""))
        if not parts.path.rstrip("/").endswith(path.rstrip("/")):
            return False
        if query_contains:
            query = parse_qs(parts.query, keep_blank_values=True)
            for key, expected in query_contains.items():
                if str(expected) not in query.get(key, []):
                    return False
        return True

    def wait_for(self, driver, path: str, query_contains: Optional[Dict[str, Any]] = None,
                 timeout: Optional[float] = None) -> Any:
        """
        Ждет завершенный ответ на запрос path (с подходящими query-параметрами) и возвращает разобранный JSON.
        Берется последний подходящий ответ документа.

        Raises:
            ApiResponseNotFound: запрос не завершился за timeout;
            ApiResponseError: статус не 2xx или тело не JSON.
        """
        deadline = time.monotonic() + (self.wait_timeout if timeout is None else timeout)
        while True:
            completed = [r for r in self.records(driver)
                         if r.get("status") is not None and self.matches(r, path, query_contains)]
            if completed:
                record = completed[-1]
                break
            if time.monotonic() >= deadline:
                raise ApiResponseNotFound(f"API-запрос {path} {query_contains or ''} не найден за {timeout or self.wait_timeout} с")
            time.sleep(self.poll_interval)

        status = record.get("status")
        if not isinstance(status, int) or not 200 <= status < 300:
            raise ApiResponseError(f"API-запрос {path} завершился статусом {status}")
        try:
            data = json.loads(record.get("body") or "")
        except ValueError as e:
            raise ApiResponseError(f"Ответ {path} не JSON (возможно, обрезан max_body_chars): {e}") from e
        if self.logger:
            self.logger.debug(f"API_RESPONSE path={path} status={status}")
        return data

    def debug_dump(self, driver) -> None:
        """API_CAPTURE_DUMP: пути и статусы перехваченных запросов (без тел и заголовков) — для поиска нового API."""
        if not self.logger:
            return
        for record in self.records(driver):
            parts = urlsplit(record.get("url", ""))
            self.logger.debug(f"API_CAPTURE_DUMP {record.get('method')} {record.get('status')} {parts.path}")


def resolve_path(data: Any, path: str) -> Any:
    """
    Значение по пути: "logV2.totalCount", "store.name", "carriages[].carriageId" (список значений из элементов списка).

    Raises:
        ApiFieldNotFound: нет поля или тип не соответствует пути.
    """
    current: Any = data
    walked: List[str] = []
    segments = [s for s in path.split(".") if s]
    for index, segment in enumerate(segments):
        walked.append(segment)
        if segment.endswith("[]"):
            key = segment[:-2]
            items = current.get(key) if isinstance(current, dict) else None
            if not isinstance(items, list):
                raise ApiFieldNotFound(f"Поле {'.'.join(walked)} не найдено или не список")
            rest = ".".join(segments[index + 1:])
            return [resolve_path(item, rest) if rest else item for item in items]
        if not isinstance(current, dict) or segment not in current:
            raise ApiFieldNotFound(f"Поле {'.'.join(walked)} не найдено в ответе")
        current = current[segment]
    return current
