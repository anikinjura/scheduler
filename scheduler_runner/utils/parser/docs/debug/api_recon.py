"""
Разведка API Турбо ПВЗ: какие fetch/XHR делает страница и в каком ответе лежат нужные значения.

Браузер поднимается через инфраструктуру парсера (snapshot профиля + обратная запись сессии),
в страницу до загрузки встраивается перехватчик fetch/XMLHttpRequest. Скрипт открывает страницы,
которые использует парсер, и ищет в JSON-ответах эталонные значения (--expect), полученные DOM-парсером.

Режимы:
  pages   — страницы шагов парсера за дату (выдача, архив перевозок Direct/Return, страницы перевозок --carriage)
  switch  — переключение ПВЗ через UI (--switch-to, затем обратно) и перехват запросов при переключении

Запуск из корня проекта (закрывает Edge текущего пользователя, как и парсер):
    python scheduler_runner/utils/parser/docs/debug/api_recon.py pages --pvz ЧЕБОКСАРЫ_144 --date 2026-09-28 --expect 508 89 24 141445739 141443498 --carriage 141445739 141443498
    python scheduler_runner/utils/parser/docs/debug/api_recon.py switch --pvz ЧЕБОКСАРЫ_144 --switch-to ЧЕБОКСАРЫ_143

Безопасность: заголовки запросов не сохраняются; тела ответов пишутся только в файл во временной папке,
который удаляется после вывода сводки (--keep оставляет его). В ответах есть действующие токены (select-v2),
логины сотрудников и номера заказов — не коммитить и не пересылать захват.
"""
import argparse
import json
import sys
import tempfile
import time
from copy import deepcopy
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))  # корень проекта
_cli_argv, sys.argv = sys.argv, sys.argv[:1]  # BaseReportParser разбирает sys.argv при создании

from scheduler_runner.utils.parser.configs.implementations.multi_step_ozon_config import MULTI_STEP_OZON_CONFIG  # noqa: E402
from scheduler_runner.utils.parser.implementations.multi_step_ozon_parser import MultiStepOzonParser  # noqa: E402
from scheduler_runner.utils.parser.parser_invocation import apply_pvz_to_parser_config, create_parser_logger  # noqa: E402

BASE = "https://turbo-pvz.ozon.ru"
# Фоновые опросы SPA, не относящиеся к данным отчетов — скрываются в сводке
NOISE = ("unread", "/count", "informers", "lms-widget", "GetBadges", "canary", "Offer/status", "cctv", "store-security",
         "envs.json", "_nuxt", "cdns.ozon.ru", "get-entrance-point-data", "pick-up-points/details", "Sessions?", "survey",
         "news/news", "GetClaimsCount")

# Записи хранятся в sessionStorage вкладки: переживают перезагрузку страницы (например, после переключения ПВЗ)
HOOK = r"""
(() => {
  if (window.__capInstalled) return; window.__capInstalled = true;
  const LIM = 200000;
  const save = (rec) => { try { const a = JSON.parse(sessionStorage.getItem('__cap') || '[]'); a.push(rec);
                               sessionStorage.setItem('__cap', JSON.stringify(a)); } catch (e) {} };
  const of = window.fetch;
  window.fetch = async function(input, init) {
    const url = typeof input === 'string' ? input : (input && input.url) || String(input);
    const rec = {kind: 'fetch', url, method: (init && init.method) || (input && input.method) || 'GET',
                 reqBody: init && typeof init.body === 'string' ? init.body.slice(0, 4000) : null, page: location.pathname, t: Date.now()};
    const resp = await of.apply(this, arguments);
    try { rec.status = resp.status; rec.resp = (await resp.clone().text()).slice(0, LIM); } catch (e) { rec.err = String(e); }
    save(rec); return resp;
  };
  const P = XMLHttpRequest.prototype, oo = P.open, os = P.send;
  P.open = function(m, u) { this.__rec = {kind: 'xhr', method: m, url: String(u), page: location.pathname, t: Date.now()}; return oo.apply(this, arguments); };
  P.send = function(b) {
    const rec = this.__rec;
    if (rec) { rec.reqBody = typeof b === 'string' ? b.slice(0, 4000) : null;
      this.addEventListener('loadend', () => { rec.status = this.status;
        try { rec.resp = ((this.responseType === '' || this.responseType === 'text') ? this.responseText : JSON.stringify(this.response) || '').slice(0, LIM); } catch (e) {}
        save(rec); }); }
    return os.apply(this, arguments);
  };
})();
"""


def build_pages(date, carriages):
    def f(obj):
        return quote(json.dumps(obj, separators=(",", ":")), safe='{}:,[]"')
    pages = [
        ("giveout", f"{BASE}/reports/giveout?filter=" + f({"startDate": f"{date}T00:00+03:00", "endDate": f"{date}T23:59+03:00", "operationTypes": ["GiveoutAll"]})),
        ("archive_direct", f"{BASE}/outbound/carriages-archive?filter=" + f({"startSentMoment": f"{date}T00:00:00+03:00", "endSentMoment": f"{date}T23:59:59+03:00", "flowType": "Direct"})),
        ("archive_return", f"{BASE}/outbound/carriages-archive?filter=" + f({"startSentMoment": f"{date}T00:00:00+03:00", "endSentMoment": f"{date}T23:59:59+03:00", "flowType": "Return"})),
    ]
    for carriage_id in carriages:
        pages.append((f"carriage_{carriage_id}", f"{BASE}/outbound/carriages-archive/{carriage_id}?filter=" + f({"articleState": "Took", "articleType": "ArticlePosting"})))
    return pages


def read_records(driver):
    return driver.execute_script("return JSON.parse(sessionStorage.getItem('__cap') || '[]')") or []


def find_values(obj, targets, path="$"):
    hits = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            hits += find_values(v, targets, f"{path}.{k}")
    elif isinstance(obj, list):
        for i, v in enumerate(obj[:200]):
            hits += find_values(v, targets, f"{path}[{i}]")
    elif not isinstance(obj, bool) and str(obj) in targets:
        hits.append(path)
    return hits


def shape(obj, depth=0):
    if isinstance(obj, dict):
        return "{" + ", ".join(f"{k}: {shape(v, depth + 1) if depth < 2 else type(v).__name__}" for k, v in list(obj.items())[:12]) + "}"
    if isinstance(obj, list):
        return f"[{len(obj)}x {shape(obj[0], depth + 1) if obj and depth < 2 else ''}]"
    return type(obj).__name__


def print_summary(phases, expect, verbose):
    targets = {str(v) for v in expect}
    for phase in phases:
        print(f"\n===== {phase['name']}  url={urlsplit(phase['url']).path}  запросов={len(phase['records'])}"
              + (f"  {phase['note']}" if phase.get("note") else ""))
        for rec in phase["records"]:
            if not verbose and any(n in rec["url"] for n in NOISE):
                continue
            u = urlsplit(rec["url"])
            hits, desc = [], ""
            try:
                data = json.loads(rec.get("resp") or "")
                hits = find_values(data, targets) if targets else []
                desc = shape(data)
            except (ValueError, TypeError):
                desc = f"(не JSON, {len(rec.get('resp') or '')} симв.)"
            mark = "  <<< " + ", ".join(hits[:4]) if hits else ""
            print(f"  {rec['method']:5} {rec.get('status')} {u.netloc if u.netloc != 'turbo-pvz.ozon.ru' else ''}{u.path}{mark}")
            if u.query:
                print(f"        query: {unquote(u.query)[:300]}")
            if rec.get("reqBody"):
                body = rec["reqBody"]
                print(f"        body : {'(содержит данные пользователя, см. файл захвата)' if 'userName' in body else body[:200]}")
            print(f"        resp : {desc[:300]}")


def run(args):
    logger = create_parser_logger()
    config = apply_pvz_to_parser_config(deepcopy(MULTI_STEP_OZON_CONFIG), args.pvz)
    parser = MultiStepOzonParser(config, logger=logger)
    phases = []
    try:
        if not parser.setup_browser():
            print("Браузер не запустился — см. logs/reports_domain/Parser/")
            return phases
        driver = parser.driver
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": HOOK})
        driver.set_page_load_timeout(60)

        if args.mode == "pages":
            for name, url in build_pages(args.date, args.carriage):
                try:
                    driver.get(url)
                except Exception as exc:
                    print(f"[{name}] навигация: {type(exc).__name__}")
                time.sleep(args.wait)
                records = read_records(driver)
                driver.execute_script("sessionStorage.setItem('__cap', '[]')")
                phases.append({"name": name, "url": driver.current_url, "records": records})
                if "/login" in driver.current_url:
                    print("Перенаправление на /login — сессия недействительна, прерываем")
                    break
        else:
            driver.get(f"{BASE}/orders")
            time.sleep(args.wait)
            start_pvz = parser.get_current_pvz()
            for target in (args.switch_to, start_pvz):
                driver.execute_script("sessionStorage.setItem('__cap', '[]')")
                ok = parser.set_pvz(target)
                time.sleep(args.wait)
                phases.append({"name": f"switch_to_{target}", "url": driver.current_url, "records": read_records(driver),
                               "note": f"set_pvz={ok}, UI после={parser.get_current_pvz()}"})
                if "/login" in driver.current_url:
                    print("Перенаправление на /login — сессия недействительна, прерываем")
                    break
    finally:
        parser.close_browser()  # закрытие + обратная запись сессии в профиль + удаление snapshot
    return phases


def main():
    cli = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    cli.add_argument("mode", choices=["pages", "switch"])
    cli.add_argument("--pvz", required=True, help="ПВЗ, с которым работает парсер (additional_params.location_id)")
    cli.add_argument("--date", help="дата отчетов YYYY-MM-DD (режим pages)")
    cli.add_argument("--carriage", nargs="*", default=[], help="id перевозок для страниц перевозок (режим pages)")
    cli.add_argument("--expect", nargs="*", default=[], help="эталонные значения для поиска в ответах")
    cli.add_argument("--switch-to", help="ПВЗ для переключения (режим switch)")
    cli.add_argument("--wait", type=int, default=10, help="секунд ожидания запросов после загрузки страницы")
    cli.add_argument("--verbose", action="store_true", help="показывать и фоновые запросы SPA")
    cli.add_argument("--keep", action="store_true", help="не удалять файл захвата")
    args = cli.parse_args(_cli_argv[1:])
    if args.mode == "pages" and not args.date:
        cli.error("для режима pages нужен --date")
    if args.mode == "switch" and not args.switch_to:
        cli.error("для режима switch нужен --switch-to")

    phases = run(args)
    out = Path(tempfile.gettempdir()) / f"api_recon_{args.mode}_{int(time.time())}.json"
    out.write_text(json.dumps(phases, ensure_ascii=False, indent=1), encoding="utf-8")
    print_summary(phases, args.expect, args.verbose)
    if args.keep:
        print(f"\nЗахват сохранен: {out}  (содержит токены и персональные данные — удалите после анализа)")
    else:
        out.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
