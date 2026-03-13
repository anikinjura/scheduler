# Reports

РџРѕРґРґРѕРјРµРЅ `scheduler_runner.tasks.reports` РѕС‚РІРµС‡Р°РµС‚ Р·Р° РїРѕР»СѓС‡РµРЅРёРµ РѕС‚С‡РµС‚РѕРІ Ozon РџР’Р—,
РїРѕРґРіРѕС‚РѕРІРєСѓ РґР°РЅРЅС‹С… Рє Р·Р°РіСЂСѓР·РєРµ РІ Google Sheets Рё РѕС‚РїСЂР°РІРєСѓ РёС‚РѕРіРѕРІС‹С… СѓРІРµРґРѕРјР»РµРЅРёР№.

## РўРµРєСѓС‰РёР№ СЃС†РµРЅР°СЂРёР№ РІС‹РїРѕР»РЅРµРЅРёСЏ

РћСЃРЅРѕРІРЅР°СЏ С‚РѕС‡РєР° РІС…РѕРґР°: [reports_processor.py](/C:/tools/scheduler/scheduler_runner/tasks/reports/reports_processor.py).

РџСЂРѕС†РµСЃСЃРѕСЂ РїРѕРґРґРµСЂР¶РёРІР°РµС‚ РґРІР° СЂРµР¶РёРјР°:

- `single` - СЃС‚Р°СЂС‹Р№ СЂРµР¶РёРј РѕР±СЂР°Р±РѕС‚РєРё РѕРґРЅРѕР№ РґР°С‚С‹.
- `backfill` - РЅРѕРІС‹Р№ СЂРµР¶РёРј `coverage-check -> batch parse -> batch upload -> batch notify`.

РџРѕ СѓРјРѕР»С‡Р°РЅРёСЋ РїСЂРѕС†РµСЃСЃРѕСЂ РёСЃРїРѕР»СЊР·СѓРµС‚ `backfill`, РµСЃР»Рё СЏРІРЅРѕ РЅРµ РїРµСЂРµРґР°РЅР° `execution_date`.

## Backfill flow

РќРѕРІС‹Р№ СЃС†РµРЅР°СЂРёР№ СЂР°Р±РѕС‚Р°РµС‚ С‚Р°Рє:

1. Р¤РѕСЂРјРёСЂСѓРµС‚СЃСЏ РґРёР°РїР°Р·РѕРЅ РґР°С‚ РґР»СЏ РїСЂРѕРІРµСЂРєРё РїРѕРєСЂС‹С‚РёСЏ.
2. Р§РµСЂРµР· `scheduler_runner.utils.uploader.check_missing_items(...)` РѕРїСЂРµРґРµР»СЏРµС‚СЃСЏ,
   РєР°РєРёС… Р·Р°РїРёСЃРµР№ РЅРµ С…РІР°С‚Р°РµС‚ РІ Google Sheets РґР»СЏ РїР°СЂС‹ РєР»СЋС‡РµР№ `Р”Р°С‚Р° + РџР’Р—`.
3. Р•СЃР»Рё РїСЂРѕРїСѓСЃРєРѕРІ РЅРµС‚, РїР°СЂСЃРµСЂ Рё Р·Р°РіСЂСѓР·С‡РёРє РЅРµ Р·Р°РїСѓСЃРєР°СЋС‚СЃСЏ.
4. Р•СЃР»Рё РїСЂРѕРїСѓСЃРєРё РµСЃС‚СЊ, РїР°СЂСЃРµСЂ РѕР±СЂР°Р±Р°С‚С‹РІР°РµС‚ РІСЃРµ РѕС‚СЃСѓС‚СЃС‚РІСѓСЋС‰РёРµ РґР°С‚С‹ РІ РѕРґРЅРѕР№ browser
   session.
5. РЈСЃРїРµС€РЅРѕ РїРѕР»СѓС‡РµРЅРЅС‹Рµ Р·Р°РїРёСЃРё Р·Р°РіСЂСѓР¶Р°СЋС‚СЃСЏ РѕРґРЅРёРј batch-РІС‹Р·РѕРІРѕРј uploader.
6. РћС‚РїСЂР°РІР»СЏРµС‚СЃСЏ Р°РіСЂРµРіРёСЂРѕРІР°РЅРЅРѕРµ СѓРІРµРґРѕРјР»РµРЅРёРµ РїРѕ РІСЃРµРјСѓ РїСЂРѕРіРѕРЅСѓ.

РўР°РєРѕР№ РїРѕРґС…РѕРґ РёСЃРєР»СЋС‡Р°РµС‚ РјРЅРѕРіРѕРєСЂР°С‚РЅС‹Р№ С†РёРєР» `open browser -> parse -> close browser`
РґР»СЏ РєР°Р¶РґРѕР№ РѕС‚СЃСѓС‚СЃС‚РІСѓСЋС‰РµР№ РґР°С‚С‹.

## РћСЃРЅРѕРІРЅС‹Рµ РјРѕРґСѓР»Рё

- [reports_processor.py](/C:/tools/scheduler/scheduler_runner/tasks/reports/reports_processor.py) -
  orchestration parsing/upload/notification.
- [scheduler_runner/utils/parser/](/C:/tools/scheduler/scheduler_runner/utils/parser/) -
  РєР°РЅРѕРЅРёС‡РµСЃРєРёР№ runtime-РїР°РєРµС‚ Selenium-РїР°СЂСЃРµСЂР°, parser facade Рё batch-РѕР±СЂР°Р±РѕС‚РєРё РґР°С‚.
- [scheduler_runner/utils/parser/docs/](/C:/tools/scheduler/scheduler_runner/utils/parser/docs/) -
  канонический docs-root parser-пакета.
- старый путь `scheduler_runner/tasks/reports/parser/` удален из основного дерева и больше не должен использоваться как runtime/docs package.
- [config/scripts/reports_processor_config.py](/C:/tools/scheduler/scheduler_runner/tasks/reports/config/scripts/reports_processor_config.py) -
  СЂР°СЃРїРёСЃР°РЅРёРµ Рё РЅР°СЃС‚СЂРѕР№РєРё backfill.
- [config/scripts/kpi_google_sheets_config.py](/C:/tools/scheduler/scheduler_runner/tasks/reports/config/scripts/kpi_google_sheets_config.py) -
  РєРѕРЅС„РёРіСѓСЂР°С†РёСЏ РїРѕРґРєР»СЋС‡РµРЅРёСЏ Рё СЃС…РµРјС‹ Google Sheets.

## CLI

РџСЂРёРјРµСЂС‹ Р·Р°РїСѓСЃРєР° РёР· РІРёСЂС‚СѓР°Р»СЊРЅРѕРіРѕ РѕРєСЂСѓР¶РµРЅРёСЏ:

```powershell
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.reports_processor --execution_date 2026-03-10
```

```powershell
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.reports_processor --backfill_days 7
```

```powershell
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.reports_processor --date_from 2026-03-04 --date_to 2026-03-10
```

## РћР¶РёРґР°РµРјРѕРµ РїРѕРІРµРґРµРЅРёРµ

- Р”Р»СЏ `single` СЂРµР¶РёРјР° Р±СЂР°СѓР·РµСЂ РѕС‚РєСЂС‹РІР°РµС‚СЃСЏ, РѕР±СЂР°Р±Р°С‚С‹РІР°РµС‚ РѕРґРЅСѓ РґР°С‚Сѓ Рё Р·Р°РєСЂС‹РІР°РµС‚СЃСЏ.
- Р”Р»СЏ `backfill` СЂРµР¶РёРјР° Р±СЂР°СѓР·РµСЂ РѕС‚РєСЂС‹РІР°РµС‚СЃСЏ РѕРґРёРЅ СЂР°Р· РЅР° РІРµСЃСЊ РЅР°Р±РѕСЂ missing dates.
- Р’ Р·Р°РіСЂСѓР·РєСѓ СѓС…РѕРґСЏС‚ С‚РѕР»СЊРєРѕ СѓСЃРїРµС€РЅРѕ СЃРїР°СЂСЃРµРЅРЅС‹Рµ РґР°С‚С‹.
- РЈРІРµРґРѕРјР»РµРЅРёРµ СЃРѕРґРµСЂР¶РёС‚ Р°РіСЂРµРіРёСЂРѕРІР°РЅРЅСѓСЋ СЃС‚Р°С‚РёСЃС‚РёРєСѓ: РґРёР°РїР°Р·РѕРЅ, missing dates,
  СѓСЃРїРµС€РЅС‹Рµ Рё РЅРµСѓСЃРїРµС€РЅС‹Рµ РґР°С‚С‹.

## РџСЂРѕРІРµСЂРєР° РёР·РјРµРЅРµРЅРёР№

Р›РѕРєР°Р»СЊРЅС‹Рµ unit-С‚РµСЃС‚С‹:

```powershell
.venv\Scripts\python.exe -m pytest scheduler_runner\utils\parser\core\tests\test_base_report_parser.py scheduler_runner\tasks\reports\tests\test_reports_processor.py -q
```

РџСЂРё РЅРµРѕР±С…РѕРґРёРјРѕСЃС‚Рё СЂРµР°Р»СЊРЅС‹Р№ smoke-check:

```powershell
.venv\Scripts\python.exe -m scheduler_runner.tasks.reports.test_coverage_check_real --pvz "Р§Р•Р‘РћРљРЎРђР Р«_340" --days 7 --json
```



