/**
 * health_watchdog.gs — проверка «кто из ПВЗ молчит» (docs/HEALTH_MONITOR_PLAN.md, раздел 5).
 *
 * Работает в проекте Apps Script книги (тот же проект, что failover), по расписанию раз в день, независимо от
 * компьютеров ПВЗ: мертвый компьютер или неработающий scheduler о себе не сообщит.
 *
 * Что проверяет для каждого активного ПВЗ из листа PVZ_REGISTRY:
 *   - лист PVZ_HEALTH: последний сбор (collected_at) старше HEALTH_MAX_AGE_HOURS → «нет данных о состоянии»;
 *   - лист KPI: последняя дата отчета старше KPI_MAX_AGE_DAYS дней → «нет отчетов».
 * Сообщает в ВК только изменения: новое «молчание», повтор раз в REMINDER_DAYS дней, «снова на связи».
 *
 * Установка (один раз):
 *   1. Создать файл health_watchdog.gs в редакторе Apps Script книги и вставить этот код.
 *   2. Настройки проекта → Свойства скрипта: VK_ACCESS_TOKEN, VK_PEER_ID (те же значения, что VK_ACCESS_TOKEN_PROD /
 *      VK_PEER_ID_PROD в .env/secrets.env на ПВЗ), VK_API_VERSION (5.199).
 *   3. Выполнить setupHealthWatchdog — создаст лист PVZ_REGISTRY (если его нет) и ежедневный триггер.
 *   4. Выполнить dryRunHealthWatchdog — проверка без отправки, результат в «Журнале выполнения».
 * Часовой пояс проекта — Europe/Moscow (Настройки проекта), иначе время в сообщениях и триггер сдвинутся.
 */

const HW_SETTINGS = {
  REGISTRY_SHEET: 'PVZ_REGISTRY',
  HEALTH_SHEET: 'PVZ_HEALTH',
  KPI_SHEET: 'KPI',
  HEALTH_MAX_AGE_HOURS: 26,   // сбор раз в день в 13:10 + запас
  KPI_MAX_AGE_DAYS: 2,        // отчет за вчера приходит вечером; 2 дня без отчета — failover тоже не помог
  REMINDER_DAYS: 3,
  TRIGGER_HOUR: 15,           // после сбора 13:10 (+ пауза до минуты)
  STATE_PROPERTY: 'HEALTH_WATCHDOG_STATE',
};

const HW_REGISTRY_HEADERS = ['pvz_id', 'active', 'expect_health', 'expect_kpi', 'notes'];
const HW_REGISTRY_INITIAL = [
  ['ЧЕБОКСАРЫ_144', true, true, true, ''],
  ['ЧЕБОКСАРЫ_143', true, true, true, ''],
  ['ЧЕБОКСАРЫ_182', true, true, true, ''],
  ['ЧЕБОКСАРЫ_340', false, true, true, 'уточнить: отчетов KPI нет с мая 2026'],
  ['СОСНОВКА_10', false, false, false, 'пункт закрыт с 12.09.2026'],
];

/** Ежедневный запуск по триггеру. */
function checkSilentPvz() {
  return hwRunHealthWatchdog_(false);
}

/** Проверка без отправки и без изменения состояния — результат в журнале выполнения. */
function dryRunHealthWatchdog() {
  return hwRunHealthWatchdog_(true);
}

/** Один раз: лист PVZ_REGISTRY (если нет) и ежедневный триггер checkSilentPvz. */
function setupHealthWatchdog() {
  const book = SpreadsheetApp.getActiveSpreadsheet();
  if (!book.getSheetByName(HW_SETTINGS.REGISTRY_SHEET)) {
    const sheet = book.insertSheet(HW_SETTINGS.REGISTRY_SHEET);
    sheet.getRange(1, 1, 1, HW_REGISTRY_HEADERS.length).setValues([HW_REGISTRY_HEADERS]);
    sheet.getRange(2, 1, HW_REGISTRY_INITIAL.length, HW_REGISTRY_HEADERS.length).setValues(HW_REGISTRY_INITIAL);
    sheet.setFrozenRows(1);
    Logger.log('Создан лист ' + HW_SETTINGS.REGISTRY_SHEET);
  }
  ScriptApp.getProjectTriggers()
    .filter((t) => t.getHandlerFunction() === 'checkSilentPvz')
    .forEach((t) => ScriptApp.deleteTrigger(t));
  ScriptApp.newTrigger('checkSilentPvz').timeBased().everyDays(1).atHour(HW_SETTINGS.TRIGGER_HOUR).create();
  Logger.log('Триггер checkSilentPvz: ежедневно около ' + HW_SETTINGS.TRIGGER_HOUR + ':00 (' + Session.getScriptTimeZone() + ')');
}

function hwRunHealthWatchdog_(dryRun) {
  const book = SpreadsheetApp.getActiveSpreadsheet();
  const now = new Date();
  const registry = hwReadRegistry_(book);
  const lastHealth = hwLastTimeBy_(book, HW_SETTINGS.HEALTH_SHEET, 'pvz_id', ['collected_at', 'work_date'], now);
  const lastKpi = hwLastTimeBy_(book, HW_SETTINGS.KPI_SHEET, 'object_name', ['work_date'], now);

  const problems = hwEvaluateSilence_(registry, lastHealth, lastKpi, now);
  const props = PropertiesService.getScriptProperties();
  const previous = JSON.parse(props.getProperty(HW_SETTINGS.STATE_PROPERTY) || '{}');
  const decision = hwDecideNotifications_(problems, previous, now);

  Logger.log('Молчат: ' + (problems.map((p) => p.key).join(', ') || 'никто') +
    '; новое: ' + decision.fresh.length + ', напоминание: ' + decision.reminders.length +
    ', снова на связи: ' + decision.recovered.length);
  if (!decision.fresh.length && !decision.reminders.length && !decision.recovered.length) {
    if (!dryRun) props.setProperty(HW_SETTINGS.STATE_PROPERTY, JSON.stringify(decision.state));
    return 'изменений нет';
  }
  const message = hwFormatWatchdogMessage_(decision);
  if (dryRun) {
    Logger.log('Пробный запуск, сообщение НЕ отправлено:\n' + message);
    return message;
  }
  hwSendVk_(message);
  props.setProperty(HW_SETTINGS.STATE_PROPERTY, JSON.stringify(decision.state));
  return message;
}

function hwReadRegistry_(book) {
  const sheet = book.getSheetByName(HW_SETTINGS.REGISTRY_SHEET);
  if (!sheet) throw new Error('Нет листа ' + HW_SETTINGS.REGISTRY_SHEET + ': выполните setupHealthWatchdog');
  const values = sheet.getDataRange().getValues();
  const col = hwIndexByHeader_(values[0]);
  return values.slice(1)
    .filter((r) => String(r[col.pvz_id] || '').trim())
    .map((r) => ({
      pvzId: String(r[col.pvz_id]).trim(),
      active: hwAsBool_(r[col.active]),
      expectHealth: hwAsBool_(r[col.expect_health]),
      expectKpi: hwAsBool_(r[col.expect_kpi]),
    }));
}

/** Самое позднее время по каждому ПВЗ: первая из колонок timeColumns, где есть значение.
 *  Даты из будущего не учитываются: в KPI есть тестовые строки 30.12.2099 — они скрывали бы молчание ПВЗ. */
function hwLastTimeBy_(book, sheetName, idColumn, timeColumns, now) {
  const latestAllowed = new Date(now.getTime() + 24 * 3600 * 1000);
  const result = {};
  const sheet = book.getSheetByName(sheetName);
  if (!sheet) return result;
  const values = sheet.getDataRange().getValues();
  if (values.length < 2) return result;
  const col = hwIndexByHeader_(values[0]);
  if (col[idColumn] === undefined) return result;
  values.slice(1).forEach((r) => {
    const id = String(r[col[idColumn]] || '').trim();
    if (!id) return;
    let time = null;
    for (const name of timeColumns) {
      if (col[name] !== undefined) {
        time = hwAsDate_(r[col[name]]);
        if (time) break;
      }
    }
    if (time && time <= latestAllowed && (!result[id] || time > result[id])) result[id] = time;
  });
  return result;
}

/** Список проблем: {key, pvzId, kind, since, text}. Чистая логика — без обращений к листам. */
function hwEvaluateSilence_(registry, lastHealth, lastKpi, now) {
  const problems = [];
  const hour = 3600 * 1000;
  const today = hwStartOfDay_(now);
  registry.filter((p) => p.active).forEach((p) => {
    if (p.expectHealth) {
      const last = lastHealth[p.pvzId];
      if (!last || now - last > HW_SETTINGS.HEALTH_MAX_AGE_HOURS * hour) {
        problems.push({
          key: p.pvzId + '|health', pvzId: p.pvzId, kind: 'health', since: last ? last.getTime() : null,
          text: p.pvzId + ': нет данных о состоянии компьютера ' +
            (last ? 'с ' + hwFmt_(last, 'dd.MM HH:mm') + ' (' + Math.floor((now - last) / hour) + ' ч)' : '— ни одного сбора') +
            ' — компьютер выключен, scheduler не работает или нет интернета',
        });
      }
    }
    if (p.expectKpi) {
      const last = lastKpi[p.pvzId];
      const ageDays = last ? Math.round((today - hwStartOfDay_(last)) / (24 * hour)) : null;
      if (!last || ageDays > HW_SETTINGS.KPI_MAX_AGE_DAYS) {
        problems.push({
          key: p.pvzId + '|kpi', pvzId: p.pvzId, kind: 'kpi', since: last ? last.getTime() : null,
          text: p.pvzId + ': нет отчетов KPI ' + (last ? 'после ' + hwFmt_(last, 'dd.MM.yyyy') + ' (' + ageDays + ' дн.)' : '— ни одного') +
            ' — сбор отчетов не работает, и помощник (failover) не восстановил',
        });
      }
    }
  });
  return problems;
}

/** Что сообщать: новое, напоминание раз в REMINDER_DAYS, снова на связи. state — сохранить после отправки. */
function hwDecideNotifications_(problems, previous, now) {
  const day = 24 * 3600 * 1000;
  const state = {};
  const fresh = [];
  const reminders = [];
  problems.forEach((p) => {
    const before = previous[p.key];
    const record = { text: p.text, firstSeen: before ? before.firstSeen : now.getTime(), lastNotified: now.getTime() };
    if (!before) {
      fresh.push(p);
    } else if (now.getTime() - before.lastNotified >= HW_SETTINGS.REMINDER_DAYS * day) {
      reminders.push(p);
    } else {
      record.lastNotified = before.lastNotified;
    }
    state[p.key] = record;
  });
  const recovered = Object.keys(previous).filter((key) => !state[key]).map((key) => ({ key: key, text: previous[key].text }));
  return { fresh: fresh, reminders: reminders, recovered: recovered, state: state };
}

function hwFormatWatchdogMessage_(decision) {
  const lines = ['🔕 Мониторинг ПВЗ (проверка в книге)'];
  if (decision.fresh.length) {
    lines.push('Молчат:');
    decision.fresh.forEach((p) => lines.push('• ' + p.text));
  }
  if (decision.reminders.length) {
    lines.push('Все еще молчат:');
    decision.reminders.forEach((p) => lines.push('• ' + p.text));
  }
  if (decision.recovered.length) {
    lines.push('Снова на связи:');
    decision.recovered.forEach((r) => lines.push('• ' + r.key.replace('|health', ' — данные о состоянии').replace('|kpi', ' — отчеты KPI')));
  }
  return lines.join('\n');
}

function hwSendVk_(message) {
  const props = PropertiesService.getScriptProperties();
  const token = props.getProperty('VK_ACCESS_TOKEN');
  const peerId = props.getProperty('VK_PEER_ID');
  if (!token || !peerId) throw new Error('Свойства скрипта VK_ACCESS_TOKEN / VK_PEER_ID не заданы');
  const response = UrlFetchApp.fetch('https://api.vk.com/method/messages.send', {
    method: 'post',
    muteHttpExceptions: true,
    payload: {
      access_token: token,
      peer_id: peerId,
      random_id: String(Math.floor(Math.random() * 2147483647)),
      message: message,
      v: props.getProperty('VK_API_VERSION') || '5.199',
    },
  });
  const body = JSON.parse(response.getContentText() || '{}');
  if (body.error) throw new Error('ВК: ' + body.error.error_code + ' ' + body.error.error_msg);
  return body.response;
}

function hwIndexByHeader_(headers) {
  const col = {};
  headers.forEach((h, i) => { col[String(h).trim()] = i; });
  return col;
}

function hwAsBool_(value) {
  if (typeof value === 'boolean') return value;
  return ['true', 'да', '1', 'yes'].indexOf(String(value).trim().toLowerCase()) >= 0;
}

/** Ячейка → Date: значение даты/времени листа, 'YYYY-MM-DD[ HH:MM:SS]' или 'DD.MM.YYYY'. */
function hwAsDate_(value) {
  if (Object.prototype.toString.call(value) === '[object Date]' && !isNaN(value)) return value;
  const s = String(value || '').trim();
  let m = s.match(/^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2})(?::(\d{2}))?)?/);
  if (m) return new Date(+m[1], +m[2] - 1, +m[3], +(m[4] || 0), +(m[5] || 0), +(m[6] || 0));
  m = s.match(/^(\d{2})\.(\d{2})\.(\d{4})/);
  if (m) return new Date(+m[3], +m[2] - 1, +m[1]);
  return null;
}

function hwStartOfDay_(date) {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate());
}

function hwFmt_(date, pattern) {
  return Utilities.formatDate(date, Session.getScriptTimeZone(), pattern);
}
