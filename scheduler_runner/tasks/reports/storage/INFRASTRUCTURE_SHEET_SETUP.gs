/**
 * INFRASTRUCTURE_SHEET_SETUP.gs
 *
 * Запусти этот скрипт в новой пустой Google-таблице.
 * Он создаст 3 листа с правильной структурой, заголовками,
 * формулами и именованными диапазонами.
 *
 * Как запустить:
 *   1. Открой новую Google-таблицу
 *   2. Расширения → Apps Script
 *   3. Вставь этот код в редактор
 *   4. Выбери функцию setupTestSpreadsheet и нажми "Выполнить"
 *   5. Дай разрешения при запросе
 *
 * ВАЖНО: После запуска этого скрипта нужно также добавить:
 *   - CustomFormulas.gs (из INFRASTRUCTURE_CustomFormulas.gs)
 *   - failover_apps_script_try_claim.gs (из INFRASTRUCTURE_failover_apps_script_try_claim.gs)
 *   - Развернуть как Web App (Расширения → Apps Script → Развёртывание)
 *   - В свойствах скрипта добавить FAILOVER_SHARED_SECRET
 */

function setupTestSpreadsheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  // Переименовываем дефолтный лист вместо удаления (Google не разрешает удалить все листы)
  const defaultSheet = ss.getSheetByName('Лист1');
  if (defaultSheet) {
    defaultSheet.setName('__TO_DELETE__');
  }

  createKpiSheet(ss);
  createFailoverStateSheet(ss);
  createKpiRewardRulesSheet(ss);

  // Создаём именованный диапазон
  ss.setNamedRange('KPI_REWARD_RULES_RANGE', 
    ss.getRange('KPI_REWARD_RULES!B2:E999'));

  // Теперь удаляем временный лист (когда есть другие листы — это разрешено)
  const tempSheet = ss.getSheetByName('__TO_DELETE__');
  if (tempSheet) {
    ss.deleteSheet(tempSheet);
  }

  Logger.log('✅ Тестовая таблица успешно настроена!');
  Logger.log('Листы созданы: KPI, KPI_FAILOVER_STATE, KPI_REWARD_RULES');
  Logger.log('Именованный диапазон: KPI_REWARD_RULES_RANGE');
}


// ============================================================
// Лист 1: KPI
// ============================================================
function createKpiSheet(ss) {
  const sheet = ss.insertSheet('KPI');

  // Заголовки (A-K)
  const headers = [
    'id',
    'work_date',
    'object_name',
    'issued_packages',
    'direct_flow',
    'return_flow',
    'reward_issued_packages',
    'reward_direct_flow',
    'reward_return_flow',
    'total_reward',
    'timestamp'
  ];

  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);

  // Форматирование заголовков
  const headerRange = sheet.getRange(1, 1, 1, headers.length);
  headerRange.setFontWeight('bold');
  headerRange.setBackground('#4A86E8');
  headerRange.setFontColor('#FFFFFF');
  headerRange.setHorizontalAlignment('center');

  // Формулы НЕ протягиваются — Python (uploader) подставляет их при каждой записи.
  // id: =B{row}&C{row}
  // reward_issued_packages: =GET_REWARD("issued_packages";D{row};$B{row};KPI_REWARD_RULES_RANGE)
  // reward_direct_flow: =GET_REWARD("direct_flow";E{row};$B{row};KPI_REWARD_RULES_RANGE)
  // reward_return_flow: =GET_REWARD("return_flow";F{row};$B{row};KPI_REWARD_RULES_RANGE)
  // total_reward: =SUM(G{row}:I{row})

  // Форматирование колонок
  sheet.setColumnWidth(2, 120); // work_date
  sheet.setColumnWidth(3, 100); // object_name
  sheet.setColumnWidth(4, 130); // issued_packages
  sheet.setColumnWidth(5, 110); // direct_flow
  sheet.setColumnWidth(6, 130); // return_flow
  sheet.setColumnWidth(7, 160); // reward_issued_packages
  sheet.setColumnWidth(8, 150); // reward_direct_flow
  sheet.setColumnWidth(9, 160); // reward_return_flow
  sheet.setColumnWidth(10, 130); // total_reward

  // Заморозка заголовка
  sheet.setFrozenRows(1);

  Logger.log('✅ Лист KPI создан (заголовки + форматирование, формулы — из Python)');
}


// ============================================================
// Лист 2: KPI_FAILOVER_STATE
// ============================================================
function createFailoverStateSheet(ss) {
  const sheet = ss.insertSheet('KPI_FAILOVER_STATE');

  // Заголовки (A-L): request_id первым, без id-формулы
  const headers = [
    'request_id',
    'work_date',
    'target_object_name',
    'owner_object_name',
    'status',
    'claimed_by',
    'claim_expires_at',
    'last_error',
    'attempt_no',
    'source_run_id',
    'updated_at',
    'timestamp'
  ];

  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);

  // Форматирование заголовков
  const headerRange = sheet.getRange(1, 1, 1, headers.length);
  headerRange.setFontWeight('bold');
  headerRange.setBackground('#E06666');
  headerRange.setFontColor('#FFFFFF');
  headerRange.setHorizontalAlignment('center');

  // Формулы НЕ протягиваются — Python (uploader) подставляет их при каждой записи.

  // Заморозка заголовка
  sheet.setFrozenRows(1);

  Logger.log('✅ Лист KPI_FAILOVER_STATE создан (заголовки + форматирование, формулы — из Python)');
}


// ============================================================
// Лист 3: KPI_REWARD_RULES
// ============================================================
function createKpiRewardRulesSheet(ss) {
  const sheet = ss.insertSheet('KPI_REWARD_RULES');

  // Заголовки (A-E): id + 4 поля данных
  // Named Range KPI_REWARD_RULES_RANGE = B2:E999 (без id)
  const headers = [
    'id',
    'kpi_type',
    'param_value',
    'reward_amount',
    'effective_date'
  ];

  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);

  // Форматирование заголовков
  const headerRange = sheet.getRange(1, 1, 1, headers.length);
  headerRange.setFontWeight('bold');
  headerRange.setBackground('#93C47D');
  headerRange.setFontColor('#000000');
  headerRange.setHorizontalAlignment('center');

  // Формулы НЕ протягиваются — Python заполняет данные напрямую.

  // Примеры данных (пороговые значения) — колонки B-E
  const sampleData = [
    ['issued_packages', 100, 5000, '2026-04-01'],
    ['issued_packages', 200, 10000, '2026-04-01'],
    ['issued_packages', 300, 15000, '2026-04-01'],
    ['direct_flow', 50, 3000, '2026-04-01'],
    ['direct_flow', 100, 6000, '2026-04-01'],
    ['direct_flow', 150, 9000, '2026-04-01'],
    ['return_flow', 10, 1000, '2026-04-01'],
    ['return_flow', 20, 2000, '2026-04-01'],
    ['return_flow', 30, 3000, '2026-04-01'],
  ];

  if (sampleData.length > 0) {
    sheet.getRange(2, 2, sampleData.length, 4)
      .setValues(sampleData);
  }

  // Форматирование дат
  sheet.getRange(2, 5, sampleData.length, 1)
    .setNumberFormat('yyyy-MM-dd');

  // Форматирование чисел
  sheet.getRange(2, 3, sampleData.length, 1)
    .setNumberFormat('#,##0');
  sheet.getRange(2, 4, sampleData.length, 1)
    .setNumberFormat('#,##0.00');

  // Заморозка заголовка
  sheet.setFrozenRows(1);

  Logger.log('✅ Лист KPI_REWARD_RULES создан с примерами данных');
}
