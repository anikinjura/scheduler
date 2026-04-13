const FAILOVER_SHEET_NAME = 'KPI_FAILOVER_STATE';
const CLAIM_STATUSES = {
  OWNER_SUCCESS: 'owner_success',
  FAILOVER_SUCCESS: 'failover_success',
  FAILOVER_CLAIMED: 'failover_claimed',
  CLAIM_EXPIRED: 'claim_expired',
};

function doPost(e) {
  try {
    const payload = JSON.parse(e.postData.contents || '{}');
    const result = routeRequest_(payload);
    return jsonResponse_(result);
  } catch (error) {
    return jsonResponse_({
      success: false,
      error: String(error && error.message ? error.message : error),
    });
  }
}

function routeRequest_(payload) {
  const action = String(payload.action || '').trim();
  if (action !== 'try_claim_failover') {
    return {
      success: false,
      error: 'unsupported_action',
    };
  }
  validateSharedSecret_(payload);
  return tryClaimFailover_(payload);
}

function validateSharedSecret_(payload) {
  const expectedSecret = PropertiesService.getScriptProperties().getProperty('FAILOVER_SHARED_SECRET');
  if (!expectedSecret) {
    throw new Error('FAILOVER_SHARED_SECRET is not configured');
  }
  if (String(payload.shared_secret || '') !== expectedSecret) {
    throw new Error('invalid_shared_secret');
  }
}

function tryClaimFailover_(payload) {
  validateRequiredFields_(payload, [
    'execution_date',
    'target_object_name',
    'owner_object_name',
    'claimer_pvz',
    'source_run_id',
  ]);

  const lock = LockService.getScriptLock();
  lock.waitLock(10000);
  try {
    const ttlMinutes = Math.max(Number(payload.ttl_minutes || 15), 1);
    const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = spreadsheet.getSheetByName(FAILOVER_SHEET_NAME);
    if (!sheet) {
      throw new Error(`Worksheet ${FAILOVER_SHEET_NAME} not found`);
    }

    const headers = getHeaders_(sheet);
    const rowInfo = findFailoverRow_(sheet, headers, payload.execution_date, payload.target_object_name);
    if (!rowInfo) {
      return { success: false, claimed: false, reason: 'row_not_found' };
    }

    const state = rowToObject_(headers, rowInfo.values);
    const currentStatus = String(state.status || '').trim();
    const now = new Date();

    if ([CLAIM_STATUSES.OWNER_SUCCESS, CLAIM_STATUSES.FAILOVER_SUCCESS].includes(currentStatus)) {
      return { success: true, claimed: false, reason: 'already_completed', state: normalizeStateForResponse_(state) };
    }

    if (currentStatus === CLAIM_STATUSES.FAILOVER_CLAIMED && isClaimActive_(state.claim_expires_at, now)) {
      return { success: true, claimed: false, reason: 'already_claimed', state: normalizeStateForResponse_(state) };
    }

    const updatedState = {
      ...state,
      status: CLAIM_STATUSES.FAILOVER_CLAIMED,
      claimed_by: payload.claimer_pvz,
      claim_expires_at: formatSheetTimestamp_(new Date(now.getTime() + ttlMinutes * 60000)),
      source_run_id: payload.source_run_id,
      last_error: String(state.last_error || ''),
      attempt_no: Number(state.attempt_no || 0) + 1,
      updated_at: formatSheetTimestamp_(now),
    };

    writeRowObject_(sheet, headers, rowInfo.rowNumber, updatedState);
    return { success: true, claimed: true, reason: 'claimed', state: normalizeStateForResponse_(updatedState) };
  } finally {
    lock.releaseLock();
  }
}

function validateRequiredFields_(payload, fields) {
  fields.forEach((field) => {
    if (!String(payload[field] || '').trim()) {
      throw new Error(`missing_required_field:${field}`);
    }
  });
}

function getHeaders_(sheet) {
  const lastColumn = sheet.getLastColumn();
  return sheet.getRange(1, 1, 1, lastColumn).getValues()[0];
}

function findFailoverRow_(sheet, headers, executionDate, targetObjectName) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return null;

  const values = sheet.getRange(2, 1, lastRow - 1, headers.length).getValues();
  const dateIndex = headers.indexOf('work_date');
  const targetObjectNameIndex = headers.indexOf('target_object_name');

  for (let offset = 0; offset < values.length; offset += 1) {
    const rowDate = normalizeExecutionDate_(values[offset][dateIndex]);
    const rowTargetObjectName = String(values[offset][targetObjectNameIndex] || '').trim();
    if (rowDate === executionDate && rowTargetObjectName === targetObjectName) {
      return { rowNumber: offset + 2, values: values[offset] };
    }
  }
  return null;
}

function rowToObject_(headers, rowValues) {
  const obj = {};
  headers.forEach((header, index) => { obj[header] = rowValues[index]; });
  return obj;
}

function normalizeStateForResponse_(state) {
  const normalized = { ...state };
  if (Object.prototype.hasOwnProperty.call(normalized, 'work_date')) {
    normalized['work_date'] = normalizeStateDateForResponse_(normalized['work_date']);
  }
  ['claim_expires_at', 'updated_at'].forEach((field) => {
    if (Object.prototype.hasOwnProperty.call(normalized, field)) {
      normalized[field] = normalizeTimestampForResponse_(normalized[field]);
    }
  });
  return normalized;
}

function normalizeStateDateForResponse_(value) {
  if (Object.prototype.toString.call(value) === '[object Date]' && !isNaN(value)) {
    return Utilities.formatDate(value, Session.getScriptTimeZone(), 'dd.MM.yyyy');
  }
  return String(value || '').trim();
}

function normalizeTimestampForResponse_(value) {
  if (Object.prototype.toString.call(value) === '[object Date]' && !isNaN(value)) {
    return Utilities.formatDate(value, Session.getScriptTimeZone(), 'dd.MM.yyyy HH:mm:ss');
  }
  return String(value || '').trim();
}

function writeRowObject_(sheet, headers, rowNumber, rowObject) {
  const rowValues = headers.map((header) => {
    if (header === 'request_id') {
      return rowObject.request_id || buildRequestId_(rowObject['work_date'], rowObject.target_object_name);
    }
    return rowObject[header];
  });
  sheet.getRange(rowNumber, 1, 1, headers.length).setValues([rowValues]);
}

function buildRequestId_(executionDate, targetObjectName) {
  return `${executionDate}|${targetObjectName}`;
}

function normalizeExecutionDate_(value) {
  if (Object.prototype.toString.call(value) === '[object Date]' && !isNaN(value)) {
    return Utilities.formatDate(value, Session.getScriptTimeZone(), 'yyyy-MM-dd');
  }
  const stringValue = String(value || '').trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(stringValue)) return stringValue;
  if (/^\d{2}\.\d{2}\.\d{4}$/.test(stringValue)) {
    const [dd, mm, yyyy] = stringValue.split('.');
    return `${yyyy}-${mm}-${dd}`;
  }
  return stringValue;
}

function isClaimActive_(claimExpiresAt, now) {
  const parsed = parseSheetTimestamp_(claimExpiresAt);
  return !!parsed && parsed.getTime() > now.getTime();
}

function parseSheetTimestamp_(value) {
  const stringValue = String(value || '').trim();
  if (!stringValue) return null;
  if (/^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2}$/.test(stringValue)) {
    const [datePart, timePart] = stringValue.split(' ');
    const [dd, mm, yyyy] = datePart.split('.');
    return new Date(`${yyyy}-${mm}-${dd}T${timePart}`);
  }
  const parsed = new Date(stringValue);
  return isNaN(parsed) ? null : parsed;
}

function formatSheetTimestamp_(date) {
  return Utilities.formatDate(date, Session.getScriptTimeZone(), 'dd.MM.yyyy HH:mm:ss');
}

function jsonResponse_(payload) {
  return ContentService.createTextOutput(JSON.stringify(payload)).setMimeType(ContentService.MimeType.JSON);
}