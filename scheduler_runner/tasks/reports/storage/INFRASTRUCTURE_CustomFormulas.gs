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