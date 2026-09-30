// Проверка селекторов парсера на текущей странице turbo-pvz.ozon.ru (только чтение, ничего не нажимает).
// Вставить в консоль DevTools (F12 -> Console). Результат копируется в буфер обмена.
// SEL — копия селекторов из configs/ (ozon_report_config.py, multi_step_ozon_config.py): при их изменении обновить и здесь.
(() => {
  const SEL = {
    pvz_input:            "//input[@id='input___v-0-0']",
    pvz_input_readonly:   "//input[@id='input___v-0-0' and @readonly]",
    pvz_input_class_ro:   "//input[contains(@class, 'ozi__input__input__') and @readonly]",
    pvz_dropdown:         "//div[@data-popover-reference='true' and .//input[@id='input___v-0-0']]",
    pvz_input_select_root:"//div[contains(@class, 'ozi__input-select__root__')]",
    pvz_option_label:     "//div[contains(@class, 'ozi__data-content__label__')]",
    teleport_target:      "//*[@id='ozi-window-teleport-target']",
    found_caption:        "//div[contains(@class, 'ozi__text-view__caption-medium__') and contains(normalize-space(.), 'Найдено')]",
    carriages_table:      "//table[contains(@class, 'ozi__table__table__')]",
    carriage_number_cell: "//table//td[1]//div[contains(@class, '_carriageNumber_')]",
    overlay_dialog:       "//div[contains(@class, 'ozi__dialog__dialog__')]",
    overlay_btn_postpone: "//button[contains(@class, 'ozi__button') and normalize-space()='Отложить']",
  };
  const x = (p, ctx = document) => {
    const r = document.evaluate(p, ctx, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null);
    return Array.from({ length: r.snapshotLength }, (_, i) => r.snapshotItem(i));
  };
  const short = (el) => {
    const cls = (el.getAttribute('class') || '').trim().replace(/\s+/g, ' ');
    const id = el.id ? `#${el.id}` : '';
    const ro = el.hasAttribute && el.hasAttribute('readonly') ? ' [readonly]' : '';
    const val = el.value ? ` value="${el.value}"` : '';
    const txt = (el.innerText || '').trim().replace(/\s+/g, ' ').slice(0, 60);
    return `<${el.tagName.toLowerCase()}${id}${ro}${val} class="${cls.slice(0, 160)}">${txt ? ' «' + txt + '»' : ''}`;
  };
  const out = [];
  out.push(`URL: ${location.href}`);
  out.push(`TITLE: ${document.title}`);
  out.push(`TIME: ${new Date().toString()}`);
  out.push('', '=== СЕЛЕКТОРЫ ПАРСЕРА ===');
  for (const [k, p] of Object.entries(SEL)) {
    let n; try { n = x(p).length; } catch (e) { n = 'ERR ' + e.message; }
    out.push(`${n > 0 ? 'OK  ' : 'MISS'} ${k.padEnd(22)} found=${n}   ${p}`);
  }
  // Поиск актуальных аналогов по префиксу класса без хэша
  const prefixes = ['ozi__text-view__caption-medium', 'ozi__table__table', 'ozi__data-content__label',
    'ozi__input-select__root', 'ozi__input__input', 'ozi__dialog__dialog', 'ozi__dropdown-item', '_carriageNumber_'];
  out.push('', '=== АНАЛОГИ ПО ПРЕФИКСУ КЛАССА (без хэша) ===');
  for (const pre of prefixes) {
    const els = x(`//*[contains(@class, '${pre}')]`);
    const classes = new Set();
    els.forEach(e => (e.getAttribute('class') || '').split(/\s+/).filter(c => c.includes(pre)).forEach(c => classes.add(c)));
    out.push(`${pre.padEnd(34)} элементов=${els.length}  классы: ${[...classes].slice(0, 6).join(', ') || '—'}`);
  }
  out.push('', '=== ЭЛЕМЕНТЫ С ТЕКСТОМ «Найдено» ===');
  x("//*[contains(normalize-space(.), 'Найдено') and not(*)]").slice(0, 8).forEach(e => out.push('  ' + short(e)));
  out.push('', '=== INPUT на странице (первые 10) ===');
  x('//input').slice(0, 10).forEach(e => out.push('  ' + short(e)));
  out.push('', '=== TABLE на странице ===');
  x('//table').slice(0, 5).forEach(e => out.push(`  ${short(e).slice(0, 220)}  rows=${e.rows ? e.rows.length : '?'}`));
  out.push('', '=== Первая строка первой таблицы (ячейки) ===');
  const tr = x('//table//tbody//tr')[0];
  if (tr) Array.from(tr.cells).slice(0, 5).forEach((td, i) => {
    const inner = td.querySelector('div,span');
    out.push(`  td[${i + 1}] «${td.innerText.trim().slice(0, 40)}»  inner: ${inner ? short(inner).slice(0, 180) : '—'}`);
  });
  const text = out.join('\n');
  console.log(text);
  try { copy(text); console.log('\n>>> Результат скопирован в буфер обмена'); } catch (e) {}
  return 'done';
})();
