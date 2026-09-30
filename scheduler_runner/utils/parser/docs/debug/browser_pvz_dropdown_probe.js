// Разметка выпадающего списка ПВЗ (только чтение). После Enter у вас 5 секунд, чтобы ОТКРЫТЬ список ПВЗ мышкой.
setTimeout(() => {
  // ПВЗ, подпись которого ищем в списке: текущий из поля выбора (можно заменить строкой)
  const TARGET = (document.getElementById('input___v-0-0') || {}).value || 'ЧЕБОКСАРЫ_144';
  const x = (p, ctx = document) => {
    const r = document.evaluate(p, ctx, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null);
    return Array.from({ length: r.snapshotLength }, (_, i) => r.snapshotItem(i));
  };
  const cls = (e) => (e.getAttribute && e.getAttribute('class') || '').trim().replace(/\s+/g, ' ');
  const tag = (e) => `<${e.tagName.toLowerCase()}${e.id ? '#' + e.id : ''}${['role', 'aria-selected', 'data-testid'].map(a => e.getAttribute(a) != null ? ` ${a}="${e.getAttribute(a)}"` : '').join('')} class="${cls(e).slice(0, 200)}">`;
  const out = [`URL: ${location.href}`, `TIME: ${new Date().toString()}`,
    `aria-expanded у поля ПВЗ: ${(document.querySelector("[data-popover-reference='true']") || {}).getAttribute?.('aria-expanded')}`, ''];
  const checks = {
    "parser_option_item (актуальный)": "//div[contains(@class, 'ozi__dropdown-item__dropdownItem__') and .//*[contains(@class, 'ozi__data-content__label__')]]",
    "item_in_teleport_with_label (до 3.11)": "//*[@id='ozi-window-teleport-target']//div[contains(@class, 'ozi__dropdown-item') and .//*[contains(@class, 'ozi__data-content__label__')]]",
    "item_any": "//div[contains(@class, 'ozi__dropdown-item')]",
    "item_in_teleport": "//*[@id='ozi-window-teleport-target']//*[contains(@class, 'ozi__dropdown-item')]",
    "label_any": "//*[contains(@class, 'ozi__data-content__label__')]",
    "label_in_teleport": "//*[@id='ozi-window-teleport-target']//*[contains(@class, 'ozi__data-content__label__')]",
    "label_target": `//*[contains(@class, 'ozi__data-content__label__') and normalize-space()='${TARGET}']`,
    "role_option": "//*[@role='option']",
  };
  out.push('=== СЕЛЕКТОРЫ ===');
  for (const [k, p] of Object.entries(checks)) out.push(`${String(x(p).length).padStart(3)}  ${k.padEnd(28)} ${p}`);

  out.push('', `=== ЦЕПОЧКА РОДИТЕЛЕЙ подписи «${TARGET}» (до 10 уровней) ===`);
  const lbl = x(`//*[contains(@class, 'ozi__data-content__label__') and normalize-space()='${TARGET}']`)[0];
  for (let e = lbl, i = 0; e && e !== document.body && i < 10; e = e.parentElement, i++) out.push(`  ${i}: ${tag(e)}`);

  out.push('', '=== ЭЛЕМЕНТЫ ozi__dropdown-item (первые 3): тег, класс, текст, где находятся ===');
  x("//*[contains(@class, 'ozi__dropdown-item')]").slice(0, 3).forEach((e, i) => {
    const inTeleport = !!e.closest('#ozi-window-teleport-target');
    out.push(`  #${i + 1} ${tag(e)} inTeleport=${inTeleport} text=«${(e.innerText || '').trim().replace(/\s+/g, ' ').slice(0, 60)}»`);
  });

  out.push('', `=== outerHTML пункта с ${TARGET} (ближайший предок с role/option/item в классе) ===`);
  let item = lbl;
  while (item && item !== document.body && !/(option|item)/i.test(cls(item) + ' ' + (item.getAttribute('role') || ''))) item = item.parentElement;
  out.push(item && item !== document.body ? item.outerHTML.replace(/<svg[\s\S]*?<\/svg>/g, '<svg/>').slice(0, 2500) : '— не найден');
  const text = out.join('\n'); console.log(text);
  try { copy(text); console.log('\n>>> Скопировано в буфер обмена'); } catch (e) { console.log('copy() недоступен — скопируйте вывод вручную'); }
}, 5000);
'Откройте список ПВЗ в течение 5 секунд...';
