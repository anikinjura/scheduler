// Проверка всплывающих окон Турбо ПВЗ против селекторов парсера (только чтение, ничего не нажимает).
// Запускать в консоли DevTools, пока окно «Новости» / уведомление открыто.
// CFG — копия overlay_config из configs/base_configs/ozon_report_config.py: при его изменении обновить и здесь.
(() => {
  const CFG = {
    overlay_selector: "//div[contains(@class, 'ozi__dialog__dialog__')]",
    close_buttons: [
      "//button[contains(@class, 'ozi__button') and normalize-space()='Отложить']",
      "//button[contains(@class, 'ozi__dialog__closeIcon__')]",
      "//button[contains(@class, '_exitButton_')]",
      "//button[contains(@class, 'ozi__icon-button__iconButton__') and contains(@class, '_exitButton_')]",
      "//button[@aria-label='Закрыть']",
      "//button[@title='Закрыть']",
      "//button[contains(@class, 'ozi__icon-button__iconButton__')][.//*[name()='svg']]",
    ],
    backdrops: [
      "//div[contains(@class, 'ozi__backdrop__backdrop__')]",
      "//div[contains(@class, 'ozi__backdrop')]",
      "//div[contains(@class, 'backdrop')]",
      "//div[contains(@class, 'modal-backdrop')]",
    ],
  };
  const x = (p, ctx = document) => {
    const r = document.evaluate(p, ctx, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null);
    return Array.from({ length: r.snapshotLength }, (_, i) => r.snapshotItem(i));
  };
  const vis = (el) => { const r = el.getBoundingClientRect(); const s = getComputedStyle(el);
    return r.width > 0 && r.height > 0 && s.visibility !== 'hidden' && s.display !== 'none' && s.opacity !== '0'; };
  const short = (el, n = 170) => {
    const cls = (el.getAttribute('class') || '').trim().replace(/\s+/g, ' ');
    const attrs = ['role', 'aria-label', 'aria-modal', 'title', 'data-testid'].map(a => el.getAttribute(a) ? ` ${a}="${el.getAttribute(a)}"` : '').join('');
    const txt = (el.innerText || '').trim().replace(/\s+/g, ' ').slice(0, 50);
    return `<${el.tagName.toLowerCase()}${el.id ? '#' + el.id : ''}${attrs} class="${cls.slice(0, n)}"> vis=${vis(el)}${txt ? ' «' + txt + '»' : ''}`;
  };
  const out = [`URL: ${location.href}`, `TIME: ${new Date().toString()}`, '', '=== КАК ВИДИТ ПАРСЕР ==='];
  const ov = x(CFG.overlay_selector);
  out.push(`${ov.length ? 'OK  ' : 'MISS'} overlay_selector found=${ov.length} visible=${ov.filter(vis).length}`);
  CFG.backdrops.forEach(p => { const e = x(p); out.push(`${e.filter(vis).length ? 'OK  ' : 'MISS'} backdrop found=${e.length} visible=${e.filter(vis).length}  ${p}`); });
  out.push('  -- кнопки закрытия (парсер кликает ПЕРВУЮ найденную по порядку):');
  CFG.close_buttons.forEach(p => { const e = x(p); out.push(`${e.filter(vis).length ? 'OK  ' : 'MISS'} found=${e.length} visible=${e.filter(vis).length}  ${p}`);
    e.slice(0, 3).forEach(b => out.push('       -> ' + short(b, 120))); });

  out.push('', '=== ДИАЛОГИ НА СТРАНИЦЕ (по классам/ролям) ===');
  const dialogs = x("//*[contains(@class,'dialog') or contains(@class,'modal') or @role='dialog' or @aria-modal='true']");
  dialogs.slice(0, 12).forEach(e => out.push('  ' + short(e)));
  out.push('', '=== АНАЛОГИ ПО ПРЕФИКСУ КЛАССА (без хэша) ===');
  ['ozi__dialog__', 'ozi__backdrop', 'ozi__modal', 'ozi__notification', 'ozi__toast', 'ozi__snackbar', 'ozi__popover', 'ozi__button__'].forEach(pre => {
    const cls = new Set(); x(`//*[contains(@class,'${pre}')]`).forEach(e => (e.getAttribute('class') || '').split(/\s+/).filter(c => c.startsWith(pre)).forEach(c => cls.add(c)));
    out.push(`${pre.padEnd(20)} ${[...cls].slice(0, 8).join(', ') || '—'}`);
  });
  out.push('', '=== КНОПКИ ВНУТРИ ВИДИМЫХ ОКОН ===');
  ['Отложить', 'Прочитано', 'Перейти', 'Закрыть'].forEach(t => x(`//button[contains(normalize-space(.),'${t}')]`).forEach(b => out.push(`  [${t}] ` + short(b))));
  out.push('', '=== ЦЕПОЧКА РОДИТЕЛЕЙ «Новости Турбо ПВЗ» и «Внимание» ===');
  ['Новости Турбо ПВЗ', 'Внимание', 'непрочитанн'].forEach(t => {
    const el = x(`//*[contains(normalize-space(text()),'${t}')]`)[0];
    out.push(`-- «${t}»: ${el ? '' : 'не найден'}`);
    for (let e = el, i = 0; e && e !== document.body && i < 9; e = e.parentElement, i++) out.push('   '.repeat(1) + '↑ ' + short(e, 140));
  });
  out.push('', '=== ЧТО ПЕРЕКРЫВАЕТ СЕЛЕКТОР ПВЗ (input___v-0-0) ===');
  const inp = document.getElementById('input___v-0-0');
  if (inp) { const r = inp.getBoundingClientRect(); const top = document.elementFromPoint(r.x + r.width / 2, r.y + r.height / 2);
    out.push(`  input rect=${Math.round(r.x)},${Math.round(r.y)} ${Math.round(r.width)}x${Math.round(r.height)}`);
    out.push(`  сверху в этой точке: ${top ? short(top) : '—'}  перекрыт=${top ? !inp.contains(top) && !top.contains(inp) : '?'}`);
  } else out.push('  input___v-0-0 не найден');
  const text = out.join('\n'); console.log(text);
  try { copy(text); console.log('\n>>> Скопировано в буфер обмена'); } catch (e) {}
  return 'done';
})();
