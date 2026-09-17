// GRENSESNITT-RIGG for Rawskap Transfer.
//
// Appen er Tauri, men frontend-en er ren HTML/JS i én fil — så den kan kjøres i headless Chrome med
// en stubb for window.__TAURI__. Da kan oppførsel MÅLES (rektangler, klasser, tekst) i stedet for å
// vurderes på et skjermbilde.
//
// ⚠ KJØR DENNE FØR DU TAGGER EN VERSJON. Utrullingen er elleve minutter CI pluss manuell
// publisering; dette tar under et minutt. 17/9 ble fire feil funnet på én ettermiddag, og tre av
// dem satt i logikk som så åpenbart riktig ut ved lesing — blant annet et 6 px gap som bare lot seg
// fastslå ved å måle det.
//
//   npm run test:ui     (eller: node test/grensesnitt.cjs)
const { spawn } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');
const http = require('http');

const KILDE = path.join(__dirname, '..', 'src', 'index.html');
const CHROME = 'C:/Program Files/Google/Chrome/Application/chrome.exe';
const sov = ms => new Promise(r => setTimeout(r, ms));
const getJson = u => new Promise((res, rej) => http.get(u, r => { let d = ''; r.on('data', c => d += c); r.on('end', () => res(JSON.parse(d))); }).on('error', rej));

// Fire filer i rota, to i en undermappe — nok til aa proeve spenn-valg og undermappe-sti.
const FILER = [
  { id: 'a1', filnavn: 'RAW00001.ARW', bytes: 65000000, mappeId: 'm1', opprettet: '2026-09-17T10:00:00Z' },
  { id: 'a2', filnavn: 'RAW00002.ARW', bytes: 66000000, mappeId: 'm1', opprettet: '2026-09-17T10:01:00Z' },
  { id: 'a3', filnavn: 'RAW00003.ARW', bytes: 67000000, mappeId: 'm1', opprettet: '2026-09-17T10:02:00Z' },
  { id: 'a4', filnavn: 'RAW00004.ARW', bytes: 68000000, mappeId: 'm1', opprettet: '2026-09-17T10:03:00Z' },
  { id: 'b1', filnavn: 'UNDER01.ARW', bytes: 61000000, mappeId: 'm2', opprettet: '2026-09-17T10:04:00Z' },
  { id: 'b2', filnavn: 'UNDER02.ARW', bytes: 62000000, mappeId: 'm2', opprettet: '2026-09-17T10:05:00Z' },
];
const MAPPER = [
  { id: 'm1', navn: 'Jobb 17. sept', forelderId: '' },
  { id: 'm2', navn: 'Utvalg', forelderId: 'm1' },
];

const STUBB = `<script>
window.__TAURI__ = (() => {
  const data = { mapper: ${JSON.stringify(MAPPER)}, filer: ${JSON.stringify(FILER)}, konto: { navn: 'Rigg' } };
  window.__kall = [];
  const invoke = async (cmd, args) => {
    window.__kall.push({ cmd, args });
    if (cmd === 'hent_liste') return data;
    // To av seks mangler lokalt — det er det «N nye» skal telle.
    if (cmd === 'mangler_lokalt') return ['a3', 'b2'];
    if (cmd === 'sjekk_versjon') return { ny: false };
    if (cmd === 'maskinnavn') return 'rigg';
    return null;
  };
  const butikk = new Map([['kobling', { portal: 'https://rawskap.no', nokkel: 'x' }], ['maal', 'C:/ned']]);
  return {
    core: { invoke },
    event: { listen: async () => () => {} },
    dialog: { open: async () => null },
    shell: { open: async () => {} },
    app: { getVersion: async () => '0.2.8' },
    store: { load: async () => ({ get: async k => butikk.get(k), set: async (k, v) => { butikk.set(k, v); }, save: async () => {} }) },
    notification: { isPermissionGranted: async () => false, requestPermission: async () => 'denied', sendNotification: () => {} },
    autostart: { isEnabled: async () => false, enable: async () => {}, disable: async () => {} },
    webview: { getCurrentWebview: () => ({ onDragDropEvent: async () => () => {} }) },
  };
})();
</script>
`;

(async () => {
  const ut = path.join(os.tmpdir(), 'transfer-rigg');
  fs.mkdirSync(ut, { recursive: true });
  const html = fs.readFileSync(KILDE, 'utf8').replace('<script type="module">', STUBB + '<script type="module">');
  const fil = path.join(ut, 'rigg.html');
  fs.writeFileSync(fil, html, 'utf8');

  const chrome = spawn(CHROME, ['--headless=new', '--disable-gpu', '--remote-debugging-port=9361',
    '--window-size=1500,950', '--allow-file-access-from-files',
    '--user-data-dir=' + path.join(os.tmpdir(), 'cdp-rigg'), 'about:blank'], { stdio: 'ignore' });
  try {
    await sov(2500);
    const mål = await getJson('http://127.0.0.1:9361/json');
    const side = mål.find(t => t.type === 'page' && t.webSocketDebuggerUrl);
    const ws = new WebSocket(side.webSocketDebuggerUrl);
    let id = 0; const venter = new Map(); const feil = [];
    const send = (m, p = {}) => new Promise(res => { const i = ++id; venter.set(i, res); ws.send(JSON.stringify({ id: i, method: m, params: p })); });
    ws.onmessage = e => {
      const m = JSON.parse(e.data);
      if (m.id && venter.has(m.id)) { venter.get(m.id)(m.result || { __feil: m.error }); venter.delete(m.id); }
      else if (m.method === 'Runtime.exceptionThrown') feil.push(String(m.params.exceptionDetails.exception?.description || m.params.exceptionDetails.text).slice(0, 200));
    };
    await new Promise(r => ws.onopen = r);
    await send('Page.enable'); await send('Runtime.enable');
    const ev = async expr => {
      const r = await send('Runtime.evaluate', { expression: expr, returnByValue: true, awaitPromise: true });
      if (r.__feil || r.exceptionDetails) return { __feil: r.exceptionDetails?.exception?.description || JSON.stringify(r.__feil) };
      return r.result?.value;
    };
    await send('Page.navigate', { url: 'file:///' + fil.replace(/\\/g, '/') });
    await sov(3500);

    let ok = 0;
    const sjekk = (navn, faktisk, ventet) => {
      const a = JSON.stringify(faktisk), b = JSON.stringify(ventet);
      if (a === b) { ok++; console.log('  ✓', navn); }
      else { feil.push(navn); console.log('  ✗', navn, '\n     fikk:      ' + a + '\n     forventet: ' + b); }
    };

    console.log('0. Oppstartsskjerm og tooltips');
    // ⚠ Blir oppstartsskjermen staaende, skjuler den HELE appen — den maa vekk naar appen er klar.
    sjekk('oppstartsskjermen er fjernet etter oppstart', await ev(`!document.getElementById('oppstart')`), true);
    // data-t-tt gjelder BARE naar spraaket er engelsk (se `if (!erNo)`), saa title= maa staa paa
    // norsk i HTML-en. Et tomt title ga ingen tooltip i det hele tatt — det var feilen 18/9.
    sjekk('«Følg mappa» har tooltip', await ev(`(document.getElementById('folg-mappe').title || '').length > 40`), true);
    sjekk('«Last ned nye» har tooltip', await ev(`(document.getElementById('last-ned-nye').title || '').length > 40`), true);

    console.log('\n1. Rendrer grensesnittet');
    // Rota har ingen filer direkte — naviger inn i mappa foerst (klikk paa mappe-raden).
    sjekk('rota viser mappa', await ev(`document.querySelectorAll('#filer .filrad.mappe').length`), 1);
    await ev(`document.querySelector('#filer .filrad.mappe').click()`);
    await sov(900);
    sjekk('mappa viser fire filer', await ev(`document.querySelectorAll('#filer .filrad[data-id]').length`), 4);
    sjekk('og undermappa', await ev(`document.querySelectorAll('#filer .filrad.mappe').length`), 1);

    console.log('\n2. «N nye»');
    await sov(800);
    sjekk('knappen teller de to som mangler lokalt (inkl. én i undermappe)',
      await ev(`(() => { const k = document.getElementById('last-ned-nye'); return k && !k.hidden && /\\(2\\)/.test(k.textContent); })()`), true);

    console.log('\n3. Flervalg');
    sjekk('klikk velger én', await ev(`(() => { const r = document.querySelectorAll('#filer .filrad[data-id]'); r[0].dispatchEvent(new MouseEvent('click', { bubbles: true })); return document.querySelectorAll('.filrad.valgt').length; })()`), 1);
    sjekk('valgbaren kommer fram', await ev(`!document.getElementById('valgbar').hidden`), true);
    sjekk('shift-klikk tar spennet', await ev(`(() => { const r = document.querySelectorAll('#filer .filrad[data-id]'); r[2].dispatchEvent(new MouseEvent('click', { bubbles: true, shiftKey: true })); return document.querySelectorAll('.filrad.valgt').length; })()`), 3);
    sjekk('ctrl-klikk tar én ut', await ev(`(() => { const r = document.querySelectorAll('#filer .filrad[data-id]'); r[0].dispatchEvent(new MouseEvent('click', { bubbles: true, ctrlKey: true })); return document.querySelectorAll('.filrad.valgt').length; })()`), 2);
    sjekk('tøm nullstiller og skjuler baren', await ev(`(() => { const b = document.getElementById('valgbar'); [...b.querySelectorAll('button')].pop().click(); return { valgte: document.querySelectorAll('.filrad.valgt').length, skjult: b.hidden }; })()`), { valgte: 0, skjult: true });

    console.log('\n4. Følg mappa');
    sjekk('av til å begynne med', await ev(`/Følg mappa/.test(document.getElementById('folg-mappe').textContent)`), true);
    sjekk('slår seg på og starter en jobb', await ev(`(async () => { document.getElementById('folg-mappe').click(); await new Promise(r => setTimeout(r, 900)); return { paa: /Følger/.test(document.getElementById('folg-mappe').textContent), jobber: document.querySelectorAll('#overf-panel .jobbrad:not(.hode)').length }; })()`), { paa: true, jobber: 1 });
    sjekk('og lar seg stoppe', await ev(`(async () => { document.getElementById('folg-mappe').click(); await new Promise(r => setTimeout(r, 300)); return /Følg mappa/.test(document.getElementById('folg-mappe').textContent); })()`), true);

    console.log('\n5. Ingen gap over den klebrige overskriftsraden');
    // ⚠ MÅLT, ikke vurdert. Rullefeltet hadde 6 px toppfyll mens raden er sticky på top:0 — altså
    // 6 px NEDENFOR toppen, og innhold skled opp i stripa. Det lot seg ikke se på et skjermbilde.
    const m = await ev(`(() => {
      document.getElementById('vis-overf').click();
      const v = document.getElementById('visning-overf'); const liste = document.getElementById('overf-panel');
      const hode = liste.querySelector('.jobbrad.hode'); const rad = liste.querySelector('.jobbrad:not(.hode)');
      const cs = e => e ? getComputedStyle(e) : null;
      const r = e => { if (!e) return null; const b = e.getBoundingClientRect(); return { t: Math.round(b.top), b: Math.round(b.bottom), l: Math.round(b.left), r: Math.round(b.right) }; };
      return {
        visningBg: cs(v)?.backgroundColor, listeBg: cs(liste)?.backgroundColor, listePad: cs(liste)?.padding,
        hodeBg: cs(hode)?.backgroundColor, hodeRekt: r(hode), listeRekt: r(liste), radRekt: r(rad),
        hodeKolonner: cs(hode)?.gridTemplateColumns, radKolonner: cs(rad)?.gridTemplateColumns,
        gapOver: hode && liste ? Math.round(hode.getBoundingClientRect().top - liste.getBoundingClientRect().top) : null,
      };
    })()`);
    sjekk('overskriftsraden ligger flush med toppen av rullefeltet', m.gapOver, 0);
    sjekk('hode og rad har samme kolonner', m.hodeKolonner === m.radKolonner, true);

    console.log('\n6. Konsoll');
    const konsoll = feil.filter(f => typeof f === 'string' && /Error|error/.test(f));
    sjekk('ingen unntak i konsollen', konsoll.length, 0);
    if (konsoll.length) console.log('   ', JSON.stringify(konsoll.slice(0, 5)));

    const feilet = feil.length - konsoll.length;
    console.log(`\n${feilet === 0 && konsoll.length === 0 ? '✓' : '✗'} ${ok} sjekker passerte, ${feil.length} feilet`);
    ws.close();
    if (feil.length) process.exitCode = 1;
  } finally { chrome.kill(); }
})().catch(e => { console.error('rigg feilet:', e.message); process.exit(1); });
