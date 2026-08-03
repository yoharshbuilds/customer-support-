/* UI render tests. MECE layer 4 of 4.
 *
 * Layers 1-3 cover logic, HTTP contract and data integrity. This one executes the
 * page's real script against payloads chosen to break chart code: empty arrays,
 * a single row, all-zero values, missing optional fields, one enormous outlier,
 * and hostile strings. A dashboard that divides by a max of 0 or interpolates
 * `undefined` into a style attribute fails here rather than in front of someone.
 *
 *   node scripts/test_ui.mjs app/static/dashboard.html
 */
import fs from 'node:fs';

const htmlPath = process.argv[2] || 'app/static/dashboard.html';
const html = fs.readFileSync(htmlPath, 'utf8');
const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];

let TOTAL = 0;
const FAILS = [];
const check = (label, cond) => {
  TOTAL++;
  if (!cond) FAILS.push(label);
  console.log(`${cond ? 'PASS' : 'FAIL'}  ${label}`);
};
const section = (n) => console.log(`\n--- ${n} ---`);

/* ---------- DOM stub ---------- */
function makeEnv(payloads) {
  const els = new Map();
  const mk = (id) => {
    if (!els.has(id)) els.set(id, {
      id, innerHTML: '', textContent: '', value: '', style: {}, dataset: {},
      classList: {toggle(){}, add(){}, remove(){}, contains: () => false},
      addEventListener(){}, querySelectorAll: () => [],
      forEach(){},
    });
    return els.get(id);
  };
  mk('#o_days').value = '30'; mk('#o_live').value = '0';
  mk('#a_days').value = '14'; mk('#a_who').value = 'all';
  mk('#t_days').value = '14'; mk('#t_live').value = '0';
  globalThis.document = {
    querySelector: mk,
    querySelectorAll: () => ({length: 0, forEach(){}}),
    addEventListener(){},
  };
  globalThis.window = {addEventListener(){}};
  globalThis.EventSource = class { addEventListener(){} close(){} };
  globalThis.getComputedStyle = () => ({getPropertyValue: () => '#2d6cdf'});
  globalThis.setInterval = () => 0;
  globalThis.clearInterval = () => {};
  globalThis.requestAnimationFrame = (fn) => fn();
  globalThis.fetch = async (url) => {
    const path = String(url).split('?')[0];
    const data = payloads[path];
    if (data === undefined) return {ok: false, status: 404, json: async () => ({})};
    return {ok: true, status: 200, json: async () => data};
  };
  const api = {};
  eval(script + '\nObject.assign(api, {loadOrders, loadTickets, loadAttendance});');
  return {mk, api, els};
}

const BASE_ATT = {
  days: [], agents: [], cells: {}, hourly: {}, today: null, data_from: null,
  shift_start_hour: 6, timezone: 'Asia/Kolkata', short_shift_hours: 4,
  support_role: 'Customer Delight', roster_ok: true, availability_days: 0,
  message_days: 0, availability_unmapped_ids: 0,
};
const BASE_ORD = {timezone: 'Asia/Kolkata', days: 30, daily: [], weekly: [], source: 'test'};
const BASE_ISS = {
  total: 0, counts: {}, share: {}, by_evidence: {}, unmapped_tags: {},
  buckets: [{key: 'delay', label: 'Fulfilment delay', color: '#c2810a'},
            {key: 'no_issue', label: 'No issue raised', color: '#6b7280'}],
  per_day: {}, outcome_by_bucket: {}, days: 30, timezone: 'Asia/Kolkata',
};
const BASE_TF = {days: 14, timezone: 'Asia/Kolkata', daily: [],
                 inflow_source: 'x', resolved_source: 'y', resolve_data_from: null,
                 inflow_stats: {}};

function payloads(over = {}) {
  return {
    '/api/attendance': {...BASE_ATT, ...(over.att || {})},
    '/api/orders': {...BASE_ORD, ...(over.ord || {})},
    '/api/issues': {...BASE_ISS, ...(over.iss || {})},
    '/api/tickets_flow': {...BASE_TF, ...(over.tf || {})},
    '/api/filters': {}, '/api/stats': {}, '/api/tickets': {rows: [], total: 0},
    '/api/median': {days: [], series: []},
  };
}

async function run(label, over, assert) {
  const {mk, api} = makeEnv(payloads(over));
  let threw = null;
  try {
    await api.loadOrders();
    await api.loadTickets();
    await api.loadAttendance();
  } catch (e) { threw = e; }
  check(`${label}: renders without throwing`, threw === null);
  if (threw) { console.log('       ', threw.message); return; }
  assert(mk);
}

/* ---------- cases ---------- */
section('empty everything (a brand-new deployment)');
await run('empty', {}, (mk) => {
  check('empty: orders chart says something, not blank', mk('#o_chart').innerHTML.length > 0);
  check('empty: no NaN in orders', !mk('#o_chart').innerHTML.includes('NaN'));
  check('empty: no undefined in orders', !mk('#o_chart').innerHTML.includes('undefined'));
  check('empty: cards have no NaN', !mk('#o_cards').innerHTML.includes('NaN'));
  check('empty: ticket chart handled', mk('#t_chart').innerHTML.length > 0);
  check('empty: issue chart handled', typeof mk('#i_chart').innerHTML === 'string');
  check('empty: hour grid handled', mk('#hrgrid').innerHTML.length > 0);
  check('empty: attendance grid handled', mk('#a_grid').innerHTML.length > 0);
});

section('all zeros (a day with no activity)');
await run('zeros', {
  ord: {daily: [{bucket: '2026-08-01', total_orders: 0, fully_cancelled: 0,
                 partly_cancelled: 0, not_cancelled: 0, cancel_rate: 0,
                 excl_fully_cancelled: 0}]},
  tf: {daily: [{bucket: '2026-08-01', raised: 0, raised_and_solved: 0,
                resolved_that_day: 0, solve_rate: 0, has_resolve_data: true}]},
}, (mk) => {
  check('zeros: no division-by-zero NaN in orders', !mk('#o_chart').innerHTML.includes('NaN'));
  check('zeros: no Infinity', !mk('#o_chart').innerHTML.includes('Infinity'));
  check('zeros: ticket chart no NaN', !mk('#t_chart').innerHTML.includes('NaN'));
  check('zeros: cards no NaN', !mk('#t_cards').innerHTML.includes('NaN'));
});

section('single row (max === value, the classic divide-by-max trap)');
await run('single', {
  ord: {daily: [{bucket: '2026-08-01', total_orders: 5, fully_cancelled: 1,
                 partly_cancelled: 1, not_cancelled: 3, cancel_rate: 20,
                 excl_fully_cancelled: 4}]},
}, (mk) => {
  const h = mk('#o_chart').innerHTML;
  check('single: one bar row', (h.match(/class="crow"/g) || []).length === 1);
  check('single: width is a finite percentage', !/width:\s*(NaN|Infinity)/.test(h));
});

section('one huge outlier (scaling must not overflow)');
await run('outlier', {
  ord: {daily: [
    {bucket: '2026-08-02', total_orders: 100000, fully_cancelled: 0,
     partly_cancelled: 0, not_cancelled: 100000, cancel_rate: 0, excl_fully_cancelled: 100000},
    {bucket: '2026-08-01', total_orders: 1, fully_cancelled: 0,
     partly_cancelled: 0, not_cancelled: 1, cancel_rate: 0, excl_fully_cancelled: 1},
  ]},
}, (mk) => {
  const widths = [...mk('#o_chart').innerHTML.matchAll(/width:([\d.]+)%/g)].map(m => +m[1]);
  check('outlier: every width within 0-100%', widths.every(w => w >= 0 && w <= 100));
  check('outlier: small row still visible (>0 width)', widths.some(w => w > 0));
});

section('missing optional fields (older API shape)');
await run('sparse', {
  ord: {daily: [{bucket: '2026-08-01', total_orders: 10}]},
  tf: {daily: [{bucket: '2026-08-01', raised: 5, raised_and_solved: null,
                resolved_that_day: 0, solve_rate: null, has_resolve_data: false}]},
  iss: {total: 1, counts: {delay: 1}, share: {delay: 100},
        per_day: {'2026-08-01': {delay: 1}}, outcome_by_bucket: {}},
}, (mk) => {
  check('sparse: orders row still renders', mk('#o_chart').innerHTML.includes('crow'));
  check('sparse: no undefined leaked into orders', !mk('#o_chart').innerHTML.includes('undefined'));
  check('sparse: pre-capture day excluded from solve chart',
        !mk('#t_chart').innerHTML.includes('crow'));
  check('sparse: issue bar rendered', mk('#i_chart').innerHTML.includes('crow'));
  check('sparse: no undefined in issue chart', !mk('#i_chart').innerHTML.includes('undefined'));
});

section('hostile strings must be escaped, not executed');
await run('xss', {
  att: {
    days: ['2026-08-01'], today: '2026-08-01', data_from: '2026-08-01',
    agents: [{name: '<img src=x onerror=alert(1)>', role: '"><script>bad()</script>',
              is_support: true, in_roster: true, expected: true, ever_active: true,
              days_present: 1, days_expected: 1, resolves: 1, avg_span_hrs: 1, total_hrs: 1}],
    cells: {'<img src=x onerror=alert(1)>': {'2026-08-01': {
      agent: '<img src=x onerror=alert(1)>', day: '2026-08-01', first: '09:00',
      last: '17:00', span_hrs: 8, resolves: 1, short: false, overnight: false,
      source: 'messages', messages: 2, conversations: 1, hours: {'9': 2}}}},
    hourly: {'<img src=x onerror=alert(1)>': {'2026-08-01': {'9': 2}}},
  },
}, (mk) => {
  const g = mk('#a_grid').innerHTML + mk('#a_summary').innerHTML + mk('#hrgrid').innerHTML;
  check('xss: no raw <img onerror', !g.includes('<img src=x'));
  check('xss: no raw <script', !g.includes('<script>bad'));
  check('xss: angle brackets escaped', g.includes('&lt;'));
});

section('CSS/animation standards in the stylesheet');
check('custom strong ease-out defined', /--ease-out:\s*cubic-bezier/.test(html));
check('no ease-in used on UI', !/transition:[^;]*\bease-in\b(?!-out)/.test(html));
check('nav buttons have press feedback', /nav button:active\{transform:scale/.test(html));
check('pager buttons have press feedback', /\.pager button:active[^{]*\{transform:scale/.test(html));
check('reduced motion honoured', html.includes('prefers-reduced-motion'));
check('no scale(0) anywhere', !/scale\(0\)/.test(html));
const longTransitions = [...html.matchAll(/transition:[^;}]*?(\d{3,})ms/g)]
  .map(m => +m[1]).filter(ms => ms > 700);
check(`no UI transition over 700ms (found: ${longTransitions.join(',') || 'none'})`,
      longTransitions.length === 0);
check('row highlight is a transition, not a keyframe', !/tr\.new\{animation/.test(html));

console.log(`\n${TOTAL - FAILS.length}/${TOTAL} passed`);
if (FAILS.length) {
  console.log('\nFAILED:');
  FAILS.forEach(f => console.log('  ' + f));
  process.exit(1);
}
process.exit(0);
