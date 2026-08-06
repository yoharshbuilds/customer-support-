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
const BASE_TF = {days: 14, timezone: 'Asia/Kolkata', daily: [], weekly: [],
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
  check('empty: weekly order chart handled', mk('#o_wchart').innerHTML.length > 0);
  check('empty: weekly ticket chart handled', mk('#t_wchart').innerHTML.length > 0);
  check('empty: no NaN in weekly charts',
        !(mk('#o_wchart').innerHTML + mk('#t_wchart').innerHTML).includes('NaN'));
  check('empty: issue chart handled', typeof mk('#i_chart').innerHTML === 'string');
  check('empty: hour grid handled', mk('#hrgrid').innerHTML.length > 0);
  check('empty: attendance grid handled', mk('#a_grid').innerHTML.length > 0);
});

section('all zeros (a day with no activity)');
await run('zeros', {
  ord: {daily: [{bucket: '2026-08-01', total_orders: 0, fully_cancelled: 0,
                 partly_cancelled: 0, not_cancelled: 0, cancel_rate: 0,
                 excl_fully_cancelled: 0}]},
  tf: {daily: [{bucket: '2026-08-01', raised: 0, measured_raised: 0,
                raised_and_solved: 0, resolved_that_day: 0, solve_rate: 0,
                has_resolve_data: true, fully_measured: true}]},
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
  check('single: one column', (h.match(/class="vcol[ "]/g) || []).length === 1);
  check('single: height is a finite percentage', !/height:\s*(NaN|Infinity)/.test(h));
  // Segments are stacked, so it is their SUM that fills the peak column, not any
  // one of them. 3+1+1 of a max of 5 must come to exactly 100%.
  const sum = [...h.matchAll(/height:([\d.]+)%/g)].reduce((a, m) => a + +m[1], 0);
  check(`single: segments of the peak column sum to 100% (got ${sum})`,
        Math.abs(sum - 100) < 0.01);
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
  const hs = [...mk('#o_chart').innerHTML.matchAll(/height:([\d.]+)%/g)].map(m => +m[1]);
  check('outlier: every height within 0-100%', hs.every(v => v >= 0 && v <= 100));
  check('outlier: small column still rendered', hs.some(v => v > 0));
  // 1 of 100000 rounds to 0.00% -- CSS min-height keeps it visible, so assert the
  // rule exists rather than expecting a non-zero percentage.
  check('outlier: min-height guards a rounded-to-zero bar',
        /\.vseg\{[^}]*min-height:\s*\dpx/.test(html));
});

section('missing optional fields (older API shape)');
await run('sparse', {
  ord: {daily: [{bucket: '2026-08-01', total_orders: 10}]},
  tf: {daily: [{bucket: '2026-08-01', raised: 5, measured_raised: 0,
                raised_and_solved: null, resolved_that_day: 0, solve_rate: null,
                has_resolve_data: false, fully_measured: false}]},
  iss: {total: 1, counts: {delay: 1}, share: {delay: 100},
        per_day: {'2026-08-01': {delay: 1}}, outcome_by_bucket: {}},
}, (mk) => {
  check('sparse: orders column still renders', mk('#o_chart').innerHTML.includes('vcol'));
  check('sparse: no undefined leaked into orders', !mk('#o_chart').innerHTML.includes('undefined'));
  // A day before resolve capture began is shown, but marked unmeasured and given
  // the neutral colour -- never counted as "still open", which would read as the
  // team leaving tickets unanswered.
  check('sparse: pre-capture day is drawn, not dropped',
        mk('#t_chart').innerHTML.includes('vcol'));
  check('sparse: pre-capture day marked as having no resolve data',
        mk('#t_chart').innerHTML.includes('vnodata'));
  check('sparse: pre-capture day not painted as still-open amber',
        !mk('#t_chart').innerHTML.includes('#c2810a'));
  check('sparse: no NaN from a null solved count',
        !mk('#t_chart').innerHTML.includes('NaN'));
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

section('vertical bar charts: day row then week row');
await run('vertical', {
  // Newest first: /api/orders and /api/tickets_flow both ORDER BY bucket DESC, and
  // the renderer reverses them so time reads left to right on screen.
  ord: {daily: [{bucket: '2026-08-04', total_orders: 20, fully_cancelled: 0,
                 partly_cancelled: 0, not_cancelled: 20, cancel_rate: 0,
                 excl_fully_cancelled: 20},
                {bucket: '2026-08-03', total_orders: 10, fully_cancelled: 1,
                 partly_cancelled: 2, not_cancelled: 7, cancel_rate: 10,
                 excl_fully_cancelled: 9}],
        weekly: [{bucket: '2026-07-27', total_orders: 30, fully_cancelled: 0,
                  partly_cancelled: 0, not_cancelled: 30, cancel_rate: 0,
                  excl_fully_cancelled: 30, days_in_bucket: 3}]},
  tf: {daily: [{bucket: '2026-08-04', raised: 4, measured_raised: 4,
                raised_and_solved: 4, resolved_that_day: 4, solve_rate: 100,
                has_resolve_data: true, fully_measured: true},
               {bucket: '2026-08-03', raised: 8, measured_raised: 8,
                raised_and_solved: 6, resolved_that_day: 6, solve_rate: 75,
                has_resolve_data: true, fully_measured: true}],
       weekly: [{bucket: '2026-08-03', raised: 12, measured_raised: 12,
                 raised_and_solved: 10, resolved_that_day: 10, solve_rate: 83.3,
                 has_resolve_data: true, fully_measured: true,
                 days_in_bucket: 2, partial: true}]},
}, (mk) => {
  const od = mk('#o_chart').innerHTML, ow = mk('#o_wchart').innerHTML;
  const td = mk('#t_chart').innerHTML, tw = mk('#t_wchart').innerHTML;
  check('vertical: order day chart has one column per day',
        (od.match(/class="vcol[ "]/g) || []).length === 2);
  check('vertical: order week chart is a separate row of columns',
        (ow.match(/class="vcol[ "]/g) || []).length === 1);
  check('vertical: ticket day chart has one column per day',
        (td.match(/class="vcol[ "]/g) || []).length === 2);
  check('vertical: ticket week chart is a separate row of columns',
        (tw.match(/class="vcol[ "]/g) || []).length === 1);
  check('vertical: bars sized by height, not width',
        /height:[\d.]+%/.test(od) && !/class="vseg" style="width/.test(od));
  // Oldest bucket leftmost: time must read left to right.
  check('vertical: oldest day is the leftmost column',
        od.indexOf('08-03') < od.indexOf('08-04'));
  check('vertical: newest day is the last column',
        td.indexOf('08-03') < td.indexOf('08-04'));
  // Heights are relative to the chart peak, so a quiet day must look quieter.
  const h = [...od.matchAll(/height:([\d.]+)%/g)].map(m => +m[1]);
  check('vertical: peak day reaches 100%', h.includes(100));
  check('vertical: every height within 0-100%', h.every(v => v >= 0 && v <= 100));
  check('vertical: a part-covered week is flagged, not shown as a drop',
        ow.includes('vpartial') && tw.includes('vpartial'));
  check('vertical: solved segment uses the solved colour', td.includes('#1a9a5f'));
  check('vertical: no NaN or undefined in any of the four charts',
        !/NaN|undefined/.test(od + ow + td + tw));
});

section('a week straddling the start of resolve capture splits, not collapses');
await run('straddle', {
  // Newest week first, as the endpoint returns it.
  tf: {daily: [], weekly: [
    {bucket: '2026-08-03', raised: 191, measured_raised: 191,
     raised_and_solved: 183, resolved_that_day: 183, solve_rate: 95.8,
     has_resolve_data: true, fully_measured: true, days_in_bucket: 2,
     partial: true},
    // Capture began on the Thursday: 400 of this week's 990 tickets are measurable,
    // 350 of those were solved. Judging the week by its Monday start would have
    // painted all 990 grey and thrown away four good days.
    {bucket: '2026-07-27', raised: 990, measured_raised: 400,
     raised_and_solved: 350, resolved_that_day: 360, solve_rate: 87.5,
     has_resolve_data: true, fully_measured: false, days_in_bucket: 7,
     partial: false},
  ]},
}, (mk) => {
  const w = mk('#t_wchart').innerHTML;
  const cols = w.split('class="vcol').slice(1);
  check('straddle: both weeks drawn', cols.length === 2);
  // 350 solved + 50 open + 590 unmeasurable = 990, nothing unattributed.
  check('straddle: the straddling week shows all three states',
        cols[0].includes('#1a9a5f') && cols[0].includes('#c2810a')
        && cols[0].includes('#6b7280'));
  check('straddle: it is not hatched as wholly unmeasured',
        !cols[0].includes('vnodata'));
  check('straddle: its segments sum to its share of the peak',
        Math.abs([...cols[0].matchAll(/height:([\d.]+)%/g)]
                 .reduce((a, m) => a + +m[1], 0) - 100) < 0.02);
  check('straddle: the tooltip says why part of it is grey',
        cols[0].includes('starts mid-week'));
  // The grey slice means "we cannot say", never "nobody answered these".
  check('straddle: grey is never described as unanswered',
        !/still open 437|unanswered/.test(cols[0]));
  check('straddle: the fully measured week shows no grey',
        !cols[1].includes('#6b7280'));
  check('straddle: solve rate is quoted over measured tickets (350/400)',
        w.includes('350/400'));
  check('straddle: no NaN', !w.includes('NaN'));
});

section('a dense window keeps its labels');
await run('dense', {
  ord: {daily: Array.from({length: 30}, (_, i) => ({
    bucket: `2026-07-${String(30 - i).padStart(2, '0')}`,
    total_orders: 100 + i, fully_cancelled: 1, partly_cancelled: 1,
    not_cancelled: 98 + i, cancel_rate: 1, excl_fully_cancelled: 99 + i}))},
}, (mk) => {
  const h = mk('#o_chart').innerHTML;
  check('dense: all 30 columns rendered',
        (h.match(/class="vcol[ "]/g) || []).length === 30);
  check('dense: labels kept (rotated), not dropped',
        (h.match(/class="vlab"/g) || []).length === 30);
  check('dense: per-column value text suppressed to avoid collision',
        !h.includes('class="vval"'));
  check('dense: every column still carries a tooltip',
        (h.match(/title="/g) || []).length === 30);
});
check('dense mode rotates labels rather than hiding them',
      /\.vchart\.dense \.vlab\{[^}]*writing-mode/.test(html));

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

section('nav must stay reachable as tabs are added');
const navTabs = (html.match(/data-p="\d"/g) || []).length;
check(`nav has ${navTabs} tabs`, navTabs >= 6);
check('nav scrolls horizontally rather than overflowing',
      /nav\{[^}]*overflow-x:\s*auto/.test(html));
check('nav buttons do not shrink or wrap mid-label',
      /nav button\{[^}]*flex:\s*0 0 auto/.test(html));
check('narrow screens give the nav its own row',
      /@media \(max-width:\s*820px\)/.test(html));
check('nav is not left dependent on margin-left:auto alone',
      /nav\{[^}]*max-width:\s*100%/.test(html));

console.log(`\n${TOTAL - FAILS.length}/${TOTAL} passed`);
if (FAILS.length) {
  console.log('\nFAILED:');
  FAILS.forEach(f => console.log('  ' + f));
  process.exit(1);
}
process.exit(0);
