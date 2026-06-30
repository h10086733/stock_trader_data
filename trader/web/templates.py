"""Web 页面 HTML 模板常量。从 app.py 原样提取，供 Flask render_template_string 使用。"""

HTML = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>行业宽度指标</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Noto+Sans+SC:wght@300;400;500;700&display=swap" rel="stylesheet">
<style>
  :root {
    --bg:        #0d0f14;
    --surface:   #141720;
    --surface2:  #1a1e2e;
    --border:    #252a3a;
    --text:      #c8cdd8;
    --text-dim:  #5a6070;
    --text-head: #8892a4;
    --red:       #ff4d6a;
    --red-dim:   rgba(255,77,106,0.12);
    --green:     #00c97a;
    --green-dim: rgba(0,201,122,0.10);
    --accent:    #3d7fff;
  }
  * { margin:0; padding:0; box-sizing:border-box; }
  body {
    background: var(--bg);
    color: var(--text);
    font-family: 'Noto Sans SC', sans-serif;
    font-size: 13px;
    min-height: 100vh;
    padding: 32px 24px;
  }

  /* 顶部 */
  .header {
    display: flex;
    align-items: flex-end;
    justify-content: space-between;
    margin-bottom: 28px;
    padding-bottom: 20px;
    border-bottom: 1px solid var(--border);
  }
  .header h1 { font-size:20px; font-weight:500; letter-spacing:.08em; color:#fff; }
  .header p  { margin-top:4px; font-size:11px; color:var(--text-dim); letter-spacing:.05em; }
  .header-right { display:flex; align-items:center; gap:12px; }

  .days-select {
    background: var(--surface2);
    border: 1px solid var(--border);
    color: var(--text);
    padding: 6px 12px;
    border-radius: 6px;
    font-size: 12px;
    font-family: inherit;
    cursor: pointer;
    outline: none;
    transition: border-color .2s;
  }
  .days-select:hover { border-color: var(--accent); }

  .refresh-btn {
    background: var(--accent);
    color: #fff;
    border: none;
    padding: 6px 16px;
    border-radius: 6px;
    font-size: 12px;
    font-family: inherit;
    cursor: pointer;
    transition: opacity .2s;
  }
  .refresh-btn:hover { opacity:.85; }
  .nav-link {
    color: var(--text);
    text-decoration: none;
    border: 1px solid var(--border);
    background: var(--surface2);
    padding: 6px 12px;
    border-radius: 6px;
    font-size: 12px;
  }
  .nav-link:hover { border-color: var(--accent); }

  /* 状态栏 */
  .status-bar {
    display: flex;
    gap: 24px;
    margin-bottom: 20px;
    padding: 12px 16px;
    background: var(--surface);
    border-radius: 8px;
    border: 1px solid var(--border);
  }
  .status-item { display:flex; flex-direction:column; gap:2px; }
  .status-label { font-size:10px; color:var(--text-dim); letter-spacing:.06em; text-transform:uppercase; }
  .status-value { font-family:'DM Mono',monospace; font-size:13px; color:var(--text); }

  /* 表格 */
  .table-wrap {
    overflow-x: auto;
    border-radius: 10px;
    border: 1px solid var(--border);
  }
  table { width:100%; border-collapse:collapse; }
  thead tr { background: var(--surface2); }
  th {
    padding: 12px 16px;
    font-size: 11px;
    font-weight: 500;
    color: var(--text-head);
    letter-spacing: .06em;
    text-transform: uppercase;
    text-align: center;
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }
  th:first-child { text-align:left; min-width:130px; }

  .date-header { display:flex; flex-direction:column; align-items:center; gap:2px; }
  .date-month  { font-size:10px; color:var(--text-dim); }
  .date-day    { font-size:13px; font-family:'DM Mono',monospace; color:var(--text); }

  tbody tr {
    border-bottom: 1px solid var(--border);
    transition: background .15s;
  }
  tbody tr:last-child { border-bottom:none; }
  tbody tr:hover { background: rgba(255,255,255,0.025); }

  td.name-cell {
    padding: 14px 16px;
    font-size: 13px;
    color: var(--text);
    white-space: nowrap;
  }
  .index-btn {
    display:block;
    width:100%;
    border:0;
    background:transparent;
    color:inherit;
    font:inherit;
    text-align:left;
    cursor:pointer;
  }
  .index-btn:hover { color:#fff; }
  .idx-code {
    display: block;
    font-size: 10px;
    font-family: 'DM Mono', monospace;
    color: var(--text-dim);
    margin-top: 2px;
  }

  td.val-cell {
    padding: 8px 6px;
    text-align: center;
  }
  .val-inner {
    display: inline-flex;
    flex-direction: column;
    align-items: center;
    gap: 4px;
    padding: 8px 14px;
    border-radius: 6px;
    min-width: 76px;
  }

  .val-inner.positive { background: var(--red-dim); }
  .val-inner.negative { background: var(--green-dim); }
  .val-inner.zero     { background: transparent; }

  .val-number {
    font-family: 'DM Mono', monospace;
    font-size: 15px;
    font-weight: 500;
  }
  .val-sub {
    font-family: 'DM Mono', monospace;
    font-size: 10px;
    color: var(--text-dim);
    white-space: nowrap;
  }
  .positive .val-number { color: var(--red); }
  .negative .val-number { color: var(--green); }
  .zero     .val-number { color: var(--text-dim); }

  .val-bar {
    width: 100%;
    height: 2px;
    border-radius: 1px;
    background: var(--border);
    position: relative;
    overflow: hidden;
  }
  .val-bar-fill {
    position: absolute;
    top: 0;
    height: 100%;
    border-radius: 1px;
  }
  .positive .val-bar-fill { background:var(--red);   left:50%; }
  .negative .val-bar-fill { background:var(--green); right:50%; }

  .empty-cell { font-size:12px; color:var(--text-dim); }
  .metric-btn {
    border: 0;
    font: inherit;
    color: inherit;
  }

  .modal-backdrop {
    position: fixed;
    inset: 0;
    display: none;
    align-items: center;
    justify-content: center;
    padding: 24px;
    background: rgba(0,0,0,.58);
    z-index: 20;
  }
  .modal-backdrop.open { display:flex; }
  .modal {
    width: min(620px, 100%);
    max-height: calc(100vh - 48px);
    overflow: auto;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    box-shadow: 0 24px 80px rgba(0,0,0,.42);
  }
  .modal-head {
    display:flex;
    align-items:flex-start;
    justify-content:space-between;
    gap:16px;
    padding:18px 20px 14px;
    border-bottom:1px solid var(--border);
  }
  .modal-title { color:#fff; font-size:16px; font-weight:500; }
  .modal-subtitle { margin-top:4px; color:var(--text-dim); font-size:11px; font-family:'DM Mono',monospace; }
  .modal-close {
    width:30px;
    height:30px;
    border:1px solid var(--border);
    border-radius:6px;
    background:var(--surface2);
    color:var(--text);
    cursor:pointer;
    font-size:18px;
    line-height:1;
  }
  .modal-close:hover { border-color:var(--accent); }
  .modal-body { padding:16px 20px 20px; }
  .weight-table th:first-child { min-width:52px; text-align:center; }
  .weight-table th:nth-child(2), .weight-table td:nth-child(2) { text-align:left; }
  .weight-table td { padding:11px 12px; border-bottom:1px solid var(--border); text-align:center; }
  .weight-table tr:last-child td { border-bottom:0; }
  .stock-code { display:block; margin-top:2px; color:var(--text-dim); font-size:10px; font-family:'DM Mono',monospace; }
  .weight-value { color:var(--red); font-family:'DM Mono',monospace; font-weight:500; }
  .weight-value.empty { color:var(--text-dim); }

  /* Loading */
  .loading {
    display: flex;
    align-items: center;
    justify-content: center;
    height: 200px;
    color: var(--text-dim);
    gap: 10px;
  }
  .spinner {
    width:16px; height:16px;
    border: 2px solid var(--border);
    border-top-color: var(--accent);
    border-radius: 50%;
    animation: spin .8s linear infinite;
  }
  @keyframes spin { to { transform:rotate(360deg); } }
</style>
</head>
<body>

<div class="header">
  <div>
    <h1>行业宽度指标</h1>
  </div>
  <div class="header-right">
    <a class="nav-link" href="/pattern">收盘形态</a>
    <a class="nav-link" href="/momentum">14:30 选股</a>
    <a class="nav-link" href="/high-confidence">高置信小集合</a>
    <a class="nav-link" href="/surge">涨停概率</a>
    <a class="nav-link" href="/csi1000">中证1000择时</a>
    <select class="days-select" id="daysSelect" onchange="loadData()">
      <option value="5">最近 5 日</option>
      <option value="10">最近 10 日</option>
      <option value="20">最近 20 日</option>
      <option value="60">最近 60 日</option>
    </select>
    <button class="refresh-btn" onclick="loadData()">↻ 刷新</button>
  </div>
</div>

<div class="status-bar">
  <div class="status-item">
    <span class="status-label">最新交易日</span>
    <span class="status-value" id="statDate">—</span>
  </div>
  <div class="status-item">
    <span class="status-label">指数数量</span>
    <span class="status-value" id="statCount">—</span>
  </div>
  <div class="status-item">
    <span class="status-label">页面更新</span>
    <span class="status-value" id="statTime">—</span>
  </div>
</div>

<div class="table-wrap" id="tableWrap">
  <div class="loading"><div class="spinner"></div>加载中…</div>
</div>

<div class="modal-backdrop" id="weightModal" onclick="onModalBackdropClick(event)">
  <div class="modal" role="dialog" aria-modal="true" aria-labelledby="weightModalTitle">
    <div class="modal-head">
      <div>
        <div class="modal-title" id="weightModalTitle">成分股权重 Top 10</div>
        <div class="modal-subtitle" id="weightModalMeta">—</div>
      </div>
      <button class="modal-close" onclick="closeWeightModal()" aria-label="关闭">×</button>
    </div>
    <div class="modal-body" id="weightModalBody"></div>
  </div>
</div>

<script>
function escapeHtml(value) {
  return String(value ?? '').replace(/[&<>"']/g, ch => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;'
  }[ch]));
}

function fmtDateHeader(s) {
  const d = new Date(s);
  const m = String(d.getMonth()+1).padStart(2,'0');
  return { month: d.getFullYear()+'/'+m, day: String(d.getDate()).padStart(2,'0') };
}

function buildTable(data) {
  const { dates, indices } = data;
  const displayDates = [...dates].reverse();

  if (!displayDates.length || !indices.length) {
    document.getElementById('tableWrap').innerHTML =
      '<div class="loading">暂无数据，请先运行 index_stats.py</div>';
    return;
  }

  document.getElementById('statDate').textContent  = displayDates[0];
  document.getElementById('statCount').textContent = indices.length + ' 个';
  document.getElementById('statTime').textContent  = new Date().toLocaleTimeString('zh-CN');

  let html = '<table><thead><tr><th>行业指数</th>';
  displayDates.forEach(d => {
    const {month, day} = fmtDateHeader(d);
    html += `<th><div class="date-header">
      <span class="date-month">${month}</span>
      <span class="date-day">${day}</span>
    </div></th>`;
  });
  html += '</tr></thead><tbody>';

  indices.forEach(idx => {
    html += `<tr><td class="name-cell">
      <button class="index-btn" onclick="showConstituents('${escapeHtml(idx.code)}')" title="查看成分股">
        ${escapeHtml(idx.name)}<span class="idx-code">${escapeHtml(idx.code)}</span>
      </button>
    </td>`;
    displayDates.forEach(d => {
      const v = idx.ma3[d];
      const dayValue = (idx.net_value && idx.net_value[d] !== undefined)
        ? idx.net_value[d]
        : idx.details?.[d]?.net_value;
      if (v === undefined || v === null) {
        html += '<td class="val-cell"><span class="empty-cell">—</span></td>';
        return;
      }
      const pct = (v * 100).toFixed(2);
      const dayPct = (dayValue === undefined || dayValue === null)
        ? '—'
        : (dayValue * 100).toFixed(2);
      const cls = v >  0.001 ? 'positive' : v < -0.001 ? 'negative' : 'zero';
      const barW = Math.min(Math.abs(v) * 100, 50);
      html += `<td class="val-cell">
        <div class="val-inner metric-btn ${cls}">
          <span class="val-number">${pct}</span>
          <span class="val-sub">日 ${dayPct}</span>
          <div class="val-bar"><div class="val-bar-fill" style="width:${barW}%"></div></div>
        </div></td>`;
    });
    html += '</tr>';
  });

  html += '</tbody></table>';
  document.getElementById('tableWrap').innerHTML = html;
}

function openWeightModal(title, meta, bodyHtml) {
  document.getElementById('weightModalTitle').textContent = title;
  document.getElementById('weightModalMeta').textContent = meta;
  document.getElementById('weightModalBody').innerHTML = bodyHtml;
  document.getElementById('weightModal').classList.add('open');
}

function closeWeightModal() {
  document.getElementById('weightModal').classList.remove('open');
}

function onModalBackdropClick(event) {
  if (event.target.id === 'weightModal') closeWeightModal();
}

async function showConstituents(code) {
  openWeightModal(`${code} 成分股`, code, '<div class="loading"><div class="spinner"></div>加载中…</div>');
  try {
    const res = await fetch('/api/index-constituents?code=' + encodeURIComponent(code) + '&limit=10');
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || '加载失败');

    document.getElementById('weightModalTitle').textContent =
      data.weight_count ? `${data.name} 成分股权重 Top 10` : `${data.name} 成分股前 10`;
    const updated = data.updated_at ? ` · 成分股更新 ${data.updated_at}` : '';
    const coverage = data.total_count ? Math.round((data.weight_coverage || 0) * 100) : 0;
    const modeText = data.weight_complete
      ? '按权重排序'
      : (data.weight_count ? `权重不完整 ${coverage}%，仅显示有权重前 10` : '暂无权重，显示成分股前 10');
    const weightDate = data.weight_date ? ` · 权重日期 ${data.weight_date}` : '';
    document.getElementById('weightModalMeta').textContent =
      `${data.code} · 共 ${data.total_count} 只 · 有权重 ${data.weight_count} 只 · ${modeText}${weightDate}${updated}`;

    if (!data.rows.length) {
      document.getElementById('weightModalBody').innerHTML =
        '<div class="loading">该指数暂无成分股数据</div>';
      return;
    }

    let html = '<table class="weight-table"><thead><tr><th>排名</th><th>成分股</th><th>交易所</th><th>权重</th></tr></thead><tbody>';
    data.rows.forEach((row, i) => {
      html += `<tr>
        <td>${i + 1}</td>
        <td>${escapeHtml(row.name || '')}<span class="stock-code">${escapeHtml(row.code)}</span></td>
        <td>${escapeHtml(row.exchange || '-')}</td>
        <td class="weight-value ${row.weight === null || row.weight === undefined ? 'empty' : ''}">${row.weight === null || row.weight === undefined ? '—' : Number(row.weight).toFixed(2) + '%'}</td>
      </tr>`;
    });
    html += '</tbody></table>';
    document.getElementById('weightModalBody').innerHTML = html;
  } catch (err) {
    document.getElementById('weightModalBody').innerHTML =
      `<div class="loading">加载失败：${escapeHtml(err.message)}</div>`;
  }
}

document.addEventListener('keydown', event => {
  if (event.key === 'Escape') closeWeightModal();
});

async function loadData() {
  const days = document.getElementById('daysSelect').value;
  document.getElementById('tableWrap').innerHTML =
    '<div class="loading"><div class="spinner"></div>加载中…</div>';
  try {
    const res  = await fetch('/api/stats?days=' + days);
    const data = await res.json();
    buildTable(data);
  } catch(err) {
    document.getElementById('tableWrap').innerHTML =
      `<div class="loading">加载失败：${err.message}</div>`;
  }
}

loadData();
</script>
</body>
</html>
"""

MOMENTUM_HTML = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>14:30 强势放量选股</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Noto+Sans+SC:wght@300;400;500;700&display=swap" rel="stylesheet">
<style>
  :root {
    --bg:#0d0f14;
    --surface:#141720;
    --surface2:#1a1e2e;
    --border:#252a3a;
    --text:#c8cdd8;
    --text-dim:#697082;
    --head:#8d96a9;
    --red:#ff4d6a;
    --green:#00c97a;
    --accent:#3d7fff;
    --amber:#f5b84b;
  }
  * { box-sizing:border-box; margin:0; padding:0; }
  body {
    min-height:100vh;
    background:var(--bg);
    color:var(--text);
    font-family:'Noto Sans SC', sans-serif;
    font-size:13px;
    padding:28px 24px;
  }
  .header {
    display:flex;
    justify-content:space-between;
    align-items:flex-end;
    gap:16px;
    padding-bottom:18px;
    margin-bottom:18px;
    border-bottom:1px solid var(--border);
  }
  h1 { font-size:20px; color:#fff; font-weight:500; letter-spacing:.04em; }
  .sub { margin-top:5px; color:var(--text-dim); font-size:11px; }
  .nav-link {
    color:var(--text);
    text-decoration:none;
    border:1px solid var(--border);
    background:var(--surface2);
    padding:7px 12px;
    border-radius:6px;
    white-space:nowrap;
  }
  .toolbar {
    display:grid;
    grid-template-columns: repeat(8, minmax(92px, 1fr));
    gap:10px;
    align-items:end;
    padding:14px;
    margin-bottom:16px;
    background:var(--surface);
    border:1px solid var(--border);
    border-radius:8px;
  }
  label { display:flex; flex-direction:column; gap:5px; color:var(--text-dim); font-size:10px; }
  input, select {
    height:34px;
    background:var(--surface2);
    color:var(--text);
    border:1px solid var(--border);
    border-radius:6px;
    padding:0 10px;
    font:12px 'Noto Sans SC', sans-serif;
    outline:none;
  }
  input:focus, select:focus { border-color:var(--accent); }
  button {
    height:34px;
    border:0;
    border-radius:6px;
    background:var(--accent);
    color:white;
    cursor:pointer;
    font:500 12px 'Noto Sans SC', sans-serif;
  }
  button:disabled { opacity:.55; cursor:default; }
  .status {
    display:flex;
    flex-wrap:wrap;
    gap:10px;
    margin-bottom:14px;
  }
  .pill {
    display:flex;
    gap:8px;
    align-items:center;
    min-height:32px;
    padding:7px 10px;
    border:1px solid var(--border);
    border-radius:6px;
    background:var(--surface);
    color:var(--text-dim);
  }
  .pill b {
    color:var(--text);
    font-family:'DM Mono', monospace;
    font-weight:500;
  }
  .profit-panel {
    margin-bottom:16px;
    border:1px solid var(--border);
    border-radius:8px;
    background:var(--surface);
    overflow:hidden;
  }
  .profit-head {
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:12px;
    padding:12px 14px;
    border-bottom:1px solid var(--border);
    background:rgba(255,255,255,.018);
  }
  .profit-title { color:#fff; font-size:13px; font-weight:500; }
  .profit-range { margin-left:8px; color:var(--text-dim); font:11px 'DM Mono', monospace; }
  .ghost-btn {
    width:auto;
    min-width:64px;
    padding:0 12px;
    border:1px solid var(--border);
    background:var(--surface2);
    color:var(--text);
  }
  .profit-grid {
    display:grid;
    grid-template-columns: repeat(6, minmax(92px, 1fr));
    gap:1px;
    background:var(--border);
  }
  .profit-stat {
    min-height:68px;
    padding:12px;
    background:var(--surface);
  }
  .profit-label { color:var(--text-dim); font-size:10px; margin-bottom:7px; }
  .profit-value { color:var(--text); font:500 18px 'DM Mono', monospace; }
  .profit-value.up { color:var(--red); }
  .profit-value.down { color:var(--green); }
  .profit-body {
    display:grid;
    grid-template-columns: minmax(360px, 1fr) minmax(420px, 1.15fr);
    gap:14px;
    padding:14px;
  }
  .mini-title {
    color:var(--head);
    font-size:11px;
    margin-bottom:8px;
  }
  .profit-days {
    display:flex;
    flex-direction:column;
    gap:6px;
  }
  .day-row {
    display:grid;
    grid-template-columns: 86px 1fr 70px 58px;
    align-items:center;
    gap:10px;
    min-height:24px;
    color:var(--text-dim);
    font-size:11px;
  }
  .bar-track {
    height:6px;
    border-radius:999px;
    background:var(--surface2);
    overflow:hidden;
  }
  .bar-fill {
    height:100%;
    width:0;
    border-radius:999px;
    background:var(--text-dim);
  }
  .bar-fill.up { background:var(--red); }
  .bar-fill.down { background:var(--green); }
  .recent-list {
    display:flex;
    flex-direction:column;
    gap:6px;
  }
  .recent-row {
    display:grid;
    grid-template-columns: 72px 70px 1fr 70px 58px;
    gap:8px;
    align-items:center;
    min-height:24px;
    color:var(--text-dim);
    font-size:11px;
  }
  .recent-code { font-family:'DM Mono', monospace; color:var(--text); }
  .recent-name { overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .table-wrap {
    overflow-x:auto;
    border:1px solid var(--border);
    border-radius:8px;
  }
  table { width:100%; border-collapse:collapse; min-width:1120px; }
  thead tr { background:var(--surface2); }
  th {
    color:var(--head);
    font-size:11px;
    font-weight:500;
    text-align:right;
    padding:11px 12px;
    border-bottom:1px solid var(--border);
    white-space:nowrap;
  }
  th:first-child, th:nth-child(2), th:last-child { text-align:left; }
  td {
    padding:10px 12px;
    border-bottom:1px solid var(--border);
    text-align:right;
    white-space:nowrap;
  }
  tbody tr:hover { background:rgba(255,255,255,.025); }
  tbody tr:last-child td { border-bottom:0; }
  .code {
    font-family:'DM Mono', monospace;
    color:var(--text-dim);
    text-align:left;
  }
  .name { color:#fff; text-align:left; }
  .num { font-family:'DM Mono', monospace; }
  .up { color:var(--red); }
  .down { color:var(--green); }
  .score {
    display:inline-flex;
    justify-content:center;
    min-width:46px;
    padding:3px 8px;
    border-radius:999px;
    background:rgba(61,127,255,.14);
    color:#8eb1ff;
    font-family:'DM Mono', monospace;
  }
  .spark { width:142px; text-align:left; }
  .reason { text-align:left; color:var(--text-dim); max-width:220px; overflow:hidden; text-overflow:ellipsis; }
  .loading {
    min-height:220px;
    display:flex;
    align-items:center;
    justify-content:center;
    color:var(--text-dim);
  }
  .spinner {
    width:16px;
    height:16px;
    margin-right:10px;
    border:2px solid var(--border);
    border-top-color:var(--accent);
    border-radius:50%;
    animation:spin .8s linear infinite;
  }
  @keyframes spin { to { transform:rotate(360deg); } }
  @media (max-width: 1100px) {
    .toolbar { grid-template-columns: repeat(4, minmax(92px, 1fr)); }
    .profit-grid { grid-template-columns: repeat(3, minmax(92px, 1fr)); }
    .profit-body { grid-template-columns: 1fr; }
  }
  @media (max-width: 640px) {
    body { padding:20px 14px; }
    .header { align-items:flex-start; flex-direction:column; }
    .toolbar { grid-template-columns: repeat(2, minmax(92px, 1fr)); }
    .profit-grid { grid-template-columns: repeat(2, minmax(92px, 1fr)); }
    .day-row { grid-template-columns: 78px 1fr 62px; }
    .day-row .day-win { display:none; }
    .recent-row { grid-template-columns: 66px 1fr 58px; }
    .recent-row .recent-date, .recent-row .recent-status { display:none; }
  }
</style>
</head>
<body>
<div class="header">
  <div>
    <h1>14:30 强势放量选股</h1>
    <div class="sub">涨幅适中、量能放大、日内位置强，默认次日 10:00 前卖出观察</div>
  </div>
  <div style="display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end">
    <a class="nav-link" href="/pattern">收盘形态</a>
    <a class="nav-link" href="/">行业宽度</a>
    <a class="nav-link" href="/high-confidence">高置信小集合</a>
    <a class="nav-link" href="/surge">涨停概率</a>
    <a class="nav-link" href="/csi1000">中证1000择时</a>
  </div>
</div>

<div class="toolbar">
  <label>股票池
    <select id="pool" onchange="syncIndexWithPool()">
      <option value="all">全市场</option>
      <option value="sector">行业池</option>
      <option value="index">指数成分</option>
    </select>
  </label>
  <label>指数
    <select id="indexCode" onchange="syncPoolWithIndex()"></select>
  </label>
  <label>截止
    <input id="cutoff" value="14:30" inputmode="numeric">
  </label>
  <label>最低涨幅%
    <input id="minGain" type="number" value="2" step="0.1">
  </label>
  <label>最高涨幅%
    <input id="maxGain" type="number" value="7.5" step="0.1">
  </label>
  <label>量比
    <input id="minVolRatio" type="number" value="1.5" step="0.1">
  </label>
  <label>成交额万元
    <input id="minAmount" type="number" value="8000" step="500">
  </label>
  <label>验证数量
    <input id="verifyLimit" type="number" value="50" step="10" min="5" max="300">
  </label>
  <button id="scanBtn" onclick="scan()">开始扫描</button>
</div>

<div class="status">
  <div class="pill">报价 <b id="quoted">—</b></div>
  <div class="pill">预筛 <b id="prefiltered">—</b></div>
  <div class="pill">验证 <b id="verified">—</b></div>
  <div class="pill">5分钟K <b id="minuteStats">—</b></div>
  <div class="pill">缓存 <b id="cacheHits">—</b></div>
  <div class="pill">入选 <b id="matched">—</b></div>
  <div class="pill">耗时 <b id="elapsed">—</b></div>
  <div class="pill">时间 <b id="scanTime">—</b></div>
</div>

<div class="profit-panel">
  <div class="profit-head">
    <div>
      <span class="profit-title">最近一个月收益</span>
      <span class="profit-range" id="profitRange">—</span>
    </div>
    <button class="ghost-btn" onclick="loadProfit()">刷新</button>
  </div>
  <div class="profit-grid">
    <div class="profit-stat">
      <div class="profit-label">平均收益</div>
      <div class="profit-value" id="profitAvg">—</div>
    </div>
    <div class="profit-stat">
      <div class="profit-label">胜率</div>
      <div class="profit-value" id="profitWin">—</div>
    </div>
    <div class="profit-stat">
      <div class="profit-label">成交记录</div>
      <div class="profit-value" id="profitSold">—</div>
    </div>
    <div class="profit-stat">
      <div class="profit-label">未结算/失败</div>
      <div class="profit-value" id="profitFailed">—</div>
    </div>
    <div class="profit-stat">
      <div class="profit-label">最好</div>
      <div class="profit-value" id="profitBest">—</div>
    </div>
    <div class="profit-stat">
      <div class="profit-label">最差</div>
      <div class="profit-value" id="profitWorst">—</div>
    </div>
  </div>
  <div class="profit-body">
    <div>
      <div class="mini-title">按买入日</div>
      <div class="profit-days" id="profitDays"><div class="loading">加载中…</div></div>
    </div>
    <div>
      <div class="mini-title">最近记录</div>
      <div class="recent-list" id="profitRecent"><div class="loading">加载中…</div></div>
    </div>
  </div>
</div>

<div class="table-wrap" id="tableWrap">
  <div class="loading">等待扫描</div>
</div>

<script>
const fmt = (value, digits=2) => value === null || value === undefined ? '—' : Number(value).toFixed(digits);
const esc = value => String(value ?? '').replace(/[&<>"']/g, ch => ({
  '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
}[ch]));
const pctText = (value, digits=2) => value === null || value === undefined ? '—' : `${Number(value).toFixed(digits)}%`;
const signedCls = value => Number(value || 0) > 0 ? 'up' : Number(value || 0) < 0 ? 'down' : '';

function setProfitValue(id, value, suffix='', digits=2) {
  const el = document.getElementById(id);
  const cls = signedCls(value);
  el.className = `profit-value ${cls}`;
  el.textContent = value === null || value === undefined ? '—' : `${Number(value).toFixed(digits)}${suffix}`;
}

async function loadIndices() {
  const res = await fetch('/api/indices');
  const data = await res.json();
  const select = document.getElementById('indexCode');
  select.innerHTML = data.indices.map(x =>
    `<option value="${esc(x.code)}">${esc(x.name)} ${esc(x.code)}</option>`
  ).join('');
}

function syncPoolWithIndex() {
  const indexCode = document.getElementById('indexCode').value;
  document.getElementById('pool').value = indexCode ? 'index' : 'all';
}

function syncIndexWithPool() {
  const pool = document.getElementById('pool').value;
  if (pool !== 'index') {
    document.getElementById('indexCode').value = '';
  }
}

function params() {
  const p = new URLSearchParams();
  ['pool','indexCode','cutoff','minGain','maxGain','minVolRatio','minAmount','verifyLimit'].forEach(id => {
    p.set(id, document.getElementById(id).value);
  });
  p.set('limit', '80');
  p.set('workers', '8');
  return p.toString();
}

function renderRows(rows) {
  if (!rows.length) {
    document.getElementById('tableWrap').innerHTML = '<div class="loading">暂无符合条件的股票</div>';
    return;
  }
  let html = `<table><thead><tr>
    <th>代码</th><th>名称</th><th>现价</th><th>涨幅</th><th>量比</th>
    <th>成交额</th><th>高位</th><th>高点回撤</th><th>高点</th><th>分时</th><th>评分</th><th>要点</th>
  </tr></thead><tbody>`;
  rows.forEach(r => {
    html += `<tr>
      <td class="code">${esc(r.code)}</td>
      <td class="name">${esc(r.name)}</td>
      <td class="num">${fmt(r.price, 2)}</td>
      <td class="num ${r.pct >= 0 ? 'up' : 'down'}">${fmt(r.pct, 2)}%</td>
      <td class="num">${fmt(r.volume_ratio, 2)}</td>
      <td class="num">${fmt(r.amount_yi, 2)}亿</td>
      <td class="num">${r.close_position === null ? '—' : fmt(r.close_position, 1) + '%'}</td>
      <td class="num">${r.pullback_pct === null ? '—' : fmt(r.pullback_pct, 2) + '%'}</td>
      <td class="num">${esc(r.high_time || '—')}</td>
      <td class="spark">${r.sparkline || '—'}</td>
      <td><span class="score">${fmt(r.score, 1)}</span></td>
      <td class="reason" title="${esc(r.reasons)}">${esc(r.reasons)}</td>
    </tr>`;
  });
  html += '</tbody></table>';
  document.getElementById('tableWrap').innerHTML = html;
}

function renderProfit(data) {
  const summary = data.summary || {};
  document.getElementById('profitRange').textContent =
    data.start_date && data.end_date ? `${data.start_date} ~ ${data.end_date}` : '暂无记录';
  setProfitValue('profitAvg', summary.avg_return_pct, '%', 2);
  setProfitValue('profitWin', summary.win_rate_pct, '%', 1);
  document.getElementById('profitSold').textContent = summary.sold_count ?? 0;
  document.getElementById('profitFailed').textContent = summary.failed_count ?? 0;
  setProfitValue('profitBest', summary.max_return_pct, '%', 2);
  setProfitValue('profitWorst', summary.min_return_pct, '%', 2);

  const days = data.by_date || [];
  const maxAbs = Math.max(1, ...days.map(x => Math.abs(Number(x.avg_return_pct || 0))));
  if (!days.length) {
    document.getElementById('profitDays').innerHTML = '<div class="loading">暂无收益记录</div>';
  } else {
    document.getElementById('profitDays').innerHTML = days.slice(0, 12).map(day => {
      const avg = Number(day.avg_return_pct || 0);
      const width = Math.max(2, Math.abs(avg) / maxAbs * 100);
      const cls = signedCls(avg);
      return `<div class="day-row">
        <span class="num">${esc(day.buy_date)}</span>
        <span class="bar-track"><span class="bar-fill ${cls}" style="width:${width}%"></span></span>
        <span class="num ${cls}">${pctText(day.avg_return_pct, 2)}</span>
        <span class="day-win">${day.sold_count || 0}笔 / ${pctText(day.win_rate_pct, 0)}</span>
      </div>`;
    }).join('');
  }

  const recent = data.recent || [];
  if (!recent.length) {
    document.getElementById('profitRecent').innerHTML = '<div class="loading">暂无最近记录</div>';
  } else {
    document.getElementById('profitRecent').innerHTML = recent.map(row => {
      const cls = signedCls(row.return_pct);
      const status = row.status === 'sold' ? (row.error === 'daily_open_fallback' ? '日线' : '分钟') : '失败';
      return `<div class="recent-row">
        <span class="recent-date num">${esc(row.buy_date)}</span>
        <span class="recent-code">${esc(row.code)}</span>
        <span class="recent-name">${esc(row.name || '')}</span>
        <span class="num ${cls}">${pctText(row.return_pct, 2)}</span>
        <span class="recent-status">${esc(status)}</span>
      </div>`;
    }).join('');
  }
}

async function loadProfit() {
  try {
    const res = await fetch('/api/momentum/profit?days=30');
    const data = await res.json();
    if (!res.ok) {
      throw new Error(data.error || '收益加载失败');
    }
    renderProfit(data);
  } catch (err) {
    document.getElementById('profitRange').textContent = '加载失败';
    document.getElementById('profitDays').innerHTML = `<div class="loading">${esc(err.message)}</div>`;
    document.getElementById('profitRecent').innerHTML = `<div class="loading">${esc(err.message)}</div>`;
  }
}

async function scan() {
  const btn = document.getElementById('scanBtn');
  btn.disabled = true;
  document.getElementById('tableWrap').innerHTML =
    '<div class="loading"><span class="spinner"></span>扫描中…</div>';
  try {
    const res = await fetch('/api/momentum/scan?' + params());
    const data = await res.json();
    if (data.meta) {
      document.getElementById('quoted').textContent = data.meta.quoted;
      document.getElementById('prefiltered').textContent = data.meta.prefiltered;
      document.getElementById('verified').textContent = data.meta.verified;
      document.getElementById('minuteStats').textContent =
        `${data.meta.minute_success ?? 0}/${data.meta.verified ?? 0}`;
      document.getElementById('cacheHits').textContent = data.meta.cache_hits ?? 0;
      document.getElementById('elapsed').textContent = data.meta.elapsed_s + 's';
      document.getElementById('scanTime').textContent = new Date().toLocaleTimeString('zh-CN');
    }
    if (!res.ok) {
      document.getElementById('matched').textContent = '0';
      throw new Error(data.error || '扫描失败');
    }
    document.getElementById('matched').textContent = data.rows.length;
    renderRows(data.rows);
  } catch (err) {
    document.getElementById('tableWrap').innerHTML = `<div class="loading">加载失败：${err.message}</div>`;
  } finally {
    btn.disabled = false;
  }
}

loadIndices();
loadProfit();
</script>
</body>
</html>
"""

PATTERN_HTML = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>收盘 K 线形态扫描</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Noto+Sans+SC:wght@300;400;500;700&display=swap" rel="stylesheet">
<style>
  :root {
    --bg:#0d0f14;
    --surface:#141720;
    --surface2:#1a1e2e;
    --border:#252a3a;
    --text:#c8cdd8;
    --text-dim:#697082;
    --head:#8d96a9;
    --red:#ff4d6a;
    --green:#00c97a;
    --accent:#3d7fff;
    --amber:#f5b84b;
  }
  * { box-sizing:border-box; margin:0; padding:0; }
  body {
    min-height:100vh;
    background:var(--bg);
    color:var(--text);
    font-family:'Noto Sans SC', sans-serif;
    font-size:13px;
    padding:28px 24px;
  }
  .header {
    display:flex;
    align-items:flex-end;
    justify-content:space-between;
    gap:16px;
    padding-bottom:18px;
    margin-bottom:18px;
    border-bottom:1px solid var(--border);
  }
  h1 { color:#fff; font-size:20px; font-weight:500; letter-spacing:.04em; }
  .sub { margin-top:5px; color:var(--text-dim); font-size:11px; }
  .nav { display:flex; gap:8px; flex-wrap:wrap; justify-content:flex-end; }
  .nav-link {
    color:var(--text);
    text-decoration:none;
    border:1px solid var(--border);
    background:var(--surface2);
    padding:7px 12px;
    border-radius:6px;
    white-space:nowrap;
  }
  .toolbar {
    display:grid;
    grid-template-columns: repeat(10, minmax(86px, 1fr));
    gap:10px;
    align-items:end;
    padding:14px;
    margin-bottom:14px;
    border:1px solid var(--border);
    border-radius:8px;
    background:var(--surface);
  }
  label { display:flex; flex-direction:column; gap:5px; color:var(--text-dim); font-size:10px; }
  input, select {
    height:34px;
    background:var(--surface2);
    color:var(--text);
    border:1px solid var(--border);
    border-radius:6px;
    padding:0 10px;
    font:12px 'Noto Sans SC', sans-serif;
    outline:none;
  }
  input:focus, select:focus { border-color:var(--accent); }
  button {
    height:34px;
    border:0;
    border-radius:6px;
    background:var(--accent);
    color:white;
    cursor:pointer;
    font:500 12px 'Noto Sans SC', sans-serif;
  }
  button.secondary {
    border:1px solid var(--border);
    background:var(--surface2);
    color:var(--text);
  }
  button:disabled { opacity:.55; cursor:default; }
  .status {
    display:flex;
    flex-wrap:wrap;
    gap:10px;
    margin-bottom:16px;
  }
  .pill {
    display:flex;
    align-items:center;
    gap:8px;
    min-height:32px;
    padding:7px 10px;
    border:1px solid var(--border);
    border-radius:6px;
    background:var(--surface);
    color:var(--text-dim);
  }
  .pill b {
    color:var(--text);
    font-family:'DM Mono', monospace;
    font-weight:500;
  }
  .grid {
    display:grid;
    grid-template-columns: repeat(auto-fill, minmax(360px, 1fr));
    gap:14px;
  }
  .card {
    border:1px solid var(--border);
    border-radius:8px;
    background:var(--surface);
    overflow:hidden;
  }
  .card-head {
    display:flex;
    align-items:flex-start;
    justify-content:space-between;
    gap:12px;
    padding:12px 13px 10px;
    border-bottom:1px solid var(--border);
    background:rgba(255,255,255,.018);
  }
  .stock-title { color:#fff; font-size:14px; font-weight:500; }
  .stock-code { display:block; margin-top:2px; color:var(--text-dim); font:10px 'DM Mono', monospace; }
  .stock-link { color:inherit; text-decoration:none; }
  .stock-link:hover .stock-title { color:#8eb1ff; }
  .stock-link:hover .stock-code { color:#8eb1ff; }
  .score {
    min-width:48px;
    padding:4px 8px;
    border-radius:999px;
    background:rgba(61,127,255,.14);
    color:#8eb1ff;
    text-align:center;
    font:500 12px 'DM Mono', monospace;
  }
  .chart { padding:10px 10px 4px; }
  .chart-link { display:block; color:inherit; text-decoration:none; }
  .metrics {
    display:grid;
    grid-template-columns: repeat(4, 1fr);
    gap:1px;
    background:var(--border);
    border-top:1px solid var(--border);
  }
  .metric {
    min-height:50px;
    padding:9px 10px;
    background:var(--surface);
  }
  .metric-label { margin-bottom:5px; color:var(--text-dim); font-size:10px; }
  .metric-value { color:var(--text); font:500 13px 'DM Mono', monospace; }
  .up { color:var(--red); }
  .down { color:var(--green); }
  .reason {
    padding:10px 12px 12px;
    color:var(--text-dim);
    font-size:11px;
    line-height:1.6;
    min-height:42px;
  }
  .section-title {
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:12px;
    margin:22px 0 12px;
    color:#fff;
    font-size:15px;
    font-weight:500;
  }
  .history {
    display:flex;
    flex-direction:column;
    gap:10px;
  }
  .history-toolbar {
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:12px;
    margin-bottom:10px;
    color:var(--text-dim);
    font-size:11px;
  }
  .history-actions {
    display:flex;
    align-items:center;
    gap:8px;
  }
  .history-actions button {
    min-width:72px;
    padding:0 10px;
  }
  .history-head {
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:10px;
    padding:10px 12px;
    border:1px solid var(--border);
    border-radius:8px;
    background:var(--surface2);
    color:var(--text);
    cursor:pointer;
  }
  .history-head:hover { border-color:var(--accent); }
  .history-date { font:500 13px 'DM Mono', monospace; color:#fff; }
  .history-meta { color:var(--text-dim); font-size:11px; }
  .history-count {
    min-width:52px;
    padding:5px 8px;
    border-radius:999px;
    background:rgba(61,127,255,.14);
    color:#8eb1ff;
    text-align:center;
    font:500 12px 'DM Mono', monospace;
  }
  .history-body {
    display:none;
    margin-top:10px;
  }
  .history-item.open .history-body { display:block; }
  .loading {
    min-height:260px;
    display:flex;
    align-items:center;
    justify-content:center;
    color:var(--text-dim);
  }
  .spinner {
    width:16px;
    height:16px;
    margin-right:10px;
    border:2px solid var(--border);
    border-top-color:var(--accent);
    border-radius:50%;
    animation:spin .8s linear infinite;
  }
  @keyframes spin { to { transform:rotate(360deg); } }
  @media (max-width: 1180px) {
    .toolbar { grid-template-columns: repeat(5, minmax(86px, 1fr)); }
  }
  @media (max-width: 700px) {
    body { padding:20px 14px; }
    .header { align-items:flex-start; flex-direction:column; }
    .nav { justify-content:flex-start; }
    .toolbar { grid-template-columns: repeat(2, minmax(86px, 1fr)); }
    .grid { grid-template-columns: 1fr; }
    .metrics { grid-template-columns: repeat(2, 1fr); }
  }
</style>
</head>
<body>
<div class="header">
  <div>
    <h1>收盘 K 线形态扫描</h1>
    <div class="sub">四根十字针 / 底部反转形态</div>
  </div>
  <div class="nav">
    <a class="nav-link" href="/">行业宽度</a>
    <a class="nav-link" href="/momentum">14:30 选股</a>
    <a class="nav-link" href="/high-confidence">高置信小集合</a>
    <a class="nav-link" href="/surge">涨停概率</a>
    <a class="nav-link" href="/csi1000">中证1000择时</a>
  </div>
</div>

<div class="toolbar">
  <label>股票池
    <select id="pool" onchange="syncIndexWithPool()">
      <option value="all">全市场</option>
      <option value="sector">行业池</option>
      <option value="index">指数成分</option>
    </select>
  </label>
  <label>形态
    <select id="patternType" onchange="onPatternTypeChange()">
      <option value="four_pin">四根十字针</option>
      <option value="bottom_reversal" selected>底部反转</option>
    </select>
  </label>
  <label>底部形态
    <select id="bottomPatternGroup">
      <option value="engulfing" selected>只看涨吞没</option>
      <option value="strong">强反转组合</option>
      <option value="single">单针确认</option>
      <option value="all">全部底部形态</option>
    </select>
  </label>
  <label>指数
    <select id="indexCode" onchange="syncPoolWithIndex()"></select>
  </label>
  <label>交易日
    <input id="tradeDate" placeholder="留空取最新">
  </label>
  <label>针实体%
    <input id="maxBodyPct" type="number" value="1.05" step="0.05">
  </label>
  <label>十字实体%
    <input id="dojiBodyPct" type="number" value="1.05" step="0.05">
  </label>
  <label>最大振幅%
    <input id="maxAmpPct" type="number" value="6.0" step="0.1">
  </label>
  <label>实体占振幅%
    <input id="maxBodyRangePct" type="number" value="35" step="1">
  </label>
  <label>MA40距离%
    <input id="maxMa40Distance" type="number" value="0" step="0.5">
  </label>
  <label>同位偏差%
    <input id="maxPairDistance" type="number" value="0.5" step="0.1">
  </label>
  <label>收盘同差%
    <input id="maxClosePairDistance" type="number" value="1.0" step="0.1">
  </label>
  <label>高低差%
    <input id="minLevelGap" type="number" value="0.8" step="0.05">
  </label>
  <label>影线最小%
    <input id="minShadowPct" type="number" value="1" step="1">
  </label>
  <label>缺影线数
    <input id="maxShadowlessCount" type="number" value="0" step="1" min="0" max="4">
  </label>
  <label>低位回看
    <input id="bottomLookbackDays" type="number" value="60" step="5">
  </label>
  <label>低位位置%
    <input id="maxBottomPosition" type="number" value="25" step="5">
  </label>
  <label>前期跌幅%
    <input id="minPriorDropPct" type="number" value="10" step="0.5">
  </label>
  <label>反转实体%
    <input id="bottomMaxBodyPct" type="number" value="3.0" step="0.1">
  </label>
  <label>收盘位置%
    <input id="minBottomClosePosition" type="number" value="75" step="5">
  </label>
  <label>量比≥
    <input id="minBottomVolumeRatio" type="number" value="2.0" step="0.1">
  </label>
  <label>量比≤
    <input id="maxBottomVolumeRatio" type="number" value="3.0" step="0.1">
  </label>
  <label>低点反弹%
    <input id="minBottomReboundPct" type="number" value="3.0" step="0.5">
  </label>
  <label>日涨幅≥%
    <input id="minBottomPctChange" type="number" value="2.5" step="0.5">
  </label>
  <label>强形涨幅≥%
    <input id="minBottomStrongGainPct" type="number" value="4.0" step="0.5">
  </label>
  <label>高于前收
    <select id="requireBottomCloseAbovePrev">
      <option value="1">要求</option>
      <option value="0">不要求</option>
    </select>
  </label>
  <label>收回MA5
    <select id="requireBottomAboveMa5">
      <option value="1">要求</option>
      <option value="0">不要求</option>
    </select>
  </label>
  <label>MA5斜率≥%
    <input id="minBottomMa5SlopePct" type="number" value="-1.0" step="0.5">
  </label>
  <label>非收盘新低
    <select id="requireBottomNotCloseNewLow">
      <option value="1">要求</option>
      <option value="0">不要求</option>
    </select>
  </label>
  <label>新低回看
    <input id="bottomNewLowLookbackDays" type="number" value="20" step="5">
  </label>
  <label>胜率回看
    <input id="patternWinLookbackDays" type="number" value="720" step="60">
  </label>
  <label>持有天数
    <input id="patternWinHoldDays" type="number" value="1" step="1">
  </label>
  <label>目标涨幅%
    <input id="patternWinTargetPct" type="number" value="3.0" step="0.5">
  </label>
  <label>成交额万元
    <input id="minAmount" type="number" value="8000" step="1000">
  </label>
  <label>总市值亿
    <input id="minMarketCapYi" type="number" value="0" step="50">
  </label>
  <label>换手%
    <input id="minTurnover" type="number" value="0" step="0.1">
  </label>
  <button id="scanBtn" onclick="scan()">扫描并保存</button>
  <button class="secondary" onclick="loadLatest()">最近结果</button>
  <button class="secondary" id="clearBtn" onclick="clearPatternHistory(false)">清空当前形态</button>
  <button class="secondary" id="clearAllBtn" onclick="clearPatternHistory(false, true)">清空全部历史</button>
  <button class="secondary" id="clearBackfillBtn" onclick="clearPatternHistory(true)">清空并回扫</button>
  <button class="secondary" id="backfillBtn" onclick="backfillPattern()">回扫</button>
</div>

<div class="status">
  <div class="pill">交易日 <b id="statDate">—</b></div>
  <div class="pill">股票池 <b id="statPool">—</b></div>
  <div class="pill">扫描 <b id="statScanned">—</b></div>
  <div class="pill">命中 <b id="statMatched">—</b></div>
  <div class="pill">耗时 <b id="statElapsed">—</b></div>
  <div class="pill">进度 <b id="statProgress">—</b></div>
  <div class="pill">保存 <b id="statSaved">—</b></div>
</div>

<div id="result"><div class="loading"><span class="spinner"></span>加载最近结果…</div></div>
<div class="section-title">
  <span>历史命中</span>
  <button class="secondary" onclick="loadHistory(1)">刷新历史</button>
</div>
<div id="history"><div class="loading"><span class="spinner"></span>加载历史…</div></div>

<script>
const esc = value => String(value ?? '').replace(/[&<>"']/g, ch => ({
  '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
}[ch]));
const fmt = (value, digits=2) => value === null || value === undefined ? '—' : Number(value).toFixed(digits);
const cls = value => Number(value || 0) > 0 ? 'up' : Number(value || 0) < 0 ? 'down' : '';
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
let historyPage = 1;
const historyPageSize = 10;

async function loadIndices() {
  const res = await fetch('/api/indices');
  const data = await res.json();
  const select = document.getElementById('indexCode');
  select.innerHTML = data.indices.map(x =>
    `<option value="${esc(x.code)}">${esc(x.name)} ${esc(x.code)}</option>`
  ).join('');
}

function syncPoolWithIndex() {
  const indexCode = document.getElementById('indexCode').value;
  document.getElementById('pool').value = indexCode ? 'index' : 'all';
}

function syncIndexWithPool() {
  const pool = document.getElementById('pool').value;
  if (pool !== 'index') document.getElementById('indexCode').value = '';
}

function params() {
  const p = new URLSearchParams();
  ['patternType','bottomPatternGroup','pool','indexCode','tradeDate','maxBodyPct','dojiBodyPct','maxAmpPct',
   'maxBodyRangePct','maxMa40Distance','maxPairDistance','maxClosePairDistance','minLevelGap',
   'minShadowPct','maxShadowlessCount','bottomLookbackDays','maxBottomPosition',
   'minPriorDropPct','bottomMaxBodyPct','minBottomClosePosition',
   'minBottomVolumeRatio','maxBottomVolumeRatio','minBottomReboundPct','minBottomPctChange',
   'minBottomStrongGainPct','requireBottomCloseAbovePrev',
   'requireBottomAboveMa5','minBottomMa5SlopePct',
   'requireBottomNotCloseNewLow','bottomNewLowLookbackDays',
   'patternWinLookbackDays','patternWinHoldDays','patternWinTargetPct',
   'minAmount','minMarketCapYi','minTurnover'].forEach(id => {
    const value = document.getElementById(id).value;
    if (value !== '') p.set(id, value);
  });
  p.set('limit', document.getElementById('patternType').value === 'bottom_reversal' ? '10' : '80');
  p.set('chartBars', '70');
  p.set('filterSaved', '1');
  p.set('save', '1');
  return p.toString();
}

function setMeta(meta, savedText='—') {
  document.getElementById('statDate').textContent = meta?.trade_date || '—';
  document.getElementById('statPool').textContent = meta?.pool || '—';
  document.getElementById('statScanned').textContent = meta?.scanned ?? '—';
  document.getElementById('statMatched').textContent = meta?.matched ?? '—';
  document.getElementById('statElapsed').textContent = meta?.elapsed_s === undefined ? '—' : `${meta.elapsed_s}s`;
  document.getElementById('statSaved').textContent = savedText;
}

function setProgressText(text) {
  document.getElementById('statProgress').textContent = text || '—';
}

function progressText(job) {
  if (!job || job.status === 'idle') return '—';
  if (job.status === 'running') {
    const total = Number(job.total || 0);
    const current = Number(job.current_index || 0);
    const pct = total > 0 ? ` ${Math.floor(current * 100 / total)}%` : '';
    const hits = job.matched_rows === null || job.matched_rows === undefined ? '' : ` 命中${job.matched_rows}`;
    const prefix = job.message ? `${job.message} · ` : '';
    return `${prefix}${current}/${total || '?'}${pct}${hits}`;
  }
  if (job.status === 'done') {
    return `完成 ${job.matched_days || 0}天/${job.matched_rows || 0}条`;
  }
  if (job.status === 'error') return '失败';
  return job.status || '—';
}

function patternBackfillDays(patternType=document.getElementById('patternType')?.value) {
  return patternType === 'four_pin' ? 365 : 30;
}

function patternBackfillLabel(patternType=document.getElementById('patternType')?.value) {
  return patternType === 'four_pin' ? '1年' : '1个月';
}

function updateBackfillButtons() {
  const label = patternBackfillLabel();
  const backfillBtn = document.getElementById('backfillBtn');
  const clearBackfillBtn = document.getElementById('clearBackfillBtn');
  if (backfillBtn) backfillBtn.textContent = `回扫${label}`;
  if (clearBackfillBtn) clearBackfillBtn.textContent = `清空并回扫${label}`;
}

async function loadPatternProgress(renderBox=false) {
  const res = await fetch('/api/pattern/progress?job=pattern_backfill');
  const job = await res.json();
  if (!res.ok) throw new Error(job.error || '进度加载失败');
  const text = progressText(job);
  setProgressText(text);
  if (renderBox && job.status === 'running') {
    const detail = job.trade_date ? `当前 ${esc(job.trade_date)} · ` : '';
    const elapsed = job.elapsed_s === null || job.elapsed_s === undefined ? '' : ` · ${fmt(job.elapsed_s, 1)}s`;
    const label = patternBackfillLabel(job.params?.pattern_type);
    document.getElementById('history').innerHTML =
      `<div class="loading"><span class="spinner"></span>正在回扫最近${label}：${detail}${esc(text)}${elapsed}</div>`;
  }
  return job;
}

function metric(label, value, className='') {
  return `<div class="metric"><div class="metric-label">${label}</div><div class="metric-value ${className}">${value}</div></div>`;
}

function xueqiuSymbol(code) {
  const text = String(code || '').trim();
  if (/^(SH|SZ|BJ)\d{6}$/i.test(text)) return text.toUpperCase();
  if (/^(5|6|9)/.test(text)) return `SH${text}`;
  if (/^(0|2|3)/.test(text)) return `SZ${text}`;
  if (/^(4|8|92)/.test(text)) return `BJ${text}`;
  return text;
}

function xueqiuUrl(code) {
  const symbol = xueqiuSymbol(code);
  return symbol ? `https://xueqiu.com/S/${encodeURIComponent(symbol)}` : '#';
}

function rowCard(row) {
  const isBottom = row.pattern_type === 'bottom_reversal';
  const stockUrl = xueqiuUrl(row.code);
  const winHold = row.pattern_win_hold_days ?? 1;
  const patternMetrics = isBottom ? `
      ${metric('形态', esc(row.pattern_name || '底部反转'))}
      ${metric('低位位置', row.bottom_position_pct === null || row.bottom_position_pct === undefined ? '—' : `${fmt(row.bottom_position_pct, 1)}%`)}
      ${metric('前期跌幅', row.prior_drop_pct === null || row.prior_drop_pct === undefined ? '—' : `${fmt(row.prior_drop_pct, 2)}%`)}
      ${metric('低点反弹', row.rebound_pct === null || row.rebound_pct === undefined ? '—' : `${fmt(row.rebound_pct, 2)}%`)}
      ${metric('收盘位置', row.close_position_pct === null || row.close_position_pct === undefined ? '—' : `${fmt(row.close_position_pct, 1)}%`)}
      ${metric('形态天数', `${row.pattern_days ?? '—'}天`)}
      ${metric('当日量比', row.volume_ratio === null || row.volume_ratio === undefined ? '—' : fmt(row.volume_ratio, 2))}
      ${metric('形态均量', row.pattern_volume_ratio === null || row.pattern_volume_ratio === undefined ? '—' : fmt(row.pattern_volume_ratio, 2))}
      ${metric(`${winHold}日胜率`, row.pattern_win_rate_pct === null || row.pattern_win_rate_pct === undefined ? '样本不足' : `${fmt(row.pattern_win_rate_pct, 1)}%`)}
      ${metric('历史样本', row.pattern_win_sample_count === null || row.pattern_win_sample_count === undefined ? '—' : `${row.pattern_win_sample_count}次`)}
      ${metric(`${winHold}日均收`, row.pattern_avg_return_pct === null || row.pattern_avg_return_pct === undefined ? '—' : `${fmt(row.pattern_avg_return_pct, 2)}%`, cls(row.pattern_avg_return_pct))}
      ${metric('达标率', row.pattern_target_rate_pct === null || row.pattern_target_rate_pct === undefined ? '—' : `${fmt(row.pattern_target_rate_pct, 1)}%`)}
      ${metric('MA5', row.ma5 === null || row.ma5 === undefined ? '—' : fmt(row.ma5, 2))}
      ${metric('MA5斜率', row.ma5_slope_pct === null || row.ma5_slope_pct === undefined ? '—' : `${fmt(row.ma5_slope_pct, 2)}%`)}
      ${metric('脱离前低', row.close_lift_pct === null || row.close_lift_pct === undefined ? '—' : `${fmt(row.close_lift_pct, 2)}%`)}
      ${metric('最大实体', `${fmt(row.doji_body_pct, 2)}%`)}
      ${metric('形态振幅', `${fmt(row.range5_pct, 2)}%`)}
      ${metric('MA40距', row.ma40_distance === null || row.ma40_distance === undefined ? '—' : `${fmt(row.ma40_distance, 2)}%`)}
    ` : `
      ${metric('最大实体', `${fmt(row.doji_body_pct, 2)}%`)}
      ${metric('4针振幅', `${fmt(row.range5_pct, 2)}%`)}
      ${metric('1/3偏差', `${fmt(row.first_third_gap, 2)}%`)}
      ${metric('2/4偏差', `${fmt(row.second_fourth_gap, 2)}%`)}
      ${metric('1/3收差', `${fmt(row.first_third_close_gap, 2)}%`)}
      ${metric('2/4收差', `${fmt(row.second_fourth_close_gap, 2)}%`)}
      ${metric('高低差', `${fmt(row.level_gap, 2)}%`)}
      ${metric('缺影线', `${row.shadowless_count ?? 0}根`)}
      ${metric('MA40距', row.ma40_distance === null || row.ma40_distance === undefined ? '—' : `${fmt(row.ma40_distance, 2)}%`)}
    `;
  return `<article class="card">
    <div class="card-head">
      <div>
        <a class="stock-link" href="${stockUrl}" target="_blank" rel="noopener noreferrer">
          <div class="stock-title">${esc(row.name || '')}</div>
          <span class="stock-code">${esc(row.code)}${row.pattern_name ? ` · ${esc(row.pattern_name)}` : ''}</span>
        </a>
      </div>
      <div class="score">${fmt(row.score, 1)}</div>
    </div>
    <div class="chart"><a class="chart-link" href="${stockUrl}" target="_blank" rel="noopener noreferrer">${row.chart || ''}</a></div>
    <div class="metrics">
      ${metric('收盘', fmt(row.close, 2))}
      ${metric('涨跌幅', `${fmt(row.pct, 2)}%`, cls(row.pct))}
      ${metric('成交额', `${fmt(row.amount_yi, 2)}亿`)}
      ${metric('总市值', row.market_cap_yi === null || row.market_cap_yi === undefined ? '—' : `${fmt(row.market_cap_yi, 0)}亿`)}
      ${metric('换手', row.turnover === null || row.turnover === undefined ? '—' : `${fmt(row.turnover, 2)}%`)}
      ${patternMetrics}
    </div>
    <div class="reason">${esc(row.reasons || '')}</div>
  </article>`;
}

function render(data, savedText='—') {
  setMeta(data.meta || {}, savedText);
  const rows = data.rows || [];
  if (!rows.length) {
    document.getElementById('result').innerHTML = '<div class="loading">暂无符合条件的股票</div>';
    return;
  }
  document.getElementById('result').innerHTML = `<div class="grid">${rows.map(rowCard).join('')}</div>`;
}

function renderHistory(data) {
  const runs = data.runs || [];
  const page = Number(data.page || historyPage || 1);
  historyPage = page;
  const dates = data.page_trade_dates || [];
  const fromDate = dates.length ? dates[dates.length - 1] : '—';
  const toDate = dates.length ? dates[0] : '—';
  const rowCount = Number(data.page_row_count || 0);
  const rangeText = data.pagination_mode === 'none'
    ? (dates.length
      ? `${rowCount} 条记录 · ${esc(fromDate)} 至 ${esc(toDate)}`
      : '暂无记录')
    : data.pagination_mode === 'rows'
    ? (dates.length
      ? `第 ${page} 页 · ${rowCount} 条记录 · ${esc(fromDate)} 至 ${esc(toDate)}`
      : `第 ${page} 页 · 暂无记录`)
    : (dates.length
      ? `第 ${page} 页 · ${esc(fromDate)} 至 ${esc(toDate)} · ${dates.length} 个交易日`
      : `第 ${page} 页 · 暂无交易日`);
  const pagerActions = data.pagination_mode === 'none' ? '' : `
      <button class="secondary" onclick="changeHistoryPage(-1)" ${data.has_prev ? '' : 'disabled'}>上一页</button>
      <button class="secondary" onclick="changeHistoryPage(1)" ${data.has_next ? '' : 'disabled'}>下一页</button>
  `;
  const pager = `<div class="history-toolbar">
    <div>${rangeText}</div>
    <div class="history-actions">${pagerActions}</div>
  </div>`;
  if (!runs.length) {
    document.getElementById('history').innerHTML = `${pager}<div class="loading">当前页暂无历史命中记录</div>`;
    return;
  }
  document.getElementById('history').innerHTML = `${pager}<div class="history">${runs.map(run => `
    <section class="history-item">
      <div class="history-head" onclick="toggleHistoryRun(this)">
        <div>
          <div class="history-date">${esc(run.trade_date)}</div>
          <div class="history-meta">run ${run.run_id} · 命中 ${run.matched} · 扫描 ${run.scanned ?? '—'} · ${esc(run.created_at || '')}</div>
        </div>
        <div class="history-count">${(run.rows || []).length} 条</div>
      </div>
      <div class="history-body">
        <div class="grid">${(run.rows || []).map(rowCard).join('')}</div>
      </div>
    </section>
  `).join('')}</div>`;
}

function changeHistoryPage(delta) {
  const nextPage = Math.max(1, historyPage + delta);
  if (nextPage === historyPage && delta < 0) return;
  loadHistory(nextPage);
}

function toggleHistoryRun(head) {
  head.closest('.history-item')?.classList.toggle('open');
}

function onPatternTypeChange() {
  historyPage = 1;
  updateBackfillButtons();
  loadLatest(false);
  loadHistory(1);
}

async function loadLatest(refreshHistory=true) {
  document.getElementById('result').innerHTML = '<div class="loading"><span class="spinner"></span>加载最近结果…</div>';
  try {
    const res = await fetch('/api/pattern/latest?' + params());
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || '加载失败');
    render(data, data.meta?.created_at || '已保存');
    if (refreshHistory) loadHistory(1);
  } catch (err) {
    setMeta({}, '—');
    document.getElementById('result').innerHTML = `<div class="loading">${esc(err.message)}</div>`;
  }
}

async function loadHistory(page=historyPage) {
  historyPage = Math.max(1, Number(page || 1));
  document.getElementById('history').innerHTML = '<div class="loading"><span class="spinner"></span>加载历史…</div>';
  try {
    const p = new URLSearchParams(params());
    p.set('hitsOnly', '1');
    p.set('page', String(historyPage));
    p.set('pageSize', String(historyPageSize));
    const res = await fetch('/api/pattern/history?' + p.toString());
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || '历史加载失败');
    renderHistory(data);
  } catch (err) {
    document.getElementById('history').innerHTML = `<div class="loading">历史加载失败：${esc(err.message)}</div>`;
  }
}

async function scan() {
  const btn = document.getElementById('scanBtn');
  btn.disabled = true;
  document.getElementById('result').innerHTML = '<div class="loading"><span class="spinner"></span>扫描中…</div>';
  try {
    const res = await fetch('/api/pattern/scan?' + params());
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || '扫描失败');
    render(data, data.saved ? `run ${data.run_id}` : '未保存');
    historyPage = 1;
    loadHistory(1);
  } catch (err) {
    document.getElementById('result').innerHTML = `<div class="loading">扫描失败：${esc(err.message)}</div>`;
  } finally {
    btn.disabled = false;
  }
}

async function backfillPattern() {
  const btn = document.getElementById('backfillBtn');
  const days = patternBackfillDays();
  const label = patternBackfillLabel();
  btn.disabled = true;
  btn.textContent = '回扫中…';
  document.getElementById('history').innerHTML = `<div class="loading"><span class="spinner"></span>正在回扫最近${label}…</div>`;
  try {
    const p = new URLSearchParams(params());
    p.set('days', String(days));
    if (document.getElementById('patternType').value === 'bottom_reversal') {
      p.set('bottomPatternGroup', 'all');
      p.set('bottomOnlyBullishEngulfing', '0');
    }
    p.delete('save');
    const res = await fetch('/api/pattern/backfill?' + p.toString());
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || '回扫失败');
    for (;;) {
      const job = await loadPatternProgress(true);
      if (job.status === 'done') {
        document.getElementById('statSaved').textContent =
          `${label} ${job.matched_days || 0} 天 / ${job.matched_rows || 0} 条`;
        break;
      }
      if (job.status === 'error') {
        throw new Error(job.error || '回扫失败');
      }
      await sleep(2000);
    }
    historyPage = 1;
    await loadHistory(1);
    await loadLatest();
  } catch (err) {
    document.getElementById('history').innerHTML = `<div class="loading">回扫失败：${esc(err.message)}</div>`;
  } finally {
    btn.disabled = false;
    updateBackfillButtons();
  }
}

async function clearPatternHistory(thenBackfill=false, clearAll=false) {
  const patternSelect = document.getElementById('patternType');
  const patternLabel = clearAll
    ? '全部形态'
    : (patternSelect.options[patternSelect.selectedIndex]?.text || '当前形态');
  const backfillLabel = patternBackfillLabel();
  const message = thenBackfill
    ? `确认清空所有${patternLabel}历史记录，并重新回扫最近${backfillLabel}？`
    : `确认清空所有${patternLabel}历史记录？`;
  if (!window.confirm(message)) return;

  const clearBtn = document.getElementById('clearBtn');
  const clearAllBtn = document.getElementById('clearAllBtn');
  const clearBackfillBtn = document.getElementById('clearBackfillBtn');
  clearBtn.disabled = true;
  clearAllBtn.disabled = true;
  clearBackfillBtn.disabled = true;
  clearBackfillBtn.textContent = thenBackfill ? '清空中…' : clearBackfillBtn.textContent;
  document.getElementById('history').innerHTML = `<div class="loading"><span class="spinner"></span>正在清空${esc(patternLabel)}历史…</div>`;
  try {
    const p = new URLSearchParams(params());
    p.delete('save');
    if (clearAll) p.set('patternType', 'all');
    p.set('confirm', '1');
    const res = await fetch('/api/pattern/clear?' + p.toString(), { method: 'POST' });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || '清空失败');
    document.getElementById('statSaved').textContent =
      `已清空 ${data.deleted_runs || 0} 次 / ${data.deleted_picks || 0} 条`;
    historyPage = 1;
    await loadHistory(1);
    await loadLatest(false);
    if (thenBackfill) await backfillPattern();
  } catch (err) {
    document.getElementById('history').innerHTML = `<div class="loading">清空失败：${esc(err.message)}</div>`;
  } finally {
    clearBtn.disabled = false;
    clearAllBtn.disabled = false;
    clearBackfillBtn.disabled = false;
    updateBackfillButtons();
  }
}

loadIndices();
loadPatternProgress();
updateBackfillButtons();
loadLatest(false);
loadHistory(1);
</script>
</body>
</html>
"""

HIGH_CONFIDENCE_HTML = """
<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>每日高置信选股</title>
<style>
:root {
  --bg:#111318; --surface:#171a21; --surface2:#20242d; --border:#2d3340;
  --text:#d7dce6; --muted:#87909f; --head:#f4f6fb; --accent:#3d7fff;
  --red:#ff5b5f; --green:#39c27f;
}
* { box-sizing:border-box; }
body {
  margin:0; padding:28px 24px; background:var(--bg); color:var(--text);
  font:13px -apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans SC",sans-serif;
}
.header { display:flex; justify-content:space-between; align-items:flex-end; gap:16px; padding-bottom:18px; margin-bottom:18px; border-bottom:1px solid var(--border); }
h1 { margin:0; color:var(--head); font-size:20px; font-weight:600; letter-spacing:0; }
.sub { margin-top:6px; color:var(--muted); font-size:12px; }
.nav { display:flex; gap:8px; flex-wrap:wrap; }
.nav a { color:var(--text); text-decoration:none; border:1px solid var(--border); background:var(--surface2); padding:7px 12px; border-radius:6px; }
.nav a:hover { border-color:var(--accent); }
.actions { display:flex; justify-content:flex-end; align-items:flex-end; gap:10px; margin-bottom:14px; }
.date-field { display:flex; flex-direction:column; gap:5px; color:var(--muted); font-size:10px; }
input {
  width:148px; height:34px; border:1px solid var(--border); border-radius:6px;
  background:var(--surface2); color:var(--text); padding:7px 9px; outline:none; font:12px inherit;
}
input:focus { border-color:var(--accent); }
button {
  height:34px; border:0; border-radius:6px; background:var(--accent); color:white; padding:0 14px;
  cursor:pointer; font:600 12px inherit;
}
.stats { display:grid; grid-template-columns:repeat(auto-fit, minmax(120px,1fr)); gap:10px; margin:14px 0; }
.stat { min-height:58px; border:1px solid var(--border); border-radius:8px; background:var(--surface); padding:10px 12px; }
.stat-label { color:var(--muted); font-size:10px; margin-bottom:6px; }
.stat-value { color:var(--head); font:600 18px "DM Mono","SFMono-Regular",monospace; }
.split { display:grid; grid-template-columns: 1fr 1fr; gap:14px; margin-bottom:14px; }
.panel { border:1px solid var(--border); border-radius:8px; background:var(--surface); overflow:hidden; }
.panel-head { display:flex; justify-content:space-between; align-items:center; gap:10px; padding:10px 12px; border-bottom:1px solid var(--border); color:var(--head); font-weight:600; }
.tags { display:flex; flex-wrap:wrap; gap:6px; padding:10px 12px; }
.tag { border:1px solid var(--border); border-radius:999px; padding:5px 8px; color:var(--text); background:var(--surface2); }
.tag b { color:var(--head); font-family:"DM Mono","SFMono-Regular",monospace; font-weight:600; }
.day { margin-top:14px; border:1px solid var(--border); border-radius:8px; background:var(--surface); overflow:hidden; }
.day-head { display:flex; justify-content:space-between; gap:10px; padding:11px 12px; border-bottom:1px solid var(--border); }
.day-title { color:var(--head); font:600 14px "DM Mono","SFMono-Regular",monospace; }
.day-meta { color:var(--muted); }
.rate { color:var(--green); font-weight:600; }
table { width:100%; border-collapse:collapse; }
th, td { padding:10px 9px; border-bottom:1px solid var(--border); text-align:left; white-space:nowrap; }
th { color:var(--muted); font-size:10px; font-weight:600; background:var(--surface2); }
tr:last-child td { border-bottom:0; }
.code { display:block; margin-top:2px; color:var(--muted); font:10px "DM Mono","SFMono-Regular",monospace; }
.stock-link { color:var(--head); text-decoration:none; font-weight:600; }
.stock-link:hover { color:#8eb1ff; }
.stock-link:hover .code { color:#8eb1ff; }
.num { font-family:"DM Mono","SFMono-Regular",monospace; }
.up { color:var(--red); } .down { color:var(--green); }
.flag { color:var(--red); font-weight:600; }
.empty { min-height:220px; display:flex; align-items:center; justify-content:center; color:var(--muted); }
.progress-box { min-height:220px; display:flex; align-items:center; justify-content:center; color:var(--text); }
.progress-inner { width:min(520px, 100%); border:1px solid var(--border); border-radius:8px; background:var(--surface); padding:16px; }
.progress-title { color:var(--head); font-weight:600; margin-bottom:8px; }
.progress-msg { color:var(--muted); margin-bottom:12px; }
.progress-track { height:8px; background:var(--surface2); border-radius:999px; overflow:hidden; }
.progress-bar { height:100%; width:0; background:var(--accent); transition:width .25s ease; }
@media (max-width: 980px) {
  body { padding:20px 14px; }
  .header { flex-direction:column; align-items:flex-start; }
  .actions { justify-content:flex-start; flex-wrap:wrap; }
  .stats, .split { grid-template-columns:1fr; }
}
</style>
</head>
<body>
<div class="header">
  <div>
    <h1>每日高置信选股</h1>
    <div class="sub">默认最近30个完整交易日 · 固定规则 · K线形态 · 指标耦合 · 小数量优先</div>
  </div>
  <div class="nav">
    <a href="/">行业宽度</a>
    <a href="/pattern">收盘形态</a>
    <a href="/momentum">14:30 选股</a>
    <a href="/surge">涨停概率</a>
    <a href="/csi1000">中证1000择时</a>
  </div>
</div>

<div class="actions">
  <label class="date-field">交易日期（留空最近30日）
    <input id="tradeDate" type="date">
  </label>
  <button onclick="loadData()">筛选</button>
  <button onclick="loadData(true)">重新同步</button>
</div>

<div class="stats" id="stats"></div>

<div class="split">
  <div class="panel">
    <div class="panel-head">形态分布</div>
    <div class="tags" id="patternTags"></div>
  </div>
  <div class="panel">
    <div class="panel-head">耦合分布</div>
    <div class="tags" id="couplingTags"></div>
  </div>
</div>

<div id="result"><div class="empty">加载中…</div></div>

<script>
function esc(v) {
  return String(v ?? '').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
}
function pct(v, digits=1) {
  return `${((Number(v) || 0) * 100).toFixed(digits)}%`;
}
function num(v, digits=2) {
  const n = Number(v);
  return Number.isFinite(n) ? n.toFixed(digits) : '-';
}
function params(refresh=false) {
  const p = new URLSearchParams();
  const date = document.getElementById('tradeDate').value;
  if (date) p.set('date', date);
  if (refresh) p.set('refresh', '1');
  return p;
}
function renderTags(id, rows) {
  const el = document.getElementById(id);
  el.innerHTML = rows.length ? rows.map(([k,v]) => `<span class="tag">${esc(k)} <b>${v}</b></span>`).join('') : '<span class="tag">无</span>';
}
function renderStats(meta) {
  const items = [
    ['覆盖区间', meta.date_start && meta.date_end ? `${meta.date_start} → ${meta.date_end}` : (meta.trade_date || '-')],
    ['交易日数', meta.days || 1],
    ['缺缓存', meta.cache_missing_days || 0],
    ['来源', meta.result_source === 'cache' ? '本地' : '实时'],
    ['日K覆盖', meta.daily_rows],
    ['扫描股票', meta.scanned_stocks],
    ['原始信号', meta.raw_signal_rows],
    ['质量信号', meta.quality_signal_rows],
    ['候选股票', meta.raw_filtered_rows],
    ['输出股票', meta.output_rows],
    ['最低市值', `${meta.min_market_cap_yi ?? '-'}亿`],
    ['市值拉取', meta.market_cap_fetched],
    ['市值缺失', meta.market_cap_missing],
    ['市值过滤', meta.market_cap_filtered],
    ['触板', meta.touch_limit_rows],
    ['封板', meta.close_limit_rows],
  ];
  document.getElementById('stats').innerHTML = items.map(([k,v]) => `
    <div class="stat"><div class="stat-label">${esc(k)}</div><div class="stat-value">${esc(v)}</div></div>
  `).join('');
}
function renderProgress(progress) {
  const pctValue = Math.max(0, Math.min(100, Number(progress.percent) || 0));
  const title = progress.phase === 'error' ? '同步失败' : '正在重新同步';
  document.getElementById('result').innerHTML = `
    <div class="progress-box">
      <div class="progress-inner">
        <div class="progress-title">${esc(title)} ${pctValue.toFixed(0)}%</div>
        <div class="progress-msg">${esc(progress.message || '准备中…')}</div>
        <div class="progress-track"><div class="progress-bar" style="width:${pctValue}%"></div></div>
      </div>
    </div>`;
}
function progressParams() {
  const p = new URLSearchParams();
  const date = document.getElementById('tradeDate').value;
  if (date) p.set('date', date);
  return p;
}
async function pollProgressOnce() {
  const query = progressParams().toString();
  const res = await fetch('/api/high-confidence/progress' + (query ? '?' + query : ''));
  const progress = await res.json();
  renderProgress(progress);
  return progress;
}
function xueqiuUrl(row) {
  const code = String(row.code || '').padStart(6, '0');
  const secucode = String(row.secucode || '').toUpperCase();
  let prefix = secucode.endsWith('.SH') ? 'SH' : 'SZ';
  if (secucode.endsWith('.BJ')) prefix = 'BJ';
  return `https://xueqiu.com/S/${prefix}${code}`;
}
function renderGroup(group) {
  const rows = group.rows.map(r => {
    const changeClass = Number(r.pct_change) >= 0 ? 'up' : 'down';
    return `<tr>
      <td><a class="stock-link" href="${esc(xueqiuUrl(r))}" target="_blank" rel="noopener noreferrer">${esc(r.name)}<span class="code">${esc(r.code)}</span></a></td>
      <td>${esc(r.pattern)}</td>
      <td>${esc(r.coupling)}</td>
      <td class="num">${r.focus_score === undefined || r.focus_score === null ? '-' : num(r.focus_score, 3)}</td>
      <td class="num">${num(r.rank_score, 2)}</td>
      <td class="num">${pct(r.hist_win_rate, 1)}</td>
      <td class="num">${r.hist_samples}</td>
      <td class="num">${r.signal_count}</td>
      <td class="num">${num(r.close, 2)}</td>
      <td class="num ${changeClass}">${num(r.pct_change, 2)}%</td>
      <td class="num">${num(r.turnover, 2)}%</td>
      <td class="num">${r.market_cap_yi === null || r.market_cap_yi === undefined ? '-' : num(r.market_cap_yi, 0)}</td>
      <td class="num">${num(r.amount_yi, 2)}</td>
      <td>${r.touch_limit ? '<span class="flag">触板</span>' : ''}${r.close_limit ? ' <span class="flag">封板</span>' : ''}</td>
    </tr>`;
  }).join('');
  return `<section class="day">
    <div class="day-head">
      <div class="day-title">${esc(group.date)}</div>
      <div class="day-meta">输出 ${group.count} / 候选 ${group.raw_count}</div>
    </div>
    <table>
      <thead><tr><th>股票</th><th>最佳形态</th><th>最佳耦合</th><th>精选分</th><th>评分</th><th>历史胜率</th><th>样本</th><th>信号数</th><th>收盘</th><th>涨跌幅</th><th>换手</th><th>总市值(亿)</th><th>成交额(亿)</th><th>状态</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>
  </section>`;
}
async function loadData(refresh=false) {
  if (refresh) {
    await syncData();
    return;
  }
  document.getElementById('result').innerHTML = '<div class="empty">加载中…</div>';
  const query = params(false).toString();
  const res = await fetch('/api/high-confidence/scan' + (query ? '?' + query : ''));
  const data = await res.json();
  if (!res.ok) {
    document.getElementById('result').innerHTML = `<div class="empty">${esc(data.error || '加载失败')}</div>`;
    return;
  }
  renderStats(data.meta);
  if (data.meta.trade_date && !document.getElementById('tradeDate').value && Number(data.meta.days || 1) <= 1) {
    document.getElementById('tradeDate').value = data.meta.trade_date;
  }
  renderTags('patternTags', data.meta.pattern_counts || []);
  renderTags('couplingTags', data.meta.coupling_counts || []);
  document.getElementById('result').innerHTML = data.groups.length
    ? data.groups.map(renderGroup).join('')
    : '<div class="empty">没有符合条件的股票</div>';
}

async function syncData() {
  document.getElementById('result').innerHTML = '<div class="empty">准备重新同步…</div>';
  const startQuery = params(false).toString();
  const startRes = await fetch('/api/high-confidence/sync' + (startQuery ? '?' + startQuery : ''), { method: 'POST' });
  const startData = await startRes.json();
  if (!startRes.ok) {
    document.getElementById('result').innerHTML = `<div class="empty">${esc(startData.error || '同步启动失败')}</div>`;
    return;
  }
  if (startData.trade_date) {
    document.getElementById('tradeDate').value = startData.trade_date;
  }
  renderProgress(startData.progress || { phase:'queued', percent:1, message:'已启动同步' });

  const progressTimer = setInterval(async () => {
    try {
      const progress = await pollProgressOnce();
      if (progress.phase === 'done') {
        clearInterval(progressTimer);
        await loadData(false);
      } else if (progress.phase === 'error') {
        clearInterval(progressTimer);
      }
    } catch (err) {
      clearInterval(progressTimer);
      document.getElementById('result').innerHTML = `<div class="empty">${esc(err.message || '同步进度读取失败')}</div>`;
      return;
    }
  }, 800);
}
loadData();
</script>
</body>
</html>
"""

SURGE_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>次日涨停/大涨概率扫描</title>
<style>
:root {
  --bg:#101217; --surface:#171b22; --surface2:#202631; --border:#303744;
  --text:#d7dce6; --muted:#8b94a3; --head:#f3f6fb; --accent:#2f80ed;
  --red:#ff5b6e; --green:#22c55e; --yellow:#eab308;
}
* { box-sizing:border-box; }
body {
  margin:0; padding:24px; background:var(--bg); color:var(--text);
  font:13px -apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans SC",sans-serif;
}
header { display:flex; justify-content:space-between; align-items:flex-end; gap:16px; border-bottom:1px solid var(--border); padding-bottom:16px; margin-bottom:16px; }
h1 { margin:0; color:var(--head); font-size:21px; font-weight:650; letter-spacing:0; }
.sub { margin-top:6px; color:var(--muted); font-size:12px; }
.nav { display:flex; gap:8px; flex-wrap:wrap; justify-content:flex-end; }
.nav a { color:var(--text); text-decoration:none; border:1px solid var(--border); background:var(--surface2); padding:7px 10px; border-radius:6px; }
.nav a:hover { border-color:var(--accent); }
.toolbar { display:flex; flex-wrap:wrap; gap:10px; align-items:end; margin-bottom:14px; }
label { display:flex; flex-direction:column; gap:5px; color:var(--muted); font-size:11px; }
input, select {
  height:34px; border:1px solid var(--border); border-radius:6px;
  background:var(--surface2); color:var(--text); padding:7px 9px; outline:none;
}
input[type="date"] { width:150px; }
input[type="number"] { width:112px; }
label.check { flex-direction:row; align-items:center; height:34px; gap:7px; color:var(--text); }
label.check input { width:auto; height:auto; }
button {
  height:34px; border:0; border-radius:6px; background:var(--accent); color:#fff;
  padding:0 14px; cursor:pointer; font-weight:650;
}
button.secondary { background:var(--surface2); border:1px solid var(--border); color:var(--text); }
button:disabled { opacity:.55; cursor:wait; }
.grid { display:grid; grid-template-columns: 1fr 320px; gap:14px; align-items:start; }
.panel { border:1px solid var(--border); border-radius:8px; background:var(--surface); overflow:hidden; }
.panel-head { display:flex; justify-content:space-between; align-items:center; gap:8px; padding:10px 12px; border-bottom:1px solid var(--border); color:var(--head); font-weight:650; }
.stats { display:grid; grid-template-columns:repeat(6, minmax(0, 1fr)); gap:8px; margin-bottom:14px; }
.stat { border:1px solid var(--border); border-radius:8px; background:var(--surface); padding:10px; min-height:58px; }
.stat-label { color:var(--muted); font-size:10px; margin-bottom:6px; }
.stat-value { color:var(--head); font:650 17px "SFMono-Regular",Consolas,monospace; }
.history { max-height:620px; overflow:auto; }
.batch { display:block; width:100%; text-align:left; color:var(--text); background:transparent; border:0; border-bottom:1px solid var(--border); border-radius:0; height:auto; padding:10px 12px; cursor:pointer; }
.batch:hover { background:var(--surface2); }
.batch strong { color:var(--head); }
.batch small { display:block; margin-top:3px; color:var(--muted); }
.progress { padding:16px; }
.progress-title { color:var(--head); font-weight:650; margin-bottom:7px; }
.progress-msg { color:var(--muted); margin-bottom:12px; }
.track { height:8px; background:var(--surface2); border-radius:999px; overflow:hidden; }
.bar { height:100%; width:0; background:var(--accent); transition:width .2s ease; }
table { width:100%; border-collapse:collapse; }
th,td { padding:9px 8px; border-bottom:1px solid var(--border); text-align:left; white-space:nowrap; }
th { color:var(--muted); font-size:10px; background:var(--surface2); font-weight:650; }
tr:last-child td { border-bottom:0; }
.num { font-family:"SFMono-Regular",Consolas,monospace; text-align:right; }
.up { color:var(--red); }
.down { color:var(--green); }
.stock { color:var(--head); font-weight:650; text-decoration:none; }
.stock span { display:block; color:var(--muted); font:10px "SFMono-Regular",Consolas,monospace; margin-top:2px; }
.pill { border:1px solid var(--border); border-radius:999px; padding:3px 7px; background:var(--surface2); }
.empty { min-height:240px; display:flex; align-items:center; justify-content:center; color:var(--muted); }
.scroll { overflow:auto; }
@media (max-width: 1020px) {
  body { padding:16px 12px; }
  header { flex-direction:column; align-items:flex-start; }
  .grid { grid-template-columns:1fr; }
  .stats { grid-template-columns:repeat(2, minmax(0, 1fr)); }
}
</style>
</head>
<body>
<header>
  <div>
    <h1>次日涨停/大涨概率扫描</h1>
    <div class="sub">实时触发扫描 · 每次保存批次 · 统计同股票同形态/耦合的次日触板和高点大涨概率</div>
  </div>
  <nav class="nav">
    <a href="/">行业宽度</a>
    <a href="/pattern">收盘形态</a>
    <a href="/momentum">14:30 选股</a>
    <a href="/high-confidence">高置信小集合</a>
    <a href="/surge">涨停概率</a>
    <a href="/csi1000">中证1000择时</a>
  </nav>
</header>

<div class="toolbar">
  <label>交易日
    <input id="tradeDate" type="date">
  </label>
  <label>历史交易日
    <input id="historyDays" type="number" value="1800" min="300" max="5000">
  </label>
  <label>大涨阈值%
    <input id="bigThreshold" type="number" value="7" min="3" max="20" step="0.5">
  </label>
  <label>最小样本
    <input id="minSamples" type="number" value="50" min="5" max="500">
  </label>
  <label>信号日最低涨跌%
    <input id="minSignalPct" type="number" value="3" min="-20" max="20" step="0.5">
  </label>
  <label>信号日最高涨跌%
    <input id="maxSignalPct" type="number" value="8.8" min="-20" max="20" step="0.1">
  </label>
  <label>最低成交额(亿)
    <input id="minAmountYi" type="number" value="5" min="0" max="200" step="0.5">
  </label>
  <label>最低高点大涨率%
    <input id="minNextHighRate" type="number" value="30" min="0" max="100" step="1">
  </label>
  <label>最低触板率%
    <input id="minTouchLimitRate" type="number" value="20" min="0" max="100" step="1">
  </label>
  <label class="check"><input id="continuationOnly" type="checkbox" checked> 强势延续</label>
  <label class="check"><input id="excludeSignalLimit" type="checkbox" checked> 排除已封板</label>
  <label class="check"><input id="only10cm" type="checkbox" checked> 只做10cm</label>
  <button id="scanBtn" onclick="startScan()">实时扫描并保存</button>
  <button class="secondary" onclick="loadLatest()">最新批次</button>
  <button class="secondary" onclick="loadHistory()">刷新历史</button>
</div>

<div class="stats" id="stats"></div>

<div class="grid">
  <main class="panel">
    <div class="panel-head">
      <span id="resultTitle">最新结果</span>
      <span id="resultMeta" class="pill">-</span>
    </div>
    <div id="result"><div class="empty">加载中…</div></div>
  </main>
  <aside class="panel">
    <div class="panel-head">历史批次</div>
    <div id="history" class="history"><div class="empty">加载中…</div></div>
  </aside>
</div>

<script>
function esc(v) {
  return String(v ?? '').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
}
function num(v, d=2) {
  const n = Number(v);
  return Number.isFinite(n) ? n.toFixed(d) : '-';
}
function pct(v, d=1) {
  const n = Number(v);
  return Number.isFinite(n) ? (n * 100).toFixed(d) + '%' : '-';
}
function queryParams() {
  const p = new URLSearchParams();
  const date = document.getElementById('tradeDate').value;
  if (date) p.set('date', date);
  p.set('historyDays', document.getElementById('historyDays').value || '1800');
  p.set('bigThreshold', document.getElementById('bigThreshold').value || '7');
  p.set('minSamples', document.getElementById('minSamples').value || '80');
  p.set('minSignalPct', document.getElementById('minSignalPct').value || '3');
  p.set('maxSignalPct', document.getElementById('maxSignalPct').value || '8.8');
  p.set('minAmountYi', document.getElementById('minAmountYi').value || '5');
  p.set('minNextHighRate', document.getElementById('minNextHighRate').value || '30');
  p.set('minTouchLimitRate', document.getElementById('minTouchLimitRate').value || '20');
  p.set('top', '10');
  p.set('continuationOnly', document.getElementById('continuationOnly').checked ? '1' : '0');
  p.set('excludeSignalLimit', document.getElementById('excludeSignalLimit').checked ? '1' : '0');
  p.set('only10cm', document.getElementById('only10cm').checked ? '1' : '0');
  return p;
}
function xueqiu(row) {
  const code = String(row.code || '').padStart(6, '0');
  const sec = String(row.secucode || '').toUpperCase();
  let prefix = sec.endsWith('.SH') ? 'SH' : 'SZ';
  if (sec.endsWith('.BJ')) prefix = 'BJ';
  return `https://xueqiu.com/S/${prefix}${code}`;
}
function renderStats(batch) {
  const items = [
    ['批次', batch?.id || '-'],
    ['交易日', batch?.trade_date || '-'],
    ['状态', batch?.status || '-'],
    ['候选', batch?.row_count ?? '-'],
    ['过滤前', batch?.pre_trade_filter_count ?? '-'],
    ['原始信号', batch?.raw_signal_count ?? '-'],
    ['股票数', batch?.stock_count ?? '-'],
    ['历史日', batch?.history_days ?? '-'],
    ['大涨阈值', `${num(batch?.big_threshold, 1)}%`],
    ['最小样本', batch?.min_samples ?? '-'],
    ['排除封板', batch?.exclude_signal_limit ? '是' : '否'],
    ['耗时', batch?.elapsed_s ? `${num(batch.elapsed_s, 1)}s` : '-'],
    ['创建', batch?.created_at || '-'],
    ['更新', batch?.updated_at || '-'],
  ];
  document.getElementById('stats').innerHTML = items.map(([k,v]) => `
    <div class="stat"><div class="stat-label">${esc(k)}</div><div class="stat-value">${esc(v)}</div></div>
  `).join('');
}
function renderProgress(progress) {
  const p = Math.max(0, Math.min(100, Number(progress.percent) || 0));
  document.getElementById('resultTitle').textContent = `扫描批次 ${progress.batch_id || '-'}`;
  document.getElementById('resultMeta').textContent = progress.status || '-';
  document.getElementById('result').innerHTML = `
    <div class="progress">
      <div class="progress-title">${esc(progress.phase || progress.status || 'running')} ${p.toFixed(0)}%</div>
      <div class="progress-msg">${esc(progress.message || '扫描中…')}</div>
      <div class="track"><div class="bar" style="width:${p}%"></div></div>
    </div>`;
}
function renderRows(rows) {
  if (!rows.length) return '<div class="empty">该批次没有候选</div>';
  return `<div class="scroll"><table>
    <thead><tr>
      <th>股票</th><th>形态</th><th>耦合</th><th class="num">样本</th>
      <th class="num">次日触板</th><th class="num">次日封板</th><th class="num">高点>=阈值</th>
      <th class="num">均高%</th><th class="num">信号日涨跌%</th><th>信号状态</th><th class="num">评分</th>
    </tr></thead>
    <tbody>${rows.map(r => {
      const cls = Number(r.pct_change) >= 0 ? 'up' : 'down';
      return `<tr>
        <td><a class="stock" href="${esc(xueqiu(r))}" target="_blank" rel="noopener noreferrer">${esc(r.name)}<span>${esc(r.code)}</span></a></td>
        <td>${esc(r.pattern)}</td>
        <td>${esc(r.coupling)}</td>
        <td class="num">${esc(r.surge_samples)}</td>
        <td class="num up">${pct(r.next_touch_limit_rate)}</td>
        <td class="num up">${pct(r.next_close_limit_rate)}</td>
        <td class="num up">${pct(r.next_high_ge_big_rate)}</td>
        <td class="num">${num(r.avg_next_high_gain_pct, 2)}</td>
        <td class="num ${cls}">${num(r.pct_change, 2)}</td>
        <td>${r.signal_close_limit ? '<span class="up">封板</span>' : (r.signal_touch_limit ? '<span class="up">触板</span>' : '')}</td>
        <td class="num">${num(r.surge_score, 2)}</td>
      </tr>`;
    }).join('')}</tbody>
  </table></div>`;
}
function renderBatch(data) {
  const batch = data.batch || {};
  renderStats(batch);
  document.getElementById('resultTitle').textContent = `批次 ${batch.id || '-'}`;
  document.getElementById('resultMeta').textContent = `${batch.trade_date || '-'} · ${batch.row_count || 0}只`;
  document.getElementById('result').innerHTML = renderRows(data.rows || []);
}
function renderHistory(data) {
  const rows = data.batches || [];
  document.getElementById('history').innerHTML = rows.length ? rows.map(b => `
    <button class="batch" onclick="loadBatch(${Number(b.id)})">
      <strong>#${esc(b.id)} ${esc(b.trade_date || '-')} · ${esc(b.row_count || 0)}只</strong>
      <small>${esc(b.status)} · 大涨阈值 ${num(b.big_threshold,1)}% · 样本 ${esc(b.min_samples)} · ${esc(b.created_at)}</small>
    </button>
  `).join('') : '<div class="empty">暂无历史批次</div>';
}
async function loadHistory() {
  const res = await fetch('/api/surge/history');
  const data = await res.json();
  if (!res.ok) throw new Error(data.error || '历史加载失败');
  renderHistory(data);
}
async function loadBatch(id) {
  const res = await fetch('/api/surge/batch/' + encodeURIComponent(id));
  const data = await res.json();
  if (!res.ok) throw new Error(data.error || '批次加载失败');
  renderBatch(data);
}
async function loadLatest() {
  const res = await fetch('/api/surge/latest');
  const data = await res.json();
  if (!res.ok) {
    document.getElementById('result').innerHTML = `<div class="empty">${esc(data.error || '暂无结果')}</div>`;
    return;
  }
  renderBatch(data);
}
async function poll(batchId) {
  for (;;) {
    const res = await fetch('/api/surge/progress');
    const progress = await res.json();
    renderProgress(progress);
    if (progress.status === 'done') {
      await loadBatch(batchId || progress.batch_id);
      await loadHistory();
      document.getElementById('scanBtn').disabled = false;
      return;
    }
    if (progress.status === 'error') {
      document.getElementById('scanBtn').disabled = false;
      return;
    }
    await new Promise(resolve => setTimeout(resolve, 1500));
  }
}
async function startScan() {
  const btn = document.getElementById('scanBtn');
  btn.disabled = true;
  renderProgress({status:'queued', phase:'queued', percent:1, message:'正在启动扫描'});
  try {
    const res = await fetch('/api/surge/scan?' + queryParams().toString(), {method:'POST'});
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || '启动失败');
    renderProgress(data.progress || {status:'running', percent:1, message:'扫描任务已启动'});
    poll(data.batch_id);
  } catch (err) {
    document.getElementById('result').innerHTML = `<div class="empty">${esc(err.message)}</div>`;
    btn.disabled = false;
  }
}
loadHistory().catch(err => { document.getElementById('history').innerHTML = `<div class="empty">${esc(err.message)}</div>`; });
loadLatest().catch(() => {});
</script>
</body>
</html>
"""

CSI1000_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>中证1000择时</title>
<style>
  :root {
    --bg:#0f1218; --panel:#171b24; --panel2:#1e2430; --line:#2b3242;
    --text:#d7dce7; --muted:#8b93a4; --head:#ffffff;
    --red:#ff5c7a; --green:#19c37d; --blue:#4d8dff; --yellow:#e5b84b;
  }
  * { box-sizing:border-box; }
  body {
    margin:0; background:var(--bg); color:var(--text);
    font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans SC",sans-serif;
  }
  .wrap { max-width:1180px; margin:0 auto; padding:24px; }
  header {
    display:flex; justify-content:space-between; gap:18px; align-items:flex-end;
    border-bottom:1px solid var(--line); padding-bottom:18px; margin-bottom:18px;
  }
  h1 { margin:0; font-size:22px; font-weight:650; color:var(--head); }
  .sub { margin-top:4px; color:var(--muted); font-size:12px; }
  nav { display:flex; gap:8px; flex-wrap:wrap; justify-content:flex-end; }
  nav a, button, select {
    border:1px solid var(--line); background:var(--panel2); color:var(--text);
    border-radius:6px; padding:7px 11px; text-decoration:none; font:inherit; cursor:pointer;
  }
  button.primary { background:var(--blue); border-color:var(--blue); color:#fff; }
  button:disabled { opacity:.55; cursor:wait; }
  .grid { display:grid; grid-template-columns:1.1fr .9fr; gap:14px; margin-bottom:14px; }
  .section { background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:16px; }
  .label { color:var(--muted); font-size:12px; margin-bottom:4px; }
  .state { display:flex; align-items:center; gap:10px; flex-wrap:wrap; }
  .state strong { font-size:28px; color:var(--head); }
  .pill { display:inline-flex; align-items:center; border:1px solid var(--line); border-radius:999px; padding:3px 9px; font-size:12px; color:var(--muted); }
  .pill.long { color:var(--green); border-color:rgba(25,195,125,.45); }
  .pill.short { color:var(--red); border-color:rgba(255,92,122,.45); }
  .pill.flat { color:var(--yellow); border-color:rgba(229,184,75,.45); }
  .metrics { display:grid; grid-template-columns:repeat(4, minmax(0, 1fr)); gap:10px; margin-top:14px; }
  .latest-metrics { display:none; }
  .metric { background:var(--panel2); border:1px solid var(--line); border-radius:7px; padding:11px; min-height:70px; }
  .metric .v { margin-top:4px; color:var(--head); font-size:18px; font-weight:650; }
  .toolbar { display:flex; justify-content:space-between; align-items:center; gap:12px; margin:18px 0 10px; }
  .toolbar h2 { margin:0; color:var(--head); font-size:17px; }
  .controls { display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
  table { width:100%; border-collapse:collapse; background:var(--panel); border:1px solid var(--line); border-radius:8px; overflow:hidden; }
  th, td { padding:10px 9px; border-bottom:1px solid var(--line); text-align:left; white-space:nowrap; }
  th { color:var(--muted); font-size:12px; font-weight:500; background:#151923; }
  td.num { text-align:right; font-variant-numeric:tabular-nums; }
  tr:last-child td { border-bottom:0; }
  .pos { color:var(--green); }
  .neg { color:var(--red); }
  .muted { color:var(--muted); }
  .reason { max-width:160px; overflow:hidden; text-overflow:ellipsis; }
  .empty { padding:36px; text-align:center; color:var(--muted); border:1px solid var(--line); border-radius:8px; background:var(--panel); }
  @media (max-width: 860px) {
    .wrap { padding:16px; }
    header { align-items:flex-start; flex-direction:column; }
    .grid { grid-template-columns:1fr; }
    .metrics { grid-template-columns:repeat(2, minmax(0, 1fr)); }
    .table-scroll { overflow-x:auto; }
  }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div>
      <h1>中证1000择时监控</h1>
      <div class="sub">中证1000择时模型 · 本地行情与宽度数据</div>
    </div>
    <nav>
      <a href="/">行业宽度</a>
      <a href="/pattern">收盘形态</a>
      <a href="/momentum">14:30 选股</a>
      <a href="/high-confidence">高置信小集合</a>
      <a href="/surge">涨停概率</a>
    </nav>
  </header>

  <div class="grid">
    <section class="section">
      <div class="label">当前策略状态</div>
      <div class="state">
        <strong id="latestState">加载中</strong>
        <span class="pill" id="latestDate">-</span>
        <span class="pill" id="latestAction">-</span>
      </div>
      <div class="sub" id="latestReason">-</div>
      <div class="metrics latest-metrics">
        <div class="metric"><div class="label">中证1000收盘</div><div class="v" id="mClose">-</div></div>
        <div class="metric"><div class="label">1000宽度MA3</div><div class="v" id="mCsi">-</div></div>
        <div class="metric"><div class="label">300宽度MA3</div><div class="v" id="mHs300">-</div></div>
        <div class="metric"><div class="label">量比5/20</div><div class="v" id="mVol">-</div></div>
      </div>
    </section>

    <section class="section">
      <div class="label">区间交易归因</div>
      <div class="metrics">
        <div class="metric"><div class="label">交易笔数</div><div class="v" id="sTrades">-</div></div>
        <div class="metric"><div class="label">交易胜率</div><div class="v" id="sWin">-</div></div>
        <div class="metric"><div class="label">多头交易贡献</div><div class="v" id="sLong">-</div></div>
        <div class="metric"><div class="label">空头交易贡献</div><div class="v" id="sShort">-</div></div>
      </div>
      <div class="sub" id="sRange">-</div>
    </section>
  </div>

  <div class="toolbar">
    <h2>已平仓交易明细</h2>
    <div class="controls">
      <select id="days">
        <option value="180" selected>最近半年</option>
        <option value="365">最近一年</option>
        <option value="730">最近两年</option>
      </select>
      <button id="reloadBtn">刷新视图</button>
      <button class="primary" id="runBtn">重算信号</button>
      <button class="primary" id="realtimeBtn">运行今日信号</button>
    </div>
  </div>
  <div class="table-scroll" id="tableWrap"></div>
</div>

<script>
const $ = id => document.getElementById(id);
const fmt = (v, d=2) => v === null || v === undefined || Number.isNaN(Number(v)) ? '-' : Number(v).toFixed(d);
const pct = (v, d=2) => v === null || v === undefined || Number.isNaN(Number(v)) ? '-' : Number(v).toFixed(d) + '%';
const ratioPct = (v, d=2) => v === null || v === undefined || Number.isNaN(Number(v)) ? '-' : (Number(v) * 100).toFixed(d) + '%';
const cls = v => Number(v) >= 0 ? 'pos' : 'neg';
function stateClass(text) {
  if (text === '多1000') return 'pill long';
  if (text === '空1000') return 'pill short';
  return 'pill flat';
}
function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}
function render(data) {
  const sig = data.latest_signal || {};
  const payload = sig.payload || {};
  $('latestState').textContent = sig.trade_state || '空仓';
  $('latestDate').textContent = sig.trade_date || '-';
  $('latestDate').className = stateClass(sig.trade_state || '空仓');
  $('latestAction').textContent = sig.action || '-';
  $('latestReason').textContent = sig.reason || '-';
  $('mClose').textContent = fmt(sig.csi_close ?? payload.close, 2);
  $('mCsi').textContent = fmt(sig.csi_score_ma3 ?? payload.csi_score_ma3, 1);
  $('mHs300').textContent = fmt(sig.hs300_score_ma3 ?? payload.hs300_score_ma3, 1);
  $('mVol').textContent = fmt(sig.vol_ratio_5_20 ?? payload.vol_ratio_5_20, 3);

  const s = data.summary || {};
  $('sTrades').textContent = s.trade_count ?? '-';
  $('sWin').textContent = pct(s.win_rate_pct, 1);
  $('sLong').textContent = pct(s.long_return_sum_pct, 2);
  $('sLong').className = 'v ' + cls(s.long_return_sum_pct || 0);
  $('sShort').textContent = pct(s.short_return_sum_pct, 2);
  $('sShort').className = 'v ' + cls(s.short_return_sum_pct || 0);
  $('sRange').textContent = `${s.start_date || '-'} 至 ${s.end_date || '-'} · 已平仓交易收益率加总 · 多头 ${s.long_count || 0} 笔 / 空头 ${s.short_count || 0} 笔`;

  const rows = data.trades || [];
  if (!rows.length) {
    $('tableWrap').innerHTML = '<div class="empty">当前区间暂无已平仓交易</div>';
    return;
  }
  let html = `<table><thead><tr>
    <th>方向</th><th>开始日期</th><th>终止日期</th><th class="num">开仓价</th>
    <th class="num">平仓/最新价</th><th class="num">持有天数</th><th class="num">单笔收益</th>
    <th>开仓原因</th><th>平仓原因</th>
  </tr></thead><tbody>`;
  for (const r of rows) {
    const p = Number(r.return_pct);
    const endText = r.is_open_mark ? '持有中' : (r.exit_date || '-');
    html += `<tr>
      <td><span class="${stateClass(r.direction_text)}">${esc(r.direction_text)}</span></td>
      <td>${esc(r.entry_date)}</td>
      <td>${esc(endText)}</td>
      <td class="num">${fmt(r.entry_price, 2)}</td>
      <td class="num">${fmt(r.exit_price, 2)}</td>
      <td class="num">${r.hold_days ?? '-'}</td>
      <td class="num ${cls(p)}">${pct(p, 2)}</td>
      <td class="reason" title="${esc(r.entry_reason)}">${esc(r.entry_reason || '-')}</td>
      <td class="reason" title="${esc(r.exit_reason_text)}">${esc(r.exit_reason_text || '-')}</td>
    </tr>`;
  }
  html += '</tbody></table>';
  $('tableWrap').innerHTML = html;
}
async function loadData() {
  $('tableWrap').innerHTML = '<div class="empty">加载中</div>';
  const res = await fetch('/api/csi1000-timing?days=' + encodeURIComponent($('days').value));
  const data = await res.json();
  if (!res.ok) throw new Error(data.error || '加载失败');
  render(data);
}
async function rerun() {
  const btn = $('runBtn');
  btn.disabled = true;
  btn.textContent = '重算中';
  try {
    const res = await fetch('/api/csi1000-timing/refresh', {method:'POST'});
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || '信号重算失败');
    await loadData();
  } finally {
    btn.disabled = false;
    btn.textContent = '重算信号';
  }
}
async function runRealtimeToday() {
  const btn = $('realtimeBtn');
  btn.disabled = true;
  btn.textContent = '运行中';
  $('tableWrap').innerHTML = '<div class="empty">正在执行今日信号流程</div>';
  try {
    const res = await fetch('/api/csi1000-timing/run-today', {method:'POST'});
    const data = await res.json();
    if (!res.ok) {
      const msg = data.stderr_tail || data.stdout_tail || data.error || '今日信号运行失败';
      throw new Error(msg);
    }
    await loadData();
  } finally {
    btn.disabled = false;
    btn.textContent = '运行今日信号';
  }
}
$('days').addEventListener('change', loadData);
$('reloadBtn').addEventListener('click', loadData);
$('runBtn').addEventListener('click', rerun);
$('realtimeBtn').addEventListener('click', runRealtimeToday);
loadData().catch(err => { $('tableWrap').innerHTML = `<div class="empty">${esc(err.message)}</div>`; });
</script>
</body>
</html>
"""

