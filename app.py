"""
行业宽度指标查询页面 — app.py
==============================
启动：
  pip install flask
  python app.py
访问：http://localhost:5000
"""

from flask import Flask, jsonify, render_template_string, request
import sqlite3

app = Flask(__name__)
DB_PATH = "stock_data.db"


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ─────────────────────────────────────────────────────────────────
# HTML 模板
# ─────────────────────────────────────────────────────────────────
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

<script>
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
    html += `<tr><td class="name-cell">${idx.name}<span class="idx-code">${idx.code}</span></td>`;
    displayDates.forEach(d => {
      const v = idx.ma3[d];
      if (v === undefined || v === null) {
        html += '<td class="val-cell"><span class="empty-cell">—</span></td>';
        return;
      }
      const pct = (v * 100).toFixed(2);
      const cls = v >  0.001 ? 'positive' : v < -0.001 ? 'negative' : 'zero';
      const barW = Math.min(Math.abs(v) * 100, 50);
      html += `<td class="val-cell">
        <div class="val-inner ${cls}">
          <span class="val-number">${pct}</span>
          <div class="val-bar"><div class="val-bar-fill" style="width:${barW}%"></div></div>
        </div></td>`;
    });
    html += '</tr>';
  });

  html += '</tbody></table>';
  document.getElementById('tableWrap').innerHTML = html;
}

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


# ─────────────────────────────────────────────────────────────────
# 路由
# ─────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/api/stats")
def api_stats():
    days = int(request.args.get("days", 5))
    days = min(max(days, 1), 250)

    conn = get_db()
    cur  = conn.cursor()

    # 取最近 N 个交易日
    cur.execute("""
        SELECT DISTINCT trade_date
        FROM index_daily_stats
        ORDER BY trade_date DESC
        LIMIT ?
    """, (days,))
    dates     = [r["trade_date"] for r in cur.fetchall()]
    dates_asc = list(reversed(dates))

    # 取所有指数
    cur.execute("SELECT code, name FROM indices ORDER BY code")
    indices = cur.fetchall()

    result = []
    for idx in indices:
        code = idx["code"]

        if not dates_asc:
            result.append({"code": code, "name": idx["name"],
                           "ma3": {}, "details": {}})
            continue

        # 多取2天历史数据用于计算MA3（最早显示日期往前2天）
        cur.execute("""
            SELECT trade_date, net_value, high_count, low_count, valid_count, total_count
            FROM index_daily_stats
            WHERE index_code = ?
              AND trade_date <= ?
              AND trade_date >= (
                  SELECT trade_date FROM index_daily_stats
                  WHERE index_code = ?
                    AND trade_date <= ?
                  ORDER BY trade_date ASC
                  LIMIT 1 OFFSET 0
              )
            ORDER BY trade_date ASC
        """, (code, dates_asc[-1], code, dates_asc[0]))

        # 用 Python 算滑动均值，不依赖窗口函数
        all_rows = cur.fetchall()

        # 先建完整的 net_value 时间序列
        nv_series = {r["trade_date"]: r["net_value"] for r in all_rows}
        detail_map = {r["trade_date"]: r for r in all_rows}

        # 取所有日期排序（包括比显示窗口更早的）
        all_dates_sorted = sorted(nv_series.keys())

        # 计算每个日期的 MA3
        ma3_map = {}
        for i, td in enumerate(all_dates_sorted):
            window = [nv_series[all_dates_sorted[j]]
                      for j in range(max(0, i-2), i+1)
                      if nv_series.get(all_dates_sorted[j]) is not None]
            ma3_map[td] = round(sum(window) / len(window), 6) if window else None

        # 只输出用户要看的日期
        ma3    = {}
        details= {}
        for td in dates_asc:
            if td in ma3_map:
                ma3[td] = ma3_map[td]
            if td in detail_map:
                r = detail_map[td]
                details[td] = {
                    "net_value":   r["net_value"],
                    "ma3":         ma3_map.get(td),
                    "high_count":  r["high_count"],
                    "low_count":   r["low_count"],
                    "valid_count": r["valid_count"],
                    "total_count": r["total_count"],
                }

        result.append({
            "code":    code,
            "name":    idx["name"],
            "ma3":     ma3,
            "details": details,
        })

    conn.close()
    return jsonify({"dates": dates_asc, "indices": result})


# ─────────────────────────────────────────────────────────────────
# 启动
# ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
