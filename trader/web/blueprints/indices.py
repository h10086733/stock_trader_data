"""指数蓝图：/api/indices, /api/index-constituents, /api/stats"""
from flask import Blueprint, jsonify, request
from trader.core.db import connect_existing
from trader.web.utils import to_int_arg

bp = Blueprint("indices", __name__)


@bp.route("/api/indices")
def api_indices():
    conn = connect_existing()
    try:
        rows = conn.execute("SELECT code, name FROM indices ORDER BY code").fetchall()
        indices = [{"code": "", "name": "全部市场"}] + [{"code": r["code"], "name": r["name"]} for r in rows]
        return jsonify({"indices": indices})
    finally:
        conn.close()


@bp.route("/api/index-constituents")
def api_index_constituents():
    code = (request.args.get("code") or "").strip()
    limit = to_int_arg("limit", 10, 1, 50)
    if not code:
        return jsonify({"error": "缺少指数代码"}), 400

    conn = connect_existing()
    try:
        idx = conn.execute("SELECT code, name FROM indices WHERE code = ?", (code,)).fetchone()
        if not idx:
            return jsonify({"error": "指数不存在"}), 404

        summary = conn.execute("""
            SELECT COUNT(*) AS total_count,
                   SUM(CASE WHEN weight IS NOT NULL THEN 1 ELSE 0 END) AS weight_count,
                   SUM(CASE WHEN weight IS NOT NULL THEN weight ELSE 0 END) AS weight_sum,
                   MAX(weight_date) AS weight_date,
                   MAX(updated_at) AS updated_at
            FROM index_constituents WHERE index_code = ?
        """, (code,)).fetchone()

        weight_count = summary["weight_count"] or 0
        total_count = summary["total_count"] or 0
        weight_sum = summary["weight_sum"] or 0
        weight_coverage = weight_count / total_count if total_count else 0
        weight_complete = weight_coverage >= 0.98 and weight_sum >= 95.0
        if weight_count > 0:
            rows = conn.execute("""
                SELECT ic.stock_code, COALESCE(s.name, ic.stock_name) AS stock_name,
                       ic.exchange, ic.weight, ic.weight_date
                FROM index_constituents ic
                LEFT JOIN stocks s ON s.code = ic.stock_code
                WHERE ic.index_code = ? AND ic.weight IS NOT NULL
                ORDER BY ic.weight DESC, ic.stock_code ASC LIMIT ?
            """, (code, limit)).fetchall()
        else:
            rows = conn.execute("""
                SELECT ic.stock_code, COALESCE(s.name, ic.stock_name) AS stock_name,
                       ic.exchange, ic.weight, ic.weight_date
                FROM index_constituents ic
                LEFT JOIN stocks s ON s.code = ic.stock_code
                WHERE ic.index_code = ?
                ORDER BY ic.stock_code ASC LIMIT ?
            """, (code, limit)).fetchall()

        return jsonify({
            "code": idx["code"], "name": idx["name"],
            "total_count": total_count, "weight_count": weight_count,
            "weight_sum": weight_sum, "weight_coverage": weight_coverage,
            "weight_complete": weight_complete,
            "weight_date": summary["weight_date"], "updated_at": summary["updated_at"],
            "rows": [
                {"code": r["stock_code"], "name": r["stock_name"],
                 "exchange": r["exchange"], "weight": r["weight"],
                 "weight_date": r["weight_date"]}
                for r in rows
            ],
        })
    finally:
        conn.close()


@bp.route("/api/stats")
def api_stats():
    days = min(max(int(request.args.get("days", 5)), 1), 250)
    conn = connect_existing()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT DISTINCT trade_date FROM index_daily_stats
            ORDER BY trade_date DESC LIMIT ?
        """, (days,))
        dates = [r["trade_date"] for r in cur.fetchall()]
        dates_asc = list(reversed(dates))

        cur.execute("SELECT code, name FROM indices ORDER BY code")
        indices = cur.fetchall()

        result = []
        for idx in indices:
            code = idx["code"]
            if not dates_asc:
                result.append({"code": code, "name": idx["name"], "ma3": {}, "details": {}})
                continue
            cur.execute("""
                SELECT trade_date, net_value, high_count, low_count, valid_count, total_count
                FROM index_daily_stats
                WHERE index_code = ? AND trade_date <= ?
                ORDER BY trade_date DESC LIMIT ?
            """, (code, dates_asc[-1], days + 2))
            all_rows = list(reversed(cur.fetchall()))
            nv_series = {r["trade_date"]: r["net_value"] for r in all_rows}
            detail_map = {r["trade_date"]: r for r in all_rows}
            all_dates_sorted = sorted(nv_series.keys())
            ma3_map = {}
            for i, td in enumerate(all_dates_sorted):
                window = [nv_series[all_dates_sorted[j]]
                          for j in range(max(0, i - 2), i + 1)
                          if nv_series.get(all_dates_sorted[j]) is not None]
                ma3_map[td] = round(sum(window) / len(window), 6) if window else None

            ma3 = {}
            net_value = {}
            details = {}
            for td in dates_asc:
                if td in ma3_map:
                    ma3[td] = ma3_map[td]
                if td in detail_map:
                    r = detail_map[td]
                    net_value[td] = r["net_value"]
                    details[td] = {
                        "net_value": r["net_value"], "ma3": ma3_map.get(td),
                        "high_count": r["high_count"], "low_count": r["low_count"],
                        "valid_count": r["valid_count"], "total_count": r["total_count"],
                    }
            result.append({"code": code, "name": idx["name"],
                           "ma3": ma3, "net_value": net_value, "details": details})

        return jsonify({"dates": dates_asc, "indices": result})
    finally:
        conn.close()
