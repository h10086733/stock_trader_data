"""高置信选股的市值获取、缓存与过滤。

来源：app.py 的 fetch_eastmoney_market_caps / load_hc_market_caps_from_db /
save_hc_market_caps / estimate_historical_cap / load_or_fetch_hc_market_caps /
apply_hc_market_cap_filter。
"""
from __future__ import annotations

import time

import requests

from trader.core import config
from trader.core.codes import infer_market
from trader.core.http import eastmoney_headers
from trader.core.utils import chunked, to_float
from trader.strategies.high_confidence import config as hc


def fetch_eastmoney_market_caps(codes):
    caps = {}
    if not codes:
        return caps
    fields = "f12,f2,f20,f21"
    headers = eastmoney_headers()
    headers["Referer"] = "https://quote.eastmoney.com/"
    for batch in chunked(list(dict.fromkeys(codes)), 80):
        params = {
            "fltt": 2,
            "invt": 2,
            "fields": fields,
            "secids": ",".join(f"{infer_market(code)}.{code}" for code in batch),
        }
        diff = []
        for url in config.EASTMONEY_QUOTE_URLS:
            for attempt in range(2):
                try:
                    resp = requests.get(url, params=params, headers=headers, timeout=8)
                    resp.raise_for_status()
                    data = resp.json().get("data") or {}
                    diff = data.get("diff") or []
                    if diff:
                        break
                except Exception:
                    time.sleep(0.25 * (attempt + 1))
            if diff:
                break
        for item in diff:
            code = str(item.get("f12") or "")
            price = to_float(item.get("f2"))
            total_cap = to_float(item.get("f20"))
            float_cap = to_float(item.get("f21"))
            caps[code] = {
                "price": price,
                "market_cap_yi": total_cap / 100000000.0 if total_cap else None,
                "float_market_cap_yi": float_cap / 100000000.0 if float_cap else None,
            }
    return caps


def load_hc_market_caps_from_db(conn, trade_date, codes):
    if not codes:
        return {}
    placeholders = ",".join("?" for _ in codes)
    rows = conn.execute(
        f"""
        SELECT code, market_cap_yi, float_market_cap_yi, source
        FROM high_confidence_market_caps
        WHERE trade_date = ? AND code IN ({placeholders})
        """,
        [trade_date, *codes],
    ).fetchall()
    caps = {
        str(row["code"]).zfill(6): {
            "market_cap_yi": row["market_cap_yi"],
            "float_market_cap_yi": row["float_market_cap_yi"],
            "source": row["source"] or "db",
        }
        for row in rows
    }
    missing_codes = [code for code in codes if code not in caps]
    if not missing_codes:
        return caps
    daily_placeholders = ",".join("?" for _ in missing_codes)
    daily_rows = conn.execute(
        f"""
        SELECT code, market_cap_yi, float_market_cap_yi
        FROM daily_prices
        WHERE trade_date = ?
          AND code IN ({daily_placeholders})
          AND market_cap_yi IS NOT NULL
        """,
        [trade_date, *missing_codes],
    ).fetchall()
    from_daily_prices = {
        str(row["code"]).zfill(6): {
            "market_cap_yi": row["market_cap_yi"],
            "float_market_cap_yi": row["float_market_cap_yi"],
            "source": "daily_prices",
        }
        for row in daily_rows
    }
    if from_daily_prices:
        save_hc_market_caps(conn, trade_date, from_daily_prices)
        caps.update(from_daily_prices)
    return caps


def save_hc_market_caps(conn, trade_date, caps):
    if not caps:
        return
    rows = [
        (
            trade_date,
            code,
            cap.get("market_cap_yi"),
            cap.get("float_market_cap_yi"),
            cap.get("source") or "eastmoney",
        )
        for code, cap in caps.items()
    ]
    conn.executemany(
        """
        INSERT INTO high_confidence_market_caps (
            trade_date, code, market_cap_yi, float_market_cap_yi, source,
            created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, datetime('now','localtime'), datetime('now','localtime'))
        ON CONFLICT(trade_date, code) DO UPDATE SET
            market_cap_yi = excluded.market_cap_yi,
            float_market_cap_yi = excluded.float_market_cap_yi,
            source = excluded.source,
            updated_at = datetime('now','localtime')
        """,
        [row for row in rows],
    )
    conn.executemany(
        """
        UPDATE daily_prices
        SET market_cap_yi = ?,
            float_market_cap_yi = ?
        WHERE trade_date = ?
          AND code = ?
        """,
        [
            (
                cap.get("market_cap_yi"),
                cap.get("float_market_cap_yi"),
                trade_date,
                code,
            )
            for code, cap in caps.items()
            if cap.get("market_cap_yi") is not None
        ],
    )
    conn.commit()


def estimate_historical_cap(row, realtime_cap):
    market_cap = realtime_cap.get("market_cap_yi")
    float_cap = realtime_cap.get("float_market_cap_yi")
    price = realtime_cap.get("price")
    close = row.get("close")
    if market_cap and price and close:
        ratio = close / price
        market_cap = market_cap * ratio
        float_cap = float_cap * ratio if float_cap else None
        source = "eastmoney_estimated"
    else:
        source = "eastmoney"
    return {
        "market_cap_yi": market_cap,
        "float_market_cap_yi": float_cap,
        "source": source,
    }


def load_or_fetch_hc_market_caps(conn, trade_date, rows, use_cache=True):
    codes = [str(row["code"]).zfill(6) for row in rows]
    caps = load_hc_market_caps_from_db(conn, trade_date, codes) if use_cache else {}
    missing_codes = [code for code in codes if code not in caps]
    fetched = {}
    if missing_codes and hc.HC_ENABLE_REALTIME_MARKET_CAP:
        realtime = fetch_eastmoney_market_caps(missing_codes)
        row_by_code = {str(row["code"]).zfill(6): row for row in rows}
        for code, cap in realtime.items():
            if cap.get("market_cap_yi") is None:
                continue
            fetched[code] = estimate_historical_cap(row_by_code.get(code, {}), cap)
        if fetched:
            if use_cache:
                save_hc_market_caps(conn, trade_date, fetched)
            caps.update(fetched)
    return caps, len(missing_codes), len(fetched)


def apply_hc_market_cap_filter(conn, trade_date, rows, use_cache=True):
    if not hc.HC_ENABLE_MARKET_CAP_FILTER:
        return rows, {
            "market_cap_checked": len(rows),
            "market_cap_missing": 0,
            "market_cap_fetched": 0,
            "market_cap_filtered": 0,
            "market_cap_source": "disabled",
            "market_cap_unavailable": False,
        }
    caps, initially_missing, fetched_count = load_or_fetch_hc_market_caps(
        conn,
        trade_date,
        rows,
        use_cache=use_cache,
    )
    kept = []
    missing = 0
    filtered = 0
    for row in rows:
        code = str(row["code"]).zfill(6)
        cap = caps.get(code) or {}
        market_cap = cap.get("market_cap_yi")
        float_cap = cap.get("float_market_cap_yi")
        if market_cap is None:
            missing += 1
            if not hc.HC_KEEP_MISSING_MARKET_CAP:
                filtered += 1
                continue
            row["market_cap_yi"] = None
            row["float_market_cap_yi"] = None
            row["market_cap_missing"] = True
            kept.append(row)
            continue
        row["market_cap_yi"] = round(market_cap, 2)
        row["float_market_cap_yi"] = round(float_cap, 2) if float_cap is not None else None
        row["market_cap_missing"] = False
        if market_cap < hc.HC_DEFAULT_MIN_MARKET_CAP_YI:
            filtered += 1
            continue
        kept.append(row)
    return kept, {
        "market_cap_checked": len(rows),
        "market_cap_missing": missing,
        "market_cap_initially_missing": initially_missing,
        "market_cap_fetched": fetched_count,
        "market_cap_filtered": filtered,
        "market_cap_source": "db+eastmoney" if use_cache else "eastmoney_nocache",
        "market_cap_unavailable": missing == len(rows) and bool(rows),
    }


