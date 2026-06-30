"""
指数历史成分股快照工具。

当前先接 baostock 可用的历史指数：
  - 000016 上证50
  - 000300 沪深300
  - 000905 中证500

baostock 本地版本没有中证1000历史成分接口。

常用命令：
  python index_history.py --init-db
  python index_history.py --fetch-baostock 000300 --start 2016-06-20 --end 2025-09-24
  python index_history.py --list 000300
"""

from __future__ import annotations

import argparse
import io
import os
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from trader.core import config


# 迁移说明：原 BASE_DIR = Path(__file__).resolve().parent 指向仓库根，
# 迁到 trader/data/indices/ 后 __file__ 不再指向根，改用 config.BASE_DIR（仓库根）。
BASE_DIR = config.BASE_DIR
DB_PATH = config.DB_PATH


BAOSTOCK_INDEX_FUNCS = {
    "000016": "query_sz50_stocks",
    "000300": "query_hs300_stocks",
    "000905": "query_zz500_stocks",
}


def connect(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS index_constituents_history (
            index_code     TEXT NOT NULL,
            effective_date DATE NOT NULL,
            stock_code     TEXT NOT NULL,
            stock_name     TEXT,
            exchange       TEXT,
            source         TEXT NOT NULL,
            query_date     DATE,
            created_at     DATETIME DEFAULT (datetime('now','localtime')),
            updated_at     DATETIME DEFAULT (datetime('now','localtime')),
            PRIMARY KEY (index_code, effective_date, stock_code, source)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_ich_index_date
        ON index_constituents_history(index_code, effective_date)
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS index_constituent_changes (
            index_code      TEXT NOT NULL,
            announcement_id INTEGER NOT NULL,
            publish_date    DATE,
            effective_date  DATE NOT NULL,
            stock_code      TEXT NOT NULL,
            stock_name      TEXT,
            change_type     TEXT NOT NULL,
            title           TEXT,
            file_name       TEXT,
            file_url        TEXT,
            source          TEXT NOT NULL,
            created_at      DATETIME DEFAULT (datetime('now','localtime')),
            updated_at      DATETIME DEFAULT (datetime('now','localtime')),
            PRIMARY KEY (index_code, announcement_id, stock_code, change_type, source)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_icc_index_effective
        ON index_constituent_changes(index_code, effective_date)
    """)
    conn.commit()


def normalize_baostock_code(code: str) -> tuple[str, str]:
    text = str(code or "").strip().lower()
    if "." in text:
        market, bare = text.split(".", 1)
        exchange = market.upper()
        return bare.zfill(6), exchange
    return text.zfill(6), ""


def normalize_market_code(code: str) -> tuple[str, str]:
    text = str(code or "").strip()
    if not text or text.lower() == "nan":
        return "", ""
    if "." in text:
        left, right = text.split(".", 1)
        left = left.strip()
        right = right.strip().upper()
        if left.upper() in ("XSHG", "XSHE", "SH", "SZ", "BJ"):
            exchange = {
                "XSHG": "SH",
                "XSHE": "SZ",
            }.get(left.upper(), left.upper())
            return right.zfill(6), exchange
        exchange = {
            "XSHG": "SH",
            "XSHE": "SZ",
        }.get(right, right)
        return left.zfill(6), exchange
    return text.zfill(6), ""


def normalize_touzid_symbol(symbol: str) -> tuple[str, str]:
    text = str(symbol or "").strip().lower()
    if len(text) >= 8 and text[:2] in ("sh", "sz", "bj") and text[2:].isdigit():
        return text[2:].zfill(6), text[:2].upper()
    return normalize_market_code(text)


def month_query_dates(start: str, end: str) -> list[str]:
    dates = pd.date_range(start=start, end=end, freq="ME")
    items = [pd.Timestamp(start), *list(dates), pd.Timestamp(end)]
    return sorted({d.strftime("%Y-%m-%d") for d in items})


def half_year_query_dates(start: str, end: str) -> list[str]:
    # 月末抓取量也不大，默认用月末能捕捉临时调整；这里保留半年辅助函数。
    dates = pd.date_range(start=start, end=end, freq="6ME")
    items = [pd.Timestamp(start), *list(dates), pd.Timestamp(end)]
    return sorted({d.strftime("%Y-%m-%d") for d in items})


def semiannual_report_dates(start: str, end: str) -> list[str]:
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    dates = []
    for year in range(start_ts.year, end_ts.year + 1):
        for month, day in ((6, 30), (12, 31)):
            item = pd.Timestamp(year=year, month=month, day=day)
            if start_ts <= item <= end_ts:
                dates.append(item.strftime("%Y-%m-%d"))
    return dates


def fetch_baostock_snapshot(bs, index_code: str, query_date: str) -> list[dict]:
    fn_name = BAOSTOCK_INDEX_FUNCS.get(index_code)
    if not fn_name:
        raise RuntimeError(
            f"baostock 不支持指数 {index_code}。可用: {', '.join(sorted(BAOSTOCK_INDEX_FUNCS))}"
        )
    rs = getattr(bs, fn_name)(date=query_date)
    if rs.error_code != "0":
        raise RuntimeError(f"{fn_name}({query_date}) 失败: {rs.error_code} {rs.error_msg}")

    rows = []
    while rs.next():
        data = dict(zip(rs.fields, rs.get_row_data()))
        stock_code, exchange = normalize_baostock_code(data.get("code"))
        rows.append({
            "index_code": index_code,
            "effective_date": data.get("updateDate") or query_date,
            "stock_code": stock_code,
            "stock_name": data.get("code_name"),
            "exchange": exchange,
            "source": "baostock",
            "query_date": query_date,
        })
    return rows


def save_history_rows(conn: sqlite3.Connection, rows: list[dict]) -> int:
    if not rows:
        return 0
    conn.executemany("""
        INSERT INTO index_constituents_history (
            index_code, effective_date, stock_code, stock_name, exchange,
            source, query_date, updated_at
        )
        VALUES (
            :index_code, :effective_date, :stock_code, :stock_name, :exchange,
            :source, :query_date, datetime('now','localtime')
        )
        ON CONFLICT(index_code, effective_date, stock_code, source) DO UPDATE SET
            stock_name = excluded.stock_name,
            exchange = excluded.exchange,
            query_date = excluded.query_date,
            updated_at = datetime('now','localtime')
    """, rows)
    conn.commit()
    return len(rows)


def save_change_rows(conn: sqlite3.Connection, rows: list[dict]) -> int:
    if not rows:
        return 0
    conn.executemany("""
        INSERT INTO index_constituent_changes (
            index_code, announcement_id, publish_date, effective_date,
            stock_code, stock_name, change_type, title, file_name, file_url,
            source, updated_at
        )
        VALUES (
            :index_code, :announcement_id, :publish_date, :effective_date,
            :stock_code, :stock_name, :change_type, :title, :file_name, :file_url,
            :source, datetime('now','localtime')
        )
        ON CONFLICT(index_code, announcement_id, stock_code, change_type, source) DO UPDATE SET
            publish_date = excluded.publish_date,
            effective_date = excluded.effective_date,
            stock_name = excluded.stock_name,
            title = excluded.title,
            file_name = excluded.file_name,
            file_url = excluded.file_url,
            updated_at = datetime('now','localtime')
    """, rows)
    conn.commit()
    return len(rows)


def cmd_fetch_baostock(conn: sqlite3.Connection, index_code: str, start: str, end: str) -> None:
    init_db(conn)
    if index_code not in BAOSTOCK_INDEX_FUNCS:
        print(
            f"baostock 当前不支持 {index_code}。"
            f"可用指数: {', '.join(sorted(BAOSTOCK_INDEX_FUNCS))}",
            file=sys.stderr,
        )
        raise SystemExit(2)

    import baostock as bs

    login = bs.login()
    if login.error_code != "0":
        raise RuntimeError(f"baostock 登录失败: {login.error_code} {login.error_msg}")

    try:
        seen_dates = set()
        total_saved = 0
        for query_date in month_query_dates(start, end):
            rows = fetch_baostock_snapshot(bs, index_code, query_date)
            effective_dates = sorted({row["effective_date"] for row in rows})
            saved = save_history_rows(conn, rows)
            total_saved += saved
            for effective_date in effective_dates:
                if effective_date in seen_dates:
                    continue
                seen_dates.add(effective_date)
                count = sum(1 for row in rows if row["effective_date"] == effective_date)
                print(
                    f"{index_code} query={query_date} effective={effective_date} rows={count}",
                    flush=True,
                )
            time.sleep(0.2)
        print(f"完成: index={index_code} snapshots={len(seen_dates)} saved_rows={total_saved}", flush=True)
    finally:
        bs.logout()


def jq_symbol(index_code: str) -> str:
    if "." in index_code:
        return index_code
    # 中证指数通常挂 XSHG。
    return f"{index_code}.XSHG"


def tushare_symbol(index_code: str) -> str:
    if "." in index_code:
        return index_code
    # Tushare 的中证指数基础表使用 .CSI；index_weight 也接受带后缀代码。
    return f"{index_code}.CSI"


def rq_symbol(index_code: str) -> str:
    if "." in index_code:
        return index_code
    # RiceQuant 指数成分接口使用交易所后缀；中证1000挂 XSHG。
    return f"{index_code}.XSHG"


def cmd_fetch_joinquant(conn: sqlite3.Connection, index_code: str, start: str,
                        end: str, frequency: str) -> None:
    init_db(conn)
    try:
        import jqdatasdk as jq
    except ImportError as exc:
        raise RuntimeError("本地没有 jqdatasdk，请先安装：python -m pip install --user jqdatasdk") from exc

    username = os.getenv("JQ_USERNAME") or os.getenv("JQ_USER")
    password = os.getenv("JQ_PASSWORD") or os.getenv("JQ_PASS")
    if username and password:
        jq.auth(username, password)

    dates = month_query_dates(start, end) if frequency == "month" else half_year_query_dates(start, end)
    index_symbol = jq_symbol(index_code)
    total_saved = 0
    for query_date in dates:
        stocks = jq.get_index_stocks(index_symbol, date=query_date)
        rows = []
        for code in stocks:
            stock_code, exchange = normalize_market_code(code)
            if not stock_code:
                continue
            rows.append({
                "index_code": index_code.split(".", 1)[0],
                "effective_date": query_date,
                "stock_code": stock_code,
                "stock_name": None,
                "exchange": exchange,
                "source": "joinquant",
                "query_date": query_date,
            })
        saved = save_history_rows(conn, rows)
        total_saved += saved
        print(f"{index_code} joinquant date={query_date} rows={len(rows)} saved={saved}", flush=True)
        time.sleep(0.1)
    print(f"完成: index={index_code} snapshots={len(dates)} saved_rows={total_saved}", flush=True)


def cmd_fetch_tushare(conn: sqlite3.Connection, index_code: str, start: str,
                      end: str, frequency: str) -> None:
    init_db(conn)
    try:
        import tushare as ts
    except ImportError as exc:
        raise RuntimeError("本地没有 tushare，请先安装：python -m pip install --user tushare") from exc

    token = os.getenv("TUSHARE_TOKEN") or os.getenv("TS_TOKEN")
    if not token:
        raise RuntimeError("请通过环境变量 TUSHARE_TOKEN 传入 Tushare Pro token")
    ts.set_token(token)
    pro = ts.pro_api()

    dates = month_query_dates(start, end) if frequency == "month" else half_year_query_dates(start, end)
    periods: list[tuple[str, str]] = []
    for left, right in zip(dates, dates[1:]):
        start_yyyymmdd = pd.Timestamp(left).strftime("%Y%m%d")
        end_yyyymmdd = pd.Timestamp(right).strftime("%Y%m%d")
        if not periods or periods[-1] != (start_yyyymmdd, end_yyyymmdd):
            periods.append((start_yyyymmdd, end_yyyymmdd))
    if not periods and dates:
        only = pd.Timestamp(dates[0]).strftime("%Y%m%d")
        periods.append((only, only))

    index_symbol = tushare_symbol(index_code)
    bare_index_code = index_code.split(".", 1)[0]
    total_saved = 0
    seen_dates: set[str] = set()
    for period_start, period_end in periods:
        df = pro.index_weight(
            index_code=index_symbol,
            start_date=period_start,
            end_date=period_end,
        )
        if df.empty:
            print(f"{index_code} tushare period={period_start}~{period_end} rows=0", flush=True)
            time.sleep(0.3)
            continue

        df = df.copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d", errors="coerce")
        rows = []
        for _, item in df.iterrows():
            trade_date = item.get("trade_date")
            if pd.isna(trade_date):
                continue
            stock_code, exchange = normalize_market_code(item.get("con_code"))
            if not stock_code:
                continue
            rows.append({
                "index_code": bare_index_code,
                "effective_date": trade_date.strftime("%Y-%m-%d"),
                "stock_code": stock_code,
                "stock_name": None,
                "exchange": exchange,
                "source": "tushare",
                "query_date": period_end,
            })
        saved = save_history_rows(conn, rows)
        total_saved += saved
        if not rows:
            print(f"{index_code} tushare period={period_start}~{period_end} valid_rows=0", flush=True)
            time.sleep(0.3)
            continue
        for effective_date, group in pd.DataFrame(rows).groupby("effective_date"):
            seen_dates.add(effective_date)
            print(
                f"{index_code} tushare period={period_start}~{period_end} "
                f"effective={effective_date} rows={len(group)} saved={saved}",
                flush=True,
            )
        time.sleep(0.3)
    print(f"完成: index={index_code} snapshots={len(seen_dates)} saved_rows={total_saved}", flush=True)


def cmd_fetch_ricequant(conn: sqlite3.Connection, index_code: str, start: str,
                        end: str, frequency: str) -> None:
    init_db(conn)
    try:
        import rqdatac as rq
    except ImportError as exc:
        raise RuntimeError("本地没有 rqdatac，请先安装：python -m pip install --user rqdatac") from exc

    username = os.getenv("RQ_USERNAME") or os.getenv("RQ_USER")
    password = os.getenv("RQ_PASSWORD") or os.getenv("RQ_PASS")
    uri = os.getenv("RQ_URI") or os.getenv("RQDATAC2_CONF") or os.getenv("RQDATAC_CONF")
    if uri:
        rq.init(uri=uri)
    elif username and password:
        addr = os.getenv("RQ_ADDR")
        if addr:
            rq.init(username=username, password=password, addr=addr)
        else:
            rq.init(username=username, password=password)
    else:
        raise RuntimeError(
            "请设置米筐账号环境变量：RQ_USERNAME/RQ_PASSWORD，"
            "或设置 RQ_URI/RQDATAC_CONF"
        )

    dates = month_query_dates(start, end) if frequency == "month" else half_year_query_dates(start, end)
    index_symbol = rq_symbol(index_code)
    bare_index_code = index_code.split(".", 1)[0]
    total_saved = 0
    for query_date in dates:
        stocks = rq.index_components(index_symbol, date=query_date)
        rows = []
        for code in stocks:
            stock_code, exchange = normalize_market_code(code)
            if not stock_code:
                continue
            rows.append({
                "index_code": bare_index_code,
                "effective_date": query_date,
                "stock_code": stock_code,
                "stock_name": None,
                "exchange": exchange,
                "source": "ricequant",
                "query_date": query_date,
            })
        saved = save_history_rows(conn, rows)
        total_saved += saved
        print(f"{index_code} ricequant date={query_date} rows={len(rows)} saved={saved}", flush=True)
        time.sleep(0.1)
    print(f"完成: index={index_code} snapshots={len(dates)} saved_rows={total_saved}", flush=True)


def touzid_symbol(index_code: str) -> str:
    bare = index_code.split(".", 1)[0]
    if bare == "000852":
        return "sh000852"
    if bare.startswith(("000", "399")):
        return f"sh{bare}" if bare.startswith("000") else f"sz{bare}"
    return bare


def cmd_fetch_touzid(conn: sqlite3.Connection, index_code: str, start: str, end: str) -> None:
    init_db(conn)
    cookie = os.getenv("TOUZID_COOKIE")
    if not cookie:
        raise RuntimeError("请通过环境变量 TOUZID_COOKIE 传入 touzid 登录 cookie")

    import requests

    url = "https://www.touzid.com/index.php?/s_company/ajax/company_indice/"
    headers = {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json;charset=UTF-8",
        "origin": "https://www.touzid.com",
        "referer": "https://www.touzid.com/indice/company_wg.html",
        "x-requested-with": "XMLHttpRequest",
        "user-agent": "Mozilla/5.0",
        "cookie": cookie,
    }
    index_symbol = touzid_symbol(index_code)
    bare_index_code = index_code.split(".", 1)[0]
    session = requests.Session()
    anchor_codes = {
        row["stock_code"]
        for row in conn.execute("""
            SELECT stock_code
            FROM index_constituents_history
            WHERE index_code = ? AND source = 'akshare_csindex_current'
        """, (bare_index_code,))
    }
    for query_date in semiannual_report_dates(start, end):
        payload = {
            "followed": "",
            "sort": {"prop": "v_pt", "order": "desc"},
            "offset": 1,
            "pagesize": 1500,
            "industry1": index_symbol,
            "industry2": "",
            "type1": "1",
            "type2": "3",
            "type33": "1",
            "report_date": query_date,
        }
        resp = session.post(url, headers=headers, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        rsm = data.get("rsm") or {}
        items = rsm.get("data") if isinstance(rsm, dict) else rsm
        items = items or []
        rows: list[dict] = []
        for item in items:
            stock_code, exchange = normalize_touzid_symbol(item.get("symbol") or item.get("sec"))
            if not stock_code:
                continue
            rows.append({
                "index_code": bare_index_code,
                "effective_date": query_date,
                "stock_code": stock_code,
                "stock_name": item.get("name"),
                "exchange": exchange,
                "source": "touzid_company_indice",
                "query_date": query_date,
                "in_date": item.get("in_date"),
                "out_date": item.get("out_date"),
            })
        row_codes = {row["stock_code"] for row in rows}
        active_codes = {
            row["stock_code"]
            for row in rows
            if row.get("in_date")
            and pd.Timestamp(row["in_date"]) <= pd.Timestamp(query_date)
            and (
                not row.get("out_date")
                or pd.Timestamp(row["out_date"]) > pd.Timestamp(query_date)
            )
        }
        extra = len(row_codes - anchor_codes) if anchor_codes else None
        missing = len(anchor_codes - row_codes) if anchor_codes else None
        print(
            f"{index_code} touzid-company date={query_date} rows={len(rows)} "
            f"active_by_in_date={len(active_codes)} akshare_extra={extra} "
            f"akshare_missing={missing} errno={data.get('errno')}",
            flush=True,
        )
        time.sleep(0.5)
    print(
        "完成: touzid company_indice 已验证为当前成分+备选池诊断数据，"
        "未写入 index_constituents_history。",
        flush=True,
    )


CSINDEX_BASE_URL = "https://www.csindex.com.cn/csindex-home"
CSINDEX_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json;charset=UTF-8",
    "Origin": "https://www.csindex.com.cn",
    "Referer": "https://www.csindex.com.cn/",
    "User-Agent": "Mozilla/5.0",
}


def csindex_index_name_pattern(index_code: str) -> str:
    if index_code == "000852":
        return r"中证\s*1000"
    if index_code == "000300":
        return r"沪深\s*300"
    if index_code == "000905":
        return r"中证\s*500"
    if index_code == "000510":
        return r"中证\s*A500"
    return re.escape(index_code)


def parse_csindex_effective_date(content: str, publish_date: str | None) -> str | None:
    text = re.sub(r"<[^>]+>", "", content or "")
    text = re.sub(r"&nbsp;", " ", text)
    matches = re.findall(
        r"(?:于|自|将于)?\s*(20\d{2})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日[^。；，,]*?(?:生效|实施|发布)",
        text,
    )
    if matches:
        y, m, d = matches[-1]
        return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
    return publish_date


def fetch_csindex_announcements(session, page: int, rows: int = 100) -> dict:
    payload = {
        "lang": "cn",
        "classlist": [],
        "indexlist": [],
        "page": {"desc": "", "key": "", "page": page, "rows": rows},
        "related_topics": [],
        "typelist": [],
    }
    resp = session.post(
        f"{CSINDEX_BASE_URL}/announcement/queryAnnouncementByVo",
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_csindex_detail(session, announcement_id: int) -> dict:
    resp = None
    for attempt in range(5):
        resp = session.get(
            f"{CSINDEX_BASE_URL}/announcement/queryAnnouncementById",
            params={"id": announcement_id},
            timeout=30,
        )
        if resp.status_code not in (403, 429):
            break
        wait_seconds = 5 * (attempt + 1)
        print(f"csindex detail id={announcement_id} status={resp.status_code}, retry in {wait_seconds}s", flush=True)
        time.sleep(wait_seconds)
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("code") != "200":
        raise RuntimeError(f"公告详情失败 id={announcement_id}: {payload}")
    return payload.get("data") or {}


def parse_csindex_excel_changes(content: bytes, index_code: str) -> dict[str, list[tuple[str, str | None]]]:
    result = {"NEW": [], "OLD": []}
    xl = pd.ExcelFile(io.BytesIO(content))
    for sheet in xl.sheet_names:
        change_type = None
        if "调入" in sheet:
            change_type = "NEW"
        elif "调出" in sheet:
            change_type = "OLD"
        if not change_type:
            continue
        df = pd.read_excel(xl, sheet_name=sheet)
        index_col = pick_column(df, ("指数代码", "index_code"))
        code_col = pick_column(df, ("证券代码", "样本代码", "成分券代码", "stock_code"))
        name_col = pick_column(df, ("证券简称", "样本简称", "成分券名称", "stock_name"))
        if not code_col:
            continue
        for _, item in df.iterrows():
            if index_col and str(item.get(index_col)).strip().zfill(6) != index_code:
                continue
            stock_code, _ = normalize_market_code(item.get(code_col))
            if not stock_code:
                continue
            stock_name = str(item.get(name_col)).strip() if name_col and pd.notna(item.get(name_col)) else None
            result[change_type].append((stock_code, stock_name))
    return result


def parse_csindex_pdf_changes(content: bytes, index_code: str) -> dict[str, list[tuple[str, str | None]]]:
    try:
        from pypdf import PdfReader
    except ImportError as exc:
        raise RuntimeError("解析 PDF 附件需要 pypdf：python -m pip install --user pypdf") from exc

    text = "\n".join((page.extract_text() or "") for page in PdfReader(io.BytesIO(content)).pages)
    index_pat = csindex_index_name_pattern(index_code)
    start = re.search(rf"{index_pat}\s*指数样本调整名单[:：]", text)
    if not start:
        return {"NEW": [], "OLD": []}
    section = text[start.end():]
    next_title = re.search(r"\n[^\n]*(?:指数样本调整名单|备选名单)[:：]", section)
    if next_title:
        section = section[:next_title.start()]

    old_rows: list[tuple[str, str | None]] = []
    new_rows: list[tuple[str, str | None]] = []
    for line in section.splitlines():
        codes = re.findall(r"(?<!\d)(?:00|30|60|68|90)\d{4}(?!\d)", line)
        if len(codes) >= 2:
            old_rows.append((codes[0], None))
            new_rows.append((codes[1], None))
    return {"NEW": new_rows, "OLD": old_rows}


def parse_csindex_attachment(session, file_url: str, file_name: str,
                             index_code: str) -> dict[str, list[tuple[str, str | None]]]:
    resp = None
    for attempt in range(5):
        resp = session.get(file_url, timeout=60)
        if resp.status_code not in (403, 429):
            break
        wait_seconds = 5 * (attempt + 1)
        print(f"csindex attachment status={resp.status_code}, retry in {wait_seconds}s file={file_name}", flush=True)
        time.sleep(wait_seconds)
    resp.raise_for_status()
    lower = file_name.lower()
    if lower.endswith((".xlsx", ".xls")):
        return parse_csindex_excel_changes(resp.content, index_code)
    if lower.endswith(".pdf"):
        return parse_csindex_pdf_changes(resp.content, index_code)
    return {"NEW": [], "OLD": []}


def fetch_csindex_change_rows(index_code: str, start: str, end: str,
                              conn: sqlite3.Connection | None = None) -> list[dict]:
    import requests

    session = requests.Session()
    session.headers.update(CSINDEX_HEADERS)
    rows: list[dict] = []
    page = 1
    page_rows = 100
    while True:
        payload = fetch_csindex_announcements(session, page=page, rows=page_rows)
        items = payload.get("data") or []
        total = int(payload.get("total") or 0)
        stop_after_page = False
        for item in items:
            title = item.get("title") or ""
            theme = item.get("theme") or ""
            publish_date = item.get("publishDate")
            if publish_date and publish_date < start:
                stop_after_page = True
                continue
            if "中证1000" not in title or theme != "指数调样":
                continue
            if publish_date and (publish_date < start or publish_date > end):
                continue
            announcement_id = item.get("id")
            if not announcement_id:
                continue
            detail = fetch_csindex_detail(session, int(announcement_id))
            effective_date = parse_csindex_effective_date(detail.get("content") or "", publish_date)
            if not effective_date:
                continue
            if effective_date < start or effective_date > end:
                continue
            attachments = detail.get("enclosureList") or []
            notice_rows: list[dict] = []
            for attachment in attachments:
                file_name = attachment.get("fileName") or ""
                file_url = attachment.get("fileUrl") or ""
                if not file_url:
                    continue
                changes = parse_csindex_attachment(session, file_url, file_name, index_code)
                for change_type, items_for_type in changes.items():
                    for stock_code, stock_name in items_for_type:
                        rows.append({
                            "index_code": index_code,
                            "announcement_id": int(announcement_id),
                            "publish_date": publish_date,
                            "effective_date": effective_date,
                            "stock_code": stock_code,
                            "stock_name": stock_name,
                            "change_type": change_type,
                            "title": detail.get("title") or title,
                            "file_name": file_name,
                            "file_url": file_url,
                            "source": "csindex_notice",
                        })
                        notice_rows.append(rows[-1])
                time.sleep(0.5)
            if conn is not None and notice_rows:
                save_change_rows(conn, notice_rows)
            print(
                f"csindex id={announcement_id} publish={publish_date} "
                f"effective={effective_date} rows_so_far={len(rows)} title={title}",
                flush=True,
            )
        if stop_after_page or not items or page * page_rows >= total:
            break
        page += 1
        time.sleep(0.1)
    return rows


def load_anchor_snapshot(conn: sqlite3.Connection, index_code: str,
                         source: str | None, anchor_date: str | None) -> tuple[str, set[str]]:
    params: list[str] = [index_code]
    where = "index_code = ?"
    if source:
        where += " AND source = ?"
        params.append(source)
    if anchor_date:
        where += " AND effective_date <= ?"
        params.append(anchor_date)
    row = conn.execute(f"""
        SELECT effective_date, source, COUNT(*) AS cnt
        FROM index_constituents_history
        WHERE {where}
        GROUP BY effective_date, source
        ORDER BY effective_date DESC
        LIMIT 1
    """, params).fetchone()
    if not row:
        raise RuntimeError(f"找不到锚点快照 index={index_code} source={source or '*'}")
    stock_rows = conn.execute("""
        SELECT stock_code
        FROM index_constituents_history
        WHERE index_code = ? AND source = ? AND effective_date = ?
    """, (index_code, row["source"], row["effective_date"])).fetchall()
    return row["effective_date"], {item["stock_code"] for item in stock_rows}


def save_snapshot(conn: sqlite3.Connection, index_code: str, effective_date: str,
                  stock_codes: set[str], source: str, query_date: str) -> int:
    rows = []
    for stock_code in sorted(stock_codes):
        _, exchange = normalize_market_code(stock_code)
        rows.append({
            "index_code": index_code,
            "effective_date": effective_date,
            "stock_code": stock_code,
            "stock_name": None,
            "exchange": exchange,
            "source": source,
            "query_date": query_date,
        })
    return save_history_rows(conn, rows)


def derive_snapshots_from_changes(conn: sqlite3.Connection, index_code: str,
                                  anchor_source: str, anchor_date: str | None,
                                  derived_source: str) -> None:
    snapshot_date, current = load_anchor_snapshot(conn, index_code, anchor_source, anchor_date)
    print(f"anchor source={anchor_source} date={snapshot_date} rows={len(current)}", flush=True)
    save_snapshot(conn, index_code, snapshot_date, current, derived_source, snapshot_date)

    change_dates = conn.execute("""
        SELECT effective_date
        FROM index_constituent_changes
        WHERE index_code = ? AND source = 'csindex_notice' AND effective_date <= ?
        GROUP BY effective_date
        ORDER BY effective_date DESC
    """, (index_code, snapshot_date)).fetchall()

    for row in change_dates:
        effective_date = row["effective_date"]
        changes = conn.execute("""
            SELECT stock_code, change_type
            FROM index_constituent_changes
            WHERE index_code = ? AND source = 'csindex_notice' AND effective_date = ?
        """, (index_code, effective_date)).fetchall()
        added = {item["stock_code"] for item in changes if item["change_type"] == "NEW"}
        removed = {item["stock_code"] for item in changes if item["change_type"] == "OLD"}
        save_snapshot(conn, index_code, effective_date, current, derived_source, effective_date)
        before = (current - added) | removed
        pre_date = (pd.Timestamp(effective_date) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        save_snapshot(conn, index_code, pre_date, before, derived_source, effective_date)
        if len(before) != 1000:
            print(
                f"WARNING {effective_date}: reversed snapshot rows={len(before)} "
                f"added={len(added)} removed={len(removed)}",
                flush=True,
            )
        else:
            print(
                f"{effective_date}: added={len(added)} removed={len(removed)} rows={len(before)}",
                flush=True,
            )
        current = before


def cmd_fetch_csindex(conn: sqlite3.Connection, index_code: str, start: str, end: str,
                      anchor_source: str, anchor_date: str | None,
                      derive: bool) -> None:
    init_db(conn)
    rows = fetch_csindex_change_rows(index_code, start, end, conn=conn)
    print(f"csindex changes rows={len(rows)}", flush=True)
    if derive:
        derive_snapshots_from_changes(
            conn,
            index_code=index_code,
            anchor_source=anchor_source,
            anchor_date=anchor_date,
            derived_source="csindex_notice_derived",
        )


def pick_column(df: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    normalized = {str(col).strip().lower(): col for col in df.columns}
    for candidate in candidates:
        key = candidate.strip().lower()
        if key in normalized:
            return normalized[key]
    return None


def cmd_import_csv(conn: sqlite3.Connection, path: str | Path, index_code: str,
                   source: str) -> None:
    init_db(conn)
    df = pd.read_csv(path)
    date_col = pick_column(df, ("date", "snapshot_date", "effective_date", "trade_date", "日期"))
    code_col = pick_column(df, ("code", "stock_code", "样本代码", "成分券代码", "证券代码"))
    name_col = pick_column(df, ("name", "stock_name", "样本简称", "成分券名称", "证券简称"))
    exch_col = pick_column(df, ("exchange", "market", "交易所"))
    if not date_col or not code_col:
        raise RuntimeError(f"CSV 至少需要日期列和代码列，当前列: {list(df.columns)}")

    rows = []
    for _, item in df.iterrows():
        date = pd.to_datetime(item.get(date_col), errors="coerce")
        if pd.isna(date):
            continue
        stock_code, exchange = normalize_market_code(item.get(code_col))
        if not stock_code:
            continue
        if exch_col and not exchange:
            exchange = str(item.get(exch_col) or "").strip().upper()
        rows.append({
            "index_code": index_code,
            "effective_date": date.strftime("%Y-%m-%d"),
            "stock_code": stock_code,
            "stock_name": str(item.get(name_col)).strip() if name_col and pd.notna(item.get(name_col)) else None,
            "exchange": exchange,
            "source": source,
            "query_date": date.strftime("%Y-%m-%d"),
        })
    saved = save_history_rows(conn, rows)
    print(f"导入完成: path={path} index={index_code} rows={len(rows)} saved={saved}")


def cmd_list(conn: sqlite3.Connection, index_code: str) -> None:
    rows = conn.execute("""
        SELECT effective_date, COUNT(*) AS cnt,
               MIN(query_date) AS first_query, MAX(query_date) AS last_query
        FROM index_constituents_history
        WHERE index_code = ?
        GROUP BY effective_date
        ORDER BY effective_date
    """, (index_code,)).fetchall()
    if not rows:
        print(f"{index_code} 暂无历史快照")
        return
    for row in rows:
        print(
            f"{row['effective_date']} rows={row['cnt']} "
            f"query_range={row['first_query']}~{row['last_query']}"
        )


def cmd_changes(conn: sqlite3.Connection, index_code: str) -> None:
    rows = conn.execute("""
        SELECT effective_date, GROUP_CONCAT(stock_code) AS codes
        FROM index_constituents_history
        WHERE index_code = ?
        GROUP BY effective_date
        ORDER BY effective_date
    """, (index_code,)).fetchall()
    previous_codes = None
    previous_date = None
    for row in rows:
        effective_date = row["effective_date"]
        codes = set((row["codes"] or "").split(","))
        if previous_codes is None:
            print(f"{effective_date} initial rows={len(codes)}")
        else:
            added = sorted(codes - previous_codes)
            removed = sorted(previous_codes - codes)
            if added or removed:
                print(
                    f"{effective_date} +{len(added)} -{len(removed)} "
                    f"prev={previous_date}"
                )
        previous_codes = codes
        previous_date = effective_date


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="指数历史成分股快照工具")
    parser.add_argument("--db", default=str(DB_PATH))
    parser.add_argument("--init-db", action="store_true")
    parser.add_argument("--fetch-baostock", metavar="INDEX_CODE",
                        help="用 baostock 拉历史快照，支持 000016/000300/000905")
    parser.add_argument("--fetch-jq", metavar="INDEX_CODE",
                        help="用 JoinQuant/jqdatasdk 拉历史快照，例如 000852")
    parser.add_argument("--fetch-tushare", metavar="INDEX_CODE",
                        help="用 Tushare Pro index_weight 拉历史快照，例如 000852")
    parser.add_argument("--fetch-rq", metavar="INDEX_CODE",
                        help="用 RiceQuant/rqdatac 拉历史快照，例如 000852")
    parser.add_argument("--fetch-touzid", metavar="INDEX_CODE",
                        help="诊断 touzid company_indice 当前成分+备选池；不写入历史成分表")
    parser.add_argument("--fetch-csindex", metavar="INDEX_CODE",
                        help="用中证指数官网公告附件抓调样记录，例如 000852")
    parser.add_argument("--derive-csindex-only", metavar="INDEX_CODE",
                        help="只使用已入库的中证官网调样记录倒推历史快照")
    parser.add_argument("--derive-csindex", action="store_true",
                        help="抓取中证公告后，从锚点快照倒推历史成分快照")
    parser.add_argument("--anchor-source", default="akshare_csindex_current",
                        help="--derive-csindex 使用的锚点快照来源")
    parser.add_argument("--anchor-date", default=None,
                        help="--derive-csindex 使用的最大锚点日期，默认取该来源最新")
    parser.add_argument("--frequency", choices=["month", "half-year"], default="month",
                        help="JoinQuant/Tushare/RiceQuant 查询频率，默认 month")
    parser.add_argument("--import-csv", metavar="PATH",
                        help="导入聚宽 Notebook 导出的历史成分 CSV")
    parser.add_argument("--index-code", default="000852", help="--import-csv 使用的指数代码")
    parser.add_argument("--source", default="joinquant", help="--import-csv 使用的数据源标识")
    parser.add_argument("--start", default="2016-06-20")
    parser.add_argument("--end", default=datetime.today().strftime("%Y-%m-%d"))
    parser.add_argument("--list", metavar="INDEX_CODE", help="查看已入库快照")
    parser.add_argument("--changes", metavar="INDEX_CODE", help="查看相邻快照成分变化")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    conn = connect(args.db)
    try:
        if args.init_db:
            init_db(conn)
            print("index_constituents_history 已初始化")
        elif args.fetch_baostock:
            cmd_fetch_baostock(conn, args.fetch_baostock, args.start, args.end)
        elif args.fetch_jq:
            cmd_fetch_joinquant(conn, args.fetch_jq, args.start, args.end, args.frequency)
        elif args.fetch_tushare:
            cmd_fetch_tushare(conn, args.fetch_tushare, args.start, args.end, args.frequency)
        elif args.fetch_rq:
            cmd_fetch_ricequant(conn, args.fetch_rq, args.start, args.end, args.frequency)
        elif args.fetch_touzid:
            cmd_fetch_touzid(conn, args.fetch_touzid, args.start, args.end)
        elif args.fetch_csindex:
            cmd_fetch_csindex(
                conn,
                args.fetch_csindex,
                args.start,
                args.end,
                args.anchor_source,
                args.anchor_date,
                args.derive_csindex,
            )
        elif args.derive_csindex_only:
            init_db(conn)
            derive_snapshots_from_changes(
                conn,
                index_code=args.derive_csindex_only,
                anchor_source=args.anchor_source,
                anchor_date=args.anchor_date,
                derived_source="csindex_notice_derived",
            )
        elif args.import_csv:
            cmd_import_csv(conn, args.import_csv, args.index_code, args.source)
        elif args.list:
            init_db(conn)
            cmd_list(conn, args.list)
        elif args.changes:
            init_db(conn)
            cmd_changes(conn, args.changes)
        else:
            print("请指定命令，例如: python index_history.py --fetch-baostock 000300")
            return 2
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
