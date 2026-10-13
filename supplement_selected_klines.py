"""
补齐华创样例入选股票的近 120 个交易日日 K。

默认读取 outputs/hc_daily_long_all.csv，排除北交所，只补需要的股票。

用法：
  python supplement_selected_klines.py --dry-run
  python supplement_selected_klines.py
"""

from __future__ import annotations

import argparse
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

import fetch

try:
    import baostock as bs
except ImportError:  # pragma: no cover - depends on local env
    bs = None


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = BASE_DIR / "outputs" / "hc_daily_long_all.csv"
DEFAULT_DB = BASE_DIR / "stock_data.db"


@dataclass
class Need:
    code: str
    secucode: str
    name: str
    market: str
    min_signal_date: str
    max_signal_date: str
    signal_dates: list[str]
    start_fetch: str
    end_fetch: str
    reason: str


def bare_code(secucode: str) -> str:
    return str(secucode).split(".", 1)[0].zfill(6)


def infer_market(secucode: str, code: str) -> str:
    suffix = str(secucode).split(".")[-1].upper() if "." in str(secucode) else ""
    if suffix == "SH" or code.startswith(("5", "6", "9")):
        return "1"
    return "0"


def is_bj(secucode: str, code: str) -> bool:
    text = str(secucode).upper()
    return text.endswith(".BJ") or code.startswith(("8", "9"))


def to_baostock_code(code: str, market: str) -> str:
    return ("sh" if market == "1" else "sz") + "." + code


def to_float(value):
    if value in (None, "", "-"):
        return None
    return float(value)


def fetch_baostock_kline(code: str, market: str, start_fetch: str, end_fetch: str) -> list[dict]:
    if bs is None:
        raise RuntimeError("baostock 未安装")
    start = datetime.strptime(start_fetch, "%Y%m%d").strftime("%Y-%m-%d")
    end = datetime.strptime(end_fetch, "%Y%m%d").strftime("%Y-%m-%d")
    fields = "date,code,open,high,low,close,volume,amount,pctChg,turn"
    rs = bs.query_history_k_data_plus(
        to_baostock_code(code, market),
        fields,
        start_date=start,
        end_date=end,
        frequency="d",
        adjustflag="2",  # 前复权
    )
    if rs.error_code != "0":
        raise RuntimeError(f"baostock query failed {rs.error_code}: {rs.error_msg}")
    rows = []
    while rs.next():
        date, _, open_, high, low, close, volume, amount, pct_chg, turn = rs.get_row_data()
        volume_shares = to_float(volume)
        rows.append({
            "trade_date": date,
            "open": to_float(open_),
            "close": to_float(close),
            "high": to_float(high),
            "low": to_float(low),
            "volume": volume_shares / 100.0 if volume_shares is not None else None,
            "amount": to_float(amount),
            "pct_change": to_float(pct_chg),
            "turnover": to_float(turn),
        })
    return rows


def load_selected(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["判断日期"] = pd.to_datetime(df["判断日期"]).dt.strftime("%Y-%m-%d")
    df["code"] = df["资产代码"].map(bare_code)
    df = df[~df.apply(lambda r: is_bj(r["资产代码"], r["code"]), axis=1)].copy()
    return df


def ensure_stock_rows(conn: sqlite3.Connection, selected: pd.DataFrame) -> None:
    rows = []
    for _, row in selected.drop_duplicates("code").iterrows():
        code = row["code"]
        rows.append({
            "code": code,
            "secucode": row["资产代码"],
            "name": row["资产名称"],
            "market": infer_market(row["资产代码"], code),
            "price_latest": None,
        })
    fetch.upsert_stocks(conn, rows)


def has_signal_day_rows(conn: sqlite3.Connection, code: str, dates: list[str]) -> tuple[bool, list[str]]:
    if not dates:
        return True, []
    placeholders = ",".join("?" for _ in dates)
    rows = conn.execute(
        f"""
        SELECT trade_date
        FROM daily_prices
        WHERE code = ? AND trade_date IN ({placeholders})
        """,
        [code] + dates,
    ).fetchall()
    got = {r[0] for r in rows}
    missing = [d for d in dates if d not in got]
    return not missing, missing


def has_lookback_rows(conn: sqlite3.Connection, code: str, dates: list[str], bars: int) -> tuple[bool, str | None]:
    for d in dates:
        row = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM daily_prices
            WHERE code = ? AND trade_date <= ?
            """,
            (code, d),
        ).fetchone()
        if not row or int(row[0]) < bars:
            return False, d
    return True, None


def build_needs(conn: sqlite3.Connection, selected: pd.DataFrame, bars: int, calendar_buffer_days: int) -> list[Need]:
    needs: list[Need] = []
    stock_meta = {
        r["code"]: r
        for r in conn.execute("SELECT code, secucode, name, market FROM stocks").fetchall()
    }
    for code, group in selected.groupby("code"):
        signal_dates = sorted(group["判断日期"].unique().tolist())
        min_signal = signal_dates[0]
        max_signal = signal_dates[-1]
        has_days, missing_days = has_signal_day_rows(conn, code, signal_dates)
        has_bars, low_bar_date = has_lookback_rows(conn, code, signal_dates, bars)
        if has_days and has_bars:
            continue

        meta = stock_meta.get(code)
        first = group.iloc[0]
        secucode = first["资产代码"]
        name = first["资产名称"]
        market = infer_market(secucode, code)
        if meta:
            secucode = meta["secucode"] or secucode
            name = meta["name"] or name
            market = meta["market"] or market

        start_dt = datetime.strptime(min_signal, "%Y-%m-%d") - timedelta(days=calendar_buffer_days)
        reasons = []
        if missing_days:
            reasons.append("缺信号日:" + ",".join(missing_days[:5]))
        if low_bar_date:
            reasons.append(f"{low_bar_date}前不足{bars}根")
        needs.append(Need(
            code=code,
            secucode=secucode,
            name=name,
            market=market,
            min_signal_date=min_signal,
            max_signal_date=max_signal,
            signal_dates=signal_dates,
            start_fetch=start_dt.strftime("%Y%m%d"),
            end_fetch=max_signal.replace("-", ""),
            reason="; ".join(reasons),
        ))
    return needs


def run(args) -> int:
    selected = load_selected(Path(args.input))
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    ensure_stock_rows(conn, selected)
    needs = build_needs(conn, selected, args.bars, args.calendar_buffer_days)

    print(f"入选非北交所记录: {len(selected)}", flush=True)
    print(f"入选非北交所股票: {selected['code'].nunique()}", flush=True)
    print(f"需要补齐股票: {len(needs)}", flush=True)
    if args.dry_run:
        for item in needs[:args.limit]:
            print(
                f"{item.secucode} {item.name} {item.start_fetch}-{item.end_fetch} "
                f"信号日{len(item.signal_dates)}个 {item.reason}"
            , flush=True)
        if len(needs) > args.limit:
            print(f"... 还有 {len(needs) - args.limit} 只", flush=True)
        return 0

    if args.provider == "baostock":
        if bs is None:
            raise RuntimeError("baostock 未安装，无法使用 --provider baostock")
        login = bs.login()
        if login.error_code != "0":
            raise RuntimeError(f"baostock login failed {login.error_code}: {login.error_msg}")
        print("baostock login success", flush=True)

    ok = 0
    fail = 0
    inserted_total = 0
    started = time.time()
    try:
        for i, item in enumerate(needs, 1):
            try:
                if args.provider == "baostock":
                    rows = fetch_baostock_kline(
                        item.code,
                        item.market,
                        item.start_fetch,
                        item.end_fetch,
                    )
                else:
                    rows = fetch.fetch_kline(
                        item.code,
                        item.market,
                        item.start_fetch,
                        item.end_fetch,
                    )
                inserted = fetch.insert_daily_prices(conn, item.code, rows)
                fetch.update_stock_price_range(conn, item.code, rows)
                inserted_total += inserted
                ok += 1
                print(
                    f"[{i}/{len(needs)}] OK {item.secucode} {item.name} "
                    f"rows={inserted} {item.start_fetch}-{item.end_fetch}",
                    flush=True,
                )
            except Exception as exc:
                fail += 1
                print(f"[{i}/{len(needs)}] FAIL {item.secucode} {item.name}: {exc}", flush=True)
            time.sleep(args.sleep)
    finally:
        if args.provider == "baostock" and bs is not None:
            bs.logout()

    elapsed = time.time() - started
    print(f"完成: ok={ok} fail={fail} rows={inserted_total} elapsed={elapsed:.1f}s", flush=True)
    return 0 if fail == 0 else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="补齐入选股票近120个交易日日K，默认排除北交所")
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--bars", type=int, default=120)
    parser.add_argument("--calendar-buffer-days", type=int, default=280)
    parser.add_argument("--sleep", type=float, default=0.25)
    parser.add_argument("--provider", choices=["eastmoney", "baostock"], default="eastmoney")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=30)
    return parser


def main() -> int:
    return run(build_parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
