"""
按反推规则过滤华创样例信号。

默认流程：
  1. 读取 outputs/hc_daily_long_all_indicators.csv
  2. 胜率/文件阈值取 70%
  3. 先筛指定 K 线形态
  4. 再筛当日总市值 >= 100 亿

用法：
  python filter_70_focus.py
  python filter_70_focus.py --patterns 长蜡烛,母子线 --min-cap 100
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


DEFAULT_PATTERNS = "长蜡烛,母子线,光头光脚缺影线,黄包车夫,风高浪大线"
DEFAULT_INPUT = Path("outputs/hc_daily_long_all_indicators.csv")
DEFAULT_COMPARE = Path("outputs/hc_daily_long_100yi_indicators.csv")
DEFAULT_OUTPUT = Path("outputs/hc_70pct_focus_patterns_cap_filter.csv")


def normalize_pattern(value) -> str:
    text = "" if pd.isna(value) else str(value).strip()
    for prefix in ("Candle ", "Pattern ", "Line ", "Doji ", "Marubozu ", "Top "):
        if text.startswith(prefix):
            return text[len(prefix):].strip()
    return text


def prepare(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["判断日期"] = pd.to_datetime(df["判断日期"]).dt.strftime("%Y-%m-%d")
    df["阈值_num"] = pd.to_numeric(
        df["阈值"].astype(str).str.replace("%", "", regex=False),
        errors="coerce",
    )
    df["code"] = df["资产代码"].astype(str).str.split(".").str[0].str.zfill(6)
    df["归一形态名称"] = df["形态名称"].map(normalize_pattern)
    df["coupling_family"] = df["耦合条件"].astype(str).str.split("-").str[-1]
    if "是否北交所" in df.columns:
        df = df[~df["是否北交所"].fillna(False).astype(bool)].copy()
    return df


def run(args) -> int:
    patterns = [item.strip() for item in args.patterns.split(",") if item.strip()]
    df = prepare(Path(args.input))
    base = df[df["阈值_num"].eq(args.threshold)].copy()
    shaped = base[base["归一形态名称"].isin(patterns)].copy()
    filtered = shaped[shaped["当日总市值"] >= args.min_cap].copy()

    cols = [
        "判断日期", "资产代码", "资产名称", "归一形态名称", "coupling_family", "耦合条件",
        "胜率", "盈亏比", "形态总出现次数", "当日总市值", "pct_change", "盘中触板", "收盘封板",
    ]
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    filtered[cols].sort_values(["判断日期", "胜率"], ascending=[True, False]).to_csv(
        output,
        index=False,
        encoding="utf-8-sig",
    )

    print(f"base rows: {len(base)} stocks: {base['code'].nunique()}")
    print(f"after patterns rows: {len(shaped)} stocks: {shaped['code'].nunique()}")
    print(f"after cap rows: {len(filtered)} stocks: {filtered['code'].nunique()}")

    if args.do_compare and Path(args.compare_input).exists():
        target = prepare(Path(args.compare_input))
        target = target[target["阈值_num"].eq(args.threshold)].copy()
        target_keys = set(zip(target["判断日期"], target["code"]))
        filtered_keys = set(zip(filtered["判断日期"], filtered["code"]))
        hit = len(target_keys & filtered_keys)
        print(f"target rows: {len(target)} stocks: {target['code'].nunique()}")
        print(f"overlap: {hit}/{len(filtered_keys)} = {hit / len(filtered_keys):.2%}" if filtered_keys else "overlap: 0")
        print(f"coverage: {hit}/{len(target_keys)} = {hit / len(target_keys):.2%}" if target_keys else "coverage: 0")

    print(f"wrote {output}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="70% 高置信形态 + 市值过滤")
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--compare-input", default=str(DEFAULT_COMPARE))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--patterns", default=DEFAULT_PATTERNS)
    parser.add_argument("--threshold", type=float, default=70)
    parser.add_argument("--min-cap", type=float, default=100)
    parser.add_argument("--no-compare", dest="do_compare", action="store_false")
    parser.set_defaults(do_compare=True)
    return parser


def main() -> int:
    return run(build_parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
