# 高置信选股策略说明

本文档整理当前仓库中“每日高置信选股 / 高置信小集合”的生产规则。实现入口主要在 `app.py`，形态与耦合扫描在 `hc_strategy_scanner.py`，历史质量计算在 `evaluate_historical_quality.py`，历史验证在 `validate_high_confidence_history.py`。

## 目标

高置信策略用于从 A 股全市场日 K 数据中筛出少量候选股。它不是单纯按某个指标排序，而是把 K 线形态、技术指标耦合、个股历史同类信号表现、市值约束和综合排序串成一条流水线。

核心思路：

```text
全市场日 K
  -> 排除北交所、退市股、历史不足股票
  -> 识别指定 K 线形态
  -> 匹配 MA / MACD / HMA 等耦合条件
  -> 计算原始信号分
  -> 只保留历史表现足够好的“股票 + 形态 + 耦合”组合
  -> 聚合同一股票的多条信号并综合排序
  -> 总市值过滤
  -> 输出每日高置信候选
```

## 当前生产默认参数

| 参数 | 当前值 | 说明 |
|---|---:|---|
| 扫描模式 | `loose` | 使用召回更高的耦合规则 |
| 形态引擎 | `recall` | 使用宽松 K 线形态识别 |
| 扫描回看日数 | 400 个交易日 | 用于构建当日指标面板 |
| 最少历史 K 线 | 120 根 | 股票少于该数量不参与扫描 |
| 默认展示天数 | 7 个完整交易日 | 页面留空日期时默认返回最近 7 个完整交易日 |
| 完整市场阈值 | 日 K 覆盖 >= 4500 只 | 用于判断完整交易日 |
| 默认输出上限 | 0 | 不截断，按规则全部输出 |
| 默认最低总市值 | 100 亿 | 低于该市值过滤掉 |
| 历史质量回看 | 4200 个交易日 | 只使用目标日前历史 |
| 历史验证窗口 | 未来 5 个交易日 | 计算历史同类信号结果 |
| 历史胜利口径 | `max_high` | 未来 5 日最高价相对当日收盘价 |
| 胜利阈值 | 2% | 未来 5 日最高收益 > 2% 算胜利 |
| 最少历史样本 | 31 次 | 同一股票 + 形态 + 耦合组合 |
| 最低历史胜率 | 70% | `hist_win_rate >= 0.70` |

当前规则版本会被拼入缓存 key，例如：

```text
hc_loose_recall_q70_s31_fwd5_max_high_wr2_max0_capon
```

## 默认扫描形态

生产页面默认只扫描 5 类形态：

| 形态 | 宽松识别要点 |
|---|---|
| 长蜡烛 | 实体或振幅明显高于 20 日均值，且实体占全日振幅较高 |
| 母子线 | 当日实体落在前一日实体范围内，且实体不超过前一日实体的 75% |
| 光头光脚缺影线 | 阳线，上下影线都极短，实体占振幅比例很高 |
| 黄包车夫 | 类十字，长上下影线，日内振幅较大 |
| 风高浪大线 | 小实体，同时上下影线较长，日内振幅较大 |

`hc_strategy_scanner.py` 还支持更多形态，例如十字、十字星、长脚十字、纺锤、短蜡烛、捉腰带线、收盘缺影线、吞噬模式、陷阱、锤头、倒锤头、蜻蜓十字、墓碑十字等。但生产默认参数只启用上面 5 类。

## 技术耦合规则

每个形态会和耦合族组合生成信号。当前生产模式为 `loose`：

| 耦合族 | 条件 | 来源类型 |
|---|---|---|
| MA5 | 收盘价距离 MA5 在 +/-3% 内 | price |
| MA30 | 收盘价低于 MA30 | price |
| MA60 | 收盘价低于 MA60 | price |
| MACD | MACD 柱状图上升 | price |
| HMA | 收盘价高于 HMA20 或 HMA30 | price |
| Fscore | MACD 上升或收盘价高于 MA10 | proxy |
| Concentration | 收盘价低于 MA30 | proxy |
| ash | MACD 上升 | proxy |

`strict` 模式存在更收紧的条件，例如 MA30/MA60 需要均线斜率下降、MACD 要在零轴下方上升等，但页面当前没有使用 `strict`。

## 原始信号评分

形态和耦合匹配后，会生成一条信号。单条信号的 `scan_score` 由形态权重、耦合权重、当日涨跌幅和换手率组成：

```text
scan_score =
  形态权重 * 10
  + 耦合权重 * 10
  + clamp(涨跌幅, -5, 10) * 0.2
  + clamp(换手率, 0, 20) * 0.05
```

主要权重：

| 形态 | 权重 |
|---|---:|
| 光头光脚缺影线 | 1.5 |
| 收盘缺影线 | 1.3 |
| 捉腰带线、吞噬模式 | 1.2 |
| 长蜡烛 | 1.1 |
| 陷阱、母子线 | 1.0 |
| 黄包车夫、风高浪大线、锤头 | 0.9 |
| 纺锤、十字、长脚十字、倒锤头、蜻蜓十字 | 0.8 |
| 短蜡烛、十字星、墓碑十字 | 0.7 |

| 耦合族 | 权重 |
|---|---:|
| MA60 | 1.3 |
| HMA | 1.2 |
| MACD | 1.1 |
| MA30 | 1.0 |
| MA5 | 0.8 |
| Fscore、Concentration、ash | 0.5 |

## 历史质量过滤

这是“高置信”的关键过滤层。对当日产生的每条信号，系统会回看目标日前的历史，按以下粒度计算表现：

```text
同一股票 code
+ 同一 K 线形态
+ 同一耦合族 coupling_family
```

生产口径：

1. 只使用目标日前的数据，避免未来函数。
2. 对每次历史同类信号，取信号日收盘价作为基准。
3. 向后看 5 个交易日。
4. 如果未来 5 日最高价相对信号日收盘价涨幅 > 2%，记为胜利。
5. 聚合得到 `hist_samples`、`hist_win_rate`、`hist_up_avg`、`hist_down_avg`、`hist_pl_ratio`。
6. 只保留：

```text
hist_samples >= 31
hist_win_rate >= 0.70
```

生产默认不读取、不写入历史质量缓存。每个交易日都会基于当天信号集合独立回看历史 K 线并计算 `hist_samples`、`hist_win_rate` 等质量指标，避免跨日期缓存导致信号组合匹配不一致。

## 股票聚合与排序

同一股票可能同时满足多个形态和多个耦合条件。质量过滤后，系统按股票聚合，保留：

- 最佳形态 `pattern`
- 最佳耦合 `coupling`
- 全部命中形态 `patterns`
- 全部命中耦合 `couplings`
- 信号数 `signal_count`
- 历史胜率、历史样本数、盈亏比
- 当日收盘价、涨跌幅、换手率、成交额
- 是否触板、是否封板

股票级 `rank_score`：

```text
rank_score =
  max(scan_score)
  + log1p(信号数) * 2.5
  + log1p(形态数) * 2.0
  + log1p(耦合数) * 1.5
  + clamp(涨跌幅, -5, 10) * 0.15
  + hist_win_rate * 12
  + log1p(hist_samples) * 0.6
  + min(hist_pl_ratio, 5) * 0.4
```

最终排序键：

```text
rank_score 降序
scan_score 降序
涨跌幅 降序
signal_count 降序
```

## 市值过滤

排序后应用市值过滤：

```text
总市值 market_cap_yi >= 100 亿
```

市值来源优先级：

1. `high_confidence_market_caps` 缓存表；
2. `daily_prices.market_cap_yi`；
3. 东方财富实时行情接口。

如果用实时总市值补历史日期，系统会按 `历史收盘价 / 实时价格` 估算历史总市值，并标记来源为 `eastmoney_estimated`。

## 涨停识别

输出字段包含触板和封板：

| 股票类型 | 涨停阈值 |
|---|---:|
| ST | 4.8% |
| 创业板、科创板 | 19.5% |
| 其他 A 股 | 9.8% |

`touch_limit` 使用当日最高价相对前收盘计算；`close_limit` 使用当日涨跌幅判断。

## 页面与 API

Web 页面：

```bash
python app.py
```

访问：

```text
http://localhost:5000/high-confidence
```

API：

| 接口 | 方法 | 说明 |
|---|---|---|
| `/api/high-confidence/scan` | GET | 获取高置信选股结果 |
| `/api/high-confidence/sync` | POST | 异步重新同步 |
| `/api/high-confidence/progress` | GET | 查询同步进度 |

常用参数：

| 参数 | 说明 |
|---|---|
| `date=YYYY-MM-DD` | 指定交易日；指定日期时默认只返回 1 天 |
| `days=N` | 返回最近 N 个完整交易日 |
| `refresh=1` | 强制重算并刷新缓存 |

示例：

```bash
curl "http://localhost:5000/api/high-confidence/scan?date=2026-06-17"
curl "http://localhost:5000/api/high-confidence/scan?days=30"
curl -X POST "http://localhost:5000/api/high-confidence/sync?date=2026-06-17"
```

## 缓存与数据表

高置信策略保留页面结果和市值缓存，但历史质量结果不缓存：

| 位置 | 内容 |
|---|---|
| `outputs/cache/hc_scan_panel_*.pkl` | 当日指标面板 |
| `outputs/cache/hc_signal_quality_*.pkl` | 已停用；生产不再读取或写入 |
| `high_confidence_scans` | 页面 payload 缓存 |
| `high_confidence_market_caps` | 市值快照缓存 |

缓存表由 `ensure_high_confidence_tables()` 自动创建。

## 常用脚本

单日扫描并和样例入选清单对比：

```bash
python hc_strategy_scanner.py --date 2026-06-17 --compare
```

预热最近 30 个完整交易日缓存：

```bash
python sync_high_confidence_history.py --days 30
```

强制刷新指定日期区间：

```bash
python sync_high_confidence_history.py --start 2026-06-01 --end 2026-06-17 --days 20 --refresh
```

验证历史表现：

```bash
python validate_high_confidence_history.py --start 2021-01-01 --end 2024-12-31 --forward-days 5
```

历史验证会输出：

| 文件 | 说明 |
|---|---|
| `outputs/hc_backtest_picks_*` | 每只入选股票及未来表现 |
| `outputs/hc_backtest_daily_*` | 每日扫描统计 |
| `outputs/hc_backtest_summary_*` | 总体表现汇总 |
| `outputs/hc_backtest_summary_year_*` | 按年汇总 |
| `outputs/hc_backtest_summary_month_*` | 按月汇总 |

## 输出字段说明

| 字段 | 说明 |
|---|---|
| `date` | 交易日 |
| `code` / `secucode` / `name` | 股票代码、交易所代码、名称 |
| `pattern` | 排名最高的形态 |
| `coupling` | 排名最高的耦合族 |
| `patterns` / `couplings` | 该股票全部命中的形态和耦合族 |
| `signal_count` | 该股票命中的信号条数 |
| `hist_win_rate` | 历史同类信号胜率 |
| `hist_samples` | 历史同类信号样本数 |
| `hist_pl_ratio` | 历史盈亏比近似值 |
| `rank_score` | 股票综合排序分 |
| `scan_score` | 单条最佳信号分 |
| `close` / `pct_change` | 收盘价、当日涨跌幅 |
| `turnover` / `amount_yi` | 换手率、成交额亿元 |
| `market_cap_yi` / `float_market_cap_yi` | 总市值、流通市值亿元 |
| `touch_limit` / `close_limit` | 触板、封板标记 |

## 注意事项

- 该策略依赖 `daily_prices`、`stocks` 以及指标计算逻辑的完整性；数据缺失会直接影响候选。
- 历史质量过滤是按“个股 + 形态 + 耦合”计算，样本数要求较高，因此输出数量可能很少。
- 市值补全依赖东方财富实时接口；历史日期使用实时市值估算时会存在偏差。
- 页面默认复用缓存，开发或复盘时如需确认最新规则结果，应使用 `refresh=1` 或相关脚本的 `--refresh`。
- 本策略文档只描述代码规则与验证口径，不构成投资建议。
