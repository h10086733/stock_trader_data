# stock_trader_data
# 低PB周期股选股策略系统

基于《点石成金》策略框架，针对A股有色金属和基础化工行业，通过 ROE 亏损 + PB 历史低位 + 行业集体亏损三重条件选股。

---

## 策略逻辑

```
全行业集体亏损（≥30%股票ROE<0）
        ↓
个股ROE低于行业阈值
        ↓
PB处于历史低位（历史25分位 × 1.2 以内）
        ↓
股价距近3年高点跌幅 > 55%（增强确认）
        ↓
买入 → 等待周期反转 → 平均涨幅200%
```

### 买入阈值

| 行业     | ROE阈值 | PB阈值 | 行业亏损要求 |
|----------|---------|--------|------------|
| 有色金属 | < -5%   | 历史25分位×1.2 | ≥ 30% |
| 基础化工 | < -2%   | 历史25分位×1.2 | ≥ 30% |

### 卖出规则（满足任一）

- ROE 连续 2 个季度转正
- 涨幅达到 100%
- PB 回到历史中位数（50分位）

### 信号强度

| 强度 | 满足条件 | 建议操作 |
|------|---------|---------|
| ★    | 行业亏损 + 个股ROE | 观察 |
| ★★   | + PB低位 | 半仓 |
| ★★★★ | + 股价跌幅>55% | 满仓 |

---

## 文件结构

```
stock_trader_data/
├── fetch.py               # 全市场股票列表 + 日K线价格入库
├── findata.py             # PB / ROE 历史财务数据入库
├── index.py               # 指数成分股管理（中证 / 国证 渠道导入）
├── index_stats.py         # 行业宽度指标计算（20日新高/新低净值）
├── sector_strategy.py     # 行业股票池管理 + 低PB信号扫描
├── backtest.py            # 历史回测
├── bt_check.py            # 回测数据就绪检查
├── app.py                 # 行业宽度指标 Web 展示（Flask）
├── data/
│   ├── nonferrous.csv     # 有色金属股票池（106只，9个子行业）
│   ├── chemicals_1.csv    # 化工股票池一（67只：民爆/涂料/钛白粉等）
│   ├── chemicals_2.csv    # 化工股票池二（123只：磁性材料/合成树脂等）
│   └── chemicals_3.csv    # 化工股票池三（104只：化肥/农药/染料等）
└── stock_data.db          # SQLite数据库（不提交到git）
```

### 股票池概览

| 大类     | 子行业数 | 股票数 |
|----------|---------|--------|
| 有色金属 | 9       | 106只  |
| 基础化工 | 24      | 280只  |
| **合计** | **33**  | **386只** |

---

## 数据库表结构

| 表名                | 说明 |
|--------------------|------|
| `stocks`           | 全市场股票基础信息（5200+只）|
| `daily_prices`     | 日K线价格（前复权，2010年至今）|
| `pb_history`       | 月度PB历史 |
| `roe_history`      | 半年报/年报ROE历史 |
| `sectors`          | 子行业定义（33个）|
| `sector_stocks`    | 行业-股票关联（386条）|
| `sector_signals`   | 实时扫描信号记录 |
| `indices`          | 指数定义（沪深300/中证军工等）|
| `index_constituents` | 指数成分股 |
| `index_daily_stats` | 行业宽度指标（20日新高/新低净值）|
| `bt_signals`       | 回测买入信号 |
| `bt_trades`        | 回测交易记录 |
| `sync_log`         | 数据同步日志 |

---

## 快速开始

### 1. 安装依赖

```bash
pip install requests flask xlrd openpyxl baostock
```

### 2. 初始化价格数据

```bash
# 全量抓取（5200只股票，约90分钟）
python fetch.py --init

# 每日增量同步（收盘后执行）
python fetch.py --sync
```

### 3. 导入行业股票池

```bash
python sector_strategy.py --import-csv --csv-dir ./data
```

### 4. 抓取 PB / ROE 数据

```bash
# 只抓策略相关的386只股票
python findata.py --stocks $(python sector_strategy.py --get-codes)
```

### 5. 检查数据就绪状态

```bash
python bt_check.py
```

### 6. 运行策略扫描

```bash
# 扫描今日信号
python sector_strategy.py --scan

# 扫描指定子行业
python sector_strategy.py --scan-sector 铜

# 查看近期所有信号
python sector_strategy.py --signals
```

### 7. 历史回测

```bash
# 全量回测（2010年至今）
python backtest.py --run

# 只回测有色金属
python backtest.py --run --sector 有色金属

# 查看回测报告
python backtest.py --report

# 查看强信号交易明细
python backtest.py --trades --strength 3
```

### 8. 行业宽度指标 Web 页面

```bash
# 先回填历史净值（首次运行）
python index_stats.py --backfill-all

# 盘中每小时刷新一次最新交易日
python index_stats.py --calc-intraday

# 收盘后做最终日线计算
python index_stats.py --calc-today

# 启动 Web 页面（行业宽度 http://localhost:5000，14:30 选股 http://localhost:5000/momentum）
python app.py
```

---

## 定时任务

每个交易日盘中和收盘后自动执行：

```bash
# crontab -e
30 11 * * 1-5  cd ~/project/stock_trader_data && python3 fetch.py --sync && python3 index_stats.py --calc-intraday >> cron.log 2>&1
30 14 * * 1-5  cd ~/project/stock_trader_data && python3 app.py --momentum-daily >> momentum_cron.log 2>&1
00 15 * * 1-5 cd ~/project/stock_trader_data && python3 fetch.py --sync && python3 index_stats.py --calc-intraday >> cron.log 2>&1
35 16 * * 1-5  cd ~/project/stock_trader_data && python3 fetch.py --sync && python3 index_stats.py --calc-today >> cron.log 2>&1
20 18 * * 0 cd ~/project/stock_trader_data && python3 index.py --update-all >> index_constituents_cron.log 2>&1
```

14:30 动量选股任务会先结算最近一个已保存买入日的次日 10:00 前卖出收益，再扫描并保存当天 14:30 入选股票。常用命令：

```bash
# 手动执行完整日任务
python3 app.py --momentum-daily

# 只扫描并保存当天14:30选股
python3 app.py --momentum-scan-save

# 只补算前一交易日买入、今天10:00前卖出的收益
python3 app.py --momentum-settle

# 查看最近收益记录
python3 app.py --momentum-report --report-limit 50

# 回填最近30个自然日内的交易日，并统计下一交易日10:00前卖出收益
python3 app.py --momentum-backfill --backfill-days 30 --verify-limit 200

# 指定日期区间回填
python3 app.py --momentum-backfill --backfill-start 2026-05-15 --backfill-end 2026-06-15 --verify-limit 200
```

历史回填优先使用历史分钟线复算 14:30 买入和次日 10:00 前卖出。如果历史分钟线接口不可用，会自动回退为日线近似口径：当天收盘价近似买入、下一交易日开盘价近似 10:00 前卖出，并在记录中标记 `daily_open_fallback`。如需禁用近似口径：

```bash
python3 app.py --momentum-backfill --backfill-days 30 --no-daily-fallback

# 已确认历史分钟线不可用时，可直接用日线近似口径快速回填
python3 app.py --momentum-backfill --backfill-days 30 --daily-fallback-only
```

### 9. 中证1000择时策略

文档 `1000判断 (1).docx` 对应的择时策略已整理到 `csi1000_timing.py`。第一版规则：

- 做多：中证1000宽度强、沪深300不弱，5日成交额均值大于20日均值的 1.1 倍，且价格较10日低点涨出 4% 以上。
- 做空：中证1000和沪深300的3日宽度均值偏弱，5日成交额不高于20日均值的 1.05 倍，且价格距10日高点回撤不超过 5%。
- 风控：空单遇中证1000两日涨幅超过 2% 时止损；跌深后默认不追空。

常用命令：

```bash
# 初始化策略表
python csi1000_timing.py --init-db

# 同步沪深300和中证1000指数日线
python csi1000_timing.py --fetch-index-prices --start 20100101

# 用当前成分股补算近10年宽度指标
python csi1000_timing.py --backfill-width --start 2016-06-20 --end 2026-06-18 --force

# 回测当前已具备宽度指标的区间
python csi1000_timing.py --backtest --start 2026-02-03 --run-key first_width_2026

# 回测最近10年
python csi1000_timing.py --backtest --start 2016-06-20 --end 2026-06-18 --run-key current_constituents_10y

# 直接使用历史 Excel 里的 300/1000 宽度做回测
python csi1000_timing.py --backtest --data-source excel --start 2016-06-20 --end 2025-09-24 --run-key excel_width_10y

# 使用本地指数行情/量能，但日期范围和 300/1000 宽度来自历史 Excel
python csi1000_timing.py --backtest --data-source excel_width --run-key excel_width_db_price_logic

# 每日信号提醒
python csi1000_timing.py --signal

# 查看最近信号和交易
python csi1000_timing.py --signals --limit 20
python csi1000_timing.py --trades --limit 20
```

注意：`--backfill-width` 使用当前成分股反推历史宽度，适合快速验证策略，但不是严格历史成分股口径。

### 10. 历史指数成分快照

`index_history.py` 用于保存历史指数成分快照。当前 baostock 支持：

- `000016` 上证50
- `000300` 沪深300
- `000905` 中证500

本地 baostock 版本没有中证1000历史成分接口。

中证1000历史成分推荐用 JoinQuant/聚宽导出：

```bash
# 1) 在聚宽 Notebook 中运行脚本，生成 csi1000_history_cons_joinquant.csv
#    脚本位置：scripts/export_joinquant_csi1000_history.py

# 2) 将 CSV 放入 data/ 后导入本地历史成分表
python index_history.py --import-csv data/csi1000_history_cons_joinquant.csv --index-code 000852 --source joinquant
```

如果本地安装并配置了 `jqdatasdk`，也可以直接拉：

```bash
export JQ_USERNAME=你的账号
export JQ_PASSWORD=你的密码
python index_history.py --fetch-jq 000852 --start 2014-12-31 --end 2026-06-30 --frequency month
```

Tushare Pro 也可以用 `index_weight` 还原历史成分，但账号需要开通指数权重/成分接口权限：

```bash
export TUSHARE_TOKEN=你的token
python index_history.py --fetch-tushare 000852 --start 2016-06-20 --end 2026-06-18 --frequency month
```

当前已验证：Tushare token 本身有效，`index_basic` 可访问；但 `index_weight` 和 `index_member` 返回无权限，所以暂时不能用该账号直接拉中证1000历史成分。

RiceQuant/米筐也已接入，但本地 `rqdatac` 初始化需要米筐账号或 URI：

```bash
export RQ_USERNAME=你的账号
export RQ_PASSWORD=你的密码
python index_history.py --fetch-rq 000852 --start 2016-06-20 --end 2026-06-18 --frequency month
```

中证指数官网公告附件是官方免费源，可以抓“指数调样”公告里的调入/调出名单，再从当前快照倒推历史快照：

```bash
# 抓公告附件并倒推，锚点默认使用 akshare_csindex_current 的最新 000852 快照
python index_history.py --fetch-csindex 000852 --start 2016-06-20 --end 2026-06-20 --derive-csindex

# 如果调样记录已经入库，只重新倒推
python index_history.py --derive-csindex-only 000852
```

当前已验证：`2026-05-06` 公告 `id=3006120` 的 Excel 附件可解析，调入 6 只、调出 6 只，并能从 `2026-06-18` 当前快照倒推出 1000 只快照。继续批量抓取时官网详情接口触发了 WAF/403，后续可等限流解除或换网络续跑。

touzid 已验证两个接口，都不能单独作为完整历史成分股来源：

- `get_follow_indice_custom`：`rp` 控制报告期财务/估值指标；`2021-06-30` 到 `2025-12-31` 每期返回 1000 行，但股票集合完全相同。
- `company_indice`：返回当前中证1000成分 1000 只 + 备选 100 只；`report_date` 不回溯历史成分，`type33=2` 只返回最新一次调出的 6 只。

`company_indice` 可用作诊断，不写入历史成分表：

```bash
export TOUZID_COOKIE='浏览器里的登录cookie'
python index_history.py --fetch-touzid 000852 --start 2025-12-31 --end 2025-12-31
```

```bash
# 初始化历史成分表
python index_history.py --init-db

# 拉取沪深300历史成分快照
python index_history.py --fetch-baostock 000300 --start 2016-06-20 --end 2025-09-24

# 查看已入库快照
python index_history.py --list 000300

# 查看相邻快照的成分变化
python index_history.py --changes 000300
```

### 添加指数成分股

```bash
python index.py --add 932365 中证现金流
python index.py --channel cnindex --add 980092 国证行业指数
python index_stats.py --backfill 932365
```


---

## 数据来源

| 数据       | 来源                  | 接口 |
|------------|----------------------|------|
| 股票列表   | 东方财富              | push2delay API |
| 日K线价格  | 东方财富              | push2his API（前复权）|
| PB历史     | 东方财富数据中心       | RPT_CUSTOM_DMSK_TREND |
| ROE历史    | 东方财富数据中心       | RPT_F10_FINANCE_DUPONT |
| 指数成分股 | 中证指数官网 / 国证指数官网 | XLS文件下载 |

---

## 注意事项

- `stock_data.db` 已加入 `.gitignore`，不提交到 git
- PB 数据最早从 2016 年开始，回测起点建议设为 `--start 2016-01-01`
- 回测存在**幸存者偏差**：已退市股票未纳入统计
- 策略统计成功率来自历史数据，不构成投资建议
