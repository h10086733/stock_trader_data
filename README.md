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
├── index.py               # 指数成分股管理（中证官网XLS导入）
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
pip install requests flask xlrd openpyxl
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

# 每日计算
python index_stats.py --calc-today

# 启动 Web 页面（访问 http://localhost:5000）
python app.py
```

---

## 定时任务

每个交易日收盘后（16:30起）自动执行：

```bash
# crontab -e
10 15 * * 1-5  cd ~/project/stock_trader_data && python fetch.py --sync >> cron.log 2>&1
35 16 * * 1-5  cd ~/project/stock_trader_data && python index_stats.py --calc-today >> cron.log 2>&1
```

---

## 数据来源

| 数据       | 来源                  | 接口 |
|------------|----------------------|------|
| 股票列表   | 东方财富              | push2delay API |
| 日K线价格  | 东方财富              | push2his API（前复权）|
| PB历史     | 东方财富数据中心       | RPT_CUSTOM_DMSK_TREND |
| ROE历史    | 东方财富数据中心       | RPT_F10_FINANCE_DUPONT |
| 指数成分股 | 中证指数官网           | XLS文件下载 |

---

## 注意事项

- `stock_data.db` 已加入 `.gitignore`，不提交到 git
- PB 数据最早从 2016 年开始，回测起点建议设为 `--start 2016-01-01`
- 回测存在**幸存者偏差**：已退市股票未纳入统计
- 策略统计成功率来自历史数据，不构成投资建议
