"""
行业宽度指标查询页面 — app.py
==============================
实现已全部迁移至 trader/ 包。本文件保留为向后兼容 shim。

启动 Web 服务：
  python app.py [--host 0.0.0.0] [--port 5000] [--debug]

CLI 用法（完整参数见 trader/cli/main.py）：
  python app.py --momentum-daily
  python app.py --csi1000-daily --csi1000-sync-index
  python app.py --pattern-backfill
  python -m trader.cli ...   ← 推荐直接使用
"""
from __future__ import annotations

# ── Web 工厂（供 gunicorn / wsgi 挂载）──────────────────────────
from trader.web import create_app as _create_app

app = _create_app()  # 模块级 Flask 实例，兼容 gunicorn -b :5000 app:app

# ── 符号 re-export（兼容 validate/sync 脚本的 import app）───────
from trader.core.config import BASE_DIR                              # noqa: E402
from trader.core.db import connect_existing as get_db                # noqa: E402
from trader.strategies.high_confidence.core import (                 # noqa: E402
    ensure_high_confidence_tables,
    get_hc_available_dates,
    high_confidence_payload,
    build_high_confidence_payload_realtime,
)
from trader.strategies.high_confidence.config import (               # noqa: E402
    HC_DEFAULT_MAX_PER_DATE,
    HC_DEFAULT_PATTERNS,
    HC_MIN_FULL_MARKET_ROWS,
)

# ── CLI 入口（cron: python app.py --momentum-daily 等）──────────
if __name__ == "__main__":
    from trader.cli.main import main
    raise SystemExit(main())
