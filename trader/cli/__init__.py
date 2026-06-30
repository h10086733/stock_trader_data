"""trader CLI 入口。

使用方式：
  python -m trader.cli [选项]
  python -m trader.cli --serve          # 启动 Web 服务
  python -m trader.cli --momentum-daily # 每日动量扫描+结算
  python -m trader.cli --csi1000-daily  # 重算中证1000择时

所有 CLI 参数与原 app.py 的 main() 保持兼容。
"""
from trader.cli.main import main

__all__ = ["main"]
