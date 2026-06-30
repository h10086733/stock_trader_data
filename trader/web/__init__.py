"""trader.web — Flask 应用工厂。

create_app() 创建 Flask app，注册所有蓝图，供 app.py 降级为 shim 后直接使用。
"""
from __future__ import annotations

from flask import Flask, jsonify
from werkzeug.exceptions import HTTPException


def create_app() -> Flask:
    """工厂函数：创建并配置 Flask 应用，注册所有蓝图。"""
    app = Flask(__name__)

    @app.errorhandler(Exception)
    def handle_error(error):
        if isinstance(error, HTTPException):
            return jsonify({"error": error.description}), error.code
        return jsonify({"error": str(error)}), 500

    from trader.web.blueprints.pages import bp as pages_bp
    from trader.web.blueprints.csi1000 import bp as csi1000_bp
    from trader.web.blueprints.high_confidence import bp as hc_bp
    from trader.web.blueprints.indices import bp as indices_bp
    from trader.web.blueprints.pattern import bp as pattern_bp
    from trader.web.blueprints.momentum import bp as momentum_bp
    from trader.web.blueprints.surge import bp as surge_bp

    for bp in [pages_bp, csi1000_bp, hc_bp, indices_bp, pattern_bp, momentum_bp, surge_bp]:
        app.register_blueprint(bp)

    return app
