"""页面路由蓝图：/ /momentum /pattern /high-confidence /surge /csi1000。"""
from flask import Blueprint, render_template_string
from trader.web.templates import (
    HTML, MOMENTUM_HTML, PATTERN_HTML,
    HIGH_CONFIDENCE_HTML, SURGE_HTML, CSI1000_HTML,
)

bp = Blueprint("pages", __name__)


@bp.route("/")
def index():
    return render_template_string(HTML)


@bp.route("/momentum")
def momentum_page():
    return render_template_string(MOMENTUM_HTML)


@bp.route("/pattern")
def pattern_page():
    return render_template_string(PATTERN_HTML)


@bp.route("/high-confidence")
def high_confidence_page():
    return render_template_string(HIGH_CONFIDENCE_HTML)


@bp.route("/surge")
def surge_page():
    return render_template_string(SURGE_HTML)


@bp.route("/csi1000")
def csi1000_page():
    return render_template_string(CSI1000_HTML)
