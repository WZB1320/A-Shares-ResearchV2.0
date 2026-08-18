"""研报筛选逻辑 — pytest 回归 (离线, 纯函数)

运行：pytest tests/test_filter_logic.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from layers.memory.report_fetcher import _is_depth_report, INSTITUTION_WHITELIST


def test_is_depth_report():
    cases = [
        ("深度研究：AI云业务拐点已至", True),
        ("首次覆盖：某某公司投资价值分析", True),
        ("专题研究：半导体行业", True),
        ("公司深度：小米汽车产业链", True),
        ("晨会纪要2025-05-20", False),
        ("日报：市场点评", False),
        ("事件点评：回购公告", False),
        ("快讯：某某公司Q1业绩预告", False),
        ("2025年报及2026一季报点评：收入利润增速均回正", False),
        ("跟踪深度：BABA回购加码信号积极", True),
    ]
    for title, expected in cases:
        assert _is_depth_report(title) == expected, f"《{title}》 期望 {expected}"


def test_institution_whitelist():
    # INSTITUTION_WHITELIST 为国内 Top30 券商白名单（不含海外投行）
    assert len(INSTITUTION_WHITELIST) >= 1
    assert "中金公司" in INSTITUTION_WHITELIST
    assert "中信证券" in INSTITUTION_WHITELIST
