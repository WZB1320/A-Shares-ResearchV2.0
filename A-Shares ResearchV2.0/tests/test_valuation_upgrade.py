"""估值模块 — pytest 回归 (离线, mock 数据, 不调 LLM)

运行：pytest tests/test_valuation_upgrade.py
注意：依据当前 ValuationSignals 真实结构 (overall_score / valuation_grade /
research_advice / risk_warnings / metrics) 编写，原脚本引用的 absolute/historical/
stock_style/valuation_level 等字段在当前实现中不存在，已据实修正。
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from layers.skills.valuation_skill import ValuationSkill, valuation_skill

valuation_data = {
    "pe_ttm": 25.5, "pe_lyr": 28.3, "pb": 3.2, "ps": 2.1, "ev_ebitda": 15.8,
    "profit_growth": 22.0, "dividend": 0.5, "current_price": 35.0,
    "pe_history": [30, 28, 27, 26, 25, 24, 23, 22, 21, 20] * 30,
    "pb_history": [4.0, 3.8, 3.6, 3.4, 3.2, 3.0, 2.8, 2.6, 2.4, 2.2] * 30,
    "ps_history": [3.0, 2.8, 2.6, 2.4, 2.2, 2.0, 1.8, 1.6, 1.4, 1.2] * 30,
}

fundamental_data = {
    "industry_pe": 30.0, "industry_pb": 4.0, "industry_ps": 2.5,
    "自由现金流": 500000, "revenue_growth": 18.0, "总股本": 100000,
    "roe": 18.5, "beta": 1.1, "is_cyclical": False,
}


def _assert_score_range(signals):
    assert isinstance(signals.overall_score, int), "overall_score 应为 int"
    assert 0 <= signals.overall_score <= 100, f"overall_score 越界: {signals.overall_score}"


def test_analyze_basic():
    signals = valuation_skill.analyze(valuation_data, fundamental_data)
    _assert_score_range(signals)
    assert isinstance(signals.valuation_grade, str) and signals.valuation_grade
    assert isinstance(signals.research_advice, str) and signals.research_advice
    assert isinstance(signals.risk_warnings, list)


def test_metrics_field_completeness():
    signals = valuation_skill.analyze(valuation_data, fundamental_data)
    m = signals.metrics
    assert m is not None
    for f in ["price", "pe_ttm", "pb", "pe_percentile", "pb_percentile",
               "industry_category", "data_available"]:
        assert hasattr(m, f), f"metrics.{f} 缺失"
    assert isinstance(signals.data_available_fields, list)
    assert isinstance(m.data_available, bool)


def test_classify_methods():
    # 当前 ValuationSkill 仅提供 classify_industry（按行业名匹配估值阈值类别）
    category, thresholds = ValuationSkill.classify_industry("白酒")
    assert category is not None, "classify_industry 应返回行业类别"
    assert thresholds is not None, "classify_industry 应返回阈值字典"
    for k in ("pe_low", "pe_high", "pb_low", "pb_high"):
        assert k in thresholds, f"阈值缺失 {k}"
    # 白酒应归入稳定消费/公用类别
    assert category == "stable", f"白酒应判定为 stable, 实际 {category}"
