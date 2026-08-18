"""资金面模块机构级升级 — pytest 回归 (离线, mock 数据, 不调 LLM)

运行：pytest tests/test_capital_upgrade.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from layers.skills.capital_skill import capital_skill

capital_data = {
    "north": [{"净流入": n} for n in [15000, 12000, 18000, 9000, 21000, 16000,
                                       14000, 11000, 19000, 22000, 17000, 13000]],
    "main": [{"主力净流入": n} for n in [8000, 6000, 12000, 5000, 15000, 9000,
                                         7000, 4000, 11000, 10000]],
    "large_order_ratio": 0.35, "medium_order_ratio": 0.40,
    "margin": [{"融资余额": n} for n in [500000, 520000, 535000, 548000]],
    "short_balance": 80000, "short_change_pct": -2.5,
    "dragon": [
        {"date": "2026-05-07", "inst_buy": 35000, "inst_sell": 12000, "total_buy": 80000, "retail_buy": 25000},
        {"date": "2026-05-06", "inst_buy": 28000, "inst_sell": 15000, "total_buy": 70000, "retail_buy": 20000},
        {"date": "2026-05-05", "inst_buy": 42000, "inst_sell": 18000, "total_buy": 90000, "retail_buy": 30000},
        {"date": "2026-05-04", "inst_buy": 20000, "inst_sell": 22000, "total_buy": 65000, "retail_buy": 28000},
    ],
}


def _assert_score_range(signals):
    assert 0 <= signals.overall_score <= 100, f"overall_score 越界: {signals.overall_score}"
    assert isinstance(signals.capital_grade, str) and signals.capital_grade


def test_analyze_basic():
    signals = capital_skill.analyze(capital_data)
    _assert_score_range(signals)
    assert signals.north is not None
    assert signals.main is not None
    assert signals.margin is not None
    assert signals.dragon is not None


def test_field_completeness():
    signals = capital_skill.analyze(capital_data)
    north_fields = ["net_inflow", "net_inflow_pct", "cumulative_5d", "cumulative_10d",
                    "cumulative_30d", "trend_5d", "trend_10d", "trend_30d", "signal", "consistency"]
    for f in north_fields:
        assert getattr(signals.north, f, None) is not None, f"north.{f} 缺失"

    main_fields = ["net_inflow", "net_inflow_pct", "large_order_ratio", "medium_order_ratio",
                   "small_order_ratio", "cumulative_5d", "cumulative_10d", "trend_5d",
                   "trend_10d", "signal", "institutional_intent"]
    for f in main_fields:
        assert getattr(signals.main, f, None) is not None, f"main.{f} 缺失"

    margin_fields = ["margin_balance", "margin_change_pct", "short_balance",
                     "short_change_pct", "net_leverage", "leverage_signal", "risk_level"]
    for f in margin_fields:
        assert getattr(signals.margin, f, None) is not None, f"margin.{f} 缺失"

    dragon_fields = ["active_days_30d", "institutional_buy_ratio", "institutional_sell_ratio",
                     "net_institutional_flow", "top5_seat_ratio", "institutional_buy_pct",
                     "retail_buy_pct", "consecutive_days", "top_buyer_type", "signal", "hot_money_trace"]
    for f in dragon_fields:
        assert getattr(signals.dragon, f, None) is not None, f"dragon.{f} 缺失"

    flow_fields = ["north_trend", "main_trend", "margin_trend", "dragon_trend",
                   "consensus_level", "divergence_warning"]
    for f in flow_fields:
        assert getattr(signals.flow_structure, f, None) is not None, f"flow_structure.{f} 缺失"

    top_fields = ["overall_score", "capital_grade", "risk_signal", "research_advice", "divergence_signals"]
    for f in top_fields:
        assert getattr(signals, f, None) is not None, f"signals.{f} 缺失"


def test_fault_tolerance():
    for bad in ({}, None, {"north": [], "main": []}, "invalid"):
        s = capital_skill.analyze(bad)
        _assert_score_range(s)  # 不应抛异常, 评分在合法范围


def test_divergence_signal():
    div_data = {
        "north": [{"净流入": 20000}] * 12,
        "main": [{"主力净流入": -5000}] * 10,
        "margin": [{"融资余额": 500000}, {"融资余额": 510000}],
        "dragon": [],
    }
    s = capital_skill.analyze(div_data)
    assert isinstance(s.divergence_signals, list), "divergence_signals 应为列表"


def test_weighted_scoring_bull_vs_bear():
    bull_data = {
        "north": [{"净流入": 30000}] * 12,
        "main": [{"主力净流入": 20000}] * 10,
        "large_order_ratio": 0.5, "medium_order_ratio": 0.35,
        "margin": [{"融资余额": 500000}, {"融资余额": 530000}, {"融资余额": 560000}],
        "short_balance": 50000, "short_change_pct": -5,
        "dragon": [
            {"date": "2026-05-07", "inst_buy": 50000, "inst_sell": 10000, "total_buy": 100000, "retail_buy": 20000},
            {"date": "2026-05-06", "inst_buy": 45000, "inst_sell": 8000, "total_buy": 90000, "retail_buy": 15000},
            {"date": "2026-05-05", "inst_buy": 55000, "inst_sell": 12000, "total_buy": 110000, "retail_buy": 25000},
        ],
    }
    bear_data = {
        "north": [{"净流入": -20000}] * 12,
        "main": [{"主力净流入": -15000}] * 10,
        "large_order_ratio": 0.15, "medium_order_ratio": 0.35,
        "margin": [{"融资余额": 500000}, {"融资余额": 480000}, {"融资余额": 460000}],
        "short_balance": 120000, "short_change_pct": 10,
        "dragon": [{"date": "2026-05-07", "inst_buy": 5000, "inst_sell": 30000, "total_buy": 50000, "retail_buy": 35000}],
    }
    s_bull = capital_skill.analyze(bull_data)
    s_bear = capital_skill.analyze(bear_data)
    assert s_bull.overall_score > s_bear.overall_score, "全积极场景评分应高于全消极场景"


def test_interface_compatibility():
    signals = capital_skill.analyze(capital_data)
    for f in ["north", "main", "margin", "dragon", "flow_structure", "overall_score",
              "capital_grade", "risk_signal", "research_advice", "divergence_signals"]:
        assert hasattr(signals, f), f"缺少字段 {f}"
