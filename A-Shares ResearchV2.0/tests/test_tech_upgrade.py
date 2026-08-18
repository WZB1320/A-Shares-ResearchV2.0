"""技术面模块机构级升级 — pytest 回归 (离线, mock 数据, 不调 LLM)

运行：pytest tests/test_tech_upgrade.py
"""
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from layers.skills.tech_skill import (
    tech_skill,
    TrendStrength, VolumeSignal, MarketRegime, TechSignals,
)


def generate_mock_kline(days=250, base_price=20.0, trend="bull"):
    random.seed(42)
    data = []
    price = base_price
    for i in range(days):
        if trend == "bull":
            price += random.uniform(-0.05, 0.15)
        elif trend == "bear":
            price += random.uniform(-0.15, 0.05)
        elif trend == "range":
            price += random.uniform(-0.10, 0.10)
        else:
            price += random.uniform(-0.12, 0.12)
        price = max(price, 5.0)
        high = price * (1 + random.uniform(0, 0.03))
        low = price * (1 - random.uniform(0, 0.03))
        open_p = low + random.uniform(0, high - low)
        volume = int(random.uniform(5000000, 20000000))
        turnover = random.uniform(1.0, 5.0)
        data.append({
            "date": f"2025-{((i // 22) % 12) + 1:02d}-{(i % 28) + 1:02d}",
            "open": round(open_p, 2), "high": round(high, 2), "low": round(low, 2),
            "close": round(price, 2), "volume": volume, "turnover": round(turnover, 2),
        })
    return data


bull_data = generate_mock_kline(250, 20.0, "bull")
bear_data = generate_mock_kline(250, 20.0, "bear")
range_data = generate_mock_kline(250, 20.0, "range")


def _assert_score_range(signals):
    assert 0 <= signals.overall_score <= 100, f"overall_score 越界: {signals.overall_score}"


def test_bull_market():
    signals = tech_skill.analyze(bull_data)
    assert isinstance(signals, TechSignals)
    assert signals.trend_strength is not None
    assert isinstance(signals.market_regime, MarketRegime)
    _assert_score_range(signals)


def test_bear_market():
    signals = tech_skill.analyze(bear_data)
    assert isinstance(signals.market_regime, MarketRegime)
    _assert_score_range(signals)


def test_range_market():
    signals = tech_skill.analyze(range_data)
    assert isinstance(signals.market_regime, MarketRegime)
    _assert_score_range(signals)


def test_field_completeness():
    signals = tech_skill.analyze(bull_data)
    ma_fields = ["ma5", "ma10", "ma20", "ma60", "ma120", "ma250",
                 "ma5_slope", "ma10_slope", "ma20_slope", "ma60_slope",
                 "golden_cross", "dead_cross", "arrangement"]
    for f in ma_fields:
        assert getattr(signals.ma_system, f, None) is not None, f"ma_system.{f} 缺失"

    macd_fields = ["dif", "dea", "macd_hist", "hist_trend", "golden_cross",
                   "dead_cross", "divergence", "momentum"]
    for f in macd_fields:
        assert getattr(signals.macd_system, f, None) is not None, f"macd_system.{f} 缺失"

    kdj_fields = ["k", "d", "j", "overbought", "oversold", "golden_cross", "dead_cross"]
    for f in kdj_fields:
        assert getattr(signals.kdj_system, f, None) is not None, f"kdj_system.{f} 缺失"

    boll_fields = ["upper", "middle", "lower", "bandwidth", "position", "squeeze", "breakout"]
    for f in boll_fields:
        assert getattr(signals.bollinger_system, f, None) is not None, f"bollinger_system.{f} 缺失"

    vol_fields = ["volume_ratio", "turnover_rate", "volume_percentile",
                  "up_volume_ratio", "down_volume_ratio", "institutional_signal",
                  "volume_trend", "signal"]
    for f in vol_fields:
        assert getattr(signals.volume_structure, f, None) is not None, f"volume_structure.{f} 缺失"

    sr_fields = ["strong_support", "support_1", "support_2", "resistance_1",
                 "resistance_2", "strong_resistance", "current_position", "break_probability"]
    for f in sr_fields:
        assert getattr(signals.support_resistance, f, None) is not None, f"support_resistance.{f} 缺失"

    div_fields = ["macd_bearish", "macd_bullish", "rsi_bearish", "rsi_bullish",
                  "volume_bearish", "volume_bullish", "divergence_count", "divergence_summary"]
    for f in div_fields:
        assert getattr(signals.divergence_signals, f, None) is not None, f"divergence_signals.{f} 缺失"

    top_fields = ["trend_strength", "market_regime", "overall_score",
                  "short_term_signal", "medium_term_signal", "risk_warning", "research_advice"]
    for f in top_fields:
        assert getattr(signals, f, None) is not None, f"signals.{f} 缺失"


def test_fault_tolerance():
    for bad in ([], None, "invalid",
                [{"close": 10.0, "high": 10.5, "low": 9.5, "volume": 1000000}],
                [{"close": None, "high": None, "low": None, "volume": None},
                 {"close": None, "high": None, "low": None, "volume": None}]):
        s = tech_skill.analyze(bad)
        assert isinstance(s.trend_strength, TrendStrength)
        _assert_score_range(s)


def test_divergence_special():
    div_data = generate_mock_kline(250, 20.0, "bull")
    for i in range(240, 250):
        div_data[i]["close"] = div_data[i]["close"] * 1.02
    s = tech_skill.analyze(div_data)
    assert isinstance(s.divergence_signals.divergence_count, int)


def test_regime_classification():
    assert tech_skill.analyze(bull_data).market_regime.value
    assert tech_skill.analyze(bear_data).market_regime.value
    assert tech_skill.analyze(range_data).market_regime.value


def test_enum_coverage():
    assert len(list(VolumeSignal)) >= 1
    assert len(list(MarketRegime)) >= 1
    assert len(list(TrendStrength)) >= 1


def test_interface_compatibility():
    signals = tech_skill.analyze(bull_data)
    for f in ["ma_system", "macd_system", "kdj_system", "bollinger_system",
              "volume_structure", "support_resistance", "divergence_signals",
              "trend_strength", "market_regime", "overall_score",
              "short_term_signal", "medium_term_signal", "risk_warning", "research_advice"]:
        assert hasattr(signals, f), f"缺少字段 {f}"


def test_tech_agent_import():
    from layers.agents.tech_agent import TechAgent, tech_agent, tech_agent_node
    from config.llm_config import DEFAULT_MODEL
    agent = TechAgent(model_name=DEFAULT_MODEL)
    assert hasattr(agent, "analyze")
    # TechAgent 通过模块级 tech_skill 在 _run_skill 中调用，不持有 tech_skill 实例属性
    assert agent.dimension == "tech"
    assert hasattr(agent, "_run_skill")
    assert hasattr(agent, "client")
    assert hasattr(agent, "data_connector")
    assert callable(tech_agent_node)
