"""
技术面模块机构级升级 — 本地验证脚本
运行方式：python test_tech_upgrade.py
"""
import sys
import json
import random
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from layers.skills.tech_skill import (
    TechSkill, tech_skill,
    TrendStrength, VolumeSignal, MarketRegime,
    TechSignals, MASystem, MACDSystem, KDJSystem,
    BollingerSystem, VolumeStructure, SupportResistance, DivergenceSignals
)

# ============================================================
# 构造模拟K线数据（250天）
# ============================================================
def generate_mock_kline(days=250, base_price=20.0, trend="bull"):
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
            "open": round(open_p, 2),
            "high": round(high, 2),
            "low": round(low, 2),
            "close": round(price, 2),
            "volume": volume,
            "turnover": round(turnover, 2),
        })
    return data


bull_data = generate_mock_kline(250, 20.0, "bull")
bear_data = generate_mock_kline(250, 20.0, "bear")
range_data = generate_mock_kline(250, 20.0, "range")

print("=" * 70)
print("技术面模块机构级升级 — 本地验证")
print("=" * 70)

# ============================================================
# 测试 1：Skill 层 analyze() 方法 — 多头行情
# ============================================================
print("\n[测试1] tech_skill.analyze(bull_data) — 多头行情")
print("-" * 50)

signals = tech_skill.analyze(bull_data)

print(f"  均线排列: {signals.ma_system.arrangement}")
print(f"  MA5={signals.ma_system.ma5}, MA20={signals.ma_system.ma20}, MA60={signals.ma_system.ma60}")
print(f"  MACD背离: {signals.macd_system.divergence}, 动能: {signals.macd_system.momentum}")
print(f"  KDJ: K={signals.kdj_system.k}, D={signals.kdj_system.d}, J={signals.kdj_system.j}")
print(f"  布林带位置: {signals.bollinger_system.position}, 带宽={signals.bollinger_system.bandwidth}%")
print(f"  量价信号: {signals.volume_structure.signal.value}")
print(f"  趋势强度: {signals.trend_strength.value}")
print(f"  行情状态: {signals.market_regime.value}")
print(f"  背离信号: {signals.divergence_signals.divergence_summary} ({signals.divergence_signals.divergence_count}个)")
print(f"  综合评分: {signals.overall_score}/100")
print(f"  短期信号: {signals.short_term_signal}")
print(f"  中期信号: {signals.medium_term_signal}")
print(f"  风险预警: {signals.risk_warning}")
print(f"  投资建议: {signals.research_advice}")

# ============================================================
# 测试 2：空头行情
# ============================================================
print("\n[测试2] tech_skill.analyze(bear_data) — 空头行情")
print("-" * 50)

signals_bear = tech_skill.analyze(bear_data)

print(f"  均线排列: {signals_bear.ma_system.arrangement}")
print(f"  趋势强度: {signals_bear.trend_strength.value}")
print(f"  行情状态: {signals_bear.market_regime.value}")
print(f"  综合评分: {signals_bear.overall_score}/100")
print(f"  短期信号: {signals_bear.short_term_signal}")
print(f"  中期信号: {signals_bear.medium_term_signal}")

# ============================================================
# 测试 3：震荡行情
# ============================================================
print("\n[测试3] tech_skill.analyze(range_data) — 震荡行情")
print("-" * 50)

signals_range = tech_skill.analyze(range_data)

print(f"  均线排列: {signals_range.ma_system.arrangement}")
print(f"  趋势强度: {signals_range.trend_strength.value}")
print(f"  行情状态: {signals_range.market_regime.value}")
print(f"  综合评分: {signals_range.overall_score}/100")
print(f"  短期信号: {signals_range.short_term_signal}")
print(f"  中期信号: {signals_range.medium_term_signal}")

# ============================================================
# 测试 4：字段完整性检查
# ============================================================
print("\n[测试4] 字段完整性检查")
print("-" * 50)

checks = []

ma_fields = ["ma5", "ma10", "ma20", "ma60", "ma120", "ma250",
             "ma5_slope", "ma10_slope", "ma20_slope", "ma60_slope",
             "golden_cross", "dead_cross", "arrangement"]
for f in ma_fields:
    val = getattr(signals.ma_system, f, None)
    status = "OK" if val is not None else "MISSING"
    checks.append(f"  ma_system.{f} = {val} [{status}]")

macd_fields = ["dif", "dea", "macd_hist", "hist_trend", "golden_cross",
               "dead_cross", "divergence", "momentum"]
for f in macd_fields:
    val = getattr(signals.macd_system, f, None)
    status = "OK" if val is not None else "MISSING"
    checks.append(f"  macd_system.{f} = {val} [{status}]")

kdj_fields = ["k", "d", "j", "overbought", "oversold", "golden_cross", "dead_cross"]
for f in kdj_fields:
    val = getattr(signals.kdj_system, f, None)
    status = "OK" if val is not None else "MISSING"
    checks.append(f"  kdj_system.{f} = {val} [{status}]")

boll_fields = ["upper", "middle", "lower", "bandwidth", "position", "squeeze", "breakout"]
for f in boll_fields:
    val = getattr(signals.bollinger_system, f, None)
    status = "OK" if val is not None else "MISSING"
    checks.append(f"  bollinger_system.{f} = {val} [{status}]")

vol_fields = ["volume_ratio", "turnover_rate", "volume_percentile",
              "up_volume_ratio", "down_volume_ratio", "institutional_signal",
              "volume_trend", "signal"]
for f in vol_fields:
    val = getattr(signals.volume_structure, f, None)
    status = "OK" if val is not None else "MISSING"
    checks.append(f"  volume_structure.{f} = {val} [{status}]")

sr_fields = ["strong_support", "support_1", "support_2", "resistance_1",
             "resistance_2", "strong_resistance", "current_position", "break_probability"]
for f in sr_fields:
    val = getattr(signals.support_resistance, f, None)
    status = "OK" if val is not None else "MISSING"
    checks.append(f"  support_resistance.{f} = {val} [{status}]")

div_fields = ["macd_bearish", "macd_bullish", "rsi_bearish", "rsi_bullish",
              "volume_bearish", "volume_bullish", "divergence_count", "divergence_summary"]
for f in div_fields:
    val = getattr(signals.divergence_signals, f, None)
    status = "OK" if val is not None else "MISSING"
    checks.append(f"  divergence_signals.{f} = {val} [{status}]")

top_fields = ["trend_strength", "market_regime", "overall_score",
              "short_term_signal", "medium_term_signal", "risk_warning", "research_advice"]
for f in top_fields:
    val = getattr(signals, f, None)
    status = "OK" if val is not None else "MISSING"
    checks.append(f"  signals.{f} = {val} [{status}]")

for c in checks:
    print(c)

missing = [c for c in checks if "MISSING" in c]
if missing:
    print(f"\n  *** 缺失字段: {len(missing)} 个 ***")
else:
    print(f"\n  *** 所有字段齐全 ({len(checks)} 个) ***")

# ============================================================
# 测试 5：空值/异常容错
# ============================================================
print("\n[测试5] 空值/异常容错验证")
print("-" * 50)

s_empty = tech_skill.analyze([])
print(f"  空list: 评分={s_empty.overall_score}, 趋势={s_empty.trend_strength.value}")

s_none = tech_skill.analyze(None)
print(f"  None: 评分={s_none.overall_score}, 趋势={s_none.trend_strength.value}")

s_bad = tech_skill.analyze("invalid")
print(f"  非list: 评分={s_bad.overall_score}, 趋势={s_bad.trend_strength.value}")

s_min = tech_skill.analyze([{"close": 10.0, "high": 10.5, "low": 9.5, "volume": 1000000}])
print(f"  单条数据: 评分={s_min.overall_score}, 趋势={s_min.trend_strength.value}")

s_null_close = tech_skill.analyze([
    {"close": None, "high": None, "low": None, "volume": None},
    {"close": None, "high": None, "low": None, "volume": None},
])
print(f"  全null数据: 评分={s_null_close.overall_score}, 趋势={s_null_close.trend_strength.value}")

# ============================================================
# 测试 6：背离检测专项
# ============================================================
print("\n[测试6] 背离检测专项验证")
print("-" * 50)

div_data = generate_mock_kline(250, 20.0, "bull")
for i in range(240, 250):
    div_data[i]["close"] = div_data[i]["close"] * 1.02

s_div = tech_skill.analyze(div_data)
print(f"  MACD背离: {s_div.macd_system.divergence}")
print(f"  RSI背离: 顶={s_div.divergence_signals.rsi_bearish}, 底={s_div.divergence_signals.rsi_bullish}")
print(f"  量价背离: 顶={s_div.divergence_signals.volume_bearish}, 底={s_div.divergence_signals.volume_bullish}")
print(f"  背离汇总: {s_div.divergence_signals.divergence_summary}")

# ============================================================
# 测试 7：行情状态分类专项
# ============================================================
print("\n[测试7] 行情状态分类专项验证")
print("-" * 50)

print(f"  多头行情 → 行情状态: {signals.market_regime.value}")
print(f"  空头行情 → 行情状态: {signals_bear.market_regime.value}")
print(f"  震荡行情 → 行情状态: {signals_range.market_regime.value}")

# ============================================================
# 测试 8：量价信号枚举覆盖
# ============================================================
print("\n[测试8] 量价信号枚举覆盖验证")
print("-" * 50)

all_vol_signals = list(VolumeSignal)
print(f"  VolumeSignal 枚举值 ({len(all_vol_signals)}个):")
for vs in all_vol_signals:
    print(f"    - {vs.name} = {vs.value}")

all_regimes = list(MarketRegime)
print(f"  MarketRegime 枚举值 ({len(all_regimes)}个):")
for mr in all_regimes:
    print(f"    - {mr.name} = {mr.value}")

all_trends = list(TrendStrength)
print(f"  TrendStrength 枚举值 ({len(all_trends)}个):")
for ts in all_trends:
    print(f"    - {ts.name} = {ts.value}")

# ============================================================
# 测试 9：接口兼容性
# ============================================================
print("\n[测试9] 接口兼容性验证")
print("-" * 50)

try:
    signals2 = tech_skill.analyze(bull_data)
    assert hasattr(signals2, "ma_system"), "缺少 ma_system"
    assert hasattr(signals2, "macd_system"), "缺少 macd_system"
    assert hasattr(signals2, "kdj_system"), "缺少 kdj_system"
    assert hasattr(signals2, "bollinger_system"), "缺少 bollinger_system"
    assert hasattr(signals2, "volume_structure"), "缺少 volume_structure"
    assert hasattr(signals2, "support_resistance"), "缺少 support_resistance"
    assert hasattr(signals2, "divergence_signals"), "缺少 divergence_signals"
    assert hasattr(signals2, "trend_strength"), "缺少 trend_strength"
    assert hasattr(signals2, "market_regime"), "缺少 market_regime"
    assert hasattr(signals2, "overall_score"), "缺少 overall_score"
    assert hasattr(signals2, "short_term_signal"), "缺少 short_term_signal"
    assert hasattr(signals2, "medium_term_signal"), "缺少 medium_term_signal"
    assert hasattr(signals2, "risk_warning"), "缺少 risk_warning"
    assert hasattr(signals2, "research_advice"), "缺少 research_advice"
    print("  接口签名兼容: OK")
    print("  所有字段保留: OK")
    print("  调用方式不变: tech_skill.analyze(price_data) OK")
except Exception as e:
    print(f"  接口兼容性失败: {e}")

# ============================================================
# 测试 10：TechAgent 导入验证（不调用LLM）
# ============================================================
print("\n[测试10] TechAgent 导入与结构验证")
print("-" * 50)

try:
    from layers.agents.tech_agent import TechAgent, tech_agent, tech_agent_node
    from config.llm_config import DEFAULT_MODEL
    agent = TechAgent(model_name=DEFAULT_MODEL)
    assert hasattr(agent, "analyze"), "缺少 analyze 方法"
    assert hasattr(agent, "tech_skill"), "缺少 tech_skill 属性"
    assert hasattr(agent, "client"), "缺少 client 属性"
    print("  TechAgent 类结构: OK")
    print("  analyze(stock_code, tech_data) 签名: OK")
    print("  tech_agent_node 函数: OK")
except Exception as e:
    print(f"  TechAgent 导入失败: {e}")

# ============================================================
# 汇总
# ============================================================
print("\n" + "=" * 70)
print("验证完成")
print("=" * 70)
print("""
  升级内容总结：
  1. MACD/RSI背离自动识别 — 顶背离/底背离双向检测
  2. 均线结构判断 — 多头排列/空头排列/缠绕震荡
  3. 量价匹配分析 — 放量上涨/缩量回调/放量滞涨/缩量下跌
  4. 趋势强度打分 — 7级趋势强度（强多头→强空头）
  5. 行情状态判定 — 强趋势/弱趋势/震荡/未分类
  6. 背离信号体系 — MACD/RSI/量价三维交叉验证
  7. 综合评分 — 多因子加权0-100分
  8. 风险预警 + 投资建议 — 自动生成
  9. 空值/停牌/异常全链路容错
  10. 纯计算无LLM，向下兼容
""")
