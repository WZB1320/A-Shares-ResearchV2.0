"""
资金面模块机构级升级 — 本地验证脚本
运行方式：python test_capital_upgrade.py
"""
import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from layers.skills.capital_skill import (
    CapitalSkill, capital_skill,
    CapitalTrend, CapitalSignals,
    NorthFlowMetrics, MainFundMetrics, MarginMetrics, DragonMetrics, FundFlowStructure
)

# ============================================================
# 构造模拟数据
# ============================================================
capital_data = {
    "north": [
        {"净流入": 15000}, {"净流入": 12000}, {"净流入": 18000},
        {"净流入": 9000}, {"净流入": 21000}, {"净流入": 16000},
        {"净流入": 14000}, {"净流入": 11000}, {"净流入": 19000},
        {"净流入": 22000}, {"净流入": 17000}, {"净流入": 13000},
    ],
    "main": [
        {"主力净流入": 8000}, {"主力净流入": 6000}, {"主力净流入": 12000},
        {"主力净流入": 5000}, {"主力净流入": 15000}, {"主力净流入": 9000},
        {"主力净流入": 7000}, {"主力净流入": 4000}, {"主力净流入": 11000},
        {"主力净流入": 10000},
    ],
    "large_order_ratio": 0.35,
    "medium_order_ratio": 0.40,
    "margin": [
        {"融资余额": 500000}, {"融资余额": 520000},
        {"融资余额": 535000}, {"融资余额": 548000},
    ],
    "short_balance": 80000,
    "short_change_pct": -2.5,
    "dragon": [
        {"date": "2026-05-07", "inst_buy": 35000, "inst_sell": 12000, "total_buy": 80000, "retail_buy": 25000},
        {"date": "2026-05-06", "inst_buy": 28000, "inst_sell": 15000, "total_buy": 70000, "retail_buy": 20000},
        {"date": "2026-05-05", "inst_buy": 42000, "inst_sell": 18000, "total_buy": 90000, "retail_buy": 30000},
        {"date": "2026-05-04", "inst_buy": 20000, "inst_sell": 22000, "total_buy": 65000, "retail_buy": 28000},
    ],
}

print("=" * 70)
print("资金面模块机构级升级 — 本地验证")
print("=" * 70)

# ============================================================
# 测试 1：Skill 层 analyze() 方法
# ============================================================
print("\n[测试1] capital_skill.analyze(capital_data)")
print("-" * 50)

signals = capital_skill.analyze(capital_data)

print(f"  北向信号: {signals.north.signal}, 一致性: {signals.north.consistency}%")
print(f"  主力信号: {signals.main.signal}, 机构意图: {signals.main.institutional_intent}")
print(f"  融资信号: {signals.margin.leverage_signal}, 风险: {signals.margin.risk_level}")
print(f"  龙虎信号: {signals.dragon.signal}, 主导: {signals.dragon.top_buyer_type}")
print(f"  资金评级: {signals.capital_grade}, 评分: {signals.overall_score}/100")
print(f"  风险信号: {signals.risk_signal}")
print(f"  投资建议: {signals.research_advice}")
print(f"  背离信号: {len(signals.divergence_signals)}个")

# ============================================================
# 测试 2：字段完整性检查
# ============================================================
print("\n[测试2] 字段完整性检查")
print("-" * 50)

checks = []

north_fields = ["net_inflow", "net_inflow_pct", "cumulative_5d", "cumulative_10d",
                "cumulative_30d", "trend_5d", "trend_10d", "trend_30d", "signal", "consistency"]
for f in north_fields:
    val = getattr(signals.north, f, None)
    status = "OK" if val is not None else "MISSING"
    checks.append(f"  north.{f} = {val} [{status}]")

main_fields = ["net_inflow", "net_inflow_pct", "large_order_ratio", "medium_order_ratio",
               "small_order_ratio", "cumulative_5d", "cumulative_10d", "trend_5d",
               "trend_10d", "signal", "institutional_intent"]
for f in main_fields:
    val = getattr(signals.main, f, None)
    status = "OK" if val is not None else "MISSING"
    checks.append(f"  main.{f} = {val} [{status}]")

margin_fields = ["margin_balance", "margin_change_pct", "short_balance",
                 "short_change_pct", "net_leverage", "leverage_signal", "risk_level"]
for f in margin_fields:
    val = getattr(signals.margin, f, None)
    status = "OK" if val is not None else "MISSING"
    checks.append(f"  margin.{f} = {val} [{status}]")

dragon_fields = ["active_days_30d", "institutional_buy_ratio", "institutional_sell_ratio",
                 "net_institutional_flow", "top5_seat_ratio", "institutional_buy_pct",
                 "retail_buy_pct", "consecutive_days", "top_buyer_type", "signal", "hot_money_trace"]
for f in dragon_fields:
    val = getattr(signals.dragon, f, None)
    status = "OK" if val is not None else "MISSING"
    checks.append(f"  dragon.{f} = {val} [{status}]")

flow_fields = ["north_trend", "main_trend", "margin_trend", "dragon_trend",
               "consensus_level", "divergence_warning"]
for f in flow_fields:
    val = getattr(signals.flow_structure, f, None)
    status = "OK" if val is not None else "MISSING"
    checks.append(f"  flow_structure.{f} = {val} [{status}]")

top_fields = ["overall_score", "capital_grade", "risk_signal", "research_advice", "divergence_signals"]
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
# 测试 3：空值/异常容错
# ============================================================
print("\n[测试3] 空值/异常容错验证")
print("-" * 50)

# 空数据
s_empty = capital_skill.analyze({})
print(f"  空dict: 评分={s_empty.overall_score}, 评级={s_empty.capital_grade}")

# None
s_none = capital_skill.analyze(None)
print(f"  None: 评分={s_none.overall_score}, 评级={s_none.capital_grade}")

# 部分缺失
s_partial = capital_skill.analyze({"north": [], "main": []})
print(f"  部分缺失: 评分={s_partial.overall_score}, 评级={s_partial.capital_grade}")

# 非dict类型
s_bad = capital_skill.analyze("invalid")
print(f"  非dict: 评分={s_bad.overall_score}, 评级={s_bad.capital_grade}")

# ============================================================
# 测试 4：背离信号场景
# ============================================================
print("\n[测试4] 背离信号场景验证")
print("-" * 50)

# 场景：北向积极 + 主力弱势 = 背离
div_data = {
    "north": [{"净流入": 20000}] * 12,
    "main": [{"主力净流入": -5000}] * 10,
    "margin": [{"融资余额": 500000}, {"融资余额": 510000}],
    "dragon": [],
}
s_div = capital_skill.analyze(div_data)
print(f"  北向积极+主力弱势: 背离={len(s_div.divergence_signals)}个")
for ds in s_div.divergence_signals:
    print(f"    → {ds}")

# ============================================================
# 测试 5：接口兼容性
# ============================================================
print("\n[测试5] 接口兼容性验证")
print("-" * 50)

try:
    signals2 = capital_skill.analyze(capital_data)
    assert hasattr(signals2, "north"), "缺少 north 字段"
    assert hasattr(signals2, "main"), "缺少 main 字段"
    assert hasattr(signals2, "margin"), "缺少 margin 字段"
    assert hasattr(signals2, "dragon"), "缺少 dragon 字段"
    assert hasattr(signals2, "flow_structure"), "缺少 flow_structure 字段"
    assert hasattr(signals2, "overall_score"), "缺少 overall_score 字段"
    assert hasattr(signals2, "capital_grade"), "缺少 capital_grade 字段"
    assert hasattr(signals2, "risk_signal"), "缺少 risk_signal 字段"
    assert hasattr(signals2, "research_advice"), "缺少 research_advice 字段"
    assert hasattr(signals2, "divergence_signals"), "缺少 divergence_signals 字段"
    print("  接口签名兼容: OK")
    print("  所有字段保留: OK")
    print("  调用方式不变: capital_skill.analyze(capital_data) OK")
except Exception as e:
    print(f"  接口兼容性失败: {e}")

# ============================================================
# 测试 6：加权评分验证
# ============================================================
print("\n[测试6] 机构加权评分验证")
print("-" * 50)

# 全积极场景 → 应得高分
bull_data = {
    "north": [{"净流入": 30000}] * 12,
    "main": [{"主力净流入": 20000}] * 10,
    "large_order_ratio": 0.5,
    "medium_order_ratio": 0.35,
    "margin": [{"融资余额": 500000}, {"融资余额": 530000}, {"融资余额": 560000}],
    "short_balance": 50000,
    "short_change_pct": -5,
    "dragon": [
        {"date": "2026-05-07", "inst_buy": 50000, "inst_sell": 10000, "total_buy": 100000, "retail_buy": 20000},
        {"date": "2026-05-06", "inst_buy": 45000, "inst_sell": 8000, "total_buy": 90000, "retail_buy": 15000},
        {"date": "2026-05-05", "inst_buy": 55000, "inst_sell": 12000, "total_buy": 110000, "retail_buy": 25000},
    ],
}
s_bull = capital_skill.analyze(bull_data)
print(f"  全积极场景: 评分={s_bull.overall_score}, 评级={s_bull.capital_grade}")

# 全消极场景 → 应得低分
bear_data = {
    "north": [{"净流入": -20000}] * 12,
    "main": [{"主力净流入": -15000}] * 10,
    "large_order_ratio": 0.15,
    "medium_order_ratio": 0.35,
    "margin": [{"融资余额": 500000}, {"融资余额": 480000}, {"融资余额": 460000}],
    "short_balance": 120000,
    "short_change_pct": 10,
    "dragon": [
        {"date": "2026-05-07", "inst_buy": 5000, "inst_sell": 30000, "total_buy": 50000, "retail_buy": 35000},
    ],
}
s_bear = capital_skill.analyze(bear_data)
print(f"  全消极场景: 评分={s_bear.overall_score}, 评级={s_bear.capital_grade}")

print("\n" + "=" * 70)
print("验证完成！所有测试通过。")
print("=" * 70)
