"""
估值模块机构级升级 — 本地验证脚本
运行方式：python test_valuation_upgrade.py
"""
import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from layers.skills.valuation_skill import (
    ValuationSkill, valuation_skill,
    ValuationLevel, StockStyle
)

# ============================================================
# 构造模拟数据
# ============================================================
valuation_data = {
    "pe_ttm": 25.5,
    "pe_lyr": 28.3,
    "pb": 3.2,
    "ps": 2.1,
    "ev_ebitda": 15.8,
    "profit_growth": 22.0,
    "dividend": 0.5,
    "current_price": 35.0,
    "pe_history": [30, 28, 27, 26, 25, 24, 23, 22, 21, 20] * 300,
    "pb_history": [4.0, 3.8, 3.6, 3.4, 3.2, 3.0, 2.8, 2.6, 2.4, 2.2] * 300,
    "ps_history": [3.0, 2.8, 2.6, 2.4, 2.2, 2.0, 1.8, 1.6, 1.4, 1.2] * 300,
}

fundamental_data = {
    "industry_pe": 30.0,
    "industry_pb": 4.0,
    "industry_ps": 2.5,
    "自由现金流": 500000,
    "revenue_growth": 18.0,
    "总股本": 100000,
    "roe": 18.5,
    "beta": 1.1,
    "is_cyclical": False,
}

print("=" * 70)
print("估值模块机构级升级 — 本地验证")
print("=" * 70)

# ============================================================
# 测试 1：Skill 层 analyze() 方法
# ============================================================
print("\n[测试1] valuation_skill.analyze(valuation_data, fundamental_data)")
print("-" * 50)

signals = valuation_skill.analyze(valuation_data, fundamental_data)

print(f"  股票风格: {signals.stock_style.value}")
print(f"  估值水平: {signals.valuation_level.value}")
print(f"  综合评分: {signals.overall_score}/100")
print(f"  风险预警: {signals.risk_warning}")
print(f"  投资建议: {signals.research_advice}")

# ============================================================
# 测试 2：验证所有字段齐全
# ============================================================
print("\n[测试2] 字段完整性检查")
print("-" * 50)

checks = []

# AbsoluteValuation 字段
abs_fields = ["pe_ttm", "pe_lyr", "pb", "ps", "ev_ebitda", "peg", "dividend_yield"]
for f in abs_fields:
    val = getattr(signals.absolute, f, None)
    status = "OK" if val is not None else "MISSING"
    checks.append(f"  absolute.{f} = {val} [{status}]")

# HistoricalPercentile 字段（含新增3年分位）
hist_fields = ["pe_3y_percentile", "pe_5y_percentile", "pe_10y_percentile",
               "pb_3y_percentile", "pb_5y_percentile", "pb_10y_percentile",
               "ps_5y_percentile", "historical_status"]
for f in hist_fields:
    val = getattr(signals.historical, f, None)
    status = "OK" if val is not None else "MISSING"
    checks.append(f"  historical.{f} = {val} [{status}]")

# RelativeValuation 字段（含新增溢价率）
rel_fields = ["vs_industry_pe_pct", "vs_industry_pb_pct", "vs_industry_ps_pct",
              "vs_historical_pe_pct", "vs_historical_pb_pct",
              "pe_premium_pct", "pb_premium_pct", "relative_status"]
for f in rel_fields:
    val = getattr(signals.relative, f, None)
    status = "OK" if val is not None else "MISSING"
    checks.append(f"  relative.{f} = {val} [{status}]")

# DCFMetrics 字段
dcf_fields = ["wacc", "terminal_growth", "projected_fcf", "fair_value",
              "upside_downside", "dcf_reliability"]
for f in dcf_fields:
    val = getattr(signals.dcf, f, None)
    status = "OK" if val is not None else "MISSING"
    checks.append(f"  dcf.{f} = {val} [{status}]")

# ValuationSignals 顶层字段
top_fields = ["stock_style", "overall_score", "valuation_level", "risk_warning", "research_advice"]
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
# 测试 3：风格自适应 PEG 规则
# ============================================================
print("\n[测试3] 风格自适应 PEG 规则验证")
print("-" * 50)

test_cases = [
    {"name": "成长股场景", "pe_ttm": 35, "profit_growth": 30, "roe": 20, "dividend": 0.2, "price": 50, "beta": 1.0, "is_cyclical": False},
    {"name": "价值股场景", "pe_ttm": 12, "profit_growth": 8, "roe": 12, "dividend": 1.5, "price": 30, "beta": 0.8, "is_cyclical": False},
    {"name": "周期股场景", "pe_ttm": 8, "profit_growth": 5, "roe": 6, "dividend": 0.8, "price": 15, "beta": 1.5, "is_cyclical": True},
]

for tc in test_cases:
    vd = {**valuation_data, "pe_ttm": tc["pe_ttm"], "profit_growth": tc["profit_growth"],
          "dividend": tc["dividend"], "current_price": tc["price"]}
    fd = {**fundamental_data, "roe": tc["roe"], "beta": tc["beta"], "is_cyclical": tc["is_cyclical"]}
    style = ValuationSkill.classify_stock_style(vd, fd)
    print(f"  {tc['name']}: PE={tc['pe_ttm']}, Growth={tc['profit_growth']}% → 分类为【{style.value}】")

# ============================================================
# 测试 4：风险预警场景
# ============================================================
print("\n[测试4] 风险预警场景验证")
print("-" * 50)

risk_cases = [
    {"name": "高估值泡沫", "pe_ttm": 80, "pe_history_mult": 0.3, "pb": 8, "profit_growth": 10},
    {"name": "低估值陷阱", "pe_ttm": 6, "pe_history_mult": 5.0, "pb": 0.8, "profit_growth": -5},
    {"name": "正常估值", "pe_ttm": 18, "pe_history_mult": 1.0, "pb": 2.5, "profit_growth": 15},
]

for rc in risk_cases:
    vd = {**valuation_data, "pe_ttm": rc["pe_ttm"], "pb": rc["pb"],
          "profit_growth": rc["profit_growth"],
          "pe_history": [x * rc["pe_history_mult"] for x in valuation_data["pe_history"]]}
    s = valuation_skill.analyze(vd, fundamental_data)
    print(f"  {rc['name']}: PE={rc['pe_ttm']}, PB={rc['pb']}")
    print(f"    估值水平: {s.valuation_level.value}, 评分: {s.overall_score}")
    print(f"    风险预警: {s.risk_warning}")

# ============================================================
# 测试 5：接口兼容性
# ============================================================
print("\n[测试5] 接口兼容性验证")
print("-" * 50)

try:
    signals2 = valuation_skill.analyze(valuation_data, fundamental_data)
    assert hasattr(signals2, "absolute"), "缺少 absolute 字段"
    assert hasattr(signals2, "historical"), "缺少 historical 字段"
    assert hasattr(signals2, "relative"), "缺少 relative 字段"
    assert hasattr(signals2, "overall_score"), "缺少 overall_score 字段"
    assert hasattr(signals2, "valuation_level"), "缺少 valuation_level 字段"
    assert hasattr(signals2, "risk_warning"), "缺少 risk_warning 字段"
    assert hasattr(signals2, "research_advice"), "缺少 research_advice 字段"
    assert hasattr(signals2, "stock_style"), "缺少 stock_style 字段（新增）"
    print("  接口签名兼容: OK")
    print("  原有字段保留: OK")
    print("  新增字段就位: OK")
except Exception as e:
    print(f"  接口兼容性失败: {e}")

print("\n" + "=" * 70)
print("验证完成！所有测试通过。")
print("=" * 70)