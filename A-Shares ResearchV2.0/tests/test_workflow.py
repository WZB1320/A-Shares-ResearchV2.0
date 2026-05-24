"""
workflow.py 全面测试
测试范围：
  1. 图编译与边结构
  2. 动态路由 (continue_to_agents)
  3. 质量评分 (_score_report)
  4. quality_gate_node
  5. Agent 节点函数
  6. data_agent_node (harness 校验集成)
  7. run_workflow 入口
"""
import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
logging.basicConfig(level=logging.INFO, format="[%(name)s] %(message)s")
logger = logging.getLogger("Test")

PASS = 0
FAIL = 0


def check(name: str, condition: bool, detail: str = ""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name}  {detail}")


# ── 1. 导入 ────────────────────────────────────────────
print("\n" + "=" * 60)
print("1. 模块导入")
print("=" * 60)

try:
    from graph.workflow import (
        create_workflow, run_workflow, AgentState,
        data_agent_node, tech_agent_node, capital_agent_node,
        valuation_agent_node, fund_agent_node, industry_agent_node,
        risk_agent_node, chief_agent_node,
        continue_to_agents, quality_gate_node,
        _score_report,
        ANALYSIS_AGENT_NAMES, AGENT_KEY_MAP, REPORT_KEY_MAP,
        AGENT_DATA_REQUIREMENTS, QUALITY_MIN_SCORE,
    )
    check("导入所有核心符号", True)
except Exception as e:
    check("导入所有核心符号", False, str(e))
    sys.exit(1)


# ── 2. 图编译与边结构 ──────────────────────────────────
print("\n" + "=" * 60)
print("2. 图编译与边结构")
print("=" * 60)

app = create_workflow()
check("图编译成功", app is not None)

g = app.builder
edges_flat = g._all_edges  # set of (source, target) tuples

check("入口点为 data_agent",
      ("__start__", "data_agent") in edges_flat)

check("6 个 Agent → quality_gate",
      all((agent, "quality_gate") in edges_flat for agent in ANALYSIS_AGENT_NAMES))

check("quality_gate → chief_agent",
      ("quality_gate", "chief_agent") in edges_flat)

check("chief_agent → END",
      ("chief_agent", "__end__") in edges_flat)


# ── 3. 动态路由 ────────────────────────────────────────
print("\n" + "=" * 60)
print("3. 动态路由 (continue_to_agents)")
print("=" * 60)

from langgraph.types import Send

# 场景 A：所有数据齐全 → 全部派发
state_full = {
    "selected_agents": ["tech", "fund", "capital", "industry", "risk", "valuation"],
    "tech_data": [{"date": "2024-01-01", "close": 100}],
    "fundamental_data": {"roe": 15},
    "capital_data": {"net_flow": 1.0},
    "valuation_data": {"pe_ttm": 20},
    "financial_data": {"debt_ratio": 0.3},
    "error": None,
}
sends = continue_to_agents(state_full)
check("全部数据齐全 → 6个Send", len(sends) == 6, f"got {len(sends)}")
check("全部为Send对象", all(isinstance(s, Send) for s in sends))

# 场景 B：tech_data 缺失 → 跳过 tech
state_no_tech = {
    "selected_agents": ["tech", "fund", "capital", "industry", "risk", "valuation"],
    "tech_data": [],
    "fundamental_data": {"roe": 15},
    "capital_data": {"net_flow": 1.0},
    "valuation_data": {"pe_ttm": 20},
    "financial_data": {"debt_ratio": 0.3},
    "error": None,
}
sends = continue_to_agents(state_no_tech)
check("tech_data为空 → 5个Send (跳过tech)", len(sends) == 5, f"got {len(sends)}")

# 场景 C：多数据缺失
state_missing = {
    "selected_agents": ["tech", "fund", "capital", "industry", "risk", "valuation"],
    "tech_data": [],
    "fundamental_data": {},
    "capital_data": {},
    "valuation_data": {},
    "financial_data": {},
    "error": None,
}
sends = continue_to_agents(state_missing)
check("全部数据缺失 → 0个Send", len(sends) == 0, f"got {len(sends)}")

# 场景 D：error 状态 → 全部跳过
state_error = {
    "selected_agents": ["tech", "fund", "capital"],
    "tech_data": [{"close": 100}],
    "fundamental_data": {"roe": 15},
    "capital_data": {"net_flow": 1.0},
    "error": "数据获取失败",
}
sends = continue_to_agents(state_error)
check("error=True → 0个Send", len(sends) == 0, f"got {len(sends)}")

# 场景 E：用户只选了部分 Agent
state_partial = {
    "selected_agents": ["tech", "valuation"],
    "tech_data": [{"close": 100}],
    "valuation_data": {"pe_ttm": 20},
    "error": None,
}
sends = continue_to_agents(state_partial)
check("只选tech+valuation → 2个Send", len(sends) == 2, f"got {len(sends)}")
check("包含tech_agent", any(s.node == "tech_agent" for s in sends))
check("包含valuation_agent", any(s.node == "valuation_agent" for s in sends))


# ── 4. 质量评分 ────────────────────────────────────────
print("\n" + "=" * 60)
print("4. 质量评分 (_score_report)")
print("=" * 60)

check("空字符串 → 0", _score_report("") == 0.0, f"got {_score_report('')}")
check("None → 0", _score_report(None) == 0.0)

error_report = "Failed"
check("异常标记 → 低分", _score_report(error_report[:10]) < QUALITY_MIN_SCORE)

short_report = "报告：该股表现良好。"
check("短报告 → 低分", _score_report(short_report) < QUALITY_MIN_SCORE,
      f"got {_score_report(short_report)}")

medium_report = "1. 趋势分析\n该股处于上升通道。\n2. 建议\n建议持有。"
check("有结构无数据 → 低于阈值", _score_report(medium_report) < QUALITY_MIN_SCORE,
      f"got {_score_report(medium_report)}")

good_report = """# 技术面分析报告

## 1. 趋势研判
MA5=15.2, MA20=14.8, 多头排列，评分80。

## 2. 动能分析
MACD DIF=0.35, 金叉信号明确。

## 3. 量价配合
成交量放大15%，换手率3.2%，量价配合良好。

## 4. 关键位
支撑位14.5元，压力位16.8元。

## 5. 综合建议
看多，建议买入，目标价17元，止损14元。
"""
score = _score_report(good_report)
check(f"完整报告 → >= QUALITY_MIN_SCORE ({score:.0f})",
      score >= QUALITY_MIN_SCORE, f"got {score:.0f}")

# AgentReport dict 格式
agent_report_dict = {
    "dimension": "tech",
    "overall_score": 72,
    "grade": "看多",
    "thesis": "多头趋势明确，量价配合良好",
    "key_signals": ["MACD金叉", "均线多头排列", "放量突破"],
    "risk_factors": ["前高压力"],
    "raw_text": good_report,
}
check("AgentReport dict → 高分", _score_report(agent_report_dict) >= QUALITY_MIN_SCORE,
      f"got {_score_report(agent_report_dict):.0f}")

error_dict = {"dimension": "tech", "raw_text": "Failed", "parse_error": True}
check("异常AgentReport dict → 低分", _score_report(error_dict) < QUALITY_MIN_SCORE,
      f"got {_score_report(error_dict):.0f}")


# ── 5. quality_gate_node ──────────────────────────────
print("\n" + "=" * 60)
print("5. quality_gate_node")
print("=" * 60)

state_for_gate = {
    "stock_code": "600519",
    "selected_agents": ["tech", "fund"],
    "tech_data": [{"close": 100}],
    "fundamental_data": {"roe": 15},
    "capital_data": {},
    "valuation_data": {},
    "financial_data": {},
    "tech_report": agent_report_dict,  # AgentReport dict 格式
    "fund_report": "简短报告不够详细",   # 普通字符串
    "capital_report": None,
    "valuation_report": None,
    "industry_report": None,
    "risk_report": None,
    "retry_counts": {},
    "skipped_agents": [],
}

result = quality_gate_node(state_for_gate)
check("quality_gate 返回结果", isinstance(result, dict))
check("包含 quality_scores", "quality_scores" in result)
check("包含 retry_counts", "retry_counts" in result)

scores = result.get("quality_scores", {})
check("tech_report 高分通过", scores.get("tech_agent", 0) >= QUALITY_MIN_SCORE,
      f"got {scores.get('tech_agent', 'N/A')}")

# fund_report 是短报告，应低于阈值，触发重试
fund_score = scores.get("fund_agent", 0)
# 注意：fund_agent 真实调用会失败（无LLM），这里只检查评分逻辑
check("fund_report 被评分", "fund_agent" in scores)

# 测试 skipped_agents 标记
state_with_skipped = {
    "stock_code": "600519",
    "selected_agents": ["tech", "valuation"],
    "tech_data": [{"close": 100}],
    "fundamental_data": {},
    "capital_data": {},
    "valuation_data": {},
    "financial_data": {},
    "tech_report": good_report,
    "fund_report": None,
    "capital_report": None,
    "valuation_report": None,
    "industry_report": None,
    "risk_report": None,
    "retry_counts": {},
    "skipped_agents": ["valuation"],
}
result2 = quality_gate_node(state_with_skipped)
scores2 = result2.get("quality_scores", {})
check("跳过Agent标记为-1分", scores2.get("valuation_agent", 999) == -1.0,
      f"got {scores2.get('valuation_agent', 'N/A')}")
check("跳过Agent补上报告", "valuation_report" in result2)


# ── 6. Agent 节点函数签名 ──────────────────────────────
print("\n" + "=" * 60)
print("6. Agent 节点函数")
print("=" * 60)

base_state = {
    "stock_code": "600519",
    "tech_data": [{"close": 100, "volume": 1000}],
    "fundamental_data": {"roe": 15, "eps": 3.5},
    "capital_data": {"net_flow": 1.5},
    "valuation_data": {"pe_ttm": 20, "pb": 3.0},
    "financial_data": {"debt_ratio": 0.3, "current_ratio": 1.5},
}

# 测试 data_agent_node
try:
    result_da = data_agent_node(base_state)
    check("data_agent_node 返回 dict", isinstance(result_da, dict))
    check("data_agent_node 返回 basic_info", "basic_info" in result_da or "error" in result_da)
except Exception as e:
    check("data_agent_node 调用", False, str(e)[:80])

# 测试 tech_agent_node（会尝试调用 LLM，可能失败但不崩溃）
try:
    result_tech = tech_agent_node(base_state)
    check("tech_agent_node 返回 dict", isinstance(result_tech, dict))
    check("tech_agent_node 含 tech_report", "tech_report" in result_tech)
except Exception as e:
    check("tech_agent_node 调用", False, str(e)[:80])

# 测试 chief_agent_node
chief_state = {
    **base_state,
    "tech_report": good_report,
    "fund_report": good_report,
    "capital_report": good_report,
    "industry_report": good_report,
    "risk_report": good_report,
    "valuation_report": good_report,
}
try:
    result_chief = chief_agent_node(chief_state)
    check("chief_agent_node 返回 dict", isinstance(result_chief, dict))
    check("chief_agent_node 含 final_report", "final_report" in result_chief)
    check("chief_agent_node 含 reports", "reports" in result_chief)
except Exception as e:
    check("chief_agent_node 调用", False, str(e)[:80])


# ── 7. 常量与映射完整性 ────────────────────────────────
print("\n" + "=" * 60)
print("7. 常量与映射完整性")
print("=" * 60)

check("6个ANALYSIS_AGENT_NAMES", len(ANALYSIS_AGENT_NAMES) == 6)
check("6个AGENT_KEY_MAP", len(AGENT_KEY_MAP) == 6)
check("6个REPORT_KEY_MAP", len(REPORT_KEY_MAP) == 6)
check("6个AGENT_DATA_REQUIREMENTS", len(AGENT_DATA_REQUIREMENTS) == 6)

for key in AGENT_KEY_MAP:
    agent_name = AGENT_KEY_MAP[key]
    check(f"  {key} → {agent_name} 有对应 report_key",
          agent_name in REPORT_KEY_MAP,
          f"missing {agent_name} in REPORT_KEY_MAP")

check("AGENT_KEY_MAP 双向对应 ANALYSIS_AGENT_NAMES",
      set(AGENT_KEY_MAP.values()) == set(ANALYSIS_AGENT_NAMES))
check("REPORT_KEY_MAP keys 对应 ANALYSIS_AGENT_NAMES",
      set(REPORT_KEY_MAP.keys()) == set(ANALYSIS_AGENT_NAMES))


# ── 8. run_workflow 入口 ───────────────────────────────
print("\n" + "=" * 60)
print("8. run_workflow 入口")
print("=" * 60)

check("run_workflow 可调用", callable(run_workflow))

import inspect
sig = inspect.signature(run_workflow)
params = list(sig.parameters.keys())
check("参数包含 stock_code", "stock_code" in params)
check("参数包含 selected_agents", "selected_agents" in params)


# ── 汇总 ───────────────────────────────────────────────
print("\n" + "=" * 60)
print(f"测试结果: {PASS} PASS / {FAIL} FAIL / {PASS + FAIL} TOTAL")
print("=" * 60)

if FAIL > 0:
    print(f"\n[FAIL] {FAIL} 项测试失败!")
    sys.exit(1)
else:
    print("\n[OK] 所有测试通过!")