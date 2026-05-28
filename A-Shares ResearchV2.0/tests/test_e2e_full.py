"""
端到端投研流程测试：验证知识库 + 多智能体辩论 + A股/美股分析
"""
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

print("=" * 60)
print("  端到端投研流程测试")
print("=" * 60)

# ── 1. 完整导入链 ──
print("\n[1] 完整导入链验证")
modules_ok = 0
try:
    from layers.agents.chief_agent import ChiefAgent
    print("  [OK] ChiefAgent")
    modules_ok += 1
except Exception as e:
    print(f"  [FAIL] ChiefAgent: {e}")

try:
    from layers.agents.debate import (
        cross_review, revise_report, run_debate_rounds,
        build_debate_summary, CROSS_PAIRINGS, RevisionResult, ReviewOpinion, DIM_LABELS,
    )
    print("  [OK] debate 模块 (cross_review/revise_report/run_debate_rounds/build_debate_summary)")
    modules_ok += 1
except Exception as e:
    print(f"  [FAIL] debate: {e}")

try:
    from layers.memory.knowledge_base import (
        init_kb, save_snapshot, build_tracking_context,
        get_reports_for_stock, get_report_consensus,
    )
    print("  [OK] knowledge_base 模块")
    modules_ok += 1
except Exception as e:
    print(f"  [FAIL] knowledge_base: {e}")

try:
    from graph.workflow import run_workflow
    print("  [OK] graph.workflow (run_workflow)")
    modules_ok += 1
except Exception as e:
    print(f"  [FAIL] graph.workflow: {e}")

print(f"  导入: {modules_ok}/4 OK")

# ── 2. 知识库功能验证 ──
print("\n[2] 知识库功能验证")
kb_ok = 0
try:
    init_kb()
    print("  [OK] init_kb")
    kb_ok += 1
except Exception as e:
    print(f"  [FAIL] init_kb: {e}")

try:
    ctx = build_tracking_context("BABA")
    if "历史分析回溯" in ctx or "首次分析" in ctx:
        print(f"  [OK] build_tracking_context (len={len(ctx)})")
        kb_ok += 1
    else:
        print(f"  [FAIL] build_tracking_context 返回异常: {ctx[:100]}")
except Exception as e:
    print(f"  [FAIL] build_tracking_context: {e}")

try:
    reports = get_reports_for_stock("BABA")
    if isinstance(reports, list):
        print(f"  [OK] get_reports_for_stock (BABA: {len(reports)} 篇)")
        kb_ok += 1
    else:
        print(f"  [FAIL] get_reports_for_stock 返回非列表: {type(reports)}")
except Exception as e:
    print(f"  [FAIL] get_reports_for_stock: {e}")

try:
    consensus = get_report_consensus("BABA")
    print(f"  [OK] get_report_consensus: {consensus[:80]}...")
    kb_ok += 1
except Exception as e:
    print(f"  [FAIL] get_report_consensus: {e}")

try:
    from layers.memory.report_fetcher import fetch_reports_for_stock
    print(f"  [OK] report_fetcher 导入成功")
    kb_ok += 1
except Exception as e:
    print(f"  [FAIL] fetch_reports_for_stock: {e}")

print(f"  知识库: {kb_ok}/5 OK")

# ── 3. 辩论引擎结构验证 ──
print("\n[3] 辩论引擎结构验证")
debate_ok = 0

from layers.agents.report_schema import AgentReport

mock = AgentReport(
    dimension="tech", overall_score=35, grade="中性偏空",
    confidence=70, thesis="MACD死叉，技术面偏空",
    key_signals=["MACD死叉"], risk_factors=["趋势破位"],
    recommendation="短期规避",
)

try:
    opinion = cross_review("tech", mock)
    assert opinion.reviewer_dim == "tech"
    assert opinion.target_dim == "tech"
    assert isinstance(opinion.is_contradiction, bool)
    assert len(opinion.contradiction_detail) > 0
    print(f"  [OK] cross_review (contradiction={opinion.is_contradiction})")
    debate_ok += 1
except Exception as e:
    print(f"  [FAIL] cross_review: {e}")

try:
    from layers.agents.debate import revise_report
    mock_opinion = ReviewOpinion(
        reviewer_dim="fund", target_dim="tech",
        is_contradiction=True,
        contradiction_detail="技术面破位与估值偏低存在时序矛盾",
        suggested_revision="建议技术面Agent考虑估值偏低提供的安全边际，评分上修3-5分",
        agree_points="K线形态分析客观",
    )
    result = revise_report("tech", mock, mock_opinion)
    assert isinstance(result, RevisionResult)
    assert result.dimension == "tech"
    assert result.original_score == 35
    assert isinstance(result.changed, bool)
    assert isinstance(result.revised_score, int)
    print(f"  [OK] revise_report: {result.original_score}->{result.revised_score} changed={result.changed}")
    debate_ok += 1
except Exception as e:
    print(f"  [FAIL] revise_report: {e}")

try:
    mock_reports = {
        "tech": AgentReport("tech", 35, "中性偏空", 70, "技术面偏空", ["s1"], ["r1"], "规避"),
        "fund": AgentReport("fund", 72, "看多", 75, "基本面优秀", ["s2"], ["r2"], "配置"),
        "capital": AgentReport("capital", 60, "中性偏多", 60, "资金面偏暖", ["s3"], ["r3"], "观望"),
        "industry": AgentReport("industry", 68, "看多", 65, "行业景气", ["s4"], ["r4"], "配置"),
        "risk": AgentReport("risk", 55, "中性", 55, "风险可控", ["s5"], ["r5"], "控制"),
        "valuation": AgentReport("valuation", 65, "中性偏多", 60, "估值合理", ["s6"], ["r6"], "分批"),
    }
    final_reports, results = run_debate_rounds(mock_reports)
    assert isinstance(final_reports, dict)
    assert isinstance(results, dict)
    assert len(final_reports) == 6
    assert len(results) == 6
    changed = sum(1 for r in results.values() if r.changed)
    print(f"  [OK] run_debate_rounds: {changed}/6 个Agent修改评分")
    debate_ok += 1
except Exception as e:
    print(f"  [FAIL] run_debate_rounds: {e}")

try:
    summary = build_debate_summary(mock_reports, results)
    assert "分歧" in summary or "修订" in summary
    print(f"  [OK] build_debate_summary (len={len(summary)})")
    debate_ok += 1
except Exception as e:
    print(f"  [FAIL] build_debate_summary: {e}")

try:
    assert len(CROSS_PAIRINGS) == 6
    labels = [f"{DIM_LABELS[r]}->{DIM_LABELS[t]}" for r, t in CROSS_PAIRINGS]
    print(f"  [OK] 审阅配对: {labels}")
    debate_ok += 1
except Exception as e:
    print(f"  [FAIL] CROSS_PAIRINGS: {e}")

print(f"  辩论引擎: {debate_ok}/5 OK")

# ── 4. ChiefAgent 集成验证 ──
print("\n[4] ChiefAgent 集成验证")
chief_ok = 0

try:
    chief = ChiefAgent("deepseek")
    assert chief.model_name == "deepseek"
    print("  [OK] ChiefAgent 实例化")
    chief_ok += 1
except Exception as e:
    print(f"  [FAIL] ChiefAgent 实例化: {e}")

try:
    from layers.agents.report_schema import aggregate_reports, reports_to_markdown
    reports = {
        "tech": AgentReport("tech", 72, "看多", 75, "走势良好", ["s1"], ["r1"], "买入"),
        "fund": AgentReport("fund", 65, "中性偏多", 70, "基本面尚可", ["s2"], ["r2"], "持有"),
    }
    md = reports_to_markdown(reports)
    assert len(md) > 0
    agg = aggregate_reports(reports)
    assert "overall_score" in agg
    print(f"  [OK] report_schema 方法可用 (markdown={len(md)}chars, agg_score={agg['overall_score']})")
    chief_ok += 1
except Exception as e:
    print(f"  [FAIL] report_schema: {e}")

try:
    from graph.workflow import ANALYSIS_AGENT_NAMES
    assert len(ANALYSIS_AGENT_NAMES) == 6
    print(f"  [OK] 6个分析Agent已注册: {ANALYSIS_AGENT_NAMES}")
    chief_ok += 1
except Exception as e:
    print(f"  [FAIL] Agent注册: {e}")

try:
    from layers.agents.report_schema import error_report as _err_report, unavailable_report as _unavail_report
    er = _err_report("tech", "测试异常")
    ur = _unavail_report("fund")
    assert er.overall_score == 50
    assert er.grade == "中性"
    assert ur.overall_score == 50
    assert ur.grade == "中性"
    print(f"  [OK] error_report/unavailable_report 兜底报告正确")
    chief_ok += 1
except Exception as ex:
    print(f"  [FAIL] 兜底报告: {ex}")

print(f"  ChiefAgent集成: {chief_ok}/4 OK")

# ── 5. A股真实分析 (600519 贵州茅台) ──
print("\n[5] A股真实分析: 600519")
try:
    result = run_workflow(stock_code="600519", selected_agents=["tech", "fund", "capital"])
    assert isinstance(result, dict), f"返回类型异常: {type(result)}"
    assert "final_report" in result, f"缺少 final_report, keys={list(result.keys())[:10]}"
    assert "reports" in result
    print(f"  [OK] A股分析完成")
    print(f"    报告长度: {len(result.get('final_report', ''))} 字符")
    print(f"    Agent报告数: {len(result.get('reports', {}))}")
    quality = result.get('quality_scores', {})
    if quality:
        avg = sum(v for v in quality.values() if v > 0) / max(1, sum(1 for v in quality.values() if v > 0))
        print(f"    质量均分: {avg:.0f}")
    has_debate = "分歧" in result.get('final_report', '') or "修订" in result.get('final_report', '')
    print(f"    辩论摘要: {'已包含' if has_debate else '未触发(正常)'}")
except Exception as e:
    import traceback
    print(f"  [FAIL] A股分析: {e}")
    traceback.print_exc()

# ── 6. 美股真实分析 (BABA) ──
print("\n[6] 美股真实分析: BABA")
try:
    result = run_workflow(stock_code="BABA", selected_agents=["tech", "fund", "capital"])
    assert isinstance(result, dict), f"返回类型异常: {type(result)}"
    assert "final_report" in result, f"缺少 final_report, keys={list(result.keys())[:10]}"
    assert "reports" in result
    print(f"  [OK] 美股分析完成")
    print(f"    报告长度: {len(result.get('final_report', ''))} 字符")
    print(f"    Agent报告数: {len(result.get('reports', {}))}")
    quality = result.get('quality_scores', {})
    if quality:
        avg = sum(v for v in quality.values() if v > 0) / max(1, sum(1 for v in quality.values() if v > 0))
        print(f"    质量均分: {avg:.0f}")
    has_debate = "分歧" in result.get('final_report', '') or "修订" in result.get('final_report', '')
    print(f"    辩论摘要: {'已包含' if has_debate else '未触发(正常)'}")
except Exception as e:
    import traceback
    print(f"  [FAIL] 美股分析: {e}")
    traceback.print_exc()

print("\n" + "=" * 60)
print("  端到端测试完成")
print("=" * 60)