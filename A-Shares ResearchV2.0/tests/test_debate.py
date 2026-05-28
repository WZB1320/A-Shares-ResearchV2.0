"""测试多智能体辩论引擎"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from layers.agents.debate import (
    cross_review, revise_report, run_debate_rounds, build_debate_summary,
    CROSS_PAIRINGS, ReviewOpinion, RevisionResult, DIM_LABELS,
)
from layers.agents.report_schema import AgentReport

print("=== 辩论引擎导入 OK ===")
print(f"审阅配对: {len(CROSS_PAIRINGS)} 对")
for r, t in CROSS_PAIRINGS:
    print(f"  {DIM_LABELS[r]} → {DIM_LABELS[t]}")
print()

print("=== 模拟 Phase 1 初版报告 ===")
mock_reports = {
    "tech": AgentReport(
        dimension="tech", overall_score=35, grade="中性偏空",
        confidence=70, thesis="MACD死叉，K线跌破60日均线，技术面偏空",
        key_signals=["MACD死叉", "跌破60日线", "量价背离"],
        risk_factors=["趋势破位风险", "支撑位失守"],
        recommendation="短期规避，等待企稳信号",
    ),
    "fund": AgentReport(
        dimension="fund", overall_score=72, grade="看多",
        confidence=75, thesis="ROE 22%行业领先，营收增速18%超预期",
        key_signals=["ROE 22%", "营收增速18%", "净利率提升"],
        risk_factors=["应收账款增长过快", "毛利率小幅下滑"],
        recommendation="中长期配置，基本面坚实",
    ),
    "capital": AgentReport(
        dimension="capital", overall_score=60, grade="中性偏多",
        confidence=60, thesis="北向资金小幅流入，主力资金中性",
        key_signals=["北向净流入", "融资余额平稳"],
        risk_factors=["主力资金偏谨慎", "散户参与度高"],
        recommendation="观望，关注主力动向",
    ),
    "industry": AgentReport(
        dimension="industry", overall_score=68, grade="看多",
        confidence=65, thesis="行业景气度回升，政策利好密集",
        key_signals=["行业PMI回升", "政策红利", "竞争格局优化"],
        risk_factors=["监管政策不确定性", "新进入者威胁"],
        recommendation="行业龙头配置价值凸显",
    ),
    "risk": AgentReport(
        dimension="risk", overall_score=55, grade="中性",
        confidence=55, thesis="负债率45%可控，但现金流趋紧需关注",
        key_signals=["负债率45%", "现金流/负债比0.8"],
        risk_factors=["现金流趋紧", "商誉减值风险", "质押比例偏高"],
        recommendation="控制仓位，设止损线",
    ),
    "valuation": AgentReport(
        dimension="valuation", overall_score=65, grade="中性偏多",
        confidence=60, thesis="PE处于历史中位，PEG<1显示成长性低估",
        key_signals=["PE 18x (历史30%分位)", "PEG 0.7", "PB 2.5x"],
        risk_factors=["行业估值中枢可能下移"],
        recommendation="估值合理偏低，分批建仓",
    ),
}

for dim, r in mock_reports.items():
    print(f"  [{r.grade}] {DIM_LABELS[dim]}: {r.overall_score}/100 — {r.thesis[:50]}...")
print()

print("=== 测试 cross_review (单次) ===")
try:
    opinion = cross_review("tech", mock_reports["fund"])
    print(f"  tech→fund: contradiction={opinion.is_contradiction}")
    print(f"  detail: {opinion.contradiction_detail[:120]}")
except Exception as e:
    print(f"  cross_review 异常: {e}")
print()

print("=== 测试 revise_report (单次) ===")
try:
    mock_opinion = ReviewOpinion(
        reviewer_dim="tech", target_dim="fund",
        is_contradiction=True,
        contradiction_detail="技术面破位与基本面看多存在时序矛盾",
        suggested_revision="建议基本面Agent考虑技术面破位对估值的影响，评分下调5-10分",
        agree_points="ROE和营收增速的分析客观合理",
    )
    result = revise_report("fund", mock_reports["fund"], mock_opinion)
    print(f"  fund: {result.original_score}→{result.revised_score} | changed={result.changed}")
except Exception as e:
    print(f"  revise_report 异常: {e}")
print()

print("=== 测试完整辩论流程 ===")
try:
    final_reports, results = run_debate_rounds(mock_reports)
    summary = build_debate_summary(mock_reports, results)
    print(summary[:500])
except Exception as e:
    print(f"  完整流程异常: {e}")

print("\n=== 完成 ===")