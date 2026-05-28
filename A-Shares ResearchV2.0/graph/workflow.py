"""
LangGraph 工作流 - Harness 架构并行调度层

严格按照 Harness 架构：
  1. data_agent 统一取数
  2. 动态路由：数据驱动跳过无效 Agent
  3. Send/recv 并行执行 6 个分析 Agent
  4. quality_gate 质量门禁：自动评分 + 低分重试
  5. chief_agent 汇总生成最终研报

兼容 harness/ 调度层（state / validator / scheduler）
可直接被 main.py 调用
"""
import logging
import re
import time
from typing import Dict, TypedDict, Optional, List, Any, Callable

from langgraph.graph import StateGraph, END
from langgraph.types import Send

from layers.agents.data_agent import DataAgent
from layers.agents.tech_agent import TechAgent
from layers.agents.fund_agent import FundAgent
from layers.agents.capital_agent import CapitalAgent
from layers.agents.industry_agent import IndustryAgent
from layers.agents.risk_agent import RiskAgent
from layers.agents.valuation_agent import ValuationAgent
from layers.agents.chief_agent import ChiefAgent
from harness.validator import harness_validator
from layers.agents.report_schema import AgentReport
from layers.agents.debate import run_debate_rounds, build_debate_summary

logger = logging.getLogger("Graph-Workflow")

# ── 常量定义 ──────────────────────────────────────────────

ANALYSIS_AGENT_NAMES = [
    "tech_agent",
    "capital_agent",
    "valuation_agent",
    "fund_agent",
    "industry_agent",
    "risk_agent",
]

AGENT_KEY_MAP: Dict[str, str] = {
    "tech": "tech_agent",
    "fund": "fund_agent",
    "capital": "capital_agent",
    "industry": "industry_agent",
    "risk": "risk_agent",
    "valuation": "valuation_agent",
}

REPORT_KEY_MAP: Dict[str, str] = {
    "tech_agent": "tech_report",
    "fund_agent": "fund_report",
    "capital_agent": "capital_report",
    "industry_agent": "industry_report",
    "risk_agent": "risk_report",
    "valuation_agent": "valuation_report",
}

# 每个 Agent 依赖的数据字段及可用性检查
AGENT_DATA_REQUIREMENTS: Dict[str, tuple] = {
    "tech": ("tech_data", lambda d: isinstance(d, list) and len(d) > 0),
    "fund": ("fundamental_data", lambda d: isinstance(d, dict) and len(d) > 0),
    "capital": ("capital_data", lambda d: isinstance(d, dict) and len(d) > 0),
    "industry": ("fundamental_data", lambda d: isinstance(d, dict) and len(d) > 0),
    "risk": ("financial_data", lambda d: isinstance(d, dict) and len(d) > 0),
    "valuation": ("valuation_data", lambda d: isinstance(d, dict) and len(d) > 0),
}

# 质量门禁阈值
QUALITY_MIN_SCORE = 50       # 低于此分触发重试
QUALITY_MIN_LENGTH = 200     # 最低字符数
MAX_RETRY_PER_AGENT = 1      # 每个 Agent 最多重试 1 次

# ── State 定义 ────────────────────────────────────────────

class AgentState(TypedDict):
    stock_code: str
    selected_agents: List[str]
    basic_info: Optional[Dict]
    capital_data: Optional[Dict]
    fundamental_data: Optional[Dict]
    tech_data: Optional[List]
    valuation_data: Optional[Dict]
    financial_data: Optional[Dict]
    tech_report: Optional[str]
    capital_report: Optional[str]
    valuation_report: Optional[str]
    fund_report: Optional[str]
    industry_report: Optional[str]
    risk_report: Optional[str]
    final_report: Optional[str]
    reports: Optional[Dict[str, str]]
    validation_errors: Optional[List[str]]
    error: Optional[str]
    skipped_agents: Optional[List[str]]
    quality_scores: Optional[Dict[str, float]]
    retry_counts: Optional[Dict[str, int]]


# ── 节点函数 ──────────────────────────────────────────────

def data_agent_node(state: AgentState) -> Dict[str, Any]:
    stock_code = state["stock_code"]
    logger.info(f"[Workflow] data_agent 开始统一取数: {stock_code}")

    agent = DataAgent()
    agent.stock_code = stock_code
    try:
        all_data = agent.fetch_all()
    except Exception as e:
        error_msg = f"数据获取失败: {str(e)}"
        logger.error(f"[Workflow] {error_msg}")
        return {"error": error_msg}

    basic_info = all_data.get("basic_info", {}) or {}
    capital_data = all_data.get("capital_data", {}) or {}
    fundamental_data = all_data.get("fundamental_data", {}) or {}
    tech_data = all_data.get("tech_data", []) or []
    valuation_data = all_data.get("valuation_data", {}) or {}
    financial_data = all_data.get("financial_data", {}) or {}

    validation_errors = []
    if basic_info:
        vr = harness_validator.validate_stock_code(stock_code)
        if not vr.is_valid():
            validation_errors.extend(vr.errors)

    completeness = harness_validator.validate_data_completeness(
        all_data,
        ["basic_info", "tech_data", "fundamental_data"]
    )
    if not completeness.is_valid():
        validation_errors.extend(completeness.errors)

    data_types = [k for k in all_data if all_data[k]]
    logger.info(
        f"[Workflow] data_agent 取数完成: {stock_code} | "
        f"已获取: {data_types} | 校验问题: {len(validation_errors)}"
    )

    return {
        "basic_info": basic_info,
        "capital_data": capital_data,
        "fundamental_data": fundamental_data,
        "tech_data": tech_data,
        "valuation_data": valuation_data,
        "financial_data": financial_data,
        "validation_errors": validation_errors if validation_errors else None,
    }


def continue_to_agents(state: AgentState) -> List[Send]:
    selected = state.get("selected_agents", [
        "tech", "fund", "capital", "industry", "risk", "valuation"
    ])

    error_val = state.get("error")
    if error_val:
        logger.warning(f"[Workflow] 数据获取失败(error={error_val})，跳过分析阶段")
        state["skipped_agents"] = selected[:]  # 全部标记为跳过
        return []

    skipped = []
    sends = []
    for key in selected:
        agent_name = AGENT_KEY_MAP.get(key)
        if not agent_name:
            continue

        required_field, check_fn = AGENT_DATA_REQUIREMENTS.get(key, (None, None))
        if required_field and check_fn:
            data = state.get(required_field)
            if not check_fn(data):
                logger.warning(
                    f"[Workflow] ⏭️ 跳过 {agent_name}: "
                    f"{required_field} 数据不足 (type={type(data).__name__}, "
                    f"len={len(data) if data else 0})"
                )
                skipped.append(key)
                continue

        sends.append(Send(agent_name, state))
        logger.debug(f"[Workflow] 派发 Send → {agent_name}")

    logger.info(
        f"[Workflow] 并行派发 {len(sends)}/{len(selected)} 个 Agent"
        + (f" | 跳过: {skipped}" if skipped else "")
    )
    state["skipped_agents"] = skipped
    return sends


# ── 6 个分析 Agent 节点 ───────────────────────────────────

def _agent_report_wrapper(
    state: AgentState,
    agent_cls,
    agent_name: str,
    data_payload: Dict,
    report_key: str,
) -> Dict[str, Any]:
    stock_code = state["stock_code"]
    logger.info(f"[Workflow] {agent_name} 开始分析: {stock_code}")
    agent = agent_cls()
    try:
        report = agent.analyze(stock_code, data_payload)
    except Exception as e:
        report = {"dimension": agent_name, "raw_text": f"⚠️ {agent_name} 分析异常: {str(e)}", "parse_error": True}
        logger.error(f"[Workflow] {agent_name} 异常: {e}")

    # 提取文本长度用于日志：可能是 dict(AgentReport) 或 str
    if isinstance(report, dict):
        text = report.get("raw_text", "") or str(report)
    else:
        text = str(report)
    logger.info(f"[Workflow] {agent_name} 完成: {stock_code} | 长度: {len(text)}")
    return {report_key: report}


def tech_agent_node(state: AgentState) -> Dict[str, Any]:
    return _agent_report_wrapper(
        state, TechAgent, "tech_agent",
        {"tech_data": state.get("tech_data", [])},
        "tech_report"
    )


def capital_agent_node(state: AgentState) -> Dict[str, Any]:
    return _agent_report_wrapper(
        state, CapitalAgent, "capital_agent",
        {"capital_data": state.get("capital_data", {})},
        "capital_report"
    )


def valuation_agent_node(state: AgentState) -> Dict[str, Any]:
    return _agent_report_wrapper(
        state, ValuationAgent, "valuation_agent",
        {
            "valuation_data": state.get("valuation_data", {}),
            "fundamental_data": state.get("fundamental_data", {}),
        },
        "valuation_report"
    )


def fund_agent_node(state: AgentState) -> Dict[str, Any]:
    return _agent_report_wrapper(
        state, FundAgent, "fund_agent",
        {"fundamental_data": state.get("fundamental_data", {})},
        "fund_report"
    )


def industry_agent_node(state: AgentState) -> Dict[str, Any]:
    return _agent_report_wrapper(
        state, IndustryAgent, "industry_agent",
        {"fundamental_data": state.get("fundamental_data", {})},
        "industry_report"
    )


def risk_agent_node(state: AgentState) -> Dict[str, Any]:
    return _agent_report_wrapper(
        state, RiskAgent, "risk_agent",
        {
            "financial_data": state.get("financial_data", {}),
            "tech_data": state.get("tech_data", []),
        },
        "risk_report"
    )


# ── 质量门禁节点 ─────────────────────────────────────────

def _score_report(report) -> float:
    """评分报告质量，支持 str / dict(AgentReport) / None"""
    if report is None:
        return 0.0
    if isinstance(report, dict):
        report = report.get("raw_text", "") or str(report)
    if not isinstance(report, str):
        report = str(report)
    if not report or report.startswith("⚠️"):
        return 0.0

    score = 0.0

    # 1) 长度分 (0-30)
    length = len(report)
    if length >= 800:
        score += 30
    elif length >= 500:
        score += 20
    elif length >= QUALITY_MIN_LENGTH:
        score += 10
    else:
        score += 5

    # 2) 结构分 (0-25)：检测标题/数字编号
    if re.search(r'#{1,3}\s|【.+】|\d+[\.\、]', report):
        score += 25
    elif '\n' in report:
        score += 10

    # 3) 数据引用分 (0-25)：检测数字+单位
    data_count = len(re.findall(r'\d+\.?\d*\s*[%亿万手倍]', report))
    if data_count >= 5:
        score += 25
    elif data_count >= 2:
        score += 15
    elif data_count >= 1:
        score += 8

    # 4) 结论分 (0-20)：检测判断性关键词
    judgment_keywords = ['看多', '看空', '震荡', '买入', '卖出', '持有', '增持', '减持',
                         '建议', '评级', '评分', '综合', '推荐', '回避', '低配', '高配',
                         'bullish', 'bearish', 'buy', 'sell', 'hold']
    match_count = sum(
        1 for kw in judgment_keywords
        if kw.lower() in report.lower()
    )
    if match_count >= 4:
        score += 20
    elif match_count >= 2:
        score += 12
    elif match_count >= 1:
        score += 5

    return score


def quality_gate_node(state: AgentState) -> Dict[str, Any]:
    logger.info("[Workflow] quality_gate 开始质量评估")

    quality_scores: Dict[str, float] = {}
    retry_counts: Dict[str, int] = dict(state.get("retry_counts") or {})
    updates: Dict[str, Any] = {}

    agent_configs = [
        ("tech_agent", "tech_report", TechAgent,
         {"tech_data": state.get("tech_data", [])}),
        ("capital_agent", "capital_report", CapitalAgent,
         {"capital_data": state.get("capital_data", {})}),
        ("valuation_agent", "valuation_report", ValuationAgent,
         {"valuation_data": state.get("valuation_data", {}),
          "fundamental_data": state.get("fundamental_data", {})}),
        ("fund_agent", "fund_report", FundAgent,
         {"fundamental_data": state.get("fundamental_data", {})}),
        ("industry_agent", "industry_report", IndustryAgent,
         {"fundamental_data": state.get("fundamental_data", {})}),
        ("risk_agent", "risk_report", RiskAgent,
         {"financial_data": state.get("financial_data", {}),
          "tech_data": state.get("tech_data", [])}),
    ]

    for agent_name_key, report_key, agent_cls, data_payload in agent_configs:
        report = state.get(report_key, "")
        if report is None:
            report = ""
        if not isinstance(report, str):
            try:
                if isinstance(report, dict) and "raw_text" in report:
                    report = report.get("raw_text", "") or ""
                else:
                    report = str(report)
            except Exception:
                report = ""
        score = _score_report(report)
        quality_scores[agent_name_key] = score

        if score >= QUALITY_MIN_SCORE:
            logger.info(
                f"[Workflow] ✅ {agent_name_key}: {score:.0f}分 → 通过"
            )
            continue

        retried = retry_counts.get(agent_name_key, 0)
        if retried >= MAX_RETRY_PER_AGENT:
            logger.warning(
                f"[Workflow] ⚠️ {agent_name_key}: {score:.0f}分 → "
                f"已达最大重试次数({MAX_RETRY_PER_AGENT})，跳过重试"
            )
            continue

        logger.warning(
            f"[Workflow] 🔄 {agent_name_key}: {score:.0f}分 → "
            f"触发重试 ({retried + 1}/{MAX_RETRY_PER_AGENT})"
        )

        stock_code = state["stock_code"]
        agent = agent_cls()
        try:
            new_report = agent.analyze(stock_code, data_payload)
            new_score = _score_report(new_report)
            quality_scores[agent_name_key] = new_score

            if new_score > score:
                updates[report_key] = new_report
                logger.info(
                    f"[Workflow] ✅ {agent_name_key}: 重试后 {new_score:.0f}分 "
                    f"(+{new_score - score:.0f})"
                )
            else:
                logger.info(
                    f"[Workflow] ⚠️ {agent_name_key}: 重试后 {new_score:.0f}分, "
                    f"未改善，保留原报告"
                )
        except Exception as e:
            logger.error(f"[Workflow] {agent_name_key} 重试异常: {e}")

        retry_counts[agent_name_key] = retried + 1

    skipped = state.get("skipped_agents") or []
    for key in skipped:
        agent_name_key = AGENT_KEY_MAP.get(key, key)
        quality_scores[agent_name_key] = -1.0
        short_key = [k for k, v in AGENT_KEY_MAP.items() if v == agent_name_key]
        report_key = REPORT_KEY_MAP.get(agent_name_key, "")
        if report_key:
            display_key = short_key[0] if short_key else key
            updates[report_key] = f"⚠️ {display_key} 数据不足，已自动跳过分析"

    avg_score = (
        sum(s for s in quality_scores.values() if s >= 0)
        / max(1, sum(1 for s in quality_scores.values() if s >= 0))
    )
    logger.info(
        f"[Workflow] quality_gate 完成 | 均分: {avg_score:.0f} | "
        f"重试: {sum(retry_counts.values())}次"
    )

    updates["quality_scores"] = quality_scores
    updates["retry_counts"] = retry_counts
    return updates


# ── Chief Agent 汇总节点 ─────────────────────────────────

def chief_agent_node(state: AgentState) -> Dict[str, Any]:
    stock_code = state["stock_code"]
    logger.info(f"[Workflow] chief_agent 开始汇总: {stock_code}")

    def _as_str(val):
        """安全转为字符串"""
        if val is None:
            return ""
        if isinstance(val, str):
            return val
        if isinstance(val, dict):
            return val.get("raw_text", "") or str(val)
        return str(val)

    tech_report = _as_str(state.get("tech_report"))
    fund_report = _as_str(state.get("fund_report"))
    capital_report = _as_str(state.get("capital_report"))
    industry_report = _as_str(state.get("industry_report"))
    risk_report = _as_str(state.get("risk_report"))
    valuation_report = _as_str(state.get("valuation_report"))

    # 构建传递给 ChiefAgent 的报告 dict
    reports_to_synthesize: Dict[str, AgentReport] = {}
    reports: Dict[str, str] = {}
    for agent_name, report_key in REPORT_KEY_MAP.items():
        raw = state.get(report_key)
        report_str = _as_str(raw)
        short_key = [k for k, v in AGENT_KEY_MAP.items() if v == agent_name]
        key = short_key[0] if short_key else agent_name
        reports[key] = report_str

        if report_str and not report_str.startswith("⚠️"):
            # 尝试从 AgentReport dict 重建对象，否则用 raw_text 构造
            try:
                if isinstance(raw, dict):
                    reports_to_synthesize[key] = AgentReport(**raw)
                else:
                    reports_to_synthesize[key] = AgentReport(
                        dimension=key,
                        overall_score=50,
                        grade="中性",
                        confidence=50,
                        thesis="",
                        key_signals=[],
                        risk_factors=[],
                        recommendation="",
                        raw_text=report_str,
                    )
            except Exception:
                reports_to_synthesize[key] = AgentReport(
                    dimension=key,
                    overall_score=50,
                    grade="中性",
                    confidence=50,
                    thesis="",
                    key_signals=[],
                    risk_factors=[],
                    recommendation="",
                    raw_text=report_str,
                )

    # Harness Validator 校验
    for report_key, report_val in [
        ("tech", tech_report), ("fund", fund_report),
        ("capital", capital_report), ("industry", industry_report),
        ("risk", risk_report), ("valuation", valuation_report),
    ]:
        if report_val and not report_val.startswith("⚠️"):
            vr = harness_validator.validate_report_content(report_val)
            if not vr.is_valid():
                logger.warning(
                    f"[Workflow] 报告校验未通过 [{report_key}]: {vr.get_summary()}"
                )

    agent = ChiefAgent()

    debate_summary = ""
    if len(reports_to_synthesize) >= 3:
        try:
            debate_start = time.time()
            debate_reports, debate_results = run_debate_rounds(reports_to_synthesize)
            debate_summary = build_debate_summary(reports_to_synthesize, debate_results)
            reports_to_synthesize = debate_reports
            changed = sum(1 for r in debate_results.values() if r.changed)
            logger.info(
                f"[Workflow] 多Agent辩论完成 | {changed}个修订 | 耗时: {time.time() - debate_start:.1f}s"
            )
        except Exception as de:
            logger.warning(f"[Workflow] 辩论流程异常，跳过: {de}")

    try:
        final_report = agent.synthesize_reports(stock_code, reports_to_synthesize)

        if debate_summary:
            final_report += "\n\n---\n\n" + debate_summary
    except Exception as e:
        logger.error(f"[Workflow] chief_agent 汇总异常: {e}")
        fallback_parts = []
        for key, report in reports.items():
            fallback_parts.append(f"## {key} 分析\n{report}")
        final_report = (
            f"# {stock_code} 投研策略报告\n\n"
            f"⚠️ LLM 汇总生成失败: {str(e)}\n\n"
            + "\n\n---\n\n".join(fallback_parts)
        )

    logger.info(
        f"[Workflow] chief_agent 汇总完成: {stock_code} | "
        f"长度: {len(final_report)}"
    )
    return {
        "final_report": final_report,
        "reports": reports,
    }


# ── 图构建 ──────────────────────────────────────────────

def create_workflow():
    workflow = StateGraph(AgentState)

    workflow.add_node("data_agent", data_agent_node)
    workflow.add_node("tech_agent", tech_agent_node)
    workflow.add_node("capital_agent", capital_agent_node)
    workflow.add_node("valuation_agent", valuation_agent_node)
    workflow.add_node("fund_agent", fund_agent_node)
    workflow.add_node("industry_agent", industry_agent_node)
    workflow.add_node("risk_agent", risk_agent_node)
    workflow.add_node("quality_gate", quality_gate_node)
    workflow.add_node("chief_agent", chief_agent_node)

    workflow.set_entry_point("data_agent")

    # data_agent → Send 并行派发 6 个 Agent
    workflow.add_conditional_edges("data_agent", continue_to_agents)

    # 6 个 Agent → quality_gate
    for agent_name in ANALYSIS_AGENT_NAMES:
        workflow.add_edge(agent_name, "quality_gate")

    # quality_gate → chief_agent → END
    workflow.add_edge("quality_gate", "chief_agent")
    workflow.add_edge("chief_agent", END)

    return workflow.compile()


# ── 入口函数 ─────────────────────────────────────────────

def run_workflow(stock_code: str, selected_agents: List[str] = None) -> Dict:
    if selected_agents is None:
        selected_agents = ["tech", "fund", "capital", "industry", "risk", "valuation"]

    app = create_workflow()

    initial_state: AgentState = {
        "stock_code": stock_code,
        "selected_agents": selected_agents,
        "basic_info": None,
        "capital_data": None,
        "fundamental_data": None,
        "tech_data": None,
        "valuation_data": None,
        "financial_data": None,
        "tech_report": None,
        "capital_report": None,
        "valuation_report": None,
        "fund_report": None,
        "industry_report": None,
        "risk_report": None,
        "final_report": None,
        "reports": None,
        "validation_errors": None,
        "error": None,
        "skipped_agents": None,
        "quality_scores": None,
        "retry_counts": None,
    }

    logger.info(f"[Workflow] 启动工作流: {stock_code} | 选中: {selected_agents}")
    result = app.invoke(initial_state)
    logger.info(f"[Workflow] 工作流完成: {stock_code}")
    return result


# ── CLI 独立运行 ────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="[%(name)s] %(asctime)s - %(message)s",
    )
    result = run_workflow("600519")
    print("\n" + "=" * 80)
    print("LangGraph Workflow Result")
    print("=" * 80)
    print("\n[Quality Scores]")
    scores = result.get("quality_scores", {})
    for k, v in scores.items():
        flag = "✅" if v >= QUALITY_MIN_SCORE else ("⏭️" if v < 0 else "🔄")
        print(f"  {flag} {k}: {v:.0f}分")
    skipped = result.get("skipped_agents") or []
    if skipped:
        print(f"\n[Skipped Agents] {skipped}")
    print("\n[Final Report]")
    print(result.get("final_report", "No report generated"))
    print("\n[Reports]")
    reports = result.get("reports", {})
    for k, v in reports.items():
        print(f"  {k}: {len(v)} chars")