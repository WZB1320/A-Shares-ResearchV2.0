import sys
import logging
import time
from pathlib import Path
from typing import Dict, Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger("ChiefAgent")

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from layers.connectors import DataConnector
from layers.agents.tech_agent import TechAgent
from layers.agents.fund_agent import FundAgent
from layers.agents.capital_agent import CapitalAgent
from layers.agents.industry_agent import IndustryAgent
from layers.agents.risk_agent import RiskAgent
from layers.agents.valuation_agent import ValuationAgent
from layers.agents.report_schema import (
    AgentReport, aggregate_reports, reports_to_markdown,
    error_report, unavailable_report
)

REPORT_MAX_TOKENS = 3000
LLM_TEMPERATURE = 0.3
MAX_PARALLEL_WORKERS = 6


class ChiefAgent:

    def __init__(self, model_name: str = DEFAULT_MODEL, data_connector: Optional[DataConnector] = None):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)
        self.data_connector = data_connector
        self.selected_agents = None

    def analyze(self, stock_code: str, state: Optional[Dict] = None) -> str:
        start_time = time.time()

        selected_agents = None
        if state is not None:
            selected_agents = state.get("selected_agents")

        if selected_agents is None:
            selected_agents = ["tech", "fund", "capital", "industry", "risk", "valuation"]

        self.selected_agents = selected_agents
        logger.info(f"[ChiefAgent] 开始机构级综合投研分析: {stock_code} | 选中Agent: {selected_agents}")

        try:
            connector = self.data_connector or DataConnector(stock_code)
        except Exception as e:
            error_msg = f"数据连接器初始化失败：{str(e)}"
            logger.error(f"[ChiefAgent] {error_msg}")
            return f"# {stock_code} 深度投研策略报告\n\n⚠️ {error_msg}"

        data_start = time.time()
        try:
            all_data = connector.fetch_all()
            tech_data = all_data.get("tech_data")
            fundamental_data = all_data.get("fundamental_data") or all_data.get("financial_data")
            capital_data = all_data.get("capital_data")
            valuation_data = all_data.get("valuation_data")
            financial_data = all_data.get("financial_data") or all_data.get("fundamental_data")
            logger.info(f"[ChiefAgent] 数据获取完成: {stock_code} | 耗时: {time.time() - data_start:.1f}s")
        except Exception as e:
            error_msg = f"数据获取失败：{str(e)}"
            logger.error(f"[ChiefAgent] {error_msg}")
            return f"# {stock_code} 深度投研策略报告\n\n⚠️ {error_msg}"

        agent_start = time.time()
        logger.info(f"[ChiefAgent] 各维度Agent并行分析开始 | 选中: {selected_agents}")

        agents_map = {
            "tech": (TechAgent, {"tech_data": tech_data}),
            "fund": (FundAgent, {"fundamental_data": fundamental_data}),
            "capital": (CapitalAgent, {"capital_data": capital_data}),
            "industry": (IndustryAgent, {"fundamental_data": fundamental_data}),
            "risk": (RiskAgent, {"financial_data": financial_data, "tech_data": tech_data}),
            "valuation": (ValuationAgent, {"valuation_data": valuation_data, "fundamental_data": fundamental_data}),
        }

        active_agents = {k: v for k, v in agents_map.items() if k in selected_agents}
        raw_reports = {}

        with ThreadPoolExecutor(max_workers=MAX_PARALLEL_WORKERS) as executor:
            future_to_agent = {}
            for name, (AgentClass, sub_state) in active_agents.items():
                agent = AgentClass(model_name=self.model_name)
                future = executor.submit(agent.analyze, stock_code, sub_state)
                future_to_agent[future] = name

            for future in as_completed(future_to_agent):
                agent_name = future_to_agent[future]
                try:
                    raw_reports[agent_name] = future.result()
                except Exception as e:
                    error_msg = f"{agent_name}分析异常：{str(e)}"
                    logger.error(f"[ChiefAgent] {error_msg}")
                    raw_reports[agent_name] = error_report(agent_name, error_msg).to_dict()

        for name in selected_agents:
            if name not in raw_reports:
                raw_reports[name] = error_report(name, "分析未执行").to_dict()

        reports = {}
        for name, raw in raw_reports.items():
            if isinstance(raw, dict):
                reports[name] = AgentReport.from_dict(raw, name)
            else:
                reports[name] = AgentReport(
                    dimension=name, overall_score=50, grade="中性",
                    confidence=0, thesis="未知格式返回",
                    key_signals=[], risk_factors=[], recommendation="",
                    raw_text=str(raw)[:500], parse_error=True
                )

        logger.info(f"[ChiefAgent] 并行分析完成 | 耗时: {time.time() - agent_start:.1f}s")

        final_report = self.synthesize_reports(stock_code, reports)

        total_time = time.time() - start_time
        logger.info(f"[ChiefAgent] 机构级综合投研分析完成: {stock_code} | 总耗时: {total_time:.1f}s")
        return final_report

    def synthesize_reports(self, stock_code: str, reports: Dict[str, AgentReport]) -> str:
        logger.info(f"[ChiefAgent] 开始综合研报汇总: {stock_code}")

        aggregation = aggregate_reports(reports)

        dimension_md = reports_to_markdown(reports)

        dim_names = {"tech": "技术面", "fund": "基本面", "capital": "资金面",
                     "industry": "行业面", "risk": "风险面", "valuation": "估值面"}

        dim_scores_text = []
        for dim_name, info in aggregation.get("dimensions", {}).items():
            label = dim_names.get(dim_name, dim_name)
            dim_scores_text.append(
                f"- {label}: 评分 {info['score']}/100 | {info['grade']} | 置信度 {info['confidence']}% | {info['thesis']}"
            )

        invalid_text = ""
        if aggregation.get("invalid_dimensions"):
            invalid_text = "\n【数据不可用维度】\n"
            for dim, reason in aggregation["invalid_dimensions"].items():
                label = dim_names.get(dim, dim)
                invalid_text += f"- {label}: {reason}\n"

        prompt = f"""你是一位资深首席策略分析师，请基于以下各维度的结构化分析数据，撰写一份专业的综合投研策略报告。

股票代码：{stock_code}

【综合评分概览（规则引擎聚合）】
综合评分：{aggregation['overall_score']}/100
综合评级：{aggregation['overall_grade']}
综合置信度：{aggregation['overall_confidence']}%
有效维度数：{aggregation['valid_count']}/{aggregation['dimension_count']}
多空共识：{aggregation['consensus']}

【各维度评分明细】
{chr(10).join(dim_scores_text) if dim_scores_text else '无有效维度数据'}

【跨维度关键信号汇总】
{chr(10).join(f'- {s}' for s in aggregation.get('all_signals', [])) if aggregation.get('all_signals') else '无'}

【跨维度风险汇总】
{chr(10).join(f'- {r}' for r in aggregation.get('all_risks', [])) if aggregation.get('all_risks') else '无'}

【多空分歧】
{chr(10).join(f'- {c}' for c in aggregation.get('conflicts', [])) if aggregation.get('conflicts') else '各维度信号方向基本一致'}
{invalid_text}

【各维度详细分析】
{dimension_md}

请按以下步骤逐步推理，再进行报告撰写：

分析推理步骤（必须按此顺序分步思考）：
Step 1: 通览各维度评分，计算各维度信号的一致性/分歧度，标注矛盾维度
Step 2: 判断多空力量对比，基于各维度共识和分歧，计算综合多空倾向
Step 3: 筛选最关键的2-3个风险点（从all_risks中优先选得分最低维度的风险）
Step 4: 推演乐观/中性/悲观三种情景下的投资路径
Step 5: 基于以上四步，制定具体的操作策略建议

请按以下结构输出综合策略报告（1200-1500字）：
1. **核心结论** - 基于综合评分 {aggregation['overall_score']}/100 和 综合评级 {aggregation['overall_grade']}，给出明确的综合研判
2. **多维度交叉验证** - 逐一分析各维度的信号方向是否一致，重点标注信号冲突的维度
3. **关键矛盾与风险** - 汇总跨维度风险，分析最可能影响投资决策的2-3个关键矛盾点
4. **情景分析** - 乐观/中性/悲观三种情景下的投资逻辑推演
5. **操作策略建议** - 具体的仓位、止损、目标价建议

要求：专业、客观、严格基于提供的数据，使用机构级术语。综合评分和评级来自规则引擎的量化聚合，请在报告中体现这一量化维度。"""

        try:
            completion = self.client.chat.completions.create(
                model=get_model_id(self.model_name),
                messages=[
                    {"role": "system", "content": "你是一位资深首席策略分析师，擅长多维度交叉验证和综合研判。请严格基于提供的数据进行分析，不要编造任何未提供的数据。如果数据不足，请明确指出并降低结论确定性。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=LLM_TEMPERATURE,
                max_tokens=REPORT_MAX_TOKENS
            )
            final_narrative = completion.choices[0].message.content.strip()

            header = f"""# {stock_code} 深度投研策略报告

## 综合评分概览

| 维度 | 评分 | 评级 | 置信度 |
|------|------|------|--------|
"""
            for dim_name, info in aggregation.get("dimensions", {}).items():
                label = dim_names.get(dim_name, dim_name)
                header += f"| {label} | {info['score']}/100 | {info['grade']} | {info['confidence']}% |\n"

            header += f"""
| **综合** | **{aggregation['overall_score']}/100** | **{aggregation['overall_grade']}** | **{aggregation['overall_confidence']}%** |

> 多空共识：{aggregation['consensus']} | 有效维度：{aggregation['valid_count']}/{aggregation['dimension_count']}

---

## 首席策略研判

"""

            if aggregation.get("conflicts"):
                header += "### ⚠️ 多空分歧提示\n\n"
                for c in aggregation["conflicts"]:
                    header += f"> {c}\n"
                header += "\n"

            final_report = header + final_narrative

            logger.info(f"[ChiefAgent] 综合研报汇总完成: {stock_code}")
            return final_report
        except Exception as e:
            error_msg = f"LLM综合汇总生成失败：{str(e)}"
            logger.error(f"[ChiefAgent] {error_msg}")

            header = f"""# {stock_code} 深度投研策略报告

## 综合评分概览

| 维度 | 评分 | 评级 | 置信度 |
|------|------|------|--------|
"""
            for dim_name, info in aggregation.get("dimensions", {}).items():
                label = dim_names.get(dim_name, dim_name)
                header += f"| {label} | {info['score']}/100 | {info['grade']} | {info['confidence']}% |\n"

            header += f"""
| **综合** | **{aggregation['overall_score']}/100** | **{aggregation['overall_grade']}** | **{aggregation['overall_confidence']}%** |

> ⚠️ LLM综合汇总生成失败：{error_msg}

---

## 各维度结构化分析

{dimension_md}
"""
            return header


def chief_agent_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    agent = ChiefAgent(model_name=DEFAULT_MODEL)
    final_report = agent.analyze(stock_code, state)
    state["final_report"] = final_report
    return state


chief_agent = ChiefAgent