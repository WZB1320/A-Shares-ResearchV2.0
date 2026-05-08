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

REPORT_MAX_TOKENS = 3000
LLM_TEMPERATURE = 0.3
MAX_PARALLEL_WORKERS = 6


class ChiefAgent:
    """
    首席汇总Agent - 协调层（机构级标准化）
    职责：协调各维度Agent并行分析 + LLM 汇总生成机构级综合研报
    """

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
            return f"⚠️ {stock_code} {error_msg}"

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
            return f"⚠️ {stock_code} {error_msg}"

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
        reports = {}

        with ThreadPoolExecutor(max_workers=MAX_PARALLEL_WORKERS) as executor:
            future_to_agent = {}
            for name, (AgentClass, sub_state) in active_agents.items():
                agent = AgentClass(model_name=self.model_name)
                future = executor.submit(agent.analyze, stock_code, sub_state)
                future_to_agent[future] = name

            for future in as_completed(future_to_agent):
                agent_name = future_to_agent[future]
                try:
                    reports[agent_name] = future.result()
                except Exception as e:
                    error_msg = f"{agent_name}分析异常：{str(e)}"
                    logger.error(f"[ChiefAgent] {error_msg}")
                    reports[agent_name] = f"⚠️ {error_msg}"

        for name in selected_agents:
            if name not in reports:
                reports[name] = f"⚠️ {name} 分析未执行"

        logger.info(f"[ChiefAgent] 并行分析完成 | 耗时: {time.time() - agent_start:.1f}s")

        tech_report = reports.get("tech", "")
        fund_report = reports.get("fund", "")
        capital_report = reports.get("capital", "")
        industry_report = reports.get("industry", "")
        risk_report = reports.get("risk", "")
        valuation_report = reports.get("valuation", "")

        final_report = self.synthesize_reports(
            stock_code, tech_report, fund_report, capital_report,
            industry_report, risk_report, valuation_report
        )

        total_time = time.time() - start_time
        logger.info(f"[ChiefAgent] 机构级综合投研分析完成: {stock_code} | 总耗时: {total_time:.1f}s")
        return final_report

    def synthesize_reports(self, stock_code: str, tech_report: str, fund_report: str,
                           capital_report: str, industry_report: str, risk_report: str,
                           valuation_report: str) -> str:
        logger.info(f"[ChiefAgent] 开始综合研报汇总: {stock_code}")

        dimension_reports = []
        if "tech" in self.selected_agents and tech_report and not tech_report.startswith("⚠️"):
            dimension_reports.append(f"### 1. 技术面分析\n{tech_report}")
        if "fund" in self.selected_agents and fund_report and not fund_report.startswith("⚠️"):
            dimension_reports.append(f"### 2. 基本面分析\n{fund_report}")
        if "capital" in self.selected_agents and capital_report and not capital_report.startswith("⚠️"):
            dimension_reports.append(f"### 3. 资金面分析\n{capital_report}")
        if "industry" in self.selected_agents and industry_report and not industry_report.startswith("⚠️"):
            dimension_reports.append(f"### 4. 行业面分析\n{industry_report}")
        if "risk" in self.selected_agents and risk_report and not risk_report.startswith("⚠️"):
            dimension_reports.append(f"### 5. 风险面分析\n{risk_report}")
        if "valuation" in self.selected_agents and valuation_report and not valuation_report.startswith("⚠️"):
            dimension_reports.append(f"### 6. 估值面分析\n{valuation_report}")

        dimension_text = "\n\n".join(dimension_reports) if dimension_reports else "（无有效的分析维度数据）"

        prompt = f"""你是一位资深首席策略分析师，请基于以下各维度机构级分析报告，撰写一份综合投研策略报告。

股票代码：{stock_code}

{{
    "dimension_text": dimension_text
}}

请按以下结构输出综合策略报告（1200-1500字）：
1. 核心结论（一句话总结+综合评级）
2. 多维度交叉验证（技术面×基本面×资金面×估值面×行业面×风险面）
3. 关键矛盾与风险提示（各维度信号冲突点）
4. 情景分析（乐观/中性/悲观三种情景）
5. 操作策略建议（仓位+止损+目标价）

要求：专业、客观、数据驱动，使用机构级术语，体现多维度交叉验证的投资研究框架。"""

        try:
            completion = self.client.chat.completions.create(
                model=get_model_id(self.model_name),
                messages=[
                    {"role": "system", "content": "你是一位资深首席策略分析师，擅长多维度交叉验证和综合研判。请用专业、客观的语言撰写综合策略报告。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=LLM_TEMPERATURE,
                max_tokens=REPORT_MAX_TOKENS
            )
            final_report = completion.choices[0].message.content.strip()
            logger.info(f"[ChiefAgent] 综合研报汇总完成: {stock_code}")
            return final_report
        except Exception as e:
            error_msg = f"生成{stock_code}综合策略报告失败：{str(e)}"
            logger.error(f"[ChiefAgent] {error_msg}")

            fallback_parts = []
            if "tech" in self.selected_agents and tech_report and not tech_report.startswith("⚠️"):
                fallback_parts.append(f"## 技术面分析\n{tech_report}")
            if "fund" in self.selected_agents and fund_report and not fund_report.startswith("⚠️"):
                fallback_parts.append(f"## 基本面分析\n{fund_report}")
            if "capital" in self.selected_agents and capital_report and not capital_report.startswith("⚠️"):
                fallback_parts.append(f"## 资金面分析\n{capital_report}")
            if "industry" in self.selected_agents and industry_report and not industry_report.startswith("⚠️"):
                fallback_parts.append(f"## 行业面分析\n{industry_report}")
            if "risk" in self.selected_agents and risk_report and not risk_report.startswith("⚠️"):
                fallback_parts.append(f"## 风险面分析\n{risk_report}")
            if "valuation" in self.selected_agents and valuation_report and not valuation_report.startswith("⚠️"):
                fallback_parts.append(f"## 估值面分析\n{valuation_report}")

            fallback = f"# {stock_code} 深度投研策略报告\n\n⚠️ LLM综合汇总生成失败：{str(e)}\n\n以下为各维度独立分析报告：\n\n" + "\n\n---\n\n".join(fallback_parts)
            return fallback


def chief_agent_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    agent = ChiefAgent(model_name=DEFAULT_MODEL)
    final_report = agent.analyze(stock_code, state)
    state["final_report"] = final_report
    return state


chief_agent = ChiefAgent
