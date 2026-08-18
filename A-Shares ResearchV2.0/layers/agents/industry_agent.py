import sys
import logging
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger("IndustryAgent")

from config.llm_config import DEFAULT_MODEL
from layers.connectors import DataConnector
from layers.skills.industry_skill import industry_skill
from layers.agents.base_agent import BaseAnalysisAgent, QUANT_ANCHOR_RULE


class IndustryAgent(BaseAnalysisAgent):
    """行业面分析 Agent"""

    MAX_TOKENS = 1600
    LLM_TEMPERATURE = 0.0

    @property
    def dimension(self) -> str:
        return "industry"

    def _get_data(self, state):
        fundamental_data = None
        if state is not None:
            fundamental_data = state.get("fundamental_data") or state.get("financial_data")
        if fundamental_data is None and self.data_connector is not None:
            fundamental_data = self.data_connector.fetch_fundamental_data()
        return fundamental_data

    def _run_skill(self, data):
        return industry_skill.analyze(data)

    def _system_prompt(self) -> str:
        return "你是一位资深券商行业分析师。请严格输出JSON格式的结构化分析结果，不要输出任何其他内容。数据不足时请降低confidence。"

    def _build_prompt(self, stock_code: str, signals, quality_context: Optional[str]) -> str:
        return f"""你是一位资深券商行业分析师，请基于以下行业数据，输出一份结构化的行业研判报告。

{quality_context or ''}

{self._build_data_context(stock_code, signals)}

请严格按以下JSON格式输出（不要包含任何其他文字，只输出JSON）：
{{
  "dimension": "industry",
  "overall_score": 60,
  "grade": "中性",
  "confidence": 40,
  "thesis": "一句话核心判断",
  "key_signals": ["信号1", "信号2"],
  "risk_factors": ["风险1", "风险2"],
  "recommendation": "操作建议",
  "supporting_data": {{
    "position": {{"industry": "行业名称", "peer_range": "可比公司数量"}},
    "data_coverage": {{"available": ["字段1"], "missing": ["字段2"]}}
  }}
}}

分析推理步骤（必须按此顺序分步思考）：
Step 1: 确认行业分类，明确该股属于什么行业，行业竞争格局如何
Step 2: 评估数据覆盖度，判断现有数据是否足够支撑可靠结论
Step 3: 梳理可比公司，判断该股在行业中的相对位置
Step 4: 综合数据可得性和行业定位，得出行业层面投资结论

要求：
1. 必须完成上述四步推理再填入JSON
2. overall_score 必须是0-100的整数
3. grade 必须是以下之一：强烈看多/看多/中性偏多/中性/中性偏空/看空/强烈看空
4. confidence 必须是0-100的整数，反映数据充分程度（数据不足时confidence应偏低）
5. key_signals 列出2-3个行业关键信号
6. risk_factors 列出2-3个行业风险
7. 仅基于提供的数据做判断，不编造数据，不要推测未提供数据的行业信息
8. 只输出JSON，不要输出任何其他内容
9. **重要**：负值（如ROE为负、PE为负）是有效数据点，表示公司亏损，不是"数据缺失"。只有当数据覆盖列表中明确标注为缺失的字段，才能在risk_factors中提及"数据缺失"。data_coverage中的available和missing必须与上方"数据可用性"部分一致。
{QUANT_ANCHOR_RULE}"""

    @staticmethod
    def _build_data_context(stock_code: str, signals) -> str:
        ctx = f"""股票代码：{stock_code}

【行业基本信息】
所属行业：{signals.industry_name}
同行业可比公司数量：{signals.peer_count}只

【数据可用性】
已获取数据：{', '.join(signals.data_available_fields) if signals.data_available_fields else '无'}
缺失数据：{', '.join(signals.data_unavailable_fields) if signals.data_unavailable_fields else '无'}

【个股财务摘要】
ROE：{signals.stock_roe}%
毛利率：{signals.stock_gross_margin}%
营收增速：{signals.stock_revenue_growth}%"""

        if signals.peer_comparison:
            pc = signals.peer_comparison
            ctx += f"""
【行业对标排名】({pc.peer_count}只可比公司)
PE(TTM)：{pc.pe_ttm} | 行业排名：{pc.pe_rank}/{pc.peer_count} | 行业中位数：{pc.pe_median} | 低于{pc.pe_percentile}%的同行
PB：{pc.pb} | 行业排名：{pc.pb_rank}/{pc.peer_count} | 行业中位数：{pc.pb_median}
ROE：{pc.roe}% | 行业排名：{pc.roe_rank}/{pc.peer_count} | 行业中位数：{pc.roe_median}% | 超过{pc.roe_percentile}%的同行
毛利率：{pc.gross_margin}% | 行业排名：{pc.margin_rank}/{pc.peer_count} | 行业中位数：{pc.margin_median}%
营收增速：{pc.revenue_growth}% | 行业排名：{pc.growth_rank}/{pc.peer_count} | 行业中位数：{pc.growth_median}%"""

        ctx += f"""
【行业综合评分】
评分：{signals.overall_score}/100
评级：{signals.industry_grade}"""
        return ctx


def industry_agent_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    agent = IndustryAgent(model_name=DEFAULT_MODEL)
    industry_report = agent.analyze(stock_code, state)
    state["industry_report"] = industry_report
    return state


industry_agent = IndustryAgent
