import sys
import logging
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger("CapitalAgent")

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from layers.connectors import DataConnector
from layers.skills.capital_skill import CapitalSkill, capital_skill
from layers.agents.report_schema import parse_json_report, error_report, unavailable_report

REPORT_MAX_TOKENS = 1200
LLM_TEMPERATURE = 0.1


class CapitalAgent:

    def __init__(self, model_name: str = DEFAULT_MODEL, data_connector: Optional[DataConnector] = None):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)
        self.data_connector = data_connector
        self.capital_skill = capital_skill

    def analyze(self, stock_code: str, state: Optional[Dict] = None) -> Dict:
        logger.info(f"[CapitalAgent] 开始机构级资金面分析: {stock_code}")

        capital_data = None
        if state is not None:
            capital_data = state.get("capital_data")

        if capital_data is None and self.data_connector is not None:
            capital_data = self.data_connector.fetch_capital_data()

        if capital_data is None:
            logger.warning(f"[CapitalAgent] 无资金面数据: {stock_code}")
            return unavailable_report("capital").to_dict()

        try:
            signals = self.capital_skill.analyze(capital_data)
        except Exception as e:
            error_msg = f"资金面指标计算失败：{str(e)}"
            logger.error(f"[CapitalAgent] {error_msg}")
            return error_report("capital", error_msg).to_dict()

        prompt = f"""你是一位资深券商资金面分析师，请基于以下资金流向分析数据，输出一份结构化的资金面研判报告。

{self._build_data_context(stock_code, signals)}

请严格按以下JSON格式输出（不要包含任何其他文字，只输出JSON）：
{{
  "dimension": "capital",
  "overall_score": 65,
  "grade": "看多",
  "confidence": 70,
  "thesis": "一句话核心判断",
  "key_signals": ["信号1", "信号2", "信号3"],
  "risk_factors": ["风险1", "风险2"],
  "recommendation": "操作建议",
  "supporting_data": {{
    "northbound": {{"trend": "流入/流出/均衡", "attitude": "积极/中性/谨慎"}},
    "main_force": {{"intent": "建仓/出货/观望", "strength": "强/中/弱"}},
    "margin": {{"sentiment": "激进/中性/谨慎", "risk": "偏高/正常/偏低"}},
    "consensus": {{"level": 65, "divergence": "有/无"}}
  }}
}}

分析推理步骤（必须按此顺序分步思考）：
Step 1: 分析北向资金态度，基于流入流出趋势，判断外资对该股的配置意愿
Step 2: 分析主力资金动向，基于主力净流入，判断机构意图是建仓/出货/观望
Step 3: 分析融资情绪和风险，基于融资余额变化，判断杠杆资金风险偏好
Step 4: 综合各类资金的一致性/分歧性，判断市场共识程度
Step 5: 综合以上四步骤，给出资金面综合研判

要求：
1. 必须完成上述五步推理再填入JSON
2. overall_score 必须是0-100的整数
3. grade 必须是以下之一：强烈看多/看多/中性偏多/中性/中性偏空/看空/强烈看空
4. confidence 必须是0-100的整数
5. key_signals 列出3-5个关键资金信号
6. risk_factors 列出2-3个资金面风险
7. 仅基于提供的数据做判断，不编造数据
8. 只输出JSON，不要输出任何其他内容"""

        try:
            completion = self.client.chat.completions.create(
                model=get_model_id(self.model_name),
                messages=[
                    {"role": "system", "content": "你是一位资深券商资金面分析师。请严格输出JSON格式的结构化分析结果，不要输出任何其他内容。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=LLM_TEMPERATURE,
                max_tokens=REPORT_MAX_TOKENS
            )
            raw = completion.choices[0].message.content.strip()
            report = parse_json_report(raw, "capital")
            logger.info(f"[CapitalAgent] 结构化资金面分析完成: {stock_code} | score={report.overall_score} grade={report.grade}")
            return report.to_dict()
        except Exception as e:
            error_msg = f"LLM调用失败：{str(e)}"
            logger.error(f"[CapitalAgent] {error_msg}")
            return error_report("capital", error_msg).to_dict()

    @staticmethod
    def _build_data_context(stock_code: str, signals) -> str:
        return f"""股票代码：{stock_code}

【北向资金】
5日趋势：{signals.north.trend_5d}
10日趋势：{signals.north.trend_10d}
30日趋势：{signals.north.trend_30d}
净流入：{signals.north.net_inflow}
信号：{signals.north.signal}

【主力资金】
5日趋势：{signals.main.trend_5d}
10日趋势：{signals.main.trend_10d}
净流入：{signals.main.net_inflow}
信号：{signals.main.signal}
机构意图：{signals.main.institutional_intent}

【融资融券】
融资余额：{signals.margin.margin_balance}
融资变化：{signals.margin.margin_change_pct}%
杠杆信号：{signals.margin.leverage_signal}
风险等级：{signals.margin.risk_level}

【龙虎榜】
30日上榜天数：{signals.dragon.active_days_30d}
机构买入占比：{signals.dragon.institutional_buy_ratio}
主力买方类型：{signals.dragon.top_buyer_type}
信号：{signals.dragon.signal}

【资金共识度】
共识度：{signals.flow_structure.consensus_level}%
背离预警：{signals.flow_structure.divergence_warning}

【资金综合评分】
评分：{signals.overall_score}/100
评级：{signals.capital_grade}"""


def capital_agent_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    agent = CapitalAgent(model_name=DEFAULT_MODEL)
    capital_report = agent.analyze(stock_code, state)
    state["capital_report"] = capital_report
    return state


capital_agent = CapitalAgent