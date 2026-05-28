import sys
import logging
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger("IndustryAgent")

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from layers.connectors import DataConnector
from layers.skills.industry_skill import IndustrySkill, industry_skill
from layers.agents.report_schema import parse_json_report, error_report, unavailable_report

REPORT_MAX_TOKENS = 1200
LLM_TEMPERATURE = 0.0


class IndustryAgent:

    def __init__(self, model_name: str = DEFAULT_MODEL, data_connector: Optional[DataConnector] = None):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)
        self.data_connector = data_connector
        self.industry_skill = industry_skill

    def analyze(self, stock_code: str, state: Optional[Dict] = None) -> Dict:
        logger.info(f"[IndustryAgent] 开始机构级行业分析: {stock_code}")

        fundamental_data = None
        quality_context = None
        if state is not None:
            fundamental_data = state.get("fundamental_data") or state.get("financial_data")
            quality_context = state.get("quality_context")

        if fundamental_data is None and self.data_connector is not None:
            fundamental_data = self.data_connector.fetch_fundamental_data()

        if fundamental_data is None:
            logger.warning(f"[IndustryAgent] 无行业数据: {stock_code}")
            return unavailable_report("industry").to_dict()

        try:
            signals = self.industry_skill.analyze(fundamental_data)
        except Exception as e:
            error_msg = f"行业指标计算失败：{str(e)}"
            logger.error(f"[IndustryAgent] {error_msg}")
            return error_report("industry", error_msg).to_dict()

        prompt = f"""你是一位资深券商行业分析师，请基于以下行业数据，输出一份结构化的行业研判报告。

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
8. 只输出JSON，不要输出任何其他内容"""

        try:
            completion = self.client.chat.completions.create(
                model=get_model_id(self.model_name),
                messages=[
                    {"role": "system", "content": "你是一位资深券商行业分析师。请严格输出JSON格式的结构化分析结果，不要输出任何其他内容。数据不足时请降低confidence。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=LLM_TEMPERATURE,
                max_tokens=REPORT_MAX_TOKENS
            )
            raw = completion.choices[0].message.content.strip()
            report = parse_json_report(raw, "industry")
            logger.info(f"[IndustryAgent] 结构化行业分析完成: {stock_code} | score={report.overall_score} grade={report.grade}")
            return report.to_dict()
        except Exception as e:
            error_msg = f"LLM调用失败：{str(e)}"
            logger.error(f"[IndustryAgent] {error_msg}")
            return error_report("industry", error_msg).to_dict()

    @staticmethod
    def _build_data_context(stock_code: str, signals) -> str:
        return f"""股票代码：{stock_code}

【行业基本信息】
所属行业：{signals.industry_name}
同行业可比公司数量：{signals.peer_count}只

【数据可用性】
已获取数据：{', '.join(signals.data_available_fields) if signals.data_available_fields else '无'}
缺失数据：{', '.join(signals.data_unavailable_fields) if signals.data_unavailable_fields else '无'}

【行业综合评分】
评分：{signals.overall_score}/100
评级：{signals.industry_grade}"""


def industry_agent_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    agent = IndustryAgent(model_name=DEFAULT_MODEL)
    industry_report = agent.analyze(stock_code, state)
    state["industry_report"] = industry_report
    return state


industry_agent = IndustryAgent