import sys
import logging
from pathlib import Path
from typing import Dict, Optional, List

logger = logging.getLogger("TechAgent")

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from layers.connectors import DataConnector
from layers.skills.tech_skill import TechSkill, tech_skill
from layers.agents.report_schema import parse_json_report, error_report, unavailable_report, AgentReport
from layers.agents.base_agent import QUANT_ANCHOR_RULE

REPORT_MAX_TOKENS = 1200
LLM_TEMPERATURE = 0.0


class TechAgent:

    def __init__(self, model_name: str = DEFAULT_MODEL, data_connector: Optional[DataConnector] = None):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)
        self.data_connector = data_connector
        self.tech_skill = tech_skill

    def analyze(self, stock_code: str, state: Optional[Dict] = None) -> Dict:
        logger.info(f"[TechAgent] 开始机构级技术面分析: {stock_code}")

        tech_data = None
        quality_context = None
        if state is not None:
            tech_data = state.get("tech_data")
            quality_context = state.get("quality_context")

        if tech_data is None and self.data_connector is not None:
            tech_data = self.data_connector.fetch_tech_data()

        if tech_data is None:
            logger.warning(f"[TechAgent] 无技术面数据: {stock_code}")
            return unavailable_report("tech").to_dict()

        try:
            signals = self.tech_skill.analyze(tech_data)
        except Exception as e:
            error_msg = f"技术面指标计算失败：{str(e)}"
            logger.error(f"[TechAgent] {error_msg}")
            return error_report("tech", error_msg).to_dict()

        prompt = f"""你是一位资深券商技术分析师，请基于以下量价技术分析数据，输出一份结构化的技术研判报告。

{quality_context or ''}

{self._build_data_context(stock_code, signals)}

请严格按以下JSON格式输出（不要包含任何其他文字，只输出JSON）：
{{
  "dimension": "tech",
  "overall_score": 72,
  "grade": "看多",
  "confidence": 75,
  "thesis": "一句话核心判断",
  "key_signals": ["信号1", "信号2", "信号3"],
  "risk_factors": ["风险1", "风险2"],
  "recommendation": "操作建议",
  "supporting_data": {{
    "trend": {{"direction": "多/空/震荡", "strength": 60}},
    "momentum": {{"macd_status": "金叉/死叉/纠缠", "kdj_status": "超买/超卖/正常"}},
    "volume": {{"signal": "放量/缩量/正常", "quality": "配合良好/背离"}}
  }}
}}

分析推理步骤（必须按此顺序分步思考）：
Step 1: 判断当前趋势方向，基于均线排列 + MACD位置，明确是多头/空头/震荡趋势
Step 2: 判断当前动能状态，基于KDJ位置 + MACD柱变化，判断动能是增强/衰减/背离
Step 3: 判断量价配合情况，基于量价信号 + 换手率，判断量能配合是否支持当前趋势
Step 4: 综合以上三步骤，计算整体强度，得出最终结论

要求：
1. 必须完成上述四步推理再填入JSON
2. overall_score 必须是0-100的整数，准确反映综合技术面强度
3. grade 必须是以下之一：强烈看多/看多/中性偏多/中性/中性偏空/看空/强烈看空
4. confidence 必须是0-100的整数，反映数据充分程度和判断确定性
5. key_signals 列出3-5个关键技术信号
6. risk_factors 列出2-3个关键技术风险
7. 仅基于提供的数据做判断，不编造数据
8. 只输出JSON，不要输出任何其他内容
{QUANT_ANCHOR_RULE}"""

        try:
            completion = self.client.chat.completions.create(
                model=get_model_id(self.model_name),
                messages=[
                    {"role": "system", "content": "你是一位资深券商技术分析师。请严格输出JSON格式的结构化分析结果，不要输出任何其他内容。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=LLM_TEMPERATURE,
                max_tokens=REPORT_MAX_TOKENS
            )
            raw = completion.choices[0].message.content.strip()
            report = parse_json_report(raw, "tech")
            logger.info(f"[TechAgent] 结构化技术面分析完成: {stock_code} | score={report.overall_score} grade={report.grade}")
            return report.to_dict()
        except Exception as e:
            error_msg = f"LLM调用失败：{str(e)}"
            logger.error(f"[TechAgent] {error_msg}")
            return error_report("tech", error_msg).to_dict()

    @staticmethod
    def _build_data_context(stock_code: str, signals) -> str:
        return f"""股票代码：{stock_code}

【均线系统】
多头排列：{signals.ma_system.arrangement}
金叉信号：{signals.ma_system.golden_cross}
死叉信号：{signals.ma_system.dead_cross}
MA5斜率：{signals.ma_system.ma5_slope:.2f}
MA20斜率：{signals.ma_system.ma20_slope:.2f}

【MACD指标】
DIF：{signals.macd_system.dif:.4f}
DEA：{signals.macd_system.dea:.4f}
MACD柱：{signals.macd_system.macd_hist:.4f}
MACD背离：{signals.macd_system.divergence}

【KDJ指标】
K：{signals.kdj_system.k:.1f}
D：{signals.kdj_system.d:.1f}
J：{signals.kdj_system.j:.1f}
超买：{'是' if signals.kdj_system.overbought else '否'}
超卖：{'是' if signals.kdj_system.oversold else '否'}

【量价分析】
量价信号：{signals.volume_structure.signal.value}
量能趋势：{signals.volume_structure.volume_trend}
换手率：{signals.volume_structure.turnover_rate:.2f}%

【趋势强度】
趋势状态：{signals.trend_strength.value}
市场状态：{signals.market_regime.value}

【技术综合评分】
评分：{signals.overall_score}/100
短期信号：{signals.short_term_signal}
中期信号：{signals.medium_term_signal}"""


def tech_agent_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    agent = TechAgent(model_name=DEFAULT_MODEL)
    tech_report = agent.analyze(stock_code, state)
    state["tech_report"] = tech_report
    return state


tech_agent = TechAgent