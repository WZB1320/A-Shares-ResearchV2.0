import sys
import logging
from pathlib import Path
from typing import Dict, Optional, List

logger = logging.getLogger("RiskAgent")

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from layers.connectors import DataConnector
from layers.skills.risk_skill import RiskSkill, risk_skill
from layers.agents.report_schema import parse_json_report, error_report, unavailable_report
from layers.agents.base_agent import QUANT_ANCHOR_RULE

REPORT_MAX_TOKENS = 1600
LLM_TEMPERATURE = 0.0


class RiskAgent:

    def __init__(self, model_name: str = DEFAULT_MODEL, data_connector: Optional[DataConnector] = None):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)
        self.data_connector = data_connector
        self.risk_skill = risk_skill

    def analyze(self, stock_code: str, state: Optional[Dict] = None) -> Dict:
        logger.info(f"[RiskAgent] 开始机构级风险分析: {stock_code}")

        financial_data = None
        tech_data = None
        quality_context = None
        if state is not None:
            financial_data = state.get("financial_data") or state.get("fundamental_data")
            tech_data = state.get("tech_data")
            quality_context = state.get("quality_context")

        if financial_data is None and self.data_connector is not None:
            financial_data = self.data_connector.fetch_financial_data()
        if tech_data is None and self.data_connector is not None:
            tech_data = self.data_connector.fetch_tech_data()

        if financial_data is None:
            logger.warning(f"[RiskAgent] 无风险分析数据: {stock_code}")
            return unavailable_report("risk").to_dict()

        try:
            signals = self.risk_skill.analyze(financial_data, tech_data)
        except Exception as e:
            error_msg = f"风险指标计算失败：{str(e)}"
            logger.error(f"[RiskAgent] {error_msg}")
            return error_report("risk", error_msg).to_dict()

        prompt = f"""你是一位资深券商风控分析师，请基于以下风险分析数据，输出一份结构化的风险评估报告。

{quality_context or ''}

{self._build_data_context(stock_code, signals)}

请严格按以下JSON格式输出（不要包含任何其他文字，只输出JSON）：
{{
  "dimension": "risk",
  "overall_score": 55,
  "grade": "中性",
  "confidence": 60,
  "thesis": "一句话核心判断",
  "key_signals": ["信号1", "信号2", "信号3"],
  "risk_factors": ["风险1", "风险2", "风险3"],
  "recommendation": "操作建议",
  "supporting_data": {{
    "financial_risk": {{"level": "低/中/高/极高", "debt_concern": "无/轻微/中等/严重"}},
    "market_risk": {{"level": "低/中/高/极高", "volatility_concern": "无/轻微/中等/严重"}},
    "unavailable_dimensions": ["维度1", "维度2"]
  }}
}}

分析推理步骤（必须按此顺序分步思考）：
Step 1: 评估财务风险，基于资产负债率、流动比率、现金流质量，判断财务安全边际
Step 2: 评估市场风险，基于波动率、趋势方向，判断价格波动带来的回撤风险
Step 3: 识别不可用维度的盲区，明确哪些风险因缺数据而无法评估
Step 4: 综合各维度风险等级，加权计算整体风险评分

要求：
1. 必须完成上述四步推理再填入JSON
2. overall_score 必须是0-100的整数（注意：风险评分，分数越高风险越低即越安全）
3. grade 必须是以下之一：强烈看多/看多/中性偏多/中性/中性偏空/看空/强烈看空
4. confidence 必须是0-100的整数
5. key_signals 列出3-5个关键风险信号
6. risk_factors 列出3-5个具体风险因素
7. 仅基于提供的数据做判断，不编造数据
8. 对标记为"不可用"的维度不进行推测
9. 只输出JSON，不要输出任何其他内容
10. **重要**：负值（如ROE为负、PE为负）是有效数据点，表示公司亏损，不是"数据缺失"。只有当数据明确标注为"不可用"或"数据可用：否"时，才能在risk_factors中提及"数据缺失"。
{QUANT_ANCHOR_RULE}"""

        try:
            completion = self.client.chat.completions.create(
                model=get_model_id(self.model_name),
                messages=[
                    {"role": "system", "content": "你是一位资深券商风控分析师。请严格输出JSON格式的结构化分析结果，不要输出任何其他内容。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=LLM_TEMPERATURE,
                max_tokens=REPORT_MAX_TOKENS
            )
            raw = completion.choices[0].message.content.strip()
            report = parse_json_report(raw, "risk")
            logger.info(f"[RiskAgent] 结构化风险分析完成: {stock_code} | score={report.overall_score} grade={report.grade}")
            return report.to_dict()
        except Exception as e:
            error_msg = f"LLM调用失败：{str(e)}"
            logger.error(f"[RiskAgent] {error_msg}")
            return error_report("risk", error_msg).to_dict()

    @staticmethod
    def _build_data_context(stock_code: str, signals) -> str:
        unavailable_text = chr(10).join(f'  - {d}' for d in signals.unavailable_dimensions) if signals.unavailable_dimensions else '无'
        return f"""股票代码：{stock_code}

【财务风险】
资产负债率：{signals.financial.debt_to_asset}%
流动比率：{signals.financial.current_ratio}
速动比率：{signals.financial.quick_ratio}
ROE：{signals.financial.roe}%
毛利率：{signals.financial.gross_margin}%
净利润同比：{signals.financial.net_profit_yoy}%
利息保障倍数：{signals.financial.interest_coverage}
现金流与利润背离：{'是' if signals.financial.cash_flow_shortfall else '否'}
连续亏损年数：{signals.financial.consecutive_losses}
审计意见：{signals.financial.audit_opinion}
财务风险等级：{signals.financial.risk_level.value}
数据可用：{'是' if signals.financial.data_available else '否'}

【市场风险】
年化波动率：{signals.market.volatility_30d:.1f}%
最大回撤：{signals.market.max_drawdown_1y:.1f}%
平均换手率：{signals.market.avg_turnover:.1f}%
尾部风险：{signals.market.tail_risk}
市场风险等级：{signals.market.risk_level.value}
数据可用：{'是' if signals.market.data_available else '否'}

【风险调整收益指标】
夏普比率：{signals.market.sharpe_ratio}
索提诺比率：{signals.market.sortino_ratio}
卡尔玛比率：{signals.market.calmar_ratio}
Beta系数：{signals.market.beta}
下行波动率：{signals.market.downside_deviation:.1f}%
VaR(95%)：{signals.market.var_95:.2f}%（单日）
CVaR(95%)：{signals.market.cvar_95:.2f}%（单日）

【不可用风险维度】
{unavailable_text}

【综合风险评分】
评分：{signals.overall_risk_score}/100
风险等级：{signals.overall_risk_level.value}"""


def risk_agent_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    agent = RiskAgent(model_name=DEFAULT_MODEL)
    risk_report = agent.analyze(stock_code, state)
    state["risk_report"] = risk_report
    return state


risk_agent = RiskAgent