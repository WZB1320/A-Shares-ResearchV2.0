import sys
import logging
from pathlib import Path
from typing import Dict, Optional, List

logger = logging.getLogger("RiskAgent")

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from layers.connectors import DataConnector
from layers.skills.risk_skill import RiskSkill, risk_skill

REPORT_MAX_TOKENS = 1800
LLM_TEMPERATURE = 0.3


class RiskAgent:
    """
    风险面Agent - 纯执行层（机构级标准化）
    职责：调用 RiskSkill 进行机构级风险分析 + LLM 生成机构级研报
    """

    def __init__(self, model_name: str = DEFAULT_MODEL, data_connector: Optional[DataConnector] = None):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)
        self.data_connector = data_connector
        self.risk_skill = risk_skill

    def analyze(self, stock_code: str, state: Optional[Dict] = None) -> str:
        logger.info(f"[RiskAgent] 开始机构级风险分析: {stock_code}")

        financial_data = None
        tech_data = None
        if state is not None:
            financial_data = state.get("financial_data") or state.get("fundamental_data")
            tech_data = state.get("tech_data")

        if financial_data is None and self.data_connector is not None:
            financial_data = self.data_connector.fetch_financial_data()
        if tech_data is None and self.data_connector is not None:
            tech_data = self.data_connector.fetch_tech_data()

        if financial_data is None:
            logger.warning(f"[RiskAgent] 无风险分析数据: {stock_code}")
            return f"⚠️ {stock_code} 风险分析数据不可用，无法生成分析报告"

        try:
            signals = self.risk_skill.analyze(financial_data, tech_data)
        except Exception as e:
            error_msg = f"风险指标计算失败：{str(e)}"
            logger.error(f"[RiskAgent] {error_msg}")
            return f"⚠️ {stock_code} {error_msg}"

        prompt = f"""你是一位资深券商风控分析师，请基于以下机构级风险分析数据，撰写一份专业的风险评估报告。

股票代码：{stock_code}

【财务风险】
资产负债率：{signals.financial_risk.debt_ratio if hasattr(signals, 'financial_risk') and hasattr(signals.financial_risk, 'debt_ratio') else 'N/A'}
流动比率：{signals.financial_risk.current_ratio if hasattr(signals, 'financial_risk') and hasattr(signals.financial_risk, 'current_ratio') else 'N/A'}
财务风险评级：{signals.financial_risk.level if hasattr(signals, 'financial_risk') and hasattr(signals.financial_risk, 'level') else 'N/A'}

【市场风险】
波动率：{signals.market_risk.volatility if hasattr(signals, 'market_risk') and hasattr(signals.market_risk, 'volatility') else 'N/A'}
最大回撤：{signals.market_risk.max_drawdown if hasattr(signals, 'market_risk') and hasattr(signals.market_risk, 'max_drawdown') else 'N/A'}
Beta：{signals.market_risk.beta if hasattr(signals, 'market_risk') and hasattr(signals.market_risk, 'beta') else 'N/A'}
市场风险评级：{signals.market_risk.level if hasattr(signals, 'market_risk') and hasattr(signals.market_risk, 'level') else 'N/A'}

【流动性风险】
日均换手率：{signals.liquidity_risk.avg_turnover if hasattr(signals, 'liquidity_risk') and hasattr(signals.liquidity_risk, 'avg_turnover') else 'N/A'}
流动性评级：{signals.liquidity_risk.level if hasattr(signals, 'liquidity_risk') and hasattr(signals.liquidity_risk, 'level') else 'N/A'}

【风险综合评分】
评分：{signals.overall_score:.1f}/100
评级：{signals.risk_level}

请按以下结构输出报告（800-1000字）：
1. 财务风险评估（杠杆水平+偿债能力+现金流风险）
2. 市场风险评估（波动率+回撤+Beta+系统性风险）
3. 流动性风险评估（换手率+冲击成本+流动性枯竭风险）
4. 尾部风险提示（极端事件+黑天鹅情景）
5. 综合风险评级与风控建议

要求：专业、客观、数据驱动，使用机构级术语。"""

        try:
            completion = self.client.chat.completions.create(
                model=get_model_id(self.model_name),
                messages=[
                    {"role": "system", "content": "你是一位资深券商风控分析师，擅长多维度风险评估和压力测试。请用专业、客观的语言撰写报告。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=LLM_TEMPERATURE,
                max_tokens=REPORT_MAX_TOKENS
            )
            report = completion.choices[0].message.content.strip()
            logger.info(f"[RiskAgent] 机构级风险分析完成: {stock_code}")
            return report
        except Exception as e:
            error_msg = f"生成{stock_code}风险报告失败：{str(e)}"
            logger.error(f"[RiskAgent] {error_msg}")
            return f"⚠️ {error_msg}"


def risk_agent_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    agent = RiskAgent(model_name=DEFAULT_MODEL)
    risk_report = agent.analyze(stock_code, state)
    state["risk_report"] = risk_report
    return state


risk_agent = RiskAgent
