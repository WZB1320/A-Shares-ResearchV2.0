import sys
import logging
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger("FundAgent")

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from layers.connectors import DataConnector
from layers.skills.fund_skill import FundSkill, fund_skill

REPORT_MAX_TOKENS = 1800
LLM_TEMPERATURE = 0.3


class FundAgent:
    """
    基本面Agent - 纯执行层（机构级标准化）
    职责：调用 FundSkill 进行机构级基本面分析 + LLM 生成机构级研报
    """

    def __init__(self, model_name: str = DEFAULT_MODEL, data_connector: Optional[DataConnector] = None):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)
        self.data_connector = data_connector
        self.fund_skill = fund_skill

    def analyze(self, stock_code: str, state: Optional[Dict] = None) -> str:
        logger.info(f"[FundAgent] 开始机构级基本面分析: {stock_code}")

        fundamental_data = None
        if state is not None:
            fundamental_data = state.get("fundamental_data") or state.get("financial_data")

        if fundamental_data is None and self.data_connector is not None:
            fundamental_data = self.data_connector.fetch_fundamental_data()

        if fundamental_data is None:
            logger.warning(f"[FundAgent] 无基本面数据: {stock_code}")
            return f"⚠️ {stock_code} 基本面数据不可用，无法生成分析报告"

        try:
            signals = self.fund_skill.analyze(fundamental_data)
        except Exception as e:
            error_msg = f"基本面指标计算失败：{str(e)}"
            logger.error(f"[FundAgent] {error_msg}")
            return f"⚠️ {stock_code} {error_msg}"

        prompt = f"""你是一位资深券商基本面分析师，请基于以下机构级财务分析数据，撰写一份专业的基本面研判报告。

股票代码：{stock_code}

【盈利能力】
ROE：{signals.profitability.roe:.2f}%
净利率：{signals.profitability.net_margin:.2f}%
毛利率：{signals.profitability.gross_margin:.2f}%
ROA：{signals.profitability.roa:.2f}%

【杜邦分析】
净利率贡献：{signals.dupont.net_margin_contribution:.2f}%
周转率贡献：{signals.dupont.turnover_contribution:.2f}%
杠杆贡献：{signals.dupont.leverage_contribution:.2f}%

【成长能力】
营收增速：{signals.growth.revenue_growth:.2f}%
利润增速：{signals.growth.profit_growth:.2f}%
扣非增速：{signals.growth.deducted_growth:.2f}%

【资产负债】
资产负债率：{signals.balance_sheet.debt_ratio:.2f}%
流动比率：{signals.balance_sheet.current_ratio:.2f}
速动比率：{signals.balance_sheet.quick_ratio:.2f}

【盈利质量】
经营现金流/净利润：{signals.quality.cash_flow_match:.2f}

【基本面综合评分】
评分：{signals.overall_score:.1f}/100
评级：{signals.fund_level}

请按以下结构输出报告（800-1000字）：
1. 盈利质量评估（ROE杜邦拆解+现金流匹配度）
2. 成长性分析（营收/利润/扣非增速趋势）
3. 财务健康度（资产负债结构+偿债能力）
4. 经营效率（周转率+费用控制）
5. 综合评分与投资建议

要求：专业、客观、数据驱动，使用机构级术语。"""

        try:
            completion = self.client.chat.completions.create(
                model=get_model_id(self.model_name),
                messages=[
                    {"role": "system", "content": "你是一位资深券商基本面分析师，擅长财务分析和企业价值评估。请用专业、客观的语言撰写报告。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=LLM_TEMPERATURE,
                max_tokens=REPORT_MAX_TOKENS
            )
            report = completion.choices[0].message.content.strip()
            logger.info(f"[FundAgent] 机构级基本面分析完成: {stock_code}")
            return report
        except Exception as e:
            error_msg = f"生成{stock_code}基本面报告失败：{str(e)}"
            logger.error(f"[FundAgent] {error_msg}")
            return f"⚠️ {error_msg}"


def fund_agent_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    agent = FundAgent(model_name=DEFAULT_MODEL)
    fund_report = agent.analyze(stock_code, state)
    state["fund_report"] = fund_report
    return state


fund_agent = FundAgent
