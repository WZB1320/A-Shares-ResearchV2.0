import sys
import logging
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger("CapitalAgent")

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from layers.connectors import DataConnector
from layers.skills.capital_skill import CapitalSkill, capital_skill

REPORT_MAX_TOKENS = 1800
LLM_TEMPERATURE = 0.3


class CapitalAgent:
    """
    资金面Agent - 纯执行层（机构级标准化）
    职责：调用 CapitalSkill 进行机构级资金面分析 + LLM 生成机构级研报
    """

    def __init__(self, model_name: str = DEFAULT_MODEL, data_connector: Optional[DataConnector] = None):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)
        self.data_connector = data_connector
        self.capital_skill = capital_skill

    def analyze(self, stock_code: str, state: Optional[Dict] = None) -> str:
        logger.info(f"[CapitalAgent] 开始机构级资金面分析: {stock_code}")

        capital_data = None
        if state is not None:
            capital_data = state.get("capital_data")

        if capital_data is None and self.data_connector is not None:
            capital_data = self.data_connector.fetch_capital_data()

        if capital_data is None:
            logger.warning(f"[CapitalAgent] 无资金面数据: {stock_code}")
            return f"⚠️ {stock_code} 资金面数据不可用，无法生成分析报告"

        try:
            signals = self.capital_skill.analyze(capital_data)
        except Exception as e:
            error_msg = f"资金面指标计算失败：{str(e)}"
            logger.error(f"[CapitalAgent] {error_msg}")
            return f"⚠️ {stock_code} {error_msg}"

        prompt = f"""你是一位资深券商资金面分析师，请基于以下机构级资金流向分析数据，撰写一份专业的资金面研判报告。

股票代码：{stock_code}

【北向资金】
5日趋势：{signals.northbound.trend_5d if hasattr(signals.northbound, 'trend_5d') else 'N/A'}
10日趋势：{signals.northbound.trend_10d if hasattr(signals.northbound, 'trend_10d') else 'N/A'}
净流入：{signals.northbound.net_flow if hasattr(signals.northbound, 'net_flow') else 'N/A'}

【主力资金】
5日趋势：{signals.main_force.trend_5d if hasattr(signals.main_force, 'trend_5d') else 'N/A'}
10日趋势：{signals.main_force.trend_10d if hasattr(signals.main_force, 'trend_10d') else 'N/A'}
净流入：{signals.main_force.net_flow if hasattr(signals.main_force, 'net_flow') else 'N/A'}

【融资融券】
融资余额：{signals.margin.margin_balance if hasattr(signals.margin, 'margin_balance') else 'N/A'}
融资情绪：{signals.margin.sentiment if hasattr(signals.margin, 'sentiment') else 'N/A'}

【龙虎榜】
上榜天数：{signals.lhb.listed_days if hasattr(signals.lhb, 'listed_days') else 'N/A'}
机构净买占比：{signals.lhb.institution_ratio if hasattr(signals.lhb, 'institution_ratio') else 'N/A'}
交易风格：{signals.lhb.trade_style if hasattr(signals.lhb, 'trade_style') else 'N/A'}

【资金背离信号】
{signals.divergence_signals if hasattr(signals, 'divergence_signals') else '无'}

【资金综合评分】
评分：{signals.overall_score:.1f}/100
评级：{signals.capital_level}

请按以下结构输出报告（800-1000字）：
1. 北向资金动向（短期/中期趋势+外资态度）
2. 主力资金行为（流入流出趋势+机构动向）
3. 融资情绪分析（杠杆资金态度+风险偏好）
4. 龙虎榜属性（上榜活跃度+机构/游资占比）
5. 资金背离提示（矛盾信号+风险预警）
6. 综合资金评级与建议

要求：专业、客观、数据驱动，使用机构级术语。"""

        try:
            completion = self.client.chat.completions.create(
                model=get_model_id(self.model_name),
                messages=[
                    {"role": "system", "content": "你是一位资深券商资金面分析师，擅长资金流向分析和主力行为研判。请用专业、客观的语言撰写报告。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=LLM_TEMPERATURE,
                max_tokens=REPORT_MAX_TOKENS
            )
            report = completion.choices[0].message.content.strip()
            logger.info(f"[CapitalAgent] 机构级资金面分析完成: {stock_code}")
            return report
        except Exception as e:
            error_msg = f"生成{stock_code}资金面报告失败：{str(e)}"
            logger.error(f"[CapitalAgent] {error_msg}")
            return f"⚠️ {error_msg}"


def capital_agent_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    agent = CapitalAgent(model_name=DEFAULT_MODEL)
    capital_report = agent.analyze(stock_code, state)
    state["capital_report"] = capital_report
    return state


capital_agent = CapitalAgent
