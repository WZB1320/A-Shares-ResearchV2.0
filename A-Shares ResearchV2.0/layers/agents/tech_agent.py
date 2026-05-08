import sys
import logging
from pathlib import Path
from typing import Dict, Optional, List

logger = logging.getLogger("TechAgent")

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from layers.connectors import DataConnector
from layers.skills.tech_skill import TechSkill, tech_skill

REPORT_MAX_TOKENS = 1800
LLM_TEMPERATURE = 0.3


class TechAgent:
    """
    技术面Agent - 纯执行层（机构级标准化）
    职责：调用 TechSkill 进行机构级量价技术分析 + LLM 生成机构级研报
    """

    def __init__(self, model_name: str = DEFAULT_MODEL, data_connector: Optional[DataConnector] = None):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)
        self.data_connector = data_connector
        self.tech_skill = tech_skill

    def analyze(self, stock_code: str, state: Optional[Dict] = None) -> str:
        logger.info(f"[TechAgent] 开始机构级技术面分析: {stock_code}")

        tech_data = None
        if state is not None:
            tech_data = state.get("tech_data")

        if tech_data is None and self.data_connector is not None:
            tech_data = self.data_connector.fetch_tech_data()

        if tech_data is None:
            logger.warning(f"[TechAgent] 无技术面数据: {stock_code}")
            return f"⚠️ {stock_code} 技术面数据不可用，无法生成分析报告"

        try:
            signals = self.tech_skill.analyze(tech_data)
        except Exception as e:
            error_msg = f"技术面指标计算失败：{str(e)}"
            logger.error(f"[TechAgent] {error_msg}")
            return f"⚠️ {stock_code} {error_msg}"

        prompt = f"""你是一位资深券商技术分析师，请基于以下机构级量价技术分析数据，撰写一份专业的技术面研判报告。

股票代码：{stock_code}

【均线系统】
多头排列：{signals.ma_system.arrangement}
金叉信号：{signals.ma_system.golden_cross}
死叉信号：{signals.ma_system.dead_cross}
MA5斜率：{signals.ma_system.ma5_slope:.2f}
MA20斜率：{signals.ma_system.ma20_slope:.2f}

【MACD指标】
DIF：{signals.macd.dif:.4f}
DEA：{signals.macd.dea:.4f}
MACD柱：{signals.macd.histogram:.4f}
MACD背离：{signals.macd.divergence if hasattr(signals.macd, 'divergence') else '无'}

【RSI指标】
RSI(6)：{signals.rsi.rsi6:.1f}
RSI(14)：{signals.rsi.rsi14:.1f}
RSI背离：{signals.rsi.divergence if hasattr(signals.rsi, 'divergence') else '无'}

【量价分析】
量价形态：{signals.volume_price.pattern if hasattr(signals.volume_price, 'pattern') else '无'}
放量信号：{signals.volume_price.volume_surge if hasattr(signals.volume_price, 'volume_surge') else '无'}

【趋势强度】
趋势评分：{signals.trend_score if hasattr(signals, 'trend_score') else 'N/A'}
市场状态：{signals.market_regime if hasattr(signals, 'market_regime') else 'N/A'}

【技术综合评分】
评分：{signals.overall_score:.1f}/100
评级：{signals.tech_level}

请按以下结构输出报告（800-1000字）：
1. 趋势研判（均线结构+趋势强度+市场状态）
2. 动能分析（MACD+RSI+背离信号）
3. 量价配合（量价形态+资金验证）
4. 关键位分析（支撑/压力位）
5. 综合评分与操作建议

要求：专业、客观、数据驱动，使用机构级术语。"""

        try:
            completion = self.client.chat.completions.create(
                model=get_model_id(self.model_name),
                messages=[
                    {"role": "system", "content": "你是一位资深券商技术分析师，擅长量价技术分析和趋势研判。请用专业、客观的语言撰写报告。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=LLM_TEMPERATURE,
                max_tokens=REPORT_MAX_TOKENS
            )
            report = completion.choices[0].message.content.strip()
            logger.info(f"[TechAgent] 机构级技术面分析完成: {stock_code}")
            return report
        except Exception as e:
            error_msg = f"生成{stock_code}技术面报告失败：{str(e)}"
            logger.error(f"[TechAgent] {error_msg}")
            return f"⚠️ {error_msg}"


def tech_agent_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    agent = TechAgent(model_name=DEFAULT_MODEL)
    tech_report = agent.analyze(stock_code, state)
    state["tech_report"] = tech_report
    return state


tech_agent = TechAgent
