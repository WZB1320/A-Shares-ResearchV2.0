import sys
import logging
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger("IndustryAgent")

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from layers.connectors import DataConnector
from layers.skills.industry_skill import IndustrySkill, industry_skill

REPORT_MAX_TOKENS = 1800
LLM_TEMPERATURE = 0.3


class IndustryAgent:
    """
    行业面Agent - 纯执行层（机构级标准化）
    职责：调用 IndustrySkill 进行机构级行业分析 + LLM 生成机构级研报
    """

    def __init__(self, model_name: str = DEFAULT_MODEL, data_connector: Optional[DataConnector] = None):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)
        self.data_connector = data_connector
        self.industry_skill = industry_skill

    def analyze(self, stock_code: str, state: Optional[Dict] = None) -> str:
        logger.info(f"[IndustryAgent] 开始机构级行业分析: {stock_code}")

        fundamental_data = None
        if state is not None:
            fundamental_data = state.get("fundamental_data") or state.get("financial_data")

        if fundamental_data is None and self.data_connector is not None:
            fundamental_data = self.data_connector.fetch_fundamental_data()

        if fundamental_data is None:
            logger.warning(f"[IndustryAgent] 无行业数据: {stock_code}")
            return f"⚠️ {stock_code} 行业数据不可用，无法生成分析报告"

        try:
            signals = self.industry_skill.analyze(fundamental_data)
        except Exception as e:
            error_msg = f"行业指标计算失败：{str(e)}"
            logger.error(f"[IndustryAgent] {error_msg}")
            return f"⚠️ {stock_code} {error_msg}"

        prompt = f"""你是一位资深券商行业分析师，请基于以下机构级行业分析数据，撰写一份专业的行业研判报告。

股票代码：{stock_code}

【行业定位】
所属行业：{signals.industry_name if hasattr(signals, 'industry_name') else 'N/A'}
行业地位：{signals.industry_position if hasattr(signals, 'industry_position') else 'N/A'}

【行业景气度】
景气评分：{signals.prosperity_score if hasattr(signals, 'prosperity_score') else 'N/A'}
景气趋势：{signals.prosperity_trend if hasattr(signals, 'prosperity_trend') else 'N/A'}

【竞争格局】
市场集中度：{signals.concentration if hasattr(signals, 'concentration') else 'N/A'}
竞争壁垒：{signals.competitive_moat if hasattr(signals, 'competitive_moat') else 'N/A'}

【行业估值】
行业PE：{signals.industry_pe if hasattr(signals, 'industry_pe') else 'N/A'}
行业PB：{signals.industry_pb if hasattr(signals, 'industry_pb') else 'N/A'}

【行业综合评分】
评分：{signals.overall_score:.1f}/100
评级：{signals.industry_level}

请按以下结构输出报告（800-1000字）：
1. 行业景气度研判（周期位置+景气趋势）
2. 竞争格局分析（集中度+壁垒+公司地位）
3. 行业估值对比（PE/PB相对行业水平）
4. 政策与催化剂（政策环境+行业事件）
5. 综合评分与配置建议

要求：专业、客观、数据驱动，使用机构级术语。"""

        try:
            completion = self.client.chat.completions.create(
                model=get_model_id(self.model_name),
                messages=[
                    {"role": "system", "content": "你是一位资深券商行业分析师，擅长行业研究和竞争格局分析。请用专业、客观的语言撰写报告。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=LLM_TEMPERATURE,
                max_tokens=REPORT_MAX_TOKENS
            )
            report = completion.choices[0].message.content.strip()
            logger.info(f"[IndustryAgent] 机构级行业分析完成: {stock_code}")
            return report
        except Exception as e:
            error_msg = f"生成{stock_code}行业报告失败：{str(e)}"
            logger.error(f"[IndustryAgent] {error_msg}")
            return f"⚠️ {error_msg}"


def industry_agent_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    agent = IndustryAgent(model_name=DEFAULT_MODEL)
    industry_report = agent.analyze(stock_code, state)
    state["industry_report"] = industry_report
    return state


industry_agent = IndustryAgent
