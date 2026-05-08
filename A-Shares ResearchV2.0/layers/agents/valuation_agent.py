import sys
import logging
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger("ValuationAgent")

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from layers.connectors import DataConnector
from layers.skills.valuation_skill import ValuationSkill, valuation_skill

REPORT_MAX_TOKENS = 1800
LLM_TEMPERATURE = 0.3


class ValuationAgent:
    """
    估值面Agent - 纯执行层（机构级标准化）
    职责：调用 ValuationSkill 进行机构级估值分析 + LLM 生成机构级研报
    """

    def __init__(self, model_name: str = DEFAULT_MODEL, data_connector: Optional[DataConnector] = None):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)
        self.data_connector = data_connector
        self.valuation_skill = valuation_skill

    def analyze(self, stock_code: str, state: Optional[Dict] = None) -> str:
        logger.info(f"[ValuationAgent] 开始机构级估值分析: {stock_code}")

        valuation_data = None
        fundamental_data = None
        if state is not None:
            valuation_data = state.get("valuation_data")
            fundamental_data = state.get("fundamental_data") or state.get("financial_data")

        if valuation_data is None and self.data_connector is not None:
            valuation_data = self.data_connector.fetch_valuation_data()
        if fundamental_data is None and self.data_connector is not None:
            fundamental_data = self.data_connector.fetch_fundamental_data()

        if valuation_data is None:
            logger.warning(f"[ValuationAgent] 无估值数据: {stock_code}")
            return f"⚠️ {stock_code} 估值数据不可用，无法生成分析报告"

        try:
            signals = self.valuation_skill.analyze(valuation_data, fundamental_data)
        except Exception as e:
            error_msg = f"估值指标计算失败：{str(e)}"
            logger.error(f"[ValuationAgent] {error_msg}")
            return f"⚠️ {stock_code} {error_msg}"

        prompt = f"""你是一位资深券商估值分析师，请基于以下机构级估值分析数据，撰写一份专业的估值研判报告。

股票代码：{stock_code}

【绝对估值】
PE(TTM)：{signals.absolute.pe if hasattr(signals.absolute, 'pe') else 'N/A'}
PB：{signals.absolute.pb if hasattr(signals.absolute, 'pb') else 'N/A'}
PS：{signals.absolute.ps if hasattr(signals.absolute, 'ps') else 'N/A'}
EV/EBITDA：{signals.absolute.ev_ebitda if hasattr(signals.absolute, 'ev_ebitda') else 'N/A'}
PEG：{signals.absolute.peg if hasattr(signals.absolute, 'peg') else 'N/A'}

【历史分位（10年维度）】
PE 10年分位：{signals.percentile.pe_10y_percentile if hasattr(signals.percentile, 'pe_10y_percentile') else 'N/A'}%
PB 10年分位：{signals.percentile.pb_10y_percentile if hasattr(signals.percentile, 'pb_10y_percentile') else 'N/A'}%
PE 5年分位：{signals.percentile.pe_5y_percentile if hasattr(signals.percentile, 'pe_5y_percentile') else 'N/A'}%
PE 3年分位：{signals.percentile.pe_3y_percentile if hasattr(signals.percentile, 'pe_3y_percentile') else 'N/A'}%

【相对估值】
PE相对行业溢价率：{signals.relative.pe_premium if hasattr(signals.relative, 'pe_premium') else 'N/A'}%
PB相对行业溢价率：{signals.relative.pb_premium if hasattr(signals.relative, 'pb_premium') else 'N/A'}%

【风险预警】
{signals.risk_warning if hasattr(signals, 'risk_warning') else '无'}

【机构建议】
{signals.research_advice if hasattr(signals, 'research_advice') else '无'}

【估值综合评分】
评分：{signals.overall_score:.1f}/100
评级：{signals.valuation_level}

请按以下结构输出报告（800-1000字）：
1. 绝对估值评估（PE/PB/PS/EV-EBITDA/PEG多维度）
2. 历史分位分析（以10年分位为核心，辅以3年/5年分位）
3. 相对估值对比（PE/PB相对行业溢价率）
4. 股票风格适配（成长/价值/周期股PEG规则）
5. 风险预警（高估值泡沫/低估值陷阱识别）
6. 综合估值评级与配置建议

要求：专业、客观、数据驱动，使用机构级术语。"""

        try:
            completion = self.client.chat.completions.create(
                model=get_model_id(self.model_name),
                messages=[
                    {"role": "system", "content": "你是一位资深券商估值分析师，擅长多维度估值分析和历史分位研判。请用专业、客观的语言撰写报告。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=LLM_TEMPERATURE,
                max_tokens=REPORT_MAX_TOKENS
            )
            report = completion.choices[0].message.content.strip()
            logger.info(f"[ValuationAgent] 机构级估值分析完成: {stock_code}")
            return report
        except Exception as e:
            error_msg = f"生成{stock_code}估值报告失败：{str(e)}"
            logger.error(f"[ValuationAgent] {error_msg}")
            return f"⚠️ {error_msg}"


def valuation_agent_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    agent = ValuationAgent(model_name=DEFAULT_MODEL)
    valuation_report = agent.analyze(stock_code, state)
    state["valuation_report"] = valuation_report
    return state


valuation_agent = ValuationAgent
