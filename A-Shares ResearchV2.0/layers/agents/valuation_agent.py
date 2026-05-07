import sys
import logging
from pathlib import Path
from typing import Dict, Optional

sys.path.append(str(Path(__file__).parent.parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="[%(name)s] %(asctime)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("ValuationAgent")

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from layers.connectors import DataConnector
from layers.skills import ValuationSkill, valuation_skill

REPORT_MAX_TOKENS = 1800
LLM_TEMPERATURE = 0.3


class ValuationAgent:
    """
    估值Agent - 纯执行层（机构级）
    职责：调用 ValuationSkill 进行机构级估值分析 + LLM 生成机构级研报
    """

    def __init__(self, model_name: str = DEFAULT_MODEL, data_connector: Optional[DataConnector] = None):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)
        self.data_connector = data_connector
        self.valuation_skill = valuation_skill

    def analyze(self, stock_code: str, valuation_data: Optional[Dict] = None, fundamental_data: Optional[Dict] = None) -> str:
        logger.info(f"[ValuationAgent] 开始机构级估值分析: {stock_code}")

        if valuation_data is None or fundamental_data is None:
            if self.data_connector is None:
                self.data_connector = DataConnector(stock_code)
            if valuation_data is None:
                valuation_data = self.data_connector.fetch_valuation_data()
            if fundamental_data is None:
                fundamental_data = self.data_connector.fetch_fundamental_data()

        signals = self.valuation_skill.analyze(valuation_data, fundamental_data)

        prompt = f"""
# A股估值量化分析指令（券商研究所标准）
你是任职于头部券商研究所的估值首席分析师，拥有15年以上A股估值研究经验，精通DCF模型、相对估值法、绝对估值法等多种估值框架。

## 一、绝对估值指标
- PE_TTM={signals.absolute.pe_ttm}，PE_LYR={signals.absolute.pe_lyr}
- PB={signals.absolute.pb}，PS={signals.absolute.ps}
- EV/EBITDA={signals.absolute.ev_ebitda}
- PEG={signals.absolute.peg}
- 股息率={signals.absolute.dividend_yield}%

## 二、历史估值分位
- PE 5年分位={signals.historical.pe_5y_percentile}%，PE 10年分位={signals.historical.pe_10y_percentile}%
- PB 5年分位={signals.historical.pb_5y_percentile}%，PB 10年分位={signals.historical.pb_10y_percentile}%
- PS 5年分位={signals.historical.ps_5y_percentile}%
- 历史估值状态：{signals.historical.historical_status}

## 三、相对估值
- 相对行业PE={signals.relative.vs_industry_pe_pct}%
- 相对行业PB={signals.relative.vs_industry_pb_pct}%
- 相对行业PS={signals.relative.vs_industry_ps_pct}%
- 相对历史PE={signals.relative.vs_historical_pe_pct}%
- 相对历史PB={signals.relative.vs_historical_pb_pct}%
- 相对估值状态：{signals.relative.relative_status}

## 四、DCF估值模型
- WACC={signals.dcf.wacc}%，永续增长率={signals.dcf.terminal_growth}%
- 预测期FCF={signals.dcf.projected_fcf}
- 每股合理价值={signals.dcf.fair_value}
- 隐含上行/下行空间={signals.dcf.upside_downside}%
- DCF可靠性：{signals.dcf.dcf_reliability}

## 五、综合评估
- 综合评分：{signals.overall_score}/100
- 估值水平：{signals.valuation_level.value}
- 风险预警：{signals.risk_warning}
- 投资建议：{signals.research_advice}

## 输出要求（机构研报标准）
1. 绝对估值分析：PE/PB/PS/EV-EBITDA的绝对水平与行业对比
2. 历史估值定位：5年/10年分位的投资含义、均值回归概率
3. 相对估值判断：vs行业/vs历史的溢价/折价逻辑
4. DCF模型深度：WACC假设、永续增长率敏感性、合理价值区间
5. 估值安全边际：下行风险测算、极端情景估值
6. 估值投资建议：基于多维度估值的综合判断、目标价区间
- 核心数据用【】标注，解释估值分位的投资含义
- 总字数1200-1800字，纯文本输出
"""
        try:
            completion = self.client.chat.completions.create(
                model=get_model_id(self.model_name),
                messages=[{"role": "user", "content": prompt}],
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
    valuation_data = state.get("valuation_data", None)
    fundamental_data = state.get("fundamental_data", None)
    agent = ValuationAgent(model_name=DEFAULT_MODEL)
    valuation_report = agent.analyze(stock_code, valuation_data, fundamental_data)
    state["valuation_report"] = valuation_report
    return state


valuation_agent = ValuationAgent
