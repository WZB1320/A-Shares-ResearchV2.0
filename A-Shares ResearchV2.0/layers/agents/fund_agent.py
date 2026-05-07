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
logger = logging.getLogger("FundAgent")

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from layers.connectors import DataConnector
from layers.skills import FundSkill, fund_skill

REPORT_MAX_TOKENS = 2000
LLM_TEMPERATURE = 0.3


class FundAgent:
    """
    基本面Agent - 纯执行层（机构级）
    职责：调用 FundSkill 进行机构级基本面分析 + LLM 生成机构级研报
    """

    def __init__(self, model_name: str = DEFAULT_MODEL, data_connector: Optional[DataConnector] = None):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)
        self.data_connector = data_connector
        self.fund_skill = fund_skill

    def analyze(self, stock_code: str, fundamental_data: Optional[Dict] = None) -> str:
        logger.info(f"[FundAgent] 开始机构级基本面分析: {stock_code}")

        if fundamental_data is None:
            if self.data_connector is None:
                self.data_connector = DataConnector(stock_code)
            fundamental_data = self.data_connector.fetch_fundamental_data()

        financial_data = fundamental_data.get("financial_data", fundamental_data)
        signals = self.fund_skill.analyze(financial_data)

        prompt = f"""
# A股基本面量化分析指令（券商研究所标准）
你是任职于头部券商研究所的行业首席分析师，拥有15年以上A股基本面研究经验，精通CFA财务报表分析框架与杜邦分析体系。请基于{stock_code}的财务数据，输出机构级基本面分析报告。

## 一、盈利能力分析（评分：{signals.profitability.score}/100）
- ROE_TTM={signals.profitability.roe_ttm}%，ROA_TTM={signals.profitability.roa_ttm}%
- 毛利率={signals.profitability.gross_margin}%，净利率={signals.profitability.net_margin}%
- 营业利润率={signals.profitability.operating_margin}%，EBITDA利润率={signals.profitability.ebitda_margin}%
- 盈利质量：{signals.profitability.profit_quality.value}

## 二、杜邦分析（ROE拆解）
- 净利润率={signals.profitability.dupont.net_margin}%
- 资产周转率={signals.profitability.dupont.asset_turnover}
- 权益乘数={signals.profitability.dupont.equity_multiplier}
- ROE={signals.profitability.dupont.roe}%
- 驱动因素：{signals.profitability.dupont.roe_contribution}

## 三、资产负债结构（评分：{signals.balance_sheet.score}/100）
- 流动比率={signals.balance_sheet.current_ratio}，速动比率={signals.balance_sheet.quick_ratio}
- 资产负债率={signals.balance_sheet.debt_to_asset}%，产权比率={signals.balance_sheet.debt_to_equity}
- 利息保障倍数={signals.balance_sheet.interest_coverage}
- 现金比率={signals.balance_sheet.cash_ratio}，营运资本={signals.balance_sheet.working_capital}
- 资产质量：{signals.balance_sheet.asset_quality}

## 四、现金流质量（评分：{signals.cash_flow.score}/100）
- 经营现金流/净利润={signals.cash_flow.ocf_to_net_profit}
- 自由现金流/净利润={signals.cash_flow.fcf_to_net_profit}
- 经营现金流覆盖率={signals.cash_flow.ocf_coverage_ratio}
- 资本支出/经营现金流={signals.cash_flow.capex_to_ocf}
- 分红率={signals.cash_flow.dividend_payout}%
- 现金质量：{signals.cash_flow.cash_quality}

## 五、成长能力（评分：{signals.growth.score}/100）
- 营收CAGR(3年)={signals.growth.revenue_cagr_3y}%
- 利润CAGR(3年)={signals.growth.profit_cagr_3y}%
- ROE_CAGR(3年)={signals.growth.roe_cagr_3y}%
- 营收加速：{"是" if signals.growth.revenue_acceleration else "否"}
- 盈利可持续性：{signals.growth.profit_sustainability}
- 成长阶段：{signals.growth.growth_stage}

## 六、营运效率（评分：{signals.operation.score}/100）
- 存货周转率={signals.operation.inventory_turnover}
- 应收周转率={signals.operation.receivable_turnover}
- 应付周转率={signals.operation.payable_turnover}
- 现金周期={signals.operation.cash_cycle}天
- 总资产周转率={signals.operation.asset_turnover}
- 效率水平：{signals.operation.efficiency_level}

## 七、综合评估
- 综合评分：{signals.overall_score}/100
- 投资评级：{signals.investment_grade}
- 风险提示：{"；".join(signals.risk_warnings) if signals.risk_warnings else "无"}
- 投资建议：{signals.research_advice}

## 输出要求（机构研报标准）
1. 盈利能力深度拆解：结合杜邦分析拆解ROE驱动因素，判断盈利质量与可持续性
2. 资产负债表健康度：偿债能力、资本结构、资产质量量化评估
3. 现金流质量分析：利润含金量、自由现金流、资本支出合理性
4. 成长能力量化建模：成长阶段判断、增速可持续性、加速/减速信号
5. 营运效率对标：周转率行业对比、现金周期优化空间
6. 基本面综合判断：投资评级逻辑、核心假设与风险点
- 每个维度附量化指标+行业分位，核心数据用【】标注
- 总字数1500-2000字，纯文本输出
"""
        try:
            completion = self.client.chat.completions.create(
                model=get_model_id(self.model_name),
                messages=[{"role": "user", "content": prompt}],
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
    fundamental_data = state.get("fundamental_data", None)
    agent = FundAgent(model_name=DEFAULT_MODEL)
    fund_report = agent.analyze(stock_code, fundamental_data)
    state["fund_report"] = fund_report
    return state


fund_agent = FundAgent
