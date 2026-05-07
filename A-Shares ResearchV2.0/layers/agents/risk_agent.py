import sys
import logging
from pathlib import Path
from typing import Dict, Optional, List

sys.path.append(str(Path(__file__).parent.parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="[%(name)s] %(asctime)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("RiskAgent")

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from layers.connectors import DataConnector
from layers.skills import RiskSkill, risk_skill

REPORT_MAX_TOKENS = 1800
LLM_TEMPERATURE = 0.3


class RiskAgent:
    """
    风险Agent - 纯执行层（机构级）
    职责：调用 RiskSkill 进行机构级风险分析 + LLM 生成机构级研报
    """

    def __init__(self, model_name: str = DEFAULT_MODEL, data_connector: Optional[DataConnector] = None):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)
        self.data_connector = data_connector
        self.risk_skill = risk_skill

    def analyze(self, stock_code: str, financial_data: Optional[Dict] = None, tech_data: Optional[List[Dict]] = None) -> str:
        logger.info(f"[RiskAgent] 开始机构级风险分析: {stock_code}")

        if financial_data is None:
            if self.data_connector is None:
                self.data_connector = DataConnector(stock_code)
            financial_data = self.data_connector.fetch_financial_data()

        signals = self.risk_skill.analyze(financial_data, tech_data)

        prompt = f"""
# A股风险量化分析指令（券商研究所标准）
你是任职于头部券商研究所的风险管理首席分析师，拥有15年以上A股风险研究经验，精通股权质押、财务风险、经营风险、市场风险、公司治理等多维度风险评估框架。

## 一、股权质押风险
- 质押比例={signals.pledge.pledge_ratio}%，质押次数={signals.pledge.pledge_count}
- 大股东质押比例={signals.pledge.major_holder_pledge}%
- 平仓线={signals.pledge.liquidation_line}
- 接近平仓：{"是" if signals.pledge.close_to_liquidation else "否"}
- 风险等级：{signals.pledge.risk_level.value}

## 二、大股东减持风险
- 近期减持比例={signals.reduction.recent_reduce_ratio}%
- 减持次数={signals.reduction.reduce_count}，减持金额={signals.reduction.reduce_amount}万元
- 减持参与方={signals.reduction.reduce_participants}家
- 持续减持：{"是" if signals.reduction.continuous_reduction else "否"}
- 风险等级：{signals.reduction.risk_level.value}

## 三、财务风险
- 资产负债率={signals.financial.debt_to_asset}%
- 流动比率={signals.financial.current_ratio}，速动比率={signals.financial.quick_ratio}
- 利息保障倍数={signals.financial.interest_coverage}
- 现金流缺口：{"是" if signals.financial.cash_flow_shortfall else "否"}
- 连续亏损年数={signals.financial.consecutive_losses}
- 审计意见：{signals.financial.audit_opinion}
- 风险等级：{signals.financial.risk_level.value}

## 四、经营风险
- 收入集中度={signals.operational.revenue_concentration}%
- 客户集中度={signals.operational.customer_concentration}%
- 供应商集中度={signals.operational.supplier_concentration}%
- 存货周转下降：{"是" if signals.operational.inventory_turnover_decline else "否"}
- 应收周转下降：{"是" if signals.operational.receivable_turnover_decline else "否"}
- 风险等级：{signals.operational.risk_level.value}

## 五、市场风险
- Beta={signals.market.beta}，30日波动率={signals.market.volatility_30d}%
- 1年最大回撤={signals.market.max_drawdown_1y}%
- 流动性评分={signals.market.liquidity_score}
- 尾部风险：{signals.market.tail_risk}
- 风险等级：{signals.market.risk_level.value}

## 六、公司治理风险
- 关联交易={signals.governance.related_party_transactions}%
- 对外担保比例={signals.governance.guarantee_ratio}%
- 高管变动次数={signals.governance.executive_turnover}
- 诉讼数量={signals.governance.lawsuit_count}
- 监管处罚次数={signals.governance.regulatory_penalties}
- 风险等级：{signals.governance.risk_level.value}

## 七、综合评估
- 综合风险评分：{signals.overall_risk_score}/100
- 综合风险等级：{signals.overall_risk_level.value}
- 风险警示：{"；".join(signals.warnings) if signals.warnings else "无明显风险"}
- 投资建议：{signals.research_advice}

## 输出要求（机构研报标准）
1. 股权质押风险评估：平仓线距离、大股东质押动机、风险传导路径
2. 减持风险分析：减持节奏、市场冲击、股东结构变化
3. 财务风险识别：偿债能力、盈利质量、审计风险
4. 经营风险量化：集中度风险、周转恶化、供应链稳定性
5. 市场风险测算：Beta敏感性、波动率预测、流动性风险
6. 公司治理审查：关联交易、担保风险、高管稳定性、合规风险
7. 风险综合判断：风险矩阵、缓释措施、投资建议
- 量化风险发生概率与影响幅度，核心数据用【】标注
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
            logger.info(f"[RiskAgent] 机构级风险分析完成: {stock_code}")
            return report
        except Exception as e:
            error_msg = f"生成{stock_code}风险报告失败：{str(e)}"
            logger.error(f"[RiskAgent] {error_msg}")
            return f"⚠️ {error_msg}"


def risk_agent_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    financial_data = state.get("financial_data", None)
    tech_data = state.get("tech_data", None)
    agent = RiskAgent(model_name=DEFAULT_MODEL)
    risk_report = agent.analyze(stock_code, financial_data, tech_data)
    state["risk_report"] = risk_report
    return state


risk_agent = RiskAgent
