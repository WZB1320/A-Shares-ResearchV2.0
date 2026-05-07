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
logger = logging.getLogger("IndustryAgent")

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from layers.connectors import DataConnector
from layers.skills import IndustrySkill, industry_skill

REPORT_MAX_TOKENS = 1800
LLM_TEMPERATURE = 0.3


class IndustryAgent:
    """
    行业Agent - 纯执行层（机构级）
    职责：调用 IndustrySkill 进行机构级行业分析 + LLM 生成机构级研报
    """

    def __init__(self, model_name: str = DEFAULT_MODEL, data_connector: Optional[DataConnector] = None):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)
        self.data_connector = data_connector
        self.industry_skill = industry_skill

    def analyze(self, stock_code: str, fundamental_data: Optional[Dict] = None) -> str:
        logger.info(f"[IndustryAgent] 开始机构级行业分析: {stock_code}")

        if fundamental_data is None:
            if self.data_connector is None:
                self.data_connector = DataConnector(stock_code)
            fundamental_data = self.data_connector.fetch_fundamental_data()

        signals = self.industry_skill.analyze(fundamental_data)

        prompt = f"""
# A股行业量化分析指令（券商研究所标准）
你是任职于头部券商研究所的行业首席分析师，拥有15年以上A股行业研究经验，精通波特五力模型、产业链分析、竞争格局评估等行业研究框架。

## 一、行业周期判断
- 行业名称：{signals.industry_name}
- 行业周期：{signals.industry_cycle.value}

## 二、产业链分析
- 上游压力：{signals.chain_metrics.upstream_pressure}
- 下游需求：{signals.chain_metrics.downstream_demand}
- 供给约束：{signals.chain_metrics.supply_constraint}
- 价格趋势：{signals.chain_metrics.price_trend}
- 库存水平：{signals.chain_metrics.inventory_level}

## 三、竞争格局分析
- 市场份额={signals.competitive.market_share}%，份额变化={signals.competitive.market_share_change}%
- CR3={signals.competitive.cr3}%，CR5={signals.competitive.cr5}%
- 赫芬达尔指数={signals.competitive.herfindahl_index}
- 竞争格局：{signals.competitive.competitive_pattern}
- 护城河强度：{signals.competitive.moat_strength}

## 四、政策环境评估
- 政策方向：{signals.policy.policy_direction}
- 补贴力度：{signals.policy.subsidy_level}
- 监管风险：{signals.policy.regulatory_risk}
- 贸易政策影响：{signals.policy.trade_policy_impact}
- 政策综合评分：{signals.policy.overall_policy_score}/100

## 五、行业估值
- 行业PE={signals.valuation.industry_pe}，行业PB={signals.valuation.industry_pb}，行业PS={signals.valuation.industry_ps}
- PE历史分位={signals.valuation.pe_history_percentile}%，PB历史分位={signals.valuation.pb_history_percentile}%
- 估值状态：{signals.valuation.valuation_status}

## 六、同业对比
- 相对行业PE={signals.peer_comparison.vs_industry_pe_pct}%
- 相对行业PB={signals.peer_comparison.vs_industry_pb_pct}%
- 相对行业ROE={signals.peer_comparison.vs_industry_roe_pct}%
- 相对行业增速={signals.peer_comparison.vs_industry_growth_pct}%
- 相对竞争力：{signals.peer_comparison.relative_strength}

## 七、综合评估
- 综合评分：{signals.overall_score}/100
- 赛道评级：{signals.industry_grade}
- 风险提示：{"；".join(signals.risk_warnings) if signals.risk_warnings else "无"}
- 投资建议：{signals.research_advice}

## 输出要求（机构研报标准）
1. 行业景气度分析：周期位置、景气驱动因素、前瞻指标
2. 产业链深度分析：上下游议价能力、供需格局、库存周期
3. 竞争格局评估：市场集中度、护城河、竞争态势演变
4. 政策影响量化：政策红利/风险、监管趋势、补贴可持续性
5. 行业估值对比：历史分位、同业对比、估值合理性
6. 行业机会与风险：赛道评级逻辑、核心假设、风险点
- 结合申万行业分类标准，核心数据用【】标注
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
            logger.info(f"[IndustryAgent] 机构级行业分析完成: {stock_code}")
            return report
        except Exception as e:
            error_msg = f"生成{stock_code}行业报告失败：{str(e)}"
            logger.error(f"[IndustryAgent] {error_msg}")
            return f"⚠️ {error_msg}"


def industry_agent_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    fundamental_data = state.get("fundamental_data", None)
    agent = IndustryAgent(model_name=DEFAULT_MODEL)
    industry_report = agent.analyze(stock_code, fundamental_data)
    state["industry_report"] = industry_report
    return state


industry_agent = IndustryAgent
