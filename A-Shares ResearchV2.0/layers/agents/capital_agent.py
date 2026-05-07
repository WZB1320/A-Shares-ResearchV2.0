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
logger = logging.getLogger("CapitalAgent")

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from layers.connectors import DataConnector
from layers.skills import CapitalSkill, capital_skill

REPORT_MAX_TOKENS = 1800
LLM_TEMPERATURE = 0.3


class CapitalAgent:
    """
    资金面Agent - 纯执行层（机构级）
    职责：调用 CapitalSkill 进行机构级资金面分析 + LLM 生成机构级研报
    """

    def __init__(self, model_name: str = DEFAULT_MODEL, data_connector: Optional[DataConnector] = None):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)
        self.data_connector = data_connector
        self.capital_skill = capital_skill

    def analyze(self, stock_code: str, capital_data: Optional[Dict] = None) -> str:
        logger.info(f"[CapitalAgent] 开始机构级资金面分析: {stock_code}")

        if capital_data is None:
            if self.data_connector is None:
                self.data_connector = DataConnector(stock_code)
            capital_data = self.data_connector.fetch_capital_data()

        signals = self.capital_skill.analyze(capital_data)

        prompt = f"""
# A股资金面量化分析指令（券商研究所标准）
你是任职于头部券商研究所的量化策略分析师，拥有12年以上A股资金流向研究经验，精通北向资金、主力资金、融资融券、龙虎榜等多维度资金分析框架。

## 一、北向资金分析（外资动向）
- 当日净流入={signals.north.net_inflow}万元，净流入占比={signals.north.net_inflow_pct}%
- 5日累计={signals.north.cumulative_5d}万元，10日累计={signals.north.cumulative_10d}万元，30日累计={signals.north.cumulative_30d}万元
- 5日趋势：{signals.north.trend_5d}，10日趋势：{signals.north.trend_10d}，30日趋势：{signals.north.trend_30d}
- 信号：{signals.north.signal}，一致性={signals.north.consistency}%

## 二、主力资金分析（机构动向）
- 当日净流入={signals.main.net_inflow}万元，净流入占比={signals.main.net_inflow_pct}%
- 大单占比={signals.main.large_order_ratio}%，中单占比={signals.main.medium_order_ratio}%，小单占比={signals.main.small_order_ratio}%
- 5日累计={signals.main.cumulative_5d}万元，10日累计={signals.main.cumulative_10d}万元
- 5日趋势：{signals.main.trend_5d}，10日趋势：{signals.main.trend_10d}
- 信号：{signals.main.signal}，机构意图：{signals.main.institutional_intent}

## 三、融资融券分析（杠杆资金）
- 融资余额={signals.margin.margin_balance}万元，变化={signals.margin.margin_change_pct}%
- 融券余额={signals.margin.short_balance}万元，变化={signals.margin.short_change_pct}%
- 净杠杆={signals.margin.net_leverage}
- 杠杆信号：{signals.margin.leverage_signal}，风险等级：{signals.margin.risk_level}

## 四、龙虎榜分析（游资动向）
- 30日上榜次数={signals.dragon.active_days_30d}
- 机构买入={signals.dragon.institutional_buy_ratio}万元，机构卖出={signals.dragon.institutional_sell_ratio}万元
- 机构净流入={signals.dragon.net_institutional_flow}万元
- TOP5席位占比={signals.dragon.top5_seat_ratio}%
- 信号：{signals.dragon.signal}，游资痕迹：{signals.dragon.hot_money_trace}

## 五、资金共识度分析
- 北向趋势：{signals.flow_structure.north_trend.value}
- 主力趋势：{signals.flow_structure.main_trend.value}
- 融资趋势：{signals.flow_structure.margin_trend.value}
- 龙虎趋势：{signals.flow_structure.dragon_trend.value}
- 共识度={signals.flow_structure.consensus_level}
- 分歧预警：{signals.flow_structure.divergence_warning}

## 六、综合评估
- 综合评分：{signals.overall_score}/100
- 资金评级：{signals.capital_grade}
- 风险信号：{signals.risk_signal}
- 投资建议：{signals.research_advice}

## 输出要求（机构研报标准）
1. 北向资金深度分析：外资持仓变化、汇率影响、全球配置逻辑
2. 主力资金行为分析：大单/中单/小单结构、机构建仓/派发信号
3. 融资融券风险评估：杠杆水平、融资买入意愿、平仓风险
4. 龙虎榜游资追踪：游资介入强度、机构席位博弈、短期情绪判断
5. 资金共识度研判：四维资金一致性分析、分歧/共识的技术含义
6. 资金面交易策略：基于资金行为的买入/持有/观望建议
- 核心数据用【】标注，结合A股特色制度（T+1、涨跌停）分析
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
            logger.info(f"[CapitalAgent] 机构级资金面分析完成: {stock_code}")
            return report
        except Exception as e:
            error_msg = f"生成{stock_code}资金面报告失败：{str(e)}"
            logger.error(f"[CapitalAgent] {error_msg}")
            return f"⚠️ {error_msg}"


def capital_agent_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    capital_data = state.get("capital_data", None)
    agent = CapitalAgent(model_name=DEFAULT_MODEL)
    capital_report = agent.analyze(stock_code, capital_data)
    state["capital_report"] = capital_report
    return state


capital_agent = CapitalAgent
