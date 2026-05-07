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
logger = logging.getLogger("TechAgent")

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from layers.connectors import DataConnector
from layers.skills import TechSkill, tech_skill

REPORT_MAX_TOKENS = 1800
LLM_TEMPERATURE = 0.3


class TechAgent:
    """
    技术面Agent - 纯执行层（机构级）
    职责：调用 TechSkill 进行机构级技术分析 + LLM 生成机构级研报
    """

    def __init__(self, model_name: str = DEFAULT_MODEL, data_connector: Optional[DataConnector] = None):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)
        self.data_connector = data_connector
        self.tech_skill = tech_skill

    def analyze(self, stock_code: str, tech_data: Optional[List[Dict]] = None) -> str:
        logger.info(f"[TechAgent] 开始机构级技术面分析: {stock_code}")

        if tech_data is None:
            if self.data_connector is None:
                self.data_connector = DataConnector(stock_code)
            tech_df = self.data_connector.fetch_tech_data()
            tech_data = tech_df.to_dict("records") if hasattr(tech_df, "to_dict") else tech_data

        signals = self.tech_skill.analyze(tech_data)

        latest = tech_data[-1] if tech_data else {}
        data_str = f"最新数据点：{latest}\n近30天数据摘要：{len(tech_data) if tech_data else 0}条记录"

        prompt = f"""
# A股技术面量化分析指令（券商研究所标准）
你是任职于头部券商研究所的量化策略首席分析师，拥有15年以上A股量化投研经验。请基于{stock_code}的技术面量化数据，输出机构级技术分析报告。

## 一、均线系统分析（量化信号）
- MA5={signals.ma_system.ma5}, MA10={signals.ma_system.ma10}, MA20={signals.ma_system.ma20}
- MA60={signals.ma_system.ma60}, MA120={signals.ma_system.ma120}, MA250={signals.ma_system.ma250}
- 短期斜率：MA5={signals.ma_system.ma5_slope}%, MA10={signals.ma_system.ma10_slope}%
- 中期斜率：MA20={signals.ma_system.ma20_slope}%, MA60={signals.ma_system.ma60_slope}%
- 均线排列：{signals.ma_system.arrangement}
- 金叉信号：{"、".join(signals.ma_system.golden_cross) if signals.ma_system.golden_cross else "无"}
- 死叉信号：{"、".join(signals.ma_system.dead_cross) if signals.ma_system.dead_cross else "无"}

## 二、MACD动能分析
- DIF={signals.macd_system.dif}, DEA={signals.macd_system.dea}, MACD柱状图={signals.macd_system.macd_hist}
- 柱状图趋势：{signals.macd_system.hist_trend}
- 金叉状态：{"是" if signals.macd_system.golden_cross else "否"}
- 死叉状态：{"是" if signals.macd_system.dead_cross else "否"}
- 背离信号：{signals.macd_system.divergence}
- 动能判断：{signals.macd_system.momentum}

## 三、KDJ超买超卖
- K={signals.kdj_system.k}, D={signals.kdj_system.d}, J={signals.kdj_system.j}
- 超买：{"是" if signals.kdj_system.overbought else "否"}，超卖：{"是" if signals.kdj_system.oversold else "否"}
- 金叉：{"是" if signals.kdj_system.golden_cross else "否"}，死叉：{"是" if signals.kdj_system.dead_cross else "否"}

## 四、布林带分析
- 上轨={signals.bollinger_system.upper}, 中轨={signals.bollinger_system.middle}, 下轨={signals.bollinger_system.lower}
- 带宽={signals.bollinger_system.bandwidth}%，当前位置：{signals.bollinger_system.position}
- 带宽收缩：{"是" if signals.bollinger_system.squeeze else "否"}
- 突破信号：{signals.bollinger_system.breakout}

## 五、量价结构分析
- 量比={signals.volume_structure.volume_ratio}，换手率={signals.volume_structure.turnover_rate}%
- 量能分位={signals.volume_structure.volume_percentile}%
- 上涨/下跌量比={signals.volume_structure.up_volume_ratio}/{signals.volume_structure.down_volume_ratio}
- 机构信号：{signals.volume_structure.institutional_signal}
- 量价信号：{signals.volume_structure.signal.value}
- 量能趋势：{signals.volume_structure.volume_trend}

## 六、支撑阻力分析
- 强支撑={signals.support_resistance.strong_support}, 支撑1={signals.support_resistance.support_1}, 支撑2={signals.support_resistance.support_2}
- 阻力1={signals.support_resistance.resistance_1}, 阻力2={signals.support_resistance.resistance_2}, 强阻力={signals.support_resistance.strong_resistance}
- 当前位置：{signals.support_resistance.current_position}
- 突破概率：{signals.support_resistance.break_probability}%

## 七、综合信号
- 趋势强度：{signals.trend_strength.value}
- 综合评分：{signals.overall_score}/100
- 短期信号：{signals.short_term_signal}
- 中期信号：{signals.medium_term_signal}
- 风险预警：{signals.risk_warning}
- 投资建议：{signals.research_advice}

## 基础数据
{data_str}

## 输出要求（机构研报标准）
1. 趋势体系分析：基于均线系统判断短中长期趋势方向与强度
2. 动能与背离分析：MACD动能变化、背离信号的技术含义
3. 超买超卖与波动：KDJ位置、布林带突破/收缩的技术意义
4. 量价行为分析：机构吸筹/派发信号、量价背离识别
5. 关键价位分析：支撑阻力位、突破概率的技术判断
6. 交易策略建议：明确的买入/持有/观望/减仓建议，标注目标价区间与止损位
- 每个维度单独成段，小标题加粗，核心数据用【】标注
- 拒绝模糊表述，所有结论必须绑定具体数值
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
            logger.info(f"[TechAgent] 机构级技术面分析完成: {stock_code}")
            return report
        except Exception as e:
            error_msg = f"生成{stock_code}技术面报告失败：{str(e)}"
            logger.error(f"[TechAgent] {error_msg}")
            return f"⚠️ {error_msg}"


def tech_agent_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    tech_data = state.get("tech_data", None)
    agent = TechAgent(model_name=DEFAULT_MODEL)
    tech_report = agent.analyze(stock_code, tech_data)
    state["tech_report"] = tech_report
    return state


tech_agent = TechAgent
