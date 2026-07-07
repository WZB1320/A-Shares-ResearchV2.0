import sys
import logging
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger("ValuationAgent")

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from layers.connectors import DataConnector
from layers.skills.valuation_skill import ValuationSkill, valuation_skill
from layers.agents.report_schema import parse_json_report, error_report, unavailable_report
from layers.agents.base_agent import QUANT_ANCHOR_RULE

REPORT_MAX_TOKENS = 1600
LLM_TEMPERATURE = 0.0


class ValuationAgent:

    def __init__(self, model_name: str = DEFAULT_MODEL, data_connector: Optional[DataConnector] = None):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)
        self.data_connector = data_connector
        self.valuation_skill = valuation_skill

    def analyze(self, stock_code: str, state: Optional[Dict] = None) -> Dict:
        logger.info(f"[ValuationAgent] 开始估值分析: {stock_code}")

        valuation_data = None
        fundamental_data = None
        quality_context = None
        if state is not None:
            valuation_data = state.get("valuation_data")
            fundamental_data = state.get("fundamental_data") or state.get("financial_data")
            quality_context = state.get("quality_context")

        if valuation_data is None and self.data_connector is not None:
            valuation_data = self.data_connector.fetch_valuation_data()
        if fundamental_data is None and self.data_connector is not None:
            fundamental_data = self.data_connector.fetch_fundamental_data()

        if valuation_data is None:
            logger.warning(f"[ValuationAgent] 无估值数据: {stock_code}")
            return unavailable_report("valuation").to_dict()

        try:
            signals = self.valuation_skill.analyze(valuation_data, fundamental_data)
        except Exception as e:
            error_msg = f"估值指标计算失败：{str(e)}"
            logger.error(f"[ValuationAgent] {error_msg}")
            return error_report("valuation", error_msg).to_dict()

        prompt = f"""你是一位资深券商估值分析师，请基于以下估值分析数据，输出一份结构化的估值研判报告。

{quality_context or ''}

{self._build_data_context(stock_code, signals, fundamental_data)}

请严格按以下JSON格式输出（不要包含任何其他文字，只输出JSON）：
{{
  "dimension": "valuation",
  "overall_score": 45,
  "grade": "看空",
  "confidence": 60,
  "thesis": "一句话核心判断",
  "key_signals": ["信号1", "信号2"],
  "risk_factors": ["风险1", "风险2"],
  "recommendation": "操作建议",
  "supporting_data": {{
    "pe_assessment": {{"level": "低估/合理/偏高/高估", "percentile": 70}},
    "pb_assessment": {{"level": "低估/合理/偏高/高估", "percentile": 65}},
    "pe_pb_score": {{
      "pe_score": "低估/合理偏低/合理/合理偏高/高估",
      "pb_score": "低估/合理偏低/合理/合理偏高/高估",
      "methodology": "PE_PB历史分位法（行业自适应阈值）",
      "industry_adjusted": true
    }}
  }}
}}

分析推理步骤（必须按此顺序分步思考）：
Step 1: 确认行业类别，读取行业自适应阈值（PE/PB低估/高估阈值）
Step 2: 评估PE分位数，对比行业阈值判断PE估值是低估/合理/高估
Step 3: 评估PB分位数，对比行业阈值判断PB估值是低估/合理/高估
Step 4: 综合PE和PB的判断，结合行业特性，给出最终估值评分

要求：
1. overall_score 必须是0-100的整数（估值评分，分数越高越便宜即估值越有吸引力）
2. grade 必须是以下之一：强烈看多/看多/中性偏多/中性/中性偏空/看空/强烈看空
3. confidence 必须是0-100的整数，反映数据充分程度
4. key_signals 列出2-3个关键估值信号
5. risk_factors 列出2-3个估值风险
6. 必须结合行业对标数据来评估：当前PE/PB分位 vs 该行业自适应阈值做判断
7. 如果PE分位虽低但属低PE行业，需在thesis中指出行业特性
8. pe_pb_score中industry_adjusted为true说明用了行业自适应分位阈值
9. pe_score/pb_score对比分位数与行业阈值(低估/高估)做判定，非绝对值判断
10. DCF估值因缺少数据不进行，不凭空估算
11. 只输出JSON，不要输出任何其他内容
12. **重要**：负值（如PE为负、PB极低）是有效数据点，表示公司亏损或市场极度悲观，不是"数据缺失"。只有当数据明确标注为"不可用"或"None"时，才能提及"数据缺失"。
13. **PE Bands 定价**：如果提供了PE Bands三线，必须基于当前股价相对Bands位置判断估值水平——跌破下轨=极度低估，位于下轨-中轨=偏低，位于中轨-上轨=合理，远超上轨=严重高估。thesis中必须明确指出"当前股价位于PE Bands的XX位置"。
14. **涨幅归因**：如果提供了PE贡献和EPS贡献，必须在thesis中明确说明"涨的是PE还是EPS"，这是判断估值可持续性的核心——PE扩张驱动的涨幅不可持续，EPS增长驱动的涨幅才健康。
{QUANT_ANCHOR_RULE}"""

        try:
            completion = self.client.chat.completions.create(
                model=get_model_id(self.model_name),
                messages=[
                    {"role": "system", "content": "你是一位资深券商估值分析师。请严格输出JSON格式的结构化分析结果，不要输出任何其他内容。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=LLM_TEMPERATURE,
                max_tokens=REPORT_MAX_TOKENS
            )
            raw = completion.choices[0].message.content.strip()
            report = parse_json_report(raw, "valuation")
            logger.info(f"[ValuationAgent] 结构化估值分析完成: {stock_code} | score={report.overall_score} grade={report.grade}")
            return report.to_dict()
        except Exception as e:
            error_msg = f"LLM调用失败：{str(e)}"
            logger.error(f"[ValuationAgent] {error_msg}")
            return error_report("valuation", error_msg).to_dict()

    @staticmethod
    def _build_data_context(stock_code: str, signals, fundamental_data=None) -> str:
        industry_label = ValuationSkill.CATEGORY_NAMES.get(signals.metrics.industry_category, "通用")

        # 提取基本面摘要
        fund_summary = ""
        if fundamental_data and isinstance(fundamental_data, dict):
            valuation = fundamental_data.get("valuation", {})
            finance_list = fundamental_data.get("finance", [])
            roe = valuation.get("净资产收益率", 0)
            gross_margin = 0
            if finance_list and isinstance(finance_list[-1], dict):
                gross_margin = finance_list[-1].get("gross_margin_ttm", 0)
            fund_summary = f"""
【基本面摘要】
ROE：{roe}%
毛利率：{gross_margin}%
"""

        # PE Bands 三线定价
        pe_bands_section = ""
        m = signals.metrics
        if m.pe_band_mid is not None and m.price is not None and m.pe_ttm is not None and m.pe_ttm > 0:
            # 判断当前股价位置
            if m.price > m.pe_band_upper_price:
                pos = f"远超上轨（高估，距上轨+{(m.price/m.pe_band_upper_price-1)*100:.1f}%）"
            elif m.price > m.pe_band_mid_price:
                pos = f"位于中轨-上轨（合理偏高）"
            elif m.price > m.pe_band_lower_price:
                pos = f"位于下轨-中轨（合理偏低）"
            else:
                pos = f"跌破下轨（低估，距下轨{(m.price/m.pe_band_lower_price-1)*100:.1f}%）"
            pe_bands_section = f"""
【PE Bands 三线定价】
下轨(25%分位): {m.pe_band_lower:.1f}x → {m.pe_band_lower_price:.2f}元
中轨(50%分位): {m.pe_band_mid:.1f}x → {m.pe_band_mid_price:.2f}元
上轨(75%分位): {m.pe_band_upper:.1f}x → {m.pe_band_upper_price:.2f}元
当前股价: {m.price:.2f}元 (PE={m.pe_ttm:.1f}x)
位置判断: {pos}
"""

        # PB Bands 三线定价
        pb_bands_section = ""
        if m.pb_band_mid is not None and m.price is not None and m.pb is not None and m.pb > 0:
            pb_bands_section = f"""
【PB Bands 三线定价】
下轨(25%分位): {m.pb_band_lower:.2f}x → {m.pb_band_lower_price:.2f}元
中轨(50%分位): {m.pb_band_mid:.2f}x → {m.pb_band_mid_price:.2f}元
上轨(75%分位): {m.pb_band_upper:.2f}x → {m.pb_band_upper_price:.2f}元
当前PB: {m.pb:.2f}x
"""

        # 涨幅归因（PE vs EPS）
        attribution_section = ""
        if m.attribution_note:
            attribution_section = f"""
【涨幅归因（1年）】
总涨幅: {m.price_return_1y:+.1f}%
PE贡献: {m.pe_return_1y:+.1f}%
EPS贡献: {m.eps_return_1y:+.1f}%
归因结论: {m.attribution_note}
"""

        return f"""股票代码：{stock_code}

【估值指标】
当前股价：{signals.metrics.price if signals.metrics.price is not None else 'N/A'}
PE_TTM：{signals.metrics.pe_ttm if signals.metrics.pe_ttm is not None else 'N/A'}
PB：{signals.metrics.pb if signals.metrics.pb is not None else 'N/A'}
PE历史分位数：{signals.metrics.pe_percentile if signals.metrics.pe_percentile is not None else 'N/A'}%
PB历史分位数：{signals.metrics.pb_percentile if signals.metrics.pb_percentile is not None else 'N/A'}%
PE十年均值：{signals.metrics.pe_10_avg if signals.metrics.pe_10_avg is not None else 'N/A'}
{fund_summary}{pe_bands_section}{pb_bands_section}{attribution_section}
【行业对标信息】
行业类别：{industry_label}
PE低估阈值(分位)：<{signals.metrics.percentile_pe_low_threshold:.0f}%
PE高估阈值(分位)：>{signals.metrics.percentile_pe_high_threshold:.0f}%
PB低估阈值(分位)：<{signals.metrics.percentile_pb_low_threshold:.0f}%
PB高估阈值(分位)：>{signals.metrics.percentile_pb_high_threshold:.0f}%

【行业分位法判定规则】
- 非银/周期行业PE/PB低估阈值更严格(<20%才算低估)
- 科技成长PE/PB高估阈值更宽容(>80%才算高估)
- 稳定消费/公用事业PE/PB使用标准阈值(<30%低估, >70%高估)
- 金融/银行需重点关注PB估值，PE参考价值相对有限

【数据可用性】
已获取数据：{', '.join(signals.data_available_fields) if signals.data_available_fields else '无'}
缺失数据：{', '.join(signals.data_unavailable_fields) if signals.data_unavailable_fields else '无'}

【估值综合评分】
评分：{signals.overall_score}/100
评级：{signals.valuation_grade}"""


def valuation_agent_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    agent = ValuationAgent(model_name=DEFAULT_MODEL)
    valuation_report = agent.analyze(stock_code, state)
    state["valuation_report"] = valuation_report
    return state


valuation_agent = ValuationAgent