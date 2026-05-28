import sys
import logging
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger("FundAgent")

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from layers.connectors import DataConnector
from layers.skills.fund_skill import FundSkill, fund_skill
from layers.agents.report_schema import parse_json_report, error_report, unavailable_report

REPORT_MAX_TOKENS = 1200
LLM_TEMPERATURE = 0.0


class FundAgent:

    def __init__(self, model_name: str = DEFAULT_MODEL, data_connector: Optional[DataConnector] = None):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)
        self.data_connector = data_connector
        self.fund_skill = fund_skill

    def analyze(self, stock_code: str, state: Optional[Dict] = None) -> Dict:
        logger.info(f"[FundAgent] 开始机构级基本面分析: {stock_code}")

        fundamental_data = None
        quality_context = None
        if state is not None:
            fundamental_data = state.get("fundamental_data") or state.get("financial_data")
            quality_context = state.get("quality_context")

        if fundamental_data is None and self.data_connector is not None:
            fundamental_data = self.data_connector.fetch_fundamental_data()

        if fundamental_data is None:
            logger.warning(f"[FundAgent] 无基本面数据: {stock_code}")
            return unavailable_report("fund").to_dict()

        try:
            signals = self.fund_skill.analyze(fundamental_data)
        except Exception as e:
            error_msg = f"基本面指标计算失败：{str(e)}"
            logger.error(f"[FundAgent] {error_msg}")
            return error_report("fund", error_msg).to_dict()

        prompt = f"""你是一位资深券商基本面分析师，请基于以下财务分析数据，输出一份结构化的基本面研判报告。

{quality_context or ''}

{self._build_data_context(stock_code, signals)}

请严格按以下JSON格式输出（不要包含任何其他文字，只输出JSON）：
{{
  "dimension": "fund",
  "overall_score": 78,
  "grade": "看多",
  "confidence": 80,
  "thesis": "一句话核心判断",
  "key_signals": ["信号1", "信号2", "信号3"],
  "risk_factors": ["风险1", "风险2"],
  "recommendation": "操作建议",
  "supporting_data": {{
    "profitability": {{"quality": "高/中/低", "roe_level": "优秀/良好/一般/差"}},
    "growth": {{"stage": "高速增长/稳健增长/低速/衰退", "sustainability": "高/中/低"}},
    "balance": {{"leverage": "偏低/适中/偏高/危险", "liquidity": "充裕/正常/偏紧"}},
    "cashflow": {{"quality": "健康/一般/恶化", "ocf_match": "匹配/偏离"}}
  }}
}}

分析推理步骤（必须按此顺序分步思考）：
Step 1: 评估盈利能力，基于ROE、净利率、ROA，判断盈利质量处于行业什么水平
Step 2: 评估成长能力，基于营收增速、利润增速，判断增长的可持续性
Step 3: 评估资产负债健康度，基于资产负债率、流动比率，判断财务安全性
Step 4: 评估现金流质量，基于经营现金流/净利润，判断盈利是否真金白银
Step 5: 综合以上四步骤，加权得出综合评分和投资评级

要求：
1. 必须完成上述五步推理再填入JSON
2. overall_score 必须是0-100的整数
3. grade 必须是以下之一：强烈看多/看多/中性偏多/中性/中性偏空/看空/强烈看空
4. confidence 必须是0-100的整数
5. key_signals 列出3-5个关键基本面信号
6. risk_factors 列出2-3个关键财务风险
7. 仅基于提供的数据做判断，不编造数据
8. 只输出JSON，不要输出任何其他内容"""

        try:
            completion = self.client.chat.completions.create(
                model=get_model_id(self.model_name),
                messages=[
                    {"role": "system", "content": "你是一位资深券商基本面分析师。请严格输出JSON格式的结构化分析结果，不要输出任何其他内容。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=LLM_TEMPERATURE,
                max_tokens=REPORT_MAX_TOKENS
            )
            raw = completion.choices[0].message.content.strip()
            report = parse_json_report(raw, "fund")
            logger.info(f"[FundAgent] 结构化基本面分析完成: {stock_code} | score={report.overall_score} grade={report.grade}")
            return report.to_dict()
        except Exception as e:
            error_msg = f"LLM调用失败：{str(e)}"
            logger.error(f"[FundAgent] {error_msg}")
            return error_report("fund", error_msg).to_dict()

    @staticmethod
    def _fmt(value, suffix="", precision=2):
        """格式化数值，0 或 None 标记为数据缺失"""
        if value is None or value == 0:
            return "数据缺失"
        fmt_str = f"{{:.{precision}f}}"
        return f"{fmt_str.format(value)}{suffix}"

    @staticmethod
    def _build_data_context(stock_code: str, signals) -> str:
        f = FundAgent._fmt
        # 统计缺失维度数
        metrics = [
            signals.profitability.roe_ttm,
            signals.profitability.net_margin,
            signals.profitability.gross_margin,
            signals.growth.revenue_growth_yoy,
            signals.growth.profit_growth_yoy,
            signals.balance_sheet.debt_to_asset,
            signals.balance_sheet.current_ratio,
            signals.cash_flow.ocf_to_net_profit,
        ]
        missing_count = sum(1 for v in metrics if v is None or v == 0)
        data_note = ""
        if missing_count > 0:
            data_note = (f"\n⚠ 数据覆盖度警告：{len(metrics)}项核心指标中{missing_count}项数据缺失。"
                         f"请仅基于有数据的指标做判断，缺失指标不要参与评分，"
                         f"不要将「数据缺失」曲解为「盈利能力差」。")

        return f"""股票代码：{stock_code}{data_note}

【盈利能力】
ROE：{f(signals.profitability.roe_ttm, '%')}
净利率：{f(signals.profitability.net_margin, '%')}
毛利率：{f(signals.profitability.gross_margin, '%')}
ROA：{f(signals.profitability.roa_ttm, '%')}
盈利质量：{signals.profitability.profit_quality.value if signals.profitability.roe_ttm else '数据不足无法评估'}

【杜邦分析】
ROE驱动因素：{signals.profitability.dupont.roe_contribution if signals.profitability.roe_ttm else '数据缺失'}
净利率：{f(signals.profitability.dupont.net_margin, '%') if signals.profitability.dupont.net_margin else '数据缺失'}
资产周转率：{signals.profitability.dupont.asset_turnover if signals.profitability.dupont.asset_turnover else '数据缺失'}
权益乘数：{signals.profitability.dupont.equity_multiplier if signals.profitability.dupont.equity_multiplier else '数据缺失'}

【成长能力】
营收同比增速：{f(signals.growth.revenue_growth_yoy, '%')}
利润同比增速：{f(signals.growth.profit_growth_yoy, '%')}
扣非增速：{f(signals.growth.deducted_profit_growth, '%')}
成长阶段：{signals.growth.growth_stage}

【资产负债】
资产负债率：{f(signals.balance_sheet.debt_to_asset, '%')}
流动比率：{f(signals.balance_sheet.current_ratio)}
速动比率：{f(signals.balance_sheet.quick_ratio)}
资产质量：{signals.balance_sheet.asset_quality}

【盈利质量】
经营现金流/净利润：{f(signals.cash_flow.ocf_to_net_profit)}
现金流质量：{signals.cash_flow.cash_quality}

【基本面综合评分】
评分：{signals.overall_score}/100
评级：{signals.investment_grade}"""


def fund_agent_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    agent = FundAgent(model_name=DEFAULT_MODEL)
    fund_report = agent.analyze(stock_code, state)
    state["fund_report"] = fund_report
    return state


fund_agent = FundAgent