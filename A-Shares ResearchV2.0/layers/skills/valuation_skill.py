import sys
import logging
from pathlib import Path
from typing import Dict, Optional, List
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger("ValuationSkill")


class ValuationLevel(Enum):
    SEVERELY_UNDERVALUED = "严重低估"
    UNDERVALUED = "低估"
    FAIRLY_VALUED = "合理"
    OVERVALUED = "高估"
    SEVERELY_OVERVALUED = "严重高估"


class StockStyle(Enum):
    GROWTH = "成长股"
    VALUE = "价值股"
    CYCLICAL = "周期股"
    UNKNOWN = "未分类"


@dataclass
class AbsoluteValuation:
    pe_ttm: float
    pe_lyr: float
    pb: float
    ps: float
    ev_ebitda: float
    peg: float
    dividend_yield: float


@dataclass
class HistoricalPercentile:
    pe_3y_percentile: float
    pe_5y_percentile: float
    pe_10y_percentile: float
    pb_3y_percentile: float
    pb_5y_percentile: float
    pb_10y_percentile: float
    ps_5y_percentile: float
    historical_status: str


@dataclass
class RelativeValuation:
    vs_industry_pe_pct: float
    vs_industry_pb_pct: float
    vs_industry_ps_pct: float
    vs_historical_pe_pct: float
    vs_historical_pb_pct: float
    pe_premium_pct: float
    pb_premium_pct: float
    relative_status: str


@dataclass
class DCFMetrics:
    wacc: float
    terminal_growth: float
    projected_fcf: float
    fair_value: float
    upside_downside: float
    dcf_reliability: str


@dataclass
class ValuationSignals:
    absolute: AbsoluteValuation
    historical: HistoricalPercentile
    relative: RelativeValuation
    dcf: DCFMetrics
    stock_style: StockStyle
    overall_score: int
    valuation_level: ValuationLevel
    risk_warning: str
    research_advice: str


class ValuationSkill:
    """
    机构级估值技能层
    覆盖：绝对估值/历史分位/相对估值/DCF模型
    标准：券商研究所估值分析框架 + 绝对估值与相对估值结合
    """

    @staticmethod
    def _safe_float(value, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def analyze_absolute(valuation_data: Dict) -> AbsoluteValuation:
        pe_ttm = ValuationSkill._safe_float(valuation_data.get("pe_ttm"))
        pe_lyr = ValuationSkill._safe_float(valuation_data.get("pe_lyr"))
        pb = ValuationSkill._safe_float(valuation_data.get("pb"))
        ps = ValuationSkill._safe_float(valuation_data.get("ps"))
        ev_ebitda = ValuationSkill._safe_float(valuation_data.get("ev_ebitda"))

        growth = ValuationSkill._safe_float(valuation_data.get("profit_growth", 10))
        peg = pe_ttm / growth if growth > 0 else 999

        dividend = ValuationSkill._safe_float(valuation_data.get("dividend", 0))
        price = ValuationSkill._safe_float(valuation_data.get("current_price", 1))
        dividend_yield = (dividend / price) * 100 if price > 0 else 0

        return AbsoluteValuation(
            pe_ttm=round(pe_ttm, 2),
            pe_lyr=round(pe_lyr, 2),
            pb=round(pb, 2),
            ps=round(ps, 2),
            ev_ebitda=round(ev_ebitda, 2),
            peg=round(peg, 2),
            dividend_yield=round(dividend_yield, 2)
        )

    @staticmethod
    def analyze_historical(valuation_data: Dict) -> HistoricalPercentile:
        pe_history = valuation_data.get("pe_history", [])
        pb_history = valuation_data.get("pb_history", [])
        ps_history = valuation_data.get("ps_history", [])
        current_pe = ValuationSkill._safe_float(valuation_data.get("pe_ttm"))
        current_pb = ValuationSkill._safe_float(valuation_data.get("pb"))
        current_ps = ValuationSkill._safe_float(valuation_data.get("ps"))

        def calc_percentile(current: float, history: List[float]) -> float:
            if not history or current <= 0:
                return 50.0
            sorted_hist = sorted(history)
            count = sum(1 for h in sorted_hist if h < current)
            return (count / len(sorted_hist)) * 100

        pe_3y = calc_percentile(current_pe, pe_history[-750:]) if len(pe_history) >= 750 else calc_percentile(current_pe, pe_history)
        pe_5y = calc_percentile(current_pe, pe_history[-1250:]) if len(pe_history) >= 1250 else calc_percentile(current_pe, pe_history)
        pe_10y = calc_percentile(current_pe, pe_history)
        pb_3y = calc_percentile(current_pb, pb_history[-750:]) if len(pb_history) >= 750 else calc_percentile(current_pb, pb_history)
        pb_5y = calc_percentile(current_pb, pb_history[-1250:]) if len(pb_history) >= 1250 else calc_percentile(current_pb, pb_history)
        pb_10y = calc_percentile(current_pb, pb_history)
        ps_5y = calc_percentile(current_ps, ps_history[-1250:]) if len(ps_history) >= 1250 else calc_percentile(current_ps, ps_history)

        avg_percentile = (pe_10y + pb_10y) / 2
        if avg_percentile < 20:
            status = "历史估值低位"
        elif avg_percentile < 40:
            status = "历史估值偏低"
        elif avg_percentile < 60:
            status = "历史估值合理"
        elif avg_percentile < 80:
            status = "历史估值偏高"
        else:
            status = "历史估值高位"

        return HistoricalPercentile(
            pe_3y_percentile=round(pe_3y, 2),
            pe_5y_percentile=round(pe_5y, 2),
            pe_10y_percentile=round(pe_10y, 2),
            pb_3y_percentile=round(pb_3y, 2),
            pb_5y_percentile=round(pb_5y, 2),
            pb_10y_percentile=round(pb_10y, 2),
            ps_5y_percentile=round(ps_5y, 2),
            historical_status=status
        )

    @staticmethod
    def analyze_relative(valuation_data: Dict, fundamental_data: Dict) -> RelativeValuation:
        stock_pe = ValuationSkill._safe_float(valuation_data.get("pe_ttm"))
        stock_pb = ValuationSkill._safe_float(valuation_data.get("pb"))
        stock_ps = ValuationSkill._safe_float(valuation_data.get("ps"))
        industry_pe = ValuationSkill._safe_float(fundamental_data.get("industry_pe"))
        industry_pb = ValuationSkill._safe_float(fundamental_data.get("industry_pb"))
        industry_ps = ValuationSkill._safe_float(fundamental_data.get("industry_ps"))

        pe_history = valuation_data.get("pe_history", [])
        pb_history = valuation_data.get("pb_history", [])
        avg_pe_hist = sum(pe_history) / len(pe_history) if pe_history else stock_pe
        avg_pb_hist = sum(pb_history) / len(pb_history) if pb_history else stock_pb

        vs_ind_pe = (stock_pe / industry_pe * 100) if industry_pe > 0 else 100
        vs_ind_pb = (stock_pb / industry_pb * 100) if industry_pb > 0 else 100
        vs_ind_ps = (stock_ps / industry_ps * 100) if industry_ps > 0 else 100
        vs_hist_pe = (stock_pe / avg_pe_hist * 100) if avg_pe_hist > 0 else 100
        vs_hist_pb = (stock_pb / avg_pb_hist * 100) if avg_pb_hist > 0 else 100

        pe_premium = vs_ind_pe - 100
        pb_premium = vs_ind_pb - 100

        if vs_ind_pe < 80 and vs_hist_pe < 80:
            status = "相对低估"
        elif vs_ind_pe > 120 and vs_hist_pe > 120:
            status = "相对高估"
        else:
            status = "估值匹配"

        return RelativeValuation(
            vs_industry_pe_pct=round(vs_ind_pe, 2),
            vs_industry_pb_pct=round(vs_ind_pb, 2),
            vs_industry_ps_pct=round(vs_ind_ps, 2),
            vs_historical_pe_pct=round(vs_hist_pe, 2),
            vs_historical_pb_pct=round(vs_hist_pb, 2),
            pe_premium_pct=round(pe_premium, 2),
            pb_premium_pct=round(pb_premium, 2),
            relative_status=status
        )

    @staticmethod
    def analyze_dcf(valuation_data: Dict, fundamental_data: Dict) -> DCFMetrics:
        fcf = ValuationSkill._safe_float(fundamental_data.get("自由现金流", 0))
        growth_5y = ValuationSkill._safe_float(fundamental_data.get("revenue_growth", 10))
        terminal_growth = min(growth_5y / 2, 3.0)
        wacc = 8.0
        shares = ValuationSkill._safe_float(fundamental_data.get("总股本", 1))
        current_price = ValuationSkill._safe_float(valuation_data.get("current_price", 1))

        if fcf <= 0 or shares <= 0:
            return DCFMetrics(wacc, terminal_growth, fcf, 0, 0, "FCF为负，DCF不适用")

        pv_fcf = 0
        for year in range(1, 6):
            future_fcf = fcf * ((1 + growth_5y/100) ** year)
            pv_fcf += future_fcf / ((1 + wacc/100) ** year)

        terminal_value = (fcf * ((1 + growth_5y/100) ** 5) * (1 + terminal_growth/100)) / \
                         ((wacc/100) - (terminal_growth/100))
        pv_terminal = terminal_value / ((1 + wacc/100) ** 5)

        enterprise_value = pv_fcf + pv_terminal
        fair_value_per_share = enterprise_value / shares

        upside = ((fair_value_per_share - current_price) / current_price * 100) if current_price > 0 else 0

        if growth_5y > 20:
            reliability = "高成长假设，DCF可靠性一般"
        elif growth_5y > 10:
            reliability = "中等成长假设，DCF可靠性良好"
        else:
            reliability = "低成长假设，DCF可靠性高"

        return DCFMetrics(
            wacc=wacc,
            terminal_growth=round(terminal_growth, 2),
            projected_fcf=round(fcf, 2),
            fair_value=round(fair_value_per_share, 2),
            upside_downside=round(upside, 2),
            dcf_reliability=reliability
        )

    @staticmethod
    def classify_stock_style(valuation_data: Dict, fundamental_data: Dict) -> StockStyle:
        pe_ttm = ValuationSkill._safe_float(valuation_data.get("pe_ttm"))
        pb = ValuationSkill._safe_float(valuation_data.get("pb"))
        growth = ValuationSkill._safe_float(valuation_data.get("profit_growth", 10))
        roe = ValuationSkill._safe_float(fundamental_data.get("roe", fundamental_data.get("净资产收益率", 0)))
        dividend = ValuationSkill._safe_float(valuation_data.get("dividend", 0))
        price = ValuationSkill._safe_float(valuation_data.get("current_price", 1))
        dividend_yield = (dividend / price) * 100 if price > 0 else 0
        beta = ValuationSkill._safe_float(fundamental_data.get("beta", 1.0))
        industry_cyclical = fundamental_data.get("is_cyclical", False)

        if industry_cyclical or (beta > 1.3 and pb < 1.5):
            return StockStyle.CYCLICAL

        if growth > 20 and pe_ttm > 20 and roe > 15:
            return StockStyle.GROWTH

        if dividend_yield > 2.5 and pb < 3 and roe > 8:
            return StockStyle.VALUE

        if growth > 15:
            return StockStyle.GROWTH
        elif dividend_yield > 2:
            return StockStyle.VALUE
        else:
            return StockStyle.UNKNOWN

    @staticmethod
    def calculate_score(absolute: AbsoluteValuation, historical: HistoricalPercentile,
                        relative: RelativeValuation, dcf: DCFMetrics,
                        stock_style: StockStyle = StockStyle.UNKNOWN) -> tuple:
        score = 50

        if absolute.pe_ttm > 0 and absolute.pe_ttm < 15:
            score += 15
        elif absolute.pe_ttm > 0 and absolute.pe_ttm < 25:
            score += 10
        elif absolute.pe_ttm > 60:
            score -= 20
        elif absolute.pe_ttm > 100:
            score -= 30
        elif absolute.pe_ttm <= 0:
            score -= 10

        if absolute.pb < 1.5:
            score += 10
        elif absolute.pb > 10:
            score -= 15

        if stock_style == StockStyle.GROWTH:
            if absolute.peg > 0 and absolute.peg < 1.5:
                score += 10
            elif absolute.peg > 3:
                score -= 15
        elif stock_style == StockStyle.VALUE:
            if absolute.peg > 0 and absolute.peg < 1.0:
                score += 10
            elif absolute.peg > 2:
                score -= 15
        else:
            if absolute.peg > 0 and absolute.peg < 1:
                score += 10
            elif absolute.peg > 2:
                score -= 10

        if historical.pe_10y_percentile < 20:
            score += 15
        elif historical.pe_10y_percentile > 80:
            score -= 15

        if relative.vs_industry_pe_pct < 80:
            score += 10
        elif relative.vs_industry_pe_pct > 120:
            score -= 10

        if dcf.upside_downside > 30:
            score += 10
        elif dcf.upside_downside < -30:
            score -= 10

        if absolute.dividend_yield > 3:
            score += 5

        score = max(0, min(100, score))

        if score >= 80:
            level = ValuationLevel.SEVERELY_UNDERVALUED
        elif score >= 65:
            level = ValuationLevel.UNDERVALUED
        elif score >= 45:
            level = ValuationLevel.FAIRLY_VALUED
        elif score >= 30:
            level = ValuationLevel.OVERVALUED
        else:
            level = ValuationLevel.SEVERELY_OVERVALUED

        return score, level

    @staticmethod
    def analyze(valuation_data: Dict, fundamental_data: Dict) -> ValuationSignals:
        logger.info("[ValuationSkill] 开始机构级估值分析")

        absolute = ValuationSkill.analyze_absolute(valuation_data)
        historical = ValuationSkill.analyze_historical(valuation_data)
        relative = ValuationSkill.analyze_relative(valuation_data, fundamental_data)
        dcf = ValuationSkill.analyze_dcf(valuation_data, fundamental_data)
        stock_style = ValuationSkill.classify_stock_style(valuation_data, fundamental_data)

        score, level = ValuationSkill.calculate_score(absolute, historical, relative, dcf, stock_style)

        risk_parts = []
        if absolute.pe_ttm <= 0:
            risk_parts.append("亏损状态，PE失效，需关注PB/PS")
        if historical.pe_10y_percentile > 90:
            risk_parts.append("PE处于10年历史90%分位以上，高估值泡沫风险")
        if historical.pe_10y_percentile > 80 and historical.pe_3y_percentile > 80:
            risk_parts.append("PE在3年和10年均处于高位，估值泡沫信号强烈")
        if relative.pe_premium_pct > 50:
            risk_parts.append(f"PE相对行业溢价{relative.pe_premium_pct:.0f}%，显著高估")
        if dcf.upside_downside < -50:
            risk_parts.append("DCF显示大幅高估")
        if historical.pe_10y_percentile < 10 and absolute.pe_ttm > 0:
            risk_parts.append("PE处于10年历史10%分位以下，需警惕低估值陷阱（基本面恶化）")
        if absolute.pb < 1 and absolute.pe_ttm > 50:
            risk_parts.append("PB破净但PE高企，可能存在资产质量风险")
        if stock_style == StockStyle.GROWTH and absolute.peg > 3:
            risk_parts.append("成长股PEG>3，估值严重透支未来增长")
        if stock_style == StockStyle.VALUE and absolute.peg > 2:
            risk_parts.append("价值股PEG>2，估值偏高")
        if not risk_parts:
            risk_parts.append("估值风险可控")

        risk = "；".join(risk_parts)

        advice_parts = []
        if level in [ValuationLevel.UNDERVALUED, ValuationLevel.SEVERELY_UNDERVALUED]:
            advice_parts.append("估值具备安全边际")
        if historical.historical_status in ["历史估值低位", "历史估值偏低"]:
            advice_parts.append("处于历史估值低位")
        if relative.relative_status == "相对低估":
            advice_parts.append("相对行业低估")
        if dcf.upside_downside > 20:
            advice_parts.append(f"DCF隐含{dcf.upside_downside:.1f}%上行空间")
        if absolute.dividend_yield > 3:
            advice_parts.append(f"股息率{absolute.dividend_yield:.1f}%，具备防御价值")
        if stock_style == StockStyle.GROWTH and absolute.peg < 1.5:
            advice_parts.append("成长股PEG合理，成长性未被充分定价")
        if stock_style == StockStyle.VALUE and absolute.peg < 1.0:
            advice_parts.append("价值股PEG合理，估值具备吸引力")
        if not advice_parts:
            advice_parts.append("估值处于合理区间")

        advice = " | ".join(advice_parts)
        logger.info(f"[ValuationSkill] 分析完成 | 股票风格: {stock_style.value} | 估值水平: {level.value} | 评分: {score}")

        return ValuationSignals(
            absolute=absolute,
            historical=historical,
            relative=relative,
            dcf=dcf,
            stock_style=stock_style,
            overall_score=score,
            valuation_level=level,
            risk_warning=risk,
            research_advice=advice
        )


valuation_skill = ValuationSkill()
