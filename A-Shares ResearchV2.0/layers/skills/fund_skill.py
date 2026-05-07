import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum

sys.path.append(str(Path(__file__).parent.parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="[%(name)s] %(asctime)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("FundSkill")


class ProfitQuality(Enum):
    EXCELLENT = "优秀"
    GOOD = "良好"
    FAIR = "一般"
    POOR = "较差"
    DANGER = "危险"


@dataclass
class DupontAnalysis:
    net_margin: float
    asset_turnover: float
    equity_multiplier: float
    roe: float
    roe_contribution: str


@dataclass
class ProfitabilityMetrics:
    roe_ttm: float
    roa_ttm: float
    gross_margin: float
    net_margin: float
    operating_margin: float
    ebitda_margin: float
    dupont: DupontAnalysis
    profit_quality: ProfitQuality
    score: int


@dataclass
class BalanceSheetStructure:
    current_ratio: float
    quick_ratio: float
    debt_to_asset: float
    debt_to_equity: float
    interest_coverage: float
    cash_ratio: float
    working_capital: float
    asset_quality: str
    score: int


@dataclass
class CashFlowMetrics:
    ocf_to_net_profit: float
    fcf_to_net_profit: float
    ocf_coverage_ratio: float
    capex_to_ocf: float
    dividend_payout: float
    cash_quality: str
    score: int


@dataclass
class GrowthMetrics:
    revenue_cagr_3y: float
    profit_cagr_3y: float
    roe_cagr_3y: float
    revenue_acceleration: bool
    profit_sustainability: str
    growth_stage: str
    score: int


@dataclass
class OperationalMetrics:
    inventory_turnover: float
    receivable_turnover: float
    payable_turnover: float
    cash_cycle: float
    asset_turnover: float
    efficiency_level: str
    score: int


@dataclass
class FundSignals:
    profitability: ProfitabilityMetrics
    balance_sheet: BalanceSheetStructure
    cash_flow: CashFlowMetrics
    growth: GrowthMetrics
    operation: OperationalMetrics
    overall_score: int
    investment_grade: str
    research_advice: str
    risk_warnings: List[str]


class FundSkill:
    """
    机构级基本面技能层
    覆盖：杜邦分析/现金流质量/营运效率/成长持续性/资产负债结构
    标准：券商研究所财务分析框架 + CFA 财务报表分析标准
    """

    @staticmethod
    def _safe_float(value, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def analyze_dupont(net_profit: float, revenue: float, total_assets: float, equity: float) -> DupontAnalysis:
        if revenue <= 0 or total_assets <= 0 or equity <= 0:
            return DupontAnalysis(0, 0, 0, 0, "数据异常")

        net_margin = net_profit / revenue
        asset_turnover = revenue / total_assets
        equity_multiplier = total_assets / equity
        roe = net_margin * asset_turnover * equity_multiplier

        contributions = []
        if net_margin > 0.15:
            contributions.append("高盈利驱动")
        elif asset_turnover > 1.0:
            contributions.append("高周转驱动")
        elif equity_multiplier > 3.0:
            contributions.append("高杠杆驱动")
        else:
            contributions.append("均衡驱动")

        return DupontAnalysis(
            net_margin=round(net_margin * 100, 2),
            asset_turnover=round(asset_turnover, 2),
            equity_multiplier=round(equity_multiplier, 2),
            roe=round(roe * 100, 2),
            roe_contribution=" | ".join(contributions)
        )

    @staticmethod
    def analyze_profitability(financial_data: Dict) -> ProfitabilityMetrics:
        roe = FundSkill._safe_float(financial_data.get("roe", financial_data.get("净资产收益率", 0)))
        roa = FundSkill._safe_float(financial_data.get("roa", financial_data.get("总资产收益率", 0)))
        gross = FundSkill._safe_float(financial_data.get("gross_profit_margin", financial_data.get("毛利率", 0)))
        net = FundSkill._safe_float(financial_data.get("net_profit_margin", financial_data.get("净利率", 0)))
        operating = FundSkill._safe_float(financial_data.get("operating_margin", 0))
        ebitda = FundSkill._safe_float(financial_data.get("ebitda_margin", 0))

        net_profit = FundSkill._safe_float(financial_data.get("net_profit", financial_data.get("净利润", 0)))
        revenue = FundSkill._safe_float(financial_data.get("revenue", financial_data.get("营业收入", 0)))
        total_assets = FundSkill._safe_float(financial_data.get("total_assets", financial_data.get("总资产", 0)))
        equity = FundSkill._safe_float(financial_data.get("equity", financial_data.get("股东权益", 0)))

        dupont = FundSkill.analyze_dupont(net_profit, revenue, total_assets, equity)

        # 如果未直接提供ROE/ROA，使用杜邦分析计算值
        if roe <= 0 and dupont.roe > 0:
            roe = dupont.roe
        if roa <= 0 and total_assets > 0:
            roa = (net_profit / total_assets) * 100
        if gross <= 0 and revenue > 0:
            cogs = FundSkill._safe_float(financial_data.get("cogs", financial_data.get("营业成本", 0)))
            gross = ((revenue - cogs) / revenue) * 100
        if net <= 0 and revenue > 0:
            net = (net_profit / revenue) * 100

        score = 50
        if roe > 20:
            score += 20
        elif roe > 15:
            score += 15
        elif roe > 10:
            score += 10
        elif roe > 5:
            score += 5
        elif roe > 0:
            score += 0
        else:
            score -= 20

        if gross > 40:
            score += 10
        elif gross > 25:
            score += 5
        elif gross > 10:
            score += 0
        else:
            score -= 10

        if net > 15:
            score += 10
        elif net > 8:
            score += 5
        elif net > 0:
            score += 0
        else:
            score -= 15

        if roa > 10:
            score += 5
        elif roa > 5:
            score += 3

        if dupont.roe_contribution == "高杠杆驱动":
            score -= 5

        score = max(0, min(100, score))

        if score >= 80:
            quality = ProfitQuality.EXCELLENT
        elif score >= 65:
            quality = ProfitQuality.GOOD
        elif score >= 50:
            quality = ProfitQuality.FAIR
        elif score >= 30:
            quality = ProfitQuality.POOR
        else:
            quality = ProfitQuality.DANGER

        return ProfitabilityMetrics(
            roe_ttm=round(roe, 2),
            roa_ttm=round(roa, 2),
            gross_margin=round(gross, 2),
            net_margin=round(net, 2),
            operating_margin=round(operating, 2),
            ebitda_margin=round(ebitda, 2),
            dupont=dupont,
            profit_quality=quality,
            score=score
        )

    @staticmethod
    def analyze_balance_sheet(financial_data: Dict) -> BalanceSheetStructure:
        current_assets = FundSkill._safe_float(financial_data.get("current_assets", financial_data.get("流动资产", 0)))
        current_liabilities = FundSkill._safe_float(financial_data.get("current_liabilities", financial_data.get("流动负债", 0)))
        total_assets = FundSkill._safe_float(financial_data.get("total_assets", financial_data.get("总资产", 0)))
        total_liabilities = FundSkill._safe_float(financial_data.get("total_liabilities", financial_data.get("总负债", 0)))
        equity = FundSkill._safe_float(financial_data.get("equity", financial_data.get("股东权益", 0)))
        cash = FundSkill._safe_float(financial_data.get("cash", financial_data.get("货币资金", 0)))
        inventory = FundSkill._safe_float(financial_data.get("inventory", financial_data.get("存货", 0)))
        receivables = FundSkill._safe_float(financial_data.get("receivables", financial_data.get("应收账款", 0)))
        ebit = FundSkill._safe_float(financial_data.get("ebit", financial_data.get("息税前利润", 0)))
        interest = FundSkill._safe_float(financial_data.get("interest_expense", financial_data.get("利息费用", 0.1)))

        current_ratio = current_assets / current_liabilities if current_liabilities > 0 else 0
        quick_ratio = (current_assets - inventory) / current_liabilities if current_liabilities > 0 else 0
        debt_to_asset = total_liabilities / total_assets if total_assets > 0 else 0
        debt_to_equity = total_liabilities / equity if equity > 0 else 0
        interest_coverage = ebit / interest if interest > 0 else 999
        cash_ratio = cash / current_liabilities if current_liabilities > 0 else 0
        working_capital = current_assets - current_liabilities

        score = 50
        if current_ratio > 2:
            score += 10
        elif current_ratio > 1.5:
            score += 5
        elif current_ratio < 1:
            score -= 10

        if quick_ratio > 1:
            score += 5
        elif quick_ratio < 0.5:
            score -= 5

        if debt_to_asset < 0.4:
            score += 10
        elif debt_to_asset < 0.6:
            score += 5
        elif debt_to_asset > 0.8:
            score -= 15

        if interest_coverage > 5:
            score += 5
        elif interest_coverage < 1:
            score -= 10

        if cash_ratio > 0.3:
            score += 5

        score = max(0, min(100, score))

        if debt_to_asset < 0.4 and current_ratio > 2:
            quality = "结构健康"
        elif debt_to_asset > 0.7 or current_ratio < 1:
            quality = "结构脆弱"
        else:
            quality = "结构一般"

        return BalanceSheetStructure(
            current_ratio=round(current_ratio, 2),
            quick_ratio=round(quick_ratio, 2),
            debt_to_asset=round(debt_to_asset * 100, 2),
            debt_to_equity=round(debt_to_equity, 2),
            interest_coverage=round(interest_coverage, 2),
            cash_ratio=round(cash_ratio, 2),
            working_capital=round(working_capital, 2),
            asset_quality=quality,
            score=score
        )

    @staticmethod
    def analyze_cash_flow(financial_data: Dict) -> CashFlowMetrics:
        ocf = FundSkill._safe_float(financial_data.get("operating_cash_flow", financial_data.get("经营活动现金流", 0)))
        net_profit = FundSkill._safe_float(financial_data.get("net_profit", financial_data.get("净利润", 0)))
        capex = FundSkill._safe_float(financial_data.get("capex", financial_data.get("资本支出", 0)))
        dividend = FundSkill._safe_float(financial_data.get("dividend", financial_data.get("分红", 0)))
        revenue = FundSkill._safe_float(financial_data.get("revenue", financial_data.get("营业收入", 0)))

        ocf_to_profit = ocf / net_profit if net_profit > 0 else 0
        fcf = ocf - capex
        fcf_to_profit = fcf / net_profit if net_profit > 0 else 0
        ocf_coverage = ocf / revenue if revenue > 0 else 0
        capex_ratio = capex / ocf if ocf > 0 else 0
        payout = dividend / net_profit if net_profit > 0 else 0

        score = 50
        if ocf_to_profit > 1:
            score += 15
        elif ocf_to_profit > 0.8:
            score += 10
        elif ocf_to_profit > 0.5:
            score += 5
        elif ocf_to_profit < 0:
            score -= 20

        if fcf_to_profit > 0.5:
            score += 10
        elif fcf_to_profit < 0:
            score -= 10

        if ocf_coverage > 0.15:
            score += 5

        if capex_ratio < 0.5:
            score += 5
        elif capex_ratio > 1:
            score -= 5

        score = max(0, min(100, score))

        if ocf_to_profit > 1 and fcf_to_profit > 0.5:
            quality = "现金创造能力极强"
        elif ocf_to_profit > 0.8:
            quality = "现金创造能力良好"
        elif ocf_to_profit > 0:
            quality = "现金创造能力一般"
        else:
            quality = "现金流紧张"

        return CashFlowMetrics(
            ocf_to_net_profit=round(ocf_to_profit, 2),
            fcf_to_net_profit=round(fcf_to_profit, 2),
            ocf_coverage_ratio=round(ocf_coverage, 2),
            capex_to_ocf=round(capex_ratio, 2),
            dividend_payout=round(payout * 100, 2),
            cash_quality=quality,
            score=score
        )

    @staticmethod
    def analyze_growth(financial_data: Dict) -> GrowthMetrics:
        revenue_history = financial_data.get("revenue_history", financial_data.get("收入历史", []))
        profit_history = financial_data.get("profit_history", financial_data.get("利润历史", []))
        roe_history = financial_data.get("roe_history", financial_data.get("ROE历史", []))

        def calc_cagr(data: List[float], years: int = 3) -> float:
            if len(data) < years or data[0] <= 0 or data[-1] <= 0:
                return 0
            return ((data[-1] / data[0]) ** (1 / years) - 1) * 100

        revenue_cagr = calc_cagr(revenue_history)
        profit_cagr = calc_cagr(profit_history)
        roe_cagr = calc_cagr(roe_history)

        acceleration = False
        if len(revenue_history) >= 3:
            recent_growth = (revenue_history[-1] - revenue_history[-2]) / revenue_history[-2] if revenue_history[-2] > 0 else 0
            previous_growth = (revenue_history[-2] - revenue_history[-3]) / revenue_history[-3] if revenue_history[-3] > 0 else 0
            acceleration = recent_growth > previous_growth

        if profit_cagr > 30 and revenue_cagr > 20:
            sustainability = "高增长可持续"
        elif profit_cagr > revenue_cagr:
            sustainability = "盈利增速快于收入，效率提升"
        elif profit_cagr < 0 and revenue_cagr > 0:
            sustainability = "增收不增利，警惕"
        else:
            sustainability = "增长平稳"

        if revenue_cagr > 30:
            stage = "高速成长期"
        elif revenue_cagr > 15:
            stage = "成长期"
        elif revenue_cagr > 5:
            stage = "成熟期"
        elif revenue_cagr > 0:
            stage = "衰退初期"
        else:
            stage = "衰退期"

        score = 50
        if revenue_cagr > 30:
            score += 20
        elif revenue_cagr > 20:
            score += 15
        elif revenue_cagr > 10:
            score += 10
        elif revenue_cagr > 0:
            score += 5
        else:
            score -= 15

        if profit_cagr > revenue_cagr:
            score += 10
        elif profit_cagr < 0:
            score -= 10

        score = max(0, min(100, score))

        return GrowthMetrics(
            revenue_cagr_3y=round(revenue_cagr, 2),
            profit_cagr_3y=round(profit_cagr, 2),
            roe_cagr_3y=round(roe_cagr, 2),
            revenue_acceleration=acceleration,
            profit_sustainability=sustainability,
            growth_stage=stage,
            score=score
        )

    @staticmethod
    def analyze_operation(financial_data: Dict) -> OperationalMetrics:
        revenue = FundSkill._safe_float(financial_data.get("revenue", financial_data.get("营业收入", 0)))
        cogs = FundSkill._safe_float(financial_data.get("cogs", financial_data.get("营业成本", 0)))
        inventory = FundSkill._safe_float(financial_data.get("inventory", financial_data.get("存货", 0)))
        receivables = FundSkill._safe_float(financial_data.get("receivables", financial_data.get("应收账款", 0)))
        payables = FundSkill._safe_float(financial_data.get("payables", financial_data.get("应付账款", 0)))
        total_assets = FundSkill._safe_float(financial_data.get("total_assets", financial_data.get("总资产", 0)))

        inventory_turnover = cogs / inventory if inventory > 0 else 0
        receivable_turnover = revenue / receivables if receivables > 0 else 0
        payable_turnover = cogs / payables if payables > 0 else 0
        cash_cycle = (365 / inventory_turnover if inventory_turnover > 0 else 0) + \
                     (365 / receivable_turnover if receivable_turnover > 0 else 0) - \
                     (365 / payable_turnover if payable_turnover > 0 else 0)
        asset_turnover = revenue / total_assets if total_assets > 0 else 0

        score = 50
        if inventory_turnover > 10:
            score += 10
        elif inventory_turnover < 3:
            score -= 5

        if receivable_turnover > 10:
            score += 10
        elif receivable_turnover < 5:
            score -= 5

        if cash_cycle < 30:
            score += 10
        elif cash_cycle > 90:
            score -= 10

        if asset_turnover > 1:
            score += 5

        score = max(0, min(100, score))

        if cash_cycle < 30 and inventory_turnover > 8:
            level = "运营效率极高"
        elif cash_cycle < 60:
            level = "运营效率良好"
        elif cash_cycle < 120:
            level = "运营效率一般"
        else:
            level = "运营效率低下"

        return OperationalMetrics(
            inventory_turnover=round(inventory_turnover, 2),
            receivable_turnover=round(receivable_turnover, 2),
            payable_turnover=round(payable_turnover, 2),
            cash_cycle=round(cash_cycle, 2),
            asset_turnover=round(asset_turnover, 2),
            efficiency_level=level,
            score=score
        )

    @staticmethod
    def analyze(financial_data: Dict) -> FundSignals:
        logger.info("[FundSkill] 开始机构级基本面分析")

        profitability = FundSkill.analyze_profitability(financial_data)
        balance_sheet = FundSkill.analyze_balance_sheet(financial_data)
        cash_flow = FundSkill.analyze_cash_flow(financial_data)
        growth = FundSkill.analyze_growth(financial_data)
        operation = FundSkill.analyze_operation(financial_data)

        overall_score = int((profitability.score + balance_sheet.score + cash_flow.score + growth.score + operation.score) / 5)

        if overall_score >= 80:
            grade = "A级-优质标的"
        elif overall_score >= 65:
            grade = "B级-良好标的"
        elif overall_score >= 50:
            grade = "C级-一般标的"
        elif overall_score >= 35:
            grade = "D级-谨慎标的"
        else:
            grade = "E级-高风险标的"

        warnings = []
        if profitability.roe_ttm < 5:
            warnings.append("ROE偏低，资本回报不足")
        if balance_sheet.debt_to_asset > 70:
            warnings.append("资产负债率过高")
        if cash_flow.ocf_to_net_profit < 0.5:
            warnings.append("现金流质量差，盈利含金量低")
        if growth.revenue_cagr_3y < 0:
            warnings.append("收入负增长")
        if operation.cash_cycle > 120:
            warnings.append("运营周期过长")
        if not warnings:
            warnings.append("暂无重大财务风险")

        advice_parts = []
        if profitability.dupont.roe_contribution == "高盈利驱动":
            advice_parts.append("高盈利质量，护城河深厚")
        if cash_flow.ocf_to_net_profit > 1:
            advice_parts.append("现金流充裕，内生增长能力强")
        if growth.revenue_acceleration:
            advice_parts.append("收入加速增长，景气度上行")
        if balance_sheet.asset_quality == "结构健康":
            advice_parts.append("财务结构稳健")
        if not advice_parts:
            advice_parts.append("基本面一般，需结合其他维度")

        advice = " | ".join(advice_parts)
        logger.info(f"[FundSkill] 分析完成 | 投资评级: {grade} | 综合评分: {overall_score}")

        return FundSignals(
            profitability=profitability,
            balance_sheet=balance_sheet,
            cash_flow=cash_flow,
            growth=growth,
            operation=operation,
            overall_score=overall_score,
            investment_grade=grade,
            research_advice=advice,
            risk_warnings=warnings
        )


fund_skill = FundSkill()
