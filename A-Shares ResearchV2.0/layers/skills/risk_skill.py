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
logger = logging.getLogger("RiskSkill")


class RiskLevel(Enum):
    CRITICAL = "极高风险"
    HIGH = "高风险"
    MEDIUM = "中等风险"
    LOW = "低风险"
    MINIMAL = "极低风险"


@dataclass
class PledgeRiskMetrics:
    pledge_ratio: float
    pledge_count: int
    major_holder_pledge: float
    close_to_liquidation: bool
    liquidation_line: float
    risk_level: RiskLevel


@dataclass
class ReductionRiskMetrics:
    recent_reduce_ratio: float
    reduce_count: int
    reduce_amount: float
    reduce_participants: int
    continuous_reduction: bool
    risk_level: RiskLevel


@dataclass
class FinancialRiskMetrics:
    debt_to_asset: float
    current_ratio: float
    quick_ratio: float
    interest_coverage: float
    cash_flow_shortfall: bool
    consecutive_losses: int
    audit_opinion: str
    risk_level: RiskLevel


@dataclass
class OperationalRiskMetrics:
    revenue_concentration: float
    customer_concentration: float
    supplier_concentration: float
    inventory_turnover_decline: bool
    receivable_turnover_decline: bool
    risk_level: RiskLevel


@dataclass
class MarketRiskMetrics:
    beta: float
    volatility_30d: float
    max_drawdown_1y: float
    liquidity_score: float
    tail_risk: str
    risk_level: RiskLevel


@dataclass
class GovernanceRiskMetrics:
    related_party_transactions: float
    guarantee_ratio: float
    executive_turnover: int
    lawsuit_count: int
    regulatory_penalties: int
    risk_level: RiskLevel


@dataclass
class RiskSignals:
    pledge: PledgeRiskMetrics
    reduction: ReductionRiskMetrics
    financial: FinancialRiskMetrics
    operational: OperationalRiskMetrics
    market: MarketRiskMetrics
    governance: GovernanceRiskMetrics
    overall_risk_score: int
    overall_risk_level: RiskLevel
    warnings: List[str]
    research_advice: str


class RiskSkill:
    """
    机构级风险技能层
    覆盖：股权质押/大股东减持/财务风险/经营风险/市场风险/公司治理
    标准：券商研究所风险分析框架 + 全面风险管理体系
    """

    @staticmethod
    def _safe_float(value, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def analyze_pledge_risk(financial_data: Dict) -> PledgeRiskMetrics:
        pledge_ratio = financial_data.get("pledge_ratio", 0)
        pledge_count = financial_data.get("pledge_count", 0)
        major_holder_pledge = financial_data.get("major_holder_pledge_ratio", 0)
        liquidation_line = financial_data.get("liquidation_line", 0)
        current_price = financial_data.get("current_price", 0)

        close_to_liquidation = False
        if liquidation_line > 0 and current_price > 0:
            close_to_liquidation = (current_price / liquidation_line) < 1.2

        if pledge_ratio > 50 or close_to_liquidation:
            level = RiskLevel.CRITICAL
        elif pledge_ratio > 30:
            level = RiskLevel.HIGH
        elif pledge_ratio > 15:
            level = RiskLevel.MEDIUM
        elif pledge_ratio > 5:
            level = RiskLevel.LOW
        else:
            level = RiskLevel.MINIMAL

        return PledgeRiskMetrics(
            pledge_ratio=round(pledge_ratio, 2),
            pledge_count=pledge_count,
            major_holder_pledge=round(major_holder_pledge, 2),
            close_to_liquidation=close_to_liquidation,
            liquidation_line=round(liquidation_line, 2),
            risk_level=level
        )

    @staticmethod
    def analyze_reduction_risk(financial_data: Dict) -> ReductionRiskMetrics:
        recent_reduce = financial_data.get("recent_reduce_ratio", 0)
        reduce_count = financial_data.get("reduce_count", 0)
        reduce_amount = financial_data.get("reduce_amount", 0)
        reduce_participants = financial_data.get("reduce_participants", 0)
        continuous = financial_data.get("continuous_reduction", False)

        if recent_reduce > 5 or (continuous and recent_reduce > 2):
            level = RiskLevel.CRITICAL
        elif recent_reduce > 3:
            level = RiskLevel.HIGH
        elif recent_reduce > 1:
            level = RiskLevel.MEDIUM
        elif recent_reduce > 0:
            level = RiskLevel.LOW
        else:
            level = RiskLevel.MINIMAL

        return ReductionRiskMetrics(
            recent_reduce_ratio=round(recent_reduce, 2),
            reduce_count=reduce_count,
            reduce_amount=round(reduce_amount, 2),
            reduce_participants=reduce_participants,
            continuous_reduction=continuous,
            risk_level=level
        )

    @staticmethod
    def analyze_financial_risk(financial_data: Dict) -> FinancialRiskMetrics:
        debt_to_asset = financial_data.get("debt_to_asset", 0)
        current_ratio = financial_data.get("current_ratio", 0)
        quick_ratio = financial_data.get("quick_ratio", 0)
        interest_coverage = financial_data.get("interest_coverage", 999)
        ocf = financial_data.get("operating_cash_flow", financial_data.get("经营活动现金流", 0))
        net_profit = financial_data.get("net_profit", financial_data.get("净利润", 0))
        consecutive_losses = financial_data.get("consecutive_losses", 0)
        audit_opinion = financial_data.get("audit_opinion", "标准无保留")

        cash_shortfall = ocf < 0 and net_profit > 0

        if consecutive_losses >= 2 or audit_opinion != "标准无保留" or debt_to_asset > 0.9:
            level = RiskLevel.CRITICAL
        elif debt_to_asset > 0.8 or interest_coverage < 1 or current_ratio < 0.5:
            level = RiskLevel.HIGH
        elif debt_to_asset > 0.7 or interest_coverage < 3 or current_ratio < 1:
            level = RiskLevel.MEDIUM
        elif debt_to_asset > 0.5 or cash_shortfall:
            level = RiskLevel.LOW
        else:
            level = RiskLevel.MINIMAL

        return FinancialRiskMetrics(
            debt_to_asset=round(debt_to_asset * 100, 2),
            current_ratio=round(current_ratio, 2),
            quick_ratio=round(quick_ratio, 2),
            interest_coverage=round(interest_coverage, 2),
            cash_flow_shortfall=cash_shortfall,
            consecutive_losses=consecutive_losses,
            audit_opinion=audit_opinion,
            risk_level=level
        )

    @staticmethod
    def analyze_operational_risk(financial_data: Dict) -> OperationalRiskMetrics:
        revenue_concentration = financial_data.get("revenue_concentration", 0)
        customer_concentration = financial_data.get("customer_concentration", 0)
        supplier_concentration = financial_data.get("supplier_concentration", 0)
        inventory_decline = financial_data.get("inventory_turnover_decline", False)
        receivable_decline = financial_data.get("receivable_turnover_decline", False)

        if revenue_concentration > 50 or customer_concentration > 40:
            level = RiskLevel.HIGH
        elif revenue_concentration > 30 or customer_concentration > 25:
            level = RiskLevel.MEDIUM
        elif revenue_concentration > 20:
            level = RiskLevel.LOW
        else:
            level = RiskLevel.MINIMAL

        return OperationalRiskMetrics(
            revenue_concentration=round(revenue_concentration, 2),
            customer_concentration=round(customer_concentration, 2),
            supplier_concentration=round(supplier_concentration, 2),
            inventory_turnover_decline=inventory_decline,
            receivable_turnover_decline=receivable_decline,
            risk_level=level
        )

    @staticmethod
    def analyze_market_risk(financial_data: Dict, tech_data: Optional[List[Dict]] = None) -> MarketRiskMetrics:
        beta = financial_data.get("beta", 1.0)
        volatility = financial_data.get("volatility_30d", 0)
        max_drawdown = financial_data.get("max_drawdown_1y", 0)
        avg_volume = financial_data.get("avg_volume", 0)
        market_cap = financial_data.get("market_cap", 1)

        liquidity = (avg_volume / market_cap * 100) if market_cap > 0 else 0

        if beta > 1.5 and volatility > 50:
            tail = "高波动高贝塔，系统性风险大"
        elif beta < 0.5:
            tail = "低贝塔防御型"
        else:
            tail = "市场波动正常"

        if beta > 2 or volatility > 60 or max_drawdown > 50:
            level = RiskLevel.HIGH
        elif beta > 1.5 or volatility > 40 or max_drawdown > 35:
            level = RiskLevel.MEDIUM
        elif beta > 1.0 or volatility > 25:
            level = RiskLevel.LOW
        else:
            level = RiskLevel.MINIMAL

        return MarketRiskMetrics(
            beta=round(beta, 2),
            volatility_30d=round(volatility, 2),
            max_drawdown_1y=round(max_drawdown, 2),
            liquidity_score=round(liquidity, 4),
            tail_risk=tail,
            risk_level=level
        )

    @staticmethod
    def analyze_governance_risk(financial_data: Dict) -> GovernanceRiskMetrics:
        related_party = financial_data.get("related_party_transactions", 0)
        guarantee = financial_data.get("guarantee_ratio", 0)
        turnover = financial_data.get("executive_turnover", 0)
        lawsuits = financial_data.get("lawsuit_count", 0)
        penalties = financial_data.get("regulatory_penalties", 0)

        if penalties > 0 or lawsuits > 5 or guarantee > 50:
            level = RiskLevel.CRITICAL
        elif lawsuits > 2 or guarantee > 30 or turnover > 3:
            level = RiskLevel.HIGH
        elif related_party > 20 or guarantee > 15 or turnover > 1:
            level = RiskLevel.MEDIUM
        elif related_party > 10:
            level = RiskLevel.LOW
        else:
            level = RiskLevel.MINIMAL

        return GovernanceRiskMetrics(
            related_party_transactions=round(related_party, 2),
            guarantee_ratio=round(guarantee, 2),
            executive_turnover=turnover,
            lawsuit_count=lawsuits,
            regulatory_penalties=penalties,
            risk_level=level
        )

    @staticmethod
    def analyze(financial_data: Dict, tech_data: Optional[List[Dict]] = None) -> RiskSignals:
        logger.info("[RiskSkill] 开始机构级风险分析")

        pledge = RiskSkill.analyze_pledge_risk(financial_data)
        reduction = RiskSkill.analyze_reduction_risk(financial_data)
        financial = RiskSkill.analyze_financial_risk(financial_data)
        operational = RiskSkill.analyze_operational_risk(financial_data)
        market = RiskSkill.analyze_market_risk(financial_data, tech_data)
        governance = RiskSkill.analyze_governance_risk(financial_data)

        risk_scores = {
            RiskLevel.CRITICAL: 0,
            RiskLevel.HIGH: 25,
            RiskLevel.MEDIUM: 50,
            RiskLevel.LOW: 75,
            RiskLevel.MINIMAL: 100
        }

        overall_score = int((
            risk_scores[pledge.risk_level] +
            risk_scores[reduction.risk_level] +
            risk_scores[financial.risk_level] +
            risk_scores[operational.risk_level] +
            risk_scores[market.risk_level] +
            risk_scores[governance.risk_level]
        ) / 6)

        if overall_score >= 80:
            overall_level = RiskLevel.MINIMAL
        elif overall_score >= 60:
            overall_level = RiskLevel.LOW
        elif overall_score >= 40:
            overall_level = RiskLevel.MEDIUM
        elif overall_score >= 20:
            overall_level = RiskLevel.HIGH
        else:
            overall_level = RiskLevel.CRITICAL

        warnings = []
        if pledge.risk_level in [RiskLevel.CRITICAL, RiskLevel.HIGH]:
            warnings.append(f"股权质押风险：质押率{pledge.pledge_ratio}%，{pledge.risk_level.value}")
        if reduction.risk_level in [RiskLevel.CRITICAL, RiskLevel.HIGH]:
            warnings.append(f"减持风险：近期减持{reduction.recent_reduce_ratio}%，{reduction.risk_level.value}")
        if financial.risk_level in [RiskLevel.CRITICAL, RiskLevel.HIGH]:
            warnings.append(f"财务风险：资产负债率{financial.debt_to_asset}%，{financial.risk_level.value}")
        if operational.risk_level in [RiskLevel.CRITICAL, RiskLevel.HIGH]:
            warnings.append(f"经营风险：收入集中度{operational.revenue_concentration}%，{operational.risk_level.value}")
        if market.risk_level in [RiskLevel.CRITICAL, RiskLevel.HIGH]:
            warnings.append(f"市场风险：Beta{market.beta}，{market.risk_level.value}")
        if governance.risk_level in [RiskLevel.CRITICAL, RiskLevel.HIGH]:
            warnings.append(f"治理风险：诉讼{governance.lawsuit_count}起，{governance.risk_level.value}")
        if not warnings:
            warnings.append("综合风险可控")

        advice_parts = []
        if pledge.close_to_liquidation:
            advice_parts.append("警惕质押平仓风险")
        if reduction.continuous_reduction:
            advice_parts.append("关注持续减持压力")
        if financial.cash_flow_shortfall:
            advice_parts.append("现金流与利润背离，盈利质量存疑")
        if market.beta > 1.5:
            advice_parts.append("高Beta标的，注意市场波动")
        if not advice_parts:
            advice_parts.append("风险结构健康")

        advice = " | ".join(advice_parts)
        logger.info(f"[RiskSkill] 分析完成 | 综合风险: {overall_level.value} | 评分: {overall_score}")

        return RiskSignals(
            pledge=pledge,
            reduction=reduction,
            financial=financial,
            operational=operational,
            market=market,
            governance=governance,
            overall_risk_score=overall_score,
            overall_risk_level=overall_level,
            warnings=warnings,
            research_advice=advice
        )


risk_skill = RiskSkill()
