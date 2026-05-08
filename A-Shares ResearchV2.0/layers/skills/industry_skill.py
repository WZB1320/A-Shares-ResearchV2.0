import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger("IndustrySkill")


class IndustryCycle(Enum):
    RECOVERY = "复苏期"
    EXPANSION = "扩张期"
    MATURITY = "成熟期"
    DECLINE = "衰退期"


class PolicyImpact(Enum):
    STRONG_POSITIVE = "强利好"
    POSITIVE = "利好"
    NEUTRAL = "中性"
    NEGATIVE = "利空"
    STRONG_NEGATIVE = "强利空"


@dataclass
class IndustryChainMetrics:
    upstream_pressure: str
    downstream_demand: str
    supply_constraint: str
    price_trend: str
    inventory_level: str


@dataclass
class CompetitiveLandscape:
    market_share: float
    market_share_change: float
    cr3: float
    cr5: float
    herfindahl_index: float
    competitive_pattern: str
    moat_strength: str


@dataclass
class PolicyEnvironment:
    policy_direction: str
    subsidy_level: str
    regulatory_risk: str
    trade_policy_impact: str
    overall_policy_score: int


@dataclass
class IndustryValuation:
    industry_pe: float
    industry_pb: float
    industry_ps: float
    pe_history_percentile: float
    pb_history_percentile: float
    valuation_status: str


@dataclass
class PeerComparison:
    vs_industry_pe_pct: float
    vs_industry_pb_pct: float
    vs_industry_roe_pct: float
    vs_industry_growth_pct: float
    relative_strength: str


@dataclass
class IndustrySignals:
    industry_name: str
    industry_cycle: IndustryCycle
    chain_metrics: IndustryChainMetrics
    competitive: CompetitiveLandscape
    policy: PolicyEnvironment
    valuation: IndustryValuation
    peer_comparison: PeerComparison
    overall_score: int
    industry_grade: str
    research_advice: str
    risk_warnings: List[str]


class IndustrySkill:
    """
    机构级行业技能层
    覆盖：产业链分析/竞争格局/政策环境/行业估值/同业对比
    标准：券商研究所行业研究框架 + 波特五力模型
    """

    @staticmethod
    def _safe_float(value, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def analyze_industry_cycle(fundamental_data: Dict) -> IndustryCycle:
        industry_growth = fundamental_data.get("industry_growth_rate", 0)
        capacity_utilization = fundamental_data.get("capacity_utilization", 0)
        profit_margin_trend = fundamental_data.get("profit_margin_trend", "平稳")

        if industry_growth > 20 and capacity_utilization > 80 and profit_margin_trend == "上升":
            return IndustryCycle.EXPANSION
        elif industry_growth > 10 and capacity_utilization > 70:
            return IndustryCycle.RECOVERY
        elif industry_growth > 0 and capacity_utilization > 60:
            return IndustryCycle.MATURITY
        else:
            return IndustryCycle.DECLINE

    @staticmethod
    def analyze_industry_chain(fundamental_data: Dict) -> IndustryChainMetrics:
        raw_material_price = fundamental_data.get("raw_material_price_trend", "平稳")
        downstream_demand = fundamental_data.get("downstream_demand", "平稳")
        supply_constraint = fundamental_data.get("supply_constraint", "无")
        price_trend = fundamental_data.get("product_price_trend", "平稳")
        inventory = fundamental_data.get("inventory_level", "正常")

        if "涨" in raw_material_price or "升" in raw_material_price:
            upstream = "成本压力上升"
        elif "跌" in raw_material_price or "降" in raw_material_price:
            upstream = "成本压力缓解"
        else:
            upstream = "成本压力平稳"

        if "旺" in downstream_demand or "强" in downstream_demand:
            demand = "下游需求旺盛"
        elif "弱" in downstream_demand or "淡" in downstream_demand:
            demand = "下游需求疲软"
        else:
            demand = "下游需求平稳"

        return IndustryChainMetrics(
            upstream_pressure=upstream,
            downstream_demand=demand,
            supply_constraint=supply_constraint if supply_constraint else "无显著约束",
            price_trend=price_trend if price_trend else "价格平稳",
            inventory_level=inventory if inventory else "库存正常"
        )

    @staticmethod
    def analyze_competitive_landscape(fundamental_data: Dict) -> CompetitiveLandscape:
        industry_stocks = fundamental_data.get("industry_stocks", [])
        market_share = fundamental_data.get("market_share", 0)
        market_share_change = fundamental_data.get("market_share_change", 0)
        cr3 = fundamental_data.get("industry_cr3", 30)
        cr5 = fundamental_data.get("industry_cr5", 45)

        if len(industry_stocks) > 0:
            hhi = sum((100 / len(industry_stocks)) ** 2 for _ in range(len(industry_stocks)))
        else:
            hhi = 0

        if cr3 > 60:
            pattern = "寡头垄断"
        elif cr3 > 40:
            pattern = "高度集中"
        elif cr3 > 20:
            pattern = "中等集中"
        else:
            pattern = "分散竞争"

        if market_share > 20 and market_share_change > 0:
            moat = "强护城河"
        elif market_share > 10:
            moat = "中等护城河"
        elif market_share > 5:
            moat = "弱护城河"
        else:
            moat = "无明显护城河"

        return CompetitiveLandscape(
            market_share=round(market_share, 2),
            market_share_change=round(market_share_change, 2),
            cr3=round(cr3, 2),
            cr5=round(cr5, 2),
            herfindahl_index=round(hhi, 2),
            competitive_pattern=pattern,
            moat_strength=moat
        )

    @staticmethod
    def analyze_policy_environment(fundamental_data: Dict) -> PolicyEnvironment:
        industry_name = fundamental_data.get("industry_name", "")
        policy_keywords_positive = ["新能源", "半导体", "人工智能", "生物医药", "高端制造", "碳中和", "数字经济"]
        policy_keywords_negative = ["房地产", "教培", "游戏", "平台经济"]
        subsidy_keywords = ["补贴", "扶持", "专项基金", "税收优惠"]
        regulatory_keywords = ["监管", "整顿", "限制", "准入"]

        if any(kw in industry_name for kw in policy_keywords_positive):
            direction = "政策大力支持"
            policy_score = 80
        elif any(kw in industry_name for kw in policy_keywords_negative):
            direction = "政策监管趋严"
            policy_score = 30
        else:
            direction = "政策环境中性"
            policy_score = 50

        if any(kw in industry_name for kw in subsidy_keywords):
            subsidy = "补贴力度大"
            policy_score += 10
        else:
            subsidy = "补贴力度一般"

        if any(kw in industry_name for kw in regulatory_keywords):
            regulatory = "监管风险较高"
            policy_score -= 10
        else:
            regulatory = "监管风险可控"

        trade = "贸易摩擦影响有限"
        if "出口" in industry_name or "外贸" in industry_name:
            trade = "需关注贸易摩擦风险"

        return PolicyEnvironment(
            policy_direction=direction,
            subsidy_level=subsidy,
            regulatory_risk=regulatory,
            trade_policy_impact=trade,
            overall_policy_score=max(0, min(100, policy_score))
        )

    @staticmethod
    def analyze_industry_valuation(fundamental_data: Dict) -> IndustryValuation:
        industry_pe = fundamental_data.get("industry_pe", 0)
        industry_pb = fundamental_data.get("industry_pb", 0)
        industry_ps = fundamental_data.get("industry_ps", 0)
        pe_history = fundamental_data.get("pe_history", [])
        pb_history = fundamental_data.get("pb_history", [])

        if pe_history and industry_pe > 0:
            pe_pct = sum(1 for pe in pe_history if industry_pe > pe) / len(pe_history) * 100
        else:
            pe_pct = 50

        if pb_history and industry_pb > 0:
            pb_pct = sum(1 for pb in pb_history if industry_pb > pb) / len(pb_history) * 100
        else:
            pb_pct = 50

        if pe_pct < 20 and pb_pct < 20:
            status = "行业估值低位"
        elif pe_pct > 80 and pb_pct > 80:
            status = "行业估值高位"
        else:
            status = "行业估值合理"

        return IndustryValuation(
            industry_pe=round(industry_pe, 2),
            industry_pb=round(industry_pb, 2),
            industry_ps=round(industry_ps, 2),
            pe_history_percentile=round(pe_pct, 2),
            pb_history_percentile=round(pb_pct, 2),
            valuation_status=status
        )

    @staticmethod
    def analyze_peer_comparison(fundamental_data: Dict) -> PeerComparison:
        stock_pe = fundamental_data.get("pe_ttm", 0)
        stock_pb = fundamental_data.get("pb", 0)
        stock_roe = fundamental_data.get("roe", 0)
        stock_growth = fundamental_data.get("revenue_growth", 0)
        industry_pe = fundamental_data.get("industry_pe", 0)
        industry_pb = fundamental_data.get("industry_pb", 0)
        industry_roe = fundamental_data.get("industry_roe", 0)
        industry_growth = fundamental_data.get("industry_growth_rate", 0)

        vs_pe = ((stock_pe / industry_pe) * 100) if industry_pe > 0 else 100
        vs_pb = ((stock_pb / industry_pb) * 100) if industry_pb > 0 else 100
        vs_roe = ((stock_roe / industry_roe) * 100) if industry_roe > 0 else 100
        vs_growth = ((stock_growth / industry_growth) * 100) if industry_growth > 0 else 100

        if vs_pe < 80 and vs_roe > 120:
            strength = "相对行业低估"
        elif vs_pe > 120 and vs_roe < 80:
            strength = "相对行业高估"
        else:
            strength = "估值与行业匹配"

        return PeerComparison(
            vs_industry_pe_pct=round(vs_pe, 2),
            vs_industry_pb_pct=round(vs_pb, 2),
            vs_industry_roe_pct=round(vs_roe, 2),
            vs_industry_growth_pct=round(vs_growth, 2),
            relative_strength=strength
        )

    @staticmethod
    def analyze(fundamental_data: Dict) -> IndustrySignals:
        logger.info("[IndustrySkill] 开始机构级行业分析")

        basic_info = fundamental_data.get("basic_info", {})
        industry_name = basic_info.get("行业", fundamental_data.get("industry_name", "未知"))

        cycle = IndustrySkill.analyze_industry_cycle(fundamental_data)
        chain = IndustrySkill.analyze_industry_chain(fundamental_data)
        competitive = IndustrySkill.analyze_competitive_landscape(fundamental_data)
        policy = IndustrySkill.analyze_policy_environment(fundamental_data)
        valuation = IndustrySkill.analyze_industry_valuation(fundamental_data)
        peer = IndustrySkill.analyze_peer_comparison(fundamental_data)

        score = 50
        if cycle == IndustryCycle.EXPANSION:
            score += 20
        elif cycle == IndustryCycle.RECOVERY:
            score += 15
        elif cycle == IndustryCycle.MATURITY:
            score += 5
        elif cycle == IndustryCycle.DECLINE:
            score -= 15

        if competitive.moat_strength == "强护城河":
            score += 15
        elif competitive.moat_strength == "中等护城河":
            score += 10
        elif competitive.moat_strength == "弱护城河":
            score += 5

        score += (policy.overall_policy_score - 50) / 5

        if valuation.valuation_status == "行业估值低位":
            score += 10
        elif valuation.valuation_status == "行业估值高位":
            score -= 10

        if peer.relative_strength == "相对行业低估":
            score += 10
        elif peer.relative_strength == "相对行业高估":
            score -= 5

        score = max(0, min(100, int(score)))

        if score >= 80:
            grade = "A级-优质赛道"
        elif score >= 65:
            grade = "B级-良好赛道"
        elif score >= 50:
            grade = "C级-一般赛道"
        elif score >= 35:
            grade = "D级-谨慎赛道"
        else:
            grade = "E级-回避赛道"

        warnings = []
        if cycle == IndustryCycle.DECLINE:
            warnings.append("行业处于衰退期")
        if competitive.competitive_pattern == "分散竞争" and competitive.market_share < 5:
            warnings.append("行业竞争激烈，公司份额小")
        if policy.regulatory_risk == "监管风险较高":
            warnings.append("政策监管风险")
        if valuation.valuation_status == "行业估值高位":
            warnings.append("行业估值偏高")
        if not warnings:
            warnings.append("行业层面暂无重大风险")

        advice_parts = []
        if cycle in [IndustryCycle.EXPANSION, IndustryCycle.RECOVERY]:
            advice_parts.append(f"行业处于{cycle.value}，景气度上行")
        if competitive.moat_strength in ["强护城河", "中等护城河"]:
            advice_parts.append(f"竞争格局{competitive.competitive_pattern}，公司护城河{competitive.moat_strength}")
        if policy.overall_policy_score > 60:
            advice_parts.append("政策环境友好")
        if peer.relative_strength == "相对行业低估":
            advice_parts.append("相对行业估值有优势")
        if not advice_parts:
            advice_parts.append("行业分析中性")

        advice = " | ".join(advice_parts)
        logger.info(f"[IndustrySkill] 分析完成 | 行业评级: {grade} | 评分: {score}")

        return IndustrySignals(
            industry_name=industry_name,
            industry_cycle=cycle,
            chain_metrics=chain,
            competitive=competitive,
            policy=policy,
            valuation=valuation,
            peer_comparison=peer,
            overall_score=score,
            industry_grade=grade,
            research_advice=advice,
            risk_warnings=warnings
        )


industry_skill = IndustrySkill()
