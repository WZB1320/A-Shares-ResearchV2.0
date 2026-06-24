import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger("IndustrySkill")


@dataclass
class IndustryBasicInfo:
    industry_name: str
    peer_count: int
    has_industry_data: bool


@dataclass
class IndustryComparison:
    """行业对标排名分析"""
    # PE 对标
    pe_ttm: float
    pe_rank: int                 # 行业排名（1=最低PE）
    pe_median: float             # 行业中位数PE
    pe_percentile: float         # PE在行业中的百分位（0-100）
    # PB 对标
    pb: float
    pb_rank: int                 # 行业排名（1=最低PB）
    pb_median: float
    pb_percentile: float
    # ROE 对标
    roe: float
    roe_rank: int                # 行业排名（1=最高ROE，越小越好）
    roe_median: float
    roe_percentile: float
    # 毛利率对标
    gross_margin: float
    margin_rank: int
    margin_median: float
    # 营收增速对标
    revenue_growth: float
    growth_rank: int
    growth_median: float
    # 行业集中度
    peer_count: int
    has_sufficient_peers: bool   # 同行业数据是否≥5只


@dataclass
class IndustrySignals:
    industry_name: str
    peer_count: int
    has_industry_data: bool
    peer_comparison: Optional[IndustryComparison]
    data_available_fields: List[str]
    data_unavailable_fields: List[str]
    overall_score: int
    industry_grade: str
    research_advice: str
    risk_warnings: List[str]


class IndustrySkill:

    @staticmethod
    def _safe_float(value, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _check_data_availability(fundamental_data: Dict) -> Tuple[List[str], List[str]]:
        available = []
        unavailable = []

        basic_info = fundamental_data.get("basic_info", {})
        if basic_info and basic_info.get("行业"):
            available.append("行业分类")
        else:
            unavailable.append("行业分类")

        industry_stocks = fundamental_data.get("industry_stocks", [])
        if industry_stocks:
            available.append(f"同行业股票({len(industry_stocks)}只)")
        else:
            unavailable.append("同行业股票")

        finance = fundamental_data.get("finance", [])
        if finance:
            available.append(f"财务数据({len(finance)}期)")
        else:
            unavailable.append("财务数据")

        return available, unavailable

    @staticmethod
    def analyze_peer_comparison(fundamental_data: Dict, stock_financials: Dict) -> Optional[IndustryComparison]:
        """计算行业对标排名
        
        将个股的 PE/PB/ROE/毛利率/营收增速与同行业可比公司进行横比，
        得到在行业中的百分位排名。
        """
        industry_stocks = fundamental_data.get("industry_stocks", [])
        if not industry_stocks or not isinstance(industry_stocks, list):
            return None
        
        if len(industry_stocks) < 3:
            return None
        
        # 提取个股财务数据
        valuation = stock_financials.get("valuation", {})
        stock_pe = IndustrySkill._safe_float(valuation.get("pe_ttm", stock_financials.get("pe_ttm", 0)))
        stock_pb = IndustrySkill._safe_float(valuation.get("pb", stock_financials.get("pb", 0)))
        stock_roe = IndustrySkill._safe_float(stock_financials.get("roe", stock_financials.get("净资产收益率", 0)))
        stock_margin = IndustrySkill._safe_float(stock_financials.get("gross_profit_margin", stock_financials.get("gross_margin_ttm", stock_financials.get("毛利率", 0))))
        stock_growth = IndustrySkill._safe_float(stock_financials.get("revenue_growth", stock_financials.get("营收增速", 0)))
        
        # 收集同行业数据
        peer_pe = []; peer_pb = []; peer_roe = []; peer_margin = []; peer_growth = []
        
        for peer in industry_stocks:
            if isinstance(peer, dict):
                pv = peer.get("valuation", peer)
                pe = IndustrySkill._safe_float(pv.get("pe_ttm", pv.get("pe", 0)))
                pb = IndustrySkill._safe_float(pv.get("pb", 0))
                roe = IndustrySkill._safe_float(peer.get("roe", peer.get("净资产收益率", 0)))
                margin = IndustrySkill._safe_float(peer.get("gross_profit_margin", peer.get("gross_margin_ttm", peer.get("毛利率", 0))))
                growth = IndustrySkill._safe_float(peer.get("revenue_growth", peer.get("营收增速", 0)))
                
                if pe > 0: peer_pe.append(pe)
                if pb > 0: peer_pb.append(pb)
                if roe != 0: peer_roe.append(roe)
                if margin != 0: peer_margin.append(margin)
                if growth != 0: peer_growth.append(growth)
        
        has_sufficient = len(peer_pe) >= 5
        
        def calc_rank(value, peer_list, ascending=True):
            """计算排名和百分位
            ascending=True: 值越小排名越前（PE/PB适用）
            ascending=False: 值越大排名越前（ROE/毛利率/增速适用）
            """
            if not peer_list or value <= 0:
                return 0, 0, 0
            sorted_list = sorted(peer_list)
            median = sorted_list[len(sorted_list) // 2]
            if ascending:
                rank = sum(1 for v in sorted_list if v < value) + 1
            else:
                rank = sum(1 for v in sorted_list if v > value) + 1
            percentile = (rank / len(sorted_list)) * 100
            return rank, median, percentile
        
        pe_rank, pe_median, pe_pct = calc_rank(stock_pe, peer_pe, ascending=True)
        pb_rank, pb_median, pb_pct = calc_rank(stock_pb, peer_pb, ascending=True)
        # ROE/毛利率/增速：越大越好
        roe_rank, roe_median, roe_pct = calc_rank(stock_roe, peer_roe, ascending=False)
        margin_rank, margin_median, _ = calc_rank(stock_margin, peer_margin, ascending=False)
        growth_rank, growth_median, _ = calc_rank(stock_growth, peer_growth, ascending=False)
        
        return IndustryComparison(
            pe_ttm=round(stock_pe, 2),
            pe_rank=pe_rank,
            pe_median=round(pe_median, 2),
            pe_percentile=round(pe_pct, 1),
            pb=round(stock_pb, 2),
            pb_rank=pb_rank,
            pb_median=round(pb_median, 2),
            pb_percentile=round(pb_pct, 1),
            roe=round(stock_roe, 2),
            roe_rank=roe_rank,
            roe_median=round(roe_median, 2),
            roe_percentile=round(roe_pct, 1),
            gross_margin=round(stock_margin, 2),
            margin_rank=margin_rank,
            margin_median=round(margin_median, 2),
            revenue_growth=round(stock_growth, 2),
            growth_rank=growth_rank,
            growth_median=round(growth_median, 2),
            peer_count=len(industry_stocks),
            has_sufficient_peers=has_sufficient,
        )

    @staticmethod
    def analyze(fundamental_data: Dict) -> IndustrySignals:
        logger.info("[IndustrySkill] 开始机构级行业分析")

        if not fundamental_data or not isinstance(fundamental_data, dict):
            logger.warning("[IndustrySkill] fundamental_data为空或类型异常")
            return IndustrySignals(
                industry_name="未知",
                peer_count=0,
                has_industry_data=False,
                peer_comparison=None,
                data_available_fields=[],
                data_unavailable_fields=["所有数据"],
                overall_score=50,
                industry_grade="数据不足-无法评级",
                research_advice="行业数据不可用，无法进行分析",
                risk_warnings=["行业数据缺失，无法评估行业风险"]
            )

        basic_info = fundamental_data.get("basic_info", {})
        industry_name = basic_info.get("行业", "未知")
        industry_stocks = fundamental_data.get("industry_stocks", [])
        peer_count = len(industry_stocks) if isinstance(industry_stocks, list) else 0

        available, unavailable = IndustrySkill._check_data_availability(fundamental_data)
        has_data = len(available) > 0

        # ── 行业对标分析 ──
        peer_comparison = IndustrySkill.analyze_peer_comparison(fundamental_data, fundamental_data)

        score = 50
        warnings = []
        advice_parts = []

        if industry_name and industry_name != "未知":
            advice_parts.append(f"所属行业：{industry_name}")
            score += 5
        else:
            warnings.append("无法确定行业分类")
            score -= 10

        if peer_count > 0:
            advice_parts.append(f"同行业可比公司{peer_count}只")
            score += 5
        else:
            warnings.append("缺少同行业可比公司数据")

        # 行业对标评分
        if peer_comparison:
            if peer_comparison.roe > 0 and peer_comparison.roe_percentile <= 20:
                advice_parts.append(f"ROE位居行业前{peer_comparison.roe_percentile:.0f}%，盈利能力突出")
                score += 15
            elif peer_comparison.roe > 0 and peer_comparison.roe_percentile <= 40:
                score += 8
            elif peer_comparison.roe > 0 and peer_comparison.roe_percentile > 80:
                warnings.append(f"ROE处于行业后{100 - peer_comparison.roe_percentile:.0f}%，盈利能力弱于同行")
                score -= 5
            
            if peer_comparison.pe_ttm > 0 and peer_comparison.pe_percentile <= 30:
                advice_parts.append(f"PE处于行业前{peer_comparison.pe_percentile:.0f}%（估值偏低），安全边际较高")
                score += 5
            elif peer_comparison.pe_ttm > 0 and peer_comparison.pe_percentile > 70:
                warnings.append(f"PE处于行业后{100 - peer_comparison.pe_percentile:.0f}%（估值偏高），需关注估值风险")
                score -= 5

        finance = fundamental_data.get("finance", [])
        if finance and isinstance(finance, list) and len(finance) > 0:
            advice_parts.append(f"可获取{len(finance)}期财务数据进行行业对标分析")
            score += 5
        else:
            warnings.append("缺少财务数据，无法进行行业对标")

        if not has_data:
            warnings.append("行业分析所需数据严重不足")

        if not warnings:
            warnings.append("行业数据基本齐全，可进行基础行业分析")

        if not advice_parts:
            advice_parts.append("行业数据不足，建议补充数据后再进行分析")

        score = max(0, min(100, score))

        if score >= 80:
            grade = "A级-行业领先"
        elif score >= 65:
            grade = "B级-行业上游"
        elif score >= 50:
            grade = "C级-行业中游"
        elif score >= 35:
            grade = "D级-行业下游"
        else:
            grade = "E级-行业劣势"

        advice = " | ".join(advice_parts)
        logger.info(f"[IndustrySkill] 分析完成 | 行业: {industry_name} | 对标: {peer_comparison is not None} | 评分: {score}")

        return IndustrySignals(
            industry_name=industry_name,
            peer_count=peer_count,
            has_industry_data=has_data,
            peer_comparison=peer_comparison,
            data_available_fields=available,
            data_unavailable_fields=unavailable,
            overall_score=score,
            industry_grade=grade,
            research_advice=advice,
            risk_warnings=warnings
        )


industry_skill = IndustrySkill()