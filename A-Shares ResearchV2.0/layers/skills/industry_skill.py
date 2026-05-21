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
class IndustrySignals:
    industry_name: str
    peer_count: int
    has_industry_data: bool
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
    def analyze(fundamental_data: Dict) -> IndustrySignals:
        logger.info("[IndustrySkill] 开始机构级行业分析")

        if not fundamental_data or not isinstance(fundamental_data, dict):
            logger.warning("[IndustrySkill] fundamental_data为空或类型异常")
            return IndustrySignals(
                industry_name="未知",
                peer_count=0,
                has_industry_data=False,
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
            grade = "A级-数据充分"
        elif score >= 65:
            grade = "B级-数据良好"
        elif score >= 50:
            grade = "C级-数据一般"
        elif score >= 35:
            grade = "D级-数据不足"
        else:
            grade = "E级-数据严重缺失"

        advice = " | ".join(advice_parts)
        logger.info(f"[IndustrySkill] 分析完成 | 行业: {industry_name} | 数据可用: {available} | 数据缺失: {unavailable}")

        return IndustrySignals(
            industry_name=industry_name,
            peer_count=peer_count,
            has_industry_data=has_data,
            data_available_fields=available,
            data_unavailable_fields=unavailable,
            overall_score=score,
            industry_grade=grade,
            research_advice=advice,
            risk_warnings=warnings
        )


industry_skill = IndustrySkill()