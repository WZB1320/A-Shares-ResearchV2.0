import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger("ValuationSkill")


@dataclass
class ValuationMetrics:
    price: Optional[float]
    pe_ttm: Optional[float]
    pb: Optional[float]
    pe_percentile: Optional[float]
    pb_percentile: Optional[float]
    pe_10_avg: Optional[float]
    industry_category: Optional[str]
    percentile_pe_low_threshold: float
    percentile_pe_high_threshold: float
    percentile_pb_low_threshold: float
    percentile_pb_high_threshold: float
    data_available: bool


@dataclass
class ValuationSignals:
    metrics: ValuationMetrics
    data_available_fields: List[str]
    data_unavailable_fields: List[str]
    overall_score: int
    valuation_grade: str
    research_advice: str
    risk_warnings: List[str]


class ValuationSkill:

    CATEGORY_THRESHOLDS = {
        "stable": {"pe_low": 0.30, "pe_high": 0.70, "pb_low": 0.30, "pb_high": 0.70,
                   "labels": ["消费", "食品", "饮料", "酒", "白酒", "啤酒", "乳制品", "调味品",
                              "医药", "医疗", "公用", "电力", "水务", "燃气", "供热",
                              "高速公路", "铁路", "港口", "机场", "电信", "传媒", "教育",
                              "家电", "家居", "商超", "零售", "农业", "养殖", "种业"]},
        "growth": {"pe_low": 0.30, "pe_high": 0.80, "pb_low": 0.30, "pb_high": 0.80,
                   "labels": ["科技", "软件", "互联网", "芯片", "半导体", "人工智能", "新能源",
                              "生物", "创新药", "电子", "计算机", "通信", "机器人"]},
        "cyclical": {"pe_low": 0.20, "pe_high": 0.70, "pb_low": 0.20, "pb_high": 0.70,
                     "labels": ["非银", "证券", "保险", "信托", "钢铁", "有色", "化工",
                                "煤炭", "石油", "航运", "造船", "建材", "建筑"]},
        "financial": {"pe_low": 0.30, "pe_high": 0.70, "pb_low": 0.30, "pb_high": 0.70,
                      "labels": ["银行", "金融", "房地产", "地产", "多元金融"]},
    }

    DEFAULT_THRESHOLD = {"pe_low": 0.30, "pe_high": 0.70, "pb_low": 0.30, "pb_high": 0.70}

    CATEGORY_NAMES = {"stable": "稳定消费/公用", "growth": "科技成长", "cyclical": "周期/非银",
                      "financial": "金融/银行", "default": "通用"}

    @classmethod
    def classify_industry(cls, industry_name: Optional[str]) -> Tuple[str, Dict[str, float]]:
        if not industry_name:
            return ("default", cls.DEFAULT_THRESHOLD)

        name = str(industry_name).lower()
        for category, cfg in cls.CATEGORY_THRESHOLDS.items():
            for label in cfg["labels"]:
                if label.lower() in name:
                    return (category, {
                        "pe_low": cfg["pe_low"], "pe_high": cfg["pe_high"],
                        "pb_low": cfg["pb_low"], "pb_high": cfg["pb_high"],
                    })
        return ("default", cls.DEFAULT_THRESHOLD)

    @staticmethod
    def _safe_float(value, default: Optional[float] = None) -> Optional[float]:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _calc_percentile(history: List[float], current: float) -> Optional[float]:
        if not history or current is None:
            return None
        try:
            valid = [v for v in history if v is not None and v > 0]
            if not valid:
                return None
            below = sum(1 for v in valid if v < current)
            return round(below / len(valid) * 100, 1)
        except Exception:
            return None

    @staticmethod
    def analyze(valuation_data: Dict, financial_data: Optional[Dict] = None) -> ValuationSignals:
        logger.info("[ValuationSkill] 开始机构级估值分析")

        available = []
        unavailable = []

        if not valuation_data or not isinstance(valuation_data, dict):
            logger.warning("[ValuationSkill] valuation_data为空")
            return ValuationSignals(
                metrics=ValuationMetrics(None, None, None, None, None, None, "default", 30.0, 70.0, 30.0, 70.0, False),
                data_available_fields=[],
                data_unavailable_fields=["所有估值数据"],
                overall_score=50,
                valuation_grade="数据不足-无法评级",
                research_advice="估值数据不可用，无法进行分析",
                risk_warnings=["估值数据缺失，无法评估估值水平"]
            )

        if valuation_data.get("_data_unavailable"):
            logger.warning("[ValuationSkill] 估值数据标记为不可用")
            return ValuationSignals(
                metrics=ValuationMetrics(None, None, None, None, None, None, "default", 30.0, 70.0, 30.0, 70.0, False),
                data_available_fields=[],
                data_unavailable_fields=["估值数据(API返回不可用)"],
                overall_score=50,
                valuation_grade="数据不足-无法评级",
                research_advice="估值数据不可用，无法进行分析",
                risk_warnings=["估值数据缺失，无法评估估值水平"]
            )

        price = ValuationSkill._safe_float(valuation_data.get("price"))
        pe_ttm = ValuationSkill._safe_float(valuation_data.get("pe_ttm"))
        pb = ValuationSkill._safe_float(valuation_data.get("pb"))
        pe_history = valuation_data.get("pe_history", [])
        pb_history = valuation_data.get("pb_history", [])
        pe_10_avg = ValuationSkill._safe_float(valuation_data.get("pe_10_avg"))

        if price is not None:
            available.append(f"当前股价({price})")
        else:
            unavailable.append("当前股价")

        if pe_ttm is not None:
            available.append(f"PE_TTM({pe_ttm})")
        else:
            unavailable.append("PE_TTM")

        if pb is not None:
            available.append(f"PB({pb})")
        else:
            unavailable.append("PB")

        pe_percentile = None
        if pe_ttm is not None and pe_history:
            pe_percentile = ValuationSkill._calc_percentile(pe_history, pe_ttm)
            if pe_percentile is not None:
                available.append(f"PE分位数({pe_percentile}%)")

        pb_percentile = None
        if pb is not None and pb_history:
            pb_percentile = ValuationSkill._calc_percentile(pb_history, pb)
            if pb_percentile is not None:
                available.append(f"PB分位数({pb_percentile}%)")

        if not pe_history:
            unavailable.append("PE历史数据")
        if not pb_history:
            unavailable.append("PB历史数据")

        unavailable.append("DCF估值(缺少自由现金流数据)")
        unavailable.append("股票风格分类(缺少Beta/股息率数据)")

        industry_name = None
        industry_category = "default"
        if financial_data and isinstance(financial_data, dict):
            basic_info = financial_data.get("basic_info", {})
            if isinstance(basic_info, dict):
                industry_name = basic_info.get("行业")
        category_key, thresholds = ValuationSkill.classify_industry(industry_name)
        industry_category = category_key

        has_data = len(available) > 0
        if not has_data:
            return ValuationSignals(
                metrics=ValuationMetrics(price, pe_ttm, pb, pe_percentile, pb_percentile, pe_10_avg,
                                       industry_category, thresholds["pe_low"] * 100, thresholds["pe_high"] * 100,
                                       thresholds["pb_low"] * 100, thresholds["pb_high"] * 100, False),
                data_available_fields=[],
                data_unavailable_fields=unavailable,
                overall_score=50,
                valuation_grade="数据不足-无法评级",
                research_advice="估值数据不足，无法进行估值分析",
                risk_warnings=["估值数据严重缺失"]
            )

        score = 50
        warnings = []
        advice_parts = []

        if pe_ttm is not None:
            if pe_ttm < 0:
                warnings.append(f"PE_TTM为负({pe_ttm:.1f})，公司处于亏损状态")
                score -= 20
            elif pe_ttm > 100:
                warnings.append(f"PE_TTM极高({pe_ttm:.1f})，估值可能存在泡沫")
                score -= 15
            elif pe_ttm > 50:
                advice_parts.append(f"PE_TTM={pe_ttm:.1f}，估值偏高")
                score -= 5
            elif pe_ttm > 20:
                advice_parts.append(f"PE_TTM={pe_ttm:.1f}，估值合理")
                score += 5
            else:
                advice_parts.append(f"PE_TTM={pe_ttm:.1f}，估值偏低")
                score += 10

        if pb is not None:
            if pb < 0:
                warnings.append(f"PB为负({pb:.2f})，净资产为负")
                score -= 20
            elif pb > 10:
                advice_parts.append(f"PB={pb:.2f}，市净率较高")
                score -= 5
            elif pb < 1:
                advice_parts.append(f"PB={pb:.2f}，破净状态")
                score += 5

        category_label = ValuationSkill.CATEGORY_NAMES.get(industry_category, "通用")

        if pe_percentile is not None:
            pe_low = thresholds["pe_low"] * 100
            pe_high = thresholds["pe_high"] * 100
            if pe_percentile < pe_low:
                advice_parts.append(f"PE处于历史{pe_percentile}%分位({category_label}低估阈值<{pe_low:.0f}%)，估值处于历史低位")
                score += 10
            elif pe_percentile > pe_high:
                advice_parts.append(f"PE处于历史{pe_percentile}%分位({category_label}高估阈值>{pe_high:.0f}%)，估值处于历史高位")
                score -= 10
            else:
                advice_parts.append(f"PE处于历史{pe_percentile}%分位，在{category_label}合理区间内")

        if pb_percentile is not None:
            pb_low = thresholds["pb_low"] * 100
            pb_high = thresholds["pb_high"] * 100
            if pb_percentile < pb_low:
                advice_parts.append(f"PB处于历史{pb_percentile}%分位({category_label}低估阈值<{pb_low:.0f}%)，估值处于历史低位")
                score += 5
            elif pb_percentile > pb_high:
                advice_parts.append(f"PB处于历史{pb_percentile}%分位({category_label}高估阈值>{pb_high:.0f}%)，估值处于历史高位")
                score -= 5

        if not warnings:
            warnings.append("基于现有数据的估值分析：未发现明显估值风险")

        if not advice_parts:
            advice_parts.append("估值数据有限，建议补充数据后深入分析")

        score = max(0, min(100, score))

        if score >= 80:
            grade = "A级-估值偏低"
        elif score >= 65:
            grade = "B级-估值合理偏低"
        elif score >= 50:
            grade = "C级-估值合理"
        elif score >= 35:
            grade = "D级-估值偏高"
        else:
            grade = "E级-估值过高"

        advice = " | ".join(advice_parts)
        logger.info(f"[ValuationSkill] 分析完成 | PE: {pe_ttm} | PB: {pb} | 评分: {score} | 可用: {available}")

        if industry_name:
            advice_parts.insert(0, f"所属行业: {industry_name}({category_label})")
            available.append(f"行业分类({category_label})")

        logger.info(f"[ValuationSkill] 分析完成 | PE: {pe_ttm} | PB: {pb} | 评分: {score} | 行业类别: {category_label} | 可用: {available}")

        return ValuationSignals(
            metrics=ValuationMetrics(price, pe_ttm, pb, pe_percentile, pb_percentile, pe_10_avg,
                                   industry_category, thresholds["pe_low"] * 100, thresholds["pe_high"] * 100,
                                   thresholds["pb_low"] * 100, thresholds["pb_high"] * 100, True),
            data_available_fields=available,
            data_unavailable_fields=unavailable,
            overall_score=score,
            valuation_grade=grade,
            research_advice=advice,
            risk_warnings=warnings
        )


valuation_skill = ValuationSkill()