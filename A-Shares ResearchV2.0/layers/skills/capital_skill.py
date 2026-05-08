import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger("CapitalSkill")


class CapitalTrend(Enum):
    STRONG_INFLOW = "强流入"
    INFLOW = "流入"
    WEAK_INFLOW = "弱流入"
    NEUTRAL = "中性"
    WEAK_OUTFLOW = "弱流出"
    OUTFLOW = "流出"
    STRONG_OUTFLOW = "强流出"


@dataclass
class NorthFlowMetrics:
    net_inflow: float
    net_inflow_pct: float
    cumulative_5d: float
    cumulative_10d: float
    cumulative_30d: float
    trend_5d: str
    trend_10d: str
    trend_30d: str
    signal: str
    consistency: float


@dataclass
class MainFundMetrics:
    net_inflow: float
    net_inflow_pct: float
    large_order_ratio: float
    medium_order_ratio: float
    small_order_ratio: float
    cumulative_5d: float
    cumulative_10d: float
    trend_5d: str
    trend_10d: str
    signal: str
    institutional_intent: str


@dataclass
class MarginMetrics:
    margin_balance: float
    margin_change_pct: float
    short_balance: float
    short_change_pct: float
    net_leverage: float
    leverage_signal: str
    risk_level: str


@dataclass
class DragonMetrics:
    active_days_30d: int
    institutional_buy_ratio: float
    institutional_sell_ratio: float
    net_institutional_flow: float
    top5_seat_ratio: float
    institutional_buy_pct: float
    retail_buy_pct: float
    consecutive_days: int
    top_buyer_type: str
    signal: str
    hot_money_trace: str


@dataclass
class FundFlowStructure:
    north_trend: CapitalTrend
    main_trend: CapitalTrend
    margin_trend: CapitalTrend
    dragon_trend: CapitalTrend
    consensus_level: float
    divergence_warning: str


@dataclass
class CapitalSignals:
    north: NorthFlowMetrics
    main: MainFundMetrics
    margin: MarginMetrics
    dragon: DragonMetrics
    flow_structure: FundFlowStructure
    overall_score: int
    capital_grade: str
    risk_signal: str
    research_advice: str
    divergence_signals: list


class CapitalSkill:
    """
    机构级资金面技能层
    覆盖：北向资金/主力资金/融资融券/龙虎榜/资金共识度
    标准：券商研究所资金流向分析框架 + 机构持仓追踪体系
    """

    @staticmethod
    def _safe_float(value, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _extract_value(data_item, key: str = None) -> float:
        if data_item is None:
            return 0.0
        if isinstance(data_item, dict):
            if not data_item:
                return 0.0
            if key and key in data_item:
                return CapitalSkill._safe_float(data_item[key])
            for k, v in data_item.items():
                if isinstance(v, (int, float)) and ("净" in str(k) or "流入" in str(k) or "额" in str(k)):
                    return CapitalSkill._safe_float(v)
            numeric_vals = [v for v in data_item.values() if isinstance(v, (int, float))]
            return CapitalSkill._safe_float(numeric_vals[0]) if numeric_vals else 0.0
        if isinstance(data_item, (list, tuple)):
            return CapitalSkill._safe_float(data_item[0]) if len(data_item) > 0 else 0.0
        return CapitalSkill._safe_float(data_item)

    @staticmethod
    def analyze_north_flow(capital_data: Dict) -> NorthFlowMetrics:
        north = capital_data.get("north", [])
        if not isinstance(north, list) or len(north) < 5:
            return NorthFlowMetrics(0, 0, 0, 0, 0, "数据不足", "数据不足", "数据不足", "数据不足", 0)

        values = [CapitalSkill._extract_value(x) for x in north]
        latest = values[-1]
        avg_5 = sum(values[-5:]) / 5
        avg_10 = sum(values[-10:]) / 10 if len(values) >= 10 else avg_5
        avg_30 = sum(values[-30:]) / 30 if len(values) >= 30 else avg_10

        cum_5d = sum(values[-5:])
        cum_10d = sum(values[-10:]) if len(values) >= 10 else cum_5d
        cum_30d = sum(values[-30:]) if len(values) >= 30 else cum_10d

        trend_5d = "持续流入" if all(v > 0 for v in values[-5:]) else "持续流出" if all(v < 0 for v in values[-5:]) else "震荡"
        trend_10d = "趋势流入" if cum_10d > 0 else "趋势流出" if cum_10d < 0 else "平衡"
        trend_30d = "趋势流入" if cum_30d > 0 else "趋势流出" if cum_30d < 0 else "平衡"

        pct = round((latest / avg_5 * 100) - 100 if avg_5 != 0 else 0, 2)

        if latest > 0 and avg_5 > 0 and avg_10 > 0:
            signal = "积极"
        elif latest < 0 and avg_5 < 0:
            signal = "谨慎"
        else:
            signal = "观望"

        positive_days = sum(1 for v in values[-10:] if v > 0)
        consistency = round(positive_days / len(values[-10:]) * 100, 2) if values else 0

        return NorthFlowMetrics(
            net_inflow=round(latest, 2),
            net_inflow_pct=pct,
            cumulative_5d=round(cum_5d, 2),
            cumulative_10d=round(cum_10d, 2),
            cumulative_30d=round(cum_30d, 2),
            trend_5d=trend_5d,
            trend_10d=trend_10d,
            trend_30d=trend_30d,
            signal=signal,
            consistency=consistency
        )

    @staticmethod
    def analyze_main_fund(capital_data: Dict) -> MainFundMetrics:
        main = capital_data.get("main", [])
        if not isinstance(main, list) or len(main) < 5:
            return MainFundMetrics(0, 0, 0, 0, 0, 0, 0, "数据不足", "数据不足", "数据不足", "数据不足")

        values = [CapitalSkill._extract_value(x, "主力净流入") for x in main]
        latest = values[-1]
        cum_5d = sum(values[-5:])
        cum_10d = sum(values[-10:]) if len(values) >= 10 else cum_5d

        large_ratio = capital_data.get("large_order_ratio", 0.3)
        medium_ratio = capital_data.get("medium_order_ratio", 0.4)
        small_ratio = 1 - large_ratio - medium_ratio

        trend_5d = "吸筹" if cum_5d > 0 else "派发" if cum_5d < 0 else "盘整"
        trend_10d = "资金加仓" if cum_10d > cum_5d else "资金减仓" if cum_10d < cum_5d else "持仓稳定"

        if latest > 0 and cum_5d > 0 and cum_10d > 0:
            signal = "强势"
            intent = "机构持续建仓"
        elif latest < 0 and cum_5d < 0:
            signal = "弱势"
            intent = "机构持续减仓"
        elif latest > 0 and cum_5d < 0:
            signal = "反弹"
            intent = "短期博弈"
        else:
            signal = "观望"
            intent = "方向不明"

        return MainFundMetrics(
            net_inflow=round(latest, 2),
            net_inflow_pct=round((latest / abs(cum_5d) * 100) if cum_5d != 0 else 0, 2),
            large_order_ratio=round(large_ratio, 2),
            medium_order_ratio=round(medium_ratio, 2),
            small_order_ratio=round(small_ratio, 2),
            cumulative_5d=round(cum_5d, 2),
            cumulative_10d=round(cum_10d, 2),
            trend_5d=trend_5d,
            trend_10d=trend_10d,
            signal=signal,
            institutional_intent=intent
        )

    @staticmethod
    def analyze_margin(capital_data: Dict) -> MarginMetrics:
        margin = capital_data.get("margin", [])
        if not isinstance(margin, list) or len(margin) < 2:
            return MarginMetrics(0, 0, 0, 0, 0, "数据不足", "低风险")

        latest_bal = CapitalSkill._extract_value(margin[-1], "融资余额")
        prev_bal = CapitalSkill._extract_value(margin[-2], "融资余额") if len(margin) >= 2 else latest_bal
        change_pct = round((latest_bal - prev_bal) / prev_bal * 100, 2) if prev_bal > 0 else 0

        short_bal = CapitalSkill._safe_float(capital_data.get("short_balance", 0))
        short_change = CapitalSkill._safe_float(capital_data.get("short_change_pct", 0))

        net_leverage = round(latest_bal / (short_bal + 1), 2)

        if change_pct > 5:
            signal = "融资大幅加仓"
        elif change_pct > 2:
            signal = "融资加仓"
        elif change_pct < -5:
            signal = "融资大幅减仓"
        elif change_pct < -2:
            signal = "融资减仓"
        else:
            signal = "融资平稳"

        if net_leverage > 5:
            risk = "高杠杆风险"
        elif net_leverage > 2:
            risk = "中等杠杆"
        else:
            risk = "低杠杆"

        return MarginMetrics(
            margin_balance=round(latest_bal, 2),
            margin_change_pct=change_pct,
            short_balance=round(short_bal, 2),
            short_change_pct=round(short_change, 2),
            net_leverage=net_leverage,
            leverage_signal=signal,
            risk_level=risk
        )

    @staticmethod
    def analyze_dragon(capital_data: Dict) -> DragonMetrics:
        dragon = capital_data.get("dragon", [])
        if not isinstance(dragon, list) or not dragon:
            return DragonMetrics(0, 0, 0, 0, 0, 0, 0, 0, "无数据", "无数据", "无游资痕迹")

        active_days = len(dragon)
        inst_buy = sum(CapitalSkill._safe_float(d.get("inst_buy", d.get("机构买入", 0))) for d in dragon)
        inst_sell = sum(CapitalSkill._safe_float(d.get("inst_sell", d.get("机构卖出", 0))) for d in dragon)
        net_inst = inst_buy - inst_sell

        total_buy = sum(CapitalSkill._safe_float(d.get("total_buy", d.get("买入总计", 1))) for d in dragon)
        top5_ratio = (inst_buy / total_buy * 100) if total_buy > 0 else 0

        retail_buy = sum(CapitalSkill._safe_float(d.get("retail_buy", d.get("游资买入", 0))) for d in dragon)
        inst_buy_pct = (inst_buy / total_buy * 100) if total_buy > 0 else 0
        retail_buy_pct = (retail_buy / total_buy * 100) if total_buy > 0 else 0

        consecutive = 0
        if dragon:
            dates = sorted([d.get("date", d.get("日期", "")) for d in dragon], reverse=True)
            consecutive = 1
            for i in range(1, len(dates)):
                if dates[i-1] and dates[i]:
                    consecutive += 1
                else:
                    break

        if inst_buy_pct > retail_buy_pct:
            top_type = "机构主导"
        elif retail_buy_pct > inst_buy_pct:
            top_type = "游资主导"
        else:
            top_type = "均衡博弈"

        if net_inst > 0 and active_days >= 3:
            signal = "机构活跃"
        elif net_inst < 0:
            signal = "机构撤退"
        else:
            signal = "游资博弈"

        if active_days >= 5 and top5_ratio > 30:
            hot_money = "强游资介入"
        elif active_days >= 3:
            hot_money = "游资关注"
        else:
            hot_money = "游资冷淡"

        return DragonMetrics(
            active_days_30d=active_days,
            institutional_buy_ratio=round(inst_buy, 2),
            institutional_sell_ratio=round(inst_sell, 2),
            net_institutional_flow=round(net_inst, 2),
            top5_seat_ratio=round(top5_ratio, 2),
            institutional_buy_pct=round(inst_buy_pct, 2),
            retail_buy_pct=round(retail_buy_pct, 2),
            consecutive_days=consecutive,
            top_buyer_type=top_type,
            signal=signal,
            hot_money_trace=hot_money
        )

    @staticmethod
    def _classify_trend(net_flow: float, cumulative_5d: float, consistency: float) -> CapitalTrend:
        if net_flow > 0 and cumulative_5d > 0 and consistency > 70:
            return CapitalTrend.STRONG_INFLOW
        elif net_flow > 0 and cumulative_5d > 0:
            return CapitalTrend.INFLOW
        elif net_flow > 0 or cumulative_5d > 0:
            return CapitalTrend.WEAK_INFLOW
        elif net_flow < 0 and cumulative_5d < 0 and consistency < 30:
            return CapitalTrend.STRONG_OUTFLOW
        elif net_flow < 0 and cumulative_5d < 0:
            return CapitalTrend.OUTFLOW
        elif net_flow < 0 or cumulative_5d < 0:
            return CapitalTrend.WEAK_OUTFLOW
        else:
            return CapitalTrend.NEUTRAL

    @staticmethod
    def analyze_flow_structure(north: NorthFlowMetrics, main: MainFundMetrics,
                                margin: MarginMetrics, dragon: DragonMetrics) -> FundFlowStructure:
        north_trend = CapitalSkill._classify_trend(north.net_inflow, north.cumulative_5d, north.consistency)
        main_trend = CapitalSkill._classify_trend(main.net_inflow, main.cumulative_5d, 50)

        margin_trend = CapitalTrend.NEUTRAL
        if margin.margin_change_pct > 3:
            margin_trend = CapitalTrend.INFLOW
        elif margin.margin_change_pct < -3:
            margin_trend = CapitalTrend.OUTFLOW

        dragon_trend = CapitalTrend.NEUTRAL
        if dragon.net_institutional_flow > 0:
            dragon_trend = CapitalTrend.INFLOW
        elif dragon.net_institutional_flow < 0:
            dragon_trend = CapitalTrend.OUTFLOW

        trends = [north_trend, main_trend, margin_trend, dragon_trend]
        inflow_count = sum(1 for t in trends if t in [CapitalTrend.STRONG_INFLOW, CapitalTrend.INFLOW, CapitalTrend.WEAK_INFLOW])
        outflow_count = sum(1 for t in trends if t in [CapitalTrend.STRONG_OUTFLOW, CapitalTrend.OUTFLOW, CapitalTrend.WEAK_OUTFLOW])

        if inflow_count >= 3:
            consensus = 0.9
            divergence = "资金高度共识-流入"
        elif inflow_count >= 2:
            consensus = 0.7
            divergence = "资金偏向流入"
        elif outflow_count >= 3:
            consensus = 0.1
            divergence = "资金高度共识-流出"
        elif outflow_count >= 2:
            consensus = 0.3
            divergence = "资金偏向流出"
        else:
            consensus = 0.5
            divergence = "资金分歧较大"

        return FundFlowStructure(
            north_trend=north_trend,
            main_trend=main_trend,
            margin_trend=margin_trend,
            dragon_trend=dragon_trend,
            consensus_level=round(consensus, 2),
            divergence_warning=divergence
        )

    @staticmethod
    def _empty_signals() -> "CapitalSignals":
        return CapitalSignals(
            north=NorthFlowMetrics(0, 0, 0, 0, 0, "无数据", "无数据", "无数据", "无数据", 0),
            main=MainFundMetrics(0, 0, 0, 0, 0, 0, 0, "无数据", "无数据", "无数据", "无数据"),
            margin=MarginMetrics(0, 0, 0, 0, 0, "无数据", "低风险"),
            dragon=DragonMetrics(0, 0, 0, 0, 0, 0, 0, 0, "无数据", "无数据", "无数据"),
            flow_structure=FundFlowStructure(
                north_trend=CapitalTrend.NEUTRAL, main_trend=CapitalTrend.NEUTRAL,
                margin_trend=CapitalTrend.NEUTRAL, dragon_trend=CapitalTrend.NEUTRAL,
                consensus_level=0.5, divergence_warning="数据不足"
            ),
            overall_score=50, capital_grade="无评级", risk_signal="数据异常",
            research_advice="数据不足，无法研判", divergence_signals=[]
        )

    @staticmethod
    def analyze(capital_data: Dict) -> CapitalSignals:
        logger.info("[CapitalSkill] 开始机构级资金面分析")

        if not capital_data or not isinstance(capital_data, dict):
            logger.warning("[CapitalSkill] capital_data为空或类型异常，返回默认空信号")
            return CapitalSkill._empty_signals()

        try:
            north = CapitalSkill.analyze_north_flow(capital_data)
        except Exception as e:
            logger.error(f"[CapitalSkill] 北向资金分析异常: {e}")
            north = NorthFlowMetrics(0, 0, 0, 0, 0, "异常", "异常", "异常", "异常", 0)

        try:
            main = CapitalSkill.analyze_main_fund(capital_data)
        except Exception as e:
            logger.error(f"[CapitalSkill] 主力资金分析异常: {e}")
            main = MainFundMetrics(0, 0, 0, 0, 0, 0, 0, "异常", "异常", "异常", "异常")

        try:
            margin = CapitalSkill.analyze_margin(capital_data)
        except Exception as e:
            logger.error(f"[CapitalSkill] 融资融券分析异常: {e}")
            margin = MarginMetrics(0, 0, 0, 0, 0, "异常", "低风险")

        try:
            dragon = CapitalSkill.analyze_dragon(capital_data)
        except Exception as e:
            logger.error(f"[CapitalSkill] 龙虎榜分析异常: {e}")
            dragon = DragonMetrics(0, 0, 0, 0, 0, 0, 0, 0, "异常", "异常", "异常")

        flow_structure = CapitalSkill.analyze_flow_structure(north, main, margin, dragon)

        divergence_signals = []
        try:
            if north.signal == "积极" and main.signal == "弱势":
                divergence_signals.append("北向流入但主力流出，外资与内资背离")
            elif north.signal == "谨慎" and main.signal == "强势":
                divergence_signals.append("北向流出但主力流入，外资与内资背离")
            if north.signal == "积极" and margin.leverage_signal in ["融资大幅减仓", "融资减仓"]:
                divergence_signals.append("北向流入但融资减仓，外资与杠杆资金背离")
            if main.signal == "强势" and margin.leverage_signal in ["融资大幅减仓", "融资减仓"]:
                divergence_signals.append("主力流入但融资减仓，机构与杠杆资金背离")
            if dragon.signal == "机构撤退" and main.signal == "强势":
                divergence_signals.append("龙虎榜机构撤退但主力流入，席位与资金背离")
            if north.consistency > 60 and main.signal == "弱势":
                divergence_signals.append("北向持续流入但主力持续流出，趋势背离")
        except Exception as e:
            logger.error(f"[CapitalSkill] 背离信号检测异常: {e}")

        try:
            score = 50
            if north.signal == "积极":
                score += int(25 * 0.4)
            elif north.signal == "谨慎":
                score -= int(25 * 0.4)

            if main.signal == "强势":
                score += int(35 * 0.43)
            elif main.signal == "弱势":
                score -= int(35 * 0.43)
            elif main.signal == "反弹":
                score += int(35 * 0.14)

            if margin.leverage_signal in ["融资大幅加仓", "融资加仓"]:
                score += int(20 * 0.25)
            elif margin.leverage_signal in ["融资大幅减仓", "融资减仓"]:
                score -= int(20 * 0.25)

            if dragon.signal == "机构活跃":
                score += int(20 * 0.5)
            elif dragon.signal == "机构撤退":
                score -= int(20 * 0.5)

            if flow_structure.consensus_level > 0.7:
                score += 10
            elif flow_structure.consensus_level < 0.3:
                score -= 10

            if divergence_signals:
                score -= len(divergence_signals) * 3

            score = max(0, min(100, score))
        except Exception as e:
            logger.error(f"[CapitalSkill] 评分计算异常: {e}")
            score = 50

        try:
            if score >= 80:
                grade = "A级-资金追捧"
            elif score >= 65:
                grade = "B级-资金流入"
            elif score >= 50:
                grade = "C级-资金平衡"
            elif score >= 35:
                grade = "D级-资金流出"
            else:
                grade = "E级-资金逃离"

            risk = ""
            if divergence_signals:
                risk = "资金背离：" + "；".join(divergence_signals[:2])
            elif flow_structure.divergence_warning == "资金分歧较大":
                risk = "资金分歧，注意方向选择"
            elif north.consistency < 30 and main.signal == "弱势":
                risk = "内外资同步流出"
            elif margin.risk_level == "高杠杆风险":
                risk = "融资杠杆过高，波动风险大"
            else:
                risk = "资金面暂无重大风险"

            advice_parts = []
            if flow_structure.consensus_level > 0.7:
                advice_parts.append("资金高度共识，趋势明确")
            if main.institutional_intent == "机构持续建仓":
                advice_parts.append("主力吸筹明显")
            if north.cumulative_30d > 0 and north.consistency > 60:
                advice_parts.append("北向持续流入，外资看好")
            if dragon.hot_money_trace == "强游资介入":
                advice_parts.append("游资活跃，短期弹性大")
            if divergence_signals:
                advice_parts.append("资金背离需警惕，建议观望确认方向")
            if not advice_parts:
                advice_parts.append("资金面信号中性")

            advice = " | ".join(advice_parts)
        except Exception as e:
            logger.error(f"[CapitalSkill] 评级/建议生成异常: {e}")
            grade = "无评级"
            risk = "研判异常"
            advice = "数据异常，无法研判"
        logger.info(f"[CapitalSkill] 分析完成 | 资金评级: {grade} | 评分: {score} | 背离信号: {len(divergence_signals)}个")

        return CapitalSignals(
            north=north,
            main=main,
            margin=margin,
            dragon=dragon,
            flow_structure=flow_structure,
            overall_score=score,
            capital_grade=grade,
            risk_signal=risk,
            research_advice=advice,
            divergence_signals=divergence_signals
        )


capital_skill = CapitalSkill()
