import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger("TechSkill")


class TrendStrength(Enum):
    STRONG_BULL = "强多头"
    BULL = "多头"
    WEAK_BULL = "弱多头"
    NEUTRAL = "震荡"
    WEAK_BEAR = "弱空头"
    BEAR = "空头"
    STRONG_BEAR = "强空头"


class VolumeSignal(Enum):
    HEAVY_UP = "放量上涨"
    HEAVY_DOWN = "放量下跌"
    LIGHT_UP = "缩量上涨"
    LIGHT_DOWN = "缩量下跌"
    HEAVY_STAGNANT = "放量滞涨"
    LIGHT_PULLBACK = "缩量回调"
    NORMAL = "量价正常"
    DIVERGENCE_TOP = "顶背离"
    DIVERGENCE_BOTTOM = "底背离"


class MarketRegime(Enum):
    STRONG_TREND = "强趋势"
    WEAK_TREND = "弱趋势"
    RANGING = "震荡"
    UNKNOWN = "未分类"


@dataclass
class DivergenceSignals:
    macd_bearish: bool
    macd_bullish: bool
    rsi_bearish: bool
    rsi_bullish: bool
    volume_bearish: bool
    volume_bullish: bool
    divergence_count: int
    divergence_summary: str


@dataclass
class MASystem:
    ma5: float
    ma10: float
    ma20: float
    ma60: float
    ma120: float
    ma250: float
    ma5_slope: float
    ma10_slope: float
    ma20_slope: float
    ma60_slope: float
    golden_cross: List[str]
    dead_cross: List[str]
    arrangement: str


@dataclass
class MACDSystem:
    dif: float
    dea: float
    macd_hist: float
    hist_trend: str
    golden_cross: bool
    dead_cross: bool
    divergence: str
    momentum: str


@dataclass
class KDJSystem:
    k: float
    d: float
    j: float
    overbought: bool
    oversold: bool
    golden_cross: bool
    dead_cross: bool


@dataclass
class BollingerSystem:
    upper: float
    middle: float
    lower: float
    bandwidth: float
    position: str
    squeeze: bool
    breakout: str


@dataclass
class VolumeStructure:
    volume_ratio: float
    turnover_rate: float
    volume_percentile: float
    up_volume_ratio: float
    down_volume_ratio: float
    institutional_signal: str
    volume_trend: str
    signal: VolumeSignal


@dataclass
class SupportResistance:
    strong_support: float
    support_1: float
    support_2: float
    resistance_1: float
    resistance_2: float
    strong_resistance: float
    current_position: str
    break_probability: float


@dataclass
class TechSignals:
    ma_system: MASystem
    macd_system: MACDSystem
    kdj_system: KDJSystem
    bollinger_system: BollingerSystem
    volume_structure: VolumeStructure
    support_resistance: SupportResistance
    divergence_signals: DivergenceSignals
    trend_strength: TrendStrength
    market_regime: MarketRegime
    overall_score: int
    short_term_signal: str
    medium_term_signal: str
    risk_warning: str
    research_advice: str


class TechSkill:
    """
    机构级技术面技能层
    覆盖：均线系统/MACD/KDJ/布林带/量价结构/支撑阻力/趋势强度
    标准：券商研究所量化分析框架
    """

    @staticmethod
    def _calc_ma(data: List[float], period: int) -> List[float]:
        if not data:
            return []
        if len(data) < period:
            avg = sum(data) / len(data)
            return [avg] * len(data)
        ma = []
        for i in range(len(data)):
            if i < period - 1:
                ma.append(sum(data[:i+1]) / (i+1))
            else:
                ma.append(sum(data[i-period+1:i+1]) / period)
        return ma

    @staticmethod
    def _calc_ema(data: List[float], period: int) -> List[float]:
        if not data:
            return []
        multiplier = 2 / (period + 1)
        ema = [data[0]]
        for i in range(1, len(data)):
            ema.append((data[i] - ema[-1]) * multiplier + ema[-1])
        return ema

    @staticmethod
    def _calc_rsi(data: List[float], period: int = 14) -> float:
        if len(data) < period + 1:
            return 50.0
        gains = []
        losses = []
        for i in range(1, len(data)):
            change = data[i] - data[i-1]
            gains.append(max(change, 0))
            losses.append(abs(min(change, 0)))
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    @staticmethod
    def _calc_rsi_series(data: List[float], period: int = 14) -> List[float]:
        if len(data) < period + 1:
            return [50.0] * len(data)
        rsi_series = []
        for i in range(period, len(data)):
            rsi_series.append(TechSkill._calc_rsi(data[:i+1], period))
        return [50.0] * period + rsi_series

    @staticmethod
    def detect_rsi_divergence(closes: List[float], period: int = 14) -> Tuple[bool, bool]:
        if len(closes) < 40:
            return False, False
        rsi_series = TechSkill._calc_rsi_series(closes, period)
        if len(rsi_series) < 40:
            return False, False

        price_segment = closes[-40:]
        rsi_segment = rsi_series[-40:]

        bearish = False
        price_highs = []
        rsi_at_highs = []
        for i in range(5, len(price_segment) - 5):
            if price_segment[i] == max(price_segment[i-5:i+6]):
                price_highs.append(price_segment[i])
                rsi_at_highs.append(rsi_segment[i])
        if len(price_highs) >= 2:
            if price_highs[-1] > price_highs[-2] and rsi_at_highs[-1] < rsi_at_highs[-2]:
                bearish = True

        bullish = False
        price_lows = []
        rsi_at_lows = []
        for i in range(5, len(price_segment) - 5):
            if price_segment[i] == min(price_segment[i-5:i+6]):
                price_lows.append(price_segment[i])
                rsi_at_lows.append(rsi_segment[i])
        if len(price_lows) >= 2:
            if price_lows[-1] < price_lows[-2] and rsi_at_lows[-1] > rsi_at_lows[-2]:
                bullish = True

        return bearish, bullish

    @staticmethod
    def analyze_divergence(df_data: List[Dict], macd_system: "MACDSystem",
                           volume_structure: "VolumeStructure") -> DivergenceSignals:
        closes = [d.get("close", 0) for d in df_data if d.get("close") is not None]
        if not closes or len(closes) < 40:
            return DivergenceSignals(False, False, False, False, False, False, 0, "数据不足")

        macd_bearish = macd_system.divergence == "顶背离"
        macd_bullish = macd_system.divergence == "底背离"

        rsi_bearish, rsi_bullish = TechSkill.detect_rsi_divergence(closes)

        vol_bearish = volume_structure.signal == VolumeSignal.DIVERGENCE_TOP
        vol_bullish = volume_structure.signal == VolumeSignal.DIVERGENCE_BOTTOM

        count = sum([macd_bearish, macd_bullish, rsi_bearish, rsi_bullish, vol_bearish, vol_bullish])

        parts = []
        if macd_bearish:
            parts.append("MACD顶背离")
        if macd_bullish:
            parts.append("MACD底背离")
        if rsi_bearish:
            parts.append("RSI顶背离")
        if rsi_bullish:
            parts.append("RSI底背离")
        if vol_bearish:
            parts.append("量价顶背离")
        if vol_bullish:
            parts.append("量价底背离")

        summary = "；".join(parts) if parts else "无背离信号"

        return DivergenceSignals(
            macd_bearish=macd_bearish, macd_bullish=macd_bullish,
            rsi_bearish=rsi_bearish, rsi_bullish=rsi_bullish,
            volume_bearish=vol_bearish, volume_bullish=vol_bullish,
            divergence_count=count, divergence_summary=summary
        )

    @staticmethod
    def analyze_ma_system(df_data: List[Dict]) -> MASystem:
        closes = [d.get("close", 0) for d in df_data if d.get("close") is not None]
        
        # 空数据保护
        if not closes:
            logger.warning("收盘价数据为空，返回默认均线分析结果")
            return MASystem(
                ma5=0.0, ma10=0.0, ma20=0.0, ma60=0.0, ma120=0.0, ma250=0.0,
                ma5_slope=0.0, ma10_slope=0.0, ma20_slope=0.0, ma60_slope=0.0,
                golden_cross=[], dead_cross=[], arrangement="数据不足"
            )
        
        if len(closes) < 250:
            logger.warning(f"数据不足250天（当前{len(closes)}天），均线分析受限")

        def safe_ma(data, period):
            ma_series = TechSkill._calc_ma(data, period)
            return ma_series[-1] if ma_series else (data[-1] if data else 0.0)

        ma5 = safe_ma(closes, 5)
        ma10 = safe_ma(closes, 10)
        ma20 = safe_ma(closes, 20)
        ma60 = safe_ma(closes, 60)
        ma120 = safe_ma(closes, 120)
        ma250 = safe_ma(closes, 250)

        ma5_series = TechSkill._calc_ma(closes, 5)
        ma10_series = TechSkill._calc_ma(closes, 10)
        ma20_series = TechSkill._calc_ma(closes, 20)
        ma60_series = TechSkill._calc_ma(closes, 60)

        ma5_slope = ((ma5_series[-1] - ma5_series[-5]) / ma5_series[-5] * 100) if len(ma5_series) >= 5 and ma5_series[-5] != 0 else 0
        ma10_slope = ((ma10_series[-1] - ma10_series[-5]) / ma10_series[-5] * 100) if len(ma10_series) >= 5 and ma10_series[-5] != 0 else 0
        ma20_slope = ((ma20_series[-1] - ma20_series[-10]) / ma20_series[-10] * 100) if len(ma20_series) >= 10 and ma20_series[-10] != 0 else 0
        ma60_slope = ((ma60_series[-1] - ma60_series[-20]) / ma60_series[-20] * 100) if len(ma60_series) >= 20 and ma60_series[-20] != 0 else 0

        golden_cross = []
        dead_cross = []
        if ma5 > ma10 > ma20 > ma60:
            arrangement = "多头排列"
        elif ma5 < ma10 < ma20 < ma60:
            arrangement = "空头排列"
        elif ma5 > ma20 and ma20 > ma60:
            arrangement = "中期多头"
        elif ma5 < ma20 and ma20 < ma60:
            arrangement = "中期空头"
        else:
            arrangement = "缠绕震荡"

        if len(ma5_series) >= 2 and len(ma10_series) >= 2:
            if ma5_series[-2] <= ma10_series[-2] and ma5_series[-1] > ma10_series[-1]:
                golden_cross.append("MA5上穿MA10")
            if ma5_series[-2] >= ma10_series[-2] and ma5_series[-1] < ma10_series[-1]:
                dead_cross.append("MA5下穿MA10")

        return MASystem(
            ma5=round(ma5, 2), ma10=round(ma10, 2), ma20=round(ma20, 2),
            ma60=round(ma60, 2), ma120=round(ma120, 2), ma250=round(ma250, 2),
            ma5_slope=round(ma5_slope, 2), ma10_slope=round(ma10_slope, 2),
            ma20_slope=round(ma20_slope, 2), ma60_slope=round(ma60_slope, 2),
            golden_cross=golden_cross, dead_cross=dead_cross,
            arrangement=arrangement
        )

    @staticmethod
    def analyze_macd(df_data: List[Dict]) -> MACDSystem:
        closes = [d.get("close", 0) for d in df_data if d.get("close") is not None]
        
        # 空数据保护
        if not closes or len(closes) < 26:
            return MACDSystem(0, 0, 0, "数据不足", False, False, "无", "中性")

        ema12 = TechSkill._calc_ema(closes, 12)
        ema26 = TechSkill._calc_ema(closes, 26)
        
        # EMA计算失败保护
        if not ema12 or not ema26 or len(ema12) != len(ema26):
            return MACDSystem(0, 0, 0, "数据不足", False, False, "无", "中性")
            
        dif = [ema12[i] - ema26[i] for i in range(len(ema12))]
        dea = TechSkill._calc_ema(dif, 9)
        
        if not dea or len(dif) != len(dea):
            return MACDSystem(0, 0, 0, "数据不足", False, False, "无", "中性")
            
        macd_hist = [(dif[i] - dea[i]) * 2 for i in range(len(dif))]

        current_dif = dif[-1] if dif else 0
        current_dea = dea[-1] if dea else 0
        current_hist = macd_hist[-1] if macd_hist else 0

        hist_trend = "增强" if (len(macd_hist) >= 5 and current_hist > macd_hist[-5]) else "减弱" if len(macd_hist) >= 5 else "中性"

        golden_cross = len(dif) >= 2 and dif[-2] <= dea[-2] and dif[-1] > dea[-1]
        dead_cross = len(dif) >= 2 and dif[-2] >= dea[-2] and dif[-1] < dea[-1]

        if current_hist > 0 and (len(macd_hist) < 5 or current_hist > macd_hist[-5]):
            momentum = "多头动能增强"
        elif current_hist > 0:
            momentum = "多头动能减弱"
        elif current_hist < 0 and (len(macd_hist) < 5 or current_hist < macd_hist[-5]):
            momentum = "空头动能增强"
        else:
            momentum = "空头动能减弱"

        divergence = "无"
        if len(closes) >= 40 and len(dif) >= 40:
            price_segment = closes[-40:]
            dif_segment = dif[-40:]

            price_highs = []
            dif_at_highs = []
            for i in range(5, len(price_segment) - 5):
                if price_segment[i] == max(price_segment[i-5:i+6]):
                    price_highs.append(price_segment[i])
                    dif_at_highs.append(dif_segment[i])

            if len(price_highs) >= 2:
                if price_highs[-1] > price_highs[-2] and dif_at_highs[-1] < dif_at_highs[-2]:
                    divergence = "顶背离"

            price_lows = []
            dif_at_lows = []
            for i in range(5, len(price_segment) - 5):
                if price_segment[i] == min(price_segment[i-5:i+6]):
                    price_lows.append(price_segment[i])
                    dif_at_lows.append(dif_segment[i])

            if len(price_lows) >= 2:
                if price_lows[-1] < price_lows[-2] and dif_at_lows[-1] > dif_at_lows[-2]:
                    divergence = "底背离"

        return MACDSystem(
            dif=round(current_dif, 3), dea=round(current_dea, 3),
            macd_hist=round(current_hist, 3), hist_trend=hist_trend,
            golden_cross=golden_cross, dead_cross=dead_cross,
            divergence=divergence, momentum=momentum
        )

    @staticmethod
    def analyze_kdj(df_data: List[Dict]) -> KDJSystem:
        if len(df_data) < 9:
            return KDJSystem(50, 50, 50, False, False, False, False)

        highs = [d.get("high", 0) for d in df_data if d.get("high") is not None]
        lows = [d.get("low", 0) for d in df_data if d.get("low") is not None]
        closes = [d.get("close", 0) for d in df_data if d.get("close") is not None]
        
        # 空数据保护
        if not highs or not lows or not closes or len(highs) < 9 or len(lows) < 9 or len(closes) < 9:
            return KDJSystem(50, 50, 50, False, False, False, False)

        rsv_list = []
        for i in range(8, len(closes)):
            period_high = max(highs[i-8:i+1])
            period_low = min(lows[i-8:i+1])
            if period_high == period_low:
                rsv = 50
            else:
                rsv = (closes[i] - period_low) / (period_high - period_low) * 100
            rsv_list.append(rsv)

        k = [50.0]
        d = [50.0]
        for rsv in rsv_list:
            k.append(k[-1] * 2/3 + rsv * 1/3)
            d.append(d[-1] * 2/3 + k[-1] * 1/3)

        current_k = k[-1] if k else 50.0
        current_d = d[-1] if d else 50.0
        current_j = 3 * current_k - 2 * current_d

        overbought = current_k > 80 and current_d > 80
        oversold = current_k < 20 and current_d < 20
        golden_cross = len(k) >= 2 and k[-2] <= d[-2] and k[-1] > d[-1]
        dead_cross = len(k) >= 2 and k[-2] >= d[-2] and k[-1] < d[-1]

        return KDJSystem(
            k=round(current_k, 2), d=round(current_d, 2), j=round(current_j, 2),
            overbought=overbought, oversold=oversold,
            golden_cross=golden_cross, dead_cross=dead_cross
        )

    @staticmethod
    def analyze_bollinger(df_data: List[Dict]) -> BollingerSystem:
        closes = [d.get("close", 0) for d in df_data if d.get("close") is not None]
        
        # 空数据保护
        if not closes or len(closes) < 20:
            return BollingerSystem(0, 0, 0, 0, "数据不足", False, "无")

        ma20 = sum(closes[-20:]) / 20
        variance = sum([(c - ma20) ** 2 for c in closes[-20:]]) / 20
        std = variance ** 0.5

        upper = ma20 + 2 * std
        lower = ma20 - 2 * std
        current = closes[-1]

        bandwidth = (upper - lower) / ma20 * 100 if ma20 != 0 else 0

        if current > upper:
            position = "上轨上方"
        elif current > ma20:
            position = "中轨上方"
        elif current > lower:
            position = "中轨下方"
        else:
            position = "下轨下方"

        squeeze = bandwidth < 5

        if len(closes) >= 2:
            if closes[-2] <= upper and current > upper:
                breakout = "向上突破"
            elif closes[-2] >= lower and current < lower:
                breakout = "向下突破"
            else:
                breakout = "无突破"
        else:
            breakout = "无突破"

        return BollingerSystem(
            upper=round(upper, 2), middle=round(ma20, 2), lower=round(lower, 2),
            bandwidth=round(bandwidth, 2), position=position,
            squeeze=squeeze, breakout=breakout
        )

    @staticmethod
    def analyze_volume_structure(df_data: List[Dict]) -> VolumeStructure:
        if len(df_data) < 20:
            return VolumeStructure(1.0, 0, 50, 1.0, 1.0, "中性", "平稳", VolumeSignal.NORMAL)

        volumes = [d.get("volume", 0) for d in df_data if d.get("volume") is not None]
        closes = [d.get("close", 0) for d in df_data if d.get("close") is not None]
        
        # 空数据保护
        if not volumes or not closes or len(volumes) < 2 or len(closes) < 2:
            return VolumeStructure(1.0, 0, 50, 1.0, 1.0, "中性", "平稳", VolumeSignal.NORMAL)

        avg_volume = sum(volumes[-20:]) / 20
        volume_ratio = volumes[-1] / avg_volume if avg_volume > 0 else 1.0

        turnover_rate = df_data[-1].get("turnover", 0) if df_data else 0

        sorted_volumes = sorted(volumes[-60:])
        percentile = sorted_volumes.index(volumes[-1]) / len(sorted_volumes) * 100 if sorted_volumes else 50

        up_volumes = [volumes[i] for i in range(1, len(closes)) if closes[i] > closes[i-1]]
        down_volumes = [volumes[i] for i in range(1, len(closes)) if closes[i] < closes[i-1]]
        up_avg = sum(up_volumes) / len(up_volumes) if up_volumes else 1
        down_avg = sum(down_volumes) / len(down_volumes) if down_volumes else 1

        up_ratio = up_avg / down_avg if down_avg > 0 else 1.0
        down_ratio = down_avg / up_avg if up_avg > 0 else 1.0

        if len(closes) >= 2:
            price_change_pct = abs(closes[-1] - closes[-2]) / closes[-2] * 100 if closes[-2] != 0 else 0
            if volume_ratio > 2 and closes[-1] > closes[-2]:
                signal = VolumeSignal.HEAVY_UP
            elif volume_ratio > 2 and closes[-1] < closes[-2]:
                signal = VolumeSignal.HEAVY_DOWN
            elif volume_ratio > 1.5 and price_change_pct < 1.0:
                signal = VolumeSignal.HEAVY_STAGNANT
            elif volume_ratio < 0.5 and closes[-1] > closes[-2]:
                signal = VolumeSignal.LIGHT_UP
            elif volume_ratio < 0.5 and closes[-1] < closes[-2]:
                signal = VolumeSignal.LIGHT_DOWN
            elif volume_ratio < 0.7 and closes[-1] < closes[-2] and price_change_pct < 2.0:
                signal = VolumeSignal.LIGHT_PULLBACK
            else:
                signal = VolumeSignal.NORMAL
        else:
            signal = VolumeSignal.NORMAL

        if len(volumes) >= 10 and len(closes) >= 10:
            price_high = max(closes[-10:])
            vol_high_idx = closes.index(price_high) if price_high in closes else -1
            if vol_high_idx >= 0 and volumes[vol_high_idx] < max(volumes[-10:]):
                signal = VolumeSignal.DIVERGENCE_TOP

        volume_trend = "放量" if volume_ratio > 1.5 else "缩量" if volume_ratio < 0.7 else "平稳"

        if up_ratio > 2:
            inst_signal = "机构吸筹"
        elif down_ratio > 2:
            inst_signal = "机构派发"
        else:
            inst_signal = "中性"

        return VolumeStructure(
            volume_ratio=round(volume_ratio, 2),
            turnover_rate=round(turnover_rate, 2),
            volume_percentile=round(percentile, 2),
            up_volume_ratio=round(up_ratio, 2),
            down_volume_ratio=round(down_ratio, 2),
            institutional_signal=inst_signal,
            volume_trend=volume_trend,
            signal=signal
        )

    @dataclass
    class SupportResistance:
        strong_support: float
        support_1: float
        support_2: float
        resistance_1: float
        resistance_2: float
        strong_resistance: float
        current_position: str
        break_probability: float

    @staticmethod
    def analyze_support_resistance(df_data: List[Dict]) -> SupportResistance:
        if len(df_data) < 60:
            return SupportResistance(0, 0, 0, 0, 0, 0, "数据不足", 0)

        closes = [d.get("close", 0) for d in df_data if d.get("close") is not None]
        highs = [d.get("high", 0) for d in df_data if d.get("high") is not None]
        lows = [d.get("low", 0) for d in df_data if d.get("low") is not None]
        
        # 空数据保护
        if not closes or not highs or not lows:
            return SupportResistance(0, 0, 0, 0, 0, 0, "数据不足", 0)
            
        current = closes[-1]

        recent_lows = sorted([l for l in lows[-60:] if l > 0])
        recent_highs = sorted([h for h in highs[-60:] if h > 0])

        support_1 = recent_lows[int(len(recent_lows) * 0.1)] if recent_lows else current * 0.95
        support_2 = recent_lows[int(len(recent_lows) * 0.25)] if recent_lows else current * 0.90
        resistance_1 = recent_highs[int(len(recent_highs) * 0.75)] if recent_highs else current * 1.05
        resistance_2 = recent_highs[int(len(recent_highs) * 0.9)] if recent_highs else current * 1.10

        strong_support = min(recent_lows) if recent_lows else current * 0.85
        strong_resistance = max(recent_highs) if recent_highs else current * 1.15

        if current < support_1:
            position = "跌破支撑"
        elif current < resistance_1:
            position = "支撑阻力区间"
        else:
            position = "突破阻力"

        dist_to_resist = (resistance_1 - current) / current if current > 0 else 0
        dist_to_support = (current - support_1) / current if current > 0 else 0
        break_prob = 0.5
        if dist_to_resist < 0.02:
            break_prob = 0.7
        elif dist_to_support < 0.02:
            break_prob = 0.3

        return SupportResistance(
            strong_support=round(strong_support, 2),
            support_1=round(support_1, 2),
            support_2=round(support_2, 2),
            resistance_1=round(resistance_1, 2),
            resistance_2=round(resistance_2, 2),
            strong_resistance=round(strong_resistance, 2),
            current_position=position,
            break_probability=round(break_prob, 2)
        )

    @staticmethod
    def calculate_trend_strength(ma_system: MASystem, macd_system: MACDSystem, kdj_system: KDJSystem) -> TrendStrength:
        score = 0

        if ma_system.arrangement == "多头排列":
            score += 3
        elif ma_system.arrangement == "中期多头":
            score += 1
        elif ma_system.arrangement == "空头排列":
            score -= 3
        elif ma_system.arrangement == "中期空头":
            score -= 1

        if macd_system.golden_cross:
            score += 2
        elif macd_system.dead_cross:
            score -= 2

        if macd_system.momentum == "多头动能增强":
            score += 1
        elif macd_system.momentum == "空头动能增强":
            score -= 1

        if kdj_system.golden_cross:
            score += 1
        elif kdj_system.dead_cross:
            score -= 1

        if kdj_system.overbought:
            score -= 1
        if kdj_system.oversold:
            score += 1

        if score >= 5:
            return TrendStrength.STRONG_BULL
        elif score >= 3:
            return TrendStrength.BULL
        elif score >= 1:
            return TrendStrength.WEAK_BULL
        elif score <= -5:
            return TrendStrength.STRONG_BEAR
        elif score <= -3:
            return TrendStrength.BEAR
        elif score <= -1:
            return TrendStrength.WEAK_BEAR
        else:
            return TrendStrength.NEUTRAL

    @staticmethod
    def calculate_overall_score(ma_system: MASystem, macd_system: MACDSystem,
                                 kdj_system: KDJSystem, bollinger_system: BollingerSystem,
                                 volume_structure: VolumeStructure, trend_strength: TrendStrength) -> int:
        score = 50

        if trend_strength == TrendStrength.STRONG_BULL:
            score += 20
        elif trend_strength == TrendStrength.BULL:
            score += 15
        elif trend_strength == TrendStrength.WEAK_BULL:
            score += 5
        elif trend_strength == TrendStrength.STRONG_BEAR:
            score -= 20
        elif trend_strength == TrendStrength.BEAR:
            score -= 15
        elif trend_strength == TrendStrength.WEAK_BEAR:
            score -= 5

        if macd_system.divergence == "底背离":
            score += 10
        elif macd_system.divergence == "顶背离":
            score -= 10

        if volume_structure.signal == VolumeSignal.HEAVY_UP:
            score += 5
        elif volume_structure.signal == VolumeSignal.HEAVY_DOWN:
            score -= 5

        if bollinger_system.breakout == "向上突破":
            score += 5
        elif bollinger_system.breakout == "向下突破":
            score -= 5

        if kdj_system.overbought:
            score -= 5
        if kdj_system.oversold:
            score += 5

        return max(0, min(100, score))

    @staticmethod
    def classify_market_regime(ma_system: MASystem, bollinger_system: BollingerSystem,
                                trend_strength: TrendStrength) -> MarketRegime:
        if trend_strength in [TrendStrength.STRONG_BULL, TrendStrength.STRONG_BEAR]:
            return MarketRegime.STRONG_TREND
        if trend_strength in [TrendStrength.BULL, TrendStrength.BEAR]:
            return MarketRegime.WEAK_TREND
        if bollinger_system.squeeze:
            return MarketRegime.RANGING
        if ma_system.arrangement == "缠绕震荡":
            return MarketRegime.RANGING
        if bollinger_system.bandwidth < 8:
            return MarketRegime.RANGING
        return MarketRegime.UNKNOWN

    @staticmethod
    def _empty_signals() -> "TechSignals":
        return TechSignals(
            ma_system=MASystem(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, [], [], "数据不足"),
            macd_system=MACDSystem(0, 0, 0, "数据不足", False, False, "无", "中性"),
            kdj_system=KDJSystem(50, 50, 50, False, False, False, False),
            bollinger_system=BollingerSystem(0, 0, 0, 0, "数据不足", False, "无"),
            volume_structure=VolumeStructure(1.0, 0, 50, 1.0, 1.0, "中性", "平稳", VolumeSignal.NORMAL),
            support_resistance=SupportResistance(0, 0, 0, 0, 0, 0, "数据不足", 0),
            divergence_signals=DivergenceSignals(False, False, False, False, False, False, 0, "数据不足"),
            trend_strength=TrendStrength.NEUTRAL,
            market_regime=MarketRegime.UNKNOWN,
            overall_score=50,
            short_term_signal="数据不足",
            medium_term_signal="数据不足",
            risk_warning="数据异常",
            research_advice="数据不足，无法研判"
        )

    @staticmethod
    def analyze(df_data: List[Dict]) -> TechSignals:
        logger.info("[TechSkill] 开始机构级技术分析")

        if not df_data or not isinstance(df_data, list) or len(df_data) == 0:
            logger.warning("[TechSkill] df_data为空或类型异常，返回默认空信号")
            return TechSkill._empty_signals()

        try:
            ma_system = TechSkill.analyze_ma_system(df_data)
        except Exception as e:
            logger.error(f"[TechSkill] 均线系统分析异常: {e}")
            ma_system = MASystem(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, [], [], "异常")

        try:
            macd_system = TechSkill.analyze_macd(df_data)
        except Exception as e:
            logger.error(f"[TechSkill] MACD分析异常: {e}")
            macd_system = MACDSystem(0, 0, 0, "异常", False, False, "无", "中性")

        try:
            kdj_system = TechSkill.analyze_kdj(df_data)
        except Exception as e:
            logger.error(f"[TechSkill] KDJ分析异常: {e}")
            kdj_system = KDJSystem(50, 50, 50, False, False, False, False)

        try:
            bollinger_system = TechSkill.analyze_bollinger(df_data)
        except Exception as e:
            logger.error(f"[TechSkill] 布林带分析异常: {e}")
            bollinger_system = BollingerSystem(0, 0, 0, 0, "异常", False, "无")

        try:
            volume_structure = TechSkill.analyze_volume_structure(df_data)
        except Exception as e:
            logger.error(f"[TechSkill] 量价结构分析异常: {e}")
            volume_structure = VolumeStructure(1.0, 0, 50, 1.0, 1.0, "中性", "平稳", VolumeSignal.NORMAL)

        try:
            support_resistance = TechSkill.analyze_support_resistance(df_data)
        except Exception as e:
            logger.error(f"[TechSkill] 支撑阻力分析异常: {e}")
            support_resistance = SupportResistance(0, 0, 0, 0, 0, 0, "异常", 0)

        try:
            trend_strength = TechSkill.calculate_trend_strength(ma_system, macd_system, kdj_system)
        except Exception as e:
            logger.error(f"[TechSkill] 趋势强度计算异常: {e}")
            trend_strength = TrendStrength.NEUTRAL

        try:
            divergence_signals = TechSkill.analyze_divergence(df_data, macd_system, volume_structure)
        except Exception as e:
            logger.error(f"[TechSkill] 背离分析异常: {e}")
            divergence_signals = DivergenceSignals(False, False, False, False, False, False, 0, "异常")

        try:
            market_regime = TechSkill.classify_market_regime(ma_system, bollinger_system, trend_strength)
        except Exception as e:
            logger.error(f"[TechSkill] 行情状态判定异常: {e}")
            market_regime = MarketRegime.UNKNOWN

        try:
            overall_score = TechSkill.calculate_overall_score(
                ma_system, macd_system, kdj_system, bollinger_system, volume_structure, trend_strength
            )
        except Exception as e:
            logger.error(f"[TechSkill] 综合评分计算异常: {e}")
            overall_score = 50

        short_signal = ""
        if trend_strength in [TrendStrength.STRONG_BULL, TrendStrength.BULL]:
            short_signal = "短期看多"
        elif trend_strength in [TrendStrength.STRONG_BEAR, TrendStrength.BEAR]:
            short_signal = "短期看空"
        else:
            short_signal = "短期震荡"

        medium_signal = ""
        if ma_system.arrangement == "多头排列":
            medium_signal = "中期趋势向上"
        elif ma_system.arrangement == "空头排列":
            medium_signal = "中期趋势向下"
        elif market_regime == MarketRegime.STRONG_TREND:
            medium_signal = "中期趋势运行中"
        else:
            medium_signal = "中期趋势不明"

        risk_parts = []
        if divergence_signals.macd_bearish:
            risk_parts.append("MACD顶背离，注意回调风险")
        if divergence_signals.rsi_bearish:
            risk_parts.append("RSI顶背离，动能衰竭")
        if divergence_signals.volume_bearish:
            risk_parts.append("量价顶背离，高位换手异常")
        if kdj_system.overbought:
            risk_parts.append("KDJ超买，短期或有调整")
        if kdj_system.oversold:
            risk_parts.append("KDJ超卖，短期或有反弹")
        if volume_structure.signal == VolumeSignal.HEAVY_STAGNANT:
            risk_parts.append("放量滞涨，主力出货嫌疑")
        if not risk_parts:
            risk_parts.append("技术面暂无明确风险信号")
        risk = "；".join(risk_parts)

        advice_parts = []
        if trend_strength in [TrendStrength.STRONG_BULL, TrendStrength.BULL]:
            advice_parts.append("趋势向好，可逢低布局")
        if divergence_signals.macd_bullish:
            advice_parts.append("MACD底背离，关注反转机会")
        if divergence_signals.rsi_bullish:
            advice_parts.append("RSI底背离，动能积蓄")
        if macd_system.golden_cross:
            advice_parts.append("MACD金叉，动能转强")
        if kdj_system.oversold:
            advice_parts.append("KDJ超卖区域，关注反弹机会")
        if volume_structure.signal == VolumeSignal.HEAVY_UP:
            advice_parts.append("放量上涨，资金介入明显")
        if volume_structure.signal == VolumeSignal.LIGHT_PULLBACK:
            advice_parts.append("缩量回调，洗盘特征")
        if market_regime == MarketRegime.RANGING:
            advice_parts.append("震荡行情，高抛低吸")
        if not advice_parts:
            advice_parts.append("技术面信号中性，建议观望")

        advice = " | ".join(advice_parts)
        logger.info(f"[TechSkill] 分析完成 | 趋势: {trend_strength.value} | 行情: {market_regime.value} | 评分: {overall_score} | 背离: {divergence_signals.divergence_count}个")

        return TechSignals(
            ma_system=ma_system,
            macd_system=macd_system,
            kdj_system=kdj_system,
            bollinger_system=bollinger_system,
            volume_structure=volume_structure,
            support_resistance=support_resistance,
            divergence_signals=divergence_signals,
            trend_strength=trend_strength,
            market_regime=market_regime,
            overall_score=overall_score,
            short_term_signal=short_signal,
            medium_term_signal=medium_signal,
            risk_warning=risk,
            research_advice=advice
        )


tech_skill = TechSkill()
