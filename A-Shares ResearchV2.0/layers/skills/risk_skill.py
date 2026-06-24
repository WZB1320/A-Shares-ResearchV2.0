import sys
import math
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger("RiskSkill")


class RiskLevel(Enum):
    CRITICAL = "极高风险"
    HIGH = "高风险"
    MEDIUM = "中等风险"
    LOW = "低风险"
    MINIMAL = "极低风险"


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
    data_available: bool


@dataclass
class MarketRiskMetrics:
    volatility_30d: float           # 年化波动率(%)
    max_drawdown_1y: float          # 最大回撤(%)
    avg_turnover: float             # 平均换手率
    tail_risk: str                  # 尾部风险描述
    risk_level: RiskLevel
    data_available: bool
    # ── 机构级风险调整收益指标 ──
    sharpe_ratio: float             # 夏普比率
    sortino_ratio: float            # 索提诺比率
    calmar_ratio: float             # 卡尔玛比率
    beta: float                     # Beta系数
    downside_deviation: float       # 下行波动率(%)
    var_95: float                   # 95%置信度VaR(%)
    cvar_95: float                  # 95%置信度CVaR(%)


@dataclass
class RiskSignals:
    financial: FinancialRiskMetrics
    market: MarketRiskMetrics
    unavailable_dimensions: List[str]
    overall_risk_score: int
    overall_risk_level: RiskLevel
    warnings: List[str]
    research_advice: str


class RiskSkill:

    @staticmethod
    def _safe_float(value, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _extract_financial_field(financial_data: Dict, field_name: str) -> Optional[float]:
        finance_list = financial_data.get("finance", [])
        if finance_list and isinstance(finance_list, list) and len(finance_list) > 0:
            latest = finance_list[-1]
            if isinstance(latest, dict):
                val = latest.get(field_name)
                if val is not None:
                    return RiskSkill._safe_float(val)
        val = financial_data.get(field_name)
        if val is not None:
            return RiskSkill._safe_float(val)
        return None

    @staticmethod
    def analyze_financial_risk(financial_data: Dict) -> FinancialRiskMetrics:
        if financial_data.get("_data_unavailable"):
            return FinancialRiskMetrics(0, 0, 0, 0, False, 0, "未知", RiskLevel.MINIMAL, False)

        debt_to_asset = RiskSkill._extract_financial_field(financial_data, "debt_to_asset")
        current_ratio = RiskSkill._extract_financial_field(financial_data, "current_ratio")
        quick_ratio = RiskSkill._extract_financial_field(financial_data, "quick_ratio")
        interest_coverage = RiskSkill._extract_financial_field(financial_data, "interest_coverage")
        ocf = RiskSkill._extract_financial_field(financial_data, "operating_cash_flow")
        net_profit = RiskSkill._extract_financial_field(financial_data, "net_profit")
        consecutive_losses = financial_data.get("consecutive_losses", 0)
        audit_opinion = financial_data.get("audit_opinion", "未知")

        has_any_data = any(v is not None for v in [debt_to_asset, current_ratio, quick_ratio])

        if not has_any_data:
            return FinancialRiskMetrics(0, 0, 0, 0, False, 0, "未知", RiskLevel.MINIMAL, False)

        debt_to_asset = debt_to_asset or 0
        current_ratio = current_ratio or 0
        quick_ratio = quick_ratio or 0
        interest_coverage = interest_coverage or 999
        ocf = ocf or 0
        net_profit = net_profit or 0

        cash_shortfall = (ocf is not None and ocf < 0) and (net_profit is not None and net_profit > 0)

        if consecutive_losses >= 2 or (audit_opinion and audit_opinion != "标准无保留" and audit_opinion != "未知") or debt_to_asset > 0.9:
            level = RiskLevel.CRITICAL
        elif debt_to_asset > 0.8 or (interest_coverage and interest_coverage < 1) or current_ratio < 0.5:
            level = RiskLevel.HIGH
        elif debt_to_asset > 0.7 or (interest_coverage and interest_coverage < 3) or current_ratio < 1:
            level = RiskLevel.MEDIUM
        elif debt_to_asset > 0.5 or cash_shortfall:
            level = RiskLevel.LOW
        else:
            level = RiskLevel.MINIMAL

        return FinancialRiskMetrics(
            debt_to_asset=round(debt_to_asset * 100, 2) if debt_to_asset < 10 else round(debt_to_asset, 2),
            current_ratio=round(current_ratio, 2),
            quick_ratio=round(quick_ratio, 2),
            interest_coverage=round(interest_coverage, 2),
            cash_flow_shortfall=cash_shortfall,
            consecutive_losses=consecutive_losses,
            audit_opinion=audit_opinion if audit_opinion else "未知",
            risk_level=level,
            data_available=True
        )

    @staticmethod
    def analyze_market_risk(financial_data: Dict, tech_data: Optional[List[Dict]] = None) -> MarketRiskMetrics:
        has_tech = tech_data and isinstance(tech_data, list) and len(tech_data) > 0

        empty = MarketRiskMetrics(0, 0, 0, "数据不足", RiskLevel.MINIMAL, False, 0, 0, 0, 0, 0, 0, 0)

        if not has_tech:
            return empty

        try:
            closes = [d.get("close", 0) for d in tech_data if d.get("close") is not None and d.get("close") > 0]
            turnovers = [d.get("turnover", 0) for d in tech_data if d.get("turnover") is not None]

            if not closes:
                return empty

            # ── 基础波动率/回撤 ──
            if len(closes) >= 20:
                returns = [(closes[i] - closes[i-1]) / closes[i-1] for i in range(1, len(closes))]
                if returns:
                    volatility = (sum(r**2 for r in returns[-20:]) / min(20, len(returns)))**0.5
                    volatility_annual = volatility * math.sqrt(252) * 100
                else:
                    volatility_annual = 0
            else:
                volatility_annual = 0

            peak = closes[0]
            max_dd = 0
            for c in closes:
                if c > peak:
                    peak = c
                dd = (peak - c) / peak * 100
                if dd > max_dd:
                    max_dd = dd

            avg_turnover = sum(turnovers) / len(turnovers) if turnovers else 0

            # ── 机构级风险调整收益指标 ──
            rets = returns if len(closes) >= 20 else []
            sharpe = sortino = calmar = beta = downside_dev = var_95 = cvar_95 = 0.0

            if rets:
                mean_ret = sum(rets) / len(rets)
                std_ret = math.sqrt(sum((r - mean_ret)**2 for r in rets) / len(rets))
                risk_free_daily = 0.03 / 252  # 假设无风险利率 3%

                # Sharpe Ratio
                if std_ret > 0:
                    sharpe = (mean_ret - risk_free_daily) / std_ret * math.sqrt(252)

                # Sortino Ratio (下行标准差)
                downside_rets = [r for r in rets if r < 0]
                if downside_rets:
                    downside_std = math.sqrt(sum((r - 0)**2 for r in downside_rets) / len(downside_rets))
                    if downside_std > 0:
                        sortino = (mean_ret - risk_free_daily) / downside_std * math.sqrt(252)
                    downside_dev = downside_std * math.sqrt(252) * 100
                else:
                    sortino = 999 if mean_ret > 0 else 0
                    downside_dev = 0

                # Calmar Ratio
                if max_dd > 0:
                    annual_return = mean_ret * 252 * 100
                    calmar = annual_return / max_dd

                # Beta (简化：用指数日收益近似，如果不可用则用自身波动率)
                # 实际应用中应从 market_data 获取基准收益率，这里做简化处理
                beta = 1.0  # 默认市场中性

                # VaR 95% (历史模拟法)
                sorted_rets = sorted(rets)
                var_idx = max(0, int(len(sorted_rets) * 0.05))
                var_95 = abs(sorted_rets[var_idx]) * 100  # 日VaR

                # CVaR 95% (条件在险价值)
                tail_rets = sorted_rets[:var_idx + 1]
                if tail_rets:
                    cvar_95 = abs(sum(tail_rets) / len(tail_rets)) * 100
                else:
                    cvar_95 = var_95

            if volatility_annual > 60 or max_dd > 50:
                tail = "高波动，系统性风险大"
                level = RiskLevel.HIGH
            elif volatility_annual > 40 or max_dd > 35:
                tail = "中等波动"
                level = RiskLevel.MEDIUM
            elif volatility_annual > 25:
                tail = "波动可控"
                level = RiskLevel.LOW
            else:
                tail = "低波动"
                level = RiskLevel.MINIMAL

            return MarketRiskMetrics(
                volatility_30d=round(volatility_annual, 2),
                max_drawdown_1y=round(max_dd, 2),
                avg_turnover=round(avg_turnover, 2),
                tail_risk=tail,
                risk_level=level,
                data_available=True,
                sharpe_ratio=round(sharpe, 2),
                sortino_ratio=round(sortino, 2),
                calmar_ratio=round(calmar, 2),
                beta=round(beta, 2),
                downside_deviation=round(downside_dev, 2),
                var_95=round(var_95, 2),
                cvar_95=round(cvar_95, 2),
            )
        except Exception as e:
            logger.error(f"[RiskSkill] 市场风险计算异常: {e}")
            return empty

    @staticmethod
    def analyze(financial_data: Dict, tech_data: Optional[List[Dict]] = None) -> RiskSignals:
        logger.info("[RiskSkill] 开始机构级风险分析")

        unavailable = []

        financial = RiskSkill.analyze_financial_risk(financial_data)
        if not financial.data_available:
            unavailable.append("财务风险(缺少财务指标数据)")

        market = RiskSkill.analyze_market_risk(financial_data, tech_data)
        if not market.data_available:
            unavailable.append("市场风险(缺少技术面数据)")

        unavailable.append("股权质押风险(API不提供此数据)")
        unavailable.append("大股东减持风险(API不提供此数据)")
        unavailable.append("经营风险(API不提供此数据)")
        unavailable.append("公司治理风险(API不提供此数据)")

        risk_scores = {RiskLevel.CRITICAL: 0, RiskLevel.HIGH: 25, RiskLevel.MEDIUM: 50, RiskLevel.LOW: 75, RiskLevel.MINIMAL: 100}

        available_scores = []
        if financial.data_available:
            available_scores.append(risk_scores[financial.risk_level])
        if market.data_available:
            available_scores.append(risk_scores[market.risk_level])

        if available_scores:
            overall_score = int(sum(available_scores) / len(available_scores))
        else:
            overall_score = 50

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
        if financial.data_available:
            if financial.risk_level in [RiskLevel.CRITICAL, RiskLevel.HIGH]:
                warnings.append(f"财务风险：资产负债率{financial.debt_to_asset}%，{financial.risk_level.value}")
            if financial.cash_flow_shortfall:
                warnings.append("现金流与利润背离，盈利质量存疑")
            if financial.consecutive_losses >= 2:
                warnings.append(f"连续{financial.consecutive_losses}年亏损")

        if market.data_available:
            if market.risk_level in [RiskLevel.CRITICAL, RiskLevel.HIGH]:
                warnings.append(f"市场风险：波动率{market.volatility_30d:.1f}%，最大回撤{market.max_drawdown_1y:.1f}%")

        if unavailable:
            warnings.append(f"以下风险维度因数据不可用无法评估：{'、'.join(unavailable[:3])}")

        if not warnings:
            warnings.append("基于现有数据的风险评估：风险可控")

        advice_parts = []
        if financial.data_available and financial.risk_level in [RiskLevel.CRITICAL, RiskLevel.HIGH]:
            advice_parts.append("财务风险较高，需重点关注")
        if market.data_available and market.risk_level in [RiskLevel.CRITICAL, RiskLevel.HIGH]:
            advice_parts.append("市场波动风险较大")
        if not advice_parts:
            advice_parts.append("基于现有数据，风险结构健康")

        advice = " | ".join(advice_parts)
        logger.info(f"[RiskSkill] 分析完成 | 综合风险: {overall_level.value} | 评分: {overall_score} | 不可用维度: {len(unavailable)}")

        return RiskSignals(
            financial=financial,
            market=market,
            unavailable_dimensions=unavailable,
            overall_risk_score=overall_score,
            overall_risk_level=overall_level,
            warnings=warnings,
            research_advice=advice
        )


risk_skill = RiskSkill()