import sys
import logging
import time
import json
import math
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

logger = logging.getLogger("BacktestEngine")

from layers.agents.report_schema import GRADE_WEIGHTS, aggregate_reports, AgentReport
from layers.skills.tech_skill import TechSkill, tech_skill
from layers.skills.fund_skill import FundSkill, fund_skill
from layers.skills.valuation_skill import ValuationSkill, valuation_skill

FORECAST_HORIZONS = [20, 60, 120]

GRADE_DIRECTION = {
    "强烈看多": "bullish",
    "看多": "bullish",
    "中性偏多": "bullish",
    "中性": "neutral",
    "中性偏空": "bearish",
    "看空": "bearish",
    "强烈看空": "bearish",
}


@dataclass
class AnalysisSnapshot:
    stock_code: str
    analysis_date: str
    forecast_horizon_days: int

    tech_score: int = 50
    tech_grade: str = "中性"
    tech_confidence: int = 0
    tech_signals: Dict[str, Any] = field(default_factory=dict)

    fund_score: int = 50
    fund_grade: str = "中性"
    fund_confidence: int = 0
    fund_signals: Dict[str, Any] = field(default_factory=dict)

    valuation_score: int = 50
    valuation_grade: str = "中性"
    valuation_confidence: int = 0
    valuation_signals: Dict[str, Any] = field(default_factory=dict)

    overall_score: int = 50
    overall_grade: str = "中性"
    overall_confidence: int = 0
    consensus: str = "无数据"

    dimension_scores: Dict[str, int] = field(default_factory=dict)
    dimension_grades: Dict[str, str] = field(default_factory=dict)

    def get_direction(self) -> str:
        return GRADE_DIRECTION.get(self.overall_grade, "neutral")

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["direction"] = self.get_direction()
        return d


@dataclass
class OutcomeData:
    stock_code: str
    analysis_date: str
    future_date: str
    forecast_horizon_days: int

    price_at_analysis: float = 0.0
    price_at_future: float = 0.0
    return_pct: float = 0.0
    benchmark_return_pct: float = 0.0
    excess_return_pct: float = 0.0
    max_drawdown_pct: float = 0.0
    volatility_pct: float = 0.0
    direction: str = "flat"

    data_available: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class BacktestResult:
    snapshot: AnalysisSnapshot
    outcome: OutcomeData

    is_correct_direction: bool = False
    is_correct_excess: bool = False
    direction_accuracy_score: float = 0.0
    score_return_correlation: float = 0.0

    dimension_correct: Dict[str, bool] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "stock_code": self.snapshot.stock_code,
            "analysis_date": self.snapshot.analysis_date,
            "forecast_days": self.snapshot.forecast_horizon_days,
            "predicted_grade": self.snapshot.overall_grade,
            "predicted_score": self.snapshot.overall_score,
            "actual_return_pct": round(self.outcome.return_pct, 2),
            "excess_return_pct": round(self.outcome.excess_return_pct, 2),
            "actual_direction": self.outcome.direction,
            "is_correct": self.is_correct_direction,
            "dimension_correct": self.dimension_correct,
        }


@dataclass
class DimAttribution:
    dimension: str
    sample_count: int = 0
    win_rate_when_bullish: float = 0.0
    win_rate_when_bearish: float = 0.0
    avg_excess_when_bullish: float = 0.0
    avg_excess_when_bearish: float = 0.0
    avg_excess_when_correct: float = 0.0
    signal_accuracy: float = 0.0
    score_return_corr: float = 0.0
    contribution: float = 0.0
    rating: str = "未评级"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class BacktestSummary:
    total_samples: int = 0
    valid_samples: int = 0
    forecast_horizon_days: int = 60

    overall_win_rate: float = 0.0
    overall_win_rate_bullish: float = 0.0
    overall_win_rate_bearish: float = 0.0

    avg_excess_return: float = 0.0
    avg_excess_when_correct: float = 0.0
    avg_excess_when_wrong: float = 0.0

    avg_return_when_bullish: float = 0.0
    avg_return_when_bearish: float = 0.0

    sharpe_ratio: float = 0.0
    info_ratio: float = 0.0
    max_drawdown: float = 0.0
    hit_rate: float = 0.0

    dimension_attribution: Dict[str, DimAttribution] = field(default_factory=dict)
    grade_accuracy: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    confidence_calibration: Dict[str, float] = field(default_factory=dict)

    top_correct_stocks: List[Dict] = field(default_factory=list)
    top_wrong_stocks: List[Dict] = field(default_factory=list)

    improvement_suggestions: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return d

    def to_report(self) -> str:
        return _format_summary_report(self)


class BacktestEngine:

    def __init__(self):
        self.results: List[BacktestResult] = []
        self.tech_skill = tech_skill
        self.fund_skill = fund_skill
        self.valuation_skill = valuation_skill

    def run_from_synthetic(
        self,
        stock_codes: List[str],
        scenarios: List[Dict[str, Any]],
        forecast_days: int = 60,
    ) -> List[BacktestResult]:
        logger.info(f"[Backtest] 开始合成数据回溯测试: {len(stock_codes)} 只股票, {len(scenarios)} 场景, 展望{forecast_days}天")

        results = []
        for stock_code in stock_codes:
            for scenario in scenarios:
                snapshot = self._build_snapshot(stock_code, scenario, forecast_days)
                outcome = self._build_outcome(stock_code, scenario, forecast_days)
                result = self._evaluate_single(snapshot, outcome)
                results.append(result)
                logger.debug(
                    f"[Backtest] {stock_code} | 预测={snapshot.overall_grade}({snapshot.overall_score}) "
                    f"| 实际收益={outcome.return_pct:+.1f}% | "
                    f"{'✓' if result.is_correct_direction else '✗'}"
                )

        self.results.extend(results)
        logger.info(f"[Backtest] 回溯测试完成: {len(results)} 样本")
        return results

    def _build_snapshot(
        self,
        stock_code: str,
        scenario: Dict[str, Any],
        forecast_days: int,
    ) -> AnalysisSnapshot:
        tech_sig = scenario.get("tech_signals", {})
        fund_sig = scenario.get("fund_signals", {})
        val_sig = scenario.get("valuation_signals", {})
        analysis_date = scenario.get("analysis_date", "2024-01-15")

        snapshot = AnalysisSnapshot(
            stock_code=stock_code,
            analysis_date=analysis_date,
            forecast_horizon_days=forecast_days,
        )

        if tech_sig:
            snapshot.tech_score = tech_sig.get("overall_score", 50)
            snapshot.tech_grade = _score_to_grade(snapshot.tech_score)
            snapshot.tech_confidence = tech_sig.get("confidence", 70)
            snapshot.tech_signals = {
                "trend": tech_sig.get("trend_direction", "震荡"),
                "momentum": tech_sig.get("momentum_status", "中性"),
                "volume": tech_sig.get("volume_signal", "正常"),
            }
        else:
            snapshot.tech_score = 50
            snapshot.tech_grade = "中性"
            snapshot.tech_confidence = 0

        if fund_sig:
            snapshot.fund_score = fund_sig.get("overall_score", 50)
            snapshot.fund_grade = _score_to_grade(snapshot.fund_score)
            snapshot.fund_confidence = fund_sig.get("confidence", 70)
            snapshot.fund_signals = {
                "roe": fund_sig.get("roe", "正常"),
                "growth": fund_sig.get("growth", "正常"),
                "cashflow": fund_sig.get("cashflow", "正常"),
            }
        else:
            snapshot.fund_score = 50
            snapshot.fund_grade = "中性"
            snapshot.fund_confidence = 0

        if val_sig:
            snapshot.valuation_score = val_sig.get("overall_score", 50)
            snapshot.valuation_grade = _score_to_grade(snapshot.valuation_score)
            snapshot.valuation_confidence = val_sig.get("confidence", 70)
            snapshot.valuation_signals = {
                "pe_percentile": val_sig.get("pe_percentile", 50),
                "pb_percentile": val_sig.get("pb_percentile", 50),
                "industry": val_sig.get("industry", "通用"),
            }
        else:
            snapshot.valuation_score = 50
            snapshot.valuation_grade = "中性"
            snapshot.valuation_confidence = 0

        scores = []
        confidences = []
        grades = []
        dim_scores = {}
        dim_grades = {}

        for dim_name, score, grade, conf in [
            ("tech", snapshot.tech_score, snapshot.tech_grade, snapshot.tech_confidence),
            ("fund", snapshot.fund_score, snapshot.fund_grade, snapshot.fund_confidence),
            ("valuation", snapshot.valuation_score, snapshot.valuation_grade, snapshot.valuation_confidence),
        ]:
            if conf > 0:
                scores.append((score, conf))
                confidences.append(conf)
                grades.append(grade)
                dim_scores[dim_name] = score
                dim_grades[dim_name] = grade

        if scores:
            total_conf = sum(confidences)
            snapshot.overall_score = round(
                sum(s * c for s, c in scores) / total_conf
            ) if total_conf > 0 else 50
        else:
            snapshot.overall_score = 50

        snapshot.overall_grade = _score_to_grade(snapshot.overall_score)
        snapshot.overall_confidence = (
            int(sum(confidences) / len(confidences)) if confidences else 0
        )
        snapshot.consensus = _build_consensus(
            [GRADE_WEIGHTS.get(g, 50) for g in grades]
        )
        snapshot.dimension_scores = dim_scores
        snapshot.dimension_grades = dim_grades

        return snapshot

    def _build_outcome(
        self,
        stock_code: str,
        scenario: Dict[str, Any],
        forecast_days: int,
    ) -> OutcomeData:
        actual_return = scenario.get("actual_return_pct", 0.0)
        benchmark_return = scenario.get("benchmark_return_pct", 0.0)
        analysis_date = scenario.get("analysis_date", "2024-01-15")

        analysis_dt = datetime.strptime(analysis_date, "%Y-%m-%d")
        future_dt = analysis_dt + timedelta(days=forecast_days)

        excess = actual_return - benchmark_return
        direction = "涨" if actual_return > 2 else ("跌" if actual_return < -2 else "平")

        return OutcomeData(
            stock_code=stock_code,
            analysis_date=analysis_date,
            future_date=future_dt.strftime("%Y-%m-%d"),
            forecast_horizon_days=forecast_days,
            price_at_analysis=scenario.get("price_at_analysis", 10.0),
            price_at_future=scenario.get("price_at_future", 10.0),
            return_pct=actual_return,
            benchmark_return_pct=benchmark_return,
            excess_return_pct=excess,
            max_drawdown_pct=scenario.get("max_drawdown_pct", abs(min(actual_return, 0))),
            volatility_pct=scenario.get("volatility_pct", abs(actual_return) * 0.6),
            direction=direction,
            data_available=True,
        )

    def _evaluate_single(
        self, snapshot: AnalysisSnapshot, outcome: OutcomeData
    ) -> BacktestResult:
        pred_direction = snapshot.get_direction()
        actual_direction = outcome.direction

        is_correct = False
        if pred_direction == "bullish" and outcome.return_pct > 2:
            is_correct = True
        elif pred_direction == "bearish" and outcome.return_pct < -2:
            is_correct = True
        elif pred_direction == "neutral" and abs(outcome.return_pct) <= 2:
            is_correct = True

        is_correct_excess = False
        if pred_direction == "bullish" and outcome.excess_return_pct > 0:
            is_correct_excess = True
        elif pred_direction == "bearish" and outcome.excess_return_pct < 0:
            is_correct_excess = True
        elif pred_direction == "neutral" and abs(outcome.excess_return_pct) <= 2:
            is_correct_excess = True

        score_accuracy = 1.0 - min(
            abs(snapshot.overall_score - 50) / 50,
            abs(outcome.return_pct) / 30 if abs(outcome.return_pct) > 0 else 1.0,
        ) * 0.5
        score_accuracy = max(0.0, min(1.0, score_accuracy))

        dim_correct = {}
        dim_map = [
            ("tech", snapshot.tech_score, snapshot.tech_grade),
            ("fund", snapshot.fund_score, snapshot.fund_grade),
            ("valuation", snapshot.valuation_score, snapshot.valuation_grade),
        ]
        for dim_name, score, grade in dim_map:
            dim_dir = GRADE_DIRECTION.get(grade, "neutral")
            if dim_dir == "bullish" and outcome.return_pct > 2:
                dim_correct[dim_name] = True
            elif dim_dir == "bearish" and outcome.return_pct < -2:
                dim_correct[dim_name] = True
            elif dim_dir == "neutral" and abs(outcome.return_pct) <= 2:
                dim_correct[dim_name] = True
            else:
                dim_correct[dim_name] = False

        return BacktestResult(
            snapshot=snapshot,
            outcome=outcome,
            is_correct_direction=is_correct,
            is_correct_excess=is_correct_excess,
            direction_accuracy_score=score_accuracy,
            dimension_correct=dim_correct,
        )

    def evaluate_all(self, forecast_days: Optional[int] = None) -> BacktestSummary:
        if forecast_days:
            results = [r for r in self.results if r.snapshot.forecast_horizon_days == forecast_days]
        else:
            results = self.results

        if not results:
            logger.warning("[Backtest] 无回溯结果可供评估")
            return BacktestSummary()

        total = len(results)
        valid = total

        correct_direction = sum(1 for r in results if r.is_correct_direction)
        correct_excess = sum(1 for r in results if r.is_correct_excess)

        bullish_results = [r for r in results if r.snapshot.get_direction() == "bullish"]
        bearish_results = [r for r in results if r.snapshot.get_direction() == "bearish"]
        neutral_results = [r for r in results if r.snapshot.get_direction() == "neutral"]

        bullish_correct = sum(1 for r in bullish_results if r.is_correct_direction)
        bearish_correct = sum(1 for r in bearish_results if r.is_correct_direction)

        excess_returns = [r.outcome.excess_return_pct for r in results]
        avg_excess = sum(excess_returns) / len(excess_returns) if excess_returns else 0.0

        correct_excess_returns = [r.outcome.excess_return_pct for r in results if r.is_correct_direction]
        wrong_excess_returns = [r.outcome.excess_return_pct for r in results if not r.is_correct_direction]
        avg_excess_correct = (
            sum(correct_excess_returns) / len(correct_excess_returns)
            if correct_excess_returns else 0.0
        )
        avg_excess_wrong = (
            sum(wrong_excess_returns) / len(wrong_excess_returns)
            if wrong_excess_returns else 0.0
        )

        bullish_returns = [r.outcome.return_pct for r in bullish_results]
        bearish_returns = [r.outcome.return_pct for r in bearish_results]
        avg_return_bullish = sum(bullish_returns) / len(bullish_returns) if bullish_returns else 0.0
        avg_return_bearish = sum(bearish_returns) / len(bearish_returns) if bearish_returns else 0.0

        returns = [r.outcome.return_pct for r in results]
        sharpe = _calc_sharpe(returns)
        info_ratio = _calc_sharpe(excess_returns)
        max_dd = max((r.outcome.max_drawdown_pct for r in results), default=0.0)

        dim_attribution = self._compute_dim_attribution(results)

        grade_accuracy = self._compute_grade_accuracy(results)
        confidence_calib = self._compute_confidence_calibration(results)

        sorted_by_excess = sorted(results, key=lambda r: r.outcome.excess_return_pct, reverse=True)
        top_correct = [r for r in sorted_by_excess if r.is_correct_direction][:5]
        top_wrong = [r for r in sorted_by_excess if not r.is_correct_direction][:5]

        suggestions = self._generate_suggestions(
            dim_attribution, grade_accuracy, overall_win_rate=correct_direction / max(total, 1)
        )

        days = results[0].snapshot.forecast_horizon_days if results else 60

        summary = BacktestSummary(
            total_samples=total,
            valid_samples=valid,
            forecast_horizon_days=days,
            overall_win_rate=correct_direction / max(total, 1),
            overall_win_rate_bullish=bullish_correct / max(len(bullish_results), 1),
            overall_win_rate_bearish=bearish_correct / max(len(bearish_results), 1),
            avg_excess_return=avg_excess,
            avg_excess_when_correct=avg_excess_correct,
            avg_excess_when_wrong=avg_excess_wrong,
            avg_return_when_bullish=avg_return_bullish,
            avg_return_when_bearish=avg_return_bearish,
            sharpe_ratio=sharpe,
            info_ratio=info_ratio,
            max_drawdown=max_dd,
            hit_rate=correct_excess / max(total, 1),
            dimension_attribution=dim_attribution,
            grade_accuracy=grade_accuracy,
            confidence_calibration=confidence_calib,
            top_correct_stocks=[r.to_dict() for r in top_correct],
            top_wrong_stocks=[r.to_dict() for r in top_wrong],
            improvement_suggestions=suggestions,
        )
        return summary

    def _compute_dim_attribution(
        self, results: List[BacktestResult]
    ) -> Dict[str, DimAttribution]:
        dims = ["tech", "fund", "valuation"]
        attribution = {}

        for dim in dims:
            dim_results = [r for r in results if dim in r.dimension_correct]
            if not dim_results:
                continue

            dim_correct = sum(1 for r in dim_results if r.dimension_correct[dim])
            dim_total = len(dim_results)

            bullish_dim = [
                r for r in dim_results
                if GRADE_DIRECTION.get(r.snapshot.dimension_grades.get(dim, "中性"), "neutral") == "bullish"
            ]
            bearish_dim = [
                r for r in dim_results
                if GRADE_DIRECTION.get(r.snapshot.dimension_grades.get(dim, "中性"), "neutral") == "bearish"
            ]

            bullish_dim_correct = sum(1 for r in bullish_dim if r.dimension_correct[dim])
            bearish_dim_correct = sum(1 for r in bearish_dim if r.dimension_correct[dim])

            avg_excess_bullish = (
                sum(r.outcome.excess_return_pct for r in bullish_dim) / len(bullish_dim)
                if bullish_dim else 0.0
            )
            avg_excess_bearish = (
                sum(r.outcome.excess_return_pct for r in bearish_dim) / len(bearish_dim)
                if bearish_dim else 0.0
            )

            correct_excess = [
                r.outcome.excess_return_pct for r in dim_results if r.dimension_correct[dim]
            ]
            avg_excess_correct_dim = (
                sum(correct_excess) / len(correct_excess) if correct_excess else 0.0
            )

            dim_scores = [r.snapshot.dimension_scores.get(dim, 50) for r in dim_results]
            dim_returns = [r.outcome.return_pct for r in dim_results]
            corr = _pearson_corr(dim_scores, dim_returns)

            contribution = _calc_contribution(
                dim_correct / max(dim_total, 1),
                corr,
                avg_excess_correct_dim,
            )

            attr = DimAttribution(
                dimension=dim,
                sample_count=dim_total,
                win_rate_when_bullish=bullish_dim_correct / max(len(bullish_dim), 1),
                win_rate_when_bearish=bearish_dim_correct / max(len(bearish_dim), 1),
                avg_excess_when_bullish=avg_excess_bullish,
                avg_excess_when_bearish=avg_excess_bearish,
                avg_excess_when_correct=avg_excess_correct_dim,
                signal_accuracy=dim_correct / max(dim_total, 1),
                score_return_corr=corr,
                contribution=contribution,
                rating=_rate_dimension(dim_correct / max(dim_total, 1), corr),
            )
            attribution[dim] = attr

        return attribution

    def _compute_grade_accuracy(
        self, results: List[BacktestResult]
    ) -> Dict[str, Dict[str, Any]]:
        grade_stats = defaultdict(lambda: {"total": 0, "correct": 0, "returns": []})

        for r in results:
            grade = r.snapshot.overall_grade
            grade_stats[grade]["total"] += 1
            if r.is_correct_direction:
                grade_stats[grade]["correct"] += 1
            grade_stats[grade]["returns"].append(r.outcome.return_pct)

        output = {}
        for grade, stats in sorted(grade_stats.items()):
            t = stats["total"]
            c = stats["correct"]
            rets = stats["returns"]
            output[grade] = {
                "count": t,
                "accuracy": c / max(t, 1),
                "avg_return": sum(rets) / len(rets) if rets else 0.0,
            }
        return output

    def _compute_confidence_calibration(
        self, results: List[BacktestResult]
    ) -> Dict[str, float]:
        buckets = defaultdict(lambda: {"total": 0, "correct": 0})

        for r in results:
            conf = r.snapshot.overall_confidence
            bucket = str((conf // 20) * 20)
            buckets[bucket]["total"] += 1
            if r.is_correct_direction:
                buckets[bucket]["correct"] += 1

        calibration = {}
        for bucket, stats in sorted(buckets.items()):
            calibration[f"confidence_{bucket}"] = (
                stats["correct"] / max(stats["total"], 1)
            )
        return calibration

    def _generate_suggestions(
        self,
        dim_attribution: Dict[str, DimAttribution],
        grade_accuracy: Dict[str, Dict[str, Any]],
        overall_win_rate: float,
    ) -> List[str]:
        suggestions = []

        if overall_win_rate < 0.50:
            suggestions.append(
                f"[WARN] 整体胜率仅 {overall_win_rate:.0%}，低于随机水平(50%)，系统信号有效性存疑，建议检查数据质量和指标计算逻辑"
            )
        elif overall_win_rate < 0.60:
            suggestions.append(
                f"整体胜率 {overall_win_rate:.0%}，略高于随机水平但仍有较大提升空间"
            )
        else:
            suggestions.append(
                f"整体胜率 {overall_win_rate:.0%}，信号具有统计学有效性，可继续优化高频误判场景"
            )

        for dim, attr in dim_attribution.items():
            dim_names = {"tech": "技术面", "fund": "基本面", "valuation": "估值面"}
            label = dim_names.get(dim, dim)
            if attr.signal_accuracy < 0.45:
                suggestions.append(
                    f"[WARN] {label}({dim})胜率 {attr.signal_accuracy:.0%}，低于50%，建议检查该维度指标逻辑或数据源"
                )
            elif attr.signal_accuracy > 0.65:
                suggestions.append(
                    f"[GOOD] {label}({dim})胜率 {attr.signal_accuracy:.0%}，贡献度 {attr.contribution:.0f}，可适度提高该维度权重"
                )

            if attr.score_return_corr < -0.1:
                suggestions.append(
                    f"[WARN] {label}({dim})评分与收益负相关({attr.score_return_corr:.2f})，评分体系可能与实际脱节"
                )
            elif attr.score_return_corr > 0.3:
                suggestions.append(
                    f"[GOOD] {label}({dim})评分与收益正相关({attr.score_return_corr:.2f})，信号质量良好"
                )

        grade_acc_strs = []
        for grade, stats in grade_accuracy.items():
            if stats["count"] >= 3 and stats["accuracy"] < 0.4:
                grade_acc_strs.append(f"评级「{grade}」准确率仅 {stats['accuracy']:.0%}")
        if grade_acc_strs:
            suggestions.append(
                f"评级偏移问题: {', '.join(grade_acc_strs)}，建议调整对应评分区间的阈值"
            )

        suggestions.append(
            "[TIP] 通用优化方向:增加更多历史时间点的回溯样本,覆盖牛/熊/震荡三种市场环境"
        )

        return suggestions


def _score_to_grade(score: int) -> str:
    thresholds = [(90, "强烈看多"), (70, "看多"), (58, "中性偏多"),
                  (42, "中性"), (30, "中性偏空"), (15, "看空")]
    for threshold, grade in thresholds:
        if score >= threshold:
            return grade
    return "强烈看空"


def _build_consensus(grade_values: List[int]) -> str:
    if not grade_values:
        return "无有效数据"
    avg = sum(grade_values) / len(grade_values)
    if avg >= 80:
        return "高度一致看多"
    elif avg >= 65:
        return "普遍偏多"
    elif avg >= 45:
        return "观点中性"
    elif avg >= 30:
        return "普遍偏空"
    else:
        return "高度一致看空"


def _calc_sharpe(returns: List[float], risk_free: float = 0.025) -> float:
    if len(returns) < 2:
        return 0.0
    mean = sum(returns) / len(returns)
    std = math.sqrt(sum((r - mean) ** 2 for r in returns) / (len(returns) - 1))
    if std == 0:
        return 0.0
    return (mean / 100 - risk_free) / (std / 100) * math.sqrt(252 / 60)


def _pearson_corr(x: List[float], y: List[float]) -> float:
    n = min(len(x), len(y))
    if n < 3:
        return 0.0
    mean_x = sum(x) / n
    mean_y = sum(y) / n
    cov = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
    std_x = math.sqrt(sum((xi - mean_x) ** 2 for xi in x))
    std_y = math.sqrt(sum((yi - mean_y) ** 2 for yi in y))
    if std_x == 0 or std_y == 0:
        return 0.0
    return cov / (std_x * std_y)


def _calc_contribution(accuracy: float, correlation: float, avg_excess: float) -> float:
    contrib = accuracy * 30 + max(correlation, 0) * 40 + min(max(avg_excess / 10, 0), 1) * 30
    return round(contrib, 0)


def _rate_dimension(accuracy: float, correlation: float) -> str:
    score = accuracy * 0.6 + max(correlation, 0) * 0.4
    if score > 0.70:
        return "A-优秀"
    elif score > 0.60:
        return "B-良好"
    elif score > 0.50:
        return "C-一般"
    elif score > 0.40:
        return "D-较差"
    else:
        return "E-无效"


def _format_summary_report(summary: BacktestSummary) -> str:
    dim_names = {"tech": "技术面", "fund": "基本面", "valuation": "估值面"}

    lines = []
    lines.append("=" * 60)
    lines.append("  A-Shares-Research 回溯测试评估报告")
    lines.append("=" * 60)
    lines.append(f"  样本数: {summary.total_samples} | 展望期: {summary.forecast_horizon_days}天")
    lines.append(f"  整体胜率: {summary.overall_win_rate:.1%}")
    lines.append(f"  看多胜率: {summary.overall_win_rate_bullish:.1%} | 看空胜率: {summary.overall_win_rate_bearish:.1%}")
    lines.append(f"  平均超额收益: {summary.avg_excess_return:+.1f}%")
    lines.append(f"  正确时超额: {summary.avg_excess_when_correct:+.1f}% | 错误时超额: {summary.avg_excess_when_wrong:+.1f}%")
    lines.append(f"  看多时平均收益: {summary.avg_return_when_bullish:+.1f}% | 看空时平均收益: {summary.avg_return_when_bearish:+.1f}%")
    lines.append(f"  Sharpe: {summary.sharpe_ratio:.2f} | IR: {summary.info_ratio:.2f} | 最大回撤: {summary.max_drawdown:.1f}%")

    lines.append("")
    lines.append("-" * 60)
    lines.append("  维度归因分析")
    lines.append("-" * 60)
    for dim, attr in summary.dimension_attribution.items():
        label = dim_names.get(dim, dim)
        lines.append(f"  {label}({dim}):")
        lines.append(f"    胜率: {attr.signal_accuracy:.1%} | 评分相关性: {attr.score_return_corr:.2f}")
        lines.append(f"    看多胜率: {attr.win_rate_when_bullish:.1%} | 看空胜率: {attr.win_rate_when_bearish:.1%}")
        lines.append(f"    看多超额: {attr.avg_excess_when_bullish:+.1f}% | 看空超额: {attr.avg_excess_when_bearish:+.1f}%")
        lines.append(f"    贡献度: {attr.contribution:.0f} | 评级: {attr.rating}")

    lines.append("")
    lines.append("-" * 60)
    lines.append("  评级准确率")
    lines.append("-" * 60)
    for grade, stats in sorted(summary.grade_accuracy.items()):
        lines.append(f"  {grade}: {stats['accuracy']:.1%} ({stats['count']}样本, 均收益{stats['avg_return']:+.1f}%)")

    if summary.improvement_suggestions:
        lines.append("")
        lines.append("-" * 60)
        lines.append("  优化建议")
        lines.append("-" * 60)
        for s in summary.improvement_suggestions:
            lines.append(f"  {s}")

    lines.append("")
    lines.append("=" * 60)
    return "\n".join(lines)