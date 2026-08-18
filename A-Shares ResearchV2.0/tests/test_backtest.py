"""回溯测试闭环 — pytest 回归 (离线, 合成数据, 不调 LLM/网络)

运行：pytest tests/test_backtest.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from layers.backtest.engine import BacktestEngine


def scenario(tech_score, fund_score, val_score, actual_return, benchmark_return=5.0, date="2024-01-15", **kw):
    return {
        "analysis_date": date,
        "tech_signals": {
            "overall_score": tech_score, "confidence": 75,
            "trend_direction": "多头" if tech_score > 60 else ("空头" if tech_score < 40 else "震荡"),
            "momentum_status": "增强" if tech_score > 60 else "衰减",
            "volume_signal": "放量上涨" if tech_score > 60 else "缩量下跌",
        },
        "fund_signals": {
            "overall_score": fund_score, "confidence": 70,
            "roe": "优秀" if fund_score > 65 else "一般",
            "growth": "高增长" if fund_score > 70 else "平稳",
            "cashflow": "充裕" if fund_score > 60 else "紧张",
        },
        "valuation_signals": {
            "overall_score": val_score, "confidence": 80,
            "pe_percentile": 80 if val_score < 40 else (20 if val_score > 60 else 50),
            "pb_percentile": 75 if val_score < 40 else (25 if val_score > 60 else 50),
            "industry": "消费",
        },
        "actual_return_pct": actual_return,
        "benchmark_return_pct": benchmark_return,
        "price_at_analysis": 100.0,
        "price_at_future": 100.0 * (1 + actual_return / 100),
        "max_drawdown_pct": abs(min(actual_return, 0)) * 0.8,
        "volatility_pct": abs(actual_return) * 0.5,
        **kw,
    }


def build_scenarios():
    scenarios = []
    scenarios.append(scenario(78, 72, 70, 18.5, 5.0))
    scenarios.append(scenario(82, 75, 78, 22.0, 4.0))
    scenarios.append(scenario(75, 80, 72, 12.0, 3.0))
    scenarios.append(scenario(72, 30, 65, -5.0, 5.0))
    scenarios.append(scenario(80, 28, 75, -8.0, 3.0))
    scenarios.append(scenario(70, 72, 25, 8.0, 4.0))
    scenarios.append(scenario(25, 35, 30, -15.0, 5.0))
    scenarios.append(scenario(20, 30, 25, -20.0, 8.0))
    scenarios.append(scenario(30, 28, 35, -10.0, 3.0))
    scenarios.append(scenario(28, 70, 30, 5.0, 5.0))
    scenarios.append(scenario(25, 72, 35, -12.0, 6.0))
    scenarios.append(scenario(52, 55, 48, 1.0, 2.0))
    scenarios.append(scenario(48, 50, 52, -1.5, 1.0))
    scenarios.append(scenario(88, 85, 82, 25.0, 5.0))
    scenarios.append(scenario(15, 20, 18, -25.0, 10.0))
    scenarios.append(scenario(65, 68, 55, -3.0, 8.0))
    scenarios.append(scenario(68, 62, 60, 3.0, 4.0))
    scenarios.append(scenario(72, 42, 50, -8.0, 3.0))
    scenarios.append(scenario(38, 45, 40, 6.0, 4.0))
    scenarios.append(scenario(35, 40, 45, -6.0, 5.0))

    for i, s in enumerate(scenarios):
        month = 1 + (i // 4)
        day = 1 + (i % 4) * 7
        s["analysis_date"] = f"2024-{month:02d}-{day:02d}"
    return scenarios


def test_backtest_synthetic_loop():
    engine = BacktestEngine()
    stocks = ["000001", "600519", "000858", "002415", "300750"]
    scenarios = build_scenarios()

    results = engine.run_from_synthetic(stocks, scenarios, forecast_days=60)
    summary = engine.evaluate_all()

    assert len(results) > 0, "回溯测试应产生样本"
    for r in results:
        assert isinstance(r.is_correct_direction, bool)

    d = summary.to_dict()
    for key in ("total_samples", "overall_win_rate", "avg_excess_return"):
        assert key in d, f"summary 缺少字段 {key}"

    report = summary.to_report()
    assert isinstance(report, str) and report.strip()


def test_profit_scenarios_higher_accuracy_than_loss():
    # 用独立引擎，避免 run_from_synthetic 跨调用累计样本污染 evaluate_all 评估
    engine_bull = BacktestEngine()
    profit = [scenario(80, 80, 80, 20.0, 5.0), scenario(85, 82, 78, 18.0, 4.0)]
    engine_bull.run_from_synthetic(["600519"], profit, forecast_days=60)
    s_bull = engine_bull.evaluate_all()

    engine_bear = BacktestEngine()
    loss = [scenario(20, 25, 22, -18.0, 5.0), scenario(15, 20, 18, -22.0, 8.0)]
    engine_bear.run_from_synthetic(["600519"], loss, forecast_days=60)
    s_bear = engine_bear.evaluate_all()

    # 看多场景方向判断正确率应 >= 看空场景 (逻辑一致性校验)
    assert s_bull.overall_win_rate >= s_bear.overall_win_rate
