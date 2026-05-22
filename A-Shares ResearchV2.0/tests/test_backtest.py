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

    scenarios.append(scenario(78, 72, 70, 18.5, 5.0))   # 全面看多,大涨
    scenarios.append(scenario(82, 75, 78, 22.0, 4.0))   # 全面看多,暴涨
    scenarios.append(scenario(75, 80, 72, 12.0, 3.0))   # 全面看多,温和涨

    scenarios.append(scenario(72, 30, 65, -5.0, 5.0))   # 技术与估值看多,基本面看空,实际跌
    scenarios.append(scenario(80, 28, 75, -8.0, 3.0))   # 同上,跌更多
    scenarios.append(scenario(70, 72, 25, 8.0, 4.0))    # 技术+基本面看多,估值看空,涨

    scenarios.append(scenario(25, 35, 30, -15.0, 5.0))  # 全面看空,大跌
    scenarios.append(scenario(20, 30, 25, -20.0, 8.0))  # 全面看空,暴跌
    scenarios.append(scenario(30, 28, 35, -10.0, 3.0))  # 全面看空,温和跌

    scenarios.append(scenario(28, 70, 30, 5.0, 5.0))    # 技术+估值看空,基本面看多,略涨(预测错)
    scenarios.append(scenario(25, 72, 35, -12.0, 6.0))  # 同上,跌(预测对)

    scenarios.append(scenario(52, 55, 48, 1.0, 2.0))    # 全面中性,横盘
    scenarios.append(scenario(48, 50, 52, -1.5, 1.0))   # 全面中性,微跌

    scenarios.append(scenario(88, 85, 82, 25.0, 5.0))   # 强烈看多,暴涨
    scenarios.append(scenario(15, 20, 18, -25.0, 10.0))  # 强烈看空,暴跌

    scenarios.append(scenario(65, 68, 55, -3.0, 8.0))    # 偏多但跌(预测错)
    scenarios.append(scenario(68, 62, 60, 3.0, 4.0))     # 偏多微涨(预测对但收益小)

    scenarios.append(scenario(72, 42, 50, -8.0, 3.0))    # 技术看多,其他中性,实际跌
    scenarios.append(scenario(38, 45, 40, 6.0, 4.0))     # 偏空,实际涨(预测错)
    scenarios.append(scenario(35, 40, 45, -6.0, 5.0))    # 偏空,实际跌(预测对)

    for i, s in enumerate(scenarios):
        month = 1 + (i // 4)
        day = 1 + (i % 4) * 7
        s["analysis_date"] = f"2024-{month:02d}-{day:02d}"

    return scenarios


def main():
    engine = BacktestEngine()

    stocks = ["000001", "600519", "000858", "002415", "300750"]
    scenarios = build_scenarios()

    print("=" * 60)
    print("  A-Shares-Research 回溯测试闭环验证")
    print("=" * 60)
    print(f"  股票池: {stocks}")
    print(f"  场景数: {len(scenarios)}")
    print(f"  总样本: {len(stocks) * len(scenarios)}")
    print()

    results = engine.run_from_synthetic(stocks, scenarios, forecast_days=60)
    summary = engine.evaluate_all()

    print(summary.to_report())

    print("\n--- 详细样本(前10条) ---")
    for i, r in enumerate(results[:10]):
        s = r.snapshot
        o = r.outcome
        mark = "[OK]" if r.is_correct_direction else "[X]"
        print(
            f"  {mark} {s.stock_code} | "
            f"预测:{s.overall_grade}({s.overall_score}) | "
            f"实际收益:{o.return_pct:+.1f}% | 超额:{o.excess_return_pct:+.1f}%"
        )

    print(f"\n  ... 共 {len(results)} 条样本")

    print("\n--- JSON 摘要 ---")
    import json
    print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2, default=str)[:2000])
    print("  ... (truncated)")

    print("\n[PASS] 回溯测试闭环验证完成")


if __name__ == "__main__":
    main()