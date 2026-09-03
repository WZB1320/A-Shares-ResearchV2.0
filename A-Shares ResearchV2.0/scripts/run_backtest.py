"""回测报告命令行入口 —— 历史报告方向准确率 + confidence 校准

用法：
    python scripts/run_backtest.py                  # 验证到期分析 + 输出全量回测报告
    python scripts/run_backtest.py --no-verify      # 只读已有落库结果出报告（不取价格）
    python scripts/run_backtest.py --horizons 20,60 # 指定展望期
    python scripts/run_backtest.py --stocks 600519,300750

输出：reports/backtest/backtest_YYYYMMDD_HHMMSS.md
"""

import sys
import argparse
from pathlib import Path
from collections import defaultdict
from datetime import datetime

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import logging
logging.basicConfig(level=logging.WARNING, format="[%(name)s] %(message)s")

from db import get_all_backtest_outcomes
from layers.backtest.runner import verify_expired_analyses, DEFAULT_HORIZONS

DIM_LABELS = {"tech": "技术面", "fund": "基本面", "capital": "资金面",
              "industry": "行业面", "risk": "风险面", "valuation": "估值面"}


def build_report(outcomes: list) -> str:
    lines = [
        "# 回测报告：方向准确率 + confidence 校准",
        "",
        f"- **报告时间**：{datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"- **样本总数**：{len(outcomes)} 条已验证分析",
        "",
    ]
    if not outcomes:
        lines.append("暂无已落库回测结果。请先运行 `python scripts/run_backtest.py`（需网络取历史价格），"
                     "或等待每日自动刷新积累样本。")
        return "\n".join(lines)

    # ── 按展望期分组的整体胜率 ──
    by_h = defaultdict(lambda: {"correct": 0, "total": 0})
    for o in outcomes:
        h = o.get("forecast_days") or 60
        b = by_h[h]
        b["total"] += 1
        b["correct"] += int(o.get("is_correct") or 0)

    lines += ["## 整体方向胜率（按展望期）", "",
              "| 展望期 | 样本数 | 胜率 | 平均收益 | 平均超额 |", "|------|------|------|------|------|"]
    ret_by_h = defaultdict(list)
    excess_by_h = defaultdict(list)
    for o in outcomes:
        h = o.get("forecast_days") or 60
        if o.get("return_pct") is not None:
            ret_by_h[h].append(o["return_pct"])
        if o.get("excess_return_pct") is not None:
            excess_by_h[h].append(o["excess_return_pct"])
    for h in sorted(by_h):
        b = by_h[h]
        avg_r = sum(ret_by_h[h]) / len(ret_by_h[h]) if ret_by_h[h] else 0
        avg_e = sum(excess_by_h[h]) / len(excess_by_h[h]) if excess_by_h[h] else 0
        lines.append(f"| {h}日 | {b['total']} | {b['correct']/b['total']:.0%} | "
                     f"{avg_r:+.1f}% | {avg_e:+.1f}% |")

    # ── 分维度胜率 ──
    dim_stats = defaultdict(lambda: {"correct": 0, "total": 0})
    for o in outcomes:
        dr = o.get("dimension_results")
        if not isinstance(dr, dict):
            continue
        for dim, correct in dr.items():
            s = dim_stats[dim]
            s["total"] += 1
            s["correct"] += int(bool(correct))

    if dim_stats:
        lines += ["", "## 分维度方向胜率", "",
                  "| 维度 | 样本数 | 胜率 | 评价 |", "|------|------|------|------|"]
        for dim, s in sorted(dim_stats.items(),
                             key=lambda kv: kv[1]["correct"] / max(kv[1]["total"], 1)):
            wr = s["correct"] / s["total"] if s["total"] else 0
            rating = "⚠ 低于随机" if wr < 0.45 else ("有效" if wr >= 0.60 else "接近随机")
            lines.append(f"| {DIM_LABELS.get(dim, dim)}({dim}) | {s['total']} | {wr:.0%} | {rating} |")

    # ── 评级分布胜率 ──
    grade_stats = defaultdict(lambda: {"correct": 0, "total": 0, "rets": []})
    for o in outcomes:
        g = o.get("predicted_grade") or "未知"
        s = grade_stats[g]
        s["total"] += 1
        s["correct"] += int(o.get("is_correct") or 0)
        if o.get("return_pct") is not None:
            s["rets"].append(o["return_pct"])

    lines += ["", "## 按预测评级的胜率与平均收益", "",
              "| 评级 | 样本数 | 胜率 | 平均实际收益 |", "|------|------|------|------|"]
    for g in ["强烈看多", "看多", "中性偏多", "中性", "中性偏空", "看空", "强烈看空", "未知"]:
        s = grade_stats.get(g)
        if not s:
            continue
        wr = s["correct"] / s["total"] if s["total"] else 0
        avg_r = sum(s["rets"]) / len(s["rets"]) if s["rets"] else 0
        lines.append(f"| {g} | {s['total']} | {wr:.0%} | {avg_r:+.1f}% |")

    # ── confidence 校准 ──
    conf_stats = defaultdict(lambda: {"correct": 0, "total": 0})
    has_conf = 0
    for o in outcomes:
        conf = o.get("predicted_confidence") or 0
        if conf <= 0:
            continue
        has_conf += 1
        b = conf_stats[str((conf // 20) * 20)]
        b["total"] += 1
        b["correct"] += int(o.get("is_correct") or 0)

    lines += ["", "## confidence 校准（自报置信度 vs 实际命中率）", ""]
    if not has_conf:
        lines.append("暂无含 confidence 的回测样本（历史记录早于 confidence 落库功能上线）。"
                     "新分析到期验证后此表自动生成。")
    else:
        lines += ["| 置信度区间 | 样本数 | 实际命中率 | 声称水平(桶中点) | 偏差 | 校准评价 |",
                  "|------|------|------|------|------|------|"]
        for bucket in sorted(conf_stats, key=int):
            s = conf_stats[bucket]
            actual = s["correct"] / s["total"] if s["total"] else 0
            claimed = (int(bucket) + 10) / 100
            delta = actual - claimed
            calib = "✅ 良好" if abs(delta) <= 0.10 else ("⚠ 高估" if delta < 0 else "⚠ 低估")
            lines.append(f"| {bucket}-{int(bucket)+20} | {s['total']} | {actual:.0%} | "
                         f"{claimed:.0%} | {delta:+.0%} | {calib} |")
        lines.append("")
        lines.append("> 校准评价标准：|实际命中率 - 声称水平| ≤ 10% 为良好；负偏差 >10% 为系统性高估（需下调 confidence）")

    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="回测报告")
    parser.add_argument("--no-verify", action="store_true", help="跳过到期验证，直接用已有落库结果出报告")
    parser.add_argument("--horizons", type=str, default=None, help="展望期，逗号分隔（默认 20,60,120）")
    parser.add_argument("--stocks", type=str, default=None, help="限定股票，逗号分隔")
    args = parser.parse_args()

    horizons = [int(h) for h in args.horizons.split(",")] if args.horizons else DEFAULT_HORIZONS
    stocks = [s.strip() for s in args.stocks.split(",")] if args.stocks else None

    if not args.no_verify:
        print(f"[回测] 验证到期分析 | 展望期: {horizons}")
        summaries = verify_expired_analyses(stock_codes=stocks, horizons=horizons)
        for h, s in sorted(summaries.items()):
            print(f"[回测] {h}日: 有效样本 {s.get('valid_samples', '?')} 条, "
                  f"方向胜率 {s.get('overall_win_rate', 0):.0%}")
        if not summaries:
            print("[回测] 无到期未验证的分析（或价格获取失败）")

    outcomes = get_all_backtest_outcomes()
    if stocks:
        outcomes = [o for o in outcomes if o.get("stock_code") in stocks]

    out_dir = PROJECT_ROOT / "reports" / "backtest"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"backtest_{ts}.md"
    out_path.write_text(build_report(outcomes), encoding="utf-8")

    print(f"[回测] 报告已生成: {out_path} | 样本 {len(outcomes)} 条")


if __name__ == "__main__":
    main()
