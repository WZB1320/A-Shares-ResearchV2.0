"""Golden Dataset 评测脚本 —— Prompt/Agent 迭代的回归测试基准

用法：
    python scripts/run_eval.py                    # 全量股票池（8只）
    python scripts/run_eval.py --quick            # 快速模式（3只，~10分钟）
    python scripts/run_eval.py --stocks 600519,300750   # 指定股票
    python scripts/run_eval.py --compare          # 与上一次评测结果对比

评测指标（与现有质量体系口径一致）：
    1. 执行成功率：ChiefAgent 全流程是否正常返回
    2. 幻觉率：data_verification.hallucination_rate（Phase 1 校验器产出）
    3. 校验通过率：data_verification.passed 占比
    4. 结构通过率：无 parse_error + 必填字段完整 + grade 合法
    5. 评分-评级一致性：overall_score 与 grade 的映射偏差（GRADE_WEIGHTS 口径）
    6. 有效维度数：排除 error/unavailable 报告（数据不足自动跳过场景）

输出：
    reports/eval/eval_YYYYMMDD_HHMMSS.json   原始结果（供 --compare 与趋势追踪）
    reports/eval/eval_YYYYMMDD_HHMMSS.md     评测报告（KPI 汇总 + 逐股明细）
"""

import sys
import json
import time
import logging
import argparse
from pathlib import Path
from datetime import datetime

# Windows 控制台 GBK 编码兼容
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.WARNING, format="[%(name)s] %(message)s")

from layers.agents.chief_agent import ChiefAgent
from layers.agents.report_schema import grade_score_gap

# ── Golden 股票池：覆盖分组差异化 + 数据质量边界样本 ──────────────
GOLDEN_POOL = {
    "600519": {"name": "贵州茅台", "group": "消费", "note": "大盘蓝筹/高ROE"},
    "300750": {"name": "宁德时代", "group": "科技", "note": "成长科技/高波动"},
    "002415": {"name": "海康威视", "group": "科技", "note": "成熟科技/白马"},
    "600019": {"name": "宝钢股份", "group": "周期", "note": "周期股/PB定价"},
    "601866": {"name": "中远海发", "group": "周期", "note": "强周期/低估值"},
    "601318": {"name": "中国平安", "group": "金融", "note": "金融/低PE高分红"},
    "000725": {"name": "京东方A", "group": "周期科技", "note": "重资产/利润波动大"},
    "002272": {"name": "川润股份", "group": "边界样本", "note": "亏损股/数据质量差"},
}

QUICK_POOL = ["600519", "300750", "002272"]  # 覆盖 消费/科技/边界 三个代表

ALL_DIMS = ["tech", "fund", "capital", "industry", "risk", "valuation"]
DIM_LABELS = {
    "tech": "技术面", "fund": "基本面", "capital": "资金面",
    "industry": "行业面", "risk": "风险面", "valuation": "估值面",
}


# ── 单维度报告评估 ──────────────────────────────────────────

def evaluate_dimension_report(dim: str, report: dict) -> dict:
    """评估单个维度报告，返回指标明细"""
    m = {
        "dim": dim,
        "status": "ok",          # ok / error / unavailable
        "structure_pass": False,
        "grade_valid": False,
        "grade_consistent": False,
        "grade_gap": None,       # 评分-评级档位差（0=完全一致，标准映射口径）
        "score": None,
        "grade": None,
        "grade_corrected": False,
        "hallucination_rate": None,
        "verification_passed": None,
        "verifiable_numbers": 0,
        "unverifiable": [],
        "confidence": None,
    }

    if not isinstance(report, dict):
        m["status"] = "error"
        return m

    thesis = report.get("thesis", "") or ""

    # error / unavailable 识别（error_report/unavailable_report 的特征）
    if report.get("parse_error") or thesis.startswith("⚠") or "不可用" in thesis[:20]:
        if "数据不足" in thesis or "不可用" in thesis:
            m["status"] = "unavailable"
        else:
            m["status"] = "error"
        return m

    # 1. 结构检查
    m["structure_pass"] = bool(
        thesis
        and report.get("key_signals")
        and report.get("risk_factors")
        and isinstance(report.get("overall_score"), int)
    )

    m["score"] = report.get("overall_score")
    m["grade"] = report.get("grade")
    m["grade_corrected"] = bool(report.get("grade_corrected"))

    # 2. 评级合法性与评分-评级一致性（标准映射口径，与纠正规则一致：gap ≤1 可接受）
    gap = grade_score_gap(m["score"], m["grade"])
    if gap is not None:
        m["grade_valid"] = True
        m["grade_gap"] = gap
        m["grade_consistent"] = gap <= 1

    # 4. 幻觉校验（Phase 1 产出）
    dv = report.get("data_verification")
    if isinstance(dv, dict):
        m["hallucination_rate"] = dv.get("hallucination_rate")
        m["verification_passed"] = dv.get("passed")
        m["verifiable_numbers"] = dv.get("total_numbers", 0)
        unverifiable = dv.get("unverifiable") or []
        m["unverifiable"] = [
            {"value": u.get("raw"), "text": (u.get("text") or "")[:60]}
            for u in unverifiable[:5]
        ]

    m["confidence"] = report.get("confidence")
    return m


# ── 单股票评估 ─────────────────────────────────────────────

def evaluate_stock(stock_code: str, meta: dict) -> dict:
    """跑一只股票全流程并评估，返回该股的评测明细"""
    start = time.time()
    result = None
    error = None
    try:
        agent = ChiefAgent()
        result = agent.analyze(stock_code)
        if isinstance(result, str):  # 防御：旧路径返回字符串
            result = {"final_report": result, "reports": {}, "_error": True}
    except Exception as e:
        import traceback
        error = f"{e.__class__.__name__}: {e}"
        logging.error(f"[Eval] {stock_code} 执行失败: {traceback.format_exc()}")

    elapsed = round(time.time() - start, 1)
    print(f"  [{'❌ ' + error if error else '✅'}] {stock_code} {meta.get('name', '')} "
          f"| 耗时 {elapsed}s | 综合评分 {result.get('overall_score') if result else '-'}")

    dim_reports = (result or {}).get("reports", {})
    dim_metrics = []
    for dim in ALL_DIMS:
        if dim in dim_reports:
            dim_metrics.append(evaluate_dimension_report(dim, dim_reports[dim]))
        else:
            mm = evaluate_dimension_report(dim, {})
            mm["status"] = "error"
            dim_metrics.append(mm)

    valid = [m for m in dim_metrics if m["status"] == "ok"]
    halluc_measured = [m for m in valid if m["hallucination_rate"] is not None]

    return {
        "stock_code": stock_code,
        "name": meta.get("name", ""),
        "group": meta.get("group", ""),
        "note": meta.get("note", ""),
        "elapsed_s": elapsed,
        "execution_success": error is None and not (result or {}).get("_error"),
        "error": error,
        "overall_score": (result or {}).get("overall_score"),
        "overall_grade": (result or {}).get("overall_grade"),
        "valid_dims": len(valid),
        "dimensions": dim_metrics,
        # 股级聚合指标（仅基于有效维度）
        "kpi": {
            "structure_pass_rate": (
                sum(1 for m in valid if m["structure_pass"]) / len(valid) if valid else None
            ),
            "grade_consistent_rate": (
                sum(1 for m in valid if m["grade_consistent"]) / len(valid) if valid else None
            ),
            "hallucination_rate_avg": (
                sum(m["hallucination_rate"] for m in halluc_measured) / len(halluc_measured)
                if halluc_measured else None
            ),
            "verification_pass_rate": (
                sum(1 for m in halluc_measured if m["verification_passed"]) / len(halluc_measured)
                if halluc_measured else None
            ),
        },
    }


# ── 汇总与报告生成 ─────────────────────────────────────────

def fmt_pct(v):
    return f"{v:.0%}" if isinstance(v, (int, float)) else "-"


def aggregate(stocks_results: list) -> dict:
    """全股票池 KPI 聚合"""
    all_dims = []
    for s in stocks_results:
        all_dims.extend(m for m in s["dimensions"] if m["status"] == "ok")
    halluc = [m for m in all_dims if m["hallucination_rate"] is not None]
    return {
        "stocks_total": len(stocks_results),
        "stocks_success": sum(1 for s in stocks_results if s["execution_success"]),
        "dims_valid": len(all_dims),
        "dims_expected": len(stocks_results) * len(ALL_DIMS),
        "structure_pass_rate": (
            sum(1 for m in all_dims if m["structure_pass"]) / len(all_dims) if all_dims else None
        ),
        "grade_consistent_rate": (
            sum(1 for m in all_dims if m["grade_consistent"]) / len(all_dims) if all_dims else None
        ),
        "hallucination_rate_avg": (
            sum(m["hallucination_rate"] for m in halluc) / len(halluc) if halluc else None
        ),
        "hallucination_rate_max": max((m["hallucination_rate"] for m in halluc), default=None),
        "verification_pass_rate": (
            sum(1 for m in halluc if m["verification_passed"]) / len(halluc) if halluc else None
        ),
        "avg_elapsed_s": (
            sum(s["elapsed_s"] for s in stocks_results) / len(stocks_results)
            if stocks_results else None
        ),
    }


def find_latest_eval(eval_dir: Path, exclude: str = None):
    """找最近一次评测的 JSON（供 --compare）"""
    files = sorted(eval_dir.glob("eval_*.json"))
    if exclude:
        files = [f for f in files if f.name != exclude]
    return files[-1] if files else None


def build_markdown(agg: dict, stocks_results: list, baseline_agg: dict = None,
                   model_info: str = "") -> str:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [
        "# Agent 评测报告（Golden Dataset）",
        "",
        f"- **评测时间**：{ts}",
        f"- **股票池**：{agg['stocks_total']} 只（成功 {agg['stocks_success']}）",
        f"- **有效维度**：{agg['dims_valid']}/{agg['dims_expected']}",
        f"- **平均耗时**：{agg['avg_elapsed_s']:.0f}s/只" if agg["avg_elapsed_s"] else "- **平均耗时**：-",
        "",
        "## 核心 KPI",
        "",
        "| 指标 | 本次" + (" | 基线 | 变化 |" if baseline_agg else " |"),
        "|------|------" + ("|------|------|" if baseline_agg else "|------|"),
    ]

    def kpi_row(label, key, fmt=fmt_pct, invert=False):
        cur = agg.get(key)
        row = f"| {label} | {fmt(cur)} |"
        if baseline_agg:
            base = baseline_agg.get(key)
            delta = ""
            if isinstance(cur, (int, float)) and isinstance(base, (int, float)):
                d = cur - base
                # invert=True 时数值越小越好（如幻觉率）
                good = (d < 0) if invert else (d > 0)
                icon = "✅" if (abs(d) < 0.02 or good) else "⚠️"
                delta = f" {icon} {d:+.1%}" if abs(d) >= 0.005 else " 持平"
            row += f" {fmt(base)} |{delta} |"
        return row

    lines += [
        kpi_row("结构通过率", "structure_pass_rate"),
        kpi_row("评分-评级一致率", "grade_consistent_rate"),
        kpi_row("平均幻觉率 ↓", "hallucination_rate_avg", invert=True),
        kpi_row("幻觉率最大值 ↓", "hallucination_rate_max", invert=True),
        kpi_row("数字校验通过率", "verification_pass_rate"),
        "",
        "> 幻觉率 = 报告自由文本中未在数据上下文找到的数字占比（阈值 20%，超限自动修正重试一次）",
        "",
        "## 逐股明细",
        "",
        "| 股票 | 分组 | 执行 | 有效维度 | 幻觉率均值 | 校验通过 | 结构通过 | 一致率 | 评级纠正 | 耗时 |",
        "|------|------|------|----------|-----------|---------|---------|-------|---------|------|",
    ]
    for s in stocks_results:
        k = s["kpi"]
        corrected = sum(1 for m in s["dimensions"] if m.get("grade_corrected"))
        lines.append(
            f"| {s['stock_code']} {s['name']} | {s['group']} | "
            f"{'✅' if s['execution_success'] else '❌'} | {s['valid_dims']}/6 | "
            f"{fmt_pct(k['hallucination_rate_avg'])} | {fmt_pct(k['verification_pass_rate'])} | "
            f"{fmt_pct(k['structure_pass_rate'])} | {fmt_pct(k['grade_consistent_rate'])} | "
            f"{corrected} | {s['elapsed_s']:.0f}s |"
        )

    # 评分-评级脱节明细（gap ≥2，供 Prompt 迭代定位）
    divergent = [
        f"- {s['stock_code']} {DIM_LABELS.get(m['dim'], m['dim'])}: "
        f"score={m['score']} grade={m['grade']}（档位差 {m['grade_gap']}）"
        + ("（已自动纠正）" if m["grade_corrected"] else "")
        for s in stocks_results for m in s["dimensions"]
        if m["status"] == "ok" and m["grade_gap"] is not None and m["grade_gap"] >= 2
    ]
    if divergent:
        lines += ["", "## 评分-评级脱节明细（档位差 ≥2）", ""] + divergent

    # 未通过校验的数字明细（供 Prompt 迭代定位问题）
    bad_numbers = []
    for s in stocks_results:
        for m in s["dimensions"]:
            if m.get("unverifiable"):
                for u in m["unverifiable"]:
                    bad_numbers.append(
                        f"- {s['stock_code']} {DIM_LABELS.get(m['dim'], m['dim'])}: "
                        f"“{u['value']}” ← {u['text']}"
                    )
    if bad_numbers:
        lines += ["", "## 未通过校验的数字明细", ""] + bad_numbers[:30]

    return "\n".join(lines) + "\n"


# ── 主流程 ────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Golden Dataset 评测")
    parser.add_argument("--quick", action="store_true", help="快速模式（3只代表股）")
    parser.add_argument("--stocks", type=str, default=None, help="指定股票，逗号分隔")
    parser.add_argument("--compare", action="store_true", help="与最近一次评测结果对比")
    args = parser.parse_args()

    if args.stocks:
        pool = {c.strip(): GOLDEN_POOL.get(c.strip(), {"name": c.strip(), "group": "自定义"})
                for c in args.stocks.split(",") if c.strip()}
    elif args.quick:
        pool = {c: GOLDEN_POOL[c] for c in QUICK_POOL if c in GOLDEN_POOL}
    else:
        pool = GOLDEN_POOL

    print("=" * 70)
    print(f"[评测] Golden Dataset 开始 | 股票池: {list(pool.keys())}")
    print("=" * 70)

    eval_dir = PROJECT_ROOT / "reports" / "eval"
    eval_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = eval_dir / f"eval_{ts}.json"
    md_path = eval_dir / f"eval_{ts}.md"

    stocks_results = []
    for code, meta in pool.items():
        print(f"\n[评测] 开始: {code} {meta.get('name', '')}（{meta.get('group', '')}）")
        stocks_results.append(evaluate_stock(code, meta))

    agg = aggregate(stocks_results)

    # 基线对比
    baseline_agg = None
    if args.compare:
        prev = find_latest_eval(eval_dir, exclude=json_path.name)
        if prev:
            try:
                baseline_agg = json.loads(prev.read_text(encoding="utf-8"))["aggregate"]
                print(f"\n[评测] 基线: {prev.name}")
            except Exception as e:
                print(f"[评测] 基线读取失败（跳过对比）: {e}")
        else:
            print("[评测] 无历史评测可对比")

    md = build_markdown(agg, stocks_results, baseline_agg)
    md_path.write_text(md, encoding="utf-8")
    json_path.write_text(
        json.dumps(
            {"timestamp": ts, "stock_pool": list(pool.keys()),
             "aggregate": agg, "stocks": stocks_results},
            ensure_ascii=False, indent=2,
        ),
        encoding="utf-8",
    )

    print("\n" + "=" * 70)
    print(f"[评测] 完成 | 结构通过率 {fmt_pct(agg['structure_pass_rate'])} | "
          f"平均幻觉率 {fmt_pct(agg['hallucination_rate_avg'])} | "
          f"校验通过率 {fmt_pct(agg['verification_pass_rate'])}")
    print(f"[评测] 报告: {md_path}")
    print(f"[评测] 数据: {json_path}")
    print("=" * 70)


if __name__ == "__main__":
    main()
