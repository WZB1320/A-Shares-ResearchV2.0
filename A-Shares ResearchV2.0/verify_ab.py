"""A/B 验证脚本：对比 ChiefAgent 开启/关闭 scheduler 两种模式的产出一致性

用法：
    python verify_ab.py 600519              # 默认股票
    python verify_ab.py 600519 --agents tech,fund,risk   # 指定维度
    python verify_ab.py 600519 --verbose    # 详细输出

对比维度：
    1. 执行成功率（是否报错）
    2. 综合评分差异（overall_score）
    3. 各维度评分差异（reports[dim].overall_score）
    4. 各维度评级差异（reports[dim].grade）
    5. 报告长度差异（final_report 字符数）
    6. 关键结论相似度（thesis 文本对比）
    7. 执行耗时差异

判定标准：
    - overall_score 差异 < 10：通过
    - 各维度评分差异 < 15：通过
    - 评级跨级跳变（如看多→看空）：警告
    - 任一模式报错：失败
"""
import sys
import time
import json
import logging
import argparse
from pathlib import Path
from difflib import SequenceMatcher

sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.WARNING,  # 减少 ChiefAgent 内部日志干扰
    format="[%(name)s] %(message)s",
)

from layers.agents.chief_agent import ChiefAgent


# ── 对比工具 ──────────────────────────────────────────────

def safe_get(d: dict, *keys, default=None):
    """安全嵌套取值"""
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k, default)
    return cur


def text_similarity(a: str, b: str) -> float:
    """文本相似度（0-1）"""
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def grade_to_numeric(grade: str) -> int:
    """评级转数值，用于判断跳变幅度"""
    mapping = {
        "强烈看多": 100, "看多": 75, "中性偏多": 62,
        "中性": 50, "中性偏空": 38, "看空": 25, "强烈看空": 0,
    }
    return mapping.get(grade, 50)


# ── 执行单次分析 ──────────────────────────────────────────

def run_once(stock_code: str, selected_agents, use_scheduler: bool, label: str):
    """执行一次分析，返回 (result_dict, elapsed_seconds, error)"""
    print(f"\n{'='*70}")
    print(f"[{label}] 开始分析 | stock={stock_code} | scheduler={use_scheduler}")
    print(f"{'='*70}")

    start = time.time()
    try:
        agent = ChiefAgent(use_scheduler=use_scheduler)
        state = {"selected_agents": selected_agents} if selected_agents else None
        result = agent.analyze(stock_code, state)
        elapsed = time.time() - start

        if isinstance(result, dict):
            print(f"[{label}] ✅ 成功 | 耗时: {elapsed:.1f}s | "
                  f"综合评分: {result.get('overall_score', '?')}")
            return result, elapsed, None
        else:
            # 理论上不会走到这里，但防御性处理
            print(f"[{label}] ⚠️ 返回类型异常: {type(result).__name__}")
            return {"final_report": str(result), "reports": {},
                    "overall_score": 0, "overall_grade": "异常"}, elapsed, None

    except Exception as e:
        elapsed = time.time() - start
        import traceback
        print(f"[{label}] ❌ 失败 | 耗时: {elapsed:.1f}s | 错误: {e}")
        print(traceback.format_exc())
        return None, elapsed, str(e)


# ── 对比分析 ─────────────────────────────────────────────

def compare_results(r_legacy, r_scheduler, verbose=False):
    """对比两次结果，返回 (passed, warnings, details)"""
    details = []
    warnings = []
    failures = []

    # 1. 基础完整性
    if r_legacy is None or r_scheduler is None:
        failures.append("任一模式执行失败，无法对比")
        return False, warnings, details

    # 2. 综合评分差异
    score_legacy = safe_get(r_legacy, "overall_score", default=0) or 0
    score_scheduler = safe_get(r_scheduler, "overall_score", default=0) or 0
    score_diff = abs(score_legacy - score_scheduler)
    details.append(f"综合评分: legacy={score_legacy} vs scheduler={score_scheduler} | 差异={score_diff}")

    if score_diff >= 10:
        failures.append(f"综合评分差异过大: {score_diff} (≥10)")
    elif score_diff >= 5:
        warnings.append(f"综合评分有差异: {score_diff} (≥5)")

    # 3. 综合评级
    grade_legacy = safe_get(r_legacy, "overall_grade", default="") or ""
    grade_scheduler = safe_get(r_scheduler, "overall_grade", default="") or ""
    grade_diff = abs(grade_to_numeric(grade_legacy) - grade_to_numeric(grade_scheduler))
    details.append(f"综合评级: legacy={grade_legacy} vs scheduler={grade_scheduler} | 数值差={grade_diff}")

    if grade_diff >= 25:  # 跨级跳变
        failures.append(f"评级跨级跳变: {grade_legacy} → {grade_scheduler}")
    elif grade_diff > 0:
        warnings.append(f"评级有差异: {grade_legacy} → {grade_scheduler}")

    # 4. 各维度评分对比
    reports_legacy = safe_get(r_legacy, "reports", default={}) or {}
    reports_scheduler = safe_get(r_scheduler, "reports", default={}) or {}
    all_dims = set(reports_legacy.keys()) | set(reports_scheduler.keys())

    details.append(f"\n各维度对比 (共 {len(all_dims)} 个维度):")
    details.append(f"{'维度':<12} {'legacy评分':<10} {'scheduler评分':<10} {'差异':<8} {'评级变化':<20}")
    details.append("-" * 70)

    for dim in sorted(all_dims):
        r1 = reports_legacy.get(dim, {})
        r2 = reports_scheduler.get(dim, {})
        s1 = safe_get(r1, "overall_score", default=0) or 0
        s2 = safe_get(r2, "overall_score", default=0) or 0
        g1 = safe_get(r1, "grade", default="") or ""
        g2 = safe_get(r2, "grade", default="") or ""
        diff = abs(s1 - s2)
        grade_change = f"{g1} → {g2}" if g1 != g2 else "一致"

        details.append(f"{dim:<12} {s1:<10} {s2:<10} {diff:<8} {grade_change:<20}")

        if diff >= 15:
            failures.append(f"维度 {dim} 评分差异过大: {diff} (≥15)")
        elif diff >= 10:
            warnings.append(f"维度 {dim} 评分有差异: {diff} (≥10)")

        # 评级跨级跳变
        g_num_diff = abs(grade_to_numeric(g1) - grade_to_numeric(g2))
        if g_num_diff >= 25:
            failures.append(f"维度 {dim} 评级跨级跳变: {g1} → {g2}")

    # 5. 报告长度对比
    report_legacy = safe_get(r_legacy, "final_report", default="") or ""
    report_scheduler = safe_get(r_scheduler, "final_report", default="") or ""
    len_legacy = len(report_legacy)
    len_scheduler = len(report_scheduler)
    len_diff = abs(len_legacy - len_scheduler)
    len_ratio = max(len_legacy, len_scheduler) / max(1, min(len_legacy, len_scheduler))

    details.append(f"\n报告长度: legacy={len_legacy} vs scheduler={len_scheduler} | 差异={len_diff} | 比值={len_ratio:.2f}")

    if len_ratio > 2.0:
        warnings.append(f"报告长度差异显著: 比值={len_ratio:.2f}")

    # 6. 关键结论相似度（thesis）
    if verbose:
        details.append("\n各维度 thesis 对比:")
        for dim in sorted(all_dims):
            t1 = safe_get(reports_legacy.get(dim, {}), "thesis", default="") or ""
            t2 = safe_get(reports_scheduler.get(dim, {}), "thesis", default="") or ""
            sim = text_similarity(t1, t2)
            details.append(f"  {dim}: 相似度={sim:.2f}")
            if sim < 0.3:
                warnings.append(f"维度 {dim} thesis 相似度过低: {sim:.2f}")

    # 7. parse_error 检查
    for dim in all_dims:
        e1 = safe_get(reports_legacy.get(dim, {}), "parse_error", default=False)
        e2 = safe_get(reports_scheduler.get(dim, {}), "parse_error", default=False)
        if e1 != e2:
            warnings.append(f"维度 {dim} parse_error 不一致: legacy={e1} vs scheduler={e2}")

    passed = len(failures) == 0
    return passed, warnings, details


# ── 主流程 ───────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="ChiefAgent A/B 验证脚本")
    parser.add_argument("stock_code", nargs="?", default="600519",
                        help="股票代码（默认 600519）")
    parser.add_argument("--agents", type=str, default=None,
                        help="指定维度，逗号分隔（如 tech,fund,risk）。默认全部6个")
    parser.add_argument("--verbose", action="store_true",
                        help="详细输出（含 thesis 相似度对比）")
    parser.add_argument("--output", type=str, default=None,
                        help="结果输出到 JSON 文件")
    args = parser.parse_args()

    stock_code = args.stock_code
    selected_agents = None
    if args.agents:
        selected_agents = [a.strip() for a in args.agents.split(",") if a.strip()]

    print("=" * 70)
    print(f"A/B 验证: ChiefAgent(use_scheduler=False) vs ChiefAgent(use_scheduler=True)")
    print(f"股票: {stock_code} | 维度: {selected_agents or '全部6个'}")
    print("=" * 70)

    # 执行两次分析
    r_legacy, t_legacy, e_legacy = run_once(
        stock_code, selected_agents, use_scheduler=False, label="A-LEGACY"
    )
    r_scheduler, t_scheduler, e_scheduler = run_once(
        stock_code, selected_agents, use_scheduler=True, label="B-SCHEDULER"
    )

    # 耗时对比
    print(f"\n{'='*70}")
    print("执行耗时对比")
    print(f"{'='*70}")
    print(f"  A-LEGACY:    {t_legacy:.1f}s {'❌ '+e_legacy if e_legacy else '✅'}")
    print(f"  B-SCHEDULER: {t_scheduler:.1f}s {'❌ '+e_scheduler if e_scheduler else '✅'}")
    if t_legacy > 0 and t_scheduler > 0:
        ratio = t_scheduler / t_legacy
        print(f"  比值 (B/A):  {ratio:.2f}x")
        if ratio > 1.5:
            print(f"  ⚠️ scheduler 模式明显更慢（{ratio:.2f}x），可能因质量门禁重试")
        elif ratio < 0.8:
            print(f"  ✅ scheduler 模式更快（{ratio:.2f}x）")

    # 对比结果
    print(f"\n{'='*70}")
    print("产出对比分析")
    print(f"{'='*70}")

    passed, warnings, details = compare_results(
        r_legacy, r_scheduler, verbose=args.verbose
    )

    for line in details:
        print(f"  {line}")

    if warnings:
        print(f"\n⚠️ 警告 ({len(warnings)}):")
        for w in warnings:
            print(f"  - {w}")

    # 最终结论
    print(f"\n{'='*70}")
    if passed and not e_legacy and not e_scheduler:
        print("✅ 验证通过：两种模式产出基本一致")
        print("   可以安全地将 use_scheduler 默认值改为 True")
    elif passed:
        print("⚠️ 部分通过：有警告但无严重失败")
        print("   建议检查警告项后再决定是否切换默认值")
    else:
        print("❌ 验证失败：两种模式产出存在显著差异")
        print("   不建议切换默认值，需排查原因")
    print(f"{'='*70}")

    # 输出 JSON
    if args.output:
        output_data = {
            "stock_code": stock_code,
            "selected_agents": selected_agents,
            "legacy": {
                "elapsed": t_legacy,
                "error": e_legacy,
                "overall_score": safe_get(r_legacy, "overall_score"),
                "overall_grade": safe_get(r_legacy, "overall_grade"),
                "report_length": len(safe_get(r_legacy, "final_report", default="") or ""),
            },
            "scheduler": {
                "elapsed": t_scheduler,
                "error": e_scheduler,
                "overall_score": safe_get(r_scheduler, "overall_score"),
                "overall_grade": safe_get(r_scheduler, "overall_grade"),
                "report_length": len(safe_get(r_scheduler, "final_report", default="") or ""),
            },
            "passed": passed,
            "warnings": warnings,
        }
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        print(f"\n结果已保存到: {args.output}")


if __name__ == "__main__":
    main()
