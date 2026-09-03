"""
回测闭环 runner：读取历史分析 → 取真实价格 → 评估 → 落库 → 生成校准上下文。

设计目标（对应"第8项：回测接入主链路"）：
  - verify_expired_analyses()：检验所有"已到期"历史分析，把方向对错结果落库。
    由 app.py 的每日刷新入口调用（非阻塞、失败跳过）。
  - get_calibration_context()：从已落库回测结果生成给 LLM 的校准提示，
    由 chief_agent.analyze() 注入，使系统"越用越准"（自我校准闭环）。

真实价格源默认走 price_fetcher.fetch_future_price（懒加载 akshare）；
离线/未安装 akshare 时该函数返回 None，样本被跳过，整套流程不报错、不依赖网络。
"""
import logging
from typing import Optional, Dict, List

from db import (
    get_analysis_records_for_backtest,
    save_backtest_outcome,
    get_backtest_outcomes,
)
from layers.backtest.engine import BacktestEngine
from layers.backtest.price_fetcher import fetch_future_price

logger = logging.getLogger("BacktestRunner")

# 默认回测展望期（与 FORECAST_HORIZONS 对齐）
DEFAULT_HORIZONS = [20, 60, 120]

# 维度中文名（校准上下文展示用）
DIM_LABELS = {"tech": "技术面", "fund": "基本面", "capital": "资金面",
              "industry": "行业面", "risk": "风险面", "valuation": "估值面"}


def verify_expired_analyses(
    stock_codes: Optional[List[str]] = None,
    horizons: Optional[List[int]] = None,
    price_fetcher=None,
) -> Dict[int, Dict]:
    """检验所有"已到期"的历史分析，落库方向验证结果。

    Args:
        stock_codes: 限定股票（None=全部有历史分析的自选股）。
        horizons: 展望期列表（None=DEFAULT_HORIZONS）。
        price_fetcher: 可注入的价格源（测试用 mock）；默认 fetch_future_price。
    Returns:
        {horizon_days: BacktestSummary.to_dict()}（仅含产生样本的周期）。
    """
    if horizons is None:
        horizons = DEFAULT_HORIZONS
    if price_fetcher is None:
        price_fetcher = fetch_future_price

    engine = BacktestEngine()
    summaries: Dict[int, Dict] = {}

    for horizon in horizons:
        pending = get_analysis_records_for_backtest(
            horizon_days=horizon, stock_codes=stock_codes
        )
        if not pending:
            continue
        results = engine.run_from_records(pending, price_fetcher, forecast_days=horizon)
        for r in results:
            try:
                save_backtest_outcome(r)
            except Exception as e:
                logger.warning(f"[BacktestRunner] 回测结果落库失败(已跳过): {e}")
        if results:
            summaries[horizon] = engine.evaluate_all(forecast_days=horizon).to_dict()
            logger.info(
                f"[BacktestRunner] 展望{horizon}天: 验证 {len(results)} 条, "
                f"方向胜率 {summaries[horizon]['overall_win_rate']:.0%}"
            )
    return summaries


def get_calibration_context(stock_code: str) -> str:
    """从已落库回测结果生成给 LLM 的校准上下文；无数据返回空串。

    三层校准信息（越用越准的反馈闭环）：
      1. 整体方向胜率（按展望期）
      2. 分维度胜率（dimension_results 落库数据聚合）
      3. confidence 校准偏差（自报置信度 vs 实际命中率的系统性偏差）
    """
    outcomes = get_backtest_outcomes(stock_code)
    if not outcomes:
        return ""

    # ── 1. 整体方向胜率（按展望期） ──
    by_h: Dict[int, Dict[str, int]] = {}
    for o in outcomes:
        h = o.get("forecast_days") or 60
        bucket = by_h.setdefault(h, {"correct": 0, "total": 0})
        bucket["total"] += 1
        if o.get("is_correct"):
            bucket["correct"] += 1

    lines = [f"[历史回测校准] 针对 {stock_code} 的方向验证历史:"]

    # ── 2. 分维度胜率（合并各展望期） ──
    dim_stats: Dict[str, Dict[str, int]] = {}
    for o in outcomes:
        dim_results = o.get("dimension_results")
        if not isinstance(dim_results, dict):
            continue
        for dim, correct in dim_results.items():
            s = dim_stats.setdefault(dim, {"correct": 0, "total": 0})
            s["total"] += 1
            if correct:
                s["correct"] += 1

    for h in sorted(by_h):
        c = by_h[h]["correct"]
        t = by_h[h]["total"]
        wr = (c / t * 100) if t else 0.0
        lines.append(f"  - {h}日展望: 方向胜率 {wr:.0f}% ({c}/{t})")

    weak_dims = []
    for dim in sorted(dim_stats, key=lambda d: dim_stats[d]["correct"] / max(dim_stats[d]["total"], 1)):
        s = dim_stats[dim]
        if s["total"] < 2:
            continue
        wr = s["correct"] / s["total"]
        label = DIM_LABELS.get(dim, dim)
        lines.append(f"  - {label}维度: 方向胜率 {wr:.0%} ({s['correct']}/{s['total']})")
        if wr < 0.45:
            weak_dims.append(label)

    # ── 3. confidence 校准偏差 ──
    conf_buckets: Dict[str, Dict[str, int]] = {}
    for o in outcomes:
        conf = o.get("predicted_confidence") or 0
        if conf <= 0:
            continue  # 旧数据无 confidence，跳过
        bucket = str((conf // 20) * 20)
        b = conf_buckets.setdefault(bucket, {"correct": 0, "total": 0})
        b["total"] += 1
        if o.get("is_correct"):
            b["correct"] += 1

    overconfident = []
    for bucket in sorted(conf_buckets):
        b = conf_buckets[bucket]
        if b["total"] < 2:
            continue
        actual = b["correct"] / b["total"]
        claimed = int(bucket) + 10  # 桶中点
        lines.append(f"  - 置信度{bucket}-{int(bucket)+20}区间: 实际命中率 {actual:.0%} ({b['correct']}/{b['total']})")
        if claimed > 0 and actual < (claimed / 100) - 0.20:
            overconfident.append(f"置信度{bucket}+区间实际命中率仅{actual:.0%}")

    # ── 汇总校准指令 ──
    lines.append("请结合上述历史准确率，对本轮各维度判断的置信度做相应调整："
                 "历史胜率高的维度可提高权重，偏低的维度请在结论中标注更高不确定性。")
    if weak_dims:
        lines.append(f"特别注意：{('、'.join(weak_dims))}维度历史方向胜率偏低（<45%），"
                     "该维度的判断请额外保守，并在 risk_factors 中提示其历史可靠性不足。")
    if overconfident:
        lines.append(f"置信度校准提示：{('；'.join(overconfident))}，存在系统性高估，"
                     "本轮 confidence 请相应下调，避免过度自信。")
    return "\n".join(lines)
