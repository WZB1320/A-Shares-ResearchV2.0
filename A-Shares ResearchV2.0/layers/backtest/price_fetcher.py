"""
真实历史价格抓取（懒加载 akshare，离线/未安装时返回 None）。

仅支持 A 股。返回结构：
    {"price_at_future": float, "benchmark_return_pct": float}
若无法取得（非 A 股 / akshare 未装 / 网络失败），返回 None —— 由调用方跳过该样本。

本模块刻意保持轻量、零顶层重依赖：akshare / pandas 均在函数内懒导入，
因此离线 pytest 导入本模块不会触发网络或缺失依赖。
"""
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict

logger = logging.getLogger("BacktestPrice")

# 沪深300 作为基准（计算超额收益）
BENCHMARK_SYMBOL = "sh000300"


def _is_ashare(stock_code: str) -> bool:
    code = stock_code.strip().upper().replace(".", "").replace("SH", "").replace("SZ", "")
    return code.isdigit() and len(code) == 6


def _clean_ashare_code(stock_code: str) -> str:
    code = stock_code.strip().upper().replace("SH", "").replace("SZ", "")
    code = code.replace(".SH", "").replace(".SZ", "")
    return code.zfill(6) if code.isdigit() else code


def _benchmark_return(ak, analysis_date: str, horizon_days: int) -> float:
    """沪深300 在 [analysis_date, analysis_date+horizon] 区间收益率。"""
    try:
        df = ak.stock_zh_index_daily(symbol=BENCHMARK_SYMBOL)
        if df is None or df.empty:
            return 0.0
        df = df.copy()
        df["date"] = _to_dt(df["date"])
        a = datetime.strptime(analysis_date, "%Y-%m-%d")
        b = a + timedelta(days=horizon_days)
        pre = df[df["date"] <= a]
        post = df[df["date"] >= b]
        if pre.empty or post.empty:
            return 0.0
        p0 = float(pre.iloc[-1]["close"])
        p1 = float(post.iloc[0]["close"])
        if p0 <= 0:
            return 0.0
        return (p1 - p0) / p0 * 100
    except Exception as e:
        logger.debug(f"[BacktestPrice] 基准收益计算失败: {e}")
        return 0.0


def _to_dt(series):
    try:
        return series.apply(lambda x: datetime.strptime(str(x)[:10], "%Y-%m-%d"))
    except Exception:
        return series


def fetch_future_price(
    stock_code: str,
    analysis_date: str,
    horizon_days: int,
    benchmark: bool = True,
) -> Optional[Dict[str, float]]:
    """取分析日后 horizon_days 天的真实收盘价，返回价格 + 基准收益。

    返回 None 表示无法取得（非 A 股 / akshare 未装 / 网络或数据缺失）。
    """
    if not _is_ashare(stock_code):
        logger.warning(f"[BacktestPrice] 暂仅支持 A 股: {stock_code}")
        return None
    try:
        import akshare as ak  # 懒导入：离线环境无 akshare 时整函数安全返回 None
    except ImportError:
        logger.warning("[BacktestPrice] akshare 未安装，无法获取真实价格（跳过真实回测）")
        return None

    try:
        code = _clean_ashare_code(stock_code)
        a_dt = datetime.strptime(analysis_date, "%Y-%m-%d")
        start = (a_dt - timedelta(days=10)).strftime("%Y%m%d")
        end = (a_dt + timedelta(days=horizon_days + 10)).strftime("%Y%m%d")

        df = ak.stock_zh_a_hist(symbol=code, period="daily",
                                start_date=start, end_date=end, adjust="qfq")
        if df is None or getattr(df, "empty", True):
            return None
        df = df.copy()
        df["date"] = _to_dt(df["日期"])
        target = a_dt + timedelta(days=horizon_days)

        at_rows = df[df["date"] >= a_dt]
        future_rows = df[df["date"] >= target]
        if at_rows.empty or future_rows.empty:
            return None

        price_at = float(at_rows.iloc[0]["收盘"])
        price_future = float(future_rows.iloc[0]["收盘"])
        if price_at <= 0:
            return None

        bench_return = (
            _benchmark_return(ak, analysis_date, horizon_days) if benchmark else 0.0
        )
        return {
            "price_at_future": price_future,
            "benchmark_return_pct": bench_return,
        }
    except Exception as e:
        logger.warning(f"[BacktestPrice] 获取真实价格失败({stock_code}): {e}")
        return None
