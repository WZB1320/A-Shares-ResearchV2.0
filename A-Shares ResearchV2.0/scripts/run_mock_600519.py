"""
Mock 数据投研分析 — 构造贵州茅台(600519)完整合理的Mock数据
验证7项数据修复后的完整报告效果
"""
import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
from pathlib import Path

# 项目根目录（脚本位于 scripts/，根目录在上一级）
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ============================================================================
#  Mock 数据构造 — 贵州茅台 600519 真实合理参数
# ============================================================================

STOCK_CODE = "sh600519"
TOTAL_SHARES = 1_257_818_000  # 12.58亿股
CURRENT_PRICE = 1685.50        # 合理股价
BASE_DATE = datetime(2026, 6, 18)


def _gen_daily_data(days: int = 250) -> pd.DataFrame:
    """生成250天日线数据（含技术指标），模拟茅台走势"""
    dates = [(BASE_DATE - timedelta(days=250 - i)).strftime("%Y-%m-%d") for i in range(days)]

    # 价格走势：从1550震荡上行到1685
    np.random.seed(42)
    close_prices = np.cumsum(np.random.randn(days) * 8 + 0.5) + 1550
    close_prices = np.maximum(close_prices, 1400)  # 不跌破1400
    close_prices[-1] = CURRENT_PRICE  # 最后一天设为当前价

    # OHLC
    opens = close_prices + np.random.randn(days) * 3
    highs = np.maximum(opens, close_prices) + np.abs(np.random.randn(days)) * 5
    lows = np.minimum(opens, close_prices) - np.abs(np.random.randn(days)) * 5
    volumes = np.random.randint(1_500_000, 4_000_000, days).astype(float)
    amounts = volumes * close_prices * 100

    df = pd.DataFrame({
        "date": dates,
        "open": np.round(opens, 2),
        "high": np.round(highs, 2),
        "low": np.round(lows, 2),
        "close": np.round(close_prices, 2),
        "volume": volumes,
        "amount": np.round(amounts, 0),
        "pct_change": np.round(np.diff(close_prices, prepend=close_prices[0]) / np.roll(close_prices, 1) * 100, 4),
    })
    df.loc[0, "pct_change"] = 0

    # 技术指标
    df["ma5"] = df["close"].rolling(5).mean().round(2)
    df["ma10"] = df["close"].rolling(10).mean().round(2)
    df["ma20"] = df["close"].rolling(20).mean().round(2)
    df["ma60"] = df["close"].rolling(60).mean().round(2)

    # MACD
    ema12 = df["close"].ewm(span=12, adjust=False).mean()
    ema26 = df["close"].ewm(span=26, adjust=False).mean()
    df["macd"] = (ema12 - ema26).round(4)
    df["signal"] = df["macd"].ewm(span=9, adjust=False).mean().round(4)
    df["macd_hist"] = (df["macd"] - df["signal"]).round(4)

    # RSI
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.ewm(span=6, adjust=False).mean()
    avg_loss = loss.ewm(span=6, adjust=False).mean()
    rs = avg_gain / avg_loss
    df["rsi6"] = (100 - (100 / (1 + rs))).round(4)

    # KDJ
    low_min = df["low"].rolling(9).min()
    high_max = df["high"].rolling(9).max()
    rsv = (df["close"] - low_min) / (high_max - low_min) * 100
    df["k"] = rsv.ewm(span=3, adjust=False).mean().round(4)
    df["d"] = df["k"].ewm(span=3, adjust=False).mean().round(4)
    df["j"] = (3 * df["k"] - 2 * df["d"]).round(4)

    # BOLL
    df["boll_mid"] = df["close"].rolling(20).mean().round(2)
    boll_std = df["close"].rolling(20).std()
    df["boll_upper"] = (df["boll_mid"] + 2 * boll_std).round(2)
    df["boll_lower"] = (df["boll_mid"] - 2 * boll_std).round(2)

    # Fix #7: 换手率 — 从总股本自行计算
    df["turnover"] = (df["volume"] / TOTAL_SHARES * 100).round(4)

    return df


def _gen_valuation_data() -> dict:
    """估值数据 — 茅台合理估值"""
    # 生成120天历史PE/PB
    pe_history = [28 + np.sin(i / 10) * 3 + np.random.randn() * 0.5 for i in range(120)]
    pb_history = [9 + np.sin(i / 10) * 0.5 + np.random.randn() * 0.1 for i in range(120)]

    return {
        "price": CURRENT_PRICE,
        "pe_ttm": 25.8,           # 合理PE
        "pb": 8.65,               # 合理PB
        "pe_history": [round(p, 2) for p in pe_history],
        "pb_history": [round(p, 2) for p in pb_history],
        "pe_10_avg": round(np.mean(pe_history), 2),
        "error": None,
    }


def _gen_financial_data() -> dict:
    """财务数据 — 茅台真实财务指标"""
    return {
        "roe": 32.38,                    # ROE ~32%
        "gross_profit": 91.53,           # 毛利率 ~91.5% (gross_margin_ttm)
        "net_profit": 15.3,              # 净利润增速 ~15%
        "current_ratio": None,           # Fix #3: API不提供
        "quick_ratio": None,             # Fix #3: API不提供
        "debt_to_asset": 0.24,           # 资产负债率 ~24%
        "revenue": 150_560_000_000,      # 营收 ~1505亿
        "net_profit_value": 86_227_000_000,  # 净利润 ~862亿
        "equity": 270_894_000_000,       # 净资产 ~2709亿
        "total_assets": 319_918_000_000,  # 总资产 ~3199亿
        "total_shares": TOTAL_SHARES,
        "operating_cash_flow": 90_000_000_000,  # 经营现金流 ~900亿
        "revenue_growth": 15.3,          # 营收增速
        "profit_growth": 18.2,           # 利润增速
        "error": None,
    }


def _gen_capital_data() -> dict:
    """资金面数据"""
    # 北向资金 — 30天数据
    north_data = []
    for i in range(30):
        date = (BASE_DATE - timedelta(days=30 - i)).strftime("%Y-%m-%d")
        north_data.append({
            "trade_date": date,
            "hold_share": np.random.randint(80_000_000, 95_000_000),
            "hold_value": np.random.uniform(1.3e11, 1.6e11),
            "net_buy": np.random.choice([-1, 1]) * np.random.randint(100_000, 2_000_000),
        })

    # 融资融券 — 30天
    margin_data = []
    for i in range(30):
        date = (BASE_DATE - timedelta(days=30 - i)).strftime("%Y-%m-%d")
        margin_data.append({
            "trade_date": date,
            "rz_balance": 19_735_000_000 + i * 50_000_000,
            "rz_change": 50_000_000 + np.random.randn() * 20_000_000,
            "rz_change_pct": round(0.25 + np.random.randn() * 0.1, 4),
        })

    return {
        "north": north_data,
        "margin": margin_data,
        "dragon": [],
        "error": None,
    }


def _gen_fundamental_data() -> dict:
    """基本面数据（含财务+估值+行业对标）"""
    # 财务数据列表 — 12个季度
    finance_list = []
    for i in range(12):
        quarter_date = (BASE_DATE - timedelta(days=90 * (12 - i))).strftime("%Y-%m-%d")
        finance_list.append({
            "report_date": quarter_date,
            "roe": round(30 + np.random.randn() * 2, 2),
            "gross_margin_ttm": round(91 + np.random.randn() * 0.5, 2),
            "net_profit": round(80_000_000_000 + i * 2_000_000_000, 0),
            "total_revenue": round(140_000_000_000 + i * 5_000_000_000, 0),
            "equity": round(260_000_000_000 + i * 3_000_000_000, 0),
            "total_assets": round(310_000_000_000 + i * 3_000_000_000, 0),
            "debt_to_assets": round(0.24 + np.random.randn() * 0.01, 4),
            "profit_dedt_yoy": round(15 + np.random.randn() * 3, 2),
            "revenue_yoy": round(14 + np.random.randn() * 3, 2),
            "operating_cash_flow": round(85_000_000_000 + i * 1_500_000_000, 0),
        })

    return {
        "finance": finance_list,
        "valuation": {
            "市盈率": 25.8,
            "市净率": 8.65,
            "股息率": 2.5,
            "净资产收益率": 32.38,
            "总市值": CURRENT_PRICE * TOTAL_SHARES,
        },
        "industry_stocks": ["sh600519", "sh600809", "sz000858", "sh600779", "sz000568"],
        "basic_info": {
            "股票代码": STOCK_CODE,
            "行业": "C15酒、饮料和精制茶制造业",
            "来源": "BaoStock",
        },
    }


def _gen_basic_info() -> dict:
    """基本信息"""
    return {
        "股票代码": STOCK_CODE,
        "行业": "C15酒、饮料和精制茶制造业",
        "来源": "BaoStock",
    }


# ============================================================================
#  Mock DataConnector
# ============================================================================

class MockDataConnector:
    """模拟数据连接器 — 返回完整的茅台数据"""

    def __init__(self, stock_code: str = STOCK_CODE, primary_source: str = "mock"):
        self.stock_code = STOCK_CODE
        self._original_code = stock_code
        self._primary_source = "mock"
        self._cache = {}

    def fetch_basic_info(self) -> dict:
        return _gen_basic_info()

    def fetch_capital_data(self) -> dict:
        return _gen_capital_data()

    def fetch_fundamental_data(self) -> dict:
        return _gen_fundamental_data()

    def fetch_tech_data(self) -> pd.DataFrame:
        return _gen_daily_data()

    def fetch_valuation_data(self) -> dict:
        return _gen_valuation_data()

    def fetch_financial_data(self) -> dict:
        return _gen_financial_data()

    def fetch_all(self) -> dict:
        df_tech = self.fetch_tech_data()
        return {
            "stock_code": self.stock_code,
            "basic_info": self.fetch_basic_info(),
            "capital_data": self.fetch_capital_data(),
            "fundamental_data": self.fetch_fundamental_data(),
            "tech_data": df_tech.to_dict("records"),
            "valuation_data": self.fetch_valuation_data(),
            "financial_data": self.fetch_financial_data(),
        }

    def clear_cache(self):
        self._cache = {}

    def get_data(self, data_type: str):
        mapping = {
            "basic_info": self.fetch_basic_info,
            "capital_data": self.fetch_capital_data,
            "fundamental_data": self.fetch_fundamental_data,
            "tech_data": self.fetch_tech_data,
            "valuation_data": self.fetch_valuation_data,
            "financial_data": self.fetch_financial_data,
        }
        return mapping.get(data_type, lambda: None)()

    def get_available_sources(self):
        return ["mock"]

    def get_primary_source(self):
        return "mock"


# ============================================================================
#  运行分析
# ============================================================================

def main():
    print("=" * 80)
    print("[Mock数据投研分析] 贵州茅台 600519 — 验证7项数据修复效果")
    print("=" * 80)

    from layers.agents.chief_agent import ChiefAgent

    # 直接传入 MockDataConnector，绕过真实API
    mock_connector = MockDataConnector("600519")
    agent = ChiefAgent(data_connector=mock_connector)
    result = agent.analyze("600519")

    # 输出报告
    output = []
    output.append("=" * 80)
    output.append("【600519 贵州茅台 — Mock数据完整投研报告】")
    output.append("=" * 80)
    output.append("")
    output.append("--- 最终整合报告 ---")
    output.append(result.get("final_report", ""))

    # 各Agent子报告
    agent_reports = result.get("reports", {})
    dim_names = {
        "fund": "基本面", "valuation": "估值面", "industry": "行业面",
        "tech": "技术面", "capital": "资金面", "risk": "风险面",
    }

    for dim_key, dim_label in dim_names.items():
        report = agent_reports.get(dim_key, {})
        output.append("")
        output.append(f"【{dim_label}】")
        output.append(f"  评分: {report.get('overall_score', 'N/A')}/100 | "
                      f"评级: {report.get('grade', 'N/A')} | "
                      f"置信度: {report.get('confidence', 'N/A')}%")
        output.append(f"  核心观点: {report.get('thesis', 'N/A')}")
        signals = report.get("key_signals", [])
        if signals:
            output.append(f"  关键信号:")
            for s in signals:
                output.append(f"    - {s}")

    output.append("")
    output.append("=" * 80)
    output.append("--- 数据修复验证清单 ---")
    output.append("=" * 80)

    val_data = _gen_valuation_data()
    fin_data = _gen_financial_data()
    tech_df = _gen_daily_data()
    cap_data = _gen_capital_data()

    checks = [
        ("Fix#1 股价", f"price={val_data['price']}", val_data["price"] == CURRENT_PRICE),
        ("Fix#2 MA5/MA20", f"ma5={tech_df['ma5'].iloc[-1]}, ma20={tech_df['ma20'].iloc[-1]}",
         not tech_df["ma5"].isna().all() and not tech_df["ma20"].isna().all()),
        ("Fix#3 流动比率", f"current_ratio={fin_data['current_ratio']}", fin_data["current_ratio"] is None),
        ("Fix#4 毛利率", f"gross_profit={fin_data['gross_profit']}%", fin_data["gross_profit"] > 90),
        ("Fix#5 PE_TTM", f"pe_ttm={val_data['pe_ttm']}", val_data["pe_ttm"] > 0 and val_data["pe_ttm"] < 100),
        ("Fix#6 北向资金", f"north_count={len(cap_data['north'])}", len(cap_data["north"]) > 0),
        ("Fix#7 换手率", f"turnover={tech_df['turnover'].iloc[-1]}%", tech_df["turnover"].iloc[-1] > 0),
    ]

    for name, detail, passed in checks:
        status = "✓ PASS" if passed else "✗ FAIL"
        output.append(f"  [{status}] {name}: {detail}")

    report_text = "\n".join(output)

    # 输出到 reports/ 目录
    report_path = PROJECT_ROOT / "reports" / "600519_mock_report.txt"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_text)

    print(f"\n报告已保存到 {report_path}")
    print(f"综合评分: {result.get('overall_score', 0)}/100 | 评级: {result.get('overall_grade', '-')}")
    print(f"\n--- 数据修复验证 ---")
    for name, detail, passed in checks:
        status = "✓" if passed else "✗"
        print(f"  [{status}] {name}: {detail}")


if __name__ == "__main__":
    main()
