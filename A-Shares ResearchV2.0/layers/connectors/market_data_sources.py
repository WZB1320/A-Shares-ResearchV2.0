"""
港股 + 中概股（美股）多市场数据源

数据源策略：
  港股主力：AkShare（免费、国内优先）
  港股兜底：新浪财经 → 富途
  美股主力：Finnhub（免费、专业）
  美股兜底：Yahoo Finance

所有 DataSource 实现同一接口 DataSourceBase，
返回的数据结构与 A 股 LocalAPIDataSource 完全一致，
确保下游 6 个分析 Agent 零改动。
"""
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

import pandas as pd

from config.env_config import env_config

logger = logging.getLogger("MarketDS")

# ── 技术指标统一计算（所有数据源复用） ──────────────────────

TECH_INDICATOR_START_DATE = "2023-01-01"


def _compute_tech_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """所有数据源统一的技术指标计算，保证下游 Agent 数据格式一致"""
    df = df.copy()

    if "date" not in df.columns:
        return df

    # 标准化列名
    df.rename(columns={
        "trade_date": "date", "Date": "date",
        "Open": "open", "High": "high", "Low": "low",
        "Close": "close", "Volume": "volume",
        "Adj Close": "adj_close",
    }, inplace=True, errors="ignore")

    required_cols = ["date", "open", "high", "low", "close", "volume"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = 0.0

    if "amount" not in df.columns:
        df["amount"] = 0.0

    close = df["close"]
    if close.isna().all() or len(close) < 2:
        df["pct_change"] = 0.0
        return df

    # 涨跌幅
    if "pct_change" not in df.columns:
        df["pct_change"] = close.pct_change() * 100

    # 换手率
    if "turnover" not in df.columns:
        df["turnover"] = 0.0

    # MACD
    df["ema12"] = close.ewm(span=12, adjust=False).mean()
    df["ema26"] = close.ewm(span=26, adjust=False).mean()
    df["macd"] = df["ema12"] - df["ema26"]
    df["signal"] = df["macd"].ewm(span=9, adjust=False).mean()
    df["macd_hist"] = df["macd"] - df["signal"]

    # RSI(6)
    delta = close.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain6 = gain.ewm(span=6, adjust=False).mean()
    avg_loss6 = loss.ewm(span=6, adjust=False).mean()
    rs6 = avg_gain6 / avg_loss6.replace(0, float('nan'))
    df["rsi6"] = 100 - (100 / (1 + rs6))

    # KDJ
    if "low" in df.columns and "high" in df.columns:
        low_min = df["low"].rolling(window=9).min()
        high_max = df["high"].rolling(window=9).max()
        h_l = high_max - low_min
        h_l = h_l.replace(0, float('nan'))
        df["rsv"] = (close - low_min) / h_l * 100
        df["k"] = df["rsv"].ewm(span=3, adjust=False).mean()
        df["d"] = df["k"].ewm(span=3, adjust=False).mean()
        df["j"] = 3 * df["k"] - 2 * df["d"]

    # 布林带
    df["boll_mid"] = close.rolling(window=20).mean()
    df["boll_std"] = close.rolling(window=20).std()
    df["boll_upper"] = df["boll_mid"] + 2 * df["boll_std"]
    df["boll_lower"] = df["boll_mid"] - 2 * df["boll_std"]

    # 均线
    df["ma5"] = close.rolling(window=5).mean()
    df["ma10"] = close.rolling(window=10).mean()
    df["ma20"] = close.rolling(window=20).mean()
    df["ma60"] = close.rolling(window=60).mean()

    return df


# ── 数据源基类（与 data_connector.DataSourceBase 接口一致） ──

class MarketDataSourceBase(ABC):
    """多市场数据源抽象基类"""

    def __init__(self, name: str):
        self.name = name
        self.available = False

    @abstractmethod
    def fetch_basic_info(self, stock_code: str) -> Dict:
        pass

    @abstractmethod
    def fetch_capital_data(self, stock_code: str) -> Dict:
        pass

    @abstractmethod
    def fetch_fundamental_data(self, stock_code: str) -> Dict:
        pass

    @abstractmethod
    def fetch_tech_data(self, stock_code: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def fetch_valuation_data(self, stock_code: str) -> Dict:
        pass

    @abstractmethod
    def fetch_financial_data(self, stock_code: str) -> Dict:
        pass


# ====================================================================
#  AkShareDataSource — 港股 + 美股数据源（免费、无 API Key、国内优先）
# ====================================================================

class AkShareDataSource(MarketDataSourceBase):
    """AkShare 数据源 — 支持港股和美股"""

    # AkShare 美股交易所前缀映射
    US_EXCHANGE_PREFIXES = ["105", "106"]  # 105=NYSE, 106=NASDAQ

    def __init__(self):
        super().__init__("akshare")
        try:
            import akshare as ak  # noqa: F401
            self.available = True
            logger.info("[AkShare] 数据源已激活 (A股+港股+美股)")
        except ImportError:
            self.available = False
            logger.warning("[AkShare] akshare 未安装，数据源不可用")

    # ── 市场识别 ──

    @staticmethod
    def _is_us_stock(stock_code: str) -> bool:
        """判断是否为美股（纯字母 ticker）"""
        code = stock_code.strip().upper().replace("US.", "").replace("HK.", "")
        return code.isalpha() and not code.isdigit()

    @staticmethod
    def _is_ashare_stock(stock_code: str) -> bool:
        """判断是否为A股（sh/sz 前缀 或 6位纯数字）"""
        code = stock_code.strip().upper()
        if code.startswith(("SH", "SZ")):
            return True
        clean = code.replace(".", "")
        if clean.isdigit() and len(clean) == 6:
            return True
        return False

    @staticmethod
    def _clean_code(stock_code: str) -> str:
        """统一清理股票代码"""
        code = stock_code.strip().upper()
        code = code.replace("HK.", "").replace("US.", "")
        return code

    @staticmethod
    def _clean_hk_code(stock_code: str) -> str:
        """港股代码标准化: 00700 或 HK.00700 → 00700"""
        code = stock_code.strip().replace("HK.", "").replace("hk.", "")
        return code.zfill(5) if code.isdigit() else code

    @staticmethod
    def _clean_ashare_code(stock_code: str) -> str:
        """A股代码标准化: sh600519 或 600519 → 600519"""
        code = stock_code.strip().upper()
        code = code.replace("SH", "").replace("SZ", "").replace(".SH", "").replace(".SZ", "")
        return code.zfill(6) if code.isdigit() else code

    @staticmethod
    def _find_us_symbol(stock_code: str) -> Optional[str]:
        """查找美股在 AkShare 中的 symbol（如 105.BABA 或 106.BABA）"""
        import akshare as ak
        ticker = AkShareDataSource._clean_code(stock_code).upper()

        # 尝试从实时行情中查找
        try:
            spot = ak.stock_us_spot_em()
            row = spot[spot["代码"] == ticker]
            if not row.empty:
                code = str(row.iloc[0].get("代码", ticker))
                # 自动加上前缀
                for prefix in AkShareDataSource.US_EXCHANGE_PREFIXES:
                    return f"{prefix}.{ticker}"
        except Exception:
            pass

        # 依次尝试 NYSE 和 NASDAQ 前缀
        for prefix in AkShareDataSource.US_EXCHANGE_PREFIXES:
            return f"{prefix}.{ticker}"

        return ticker

    # ── 六个标准方法 ──

    def fetch_basic_info(self, stock_code: str) -> Dict:
        if not self.available:
            return {}

        if self._is_us_stock(stock_code):
            return self._fetch_basic_info_us(stock_code)
        if self._is_ashare_stock(stock_code):
            return self._fetch_basic_info_ashare(stock_code)
        return self._fetch_basic_info_hk(stock_code)

    def _fetch_basic_info_hk(self, stock_code: str) -> Dict:
        try:
            import akshare as ak
            code = self._clean_hk_code(stock_code)
            try:
                spot_df = ak.stock_hk_spot_em()
                row = spot_df[spot_df["代码"] == code]
                if row.empty:
                    code_z = code.zfill(5)
                    row = spot_df[spot_df["代码"] == code_z]
                if not row.empty:
                    r = row.iloc[0]
                    return {
                        "股票代码": f"HK.{code}", "名称": str(r.get("名称", "")),
                        "行业": "", "来源": "akshare",
                        "最新价": float(r.get("最新价", 0) or 0),
                        "涨跌幅": float(r.get("涨跌幅", 0) or 0),
                    }
            except Exception:
                pass
            return {"股票代码": f"HK.{code}", "行业": "", "来源": "akshare", "_no_data": True}
        except Exception as e:
            logger.warning(f"[AkShare] basic_info(HK) 获取失败({stock_code}): {e}")
            return {}

    def _fetch_basic_info_us(self, stock_code: str) -> Dict:
        try:
            import akshare as ak
            ticker = self._clean_code(stock_code).upper()
            try:
                spot = ak.stock_us_spot_em()
                row = spot[spot["代码"] == ticker]
                if not row.empty:
                    r = row.iloc[0]
                    return {
                        "股票代码": ticker, "名称": str(r.get("名称", ticker)),
                        "行业": "", "来源": "akshare",
                        "最新价": float(r.get("最新价", 0) or 0),
                        "涨跌幅": float(r.get("涨跌幅", 0) or 0),
                    }
            except Exception:
                pass
            return {"股票代码": ticker, "行业": "", "来源": "akshare", "_no_data": True}
        except Exception as e:
            logger.warning(f"[AkShare] basic_info(US) 获取失败({stock_code}): {e}")
            return {}

    def fetch_capital_data(self, stock_code: str) -> Dict:
        """资金面数据"""
        if not self.available:
            return {"north": [], "margin": [], "dragon": [], "_data_unavailable": True}
        if self._is_us_stock(stock_code):
            return {"north": [], "margin": [], "dragon": [], "note": "美股不提供资金流向数据"}
        if self._is_ashare_stock(stock_code):
            return self._fetch_capital_ashare(stock_code)

        try:
            import akshare as ak
            south = []
            for fn_name in ["stock_hsgt_south_net_flow_in_em", "stock_hsgt_hist_em"]:
                try:
                    fn = getattr(ak, fn_name, None)
                    if fn:
                        south_df = fn()
                        if not south_df.empty:
                            south = south_df.tail(30).to_dict("records")
                            break
                except Exception:
                    continue
            return {"north": [], "margin": [], "dragon": [], "south": south,
                    "note": "港股：南向资金替代北向资金"}
        except Exception as e:
            logger.warning(f"[AkShare] capital_data 获取失败: {e}")
            return {"north": [], "margin": [], "dragon": [], "south": [], "_data_unavailable": True}

    def _fetch_capital_ashare(self, stock_code: str) -> Dict:
        try:
            import akshare as ak
            north = []
            try:
                north_df = ak.stock_hsgt_north_net_flow_in_em(symbol="北上")
                if not north_df.empty:
                    north = north_df.tail(30).to_dict("records")
            except Exception:
                pass
            return {"north": north, "margin": [], "dragon": [], "note": "A股：北向资金"}
        except Exception as e:
            logger.warning(f"[AkShare] capital_data(A股) 获取失败: {e}")
            return {"north": [], "margin": [], "dragon": [], "_data_unavailable": True}

    def fetch_fundamental_data(self, stock_code: str) -> Dict:
        if not self.available:
            return {"finance": [], "valuation": {}, "industry_stocks": [], "basic_info": {}, "_data_unavailable": True}
        if self._is_us_stock(stock_code):
            return self._fetch_fundamental_us(stock_code)
        if self._is_ashare_stock(stock_code):
            return self._fetch_fundamental_ashare(stock_code)
        return self._fetch_fundamental_hk(stock_code)

    def _fetch_fundamental_hk(self, stock_code: str) -> Dict:
        try:
            import akshare as ak
            code = self._clean_hk_code(stock_code)
            basic_info = self._fetch_basic_info_hk(stock_code)

            finance = []
            try:
                fin_df = ak.stock_hk_financial_indicator_em(symbol=code)
                if not fin_df.empty:
                    finance = fin_df.to_dict("records")
            except Exception:
                pass

            valuation = {}
            if finance:
                latest = finance[0]
                valuation = {
                    "市盈率": float(latest.get("市盈率", 0) or 0),
                    "市净率": float(latest.get("市净率", 0) or 0),
                    "股息率": float(latest.get("股息率", 0) or 0),
                    "净资产收益率": float(latest.get("净资产收益率", 0) or 0),
                    "总市值": float(latest.get("总市值", 0) or 0),
                }

            return {"finance": finance, "valuation": valuation, "industry_stocks": [],
                    "basic_info": basic_info}
        except Exception as e:
            logger.warning(f"[AkShare] fundamental_data(HK) 获取失败({stock_code}): {e}")
            return {"finance": [], "valuation": {}, "industry_stocks": [], "basic_info": {}, "_data_unavailable": True}

    def _fetch_fundamental_us(self, stock_code: str) -> Dict:
        """美股基本面：从 spot 和 financial 接口获取"""
        try:
            import akshare as ak
            ticker = self._clean_code(stock_code).upper()
            basic_info = self._fetch_basic_info_us(stock_code)

            valuation = {}
            # 尝试获取美股财务指标
            try:
                fin_df = ak.stock_us_fundamental(stock=ticker, symbol="")
                # stock_us_fundamental 返回大量财务数据行
                if not fin_df.empty:
                    # 提取关键指标
                    pe_row = fin_df[fin_df.iloc[:, 0].str.contains("市盈率|PE", na=False)]
                    pb_row = fin_df[fin_df.iloc[:, 0].str.contains("市净率|PB", na=False)]
                    roe_row = fin_df[fin_df.iloc[:, 0].str.contains("净资产收益率|ROE", na=False)]
                    valuation["市盈率"] = float(pe_row.iloc[0, 1]) if not pe_row.empty else 0
                    valuation["市净率"] = float(pb_row.iloc[0, 1]) if not pb_row.empty else 0
                    valuation["净资产收益率"] = float(roe_row.iloc[0, 1]) if not roe_row.empty else 0
                    valuation["总市值"] = 0
                    valuation["股息率"] = 0
            except Exception:
                pass

            return {"finance": [], "valuation": valuation, "industry_stocks": [],
                    "basic_info": basic_info}
        except Exception as e:
            logger.warning(f"[AkShare] fundamental_data(US) 获取失败({stock_code}): {e}")
            return {"finance": [], "valuation": {}, "industry_stocks": [], "basic_info": basic_info, "_data_unavailable": True}

    # ── A 股数据获取（兜底用） ──

    def _fetch_basic_info_ashare(self, stock_code: str) -> Dict:
        try:
            import akshare as ak
            code = self._clean_ashare_code(stock_code)
            try:
                spot_df = ak.stock_zh_a_spot_em()
                row = spot_df[spot_df["代码"] == code]
                if not row.empty:
                    r = row.iloc[0]
                    return {
                        "股票代码": code, "名称": str(r.get("名称", "")),
                        "行业": str(r.get("所属行业", "")), "来源": "akshare",
                        "最新价": float(r.get("最新价", 0) or 0),
                        "涨跌幅": float(r.get("涨跌幅", 0) or 0),
                    }
            except Exception:
                pass
            return {"股票代码": code, "行业": "", "来源": "akshare", "_no_data": True}
        except Exception as e:
            logger.warning(f"[AkShare] basic_info(A股) 获取失败({stock_code}): {e}")
            return {}

    def _fetch_fundamental_ashare(self, stock_code: str) -> Dict:
        try:
            import akshare as ak
            import re
            code = self._clean_ashare_code(stock_code)
            basic_info = self._fetch_basic_info_ashare(stock_code)

            valuation = {}
            try:
                fin_df = ak.stock_financial_abstract_ths(symbol=code, indicator="按报告期")
                if not fin_df.empty:
                    latest = fin_df.iloc[-1]

                    def _pct(val) -> float:
                        s = str(val).replace("%", "").strip()
                        try:
                            return float(s) / 100.0
                        except (ValueError, TypeError):
                            return 0.0

                    valuation = {
                        "市盈率": 0,
                        "市净率": 0,
                        "净资产收益率": _pct(latest.get("净资产收益率", 0)),
                        "总市值": 0,
                        "营业总收入": AkShareDataSource._parse_ashare_value(latest.get("营业总收入", 0)),
                        "归属净利润": AkShareDataSource._parse_ashare_value(latest.get("净利润", 0)),
                        "销售毛利率": _pct(latest.get("销售毛利率", 0)),
                        "销售净利率": _pct(latest.get("销售净利率", 0)),
                        "资产负债率": _pct(latest.get("资产负债率", 0)),
                        "净利润同比增长率": _pct(latest.get("净利润同比增长率", 0)),
                        "营业总收入同比增长率": _pct(latest.get("营业总收入同比增长率", 0)),
                        "股东权益": AkShareDataSource._parse_ashare_value(latest.get("股东权益", 0)),
                        "经营活动现金流": AkShareDataSource._parse_ashare_value(latest.get("经营活动现金净流量", 0)),
                        "流动比率": _pct(latest.get("流动比率", 0)),
                    }
            except Exception:
                pass

            # 尝试从实时行情补 PE/PB
            if valuation.get("市盈率", 0) == 0:
                try:
                    spot_df = ak.stock_zh_a_spot_em()
                    row = spot_df[spot_df["代码"] == code]
                    if not row.empty:
                        r = row.iloc[0]
                        valuation["市盈率"] = float(r.get("市盈率-动态", r.get("市盈率", 0)) or 0)
                        valuation["市净率"] = float(r.get("市净率", 0) or 0)
                        valuation["总市值"] = float(r.get("总市值", 0) or 0)
                except Exception:
                    pass

            finance = []
            try:
                fin_df2 = ak.stock_financial_abstract_ths(symbol=code, indicator="按报告期")
                if not fin_df2.empty:
                    finance = fin_df2.to_dict("records")
            except Exception:
                pass

            return {
                "finance": finance,
                "valuation": valuation if valuation else {"_note": "AkShare未获取到估值数据"},
                "industry_stocks": [],
                "basic_info": basic_info,
            }
        except Exception as e:
            logger.warning(f"[AkShare] fundamental_data(A股) 获取失败({stock_code}): {e}")
            return {"finance": [], "valuation": {}, "industry_stocks": [], "basic_info": {}, "_data_unavailable": True}

    @staticmethod
    def _parse_ashare_value(raw) -> float:
        """解析A股数额字符串: '272.43亿' → 27243000000.0, '21.7600' → 21.76"""
        import re
        s = str(raw).strip()
        if not s or s in ("False", "None", "nan", ""):
            return 0.0
        try:
            return float(s)
        except ValueError:
            unit_map = {"亿": 1e8, "万": 1e4, "%": 1}
            for unit, mult in unit_map.items():
                if unit in s:
                    try:
                        return float(s.replace(unit, "").strip()) * mult
                    except ValueError:
                        return 0.0
            try:
                return float(re.sub(r"[^\d.\-]", "", s))
            except ValueError:
                return 0.0

    def _fetch_financial_ashare(self, stock_code: str) -> Dict:
        try:
            import akshare as ak
            import re
            code = self._clean_ashare_code(stock_code)

            roe = 0.0
            gross_margin = 0.0
            net_profit_margin = 0.0
            revenue = 0.0
            net_profit = 0.0
            debt_ratio = 0.0
            profit_growth = 0.0
            revenue_growth = 0.0

            try:
                fin_df = ak.stock_financial_abstract_ths(symbol=code, indicator="按报告期")
                if not fin_df.empty:
                    latest = fin_df.iloc[-1]

                    def _pct(val) -> float:
                        s = str(val).replace("%", "").strip()
                        try:
                            return float(s) / 100.0
                        except (ValueError, TypeError):
                            return 0.0

                    roe = _pct(latest.get("净资产收益率", 0))
                    gross_margin = _pct(latest.get("销售毛利率", 0))
                    net_profit_margin = _pct(latest.get("销售净利率", 0))
                    debt_ratio = _pct(latest.get("资产负债率", 0))
                    profit_growth = _pct(latest.get("净利润同比增长率", 0))
                    revenue_growth = _pct(latest.get("营业总收入同比增长率", 0))
                    revenue = AkShareDataSource._parse_ashare_value(latest.get("营业总收入", 0))
                    net_profit = AkShareDataSource._parse_ashare_value(latest.get("净利润", 0))
            except Exception:
                pass

            return {
                "roe": roe,
                "净资产收益率": roe,
                "gross_profit_margin": gross_margin,
                "毛利率": gross_margin,
                "net_profit_margin": net_profit_margin,
                "revenue": revenue,
                "net_profit": net_profit,
                "debt_to_asset": debt_ratio,
                "资产负债率": debt_ratio,
                "profit_growth": profit_growth,
                "revenue_growth": revenue_growth,
                "source": "akshare_ths",
            }
        except Exception as e:
            logger.warning(f"[AkShare] financial_data(A股) 获取失败({stock_code}): {e}")
            return {"roe": None, "gross_profit": None, "net_profit": None, "_data_unavailable": True}

    # ── K线/技术分析 ──

    def fetch_tech_data(self, stock_code: str) -> pd.DataFrame:
        if not self.available:
            return pd.DataFrame()
        if self._is_us_stock(stock_code):
            return self._fetch_tech_data_us(stock_code)
        if self._is_ashare_stock(stock_code):
            return self._fetch_tech_data_ashare(stock_code)
        return self._fetch_tech_data_hk(stock_code)

    def _fetch_tech_data_hk(self, stock_code: str) -> pd.DataFrame:
        try:
            import akshare as ak
            code = self._clean_hk_code(stock_code)
            end_date = datetime.now().strftime("%Y%m%d")

            df = ak.stock_hk_daily(symbol=code, adjust="qfq")
            if df.empty:
                logger.warning(f"[AkShare] 港股{code} 无日线数据")
                return pd.DataFrame()

            df.rename(columns={"日期": "date"}, inplace=True)
            df["date"] = pd.to_datetime(df["date"])
            df = df[df["date"] >= TECH_INDICATOR_START_DATE]
            return _compute_tech_indicators(df)
        except Exception as e:
            logger.warning(f"[AkShare] tech_data(HK) 获取失败({stock_code}): {e}")
            return pd.DataFrame()

    def _fetch_tech_data_us(self, stock_code: str) -> pd.DataFrame:
        try:
            import akshare as ak
            ticker = self._clean_code(stock_code).upper()
            start = (datetime.now() - timedelta(days=365 * 2)).strftime("%Y%m%d")
            end = datetime.now().strftime("%Y%m%d")

            # 尝试不同前缀
            for prefix in self.US_EXCHANGE_PREFIXES:
                try:
                    symbol = f"{prefix}.{ticker}"
                    df = ak.stock_us_hist(symbol=symbol, period="daily",
                                          start_date=start, end_date=end, adjust="qfq")
                    if not df.empty:
                        df.rename(columns={"日期": "date"}, inplace=True)
                        df["date"] = pd.to_datetime(df["date"])
                        df = df[df["date"] >= TECH_INDICATOR_START_DATE]
                        return _compute_tech_indicators(df)
                except Exception:
                    continue

            logger.warning(f"[AkShare] 美股{ticker} 无日线数据")
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"[AkShare] tech_data(US) 获取失败({stock_code}): {e}")
            return pd.DataFrame()

    def _fetch_tech_data_ashare(self, stock_code: str) -> pd.DataFrame:
        try:
            import akshare as ak
            code = self._clean_ashare_code(stock_code)
            start = (datetime.now() - timedelta(days=365 * 2)).strftime("%Y%m%d")
            end = datetime.now().strftime("%Y%m%d")

            df = ak.stock_zh_a_hist(symbol=code, period="daily",
                                    start_date=start, end_date=end, adjust="qfq")
            if df.empty:
                logger.warning(f"[AkShare] A股{code} 无日线数据")
                return pd.DataFrame()

            df.rename(columns={"日期": "date"}, inplace=True)
            df["date"] = pd.to_datetime(df["date"])
            df = df[df["date"] >= TECH_INDICATOR_START_DATE]
            return _compute_tech_indicators(df)
        except Exception as e:
            logger.warning(f"[AkShare] tech_data(A股) 获取失败({stock_code}): {e}")
            return pd.DataFrame()

    def fetch_valuation_data(self, stock_code: str) -> Dict:
        if not self.available:
            return {"price": None, "pe_ttm": None, "pb": None, "pe_history": [], "pb_history": [], "_data_unavailable": True}
        try:
            fund = self.fetch_fundamental_data(stock_code)
            val = fund.get("valuation", {})
            return {
                "price": val.get("总市值", 0),
                "pe_ttm": val.get("市盈率", 0),
                "pb": val.get("市净率", 0),
                "pe_history": [], "pb_history": [],
                "pe_10_avg": val.get("市盈率", 0),
            }
        except Exception as e:
            logger.warning(f"[AkShare] valuation_data 获取失败: {e}")
            return {"price": None, "pe_ttm": None, "pb": None, "pe_history": [], "pb_history": [], "_data_unavailable": True}

    def fetch_financial_data(self, stock_code: str) -> Dict:
        if not self.available:
            return {"roe": None, "gross_profit": None, "net_profit": None, "_data_unavailable": True}
        try:
            if self._is_ashare_stock(stock_code):
                return self._fetch_financial_ashare(stock_code)
            fund = self.fetch_fundamental_data(stock_code)
            val = fund.get("valuation", {})
            return {
                "roe": val.get("净资产收益率", 0),
                "gross_profit": 0,
                "net_profit": "从AkShare获取",
            }
        except Exception as e:
            logger.warning(f"[AkShare] financial_data 获取失败: {e}")
            return {"roe": None, "gross_profit": None, "net_profit": None, "_data_unavailable": True}


# ====================================================================
#  YahooDataSource — 美股兜底 + 港股补充（免费、稳定）
#  使用 requests 直接调 Yahoo Finance API v8/v10，绕过 yfinance
# ====================================================================

class YahooDataSource(MarketDataSourceBase):
    """Yahoo Finance 数据源（美股、港股通用）
    不依赖 yfinance，直接用 requests 调 Yahoo Finance API
    """

    YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    YAHOO_QUOTE_URL = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    def __init__(self):
        super().__init__("yahoo")
        import requests as _r
        self.available = True
        logger.info("[Yahoo] 数据源已激活 (requests 模式)")

    @staticmethod
    def _to_yahoo_symbol(stock_code: str) -> str:
        """转为 Yahoo Finance 符号
        - 美股: BABA → BABA (不变)
        - 港股: 00700 → 0700.HK, 09988 → 9988.HK
        """
        code = stock_code.strip().upper()
        code = code.replace("HK.", "").replace("US.", "")

        if code.isdigit() or (code.endswith(".HK")):
            # 港股
            code = code.replace(".HK", "")
            code = code.lstrip("0") or "0"
            return f"{code}.HK"
        return code

    def _chart_request(self, symbol: str, range_str: str = "2y") -> Optional[Dict]:
        """请求 K 线数据"""
        import requests
        url = self.YAHOO_CHART_URL.format(symbol=symbol)
        try:
            resp = requests.get(url, params={
                "range": range_str, "interval": "1d", "includePrePost": "false"
            }, headers=self.HEADERS, timeout=30)
            if resp.status_code != 200:
                logger.warning(f"[Yahoo] chart API HTTP {resp.status_code} for {symbol}")
                return None
            data = resp.json()
            result = data.get("chart", {}).get("result", [])
            return result[0] if result else None
        except Exception as e:
            logger.warning(f"[Yahoo] chart 请求失败({symbol}): {e}")
            return None

    def _quote_request(self, symbol: str, modules: List[str]) -> Optional[Dict]:
        """请求报价/基本面数据"""
        import requests
        url = self.YAHOO_QUOTE_URL.format(symbol=symbol)
        try:
            resp = requests.get(url, params={
                "modules": ",".join(modules)
            }, headers=self.HEADERS, timeout=30)
            if resp.status_code == 429:
                logger.warning(f"[Yahoo] 请求频率过高({symbol})，等待 5 秒重试...")
                import time
                time.sleep(5)
                resp = requests.get(url, params={"modules": ",".join(modules)}, headers=self.HEADERS, timeout=30)
            if resp.status_code != 200:
                logger.warning(f"[Yahoo] quote API HTTP {resp.status_code} for {symbol}")
                return None
            data = resp.json()
            result = data.get("quoteSummary", {}).get("result", [])
            return result[0] if result else None
        except Exception as e:
            logger.warning(f"[Yahoo] quote 请求失败({symbol}): {e}")
            return None

    def fetch_basic_info(self, stock_code: str) -> Dict:
        if not self.available:
            return {}
        try:
            symbol = self._to_yahoo_symbol(stock_code)
            quote = self._quote_request(symbol, ["assetProfile", "price"])
            if not quote:
                return {"股票代码": symbol, "来源": "yahoo", "_no_data": True}

            profile = quote.get("assetProfile", {}) or {}
            price_info = quote.get("price", {}) or {}

            return {
                "股票代码": symbol,
                "名称": str(profile.get("longName", profile.get("shortName", ""))),
                "行业": str(profile.get("industry", profile.get("sector", ""))),
                "来源": "yahoo",
                "最新价": float(price_info.get("regularMarketPrice", {}).get("raw", 0) or 0),
                "涨跌幅": float(price_info.get("regularMarketChangePercent", {}).get("raw", 0) or 0),
                "市值": float(price_info.get("marketCap", {}).get("raw", 0) or 0) if "marketCap" in price_info else 0,
            }
        except Exception as e:
            logger.warning(f"[Yahoo] basic_info 获取失败({stock_code}): {e}")
            return {}

    def fetch_capital_data(self, stock_code: str) -> Dict:
        return {"north": [], "margin": [], "dragon": [], "note": "Yahoo不提供资金流向数据"}

    def fetch_fundamental_data(self, stock_code: str) -> Dict:
        if not self.available:
            return {"finance": [], "valuation": {}, "industry_stocks": [], "basic_info": {}, "_data_unavailable": True}
        try:
            symbol = self._to_yahoo_symbol(stock_code)
            quote = self._quote_request(symbol, [
                "assetProfile", "defaultKeyStatistics", "financialData", "summaryDetail"
            ])
            if not quote:
                return {"finance": [], "valuation": {}, "industry_stocks": [],
                        "basic_info": self.fetch_basic_info(stock_code), "_data_unavailable": True}

            def _raw(d, key):
                """提取 raw 值"""
                v = d.get(key, {})
                return v.get("raw", 0) if isinstance(v, dict) else (v or 0)

            stats = quote.get("defaultKeyStatistics", {}) or {}
            fin_data = quote.get("financialData", {}) or {}
            summary = quote.get("summaryDetail", {}) or {}
            profile = quote.get("assetProfile", {}) or {}

            valuation = {
                "市盈率": float(_raw(summary, "trailingPE") or _raw(summary, "forwardPE") or 0),
                "市净率": float(_raw(summary, "priceToBook") or 0),
                "股息率": float(_raw(summary, "dividendYield") or 0) * 100,
                "净资产收益率": float(_raw(fin_data, "returnOnEquity") or 0) * 100,
                "总市值": float(_raw(stats, "enterpriseValue") or 0),
            }

            basic_info = {
                "股票代码": symbol,
                "名称": str(profile.get("longName", profile.get("shortName", ""))),
                "行业": str(profile.get("industry", "")),
                "来源": "yahoo",
            }

            return {
                "finance": [],
                "valuation": valuation,
                "industry_stocks": [],
                "basic_info": basic_info,
            }
        except Exception as e:
            logger.warning(f"[Yahoo] fundamental_data 获取失败({stock_code}): {e}")
            return {"finance": [], "valuation": {}, "industry_stocks": [],
                    "basic_info": self.fetch_basic_info(stock_code), "_data_unavailable": True}

    def fetch_tech_data(self, stock_code: str) -> pd.DataFrame:
        if not self.available:
            return pd.DataFrame()
        try:
            symbol = self._to_yahoo_symbol(stock_code)
            chart = self._chart_request(symbol, "2y")
            if not chart:
                return pd.DataFrame()

            timestamps = chart.get("timestamp", [])
            quotes = chart.get("indicators", {}).get("quote", [{}])[0]
            adjclose_list = chart.get("indicators", {}).get("adjclose", [{}])
            adjclose = adjclose_list[0].get("adjclose", []) if adjclose_list else []

            if not timestamps:
                return pd.DataFrame()

            df = pd.DataFrame({
                "date": pd.to_datetime(timestamps, unit="s"),
                "open": quotes.get("open", []),
                "high": quotes.get("high", []),
                "low": quotes.get("low", []),
                "close": adjclose if adjclose else quotes.get("close", []),
                "volume": quotes.get("volume", []),
            })

            df.dropna(subset=["close"], inplace=True)
            if df.empty:
                return pd.DataFrame()

            df = df[df["date"] >= TECH_INDICATOR_START_DATE]
            return _compute_tech_indicators(df)
        except Exception as e:
            logger.warning(f"[Yahoo] tech_data 获取失败({stock_code}): {e}")
            return pd.DataFrame()

    def fetch_valuation_data(self, stock_code: str) -> Dict:
        if not self.available:
            return {"price": None, "pe_ttm": None, "pb": None, "pe_history": [], "pb_history": [], "_data_unavailable": True}
        try:
            fund = self.fetch_fundamental_data(stock_code)
            val = fund.get("valuation", {})
            return {
                "price": val.get("总市值", 0),
                "pe_ttm": val.get("市盈率", 0),
                "pb": val.get("市净率", 0),
                "pe_history": [],
                "pb_history": [],
                "pe_10_avg": val.get("市盈率", 0),
            }
        except Exception as e:
            logger.warning(f"[Yahoo] valuation_data 获取失败({stock_code}): {e}")
            return {"price": None, "pe_ttm": None, "pb": None, "pe_history": [], "pb_history": [], "_data_unavailable": True}

    def fetch_financial_data(self, stock_code: str) -> Dict:
        if not self.available:
            return {"roe": None, "gross_profit": None, "net_profit": None, "_data_unavailable": True}
        try:
            symbol = self._to_yahoo_symbol(stock_code)
            quote = self._quote_request(symbol, ["financialData"])
            if not quote:
                return {"roe": None, "gross_profit": None, "net_profit": None, "_data_unavailable": True}

            def _raw(d, key):
                v = d.get(key, {})
                return v.get("raw", 0) if isinstance(v, dict) else (v or 0)

            fin_data = quote.get("financialData", {}) or {}
            return {
                "roe": float(_raw(fin_data, "returnOnEquity") or 0) * 100,
                "gross_profit": float(_raw(fin_data, "grossMargins") or 0) * 100,
                "net_profit": float(_raw(fin_data, "totalRevenue") or 0),
            }
        except Exception as e:
            logger.warning(f"[Yahoo] financial_data 获取失败({stock_code}): {e}")
            return {"roe": None, "gross_profit": None, "net_profit": None, "_data_unavailable": True}


# ====================================================================
#  FinnhubDataSource — 美股主力（专业级、需 API Key）
# ====================================================================

class FinnhubDataSource(MarketDataSourceBase):
    """Finnhub 美股数据源"""

    def __init__(self):
        super().__init__("finnhub")
        self._client = None
        api_key = env_config.FINNHUB_API_KEY
        if api_key:
            try:
                import finnhub  # noqa: F401
                self.available = True
                logger.info("[Finnhub] 数据源已激活")
            except ImportError:
                self.available = False
                logger.warning("[Finnhub] finnhub-python 未安装")
        else:
            self.available = False
            logger.info("[Finnhub] 未配置 FINNHUB_API_KEY，跳过")

    def _get_client(self):
        if self._client is None:
            import finnhub
            self._client = finnhub.Client(api_key=env_config.FINNHUB_API_KEY)
        return self._client

    def fetch_basic_info(self, stock_code: str) -> Dict:
        if not self.available:
            return {}
        try:
            client = self._get_client()
            profile = client.company_profile2(symbol=stock_code)
            return {
                "股票代码": stock_code,
                "名称": profile.get("name", ""),
                "行业": profile.get("finnhubIndustry", ""),
                "来源": "finnhub",
                "市值": profile.get("marketCapitalization", 0),
            }
        except Exception as e:
            logger.warning(f"[Finnhub] basic_info 获取失败({stock_code}): {e}")
            return {}

    def fetch_capital_data(self, stock_code: str) -> Dict:
        """Finnhub 免费版不提供资金流向"""
        return {"north": [], "margin": [], "dragon": [], "note": "Finnhub不提供资金流向数据"}

    def fetch_fundamental_data(self, stock_code: str) -> Dict:
        if not self.available:
            return {"finance": [], "valuation": {}, "industry_stocks": [], "basic_info": {}, "_data_unavailable": True}
        try:
            client = self._get_client()
            basic_info = self.fetch_basic_info(stock_code)

            # 估值指标
            metrics = client.company_basic_financials(stock_code, "all")
            metric = metrics.get("metric", {}) if metrics else {}

            valuation = {
                "市盈率": metric.get("peBasicExclExtraTTM", metric.get("peTTM", 0)) or 0,
                "市净率": metric.get("pbQuarterly", metric.get("pbAnnual", 0)) or 0,
                "股息率": metric.get("dividendYieldIndicatedAnnual", 0) or 0,
                "净资产收益率": (metric.get("roeTTM", metric.get("roeRfy", 0)) or 0) * 100,
                "总市值": basic_info.get("市值", 0),
            }

            return {
                "finance": [],
                "valuation": valuation,
                "industry_stocks": [],
                "basic_info": basic_info,
            }
        except Exception as e:
            logger.warning(f"[Finnhub] fundamental_data 获取失败({stock_code}): {e}")
            return {"finance": [], "valuation": {}, "industry_stocks": [], "basic_info": {}, "_data_unavailable": True}

    def fetch_tech_data(self, stock_code: str) -> pd.DataFrame:
        if not self.available:
            return pd.DataFrame()
        try:
            client = self._get_client()
            end = int(datetime.now().timestamp())
            start = int((datetime.now() - timedelta(days=365 * 2)).timestamp())

            res = client.stock_candles(stock_code, "D", start, end)
            if res.get("s") != "ok":
                return pd.DataFrame()

            df = pd.DataFrame({
                "date": pd.to_datetime(res["t"], unit="s"),
                "open": res["o"],
                "high": res["h"],
                "low": res["l"],
                "close": res["c"],
                "volume": res["v"],
            })
            df = df[df["date"] >= TECH_INDICATOR_START_DATE]
            return _compute_tech_indicators(df)
        except Exception as e:
            logger.warning(f"[Finnhub] tech_data 获取失败({stock_code}): {e}")
            return pd.DataFrame()

    def fetch_valuation_data(self, stock_code: str) -> Dict:
        if not self.available:
            return {"price": None, "pe_ttm": None, "pb": None, "pe_history": [], "pb_history": [], "_data_unavailable": True}
        try:
            client = self._get_client()
            quote = client.quote(stock_code)
            metrics = client.company_basic_financials(stock_code, "all")
            metric = metrics.get("metric", {}) if metrics else {}

            return {
                "price": quote.get("c", 0),
                "pe_ttm": metric.get("peBasicExclExtraTTM", metric.get("peTTM", 0)) or 0,
                "pb": metric.get("pbQuarterly", 0) or 0,
                "pe_history": [],
                "pb_history": [],
                "pe_10_avg": metric.get("peBasicExclExtraTTM", 0) or 0,
            }
        except Exception as e:
            logger.warning(f"[Finnhub] valuation_data 获取失败({stock_code}): {e}")
            return {"price": None, "pe_ttm": None, "pb": None, "pe_history": [], "pb_history": [], "_data_unavailable": True}

    def fetch_financial_data(self, stock_code: str) -> Dict:
        if not self.available:
            return {"roe": None, "gross_profit": None, "net_profit": None, "_data_unavailable": True}
        try:
            client = self._get_client()
            metrics = client.company_basic_financials(stock_code, "all")
            metric = metrics.get("metric", {}) if metrics else {}
            # Finnhub 返回的已是百分比值(如10.22=10.22%)，无需再*100
            return {
                "roe": round(float(metric.get("roeTTM", 0) or 0), 2),
                "gross_profit": round(float(metric.get("grossMarginTTM", 0) or 0), 2),
                "net_profit_margin": round(float(metric.get("netProfitMarginTTM", 0) or 0), 2),
                "net_profit": f"{round(float(metric.get('netProfitMarginTTM', 0) or 0), 2)}%",
            }
        except Exception as e:
            logger.warning(f"[Finnhub] financial_data 获取失败({stock_code}): {e}")
            return {"roe": None, "gross_profit": None, "net_profit": None, "_data_unavailable": True}


# ====================================================================
#  SinaDataSource — 港股兜底1（新浪财经，无需 API Key）
# ====================================================================

class SinaDataSource(MarketDataSourceBase):
    """新浪财经 + 腾讯财经数据源 — A股 + 港股 + 美股"""

    SINA_HEADERS = {"Referer": "https://finance.sina.com.cn"}
    TENCENT_HEADERS = {"User-Agent": "Mozilla/5.0"}

    def __init__(self):
        super().__init__("sina")
        # Sina 不需要额外依赖，始终激活；各方法内部自行处理网络失败
        self.available = True
        logger.info("[Sina] 数据源已激活 (A股+港股+美股)")

    # ── 市场识别 ──

    @staticmethod
    def _is_us_stock(stock_code: str) -> bool:
        code = stock_code.strip().upper().replace("HK.", "").replace("US.", "")
        code = code.replace("SH", "").replace("SZ", "")
        return code.isalpha() and not code.isdigit()

    @staticmethod
    def _is_ashare_stock(stock_code: str) -> bool:
        """判断是否为A股：sh/sz 前缀 或 6位纯数字"""
        code = stock_code.strip().lower()
        if code.startswith(("sh", "sz")):
            return True
        clean = code.replace("us.", "").replace("hk.", "")
        return clean.isdigit() and len(clean) == 6

    @staticmethod
    def _clean_hk_code(stock_code: str) -> str:
        code = stock_code.strip().replace("HK.", "").replace("hk.", "")
        return code.zfill(5)

    @staticmethod
    def _to_sina_us_symbol(stock_code: str) -> str:
        """转为新浪美股代码: BABA → gb_baba"""
        code = stock_code.strip().upper().replace("US.", "")
        return f"gb_{code.lower()}"

    @staticmethod
    def _to_sina_ashare_symbol(stock_code: str) -> str:
        """转为新浪A股代码: 600519 → sh600519, sz002843 → sz002843"""
        code = stock_code.strip().lower().replace(".sh", "").replace(".sz", "")
        if code.startswith(("sh", "sz")):
            return code
        return f"sh{code}" if code.startswith(("6", "9")) else f"sz{code}"

    @staticmethod
    def _to_eastmoney_secid(stock_code: str) -> str:
        """转为东方财富 secid: sh600519 → 1.600519, sz002843 → 0.002843"""
        code = stock_code.strip().lower().replace(".sh", "").replace(".sz", "")
        if code.startswith("sh"):
            return f"1.{code[2:]}"
        if code.startswith("sz"):
            return f"0.{code[2:]}"
        return f"1.{code}" if code.startswith(("6", "9")) else f"0.{code}"

    # ── A股实时行情 ──

    def _fetch_ashare_quote(self, stock_code: str) -> Optional[Dict]:
        """获取A股实时行情"""
        import requests
        symbol = self._to_sina_ashare_symbol(stock_code)
        url = f"https://hq.sinajs.cn/list={symbol}"
        try:
            resp = requests.get(url, headers=self.SINA_HEADERS, timeout=10)
            resp.encoding = "gbk"
            data = resp.text
            if '=""' in data or not data:
                return None
            parts = data.split('"')[1].split(",")
            if len(parts) < 10:
                return None
            # A股字段: [0]=名称,[1]=今开,[2]=昨收,[3]=当前价,[4]=最高,[5]=最低,
            # [8]=成交量(股),[9]=成交额,[30]=日期
            return {
                "name": parts[0],
                "open": float(parts[1]) if parts[1] else 0,
                "close_prev": float(parts[2]) if parts[2] else 0,
                "price": float(parts[3]) if parts[3] else 0,
                "high": float(parts[4]) if parts[4] else 0,
                "low": float(parts[5]) if parts[5] else 0,
                "volume": float(parts[8]) if len(parts) > 8 and parts[8] else 0,
                "amount": float(parts[9]) if len(parts) > 9 and parts[9] else 0,
                "date": parts[30] if len(parts) > 30 else "",
            }
        except Exception:
            return None

    # ── A股 K 线（东方财富 + Yahoo 备份） ──

    def _fetch_ashare_kline(self, stock_code: str) -> pd.DataFrame:
        """获取A股日线 K 线 — 东方财富(主) / Yahoo(备)"""
        import requests, time

        # ── 主源: 东方财富 ──
        secid = self._to_eastmoney_secid(stock_code)
        url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        for attempt in range(3):
            try:
                params = {
                    "secid": secid,
                    "fields1": "f1,f2,f3,f4,f5,f6",
                    "fields2": "f51,f52,f53,f54,f55,f56,f57",
                    "klt": "101",
                    "fqt": "1",
                    "end": "20500101",
                    "lmt": "500",
                }
                resp = requests.get(url, params=params, timeout=30)
                data = resp.json()
                klines = (data.get("data") or {}).get("klines") or []
                if not klines:
                    continue

                records = []
                for row in klines:
                    parsed = self._parse_eastmoney_kline(row)
                    if parsed:
                        records.append(parsed)

                df = pd.DataFrame(records)
                if df.empty:
                    continue
                df = df[df["date"] >= TECH_INDICATOR_START_DATE]
                logger.info(f"[Sina/EM] A股K线获取成功, rows={len(df)}")
                return _compute_tech_indicators(df)
            except Exception as e:
                if attempt < 2:
                    time.sleep(1.5 * (attempt + 1))
                    continue
                logger.warning(f"[Sina/EM] A股K线 {secid} 失败(重试{attempt+1}次): {e}")

        # ── 备源: Yahoo Finance (部分A股支持) ──
        try:
            code = stock_code.strip().lower().replace("sh", "").replace("sz", "")
            suffix = ".SS" if code.startswith(("6", "9")) else ".SZ"
            yahoo_symbol = f"{code}{suffix}"
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}"
            params = {"range": "2y", "interval": "1d"}
            headers = {"User-Agent": "Mozilla/5.0"}
            resp = requests.get(url, params=params, headers=headers, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                result = data["chart"]["result"][0]
                timestamps = result.get("timestamp", [])
                quote = result["indicators"]["quote"][0]
                records = []
                for i, ts in enumerate(timestamps):
                    records.append({
                        "date": pd.to_datetime(ts, unit="s"),
                        "open": float(quote["open"][i]) if quote.get("open") and quote["open"][i] is not None else 0,
                        "close": float(quote["close"][i]) if quote.get("close") and quote["close"][i] is not None else 0,
                        "high": float(quote["high"][i]) if quote.get("high") and quote["high"][i] is not None else 0,
                        "low": float(quote["low"][i]) if quote.get("low") and quote["low"][i] is not None else 0,
                        "volume": float(quote["volume"][i]) if quote.get("volume") and quote["volume"][i] is not None else 0,
                    })
                df = pd.DataFrame(records)
                if not df.empty:
                    df = df[df["date"] >= TECH_INDICATOR_START_DATE]
                    logger.info(f"[Sina/Yahoo] A股K线获取成功, rows={len(df)}")
                    return _compute_tech_indicators(df)
        except Exception as e:
            logger.warning(f"[Sina/Yahoo] A股K线失败: {e}")

        logger.warning(f"[Sina] A股K线无法获取: {stock_code}")
        return pd.DataFrame()

    # ── 港股实时行情 ──

    def _fetch_hk_quote(self, stock_code: str) -> Optional[Dict]:
        import requests
        code = self._clean_hk_code(stock_code)
        url = f"https://hq.sinajs.cn/list=hk{code}"
        try:
            resp = requests.get(url, headers=self.SINA_HEADERS, timeout=10)
            resp.encoding = "gbk"
            data = resp.text
            if "FAILED" in data or not data:
                return None
            parts = data.split('"')[1].split(",")
            if len(parts) < 5:
                return None
            return {
                "name": parts[0], "open": float(parts[1]) if parts[1] else 0,
                "close_prev": float(parts[2]) if parts[2] else 0,
                "high": float(parts[4]) if parts[4] else 0,
                "low": float(parts[5]) if parts[5] else 0,
                "price": float(parts[6]) if parts[6] else 0,
                "change": float(parts[7]) if parts[7] else 0,
                "pct_change": float(parts[8]) if parts[8] else 0,
            }
        except Exception:
            return None

    # ── 美股实时行情 ──

    def _fetch_us_quote(self, stock_code: str) -> Optional[Dict]:
        """获取美股实时行情: gb_baba"""
        import requests
        symbol = self._to_sina_us_symbol(stock_code)
        url = f"https://hq.sinajs.cn/list={symbol}"
        try:
            resp = requests.get(url, headers=self.SINA_HEADERS, timeout=10)
            resp.encoding = "gbk"
            data = resp.text
            if '=""' in data or not data:
                return None
            # 解析: var hq_str_gb_baba="阿里巴巴,129.4700,-0.41,2026-05-27 09:38:32,-0.5300,..."
            parts = data.split('"')[1].split(",")
            if len(parts) < 5:
                return None
            # Sina 美股字段: [0]=名称,[1]=最新价,[2]=涨跌幅,[3]=时间,[4]=涨跌额,
            # [5]=开盘,[6]=最高,[7]=最低,[8]=52周高,[9]=52周低,
            # [10]=成交量,[11]=均量,[12]=市值,[13]=每股收益,[14]=市盈率,
            # [17]=市净率,[19]=总股本,[26]=昨收
            return {
                "name": parts[0],
                "price": float(parts[1]) if parts[1] else 0,
                "pct_change": float(parts[2]) if parts[2] else 0,
                "time": parts[3] if len(parts) > 3 else "",
                "change": float(parts[4]) if len(parts) > 4 and parts[4] else 0,
                "open": float(parts[5]) if len(parts) > 5 and parts[5] else 0,
                "high": float(parts[6]) if len(parts) > 6 and parts[6] else 0,
                "low": float(parts[7]) if len(parts) > 7 and parts[7] else 0,
                "high_52": float(parts[8]) if len(parts) > 8 and parts[8] else 0,
                "low_52": float(parts[9]) if len(parts) > 9 and parts[9] else 0,
                "volume": float(parts[10]) if len(parts) > 10 and parts[10] else 0,
                "market_cap": float(parts[12]) if len(parts) > 12 and parts[12] else 0,
                "eps": float(parts[13]) if len(parts) > 13 and parts[13] else 0,
                "pe": float(parts[14]) if len(parts) > 14 and parts[14] else 0,
                "pb": float(parts[17]) if len(parts) > 17 and parts[17] else 0,
                "shares": float(parts[19]) if len(parts) > 19 and parts[19] else 0,
                "close_prev": float(parts[26]) if len(parts) > 26 and parts[26] else 0,
            }
        except Exception:
            return None

    # ── 美股 K 线（东方财富） ──

    @staticmethod
    def _parse_eastmoney_kline(kline_str: str) -> Optional[Dict]:
        """解析东方财富 K 线字符串: 日期,开盘,收盘,最高,最低,成交量,成交额,振幅,涨跌幅,涨跌额,换手率"""
        parts = kline_str.split(",")
        if len(parts) < 6:
            return None
        return {
            "date": pd.to_datetime(parts[0]),
            "open": float(parts[1]),
            "close": float(parts[2]),
            "high": float(parts[3]),
            "low": float(parts[4]),
            "volume": float(parts[5]),
        }

    def _fetch_us_kline(self, stock_code: str) -> pd.DataFrame:
        """获取美股日线 K 线 — Yahoo (主，稳定) / 东方财富 (备)"""
        import requests, time
        code = stock_code.strip().upper().replace("US.", "")

        # ── 主源: Yahoo Finance (直连 API，最稳定) ──
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{code}"
            params = {"range": "2y", "interval": "1d"}
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
            for attempt in range(3):
                try:
                    resp = requests.get(url, params=params, headers=headers, timeout=30)
                    if resp.status_code != 200:
                        if attempt < 2:
                            time.sleep(1.5 * (attempt + 1))
                            continue
                        break
                    data = resp.json()
                    result = data["chart"]["result"][0]
                    timestamps = result.get("timestamp", [])
                    quote = result["indicators"]["quote"][0]
                    if not timestamps:
                        break

                    records = []
                    for i, ts in enumerate(timestamps):
                        records.append({
                            "date": pd.to_datetime(ts, unit="s"),
                            "open": float(quote["open"][i]) if quote.get("open") and quote["open"][i] is not None else 0,
                            "close": float(quote["close"][i]) if quote.get("close") and quote["close"][i] is not None else 0,
                            "high": float(quote["high"][i]) if quote.get("high") and quote["high"][i] is not None else 0,
                            "low": float(quote["low"][i]) if quote.get("low") and quote["low"][i] is not None else 0,
                            "volume": float(quote["volume"][i]) if quote.get("volume") and quote["volume"][i] is not None else 0,
                        })

                    df = pd.DataFrame(records)
                    if not df.empty:
                        df = df[df["date"] >= TECH_INDICATOR_START_DATE]
                        logger.info(f"[Sina/Yahoo] 美股K线获取成功, rows={len(df)}")
                        return _compute_tech_indicators(df)
                    break
                except Exception:
                    if attempt < 2:
                        time.sleep(1.5 * (attempt + 1))
                        continue
        except Exception as e:
            logger.warning(f"[Sina/Yahoo] US K线失败: {e}")

        # ── 备源: 东方财富 (106=NYSE / 105=NASDAQ) ──
        url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        for secid_prefix in ["106", "105"]:
            for attempt in range(3):
                try:
                    params = {
                        "secid": f"{secid_prefix}.{code}",
                        "fields1": "f1,f2,f3,f4,f5,f6",
                        "fields2": "f51,f52,f53,f54,f55,f56,f57",
                        "klt": "101",
                        "fqt": "1",
                        "end": "20500101",
                        "lmt": "500",
                    }
                    resp = requests.get(url, params=params, timeout=30)
                    data = resp.json()
                    klines = (data.get("data") or {}).get("klines") or []
                    if not klines:
                        continue

                    records = []
                    for row in klines:
                        parsed = self._parse_eastmoney_kline(row)
                        if parsed:
                            records.append(parsed)

                    df = pd.DataFrame(records)
                    if df.empty:
                        continue
                    df = df[df["date"] >= TECH_INDICATOR_START_DATE]
                    return _compute_tech_indicators(df)
                except Exception as e:
                    if attempt < 2:
                        time.sleep(1.5 * (attempt + 1))
                        continue
                    logger.warning(f"[Sina/EM] US K线 {secid_prefix}.{code} 失败(重试{attempt+1}次): {e}")

        logger.warning(f"[Sina] US K线无法获取: {stock_code}")
        return pd.DataFrame()

    # ── 六方法（智能路由 A股/港股/美股） ──

    def fetch_basic_info(self, stock_code: str) -> Dict:
        if self._is_ashare_stock(stock_code):
            quote = self._fetch_ashare_quote(stock_code)
            if not quote:
                return {}
            pct = 0.0
            if quote.get("close_prev") and quote["close_prev"] > 0:
                pct = round((quote["price"] - quote["close_prev"]) / quote["close_prev"] * 100, 2)
            return {
                "股票代码": stock_code.strip().upper(),
                "名称": quote.get("name", ""),
                "行业": "", "来源": "sina",
                "最新价": quote.get("price", 0),
                "涨跌幅": pct,
            }
        if self._is_us_stock(stock_code):
            quote = self._fetch_us_quote(stock_code)
            if not quote:
                return {}
            return {
                "股票代码": stock_code.strip().upper(),
                "名称": quote.get("name", ""),
                "行业": "",
                "来源": "sina",
                "最新价": quote.get("price", 0),
                "涨跌幅": quote.get("pct_change", 0),
                "市盈率": quote.get("pe", 0),
                "市净率": quote.get("pb", 0),
                "市值": quote.get("market_cap", 0),
                "每股收益": quote.get("eps", 0),
                "总股本": quote.get("shares", 0),
                "52周高": quote.get("high_52", 0),
                "52周低": quote.get("low_52", 0),
            }
        # 港股
        quote = self._fetch_hk_quote(stock_code)
        if not quote:
            return {}
        return {
            "股票代码": f"HK.{self._clean_hk_code(stock_code)}",
            "名称": quote.get("name", ""),
            "行业": "", "来源": "sina",
            "最新价": quote.get("price", 0),
            "涨跌幅": quote.get("pct_change", 0),
        }

    def fetch_capital_data(self, stock_code: str) -> Dict:
        return {"north": [], "margin": [], "dragon": [], "note": "新浪不提供资金流向数据"}

    def fetch_fundamental_data(self, stock_code: str) -> Dict:
        if self._is_ashare_stock(stock_code):
            basic_info = self.fetch_basic_info(stock_code)
            valuation = {
                "市盈率": 0, "市净率": 0, "股息率": 0,
                "净资产收益率": 0, "总市值": 0,
            }
            return {"finance": [], "valuation": valuation, "industry_stocks": [],
                    "basic_info": basic_info,
                    "_data_unavailable": True, "_note": "A股财务数据请启动本地API获取"}
        if self._is_us_stock(stock_code):
            quote = self._fetch_us_quote(stock_code)
            basic_info = self.fetch_basic_info(stock_code)
            financial = self.fetch_financial_data(stock_code)  # 含 Finnhub 补充的 ROE/毛利率
            valuation = {}
            if quote:
                valuation = {
                    "市盈率": quote.get("pe", 0),
                    "市净率": quote.get("pb", 0),
                    "股息率": 0,
                    "净资产收益率": financial.get("roe", 0),
                    "总市值": quote.get("market_cap", 0),
                }
            # Finnhub 补充修正 PE/PB（Sina GB 行情接口 PB 数据不可靠）
            try:
                from layers.connectors.market_data_sources import FinnhubDataSource
                fh = FinnhubDataSource()
                if fh.available:
                    fh_fund = fh.fetch_fundamental_data(stock_code)
                    fh_val = fh_fund.get("valuation", {})
                    if fh_val and not fh_fund.get("_data_unavailable"):
                        fh_pe = fh_val.get("市盈率", 0)
                        fh_pb = fh_val.get("市净率", 0)
                        if fh_pb and fh_pb > 0:
                            valuation["市净率"] = fh_pb
                            logger.info(f"[Sina] 美股 PB 从 Finnhub 修正: {quote.get('pb', 0):.2f} → {fh_pb:.2f}")
                        if fh_pe and fh_pe > 0:
                            valuation["市盈率"] = fh_pe
                            logger.info(f"[Sina] 美股 PE 从 Finnhub 修正: {quote.get('pe', 0):.2f} → {fh_pe:.2f}")
            except Exception as e:
                logger.debug(f"[Sina] Finnhub 补充 PE/PB 跳过: {e}")
            # 注入财务指标到返回 dict 的顶层，供 fund_skill 直接读取
            result = {"finance": [], "valuation": valuation, "industry_stocks": [], "basic_info": basic_info}
            for k in ("roe", "gross_profit", "net_profit", "eps", "market_cap"):
                if k in financial:
                    result[k] = financial[k]
            # 兼容 fund_skill 的多种 key 查找方式
            result["净资产收益率"] = financial.get("roe", 0)
            result["毛利率"] = financial.get("gross_profit", 0)
            result["gross_profit_margin"] = financial.get("gross_profit", 0)
            result["net_profit_margin"] = financial.get("net_profit_margin", 0)
            return result
        return {"finance": [], "valuation": {}, "industry_stocks": [],
                "basic_info": self.fetch_basic_info(stock_code), "_data_unavailable": True}

    def fetch_tech_data(self, stock_code: str) -> pd.DataFrame:
        if self._is_ashare_stock(stock_code):
            return self._fetch_ashare_kline(stock_code)
        if self._is_us_stock(stock_code):
            return self._fetch_us_kline(stock_code)
        # 港股K线暂时不支持
        return pd.DataFrame()

    def fetch_valuation_data(self, stock_code: str) -> Dict:
        if self._is_ashare_stock(stock_code):
            basic_info = self.fetch_basic_info(stock_code)
            return {
                "price": basic_info.get("最新价", 0),
                "pe_ttm": 0, "pb": 0,
                "pe_history": [], "pb_history": [],
                "pe_10_avg": 0, "market_cap": 0,
                "_note": "A股估值数据请启动本地API获取",
            }
        if self._is_us_stock(stock_code):
            fund = self.fetch_fundamental_data(stock_code)
            val = fund.get("valuation", {})
            basic_info = self.fetch_basic_info(stock_code)
            return {
                "price": basic_info.get("最新价", 0),
                "pe_ttm": val.get("市盈率", 0),
                "pb": val.get("市净率", 0),
                "pe_history": [], "pb_history": [],
                "pe_10_avg": val.get("市盈率", 0),
                "market_cap": val.get("总市值", 0),
            }
        return {"price": None, "pe_ttm": None, "pb": None, "pe_history": [], "pb_history": [], "_data_unavailable": True}

    def fetch_financial_data(self, stock_code: str) -> Dict:
        if self._is_ashare_stock(stock_code):
            return {
                "roe": 0, "gross_profit": 0, "net_profit": "A股请启动本地API",
                "eps": 0, "market_cap": 0,
                "_data_unavailable": True,
            }
        if self._is_us_stock(stock_code):
            basic_info = self.fetch_basic_info(stock_code)
            result = {
                "roe": 0,
                "gross_profit": 0,
                "net_profit": "从新浪获取",
                "eps": basic_info.get("每股收益", 0),
                "market_cap": basic_info.get("市值", 0),
            }
            # 尝试 Finnhub 补充财务细项（ROE / 毛利率 / 净利润）
            try:
                from layers.connectors.market_data_sources import FinnhubDataSource
                fh = FinnhubDataSource()
                if fh.available:
                    fh_fin = fh.fetch_financial_data(stock_code)
                    if fh_fin and not fh_fin.get("_data_unavailable"):
                        if fh_fin.get("roe"):
                            result["roe"] = fh_fin["roe"]
                        if fh_fin.get("gross_profit"):
                            result["gross_profit"] = fh_fin["gross_profit"]
                        if fh_fin.get("net_profit"):
                            result["net_profit"] = f"Finnhub: {fh_fin['net_profit']}"
                        logger.info(f"[Sina] 美股财务数据从 Finnhub 补充成功")
            except Exception as e:
                logger.debug(f"[Sina] Finnhub 财务补充跳过: {e}")
            return result
        return {"roe": None, "gross_profit": None, "net_profit": None, "_data_unavailable": True}


# ====================================================================
#  FutuDataSource — 港股兜底2（富途 OpenAPI，需可选 API Key）
# ====================================================================

class FutuDataSource(MarketDataSourceBase):
    """富途 OpenAPI 数据源（兜底）"""

    def __init__(self):
        super().__init__("futu")
        self._quote_ctx = None
        api_key = env_config.FUTU_API_KEY
        if api_key:
            try:
                from futu import OpenQuoteContext  # noqa: F401
                self.available = True
                logger.info("[Futu] 数据源已激活")
            except ImportError:
                self.available = False
                logger.info("[Futu] futu-api 未安装，跳过")
        else:
            self.available = False
            logger.info("[Futu] 未配置 FUTU_API_KEY，跳过")

    @staticmethod
    def _clean_hk_code(stock_code: str) -> str:
        code = stock_code.strip().replace("HK.", "").replace("hk.", "")
        return code.zfill(5)

    def _try_connect(self):
        if self._quote_ctx is None and self.available:
            try:
                from futu import OpenQuoteContext
                self._quote_ctx = OpenQuoteContext(host="127.0.0.1", port=11111)
            except Exception:
                self.available = False

    def fetch_basic_info(self, stock_code: str) -> Dict:
        self._try_connect()
        if not self.available or self._quote_ctx is None:
            return {}
        try:
            code = f"HK.{self._clean_hk_code(stock_code)}"
            ret, data = self._quote_ctx.get_market_snapshot([code])
            if ret == 0 and not data.empty:
                r = data.iloc[0]
                return {
                    "股票代码": code,
                    "名称": str(r.get("stock_name", "")),
                    "行业": "",
                    "来源": "futu",
                    "最新价": float(r.get("last_price", 0) or 0),
                    "涨跌幅": float(r.get("change_rate", 0) or 0),
                }
            return {}
        except Exception as e:
            logger.warning(f"[Futu] basic_info 获取失败({stock_code}): {e}")
            return {}

    def fetch_capital_data(self, stock_code: str) -> Dict:
        return {"north": [], "margin": [], "dragon": [], "note": "富途不提供资金流向数据"}

    def fetch_fundamental_data(self, stock_code: str) -> Dict:
        return {"finance": [], "valuation": {}, "industry_stocks": [], "basic_info": self.fetch_basic_info(stock_code), "_data_unavailable": True}

    def fetch_tech_data(self, stock_code: str) -> pd.DataFrame:
        self._try_connect()
        if not self.available or self._quote_ctx is None:
            return pd.DataFrame()
        try:
            code = f"HK.{self._clean_hk_code(stock_code)}"
            ret, data = self._quote_ctx.request_history_kline(
                code, start=TECH_INDICATOR_START_DATE, ktype="K_DAY"
            )
            if ret != 0 or data.empty:
                return pd.DataFrame()

            df = data.rename(columns={
                "time_key": "date", "open": "open", "close": "close",
                "high": "high", "low": "low", "volume": "volume",
            })
            return _compute_tech_indicators(df)
        except Exception as e:
            logger.warning(f"[Futu] tech_data 获取失败({stock_code}): {e}")
            return pd.DataFrame()

    def fetch_valuation_data(self, stock_code: str) -> Dict:
        return {"price": None, "pe_ttm": None, "pb": None, "pe_history": [], "pb_history": [], "_data_unavailable": True}

    def fetch_financial_data(self, stock_code: str) -> Dict:
        return {"roe": None, "gross_profit": None, "net_profit": None, "_data_unavailable": True}


# ── 导出 ──────────────────────────────────────────────────

__all__ = [
    "MarketDataSourceBase",
    "AkShareDataSource",
    "YahooDataSource",
    "FinnhubDataSource",
    "SinaDataSource",
    "FutuDataSource",
    "_compute_tech_indicators",
]