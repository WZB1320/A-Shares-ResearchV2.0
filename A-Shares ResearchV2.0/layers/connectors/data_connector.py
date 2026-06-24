
import sys
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from functools import wraps
from abc import ABC, abstractmethod
from enum import Enum
from datetime import datetime, timedelta

logger = logging.getLogger("DataConnector")

import pandas as pd
import requests

from config.env_config import env_config

DATA_FETCH_RETRY = 3
CAPITAL_DATA_DAYS = 30
FINANCE_DATA_YEARS = 3
INDUSTRY_STOCK_LIMIT = 10
TECH_DATA_START_DATE = "2023-01-01"
API_BASE_URL = "http://127.0.0.1:8001"


def _standardize_stock_code(stock_code: str) -> str:
    """
    标准化股票代码格式，转换为数据API接受的格式
    - 纯数字代码: 002843 -> sz002843, 600000 -> sh600000
    - 已经带前缀的: sz002843 -> sz002843
    """
    stock_code = stock_code.strip()
    
    # 已经是标准格式
    if stock_code.startswith("sh") or stock_code.startswith("sz"):
        return stock_code
    
    # 去除可能的.SZ/.SH后缀
    if stock_code.endswith(".SZ") or stock_code.endswith(".sz"):
        return "sz" + stock_code[:-3]
    if stock_code.endswith(".SH") or stock_code.endswith(".sh"):
        return "sh" + stock_code[:-3]
    
    # 纯数字，根据开头判断
    if stock_code.startswith(("600", "601", "603", "605", "688", "689")):
        return "sh" + stock_code
    else:
        return "sz" + stock_code


def _classify_market(stock_code: str) -> str:
    """
    识别股票所属市场
      "ashare" — A股：sh/sz 开头，或 6 位纯数字
      "hk"     — 港股：5 位纯数字，或 HK. 前缀
      "us"     — 美股（中概股）：纯字母 ticker，或 US. 前缀
    """
    code = stock_code.strip().upper()

    if code.startswith(("HK.", "HK")):
        return "hk"
    if code.startswith(("US.", "US")):
        return "us"
    if code.startswith(("SH", "SZ")):
        return "ashare"

    clean = code.replace(".", "")
    if clean.isdigit():
        if len(clean) == 5:
            return "hk"
        return "ashare"  # 6位数字 → A股
    if clean.isalpha():
        return "us"

    return "ashare"  # fallback


class DataSourceType(Enum):
    """数据源类型"""
    LOCAL_API = "local_api"
    AKSHARE = "akshare"
    SINA = "sina"
    FUTU = "futu"
    FINNHUB = "finnhub"
    YAHOO = "yahoo"


class DataSourceBase(ABC):
    """数据源抽象基类"""

    def __init__(self, name: str):
        self.name = name
        self.available = False

    @abstractmethod
    def fetch_basic_info(self, stock_code: str) -> Dict:
        """获取股票基本信息"""
        pass

    @abstractmethod
    def fetch_capital_data(self, stock_code: str) -> Dict:
        """获取资金面数据"""
        pass

    @abstractmethod
    def fetch_fundamental_data(self, stock_code: str) -> Dict:
        """获取基本面数据"""
        pass

    @abstractmethod
    def fetch_tech_data(self, stock_code: str) -> pd.DataFrame:
        """获取技术面数据"""
        pass

    @abstractmethod
    def fetch_valuation_data(self, stock_code: str) -> Dict:
        """获取估值数据"""
        pass

    @abstractmethod
    def fetch_financial_data(self, stock_code: str) -> Dict:
        """获取财务数据"""
        pass


class LocalAPIDataSource(DataSourceBase):
    """本地API数据源实现"""

    def __init__(self):
        super().__init__("local_api")
        self.available = self._check_health()
        if self.available:
            logger.info("[DataSource] 本地API数据源已激活")

    def _check_health(self) -> bool:
        """检查API服务是否健康"""
        try:
            resp = requests.get(f"{API_BASE_URL}/api/health", timeout=5)
            if resp.status_code == 200:
                return True
            return False
        except Exception as e:
            logger.warning(f"[DataSource] 本地API服务不可用: {str(e)}")
            return False

    def _safe_fetch(self, url: str, params: Optional[Dict] = None, error_msg: str = "数据获取失败") -> Optional[Dict]:
        """安全获取数据，失败返回None"""
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            else:
                logger.warning(f"[LocalAPI] {error_msg}: HTTP {resp.status_code}")
                return None
        except Exception as e:
            logger.warning(f"[LocalAPI] {error_msg}: {str(e)[:100]}")
            return None

    def fetch_basic_info(self, stock_code: str) -> Dict:
        """获取股票基本信息"""
        if not self.available:
            return {}

        # 获取行业信息
        industry_data = self._safe_fetch(
            f"{API_BASE_URL}/api/industry/{stock_code}",
            error_msg=f"行业信息获取失败({stock_code})"
        )

        basic_info = {}
        if industry_data and industry_data.get("count", 0) > 0:
            item = industry_data["data"][0]
            basic_info["股票代码"] = stock_code
            basic_info["行业"] = item.get("industry_name", "未知")
            basic_info["来源"] = item.get("source", "")

        return basic_info

    def fetch_capital_data(self, stock_code: str) -> Dict:
        """获取资金面数据（北向/融资融券/龙虎榜）"""
        if not self.available:
            return {"north": [], "margin": [], "dragon": [], "error": "本地API不可用"}

        capital_data = {"north": [], "margin": [], "dragon": [], "error": None}

        # 计算日期范围
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

        # Fix #6: 北向资金 — 数据源可能失效(count=0)，标注"暂不可用"
        north_data = self._safe_fetch(
            f"{API_BASE_URL}/api/northbound/{stock_code}",
            params={"start_date": start_date, "end_date": end_date, "limit": CAPITAL_DATA_DAYS},
            error_msg=f"北向资金获取失败({stock_code})"
        )
        if north_data and north_data.get("data"):
            capital_data["north"] = north_data["data"]
        else:
            capital_data["north_unavailable"] = True  # 标注数据源暂不可用

        # 获取融资融券数据
        margin_data = self._safe_fetch(
            f"{API_BASE_URL}/api/margin/{stock_code}",
            params={"start_date": start_date, "end_date": end_date, "limit": CAPITAL_DATA_DAYS},
            error_msg=f"融资融券数据获取失败({stock_code})"
        )
        if margin_data and margin_data.get("data"):
            capital_data["margin"] = margin_data["data"]

        # 获取龙虎榜数据
        dragon_data = self._safe_fetch(
            f"{API_BASE_URL}/api/dragon/{stock_code}",
            params={"start_date": start_date, "end_date": end_date, "limit": 5},
            error_msg=f"龙虎榜数据获取失败({stock_code})"
        )
        if dragon_data and dragon_data.get("data"):
            capital_data["dragon"] = dragon_data["data"]

        return capital_data

    def fetch_fundamental_data(self, stock_code: str) -> Dict:
        """获取基本面数据（财务+估值+行业对标）"""
        if not self.available:
            return {"finance": [], "valuation": {}, "industry_stocks": [], "basic_info": {}}

        fundamental_data = {"finance": [], "valuation": {}, "industry_stocks": [], "basic_info": {}}

        try:
            # 获取基本信息
            basic_info = self.fetch_basic_info(stock_code)
            fundamental_data["basic_info"] = basic_info

            # 获取财务指标
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=365*FINANCE_DATA_YEARS)).strftime("%Y-%m-%d")
            finance_data = self._safe_fetch(
                f"{API_BASE_URL}/api/indicators/financial/{stock_code}",
                params={"start_date": start_date, "end_date": end_date},
                error_msg=f"财务指标获取失败({stock_code})"
            )
            if finance_data and finance_data.get("data"):
                fundamental_data["finance"] = finance_data["data"]

            # 获取估值指标
            valuation_data = self._safe_fetch(
                f"{API_BASE_URL}/api/indicators/valuation/{stock_code}",
                params={"start_date": start_date, "end_date": end_date, "limit": 250},
                error_msg=f"估值指标获取失败({stock_code})"
            )
            if valuation_data and valuation_data.get("data") and len(valuation_data["data"]) > 0:
                latest = valuation_data["data"][-1]
                fundamental_data["valuation"] = {
                    "市盈率": latest.get("pe_ttm"),
                    "市净率": latest.get("pb"),
                    "股息率": latest.get("ps"),
                    "净资产收益率": latest.get("roe"),
                    "总市值": latest.get("total_mv")
                }

            # 获取同行业股票
            industry = basic_info.get("行业", "")
            if industry:
                industry_peers = self._safe_fetch(
                    f"{API_BASE_URL}/api/industry/{industry}/stocks",
                    error_msg=f"行业股票获取失败({industry})"
                )
                if industry_peers and industry_peers.get("data"):
                    fundamental_data["industry_stocks"] = [
                        item.get("stock_code") for item in industry_peers["data"][:INDUSTRY_STOCK_LIMIT]
                    ]
        except Exception as e:
            logger.error(f"[LocalAPI] 基本面数据获取失败: {str(e)[:100]}")

        return fundamental_data

    def fetch_tech_data(self, stock_code: str) -> pd.DataFrame:
        """获取技术面数据（含衍生指标）"""
        if not self.available:
            return pd.DataFrame()

        try:
            end_date = datetime.now().strftime("%Y-%m-%d")

            # Fix #2: 优先从 /api/indicators/technical/ 获取技术指标
            tech_data = self._safe_fetch(
                f"{API_BASE_URL}/api/indicators/technical/{stock_code}",
                params={"start_date": TECH_DATA_START_DATE, "end_date": end_date},
                error_msg=f"技术指标获取失败({stock_code})"
            )

            # 获取日线数据（OHLCV基础数据）
            daily_data = self._safe_fetch(
                f"{API_BASE_URL}/api/daily/{stock_code}",
                params={"start_date": TECH_DATA_START_DATE, "end_date": end_date},
                error_msg=f"日线行情获取失败({stock_code})"
            )

            if not daily_data or not daily_data.get("data"):
                raise ValueError(f"{stock_code}无有效技术面数据")

            df = pd.DataFrame(daily_data["data"])

            # 重命名列以统一格式
            df.rename(columns={
                "trade_date": "date",
                "open": "open",
                "close": "close",
                "high": "high",
                "low": "low",
                "vol": "volume",
                "amount": "amount"
            }, inplace=True, errors="ignore")

            # 确保列存在
            required_cols = ["date", "open", "high", "low", "close", "volume", "amount"]
            for col in required_cols:
                if col not in df.columns:
                    df[col] = 0.0

            # Fix #2: 如果技术指标接口有数据，合并MA5/MA20等指标
            if tech_data and tech_data.get("data"):
                tech_df = pd.DataFrame(tech_data["data"])
                # 重命名技术指标列
                tech_col_map = {
                    "trade_date": "date",
                    "ma5": "ma5",
                    "ma10": "ma10",
                    "ma20": "ma20",
                    "ma60": "ma60",
                    "macd": "macd",
                    "macd_signal": "signal",
                    "macd_hist": "macd_hist",
                    "rsi": "rsi6",
                    "kdj_k": "k",
                    "kdj_d": "d",
                    "kdj_j": "j",
                    "boll_upper": "boll_upper",
                    "boll_mid": "boll_mid",
                    "boll_lower": "boll_lower",
                    "turnover_rate": "turnover",
                }
                tech_df.rename(columns=tech_col_map, inplace=True, errors="ignore")
                # 按日期合并
                if "date" in tech_df.columns:
                    tech_df["date"] = tech_df["date"].astype(str)
                    df["date"] = df["date"].astype(str)
                    df = df.merge(tech_df[["date"] + [c for c in tech_df.columns if c != "date"]],
                                  on="date", how="left", suffixes=("", "_tech"))
                    # 用技术指标接口的数据覆盖本地计算
                    for col in ["ma5", "ma10", "ma20", "ma60", "macd", "signal", "macd_hist",
                                "rsi6", "k", "d", "j", "boll_upper", "boll_mid", "boll_lower", "turnover"]:
                        if f"{col}_tech" in df.columns:
                            df[col] = df[f"{col}_tech"].combine_first(df.get(col))
                            df.drop(columns=[f"{col}_tech"], inplace=True, errors="ignore")

            # 计算涨跌幅
            if "pct_change" not in df.columns and "close" in df.columns:
                df["pct_change"] = df["close"].pct_change() * 100

            # 本地计算技术指标（仅填充API未提供的字段）
            if "close" in df.columns:
                # MACD — 仅在API未提供时本地计算
                if "macd" not in df.columns or df["macd"].isna().all():
                    df["ema12"] = df["close"].ewm(span=12, adjust=False).mean()
                    df["ema26"] = df["close"].ewm(span=26, adjust=False).mean()
                    df["macd"] = df["ema12"] - df["ema26"]
                    df["signal"] = df["macd"].ewm(span=9, adjust=False).mean()
                    df["macd_hist"] = df["macd"] - df["signal"]

                # RSI — 仅在API未提供时本地计算
                if "rsi6" not in df.columns or df["rsi6"].isna().all():
                    delta = df["close"].diff()
                    gain = delta.where(delta > 0, 0)
                    loss = -delta.where(delta < 0, 0)
                    avg_gain6 = gain.ewm(span=6, adjust=False).mean()
                    avg_loss6 = loss.ewm(span=6, adjust=False).mean()
                    rs6 = avg_gain6 / avg_loss6
                    df["rsi6"] = 100 - (100 / (1 + rs6))

                # KDJ — 仅在API未提供时本地计算
                if "k" not in df.columns or df["k"].isna().all():
                    if "low" in df.columns and "high" in df.columns:
                        low_min = df["low"].rolling(window=9).min()
                        high_max = df["high"].rolling(window=9).max()
                        df["rsv"] = (df["close"] - low_min) / (high_max - low_min) * 100
                        df["k"] = df["rsv"].ewm(span=3, adjust=False).mean()
                        df["d"] = df["k"].ewm(span=3, adjust=False).mean()
                        df["j"] = 3 * df["k"] - 2 * df["d"]

                # BOLL — 仅在API未提供时本地计算
                if "boll_mid" not in df.columns or df["boll_mid"].isna().all():
                    df["boll_mid"] = df["close"].rolling(window=20).mean()
                    df["boll_std"] = df["close"].rolling(window=20).std()
                    df["boll_upper"] = df["boll_mid"] + 2 * df["boll_std"]
                    df["boll_lower"] = df["boll_mid"] - 2 * df["boll_std"]

                # MA — 仅在API未提供时本地计算
                for window, col_name in [(5, "ma5"), (10, "ma10"), (20, "ma20"), (60, "ma60")]:
                    if col_name not in df.columns or df[col_name].isna().all():
                        df[col_name] = df["close"].rolling(window=window).mean()

            # Fix #7: 换手率 — API恒为null，从总股本自行计算
            if "turnover" not in df.columns or df["turnover"].isna().all() or (df["turnover"] == 0).all():
                cap_data = self._safe_fetch(
                    f"{API_BASE_URL}/api/capital/{stock_code}",
                    error_msg=f"总股本获取失败({stock_code})"
                )
                total_shares = None
                if cap_data and cap_data.get("data"):
                    cap_item = cap_data["data"][0] if isinstance(cap_data["data"], list) else cap_data["data"]
                    total_shares = cap_item.get("total_shares") or cap_item.get("total_share")

                if total_shares and total_shares > 0 and "volume" in df.columns:
                    df["turnover"] = (df["volume"] / total_shares * 100).round(4)
                else:
                    df["turnover"] = 0.0

            return df
        except Exception as e:
            logger.error(f"[LocalAPI] 技术面数据获取失败: {str(e)[:100]}")
            return pd.DataFrame()

    def fetch_valuation_data(self, stock_code: str) -> Dict:
        """获取估值数据（PE/PB/历史分位）"""
        if not self.available:
            return {"price": None, "pe_ttm": None, "pb": None, "pe_history": [], "pb_history": [], "pe_10_avg": None, "error": "本地API不可用"}

        valuation_data = {"price": None, "pe_ttm": None, "pb": None, "pe_history": [], "pb_history": [], "pe_10_avg": None, "error": None}

        try:
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=365*2)).strftime("%Y-%m-%d")

            val_data = self._safe_fetch(
                f"{API_BASE_URL}/api/indicators/valuation/{stock_code}",
                params={"start_date": start_date, "end_date": end_date, "limit": 500},
                error_msg=f"估值数据获取失败({stock_code})"
            )

            if val_data and val_data.get("data"):
                if len(val_data["data"]) > 0:
                    latest = val_data["data"][-1]
                    pe_ttm = latest.get("pe_ttm")
                    pb = latest.get("pb")
                    # Fix #5: PE为负表示公司亏损，标注"PE不适用"而非传0
                    valuation_data["pe_ttm"] = pe_ttm if pe_ttm is not None else None
                    valuation_data["pb"] = pb if pb is not None else None
                    # Fix #1: 股价从日线接口获取，估值接口可能不含close字段
                    valuation_data["price"] = latest.get("close")
                    if not valuation_data["price"]:
                        # 估值接口无close，从日线接口获取最新收盘价
                        daily = self._safe_fetch(
                            f"{API_BASE_URL}/api/daily/{stock_code}",
                            params={"start_date": (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d"),
                                    "end_date": end_date, "limit": 1},
                            error_msg=f"最新股价获取失败({stock_code})"
                        )
                        if daily and daily.get("data") and len(daily["data"]) > 0:
                            valuation_data["price"] = daily["data"][0].get("close")

                # 收集历史数据
                pe_list = []
                pb_list = []
                for item in val_data["data"]:
                    if item.get("pe_ttm"):
                        pe_list.append(item["pe_ttm"])
                    if item.get("pb"):
                        pb_list.append(item["pb"])

                valuation_data["pe_history"] = pe_list[-120:] if pe_list else []
                valuation_data["pb_history"] = pb_list[-120:] if pb_list else []

                if valuation_data["pe_history"]:
                    valuation_data["pe_10_avg"] = round(
                        sum(valuation_data["pe_history"]) / len(valuation_data["pe_history"]), 2
                    )
        except Exception as e:
            valuation_data["error"] = str(e)
            logger.error(f"[LocalAPI] 估值数据获取失败: {str(e)[:100]}")

        return valuation_data

    def fetch_financial_data(self, stock_code: str) -> Dict:
        """获取财务数据"""
        if not self.available:
            return {"roe": None, "gross_profit": None, "net_profit": None, "current_ratio": None, "quick_ratio": None, "error": "本地API不可用"}

        financial_data = {"roe": None, "gross_profit": None, "net_profit": None, "current_ratio": None, "quick_ratio": None, "error": None}

        try:
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=365*3)).strftime("%Y-%m-%d")

            fina_data = self._safe_fetch(
                f"{API_BASE_URL}/api/indicators/financial/{stock_code}",
                params={"start_date": start_date, "end_date": end_date},
                error_msg=f"财务数据获取失败({stock_code})"
            )

            if fina_data and fina_data.get("data") and len(fina_data["data"]) > 0:
                latest = fina_data["data"][-1]
                financial_data["roe"] = latest.get("roe")
                # Fix #4: 字段名带后缀，用 gross_margin_ttm 而非 grossprofit_margin
                financial_data["gross_profit"] = latest.get("gross_margin_ttm") or latest.get("grossprofit_margin")
                financial_data["net_profit"] = latest.get("profit_dedt_yoy")
                # Fix #3: 流动比率/速动比率 API不提供，标注"未提供"
                financial_data["current_ratio"] = None  # API不提供
                financial_data["quick_ratio"] = None    # API不提供
                financial_data["debt_to_asset"] = latest.get("debt_to_assets")
        except Exception as e:
            financial_data["error"] = str(e)
            logger.error(f"[LocalAPI] 财务数据获取失败: {str(e)[:100]}")

        return financial_data


def retry_decorator(max_retries: int = DATA_FETCH_RETRY, delay: float = 2.0):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"第{attempt+1}次采集失败（{func.__name__}），{delay}秒后重试：{str(e)[:100]}")
                        time.sleep(delay)
                        continue
                    else:
                        raise Exception(f"采集失败（{func.__name__}）：{str(e)}")
        return wrapper
    return decorator


class DataConnector:
    """
    Anthropic 标准 Connector 层 — 多市场数据源统一入口
    支持 A股（本地API）、港股（AkShare/新浪/富途）、美股中概股（Finnhub/Yahoo）
    """

    def __init__(self, stock_code: str, primary_source: str = "auto"):
        self._original_code = stock_code
        self._market = _classify_market(stock_code)
        self.stock_code = _standardize_stock_code(stock_code) if self._market == "ashare" else stock_code.strip()
        self._cache: Dict[str, Any] = {}
        self._sources: Dict[str, Any] = {}
        self._primary_source: Optional[str] = None

        # 初始化数据源
        self._init_sources(primary_source)

        logger.info(
            f"[DataConnector] 初始化 | 原始代码={stock_code} | 市场={self._market} | "
            f"标准代码={self.stock_code} | 主数据源={self._primary_source}"
        )

    def _init_sources(self, primary_source: str) -> None:
        """根据市场类型初始化数据源（优先级链）"""
        from layers.connectors.market_data_sources import (
            AkShareDataSource, YahooDataSource, FinnhubDataSource,
            SinaDataSource, FutuDataSource,
        )

        if self._market == "ashare":
            # === A 股：本地 API（主力） + 外部兜底（数据缺失时自动降级） ===
            local = LocalAPIDataSource()
            if local.available:
                self._sources["local_api"] = local
                self._primary_source = "local_api"
            else:
                logger.warning("[DataConnector] 本地API不可用，降级到外部数据源")

            # 始终注册外部兜底源（本地API返回空数据时自动切换）
            for name, src in [("akshare", AkShareDataSource()), ("sina", SinaDataSource())]:
                if src.available and name not in self._sources:
                    self._sources[name] = src
                    if self._primary_source is None:
                        self._primary_source = name

            if not self._sources:
                raise RuntimeError(
                    "没有可用的A股数据源！请确保本地API服务正在运行 http://127.0.0.1:8000，"
                    "或检查网络连接以使用新浪外部数据源"
                )

        elif self._market == "hk":
            # === 港股：AkShare → 新浪 → 富途 优先级 ===
            sources = [
                ("akshare", AkShareDataSource()),
                ("sina", SinaDataSource()),
                ("futu", FutuDataSource()),
            ]
            for name, src in sources:
                if src.available:
                    self._sources[name] = src
                    if self._primary_source is None:
                        self._primary_source = name

            if not self._sources:
                raise RuntimeError(
                    "没有可用的港股数据源！请安装 akshare: pip install akshare"
                )

        elif self._market == "us":
            # === 美股：Sina → AkShare → Finnhub → Yahoo 优先级 ===
            sources = [
                ("sina", SinaDataSource()),
                ("akshare", AkShareDataSource()),
                ("finnhub", FinnhubDataSource()),
                ("yahoo", YahooDataSource()),
            ]
            for name, src in sources:
                if src.available:
                    self._sources[name] = src
                    if self._primary_source is None:
                        self._primary_source = name

            if not self._sources:
                raise RuntimeError(
                    "没有可用的美股数据源！请检查网络连接或安装 akshare: pip install akshare"
                )

        # 如果指定了特定数据源，尝试切换
        if primary_source != "auto" and primary_source in self._sources:
            self._primary_source = primary_source

    def _get_source(self, source_name: Optional[str] = None) -> DataSourceBase:
        """获取指定数据源，默认返回主数据源"""
        if source_name and source_name in self._sources:
            return self._sources[source_name]
        return self._sources[self._primary_source]

    def _get_cached(self, key: str, fetch_fn: callable) -> Any:
        if key not in self._cache:
            self._cache[key] = fetch_fn()
        return self._cache[key]

    def _fetch_with_fallback(self, fetch_method: str, *args, **kwargs) -> Any:
        """多源降级获取：按优先级依次尝试，主源失败自动切换到备用源"""
        ordered_sources = [
            self._primary_source,
            *[s for s in self._sources if s != self._primary_source]
        ]

        last_error = None
        for src_name in ordered_sources:
            try:
                src = self._sources[src_name]
                method = getattr(src, fetch_method)
                result = method(self.stock_code, *args, **kwargs)
                logger.info(f"[DataConnector] {fetch_method} 从 {src_name} 获取成功")
                return result
            except Exception as e:
                last_error = e
                logger.warning(f"[DataConnector] {fetch_method} 从 {src_name} 失败: {str(e)[:80]}")

        if last_error:
            logger.error(f"[DataConnector] {fetch_method} 所有数据源均失败: {str(last_error)[:100]}")
        return self._get_empty_result(fetch_method)

    @staticmethod
    def _is_financial_data_empty(data: Dict) -> bool:
        """判断财务数据是否实质上为空（core_keys 全为 None）"""
        if not data:
            return True
        core_keys = [
            data.get("roe"),
            data.get("净资产收益率"),
            data.get("gross_profit"),
            data.get("gross_profit_margin"),
            data.get("毛利率"),
            data.get("net_profit"),
            data.get("net_profit_margin"),
        ]
        return all(v is None or v == 0 or v == "数据获取异常"
                   for v in core_keys)

    @staticmethod
    def _is_fundamental_data_empty(data: Dict) -> bool:
        """判断基本面数据是否实质上为空"""
        if not data:
            return True
        finance = data.get("finance", [])
        valuation = data.get("valuation", {})
        has_finance = isinstance(finance, list) and len(finance) > 0
        has_valuation = bool(valuation) and any(
            v for k, v in valuation.items()
            if k not in ("_data_unavailable", "_note") and v is not None and v != 0
        )
        if has_finance or has_valuation:
            return False
        return True

    def _fetch_with_quality_fallback(self, fetch_method: str, empty_checker: callable) -> Any:
        """质量感知降级：主源返回空数据时自动尝试备用源，不依赖异常抛出"""
        ordered_sources = [
            self._primary_source,
            *[s for s in self._sources if s != self._primary_source]
        ]

        last_error = None
        for src_name in ordered_sources:
            try:
                src = self._sources[src_name]
                method = getattr(src, fetch_method)
                result = method(self.stock_code)
                if empty_checker(result):
                    logger.warning(
                        f"[DataConnector] {fetch_method} 从 {src_name} 获取到空数据，尝试降级"
                    )
                    continue
                logger.info(f"[DataConnector] {fetch_method} 从 {src_name} 获取成功（质量通过）")
                return result
            except Exception as e:
                last_error = str(e)[:80]
                logger.warning(f"[DataConnector] {fetch_method} 从 {src_name} 异常: {last_error}")

        if last_error:
            logger.error(f"[DataConnector] {fetch_method} 所有数据源均失败或返回空数据: {last_error}")
        else:
            logger.warning(f"[DataConnector] {fetch_method} 所有数据源均返回空数据")
        return self._get_empty_result(fetch_method)

    def _get_empty_result(self, fetch_method: str) -> Any:
        """获取空结果 — 使用 None 标记不可用数据，下游可据此判断数据缺失"""
        empty_results = {
            "fetch_basic_info": {"_data_unavailable": True},
            "fetch_capital_data": {"north": [], "margin": [], "dragon": [], "_data_unavailable": True},
            "fetch_fundamental_data": {"finance": [], "valuation": {}, "industry_stocks": [], "basic_info": {}, "_data_unavailable": True},
            "fetch_tech_data": pd.DataFrame(),
            "fetch_valuation_data": {"price": None, "pe_ttm": None, "pb": None, "pe_history": [], "pb_history": [], "_data_unavailable": True},
            "fetch_financial_data": {"roe": None, "gross_profit": None, "net_profit": None, "_data_unavailable": True}
        }
        return empty_results.get(fetch_method, None)

    @retry_decorator()
    def fetch_basic_info(self) -> Dict:
        """获取股票基本信息"""
        def _fetch():
            return self._fetch_with_fallback("fetch_basic_info")
        return self._get_cached("basic_info", _fetch)

    @retry_decorator()
    def fetch_capital_data(self) -> Dict:
        """获取资金面数据（北向/融资融券/龙虎榜）"""
        def _fetch():
            return self._fetch_with_fallback("fetch_capital_data")
        return self._get_cached("capital_data", _fetch)

    @retry_decorator()
    def fetch_fundamental_data(self) -> Dict:
        """获取基本面数据（财务+估值+行业对标），本地API空数据时自动降级到外部源"""
        def _fetch():
            return self._fetch_with_quality_fallback(
                "fetch_fundamental_data", self._is_fundamental_data_empty
            )
        return self._get_cached("fundamental_data", _fetch)

    @retry_decorator()
    def fetch_tech_data(self) -> pd.DataFrame:
        """获取技术面数据（含衍生指标）"""
        def _fetch():
            return self._fetch_with_fallback("fetch_tech_data")
        result = self._get_cached("tech_data_df", _fetch)
        return result if isinstance(result, pd.DataFrame) else pd.DataFrame()

    @retry_decorator()
    def fetch_valuation_data(self) -> Dict:
        """获取估值数据（PE/PB/历史分位）"""
        def _fetch():
            return self._fetch_with_fallback("fetch_valuation_data")
        return self._get_cached("valuation_data", _fetch)

    @retry_decorator()
    def fetch_financial_data(self) -> Dict:
        """获取财务数据，本地API空数据时自动降级到外部源"""
        def _fetch():
            return self._fetch_with_quality_fallback(
                "fetch_financial_data", self._is_financial_data_empty
            )
        return self._get_cached("financial_data", _fetch)

    def fetch_all(self) -> Dict:
        """获取所有维度数据"""
        logger.info(f"[DataConnector] 开始获取全量数据: {self.stock_code}")
        df_tech = self.fetch_tech_data()
        
        # 确保 tech_data 按日期升序排列（最新日期在最后）
        if not df_tech.empty and "date" in df_tech.columns:
            df_tech = df_tech.sort_values("date").reset_index(drop=True)
        
        all_data = {
            "stock_code": self.stock_code,
            "basic_info": self.fetch_basic_info(),
            "capital_data": self.fetch_capital_data(),
            "fundamental_data": self.fetch_fundamental_data(),
            "tech_data": df_tech.to_dict("records"),
            "valuation_data": self.fetch_valuation_data(),
            "financial_data": self.fetch_financial_data()
        }
        logger.info(f"[DataConnector] 全量数据获取完成: {self.stock_code}")
        return all_data

    def clear_cache(self) -> None:
        """清除缓存"""
        self._cache = {}
        logger.info(f"[DataConnector] 缓存已清除: {self.stock_code}")

    def get_data(self, data_type: str) -> Any:
        """按类型获取数据（便捷接口）"""
        method_map = {
            "basic_info": self.fetch_basic_info,
            "capital_data": self.fetch_capital_data,
            "fundamental_data": self.fetch_fundamental_data,
            "tech_data": self.fetch_tech_data,
            "valuation_data": self.fetch_valuation_data,
            "financial_data": self.fetch_financial_data
        }

        if data_type not in method_map:
            raise ValueError(f"未知数据类型: {data_type}")

        return method_map[data_type]()

    def get_available_sources(self) -> List[str]:
        """获取所有可用的数据源名称"""
        return list(self._sources.keys())

    def get_primary_source(self) -> str:
        """获取当前主数据源名称"""
        return self._primary_source

    def switch_source(self, source_name: str) -> bool:
        """切换主数据源"""
        if source_name in self._sources:
            self._primary_source = source_name
            self.clear_cache()
            logger.info(f"[DataConnector] 已切换主数据源为: {source_name}")
            return True
        logger.warning(f"[DataConnector] 无法切换到 {source_name}，数据源不可用")
        return False


def get_data_connector(stock_code: str, primary_source: str = "auto") -> DataConnector:
    """获取 DataConnector 实例"""
    standardized_code = _standardize_stock_code(stock_code)
    return DataConnector(standardized_code, primary_source)
