import sys
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from functools import wraps
from abc import ABC, abstractmethod
from enum import Enum

logger = logging.getLogger("DataConnector")

import pandas as pd

# 尝试导入各数据源
AKSHARE_AVAILABLE = False
TUSHARE_AVAILABLE = False

try:
    import akshare as ak
    AKSHARE_AVAILABLE = True
    logger.info("[DataConnector] akshare 已加载")
except ImportError:
    logger.warning("[DataConnector] akshare 未安装，相关功能不可用")

try:
    import tushare as ts
    TUSHARE_AVAILABLE = True
    logger.info("[DataConnector] tushare 已加载")
except ImportError:
    logger.warning("[DataConnector] tushare 未安装，相关功能不可用")

from config.env_config import env_config


DATA_FETCH_RETRY = 3
CAPITAL_DATA_DAYS = 30
FINANCE_DATA_YEARS = 3
INDUSTRY_STOCK_LIMIT = 10
TECH_DATA_START_DATE = "20230101"


class DataSourceType(Enum):
    """数据源类型"""
    AKSHARE = "akshare"
    TUSHARE = "tushare"
    CACHE = "cache"


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


class AkshareDataSource(DataSourceBase):
    """AkShare数据源实现"""

    def __init__(self):
        super().__init__("akshare")
        self.available = AKSHARE_AVAILABLE
        if self.available:
            logger.info("[DataSource] akshare 数据源已激活")

    def _safe_fetch(self, fetch_fn, default_value=None, error_msg="数据获取失败"):
        """安全获取数据，失败返回默认值"""
        try:
            return fetch_fn()
        except Exception as e:
            logger.warning(f"[Akshare] {error_msg}: {str(e)[:100]}")
            return default_value

    def fetch_basic_info(self, stock_code: str) -> Dict:
        if not self.available:
            return {}

        def _fetch():
            basic_df = ak.stock_individual_info_em(symbol=stock_code)
            basic_info = basic_df.set_index("item").to_dict()["value"]

            if "行业" not in basic_info:
                name_code_df = ak.stock_info_a_code_name()
                code_digit = stock_code[:6]
                stock_row = name_code_df[name_code_df['code'] == code_digit]
                if not stock_row.empty:
                    basic_info["行业"] = "未知-需确认接口"
            return basic_info

        return self._safe_fetch(_fetch, {}, f"基本信息获取失败({stock_code})")

    def fetch_capital_data(self, stock_code: str) -> Dict:
        if not self.available:
            return {"north": [], "main": [], "margin": [], "dragon": [], "error": "akshare未安装"}

        capital_data = {"north": [], "main": [], "margin": [], "dragon": [], "error": None}

        try:
            north_df = ak.stock_hsgt_hist_em(symbol="北向")
            if not north_df.empty:
                capital_data["north"] = north_df.tail(CAPITAL_DATA_DAYS).to_dict("records")
        except Exception as e:
            logger.warning(f"[Akshare] 北向资金获取失败: {str(e)[:100]}")

        try:
            main_df = ak.stock_main_fund_daily(symbol=stock_code, indicator="今日")
            if not main_df.empty:
                capital_data["main"] = main_df.tail(CAPITAL_DATA_DAYS).to_dict("records")
        except Exception as e:
            logger.warning(f"[Akshare] 主力资金获取失败: {str(e)[:100]}")

        try:
            margin_df = ak.stock_margin_trading_sse(symbol=stock_code)
            if not margin_df.empty:
                capital_data["margin"] = margin_df.tail(CAPITAL_DATA_DAYS).to_dict("records")
        except Exception as e:
            logger.warning(f"[Akshare] 融资融券获取失败: {str(e)[:100]}")

        try:
            dragon_df = ak.stock_lh_yyb_jg_gg_df(symbol=stock_code)
            if not dragon_df.empty:
                capital_data["dragon"] = dragon_df.tail(5).to_dict("records")
        except Exception as e:
            logger.warning(f"[Akshare] 龙虎榜获取失败: {str(e)[:100]}")

        return capital_data

    def fetch_fundamental_data(self, stock_code: str) -> Dict:
        if not self.available:
            return {"finance": [], "valuation": {}, "industry_stocks": [], "basic_info": {}}

        fundamental_data = {"finance": [], "valuation": {}, "industry_stocks": [], "basic_info": {}}

        try:
            basic_info = self.fetch_basic_info(stock_code)
            fundamental_data["basic_info"] = basic_info

            finance_df = ak.stock_financial_analysis_indicator(
                symbol=stock_code, indicator="盈利能力"
            )
            if not finance_df.empty:
                finance_df = finance_df.T
                finance_df.columns = finance_df.iloc[0]
                finance_df = finance_df[1:].tail(FINANCE_DATA_YEARS)
                fundamental_data["finance"] = finance_df.to_dict("records")

            valuation_raw = ak.stock_a_lg_indicator_en(symbol=stock_code)
            if not valuation_raw.empty:
                latest_val = valuation_raw.iloc[-1]
                fundamental_data["valuation"] = {
                    "市盈率": latest_val.get("市盈率", None),
                    "市净率": latest_val.get("市净率", None),
                    "股息率": latest_val.get("股息率", None),
                    "净资产收益率": latest_val.get("净资产收益率", None),
                    "总市值": latest_val.get("总市值", None)
                }

            industry = basic_info.get("行业", "未知")
            if industry not in ["未知", "未知-需确认接口"]:
                try:
                    industry_list = ak.stock_board_industry_name_em()
                    target_board = industry_list[industry_list['name'] == industry]
                    if not target_board.empty:
                        board_code = target_board.iloc[0]['code']
                        cons_df = ak.stock_board_industry_cons_em(symbol=board_code)
                        if not cons_df.empty:
                            fundamental_data["industry_stocks"] = cons_df["symbol"].tolist()[:INDUSTRY_STOCK_LIMIT]
                except:
                    fundamental_data["industry_stocks"] = []
        except Exception as e:
            logger.error(f"[Akshare] 基本面数据获取失败: {str(e)[:100]}")

        return fundamental_data

    def fetch_tech_data(self, stock_code: str) -> pd.DataFrame:
        if not self.available:
            return pd.DataFrame()

        try:
            tech_df = ak.stock_zh_a_hist(
                symbol=stock_code,
                period="daily",
                adjust="qfq",
                start_date=TECH_DATA_START_DATE
            )

            if tech_df.empty:
                raise ValueError(f"{stock_code}无有效技术面数据")

            tech_df.rename(columns={
                "日期": "date",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
                "成交额": "amount"
            }, inplace=True)

            if "pct_change" not in tech_df.columns:
                tech_df["pct_change"] = tech_df["close"].pct_change() * 100

            # 计算技术指标
            tech_df["ema12"] = tech_df["close"].ewm(span=12, adjust=False).mean()
            tech_df["ema26"] = tech_df["close"].ewm(span=26, adjust=False).mean()
            tech_df["macd"] = tech_df["ema12"] - tech_df["ema26"]
            tech_df["signal"] = tech_df["macd"].ewm(span=9, adjust=False).mean()
            tech_df["macd_hist"] = tech_df["macd"] - tech_df["signal"]

            delta = tech_df["close"].diff()
            gain = delta.where(delta > 0, 0)
            loss = -delta.where(delta < 0, 0)
            avg_gain6 = gain.ewm(span=6, adjust=False).mean()
            avg_loss6 = loss.ewm(span=6, adjust=False).mean()
            rs6 = avg_gain6 / avg_loss6
            tech_df["rsi6"] = 100 - (100 / (1 + rs6))

            low_min = tech_df["low"].rolling(window=9).min()
            high_max = tech_df["high"].rolling(window=9).max()
            tech_df["rsv"] = (tech_df["close"] - low_min) / (high_max - low_min) * 100
            tech_df["k"] = tech_df["rsv"].ewm(span=3, adjust=False).mean()
            tech_df["d"] = tech_df["k"].ewm(span=3, adjust=False).mean()
            tech_df["j"] = 3 * tech_df["k"] - 2 * tech_df["d"]

            tech_df["boll_mid"] = tech_df["close"].rolling(window=20).mean()
            tech_df["boll_std"] = tech_df["close"].rolling(window=20).std()
            tech_df["boll_upper"] = tech_df["boll_mid"] + 2 * tech_df["boll_std"]
            tech_df["boll_lower"] = tech_df["boll_mid"] - 2 * tech_df["boll_std"]

            tech_df["ma5"] = tech_df["close"].rolling(window=5).mean()
            tech_df["ma10"] = tech_df["close"].rolling(window=10).mean()
            tech_df["ma20"] = tech_df["close"].rolling(window=20).mean()
            tech_df["ma60"] = tech_df["close"].rolling(window=60).mean()

            tech_df["turnover"] = 0.0

            return tech_df
        except Exception as e:
            logger.error(f"[Akshare] 技术面数据获取失败: {str(e)[:100]}")
            return pd.DataFrame()

    def fetch_valuation_data(self, stock_code: str) -> Dict:
        if not self.available:
            return {"price": 0, "pe_ttm": 0, "pb": 0, "pe_history": [], "pb_history": [], "pe_10_avg": 0, "error": "akshare未安装"}

        valuation_data = {"price": 0, "pe_ttm": 0, "pb": 0, "pe_history": [], "pb_history": [], "pe_10_avg": 0, "error": None}

        try:
            spot = ak.stock_zh_a_spot_em(symbol=stock_code)
            valuation_data["price"] = round(float(spot.iloc[0]["最新价"]), 2)

            val = ak.stock_a_pe_pb(symbol=stock_code)
            valuation_data["pe_ttm"] = round(float(val["pe"].iloc[-1]), 2)
            valuation_data["pb"] = round(float(val["pb"].iloc[-1]), 2)
            valuation_data["pe_history"] = val["pe"].dropna().tail(120).tolist()
            valuation_data["pb_history"] = val["pb"].dropna().tail(120).tolist()
            valuation_data["pe_10_avg"] = round(
                sum(valuation_data["pe_history"]) / len(valuation_data["pe_history"]), 2
            ) if valuation_data["pe_history"] else 0
        except Exception as e:
            valuation_data["error"] = str(e)
            logger.error(f"[Akshare] 估值数据获取失败: {str(e)[:100]}")

        return valuation_data

    def fetch_financial_data(self, stock_code: str) -> Dict:
        if not self.available:
            return {"roe": 0, "gross_profit": 0, "net_profit": "数据获取异常", "error": "akshare未安装"}

        financial_data = {"roe": 0, "gross_profit": 0, "net_profit": "数据获取异常", "error": None}

        try:
            fina = ak.stock_financial_report_emu(symbol=stock_code)
            financial_data["roe"] = round(float(fina.iloc[-1]["净资产收益率(%)"]), 2)
            financial_data["gross_profit"] = round(float(fina.iloc[-1]["销售毛利率(%)"]), 2)
            financial_data["net_profit"] = fina.iloc[-1]["净利润同比增长率(%)"]
        except Exception as e:
            financial_data["error"] = str(e)
            logger.error(f"[Akshare] 财务数据获取失败: {str(e)[:100]}")

        return financial_data


class TushareDataSource(DataSourceBase):
    """Tushare数据源实现"""

    def __init__(self):
        super().__init__("tushare")
        self.token = env_config.TUSHARE_TOKEN
        self.ts_pro = None

        if TUSHARE_AVAILABLE and self.token:
            try:
                ts.set_token(self.token)
                self.ts_pro = ts.pro_api()
                self.available = True
                logger.info("[DataSource] tushare 数据源已激活")
            except Exception as e:
                logger.warning(f"[DataSource] tushare 初始化失败: {str(e)[:100]}")
        elif TUSHARE_AVAILABLE and not self.token:
            logger.warning("[DataSource] tushare 已安装但未配置TOKEN，请在.env中设置TUSHARE_TOKEN")

    def _safe_fetch(self, fetch_fn, default_value=None, error_msg="数据获取失败"):
        """安全获取数据，失败返回默认值"""
        try:
            return fetch_fn()
        except Exception as e:
            logger.warning(f"[Tushare] {error_msg}: {str(e)[:100]}")
            return default_value

    def fetch_basic_info(self, stock_code: str) -> Dict:
        if not self.available:
            return {}

        def _fetch():
            # 转换股票代码格式（tushare需要带后缀）
            ts_code = self._format_ts_code(stock_code)
            df = self.ts_pro.stock_basic(ts_code=ts_code, fields="ts_code,name,industry,area,list_date")
            if not df.empty:
                row = df.iloc[0]
                return {
                    "股票代码": row.get("ts_code", ""),
                    "股票简称": row.get("name", ""),
                    "行业": row.get("industry", "未知"),
                    "地域": row.get("area", ""),
                    "上市时间": row.get("list_date", "")
                }
            return {}

        return self._safe_fetch(_fetch, {}, f"基本信息获取失败({stock_code})")

    def fetch_capital_data(self, stock_code: str) -> Dict:
        if not self.available:
            return {"north": [], "main": [], "margin": [], "dragon": [], "error": "tushare未配置"}

        capital_data = {"north": [], "main": [], "margin": [], "dragon": [], "error": None}
        ts_code = self._format_ts_code(stock_code)

        # 获取沪深港通持股数据（北向资金）
        try:
            north_df = self.ts_pro.hk_hold(ts_code=ts_code, limit=CAPITAL_DATA_DAYS)
            if not north_df.empty:
                capital_data["north"] = north_df.to_dict("records")
        except Exception as e:
            logger.warning(f"[Tushare] 北向资金获取失败: {str(e)[:100]}")

        # 获取融资融券数据
        try:
            margin_df = self.ts_pro.margin_detail(ts_code=ts_code, limit=CAPITAL_DATA_DAYS)
            if not margin_df.empty:
                capital_data["margin"] = margin_df.to_dict("records")
        except Exception as e:
            logger.warning(f"[Tushare] 融资融券获取失败: {str(e)[:100]}")

        return capital_data

    def fetch_fundamental_data(self, stock_code: str) -> Dict:
        if not self.available:
            return {"finance": [], "valuation": {}, "industry_stocks": [], "basic_info": {}}

        fundamental_data = {"finance": [], "valuation": {}, "industry_stocks": [], "basic_info": {}}
        ts_code = self._format_ts_code(stock_code)

        try:
            basic_info = self.fetch_basic_info(stock_code)
            fundamental_data["basic_info"] = basic_info

            # 获取财务指标数据
            fina_df = self.ts_pro.fina_indicator(ts_code=ts_code, limit=FINANCE_DATA_YEARS * 4)
            if not fina_df.empty:
                fundamental_data["finance"] = fina_df.to_dict("records")

            # 获取每日指标（包含估值数据）
            daily_df = self.ts_pro.daily_basic(ts_code=ts_code, limit=1)
            if not daily_df.empty:
                latest = daily_df.iloc[0]
                fundamental_data["valuation"] = {
                    "市盈率": latest.get("pe", None),
                    "市净率": latest.get("pb", None),
                    "股息率": latest.get("dv_ratio", None),
                    "净资产收益率": latest.get("roe", None),
                    "总市值": latest.get("total_mv", None)
                }

            # 获取同行业股票
            industry = basic_info.get("行业", "")
            if industry:
                try:
                    industry_df = self.ts_pro.stock_basic(exchange='', list_status='L',
                                                         fields='ts_code,name,industry')
                    peers = industry_df[industry_df['industry'] == industry]
                    fundamental_data["industry_stocks"] = peers["ts_code"].tolist()[:INDUSTRY_STOCK_LIMIT]
                except:
                    fundamental_data["industry_stocks"] = []
        except Exception as e:
            logger.error(f"[Tushare] 基本面数据获取失败: {str(e)[:100]}")

        return fundamental_data

    def fetch_tech_data(self, stock_code: str) -> pd.DataFrame:
        if not self.available:
            return pd.DataFrame()

        try:
            ts_code = self._format_ts_code(stock_code)
            df = self.ts_pro.daily(ts_code=ts_code, start_date=TECH_DATA_START_DATE)

            if df.empty:
                raise ValueError(f"{stock_code}无有效技术面数据")

            # 重命名列以统一格式
            df.rename(columns={
                "trade_date": "date",
                "open": "open",
                "close": "close",
                "high": "high",
                "low": "low",
                "vol": "volume",
                "amount": "amount"
            }, inplace=True)

            # 计算涨跌幅
            df["pct_change"] = df["close"].pct_change() * 100

            # 计算技术指标（与akshare保持一致）
            df["ema12"] = df["close"].ewm(span=12, adjust=False).mean()
            df["ema26"] = df["close"].ewm(span=26, adjust=False).mean()
            df["macd"] = df["ema12"] - df["ema26"]
            df["signal"] = df["macd"].ewm(span=9, adjust=False).mean()
            df["macd_hist"] = df["macd"] - df["signal"]

            delta = df["close"].diff()
            gain = delta.where(delta > 0, 0)
            loss = -delta.where(delta < 0, 0)
            avg_gain6 = gain.ewm(span=6, adjust=False).mean()
            avg_loss6 = loss.ewm(span=6, adjust=False).mean()
            rs6 = avg_gain6 / avg_loss6
            df["rsi6"] = 100 - (100 / (1 + rs6))

            low_min = df["low"].rolling(window=9).min()
            high_max = df["high"].rolling(window=9).max()
            df["rsv"] = (df["close"] - low_min) / (high_max - low_min) * 100
            df["k"] = df["rsv"].ewm(span=3, adjust=False).mean()
            df["d"] = df["k"].ewm(span=3, adjust=False).mean()
            df["j"] = 3 * df["k"] - 2 * df["d"]

            df["boll_mid"] = df["close"].rolling(window=20).mean()
            df["boll_std"] = df["close"].rolling(window=20).std()
            df["boll_upper"] = df["boll_mid"] + 2 * df["boll_std"]
            df["boll_lower"] = df["boll_mid"] - 2 * df["boll_std"]

            df["ma5"] = df["close"].rolling(window=5).mean()
            df["ma10"] = df["close"].rolling(window=10).mean()
            df["ma20"] = df["close"].rolling(window=20).mean()
            df["ma60"] = df["close"].rolling(window=60).mean()

            df["turnover"] = 0.0

            return df
        except Exception as e:
            logger.error(f"[Tushare] 技术面数据获取失败: {str(e)[:100]}")
            return pd.DataFrame()

    def fetch_valuation_data(self, stock_code: str) -> Dict:
        if not self.available:
            return {"price": 0, "pe_ttm": 0, "pb": 0, "pe_history": [], "pb_history": [], "pe_10_avg": 0, "error": "tushare未配置"}

        valuation_data = {"price": 0, "pe_ttm": 0, "pb": 0, "pe_history": [], "pb_history": [], "pe_10_avg": 0, "error": None}
        ts_code = self._format_ts_code(stock_code)

        try:
            # 获取最新每日指标
            daily_df = self.ts_pro.daily_basic(ts_code=ts_code, limit=1)
            if not daily_df.empty:
                latest = daily_df.iloc[0]
                valuation_data["price"] = round(float(latest.get("close", 0)), 2)
                valuation_data["pe_ttm"] = round(float(latest.get("pe_ttm", 0)), 2)
                valuation_data["pb"] = round(float(latest.get("pb", 0)), 2)

            # 获取历史估值数据
            hist_df = self.ts_pro.daily_basic(ts_code=ts_code, start_date="20230101")
            if not hist_df.empty:
                valuation_data["pe_history"] = hist_df["pe_ttm"].dropna().tail(120).tolist()
                valuation_data["pb_history"] = hist_df["pb"].dropna().tail(120).tolist()
                if valuation_data["pe_history"]:
                    valuation_data["pe_10_avg"] = round(
                        sum(valuation_data["pe_history"]) / len(valuation_data["pe_history"]), 2
                    )
        except Exception as e:
            valuation_data["error"] = str(e)
            logger.error(f"[Tushare] 估值数据获取失败: {str(e)[:100]}")

        return valuation_data

    def fetch_financial_data(self, stock_code: str) -> Dict:
        if not self.available:
            return {"roe": 0, "gross_profit": 0, "net_profit": "数据获取异常", "error": "tushare未配置"}

        financial_data = {"roe": 0, "gross_profit": 0, "net_profit": "数据获取异常", "error": None}
        ts_code = self._format_ts_code(stock_code)

        try:
            fina_df = self.ts_pro.fina_indicator(ts_code=ts_code, limit=1)
            if not fina_df.empty:
                latest = fina_df.iloc[0]
                financial_data["roe"] = round(float(latest.get("roe", 0)), 2)
                financial_data["gross_profit"] = round(float(latest.get("grossprofit_margin", 0)), 2)
                financial_data["net_profit"] = latest.get("profit_dedt_yoy", "数据获取异常")
        except Exception as e:
            financial_data["error"] = str(e)
            logger.error(f"[Tushare] 财务数据获取失败: {str(e)[:100]}")

        return financial_data

    def _format_ts_code(self, stock_code: str) -> str:
        """转换股票代码为tushare格式（添加.SH/.SZ后缀）"""
        stock_code = stock_code.strip()
        if "." in stock_code:
            return stock_code
        # 沪市
        if stock_code.startswith(("600", "601", "603", "605", "688")):
            return f"{stock_code}.SH"
        # 深市
        elif stock_code.startswith(("000", "001", "002", "003", "300")):
            return f"{stock_code}.SZ"
        return stock_code


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
    Anthropic 标准 Connector 层 - 多数据源统一入口
    支持 akshare / tushare 多数据源切换与降级
    优先级：主数据源 > 备用数据源 > 缓存
    """

    def __init__(self, stock_code: str, primary_source: str = "auto"):
        self.stock_code = stock_code
        self._cache: Dict[str, Any] = {}
        self._sources: Dict[str, DataSourceBase] = {}
        self._primary_source: Optional[str] = None

        # 初始化数据源
        self._init_sources(primary_source)

        logger.info(f"[DataConnector] 初始化 | stock_code={stock_code} | 主数据源={self._primary_source}")

    def _init_sources(self, primary_source: str) -> None:
        """初始化所有可用数据源"""
        akshare_ds = AkshareDataSource()
        tushare_ds = TushareDataSource()

        if akshare_ds.available:
            self._sources["akshare"] = akshare_ds
        if tushare_ds.available:
            self._sources["tushare"] = tushare_ds

        if not self._sources:
            raise RuntimeError("没有可用的数据源！请至少安装 akshare 或配置 tushare")

        # 确定主数据源
        if primary_source == "auto":
            # 自动选择：优先akshare，其次tushare
            if "akshare" in self._sources:
                self._primary_source = "akshare"
            else:
                self._primary_source = list(self._sources.keys())[0]
        elif primary_source in self._sources:
            self._primary_source = primary_source
        else:
            available = list(self._sources.keys())
            logger.warning(f"指定的主数据源 {primary_source} 不可用，自动切换到 {available[0]}")
            self._primary_source = available[0]

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
        """
        带降级机制的数据获取
        1. 先尝试主数据源
        2. 失败则尝试其他数据源
        3. 所有数据源失败返回空结果
        """
        errors = []

        # 尝试主数据源
        primary = self._get_source()
        try:
            method = getattr(primary, fetch_method)
            result = method(self.stock_code, *args, **kwargs)
            # 检查结果是否有效
            if self._is_valid_result(result, fetch_method):
                logger.info(f"[DataConnector] {fetch_method} 从 {primary.name} 获取成功")
                return result
        except Exception as e:
            errors.append(f"{primary.name}: {str(e)[:100]}")

        # 尝试其他数据源
        for name, source in self._sources.items():
            if name == self._primary_source:
                continue
            try:
                method = getattr(source, fetch_method)
                result = method(self.stock_code, *args, **kwargs)
                if self._is_valid_result(result, fetch_method):
                    logger.info(f"[DataConnector] {fetch_method} 从 {source.name} 降级获取成功")
                    return result
            except Exception as e:
                errors.append(f"{source.name}: {str(e)[:100]}")

        # 所有数据源都失败
        logger.error(f"[DataConnector] {fetch_method} 所有数据源均失败: {'; '.join(errors)}")
        return self._get_empty_result(fetch_method)

    def _is_valid_result(self, result: Any, fetch_method: str) -> bool:
        """检查结果是否有效"""
        if result is None:
            return False
        if isinstance(result, pd.DataFrame) and result.empty:
            return False
        if isinstance(result, dict):
            # 检查是否有错误标记
            if result.get("error") and "未安装" not in str(result.get("error", "")):
                return False
            # 检查是否有实质数据
            if fetch_method == "fetch_basic_info" and not result:
                return False
            if fetch_method == "fetch_capital_data":
                has_data = any(result.get(k) for k in ["north", "main", "margin", "dragon"])
                return has_data
            if fetch_method == "fetch_fundamental_data":
                has_data = result.get("finance") or result.get("valuation")
                return has_data
        return True

    def _get_empty_result(self, fetch_method: str) -> Any:
        """获取空结果"""
        empty_results = {
            "fetch_basic_info": {},
            "fetch_capital_data": {"north": [], "main": [], "margin": [], "dragon": [], "error": "所有数据源均不可用"},
            "fetch_fundamental_data": {"finance": [], "valuation": {}, "industry_stocks": [], "basic_info": {}},
            "fetch_tech_data": pd.DataFrame(),
            "fetch_valuation_data": {"price": 0, "pe_ttm": 0, "pb": 0, "pe_history": [], "pb_history": [], "pe_10_avg": 0, "error": "所有数据源均不可用"},
            "fetch_financial_data": {"roe": 0, "gross_profit": 0, "net_profit": "数据获取异常", "error": "所有数据源均不可用"}
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
        """获取资金面数据（北向/主力/融资融券/龙虎榜）"""
        def _fetch():
            return self._fetch_with_fallback("fetch_capital_data")
        return self._get_cached("capital_data", _fetch)

    @retry_decorator()
    def fetch_fundamental_data(self) -> Dict:
        """获取基本面数据（财务+估值+行业对标）"""
        def _fetch():
            return self._fetch_with_fallback("fetch_fundamental_data")
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
        """获取财务数据"""
        def _fetch():
            return self._fetch_with_fallback("fetch_financial_data")
        return self._get_cached("financial_data", _fetch)

    def fetch_all(self) -> Dict:
        """获取所有维度数据"""
        logger.info(f"[DataConnector] 开始获取全量数据: {self.stock_code}")
        all_data = {
            "stock_code": self.stock_code,
            "basic_info": self.fetch_basic_info(),
            "capital_data": self.fetch_capital_data(),
            "fundamental_data": self.fetch_fundamental_data(),
            "tech_data": self.fetch_tech_data().to_dict("records"),
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
    return DataConnector(stock_code, primary_source)
