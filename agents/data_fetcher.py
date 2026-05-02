import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from functools import wraps

# 统一路径处理
sys.path.append(str(Path(__file__).parent.parent))

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="[%(name)s] %(asctime)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("数据获取Agent")

from config.env_config import env_config
import akshare as ak
import pandas as pd

# 提取常量（统一管理）
DATA_FETCH_RETRY = 3  # 数据采集重试次数
CAPITAL_DATA_DAYS = 30  # 资金数据采集天数
FINANCE_DATA_YEARS = 3  # 财务数据采集年数
INDUSTRY_STOCK_LIMIT = 10  # 同行业股票数量上限
TECH_DATA_START_DATE = "20230101"  # 技术面数据起始日期

def retry_decorator(max_retries: int = DATA_FETCH_RETRY):
    """数据采集重试装饰器（统一重试逻辑）"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"第{attempt+1}次采集失败（{func.__name__}），重试中：{str(e)}")
                        continue
                    else:
                        raise Exception(f"采集失败（{func.__name__}）：{str(e)}")
        return wrapper
    return decorator

class DataFetcherAgent:
    """统一数据获取Agent - 为所有分析Agent提供标准化数据源"""
    
    def __init__(self, stock_code: str):
        self.stock_code = stock_code
        self._cached_data: Dict = {}  # 数据缓存（避免重复采集）

    @retry_decorator()
    def get_basic_info(self) -> Dict:
        """获取股票基本信息（缓存+重试）"""
        if "basic_info" in self._cached_data:
            return self._cached_data["basic_info"]
        
        logger.info(f"采集{self.stock_code}基本信息")
        basic_df = ak.stock_individual_info_em(symbol=self.stock_code)
        basic_info = basic_df.set_index("item").to_dict()["value"]
        self._cached_data["basic_info"] = basic_info
        return basic_info

    @retry_decorator()
    def get_capital_data(self) -> Dict:
        """获取资金面数据（北向/主力/融资融券/龙虎榜）"""
        if "capital_data" in self._cached_data:
            return self._cached_data["capital_data"]
        
        logger.info(f"采集{self.stock_code}近{CAPITAL_DATA_DAYS}天资金数据")
        capital_data = {
            "north": [],
            "main": [],
            "margin": [],
            "dragon": [],
            "error": None
        }
        
        try:
            # 北向资金
            north_df = ak.stock_hsgt_hist_em(symbol=self.stock_code).tail(CAPITAL_DATA_DAYS)
            capital_data["north"] = north_df.to_dict("records")
            
            # 主力资金
            main_df = ak.stock_zh_a_main_hist_em(symbol=self.stock_code).tail(CAPITAL_DATA_DAYS)
            capital_data["main"] = main_df.to_dict("records")
            
            # 融资融券
            margin_df = ak.stock_margin_trade_hist_em(symbol=self.stock_code).tail(CAPITAL_DATA_DAYS)
            capital_data["margin"] = margin_df.to_dict("records")
            
            # 龙虎榜
            dragon_df = ak.stock_dragon_and_tiger_list_em(symbol=self.stock_code)
            if not dragon_df.empty:
                capital_data["dragon"] = dragon_df.to_dict("records")
                
            self._cached_data["capital_data"] = capital_data
            logger.info(f"{self.stock_code}资金数据采集完成")
            return capital_data
        
        except Exception as e:
            error_msg = f"资金数据采集失败：{str(e)}"
            logger.error(error_msg)
            capital_data["error"] = error_msg
            self._cached_data["capital_data"] = capital_data
            return capital_data

    @retry_decorator()
    def get_fundamental_data(self) -> Dict:
        """获取基本面数据（财务+估值+行业对标）"""
        if "fundamental_data" in self._cached_data:
            return self._cached_data["fundamental_data"]
        
        logger.info(f"采集{self.stock_code}基本面数据（近{FINANCE_DATA_YEARS}年）")
        fundamental_data = {
            "finance": [],
            "valuation": {},
            "industry_stocks": [],
            "basic_info": self.get_basic_info()  # 复用基本信息
        }
        
        # 财务指标（近N年）
        finance_df = ak.stock_financial_analysis_indicator(symbol=self.stock_code)
        if not finance_df.empty:
            fundamental_data["finance"] = finance_df.tail(FINANCE_DATA_YEARS).to_dict("records")
        
        # 估值指标
        valuation_df = ak.stock_valuation_analysis(symbol=self.stock_code)
        if not valuation_df.empty:
            fundamental_data["valuation"] = valuation_df.iloc[0].to_dict()
        
        # 行业对标
        industry = fundamental_data["basic_info"].get("行业", "未知")
        if industry != "未知":
            industry_stocks = ak.stock_board_industry_cons_em(industry=industry)
            fundamental_data["industry_stocks"] = industry_stocks["代码"].tolist()[:INDUSTRY_STOCK_LIMIT]
        
        self._cached_data["fundamental_data"] = fundamental_data
        logger.info(f"{self.stock_code}基本面数据采集完成")
        return fundamental_data

    @retry_decorator()
    def get_tech_data(self) -> pd.DataFrame:
        """获取技术面数据（含衍生指标）"""
        if "tech_data" in self._cached_data:
            return self._cached_data["tech_data"]
        
        logger.info(f"采集{self.stock_code}技术面数据（起始：{TECH_DATA_START_DATE}）")
        # 基础日线数据
        tech_df = ak.stock_zh_a_hist(
            symbol=self.stock_code,
            period="daily",
            adjust="qfq",
            start_date=TECH_DATA_START_DATE
        )
        
        if tech_df.empty:
            raise ValueError(f"{self.stock_code}无有效技术面数据")
        
        # 列名标准化
        tech_df.rename(columns={
            "日期": "date", "收盘": "close", "开盘": "open",
            "最高": "high", "最低": "low", "成交量": "volume",
            "涨跌幅": "pct_change", "成交额": "amount"
        }, inplace=True)
        
        # 计算衍生技术指标
        # EMA & MACD
        tech_df["ema12"] = tech_df["close"].ewm(span=12, adjust=False).mean()
        tech_df["ema26"] = tech_df["close"].ewm(span=26, adjust=False).mean()
        tech_df["macd"] = tech_df["ema12"] - tech_df["ema26"]
        tech_df["signal"] = tech_df["macd"].ewm(span=9, adjust=False).mean()
        tech_df["macd_hist"] = tech_df["macd"] - tech_df["signal"]
        
        # RSI(6)
        delta = tech_df["close"].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain6 = gain.ewm(span=6, adjust=False).mean()
        avg_loss6 = loss.ewm(span=6, adjust=False).mean()
        rs6 = avg_gain6 / avg_loss6
        tech_df["rsi6"] = 100 - (100 / (1 + rs6))
        
        # KDJ
        low_min = tech_df["low"].rolling(window=9).min()
        high_max = tech_df["high"].rolling(window=9).max()
        tech_df["rsv"] = (tech_df["close"] - low_min) / (high_max - low_min) * 100
        tech_df["k"] = tech_df["rsv"].ewm(span=3, adjust=False).mean()
        tech_df["d"] = tech_df["k"].ewm(span=3, adjust=False).mean()
        tech_df["j"] = 3 * tech_df["k"] - 2 * tech_df["d"]
        
        # BOLL带
        tech_df["boll_mid"] = tech_df["close"].rolling(window=20).mean()
        tech_df["boll_std"] = tech_df["close"].rolling(window=20).std()
        tech_df["boll_upper"] = tech_df["boll_mid"] + 2 * tech_df["boll_std"]
        tech_df["boll_lower"] = tech_df["boll_mid"] - 2 * tech_df["boll_std"]
        
        # 均线
        tech_df["ma5"] = tech_df["close"].rolling(window=5).mean()
        tech_df["ma10"] = tech_df["close"].rolling(window=10).mean()
        tech_df["ma20"] = tech_df["close"].rolling(window=20).mean()
        tech_df["ma60"] = tech_df["close"].rolling(window=60).mean()
        
        # 换手率（默认0，可扩展）
        tech_df["turnover"] = 0.0
        
        self._cached_data["tech_data"] = tech_df
        logger.info(f"{self.stock_code}技术面数据采集完成（{len(tech_df)}条）")
        return tech_df

    def get_all_data(self) -> Dict:
        """获取所有维度数据（一键式）"""
        logger.info(f"开始采集{self.stock_code}全维度数据")
        all_data = {
            "basic_info": self.get_basic_info(),
            "capital_data": self.get_capital_data(),
            "fundamental_data": self.get_fundamental_data(),
            "tech_data": self.get_tech_data().to_dict("records"),  # 转成字典方便传输
            "stock_code": self.stock_code
        }
        logger.info(f"{self.stock_code}全维度数据采集完成")
        return all_data

# 供LangGraph调用的节点函数
def data_fetcher_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    data_fetcher = DataFetcherAgent(stock_code)
    all_data = data_fetcher.get_all_data()
    state.update(all_data)
    return state

# 测试代码
if __name__ == "__main__":
    try:
        # 初始化数据获取Agent
        data_fetcher = DataFetcherAgent("600519")
        
        # 按需获取单维度数据
        basic_info = data_fetcher.get_basic_info()
        print(f"✅ 基本信息采集完成：行业={basic_info.get('行业')}")
        
        # 或一键获取所有数据
        all_data = data_fetcher.get_all_data()
        print(f"✅ 全维度数据采集完成：")
        print(f"  - 资金数据维度：{list(all_data['capital_data'].keys())}")
        print(f"  - 基本面数据维度：{list(all_data['fundamental_data'].keys())}")
        print(f"  - 技术面数据条数：{len(all_data['tech_data'])}")
        
    except Exception as e:
        logger.error(f"数据采集失败：{str(e)}")
        print(f"❌ 执行失败：{str(e)}")