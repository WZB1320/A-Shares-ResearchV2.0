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
        try:
            # 方法1: 使用 EM 接口获取基础信息
            basic_df = ak.stock_individual_info_em(symbol=self.stock_code)
            basic_info = basic_df.set_index("item").to_dict()["value"]
            
            # 方法2: 获取行业分类 (用于后续行业对比)
            # 由于 EM 接口有时不稳定，这里补充一个兜底
            if "行业" not in basic_info:
                name_code_df = ak.stock_info_a_code_name()
                # 将代码转为 6位纯数字格式进行匹配
                code_digit = self.stock_code[:6]
                stock_row = name_code_df[name_code_df['code'] == code_digit]
                if not stock_row.empty:
                    # 这里假设接口返回包含行业列，实际可能需要根据最新 akshare 返回结构调整
                    # 如果没有，暂时标记为未知
                    basic_info["行业"] = "未知-需确认接口"
            
            self._cached_data["basic_info"] = basic_info
            return basic_info
        except Exception as e:
            logger.warning(f"主接口失败，使用模拟数据兜底: {e}")
            return {
                "公司名称": self.stock_code,
                "行业": "未知",
                "上市日期": "未知"
            }

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
            # 北向资金 (沪/深股通)
            # 注意：新版本 akshare 可能需要区分沪市和深市，或者使用通用接口
            north_df = ak.stock_hsgt_hist_em(symbol="北向")
            if not north_df.empty:
                # 筛选该股票的数据 (具体字段取决于接口返回结构)
                # 由于 AkShare 接口变动频繁，这里简化处理，获取全量后在前端处理
                # 或者使用 stock_hsgt_north_net_flow_in_em 获取净流入
                capital_data["north"] = north_df.tail(CAPITAL_DATA_DAYS).to_dict("records")

            # 主力资金 (新版本接口可能为 stock_zh_a_main_large_inflow_outflow)
            # 这里使用通用的历史接口
            main_df = ak.stock_main_fund_daily(symbol=self.stock_code, indicator="今日")
            if not main_df.empty:
                capital_data["main"] = main_df.tail(CAPITAL_DATA_DAYS).to_dict("records")

            # 融资融券
            margin_df = ak.stock_margin_trading_sse(symbol=self.stock_code)
            if not margin_df.empty:
                capital_data["margin"] = margin_df.tail(CAPITAL_DATA_DAYS).to_dict("records")

            # 龙虎榜
            dragon_df = ak.stock_lh_yyb_jg_gg_df(symbol=self.stock_code)
            if not dragon_df.empty:
                capital_data["dragon"] = dragon_df.tail(5).to_dict("records") # 只取最近5条

            logger.info(f"{self.stock_code}资金数据采集完成")
        except Exception as e:
            error_msg = f"资金数据采集部分失败：{str(e)}"
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
            "basic_info": self.get_basic_info()
        }

        try:
            # --- 1. 财务指标 (近N年) ---
            # 使用新的财务分析接口
            finance_df = ak.stock_financial_analysis_indicator(symbol=self.stock_code, indicator="盈利能力")
            if not finance_df.empty:
                # 转置并取最近几年
                finance_df = finance_df.T
                finance_df.columns = finance_df.iloc[0]
                finance_df = finance_df[1:].tail(FINANCE_DATA_YEARS)
                fundamental_data["finance"] = finance_df.to_dict("records")

            # --- 2. 估值指标 (核心修复点) ---
            # 原函数 stock_valuation_analysis 已废弃
            # 使用理杏仁/萝卜投研等底层接口替代 (ak.stock_a_lg_indicator_en)
            # 这个接口需要指定 symbol 和 indicator
            valuation_raw = ak.stock_a_lg_indicator_en(symbol=self.stock_code)
            
            if not valuation_raw.empty:
                # 获取最新一条数据
                latest_val = valuation_raw.iloc[-1]
                # 常用估值字段映射
                valuation_dict = {
                    "市盈率": latest_val.get("市盈率", None),
                    "市净率": latest_val.get("市净率", None),
                    "股息率": latest_val.get("股息率", None),
                    "净资产收益率": latest_val.get("净资产收益率", None),
                    "总市值": latest_val.get("总市值", None)
                }
                fundamental_data["valuation"] = valuation_dict

            # --- 3. 行业对标 ---
            industry = fundamental_data["basic_info"].get("行业", "未知")
            if industry != "未知" and industry != "未知-需确认接口":
                # 获取行业成分股列表
                # 先获取该股的行业代码，再获取成分股
                try:
                    # 获取行业板块列表
                    industry_list = ak.stock_board_industry_name_em()
                    # 这里简化处理，直接通过行业名称搜索
                    target_board = industry_list[industry_list['name'] == industry]
                    if not target_board.empty:
                        board_code = target_board.iloc[0]['code']
                        # 获取该板块成分股
                        cons_df = ak.stock_board_industry_cons_em(symbol=board_code)
                        if not cons_df.empty:
                            fundamental_data["industry_stocks"] = cons_df["symbol"].tolist()[:INDUSTRY_STOCK_LIMIT]
                except:
                    # 兜底：如果通过名称找不到，尝试模糊匹配或直接返回空
                    logger.warning(f"无法获取行业 {industry} 的成分股")
                    fundamental_data["industry_stocks"] = []

        except Exception as e:
            logger.error(f"基本面数据采集部分失败，但仍返回已有数据: {str(e)}")
            # 即使出错，也返回已采集到的部分数据，避免流程中断
            pass

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
        # 注意：新版本 ak 可能需要指定 adjust 参数为 "qfq" 或 "hfq"
        tech_df = ak.stock_zh_a_hist(
            symbol=self.stock_code,
            period="daily",
            adjust="qfq",
            start_date=TECH_DATA_START_DATE
        )
        
        if tech_df.empty:
            raise ValueError(f"{self.stock_code}无有效技术面数据")

        # --- 列名标准化 (适配新版本 AkShare 默认列名) ---
        # 新版本 ak.stock_zh_a_hist 返回的列名通常是英文或标准中文
        # 假设返回列名为: ['日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额']
        tech_df.rename(columns={
            "日期": "date",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
            "成交额": "amount"
            # "涨跌幅" 列如果不存在，需要手动计算
        }, inplace=True)

        # 如果没有涨跌幅，手动计算
        if "pct_change" not in tech_df.columns:
            tech_df["pct_change"] = tech_df["close"].pct_change() * 100

        # --- 计算衍生技术指标 ---
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

        # 换手率（AkShare 新接口可能不直接提供，需从基础数据获取或设为0）
        # 这里假设基础数据没有，先设为0，或者你可以调用 stock_zh_a_hist_min 获取分钟数据推算
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
        print(f" - 资金数据维度：{list(all_data['capital_data'].keys())}")
        print(f" - 基本面数据维度：{list(all_data['fundamental_data'].keys())}")
        print(f" - 技术面数据条数：{len(all_data['tech_data'])}")
    except Exception as e:
        logger.error(f"数据采集失败：{str(e)}")
        print(f"❌ 执行失败：{str(e)}")