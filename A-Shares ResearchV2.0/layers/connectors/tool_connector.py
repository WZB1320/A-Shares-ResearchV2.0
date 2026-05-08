import sys
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Callable
from functools import wraps

logger = logging.getLogger("ToolConnector")


class ToolConnector:
    """
    Anthropic 标准 Connector 层 - 统一工具调用入口
    封装所有外部工具调用（akshare/tushare/计算函数等）
    为 Agent 提供统一、可测试的工具接口
    """

    def __init__(self):
        self._tools: Dict[str, Callable] = {}
        self._register_default_tools()
        logger.info("[ToolConnector] 初始化完成")

    def _register_default_tools(self) -> None:
        self.register_tool("calc_10year_percentile", self._calc_10year_percentile)
        self.register_tool("format_num", self._format_num)
        self.register_tool("calculate_ma", self._calculate_ma)
        self.register_tool("calculate_rsi", self._calculate_rsi)
        self.register_tool("calculate_macd", self._calculate_macd)
        self.register_tool("calculate_boll", self._calculate_boll)
        self.register_tool("calculate_volatility", self._calculate_volatility)

    def register_tool(self, name: str, func: Callable) -> None:
        self._tools[name] = func
        logger.info(f"[ToolConnector] 注册工具: {name}")

    def unregister_tool(self, name: str) -> None:
        if name in self._tools:
            del self._tools[name]
            logger.info(f"[ToolConnector] 注销工具: {name}")

    def call_tool(self, name: str, **kwargs) -> Any:
        if name not in self._tools:
            raise ValueError(f"工具未注册: {name}")
        try:
            result = self._tools[name](**kwargs)
            logger.debug(f"[ToolConnector] 工具调用成功: {name}")
            return result
        except Exception as e:
            logger.error(f"[ToolConnector] 工具调用失败: {name} | error={str(e)}")
            raise

    def get_tool(self, name: str) -> Optional[Callable]:
        return self._tools.get(name)

    def list_tools(self) -> List[str]:
        return list(self._tools.keys())

    @staticmethod
    def _calc_10year_percentile(history: List[float], current: float) -> float:
        """计算历史分位"""
        if not history or len(history) == 0:
            return 0.0
        arr = sorted(history)
        cnt = 0
        for v in arr:
            if v <= current:
                cnt += 1
        return round(cnt / len(arr) * 100, 2)

    @staticmethod
    def _format_num(num: Any, default: float = 0.0) -> float:
        """数字格式化"""
        try:
            return round(float(num), 2)
        except:
            return default

    @staticmethod
    def _calculate_ma(prices: List[float], period: int) -> float:
        """计算移动平均"""
        if len(prices) < period:
            return 0.0
        return round(sum(prices[-period:]) / period, 2)

    @staticmethod
    def _calculate_rsi(prices: List[float], period: int = 14) -> float:
        """计算RSI"""
        if len(prices) < period + 1:
            return 50.0

        deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        gains = [d if d > 0 else 0 for d in deltas[-period:]]
        losses = [-d if d < 0 else 0 for d in deltas[-period:]]

        avg_gain = sum(gains) / period if gains else 0
        avg_loss = sum(losses) / period if losses else 0

        if avg_loss == 0:
            return 100.0

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return round(rsi, 2)

    @staticmethod
    def _calculate_macd(prices: List[float]) -> Dict[str, float]:
        """计算MACD"""
        if len(prices) < 26:
            return {"macd": 0, "signal": 0, "hist": 0}

        import pandas as pd
        df = pd.DataFrame({"close": prices})
        df["ema12"] = df["close"].ewm(span=12, adjust=False).mean()
        df["ema26"] = df["close"].ewm(span=26, adjust=False).mean()
        df["macd"] = df["ema12"] - df["ema26"]
        df["signal"] = df["macd"].ewm(span=9, adjust=False).mean()
        df["hist"] = df["macd"] - df["signal"]

        latest = df.iloc[-1]
        return {
            "macd": round(float(latest["macd"]), 2),
            "signal": round(float(latest["signal"]), 2),
            "hist": round(float(latest["hist"]), 2)
        }

    @staticmethod
    def _calculate_boll(prices: List[float], period: int = 20) -> Dict[str, float]:
        """计算布林带"""
        if len(prices) < period:
            return {"upper": 0, "mid": 0, "lower": 0}

        import pandas as pd
        df = pd.DataFrame({"close": prices})
        df["mid"] = df["close"].rolling(window=period).mean()
        df["std"] = df["close"].rolling(window=period).std()
        df["upper"] = df["mid"] + 2 * df["std"]
        df["lower"] = df["mid"] - 2 * df["std"]

        latest = df.iloc[-1]
        return {
            "upper": round(float(latest["upper"]), 2),
            "mid": round(float(latest["mid"]), 2),
            "lower": round(float(latest["lower"]), 2)
        }

    @staticmethod
    def _calculate_volatility(prices: List[float], period: int = 20) -> float:
        """计算波动率"""
        if len(prices) < period:
            return 0.0

        import pandas as pd
        df = pd.DataFrame({"close": prices})
        returns = df["close"].pct_change().dropna()
        volatility = returns.rolling(window=period).std() * (252 ** 0.5)
        return round(float(volatility.iloc[-1]), 4) if not volatility.empty else 0.0


_tool_connector_instance: Optional[ToolConnector] = None


def get_tool_connector() -> ToolConnector:
    """获取 ToolConnector 单例"""
    global _tool_connector_instance
    if _tool_connector_instance is None:
        _tool_connector_instance = ToolConnector()
    return _tool_connector_instance


def call_tool(name: str, **kwargs) -> Any:
    """快捷工具调用"""
    return get_tool_connector().call_tool(name, **kwargs)
