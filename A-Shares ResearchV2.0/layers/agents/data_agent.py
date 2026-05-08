import sys
import logging
from pathlib import Path
from typing import Dict, Optional, Any

logger = logging.getLogger("DataAgent")

from config.llm_config import DEFAULT_MODEL


class DataAgent:
    """
    数据Agent - 统一数据获取入口（机构级标准化）
    职责：调用 DataConnector 获取所有数据，为其他Agent提供数据源
    """

    def __init__(self, model_name: str = DEFAULT_MODEL, data_connector: Optional[Any] = None):
        self.model_name = model_name.lower()
        self.data_connector = data_connector
        self.stock_code = None
        self._connector = None
        self._cached_data: Dict[str, Any] = {}

    def _get_connector(self):
        if self._connector is None:
            from layers.connectors import DataConnector
            if self.stock_code is None:
                raise ValueError("stock_code 未设置，请先调用 analyze() 或设置 stock_code")
            self._connector = DataConnector(self.stock_code)
        return self._connector

    def analyze(self, stock_code: str, state: Optional[Dict] = None) -> str:
        logger.info(f"[DataAgent] 开始数据获取: {stock_code}")

        self.stock_code = stock_code
        try:
            all_data = self.fetch_all()
            data_types = [k for k in all_data.keys() if all_data[k] is not None]
            summary = f"[DataAgent] {stock_code} 数据获取完成 | 已获取: {', '.join(data_types)} | 共{len(data_types)}类数据"
            logger.info(f"[DataAgent] 数据获取完成: {stock_code} | 类型数: {len(data_types)}")
            return summary
        except Exception as e:
            error_msg = f"获取{stock_code}数据失败：{str(e)}"
            logger.error(f"[DataAgent] {error_msg}")
            return f"⚠️ {error_msg}"

    def fetch_basic_info(self) -> Dict:
        logger.info(f"[DataAgent] 获取基本信息: {self.stock_code}")
        connector = self._get_connector()
        data = connector.fetch_basic_info()
        self._cached_data["basic_info"] = data
        return data

    def fetch_capital_data(self) -> Dict:
        logger.info(f"[DataAgent] 获取资金数据: {self.stock_code}")
        connector = self._get_connector()
        data = connector.fetch_capital_data()
        self._cached_data["capital_data"] = data
        return data

    def fetch_fundamental_data(self) -> Dict:
        logger.info(f"[DataAgent] 获取基本面数据: {self.stock_code}")
        connector = self._get_connector()
        data = connector.fetch_fundamental_data()
        self._cached_data["fundamental_data"] = data
        return data

    def fetch_tech_data(self):
        logger.info(f"[DataAgent] 获取技术面数据: {self.stock_code}")
        connector = self._get_connector()
        data = connector.fetch_tech_data()
        self._cached_data["tech_data"] = data
        return data

    def fetch_valuation_data(self) -> Dict:
        logger.info(f"[DataAgent] 获取估值数据: {self.stock_code}")
        connector = self._get_connector()
        data = connector.fetch_valuation_data()
        self._cached_data["valuation_data"] = data
        return data

    def fetch_financial_data(self) -> Dict:
        logger.info(f"[DataAgent] 获取财务数据: {self.stock_code}")
        connector = self._get_connector()
        data = connector.fetch_financial_data()
        self._cached_data["financial_data"] = data
        return data

    def fetch_all(self) -> Dict:
        logger.info(f"[DataAgent] 获取全量数据: {self.stock_code}")
        connector = self._get_connector()
        all_data = connector.fetch_all()
        self._cached_data = all_data
        return all_data

    def get_data(self, data_type: str) -> Any:
        if data_type in self._cached_data:
            return self._cached_data[data_type]

        connector = self._get_connector()
        return connector.get_data(data_type)


def data_agent_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    agent = DataAgent(model_name=DEFAULT_MODEL)
    agent.analyze(stock_code, state)
    state["all_data"] = agent._cached_data
    return state


data_agent = DataAgent
