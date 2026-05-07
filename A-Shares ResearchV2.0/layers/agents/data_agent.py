import sys
import logging
from pathlib import Path
from typing import Dict, Optional, Any

sys.path.append(str(Path(__file__).parent.parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="[%(name)s] %(asctime)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("DataAgent")


class DataAgent:
    """
    数据Agent - 统一数据获取入口
    职责：调用 DataConnector 获取所有数据，为其他Agent提供数据源
    """

    def __init__(self, stock_code: str):
        self.stock_code = stock_code
        self._connector = None
        self._cached_data: Dict[str, Any] = {}

    def _get_connector(self):
        if self._connector is None:
            from layers.connectors import DataConnector
            self._connector = DataConnector(self.stock_code)
        return self._connector

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


def data_agent_node(stock_code: str) -> Dict:
    agent = DataAgent(stock_code)
    return agent.fetch_all()


data_agent = DataAgent
