"""多市场数据源测试 (需要网络/akshare, 默认跳过)

运行（含网络）：pytest -m integration tests/test_market_data.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from layers.connectors import DataConnector


@pytest.mark.integration
def test_hk_data_fetch():
    dc = DataConnector("00700")
    basic = dc.fetch_basic_info()
    assert isinstance(basic, dict) and basic.get("名称"), "港股 basic_info 异常"

    tech = dc.fetch_tech_data()
    assert tech is not None and len(tech) > 0, "港股 tech_data 为空"

    val = dc.fetch_valuation_data()
    assert isinstance(val, dict), "港股 valuation 异常"

    fin = dc.fetch_financial_data()
    assert isinstance(fin, dict), "港股 financial 异常"

    all_data = dc.fetch_all()
    assert isinstance(all_data, dict) and len(all_data) > 0, "fetch_all 为空"


@pytest.mark.integration
def test_us_data_fetch():
    dc2 = DataConnector("BABA")
    basic2 = dc2.fetch_basic_info()
    assert isinstance(basic2, dict), "美股 basic_info 异常"

    tech2 = dc2.fetch_tech_data()
    assert tech2 is not None, "美股 tech_data 异常"

    val2 = dc2.fetch_valuation_data()
    assert isinstance(val2, dict), "美股 valuation 异常"

    fin2 = dc2.fetch_financial_data()
    assert isinstance(fin2, dict), "美股 financial 异常"
