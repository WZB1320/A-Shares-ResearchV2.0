"""多市场数据源测试"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import logging
logging.basicConfig(level=logging.WARNING)

from layers.connectors import DataConnector

# ── 港股测试 ──
print('=== 港股 00700 (腾讯) ===')
dc = DataConnector('00700')
basic = dc.fetch_basic_info()
print(f"  basic_info: {basic.get('名称', '?')}")

tech = dc.fetch_tech_data()
print(f"  tech_data: {len(tech)} rows, cols: {list(tech.columns)[:8]}...")

val = dc.fetch_valuation_data()
print(f"  valuation: PE={val.get('pe_ttm')}, PB={val.get('pb')}")

fin = dc.fetch_financial_data()
print(f"  financial: ROE={fin.get('roe')}")

all_data = dc.fetch_all()
print(f"  fetch_all keys: {list(all_data.keys())}")

# ── 美股测试 ──
print()
print('=== 美股 BABA (阿里巴巴) ===')
dc2 = DataConnector('BABA')
basic2 = dc2.fetch_basic_info()
print(f"  basic_info: {basic2.get('名称', '?')}")

tech2 = dc2.fetch_tech_data()
print(f"  tech_data: {len(tech2)} rows")

val2 = dc2.fetch_valuation_data()
print(f"  valuation: PE={val2.get('pe_ttm')}, PB={val2.get('pb')}")

fin2 = dc2.fetch_financial_data()
print(f"  financial: ROE={fin2.get('roe')}, GP={fin2.get('gross_profit')}")

print()
print('Done - all data fetch tests passed')