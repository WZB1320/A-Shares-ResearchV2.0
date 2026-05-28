"""A股 + 美股 双路径数据获取测试"""
import sys, logging
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
logging.basicConfig(level=logging.WARNING)

from layers.connectors import DataConnector

S = "=" * 60

# 1. A股 600519
print(f"\n{S}")
print("1. A股 600519 (贵州茅台)")
try:
    dc = DataConnector("600519")
    print(f"   市场: {dc._market}, 主源: {dc._primary_source}")
    print(f"   可用源: {dc.get_available_sources()}")
    data = dc.fetch_all()
    bi = data.get("basic_info", {})
    print(f"   名称: {bi.get('名称', 'N/A')}, 最新价: {bi.get('最新价', 'N/A')}")
    tech = data.get("tech_data")
    print(f"   K线行数: {len(tech) if tech else 0}")
    if tech:
        print(f"   首条: {tech[0].get('date', 'N/A')} | 末条: {tech[-1].get('date', 'N/A')}")
except Exception as e:
    print(f"   FAIL: {type(e).__name__}: {str(e)[:200]}")

# 2. BABA
print(f"\n{S}")
print("2. BABA (美股)")
try:
    dc = DataConnector("BABA")
    print(f"   市场: {dc._market}, 主源: {dc._primary_source}")
    print(f"   可用源: {dc.get_available_sources()}")
    data = dc.fetch_all()
    bi = data.get("basic_info", {})
    print(f"   名称: {bi.get('名称', 'N/A')}, 最新价: {bi.get('最新价', 'N/A')}")
    fin = data.get("financial_data", {})
    print(f"   ROE: {fin.get('roe', 'N/A')}, 毛利率: {fin.get('gross_profit', 'N/A')}")
except Exception as e:
    print(f"   FAIL: {type(e).__name__}: {str(e)[:200]}")

print(f"\n{S}")
print("Done")