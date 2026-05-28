"""BABA 数据获取诊断 — Sina 美股数据源"""
import sys, logging
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
logging.basicConfig(level=logging.WARNING, format="[%(name)s] %(message)s")

from layers.connectors.market_data_sources import SinaDataSource
from layers.connectors import DataConnector

print("="*60)
print("[0] SinaDataSource 可用性 + BABA 美股测试")
print("="*60)
sina = SinaDataSource()
print(f"  available = {sina.available}")
print(f"  is_us(BABA) = {sina._is_us_stock('BABA')}")
print(f"  to_sina_us_symbol(BABA) = {sina._to_sina_us_symbol('BABA')}")

# basic_info
print()
print("[0a] Sina fetch_basic_info('BABA')")
r = sina.fetch_basic_info("BABA")
print(f"  basic_info: {r}")

# tech_data
print()
print("[0b] Sina fetch_tech_data('BABA')")
tech_df = sina.fetch_tech_data("BABA")
print(f"  tech_data: rows={len(tech_df)}, columns={list(tech_df.columns)[:] if not tech_df.empty else 'EMPTY'}")

# fundamental
print()
print("[0c] Sina fetch_fundamental_data('BABA')")
fund = sina.fetch_fundamental_data("BABA")
for k, v in fund.items():
    if k == 'valuation':
        print(f"  valuation: {v}")
    else:
        print(f"  {k}: {str(v)[:120]}")

# financial
print()
print("[0d] Sina fetch_financial_data('BABA')")
fin = sina.fetch_financial_data("BABA")
print(f"  financial: {fin}")

# ── DataConnector 完整测试 ──
print()
print("="*60)
print("[1] DataConnector('BABA') 完整测试")
print("="*60)
dc = DataConnector("BABA")
print(f"  market = {dc._market}")
print(f"  primary = {dc.get_primary_source()}")
print(f"  sources = {dc.get_available_sources()}")

all_data = dc.fetch_all()
for k, v in all_data.items():
    if k == 'tech_data':
        print(f"  {k}: list with {len(v)} records")
    elif isinstance(v, dict):
        print(f"  {k}: dict with keys {list(v.keys())[:8]}")
    else:
        print(f"  {k}: {type(v).__name__} - {str(v)[:80]}")

print()
print("Done")