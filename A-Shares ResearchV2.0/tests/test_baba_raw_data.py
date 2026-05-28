"""BABA 原始数据获取 — 逐维度输出实际数据内容"""
import sys, logging, json
from pathlib import Path
sys.path.insert(0, str(Path.cwd()))
logging.basicConfig(level=logging.WARNING)

from layers.connectors import DataConnector

S = "=" * 60
dc = DataConnector("BABA")
print(f"\n{S}")
print(f"【数据源信息】")
print(f"  市场: {dc._market}")
print(f"  主源: {dc.get_primary_source()}")
print(f"  可用源: {dc.get_available_sources()}")

all_data = dc.fetch_all()

# ── 1. basic_info ──
print(f"\n{S}")
print(f"【1. basic_info 基本信息】")
bi = all_data.get("basic_info", {})
for k, v in bi.items():
    print(f"  {k}: {v}")

# ── 2. tech_data ──
print(f"\n{S}")
print(f"【2. tech_data 技术面K线数据】")
td = all_data.get("tech_data", [])
print(f"  总行数: {len(td)}")
if td:
    print(f"  列名: {list(td[0].keys())}")
    print(f"  第一行: {json.dumps(td[0], default=str)}")
    print(f"  最后一行: {json.dumps(td[-1], default=str)}")

# ── 3. fundamental_data ──
print(f"\n{S}")
print(f"【3. fundamental_data 基本面数据】")
fd = all_data.get("fundamental_data", {})
print(f"  keys: {list(fd.keys())}")
print(f"  valuation: {json.dumps(fd.get('valuation', {}), default=str)}")
print(f"  finance: {fd.get('finance', [])}")
print(f"  industry_stocks: {fd.get('industry_stocks', [])}")
bi2 = fd.get("basic_info", {})
print(f"  basic_info (内嵌): {json.dumps(bi2, default=str) if bi2 else 'EMPTY'}")

# ── 4. valuation_data ──
print(f"\n{S}")
print(f"【4. valuation_data 估值数据】")
vd = all_data.get("valuation_data", {})
for k, v in vd.items():
    if isinstance(v, list):
        print(f"  {k}: list(len={len(v)})")
    else:
        print(f"  {k}: {v}")

# ── 5. financial_data ──
print(f"\n{S}")
print(f"【5. financial_data 财务数据】")
find = all_data.get("financial_data", {})
for k, v in find.items():
    print(f"  {k}: {v}")

# ── 6. capital_data ──
print(f"\n{S}")
print(f"【6. capital_data 资金面数据】")
cd = all_data.get("capital_data", {})
for k, v in cd.items():
    if isinstance(v, list):
        print(f"  {k}: list(len={len(v)})")
    else:
        print(f"  {k}: {v}")

print(f"\n{S}")
print("Done")