"""测试 AkShare + Finnhub 美股财务数据"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd()))

from config.env_config import EnvConfig

print("=" * 60)
print("1. Finnhub 美股财务数据")
print("=" * 60)
key = EnvConfig.FINNHUB_API_KEY
print(f"  API Key: {'***' + key[-4:] if key else 'NOT SET'}")

if key:
    import requests
    # Finnhub 公司基本面指标
    for endpoint, desc in [
        ("stock/metric", "估值指标(PE/PB/EPS/ROE等)"),
        ("stock/profile2", "公司概况"),
    ]:
        url = f"https://finnhub.io/api/v1/{endpoint}"
        params = {"symbol": "BABA", "token": key}
        try:
            r = requests.get(url, params=params, timeout=15)
            data = r.json()
            print(f"\n  {desc}:")
            print(f"    status={r.status_code}")
            if isinstance(data, dict):
                # Filter to only non-empty/non-zero values
                for k, v in data.items():
                    if v not in (None, "", 0, 0.0, [], {}):
                        print(f"    {k}: {v}")
            else:
                print(f"    {str(data)[:200]}")
        except Exception as e:
            print(f"    ERROR: {e}")

print()
print("=" * 60)
print("2. AkShare 美股财务数据")
print("=" * 60)

import akshare as ak
# Test AkShare US stock functions
for func_name in ["stock_us_spot_em", "stock_us_valuation_baidu"]:
    print(f"\n  {func_name}:")
    try:
        func = getattr(ak, func_name)
        result = func()
        if hasattr(result, 'head'):
            print(f"    type=DataFrame, shape={result.shape}, columns={list(result.columns)[:10]}")
            baba_row = result[result.apply(lambda r: r.astype(str).str.contains('BABA|阿里巴巴').any(), axis=1)]
            if not baba_row.empty:
                print(f"    BABA row: {baba_row.iloc[0].to_dict()}")
            else:
                print("    BABA not found")
        else:
            print(f"    type={type(result).__name__}, {str(result)[:200]}")
    except Exception as e:
        print(f"    ERROR: {type(e).__name__}: {str(e)[:150]}")