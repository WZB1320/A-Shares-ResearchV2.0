"""验证 tech_data 日期修复"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from layers.connectors import DataConnector

dc = DataConnector("600519")
data = dc.fetch_all()

print("=== tech_data 最后三条（最新） ===")
td = data.get("tech_data")
if td:
    for i, row in enumerate(td[-3:]):
        print(f"  {len(td)-3+i+1}. {row['date']} | close={row['close']}")