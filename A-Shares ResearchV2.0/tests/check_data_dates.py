"""查看数据获取的日期来源"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from layers.connectors import DataConnector

dc = DataConnector("600519")
data = dc.fetch_all()
print("=== basic_info ===")
print(data.get("basic_info"))
print("\n=== tech_data keys/columns ===")
tech = data.get("tech_data")
print("len:", len(tech) if tech else "NONE")
if tech and hasattr(tech, '__len__') and len(tech) > 0:
    if hasattr(tech, 'iloc'):
        print("columns:", list(tech.columns))
        print("tail:\n", tech.tail(3))
    elif isinstance(tech, list):
        print(tech[-3:])

print("\n=== other dates ===")
print("fundamental:", data.get("fundamental_data", {}).keys())
print("valuation:", data.get("valuation_data", {}))