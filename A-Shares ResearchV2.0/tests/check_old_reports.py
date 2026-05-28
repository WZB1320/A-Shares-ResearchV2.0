"""查看存量研报的发布日期"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from layers.memory.knowledge_base import get_reports_for_stock, init_kb

init_kb()
print("--- BABA 存量研报 ---")
reports = get_reports_for_stock("BABA")
for r in reports:
    print(f"  {r['title']} | {r['pub_date']} | {r['institution']}")

print("\n--- 600519 存量研报 ---")
reports600 = get_reports_for_stock("600519")
for r in reports600:
    print(f"  {r['title']} | {r['pub_date']} | {r['institution']}")

print("\n--- get_report_consensus 内容 ---")
from layers.memory.knowledge_base import get_report_consensus
print(get_report_consensus("BABA"))