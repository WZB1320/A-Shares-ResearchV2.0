"""BABA 完整 workflow 测试"""
import sys, logging
from pathlib import Path
sys.path.insert(0, str(Path.cwd()))

logging.basicConfig(level=logging.WARNING)

from graph.workflow import run_workflow

print("=" * 60)
print("BABA 完整 Workflow 分析开始...")
print("=" * 60)

result = run_workflow("BABA", selected_agents=["technical", "fundamental", "valuation", "risk"])

print("\n" + "=" * 60)
print("完成! 各 Agent 报告:")
print("=" * 60)

reports = result.get("reports", {})
for name, report in reports.items():
    print(f"\n--- [{name}] ---")
    if isinstance(report, str):
        report = report.strip()
        if len(report) > 500:
            print(report[:500] + "\n... (truncated)")
        else:
            print(report)
    else:
        print(f"  type={type(report).__name__}, val={str(report)[:200]}")