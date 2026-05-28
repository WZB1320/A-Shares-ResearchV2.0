"""BABA 完整分析测试"""
import sys, logging
from pathlib import Path
sys.path.insert(0, str(Path.cwd()))

logging.basicConfig(level=logging.WARNING)

from layers.agents.chief_agent import ChiefAgent

print("=" * 60)
print("BABA ChiefAgent 完整分析")
print("=" * 60)

agent = ChiefAgent()
result = agent.analyze("BABA")

print()
print("=" * 60)
print("【最终整合报告】")
print("=" * 60)
print(result.get("final_report", "N/A")[:2500])

print()
print("=" * 60)
print("【各维度评分】")
print("=" * 60)
for k, v in result.get("reports", {}).items():
    print(f"  {k}: score={v.get('overall_score')}, grade={v.get('grade')}, "
          f"confidence={v.get('confidence')}%")

print()
print("Done")