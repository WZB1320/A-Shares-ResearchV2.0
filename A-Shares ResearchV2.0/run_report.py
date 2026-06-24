"""运行完整分析并保存结果到文件"""
import sys
import json
import logging
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(level=logging.INFO, format="[%(name)s] %(asctime)s - %(message)s")

from layers.agents.chief_agent import ChiefAgent

print("=" * 80)
print("[A股投研系统] 开始分析: 002272")
print("=" * 80)

agent = ChiefAgent()
result = agent.analyze("002272")

output = []
output.append("=" * 80)
output.append(f"【002272 完整投研报告】")
output.append("=" * 80)
output.append("")
output.append("--- 最终整合报告 ---")
output.append(result.get("final_report", ""))
output.append("")

dim_labels = {
    "tech": "技术面", "fund": "基本面", "capital": "资金面",
    "industry": "行业面", "risk": "风险面", "valuation": "估值面",
}
reports = result.get("reports", {})
for key, label in dim_labels.items():
    if key in reports:
        output.append(f"【{label}】")
        output.append(json.dumps(reports[key], ensure_ascii=False, indent=2))
        output.append("")

output.append("=" * 80)
output.append(f"综合评分: {result.get('overall_score', 0)}/100 | 评级: {result.get('overall_grade', '-')}")
output.append("=" * 80)

report_text = "\n".join(output)

with open("report_002272.txt", "w", encoding="utf-8") as f:
    f.write(report_text)

print("\n报告已保存到 report_002272.txt")
print(f"综合评分: {result.get('overall_score', 0)}/100 | 评级: {result.get('overall_grade', '-')}")