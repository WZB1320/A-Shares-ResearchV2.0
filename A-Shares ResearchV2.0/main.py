import sys
import logging
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="[%(name)s] %(asctime)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

from layers.agents.chief_agent import ChiefAgent


def run_analysis(stock_code: str, selected_agents: list = None):
    """统一分析入口 - ChiefAgent 编排

    ChiefAgent 内部完成：数据获取 → 校验 → 并行分析 → 辩论 → 汇总 → 知识库存储
    """
    print("\n" + "=" * 80)
    print(f"[A股投研系统] 开始分析: {stock_code}")
    print("=" * 80)

    agent = ChiefAgent()
    state = {"selected_agents": selected_agents} if selected_agents else None
    result = agent.analyze(stock_code, state)

    print("\n" + "=" * 80)
    print(f"【{stock_code} 完整投研报告】")
    print("=" * 80)
    print("\n--- 最终整合报告 ---")
    print(result["final_report"])

    print("\n--- 各维度报告 ---")
    dim_labels = {
        "capital": "资金面",
        "fundamental": "基本面",
        "industry": "行业面",
        "risk": "风险面",
        "technical": "技术面",
        "valuation": "估值面",
    }
    reports = result.get("reports", {})
    for key, label in dim_labels.items():
        if key in reports:
            print(f"\n【{label}】")
            print(reports[key])

    print("\n" + "=" * 80)
    print(f"综合评分: {result.get('overall_score', 0)}/100 | 评级: {result.get('overall_grade', '-')}")
    print("=" * 80)

    return result


def main():
    print("=" * 80)
    print("A股多维度机构级投研系统")
    print("=" * 80)

    stock_code = input("\n输入股票代码(默认600519)：") or "600519"

    try:
        run_analysis(stock_code)
    except Exception as e:
        print(f"\n❌ 分析失败：{e}")
        import traceback
        print(traceback.format_exc())

    print("\n" + "=" * 80)
    print("✅ 分析完成！")
    print("=" * 80)


if __name__ == "__main__":
    main()
