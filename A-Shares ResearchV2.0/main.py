import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from layers.agents.chief_agent import ChiefAgent
from graph.workflow import run_workflow


def run_simple_mode(stock_code: str):
    print("\n" + "="*80)
    print("[Harness架构] 简单模式 - ChiefAgent")
    print("="*80)
    print("\n首席Agent正在进行完整分析...")

    agent = ChiefAgent()
    result = agent.analyze(stock_code)

    print("\n" + "="*80)
    print(f"【{stock_code} 完整投研报告】")
    print("="*80)
    print("\n--- 最终整合报告 ---")
    print(result["final_report"])

    print("\n--- 各维度报告 ---")
    print("\n【资金面】")
    print(result["reports"]["capital"])
    print("\n【基本面】")
    print(result["reports"]["fundamental"])
    print("\n【行业面】")
    print(result["reports"]["industry"])
    print("\n【风险面】")
    print(result["reports"]["risk"])
    print("\n【技术面】")
    print(result["reports"]["technical"])
    if "valuation" in result["reports"]:
        print("\n【估值面】")
        print(result["reports"]["valuation"])

    return result


def run_harness_mode(stock_code: str):
    print("\n" + "="*80)
    print("[Harness架构] 调度器模式")
    print("="*80)

    from harness.scheduler import HarnessScheduler, SchedulerConfig
    from harness.state import HarnessStateManager
    from layers.agents.data_agent import DataAgent
    from layers.agents.tech_agent import TechAgent
    from layers.agents.fund_agent import FundAgent
    from layers.agents.capital_agent import CapitalAgent
    from layers.agents.industry_agent import IndustryAgent
    from layers.agents.risk_agent import RiskAgent
    from layers.agents.valuation_agent import ValuationAgent

    scheduler = HarnessScheduler(SchedulerConfig(
        max_retries=3,
        parallel_execution=True,
        max_workers=4,
        checkpoint_enabled=True,
        validation_enabled=True
    ))

    data_agent = DataAgent(stock_code)
    all_data = data_agent.fetch_all()

    state = {
        "stock_code": stock_code,
        "basic_info": all_data.get("basic_info", {}),
        "capital_data": all_data.get("capital_data", {}),
        "fundamental_data": all_data.get("fundamental_data", {}),
        "tech_data": all_data.get("tech_data", []),
        "valuation_data": all_data.get("valuation_data", {}),
        "financial_data": all_data.get("financial_data", {})
    }

    tech_report = TechAgent().analyze(stock_code, state["tech_data"])
    fund_report = FundAgent().analyze(stock_code, state["fundamental_data"])
    capital_report = CapitalAgent().analyze(stock_code, state["capital_data"])
    industry_report = IndustryAgent().analyze(stock_code, state["fundamental_data"])
    risk_report = RiskAgent().analyze(stock_code, state["financial_data"])
    valuation_report = ValuationAgent().analyze(stock_code, state["valuation_data"], state["fundamental_data"])

    result = {
        "stock_code": stock_code,
        "reports": {
            "capital": capital_report,
            "fundamental": fund_report,
            "industry": industry_report,
            "risk": risk_report,
            "technical": tech_report,
            "valuation": valuation_report
        }
    }

    print("\n--- 各维度报告 ---")
    print("\n【资金面】")
    print(result["reports"]["capital"])
    print("\n【基本面】")
    print(result["reports"]["fundamental"])
    print("\n【行业面】")
    print(result["reports"]["industry"])
    print("\n【风险面】")
    print(result["reports"]["risk"])
    print("\n【技术面】")
    print(result["reports"]["technical"])
    print("\n【估值面】")
    print(result["reports"]["valuation"])

    return result


def main():
    print("="*80)
    print("A股5维机构级投研系统 [Harness架构]")
    print("="*80)

    print("\n请选择运行模式：")
    print("  1. 简单模式 - ChiefAgent（Harness架构，推荐）")
    print("  2. 调度器模式 - HarnessScheduler（并行执行）")
    print("  3. LangGraph模式 - 原有工作流（兼容）")

    choice = input("\n请输入选项 (1/2/3，默认1)：") or "1"

    stock_code = input("\n输入股票代码(默认600519)：") or "600519"

    try:
        if choice == "1":
            run_simple_mode(stock_code)
        elif choice == "2":
            run_harness_mode(stock_code)
        elif choice == "3":
            run_workflow(stock_code)
        else:
            print("\n❌ 无效选项，默认使用简单模式")
            run_simple_mode(stock_code)

    except Exception as e:
        print(f"\n❌ 分析失败：{e}")
        import traceback
        print(traceback.format_exc())

    print("\n" + "="*80)
    print("✅ 分析完成！")
    print("="*80)


if __name__ == "__main__":
    main()
