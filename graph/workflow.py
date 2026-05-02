from langgraph.graph import StateGraph, END
from typing import Dict, TypedDict, Optional
# 导入所有Agent节点
from agents.data_fetcher import data_fetcher_node
from agents.tech_analyzer import tech_analyzer_node
from agents.fund_analyzer import fund_analyzer_node
from agents.capital_analyzer import capital_analyzer_node
from agents.industry_analyzer import industry_analyzer_node
from agents.risk_analyzer import risk_analyzer_node
from agents.valuation_analyzer import valuation_analyzer_node
from agents.chief_reviewer import chief_reviewer_node

# 定义全局状态（所有Agent共享数据）- 使用 TypedDict 消除警告
class AgentState(TypedDict):
    stock_code: str
    basic_info: Optional[Dict]
    capital_data: Optional[Dict]
    fundamental_data: Optional[Dict]
    tech_data: Optional[list]
    capital_report: Optional[str]
    fund_report: Optional[str]
    industry_report: Optional[str]
    risk_report: Optional[str]
    tech_report: Optional[str]
    valuation_report: Optional[str]
    final_report: Optional[str]
    reports: Optional[Dict]

def create_workflow():
    workflow = StateGraph(AgentState)

    # 1. 添加所有节点
    workflow.add_node("data_fetcher", data_fetcher_node)
    workflow.add_node("tech_analyzer", tech_analyzer_node)
    workflow.add_node("fund_analyzer", fund_analyzer_node)
    workflow.add_node("capital_analyzer", capital_analyzer_node)
    workflow.add_node("industry_analyzer", industry_analyzer_node)
    workflow.add_node("risk_analyzer", risk_analyzer_node)
    workflow.add_node("valuation_analyzer", valuation_analyzer_node)
    workflow.add_node("chief_reviewer", chief_reviewer_node)

    # 2. 设置入口：先拉数据
    workflow.set_entry_point("data_fetcher")

    # 3. 拉完数据 → 并行执行所有分析Agent
    analysis_agents = [
        "tech_analyzer",
        "fund_analyzer",
        "capital_analyzer",
        "industry_analyzer",
        "risk_analyzer",
        "valuation_analyzer"
    ]
    for agent in analysis_agents:
        workflow.add_edge("data_fetcher", agent)

    # 4. 所有分析完成 → 交给首席总结
    for agent in analysis_agents:
        workflow.add_edge(agent, "chief_reviewer")

    # 5. 总结完成 → 结束
    workflow.add_edge("chief_reviewer", END)

    # 编译流程
    return workflow.compile()

# 全局单例运行
app = create_workflow()

def run_workflow(stock_code: str = "600519"):
    """
    运行 LangGraph 工作流模式
    """
    print("\n" + "="*80)
    print("[LangGraph] 工作流模式启动")
    print("="*80)
    
    initial_state = {"stock_code": stock_code}
    result = app.invoke(initial_state)
    
    print("\n" + "="*80)
    print("[OK] 工作流分析完成！")
    print("="*80)
    
    if "final_report" in result:
        print("\n--- 最终整合报告 ---")
        print(result["final_report"])
    
    if "reports" in result:
        print("\n--- 各维度报告 ---")
        reports = result["reports"]
        if "capital" in reports:
            print("\n【资金面】")
            print(reports["capital"])
        if "fundamental" in reports:
            print("\n【基本面】")
            print(reports["fundamental"])
        if "industry" in reports:
            print("\n【行业面】")
            print(reports["industry"])
        if "risk" in reports:
            print("\n【风险面】")
            print(reports["risk"])
        if "technical" in reports:
            print("\n【技术面】")
            print(reports["technical"])
        if "valuation" in reports:
            print("\n【估值面】")
            print(reports["valuation"])
    
    return result

if __name__ == "__main__":
    import sys
    stock_code = "600519"
    if len(sys.argv) > 1:
        stock_code = sys.argv[1]
    run_workflow(stock_code)