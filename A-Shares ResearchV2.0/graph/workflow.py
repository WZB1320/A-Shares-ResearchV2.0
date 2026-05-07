from langgraph.graph import StateGraph, END
from typing import Dict, TypedDict, Optional

from layers.agents.data_agent import data_agent_node
from layers.agents.tech_agent import tech_agent_node
from layers.agents.fund_agent import fund_agent_node
from layers.agents.capital_agent import capital_agent_node
from layers.agents.industry_agent import industry_agent_node
from layers.agents.risk_agent import risk_agent_node
from layers.agents.valuation_agent import valuation_agent_node
from layers.agents.chief_agent import chief_agent_node


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

    workflow.add_node("data_agent", data_agent_node)
    workflow.add_node("tech_agent", tech_agent_node)
    workflow.add_node("fund_agent", fund_agent_node)
    workflow.add_node("capital_agent", capital_agent_node)
    workflow.add_node("industry_agent", industry_agent_node)
    workflow.add_node("risk_agent", risk_agent_node)
    workflow.add_node("valuation_agent", valuation_agent_node)
    workflow.add_node("chief_agent", chief_agent_node)

    workflow.set_entry_point("data_agent")

    analysis_agents = [
        "tech_agent",
        "fund_agent",
        "capital_agent",
        "industry_agent",
        "risk_agent",
        "valuation_agent",
    ]

    for agent in analysis_agents:
        workflow.add_edge("data_agent", agent)

    for agent in analysis_agents:
        workflow.add_edge(agent, "chief_agent")

    workflow.add_edge("chief_agent", END)

    return workflow.compile()


def run_workflow(stock_code: str) -> Dict:
    app = create_workflow()

    initial_state = {
        "stock_code": stock_code,
        "basic_info": None,
        "capital_data": None,
        "fundamental_data": None,
        "tech_data": None,
        "capital_report": None,
        "fund_report": None,
        "industry_report": None,
        "risk_report": None,
        "tech_report": None,
        "valuation_report": None,
        "final_report": None,
        "reports": None
    }

    result = app.invoke(initial_state)
    return result


if __name__ == "__main__":
    result = run_workflow("600519")
    print("\n" + "="*80)
    print("LangGraph Workflow Result")
    print("="*80)
    print("\n[Final Report]")
    print(result.get("final_report", "No report generated"))
