"""Agent 标准化校验 — pytest 回归 (离线, 仅签名/属性检查, 不调 LLM)

运行：pytest tests/test_agent_standardization.py
"""
import inspect
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from layers.agents.data_agent import DataAgent, data_agent_node
from layers.agents.tech_agent import TechAgent, tech_agent_node
from layers.agents.fund_agent import FundAgent, fund_agent_node
from layers.agents.capital_agent import CapitalAgent, capital_agent_node
from layers.agents.industry_agent import IndustryAgent, industry_agent_node
from layers.agents.risk_agent import RiskAgent, risk_agent_node
from layers.agents.valuation_agent import ValuationAgent, valuation_agent_node
from layers.agents.chief_agent import ChiefAgent, chief_agent_node
from layers.agents import (
    data_agent, tech_agent, fund_agent, capital_agent,
    industry_agent, risk_agent, valuation_agent, chief_agent,
)


def test_agent_imports_and_aliases():
    for name, alias in {
        "data_agent": data_agent, "tech_agent": tech_agent, "fund_agent": fund_agent,
        "capital_agent": capital_agent, "industry_agent": industry_agent,
        "risk_agent": risk_agent, "valuation_agent": valuation_agent, "chief_agent": chief_agent,
    }.items():
        assert alias is not None, f"别名缺失 {name}"


def test_init_signature_unified():
    agents = {
        "DataAgent": DataAgent, "TechAgent": TechAgent, "FundAgent": FundAgent,
        "CapitalAgent": CapitalAgent, "IndustryAgent": IndustryAgent,
        "RiskAgent": RiskAgent, "ValuationAgent": ValuationAgent, "ChiefAgent": ChiefAgent,
    }
    for name, cls in agents.items():
        params = list(inspect.signature(cls.__init__).parameters.keys())
        assert "model_name" in params, f"{name} 缺少 model_name 参数"
        assert "data_connector" in params, f"{name} 缺少 data_connector 参数"


def test_analyze_signature_unified():
    agents = {
        "DataAgent": DataAgent, "TechAgent": TechAgent, "FundAgent": FundAgent,
        "CapitalAgent": CapitalAgent, "IndustryAgent": IndustryAgent,
        "RiskAgent": RiskAgent, "ValuationAgent": ValuationAgent, "ChiefAgent": ChiefAgent,
    }
    for name, cls in agents.items():
        params = list(inspect.signature(cls.analyze).parameters.keys())
        assert "stock_code" in params, f"{name}.analyze 缺少 stock_code 参数"
        assert "state" in params, f"{name}.analyze 缺少 state 参数"


def test_node_functions_have_state():
    nodes = {
        "data_agent_node": data_agent_node, "tech_agent_node": tech_agent_node,
        "fund_agent_node": fund_agent_node, "capital_agent_node": capital_agent_node,
        "industry_agent_node": industry_agent_node, "risk_agent_node": risk_agent_node,
        "valuation_agent_node": valuation_agent_node, "chief_agent_node": chief_agent_node,
    }
    for name, func in nodes.items():
        params = list(inspect.signature(func).parameters.keys())
        assert "state" in params, f"{name} 缺少 state 参数"


def test_report_max_tokens_unified():
    # 各分析 Agent 通过类属性 MAX_TOKENS 统一约束单次报告 token 上限
    from layers.agents.tech_agent import TechAgent
    from layers.agents.fund_agent import FundAgent
    from layers.agents.capital_agent import CapitalAgent
    from layers.agents.industry_agent import IndustryAgent
    from layers.agents.risk_agent import RiskAgent
    from layers.agents.valuation_agent import ValuationAgent
    for name, cls in {
        "TechAgent": TechAgent, "FundAgent": FundAgent, "CapitalAgent": CapitalAgent,
        "IndustryAgent": IndustryAgent, "RiskAgent": RiskAgent, "ValuationAgent": ValuationAgent,
    }.items():
        assert hasattr(cls, "MAX_TOKENS"), f"{name} 缺少 MAX_TOKENS 类属性"
        val = cls.MAX_TOKENS
        assert isinstance(val, int) and 800 <= val <= 3000, f"{name}.MAX_TOKENS={val} 超出预期区间"
    # 首席 Agent 单独以 REPORT_MAX_TOKENS 约束综合报告上限
    from layers.agents.chief_agent import REPORT_MAX_TOKENS
    assert isinstance(REPORT_MAX_TOKENS, int) and REPORT_MAX_TOKENS > 0
