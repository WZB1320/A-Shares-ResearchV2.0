"""全量模块导入完整性验证"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

tests_passed = 0
tests_failed = 0

def check(name):
    global tests_passed, tests_failed
    tests_passed += 1
    print(f"  [OK] {name}")

def check_condition(label, cond):
    global tests_passed, tests_failed
    if cond:
        tests_passed += 1
        print(f"  [OK] {label}")
    else:
        tests_failed += 1
        print(f"  [FAIL] {label}")


print("=" * 60)
print("  全量模块导入完整性验证")
print("=" * 60)

print("\n--- skills ---")
from layers.skills import TechSkill, tech_skill
from layers.skills import FundSkill, fund_skill
from layers.skills import CapitalSkill, capital_skill
from layers.skills import IndustrySkill, industry_skill
from layers.skills import ValuationSkill, valuation_skill
from layers.skills import RiskSkill, risk_skill
check("TechSkill, tech_skill")
check("FundSkill, fund_skill")
check("CapitalSkill, capital_skill")
check("IndustrySkill, industry_skill")
check("ValuationSkill, valuation_skill")
check("RiskSkill, risk_skill")

print("\n--- connectors ---")
from layers.connectors import DataConnector, get_data_connector
from layers.connectors import ToolConnector, get_tool_connector, call_tool
check("DataConnector, get_data_connector")
check("ToolConnector, get_tool_connector, call_tool")

print("\n--- agents ---")
from layers.agents import DataAgent, data_agent, data_agent_node
from layers.agents import TechAgent, tech_agent, tech_agent_node
from layers.agents import FundAgent, fund_agent, fund_agent_node
from layers.agents import CapitalAgent, capital_agent, capital_agent_node
from layers.agents import IndustryAgent, industry_agent, industry_agent_node
from layers.agents import RiskAgent, risk_agent, risk_agent_node
from layers.agents import ValuationAgent, valuation_agent, valuation_agent_node
from layers.agents import ChiefAgent, chief_agent, chief_agent_node
check("DataAgent, data_agent, data_agent_node")
check("TechAgent, tech_agent, tech_agent_node")
check("FundAgent, fund_agent, fund_agent_node")
check("CapitalAgent, capital_agent, capital_agent_node")
check("IndustryAgent, industry_agent, industry_agent_node")
check("RiskAgent, risk_agent, risk_agent_node")
check("ValuationAgent, valuation_agent, valuation_agent_node")
check("ChiefAgent, chief_agent, chief_agent_node")

print("\n--- validators ---")
from layers.validators import DataValidator, QualityReport, DimensionQuality, FieldValidation, validator
check("DataValidator, QualityReport, DimensionQuality, FieldValidation, validator")

print("\n--- backtest ---")
from layers.backtest import (
    BacktestEngine, AnalysisSnapshot, OutcomeData,
    BacktestResult, DimAttribution, BacktestSummary
)
check("BacktestEngine, AnalysisSnapshot, OutcomeData, BacktestResult, DimAttribution, BacktestSummary")

print("\n--- layers (top-level __init__) ---")
from layers import (
    TechSkill, tech_skill,
    FundSkill, fund_skill,
    CapitalSkill, capital_skill,
    IndustrySkill, industry_skill,
    ValuationSkill, valuation_skill,
    RiskSkill, risk_skill,
    DataConnector, get_data_connector,
    ToolConnector, get_tool_connector, call_tool,
    DataAgent, data_agent, data_agent_node,
    TechAgent, tech_agent, tech_agent_node,
    FundAgent, fund_agent, fund_agent_node,
    CapitalAgent, capital_agent, capital_agent_node,
    IndustryAgent, industry_agent, industry_agent_node,
    RiskAgent, risk_agent, risk_agent_node,
    ValuationAgent, valuation_agent, valuation_agent_node,
    ChiefAgent, chief_agent, chief_agent_node,
    DataValidator, QualityReport, DimensionQuality, FieldValidation, validator,
)
check("layers top-level (skills + connectors + agents + validators)")

print("\n--- config ---")
from config import env_config
from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
check("config.env_config")
check("config.llm_config (get_llm_client, get_model_id, DEFAULT_MODEL)")

print("\n--- graph ---")
from graph import workflow
check("graph.workflow")

print("\n--- harness ---")
from harness import scheduler, state, validator as harness_validator
check("harness.scheduler")
check("harness.state")
check("harness.validator")

print(f"\n{'=' * 60}")
print(f"  Results: {tests_passed} passed, {tests_failed} failed")
print(f"{'=' * 60}")

if tests_failed > 0:
    sys.exit(1)