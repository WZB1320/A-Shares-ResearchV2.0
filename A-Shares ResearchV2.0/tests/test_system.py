import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
import traceback

passed = 0
failed = 0
errors = []

def check(name, fn):
    global passed, failed
    try:
        fn()
        passed += 1
        print(f"  [OK] {name}")
    except Exception as e:
        failed += 1
        errors.append(f"{name}: {e}")
        print(f"  [FAIL] {name}: {e}")

print("=" * 60)
print("  A-Shares-Research V3.0 Harness - Full System Verification")
print("=" * 60)

# Module-level imports
print("\n--- 1. Config layer ---")
check("config.env_config", lambda: __import__("config.env_config"))
check("config.llm_config", lambda: __import__("config.llm_config"))

print("\n--- 2. Graph (workflow) layer ---")
check("graph.workflow", lambda: __import__("graph.workflow"))

print("\n--- 3. Harness layer ---")
check("harness.scheduler", lambda: __import__("harness.scheduler"))
check("harness.state", lambda: __import__("harness.state"))
check("harness.validator", lambda: __import__("harness.validator"))

print("\n--- 4. Layered connectors ---")
check("connectors.data_connector", lambda: __import__("layers.connectors.data_connector"))
check("connectors.tool_connector", lambda: __import__("layers.connectors.tool_connector"))

print("\n--- 5. Skills (all 6 dimensions) ---")
check("tech_skill", lambda: __import__("layers.skills.tech_skill"))
check("fund_skill", lambda: __import__("layers.skills.fund_skill"))
check("capital_skill", lambda: __import__("layers.skills.capital_skill"))
check("industry_skill", lambda: __import__("layers.skills.industry_skill"))
check("risk_skill", lambda: __import__("layers.skills.risk_skill"))
check("valuation_skill", lambda: __import__("layers.skills.valuation_skill"))

print("\n--- 6. Agents (all 7 agents + report_schema) ---")
check("report_schema", lambda: __import__("layers.agents.report_schema"))
check("tech_agent", lambda: __import__("layers.agents.tech_agent"))
check("fund_agent", lambda: __import__("layers.agents.fund_agent"))
check("capital_agent", lambda: __import__("layers.agents.capital_agent"))
check("industry_agent", lambda: __import__("layers.agents.industry_agent"))
check("risk_agent", lambda: __import__("layers.agents.risk_agent"))
check("valuation_agent", lambda: __import__("layers.agents.valuation_agent"))
check("chief_agent", lambda: __import__("layers.agents.chief_agent"))
check("data_agent", lambda: __import__("layers.agents.data_agent"))

print("\n--- 7. Top-level (app, main) ---")
check("app module", lambda: __import__("app"))
check("main module", lambda: __import__("main"))

# Instance creation tests
print("\n--- 8. LLM Client ---")
from config.llm_config import get_llm_client, get_model_id
check("LLM client creation", lambda: get_llm_client("deepseek"))
check("LLM model_id", lambda: get_model_id("deepseek"))

print("\n--- 9. Agent Instantiation ---")
from layers.agents.tech_agent import TechAgent
from layers.agents.fund_agent import FundAgent
from layers.agents.capital_agent import CapitalAgent
from layers.agents.industry_agent import IndustryAgent
from layers.agents.risk_agent import RiskAgent
from layers.agents.valuation_agent import ValuationAgent
from layers.agents.chief_agent import ChiefAgent

check("TechAgent instantiate", lambda: TechAgent("deepseek"))
check("FundAgent instantiate", lambda: FundAgent("deepseek"))
check("CapitalAgent instantiate", lambda: CapitalAgent("deepseek"))
check("IndustryAgent instantiate", lambda: IndustryAgent("deepseek"))
check("RiskAgent instantiate", lambda: RiskAgent("deepseek"))
check("ValuationAgent instantiate", lambda: ValuationAgent("deepseek"))
check("ChiefAgent instantiate", lambda: ChiefAgent("deepseek"))

print("\n--- 10. AgentReport Data Structure ---")
from layers.agents.report_schema import (
    AgentReport, parse_json_report, aggregate_reports,
    reports_to_markdown, error_report, unavailable_report
)

check("AgentReport create+to_dict+from_dict", lambda: (
    AgentReport.from_dict(
        AgentReport("tech", 72, "kan duo", 75, "good",
                   ["s1"], ["r1"], "buy").to_dict()
    )
))

check("parse_json valid", lambda: parse_json_report(
    '{"dimension":"tech","overall_score":72,"grade":"kan duo","confidence":75,'
    '"thesis":"t","key_signals":["s1"],"risk_factors":["r1"],"recommendation":"buy"}',
    "tech"
))

check("aggregate 3-dim reports", lambda: aggregate_reports({
    "tech": AgentReport("tech", 72, "kan duo", 75, "g", ["s1"], ["r1"], "b"),
    "fund": AgentReport("fund", 65, "zhong xing", 70, "o", ["s2"], ["r2"], "h"),
    "risk": AgentReport("risk", 40, "kan kong", 60, "b", ["s3"], ["r3"], "s"),
}))

check("reports_to_markdown", lambda: reports_to_markdown({
    "tech": AgentReport("tech", 72, "kan duo", 75, "g", ["s1"], ["r1"], "b"),
}))

check("error_report", lambda: error_report("tech", "test"))
check("unavailable_report", lambda: unavailable_report("fund"))

def _check(val, expected):
    assert val == expected, f"got {val}, expected {expected}"

print("\n--- 11. ValuationSkill Industry Classification ---")
from layers.skills.valuation_skill import ValuationSkill
check("classify - baijiu", lambda: _check(ValuationSkill.classify_industry("\u767d\u9152")[0], "stable"))
check("classify - quan shang", lambda: _check(ValuationSkill.classify_industry("\u8bc1\u5238")[0], "cyclical"))
check("classify - bank", lambda: _check(ValuationSkill.classify_industry("\u94f6\u884c")[0], "financial"))
check("classify - chip", lambda: _check(ValuationSkill.classify_industry("\u82af\u7247")[0], "growth"))
check("classify - unknown", lambda: _check(ValuationSkill.classify_industry("XYZ-unknown-sector")[0], "default"))

print("\n--- 12. Skill Analyzers (dry run with mock data) ---")
mock_tech = {"ma_system": {}}
mock_fund = {"basic_info": {"hang ye": "bai jiu"}}
mock_val = {"price": 25.5, "pe_ttm": 15.2, "pb": 2.1,
            "pe_history": [10,12,14,18,22,25,30,28,20,15,12],
            "pb_history": [1.0,1.2,1.5,2.0,2.5,3.0,2.8,2.2,1.8,1.5,1.2]}

from layers.skills.valuation_skill import valuation_skill
check("ValuationSkill.analyze", lambda: valuation_skill.analyze(mock_val, mock_fund))

check("IndustrySkill.analyze", lambda: (
    __import__("layers.skills.industry_skill").industry_skill.analyze(mock_fund)
))

# Final results
print("\n" + "=" * 60)
print(f"  Results: {passed} passed, {failed} failed")
if errors:
    print("  Errors:")
    for e in errors:
        print(f"    - {e}")
print("=" * 60)
sys.exit(0 if failed == 0 else 1)