"""全量模块导入、实例化与数据结构冒烟 (pytest 版，离线)

运行：pytest tests/test_system.py
graph.workflow 已废弃删除，不再校验。
"""
import importlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def _imp(name: str):
    return importlib.import_module(name)


def test_system_imports_and_instantiation():
    # Config / LLM client (仅构造，不联网)
    from config.llm_config import get_llm_client, get_model_id
    assert get_llm_client("deepseek") is not None
    assert get_model_id("deepseek")

    # Skills 导入
    for m in [
        "layers.skills.tech_skill", "layers.skills.fund_skill",
        "layers.skills.capital_skill", "layers.skills.industry_skill",
        "layers.skills.valuation_skill", "layers.skills.risk_skill",
    ]:
        assert _imp(m) is not None, f"无法导入 {m}"

    # Agents 导入 + 类存在
    agents = [
        "tech_agent", "fund_agent", "capital_agent", "industry_agent",
        "risk_agent", "valuation_agent", "chief_agent",
    ]
    for a in agents:
        mod = _imp(f"layers.agents.{a}")
        assert hasattr(mod, a) and callable(getattr(mod, a)), f"{a} 缺少类"

    # Agents 实例化 (不触发 LLM)
    from layers.agents.tech_agent import TechAgent
    from layers.agents.fund_agent import FundAgent
    from layers.agents.capital_agent import CapitalAgent
    from layers.agents.industry_agent import IndustryAgent
    from layers.agents.risk_agent import RiskAgent
    from layers.agents.valuation_agent import ValuationAgent
    from layers.agents.chief_agent import ChiefAgent

    for Cls in (TechAgent, FundAgent, CapitalAgent, IndustryAgent,
                RiskAgent, ValuationAgent, ChiefAgent):
        inst = Cls("deepseek")
        assert hasattr(inst, "analyze"), f"{Cls.__name__} 缺少 analyze 方法"

    # report_schema 数据结构
    from layers.agents.report_schema import (
        AgentReport, parse_json_report, aggregate_reports,
        reports_to_markdown, error_report, unavailable_report,
    )
    r = AgentReport("tech", 72, "kan duo", 75, "good", ["s1"], ["r1"], "buy")
    assert AgentReport.from_dict(r.to_dict()).dimension == "tech"
    assert parse_json_report(
        '{"dimension":"tech","overall_score":72,"grade":"kan duo","confidence":75,'
        '"thesis":"t","key_signals":["s1"],"risk_factors":["r1"],"recommendation":"buy"}',
        "tech",
    ) is not None
    assert aggregate_reports({"tech": r}) is not None
    assert reports_to_markdown({"tech": r})
    assert error_report("tech", "test")
    assert unavailable_report("fund")

    # ValuationSkill 行业分类
    from layers.skills.valuation_skill import ValuationSkill
    assert ValuationSkill.classify_industry("白酒")[0] == "stable"
    assert ValuationSkill.classify_industry("证券")[0] == "cyclical"
    assert ValuationSkill.classify_industry("银行")[0] == "financial"
    assert ValuationSkill.classify_industry("芯片")[0] == "growth"
    assert ValuationSkill.classify_industry("XYZ-unknown-sector")[0] == "default"

    # Skill 干跑 (mock 数据, 不调 LLM)
    from layers.skills.valuation_skill import valuation_skill
    from layers.skills.industry_skill import industry_skill
    mock_fund = {"basic_info": {"行业": "白酒"}}
    mock_val = {
        "price": 25.5, "pe_ttm": 15.2, "pb": 2.1,
        "pe_history": [10, 12, 14, 18, 22, 25, 30, 28, 20, 15, 12],
        "pb_history": [1.0, 1.2, 1.5, 2.0, 2.5, 3.0, 2.8, 2.2, 1.8, 1.5, 1.2],
    }
    assert valuation_skill.analyze(mock_val, mock_fund) is not None
    assert industry_skill.analyze(mock_fund) is not None
