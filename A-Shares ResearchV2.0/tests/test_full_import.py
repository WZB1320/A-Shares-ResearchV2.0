"""全量模块导入完整性验证 (pytest 版)

运行：pytest tests/test_full_import.py
graph 包已废弃删除，不再校验。
"""
import importlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def test_all_module_imports():
    mods = [
        "layers.skills.tech_skill", "layers.skills.fund_skill",
        "layers.skills.capital_skill", "layers.skills.industry_skill",
        "layers.skills.valuation_skill", "layers.skills.risk_skill",
        "layers.connectors.data_connector", "layers.connectors.tool_connector",
        "layers.agents.data_agent", "layers.agents.tech_agent", "layers.agents.fund_agent",
        "layers.agents.capital_agent", "layers.agents.industry_agent",
        "layers.agents.risk_agent", "layers.agents.valuation_agent",
        "layers.agents.chief_agent",
        "layers.validators", "layers.backtest.engine",
        "config.env_config", "config.llm_config",
        "harness.scheduler", "harness.state", "harness.validator",
    ]
    for m in mods:
        assert importlib.import_module(m) is not None, f"无法导入 {m}"

    # top-level layers 聚合导出
    layers = importlib.import_module("layers")
    for name in ["TechSkill", "FundSkill", "DataAgent", "ChiefAgent", "DataValidator"]:
        assert hasattr(layers, name), f"layers 缺少聚合导出 {name}"
