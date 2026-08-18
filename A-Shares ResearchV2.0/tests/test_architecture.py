"""三层架构模块导入完整性验证 (pytest 版)

运行：pytest tests/test_architecture.py
仅验证“能否导入/实例化”，不触发网络/LLM。graph.workflow 已废弃删除，不再校验。
"""
import importlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def _imp(name: str):
    return importlib.import_module(name)


def test_architecture_imports():
    # Skills (6 个维度)
    for m in [
        "layers.skills.tech_skill", "layers.skills.fund_skill",
        "layers.skills.capital_skill", "layers.skills.industry_skill",
        "layers.skills.valuation_skill", "layers.skills.risk_skill",
    ]:
        assert _imp(m) is not None, f"无法导入 {m}"

    # Connectors (顶层 import pandas/requests，已在托管环境安装)
    for m in [
        "layers.connectors.data_connector", "layers.connectors.tool_connector",
        "layers.connectors.market_data_sources",
    ]:
        assert _imp(m) is not None, f"无法导入 {m}"

    # Agents (7 个 + report_schema)
    for m in [
        "layers.agents.tech_agent", "layers.agents.fund_agent",
        "layers.agents.capital_agent", "layers.agents.industry_agent",
        "layers.agents.risk_agent", "layers.agents.valuation_agent",
        "layers.agents.chief_agent", "layers.agents.data_agent",
        "layers.agents.report_schema",
    ]:
        assert _imp(m) is not None, f"无法导入 {m}"

    # Harness
    assert _imp("harness.scheduler") is not None
    assert _imp("harness.state") is not None
    assert _imp("harness.validator") is not None

    # Top-level 聚合包
    assert _imp("layers") is not None
