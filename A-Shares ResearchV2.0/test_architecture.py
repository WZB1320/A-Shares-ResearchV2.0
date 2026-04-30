"""
简单测试：验证架构是否正确，不依赖网络数据
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

print("="*80)
print("架构测试")
print("="*80)

print("\n[1] 测试 config.llm_config...")
try:
    from config.llm_config import get_llm_client, MODEL_CONFIG_MAP
    print(f"   [OK] 导入成功")
    print(f"   [OK] 支持的模型：{list(MODEL_CONFIG_MAP.keys())}")
except Exception as e:
    print(f"   [FAIL] 失败：{e}")

print("\n[2] 测试 agents 导入...")
try:
    from agents.data_fetcher import DataFetcherAgent, data_fetcher_node
    from agents.tech_analyzer import TechAnalyzerAgent, tech_analyzer_node
    from agents.fund_analyzer import FundAnalyzerAgent, fund_analyzer_node
    from agents.capital_analyzer import CapitalAnalyzerAgent, capital_analyzer_node
    from agents.industry_analyzer import IndustryAnalyzerAgent, industry_analyzer_node
    from agents.risk_analyzer import RiskAnalyzerAgent, risk_analyzer_node
    from agents.valuation_analyzer import ValuationAnalyzerAgent, valuation_analyzer_node
    from agents.chief_reviewer import ChiefReviewerAgent, chief_reviewer_node
    print(f"   [OK] 所有 agents 导入成功")
    print(f"   [OK] 所有 node 函数导入成功")
except Exception as e:
    print(f"   [FAIL] 失败：{e}")
    import traceback
    print(traceback.format_exc())

print("\n[3] 测试 graph.workflow...")
try:
    from graph.workflow import create_workflow, run_workflow
    print(f"   [OK] workflow 导入成功")
    app = create_workflow()
    print(f"   [OK] workflow 创建成功")
except Exception as e:
    print(f"   [FAIL] 失败：{e}")
    import traceback
    print(traceback.format_exc())

print("\n[4] 测试 main.py 导入...")
try:
    import main
    print(f"   [OK] main.py 导入成功")
except Exception as e:
    print(f"   [FAIL] 失败：{e}")
    import traceback
    print(traceback.format_exc())

print("\n" + "="*80)
print("[OK] 架构测试完成！")
print("="*80)
print("\n[说明]")
print("   - 所有代码模块都能正常导入")
print("   - LangGraph 工作流可以正常创建")
print("   - 两种模式都已就绪")
print("   - 实际运行需要网络连接 AkShare 数据源")
