import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

print('=== 1. 导入测试 ===')
from layers.agents.data_agent import DataAgent, data_agent_node
from layers.agents.tech_agent import TechAgent, tech_agent_node
from layers.agents.fund_agent import FundAgent, fund_agent_node
from layers.agents.capital_agent import CapitalAgent, capital_agent_node
from layers.agents.industry_agent import IndustryAgent, industry_agent_node
from layers.agents.risk_agent import RiskAgent, risk_agent_node
from layers.agents.valuation_agent import ValuationAgent, valuation_agent_node
from layers.agents.chief_agent import ChiefAgent, chief_agent_node
print('全部导入成功')

print()
print('=== 2. __init__ 签名统一检查 ===')
import inspect
agents = {
    'DataAgent': DataAgent,
    'TechAgent': TechAgent,
    'FundAgent': FundAgent,
    'CapitalAgent': CapitalAgent,
    'IndustryAgent': IndustryAgent,
    'RiskAgent': RiskAgent,
    'ValuationAgent': ValuationAgent,
    'ChiefAgent': ChiefAgent,
}
for name, cls in agents.items():
    sig = inspect.signature(cls.__init__)
    params = list(sig.parameters.keys())
    print(f'{name}.__init__({", ".join(params)})')
    assert 'model_name' in params, f'{name} 缺少 model_name 参数'
    assert 'data_connector' in params, f'{name} 缺少 data_connector 参数'
print('__init__ signature unified [OK]')

print()
print('=== 3. analyze 方法签名统一检查 ===')
for name, cls in agents.items():
    sig = inspect.signature(cls.analyze)
    params = list(sig.parameters.keys())
    print(f'{name}.analyze({", ".join(params)})')
    assert 'stock_code' in params, f'{name}.analyze 缺少 stock_code 参数'
    assert 'state' in params, f'{name}.analyze 缺少 state 参数'
print('analyze signature unified [OK]')

print()
print('=== 4. 返回类型注解检查 ===')
for name, cls in agents.items():
    sig = inspect.signature(cls.analyze)
    ret = sig.return_annotation
    print(f'{name}.analyze 返回类型: {ret}')
print('return type check done [OK]')

print()
print('=== 5. 模块别名检查 ===')
from layers.agents import data_agent, tech_agent, fund_agent, capital_agent
from layers.agents import industry_agent, risk_agent, valuation_agent, chief_agent
aliases = {
    'data_agent': data_agent,
    'tech_agent': tech_agent,
    'fund_agent': fund_agent,
    'capital_agent': capital_agent,
    'industry_agent': industry_agent,
    'risk_agent': risk_agent,
    'valuation_agent': valuation_agent,
    'chief_agent': chief_agent,
}
for name, alias in aliases.items():
    print(f'{name} = {alias.__name__}')
print('module alias check [OK]')

print()
print('=== 6. node 函数检查 ===')
nodes = {
    'data_agent_node': data_agent_node,
    'tech_agent_node': tech_agent_node,
    'fund_agent_node': fund_agent_node,
    'capital_agent_node': capital_agent_node,
    'industry_agent_node': industry_agent_node,
    'risk_agent_node': risk_agent_node,
    'valuation_agent_node': valuation_agent_node,
    'chief_agent_node': chief_agent_node,
}
for name, func in nodes.items():
    sig = inspect.signature(func)
    params = list(sig.parameters.keys())
    print(f'{name}({", ".join(params)})')
    assert 'state' in params, f'{name} 缺少 state 参数'
print('node function check [OK]')

print()
print('=== 7. REPORT_MAX_TOKENS 统一检查 ===')
from layers.agents.tech_agent import REPORT_MAX_TOKENS as tech_tokens
from layers.agents.fund_agent import REPORT_MAX_TOKENS as fund_tokens
from layers.agents.capital_agent import REPORT_MAX_TOKENS as capital_tokens
from layers.agents.industry_agent import REPORT_MAX_TOKENS as industry_tokens
from layers.agents.risk_agent import REPORT_MAX_TOKENS as risk_tokens
from layers.agents.valuation_agent import REPORT_MAX_TOKENS as valuation_tokens
from layers.agents.chief_agent import REPORT_MAX_TOKENS as chief_tokens

analysis_tokens = {
    'tech': tech_tokens,
    'fund': fund_tokens,
    'capital': capital_tokens,
    'industry': industry_tokens,
    'risk': risk_tokens,
    'valuation': valuation_tokens,
}
for name, val in analysis_tokens.items():
    print(f'{name}: {val}')
    assert val == 1800, f'{name} REPORT_MAX_TOKENS={val}, 期望1800'
print(f'chief: {chief_tokens} (汇总Agent允许3000)')
print('REPORT_MAX_TOKENS unified [OK]')

print()
print('=== ALL CHECKS PASSED! Standardization complete ===')
