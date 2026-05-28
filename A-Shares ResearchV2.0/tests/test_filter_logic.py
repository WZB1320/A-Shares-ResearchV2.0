"""测试研报筛选逻辑"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from layers.memory.report_fetcher import _is_depth_report, INSTITUTION_WHITELIST

print("=== 标题筛选 ===")
tests = [
    ("深度研究：AI云业务拐点已至", True),
    ("首次覆盖：某某公司投资价值分析", True),
    ("专题研究：半导体行业", True),
    ("公司深度：小米汽车产业链", True),
    ("晨会纪要2025-05-20", False),
    ("日报：市场点评", False),
    ("事件点评：回购公告", False),
    ("快讯：某某公司Q1业绩预告", False),
    ("2025年报及2026一季报点评：收入利润增速均回正", False),
    ("跟踪深度：BABA回购加码信号积极", True),
]
all_ok = True
for title, expected in tests:
    result = _is_depth_report(title)
    status = "OK" if result == expected else "FAIL"
    if result != expected:
        all_ok = False
    print(f"  [{status}] 《{title[:35]}》→ {result} (expected {expected})")

print(f"\n=== 机构白名单 ===")
print(f"  共 {len(INSTITUTION_WHITELIST)} 家券商")
print(f"  抽查: {'中金公司' in INSTITUTION_WHITELIST}, {'中信证券' in INSTITUTION_WHITELIST}, {'野村证券' in INSTITUTION_WHITELIST}")

print(f"\n{'全部通过' if all_ok else '有失败项'}")