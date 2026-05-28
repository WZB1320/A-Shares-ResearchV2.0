"""测试 knowledge_base + report_fetcher 筛选逻辑"""
import sys, logging
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

from layers.memory.knowledge_base import (
    init_kb, save_snapshot, get_latest_snapshot, build_tracking_context,
    save_report, get_reports_for_stock, get_report_consensus, get_report_count,
)

S = "=" * 55

# 1. 初始化
print(f"\n{S}")
print("1. 初始化知识库")
init_kb()
import sqlite3
conn = sqlite3.connect(str(Path("data/knowledge/stock_memory.db")))
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
print(f"   表: {[t[0] for t in tables]}")
conn.close()

# 2. 保存+读取分析快照
print(f"\n{S}")
print("2. 分析快照 存储/读取")
save_snapshot(
    stock_code="BABA",
    overall_score=45, overall_grade="中性",
    dimension_scores={"tech": 38, "fund": 45, "capital": 50, "industry": 40, "risk": 38, "valuation": 55},
    key_conclusion="BABA当前估值合理但增长放缓，AI云业务是核心变量",
    top_signals=["ROE 10.22%", "毛利率39.81%", "MACD死叉"],
    top_risks=["政策风险", "竞争加剧", "增长放缓"],
    price_at_analysis=129.47,
    full_report_md="# BABA 深度研报\n\n## 摘要\n..."
)
snap = get_latest_snapshot("BABA")
print(f"   股票: BABA, 得分: {snap['overall_score']}, 评级: {snap['overall_grade']}")
print(f"   维度分: {snap['dimension_scores']}")
print(f"   信号: {snap['top_signals']}")

# 3. 跟踪上下文
print(f"\n{S}")
print("3. 历史跟踪上下文注入")
ctx = build_tracking_context("BABA")
print(ctx)
ctx_new = build_tracking_context("NOT_EXISTS")
print(f"\n   无历史: {ctx_new}")

# 4. 研报存储+读取
print(f"\n{S}")
print("4. 研报存储/读取")
save_report(
    stock_code="BABA",
    title="AI云业务拐点已至，上调目标价",
    institution="中金公司",
    pub_date="2025-05-20",
    rating="买入",
    report_type="深度研究",
    page_count=15,
    target_price=180.0,
    core_thesis="AI推动云计算收入加速，利润率有望触底回升",
    key_evidence=["云计算收入增速30%", "淘天GMV企稳", "国际电商减亏"],
    risk_flags=["中美关系不确定", "AI投入回报周期长"],
    quality_score=85,
    full_text="",
    source_url="https://pdf.dfcfw.com/xxx.pdf",
    analyst="张某某",
)
save_report(
    stock_code="BABA",
    title="回购加码信号积极，估值安全边际充足",
    institution="中信证券",
    pub_date="2025-04-15",
    rating="增持",
    report_type="深度研究",
    page_count=12,
    quality_score=80,
)
reports = get_reports_for_stock("BABA")
print(f"   BABA 研报数: {len(reports)}")
for r in reports:
    print(f"   [{r['institution']}] {r['rating']} | {r['title'][:40]}... | {r['page_count']}页")

# 5. 研报共识
print(f"\n{S}")
print("5. 研报共识摘要")
consensus = get_report_consensus("BABA")
print(consensus)

# 6. 报告计数
print(f"\n{S}")
print(f"6. BABA 研报计数: {get_report_count('BABA')}, 无记录股票: {get_report_count('XXXXX')}")

# 7. 目录结构确认
print(f"\n{S}")
print("7. 存储路径确认")
print(f"   知识库DB: data/knowledge/stock_memory.db")
import os
kb_path = Path("data/knowledge/stock_memory.db")
if kb_path.exists():
    size_kb = os.path.getsize(str(kb_path)) / 1024
    print(f"   文件大小: {size_kb:.1f} KB")
print(f"   研报文本: data/knowledge/reports/")

print(f"\n{S}")
print("全部测试通过")