"""knowledge_base + report_fetcher 离线筛选/存取 — pytest 回归 (离线, 本地 SQLite)

运行：pytest tests/test_memory.py
注意：会向本地知识库 DB 写入 BABA 测试数据, 测试结束清理。
"""
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from layers.memory.knowledge_base import (
    init_kb, save_snapshot, get_latest_snapshot, build_tracking_context,
    save_report, get_reports_for_stock, get_report_consensus, get_report_count,
)
from db import init_db

DATA_DIR = Path(__file__).parent.parent / "data"
KB_DB = DATA_DIR / "knowledge" / "stock_memory.db"
HARNESS_DB = DATA_DIR / "harness.db"
TEST_CODE = "BABA"


def _delete_from(db, tables, code):
    if not db.exists():
        return
    c = sqlite3.connect(str(db))
    try:
        for t in tables:
            try:
                c.execute(f"DELETE FROM {t} WHERE stock_code = ?", (code,))
            except sqlite3.OperationalError:
                # 表尚未创建（如 analysis_records 依赖 db.init_db）
                pass
        c.commit()
    finally:
        c.close()


def _cleanup():
    _delete_from(KB_DB, ["research_reports", "stock_timeline"], TEST_CODE)
    _delete_from(HARNESS_DB, ["analysis_records"], TEST_CODE)


def test_kb_offline_save_read():
    try:
        init_kb()
        # save_snapshot 已委托到 db.analysis_records（harness.db），需先建表
        init_db()
        conn = sqlite3.connect(str(KB_DB))
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        conn.close()
        assert {"research_reports", "stock_timeline"}.issubset({t[0] for t in tables}), "知识库表缺失"

        # 快照
        save_snapshot(
            stock_code=TEST_CODE,
            overall_score=45, overall_grade="中性",
            dimension_scores={"tech": 38, "fund": 45, "capital": 50, "industry": 40, "risk": 38, "valuation": 55},
            key_conclusion="BABA当前估值合理但增长放缓，AI云业务是核心变量",
            top_signals=["ROE 10.22%", "毛利率39.81%", "MACD死叉"],
            top_risks=["政策风险", "竞争加剧", "增长放缓"],
            price_at_analysis=129.47,
            full_report_md="# BABA 深度研报\n\n## 摘要\n...",
        )
        snap = get_latest_snapshot(TEST_CODE)
        assert snap is not None
        assert snap["overall_score"] == 45
        assert snap["overall_grade"] == "中性"
        assert snap["dimension_scores"]["tech"] == 38

        # 跟踪上下文
        ctx = build_tracking_context(TEST_CODE)
        assert "BABA" in ctx or "中性" in ctx
        ctx_new = build_tracking_context("NOT_EXISTS")
        assert "首次分析" in ctx_new

        # 研报
        save_report(
            stock_code=TEST_CODE,
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
            stock_code=TEST_CODE,
            title="回购加码信号积极，估值安全边际充足",
            institution="中信证券",
            pub_date="2025-04-15",
            rating="增持",
            report_type="深度研究",
            page_count=12,
            quality_score=80,
        )
        reports = get_reports_for_stock(TEST_CODE)
        assert len(reports) >= 2
        consensus = get_report_consensus(TEST_CODE)
        assert "中金公司" in consensus
        assert get_report_count(TEST_CODE) >= 2
    finally:
        _cleanup()
