"""知识库 + 研报管理 — 持久化层"""
import sqlite3
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger("KnowledgeBase")

DATA_DIR = Path(__file__).parent.parent.parent / "data" / "knowledge"
KB_PATH = DATA_DIR / "stock_memory.db"


def _get_conn() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(KB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_kb():
    conn = _get_conn()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS analysis_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                analysis_time TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
                overall_score INTEGER DEFAULT 50,
                overall_grade TEXT DEFAULT '',
                dimension_scores TEXT DEFAULT '{}',
                key_conclusion TEXT DEFAULT '',
                top_signals TEXT DEFAULT '[]',
                top_risks TEXT DEFAULT '[]',
                price_at_analysis REAL,
                full_report_md TEXT DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_snapshot_stock_time
                ON analysis_snapshots(stock_code, analysis_time DESC);

            CREATE TABLE IF NOT EXISTS research_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                title TEXT DEFAULT '',
                institution TEXT DEFAULT '',
                analyst TEXT DEFAULT '',
                pub_date TEXT DEFAULT '',
                rating TEXT DEFAULT '',
                report_type TEXT DEFAULT '',
                page_count INTEGER DEFAULT 0,
                target_price REAL,
                core_thesis TEXT DEFAULT '',
                key_evidence TEXT DEFAULT '[]',
                risk_flags TEXT DEFAULT '[]',
                quality_score INTEGER DEFAULT 0,
                full_text TEXT DEFAULT '',
                source_url TEXT DEFAULT '',
                fetched_at TEXT DEFAULT (datetime('now', 'localtime'))
            );
            CREATE INDEX IF NOT EXISTS idx_report_stock_date
                ON research_reports(stock_code, pub_date DESC);

            CREATE TABLE IF NOT EXISTS stock_timeline (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                event_date TEXT DEFAULT '',
                event_type TEXT DEFAULT '',
                title TEXT DEFAULT '',
                impact TEXT DEFAULT '',
                summary TEXT DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_timeline_stock
                ON stock_timeline(stock_code, event_date DESC);
        """)
        conn.commit()
        logger.info(f"[KB] 知识库初始化完成: {KB_PATH}")
    finally:
        conn.close()


# ==================== 分析快照 ====================

def save_snapshot(
    stock_code: str,
    overall_score: int,
    overall_grade: str,
    dimension_scores: Dict[str, int],
    key_conclusion: str,
    top_signals: List[str],
    top_risks: List[str],
    price_at_analysis: float = None,
    full_report_md: str = "",
) -> bool:
    conn = _get_conn()
    try:
        conn.execute(
            """INSERT INTO analysis_snapshots
               (stock_code, overall_score, overall_grade, dimension_scores,
                key_conclusion, top_signals, top_risks, price_at_analysis, full_report_md)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                stock_code.strip(),
                overall_score,
                overall_grade,
                json.dumps(dimension_scores, ensure_ascii=False),
                key_conclusion,
                json.dumps(top_signals, ensure_ascii=False),
                json.dumps(top_risks, ensure_ascii=False),
                price_at_analysis,
                full_report_md,
            ),
        )
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"[KB] 保存快照失败 ({stock_code}): {e}")
        return False
    finally:
        conn.close()


def get_latest_snapshot(stock_code: str) -> Optional[Dict]:
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM analysis_snapshots WHERE stock_code = ? ORDER BY analysis_time DESC LIMIT 1",
            (stock_code.strip(),),
        ).fetchone()
        if row:
            d = dict(row)
            for key in ("dimension_scores", "top_signals", "top_risks"):
                raw = d.get(key, "{}")
                if isinstance(raw, str):
                    d[key] = json.loads(raw)
            return d
        return None
    finally:
        conn.close()


def get_snapshot_history(stock_code: str, limit: int = 5) -> List[Dict]:
    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM analysis_snapshots WHERE stock_code = ? ORDER BY analysis_time DESC LIMIT ?",
            (stock_code.strip(), limit),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def build_tracking_context(stock_code: str) -> str:
    """构建上一次分析的上下文摘要，注入 Agent prompt"""
    prev = get_latest_snapshot(stock_code)
    if not prev:
        return "（首次分析，无历史记录可回溯）"

    prev_time = prev.get("analysis_time", "未知")
    prev_score = prev.get("overall_score", 0)
    prev_grade = prev.get("overall_grade", "未知")
    prev_conclusion = prev.get("key_conclusion", "无记录")
    prev_price = prev.get("price_at_analysis")
    dim_scores = prev.get("dimension_scores", {})

    price_str = f"当时股价: {prev_price}" if prev_price else "股价: 未记录"
    score_breakdown = ", ".join(f"{k}:{v}" for k, v in sorted(dim_scores.items())) if dim_scores else "无分维度记录"

    return f"""【历史分析回溯 — {prev_time}】
上次综合评分: {prev_score}/100 ({prev_grade}) | {price_str}
各维度评分: {score_breakdown}
上次核心结论: {prev_conclusion}
请在本次分析时，关注上次结论是否仍然成立，哪些信号发生变化。"""


# ==================== 研报管理 ====================

def save_report(
    stock_code: str,
    title: str,
    institution: str,
    pub_date: str,
    rating: str = "",
    report_type: str = "",
    page_count: int = 0,
    target_price: float = None,
    core_thesis: str = "",
    key_evidence: List[str] = None,
    risk_flags: List[str] = None,
    quality_score: int = 0,
    full_text: str = "",
    source_url: str = "",
    analyst: str = "",
) -> bool:
    conn = _get_conn()
    try:
        existing = conn.execute(
            "SELECT id FROM research_reports WHERE stock_code = ? AND title = ? AND pub_date = ?",
            (stock_code.strip(), title, pub_date),
        ).fetchone()
        if existing:
            return True

        conn.execute(
            """INSERT INTO research_reports
               (stock_code, title, institution, analyst, pub_date, rating,
                report_type, page_count, target_price, core_thesis,
                key_evidence, risk_flags, quality_score, full_text, source_url)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                stock_code.strip(), title, institution, analyst, pub_date, rating,
                report_type, page_count, target_price, core_thesis,
                json.dumps(key_evidence or [], ensure_ascii=False),
                json.dumps(risk_flags or [], ensure_ascii=False),
                quality_score, full_text, source_url,
            ),
        )
        conn.commit()

        # 只保留最新 10 篇，删除旧报告
        conn.execute("""
            DELETE FROM research_reports WHERE stock_code = ? AND id NOT IN (
                SELECT id FROM research_reports WHERE stock_code = ?
                ORDER BY pub_date DESC LIMIT 10
            )
        """, (stock_code.strip(), stock_code.strip()))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"[KB] 保存研报失败 ({stock_code}): {e}")
        return False
    finally:
        conn.close()


def get_reports_for_stock(stock_code: str, limit: int = 10) -> List[Dict]:
    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM research_reports WHERE stock_code = ? ORDER BY pub_date DESC LIMIT ?",
            (stock_code.strip(), limit),
        ).fetchall()
        results = []
        for r in rows:
            d = dict(r)
            for key in ("key_evidence", "risk_flags"):
                raw = d.get(key, "[]")
                if isinstance(raw, str):
                    d[key] = json.loads(raw)
            results.append(d)
        return results
    finally:
        conn.close()


def get_report_consensus(stock_code: str) -> str:
    """构建研报共识摘要"""
    reports = get_reports_for_stock(stock_code, limit=10)
    if not reports:
        return "（无存量研报）"

    ratings = [r.get("rating", "") for r in reports if r.get("rating")]
    institutions = list({r.get("institution", "") for r in reports if r.get("institution")})
    latest = reports[0]

    rating_dist = {}
    for r in ratings:
        rating_dist[r] = rating_dist.get(r, 0) + 1
    consensus_rating = max(rating_dist, key=rating_dist.get) if rating_dist else "无评级"

    return f"""【机构研报共识 — {len(reports)} 篇存量报告】
覆盖机构: {", ".join(institutions[:8])}{"..." if len(institutions) > 8 else ""}
最近研报: {latest.get("title", "")} ({latest.get("institution", "")}, {latest.get("pub_date", "")})
评级分布: {rating_dist}
共识评级: {consensus_rating}"""


def get_report_count(stock_code: str) -> int:
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM research_reports WHERE stock_code = ?",
            (stock_code.strip(),),
        ).fetchone()
        return row["cnt"] if row else 0
    finally:
        conn.close()


# ==================== 启动初始化 ====================
init_kb()