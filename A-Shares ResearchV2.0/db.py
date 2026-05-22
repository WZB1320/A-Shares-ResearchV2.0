"""
SQLite 数据库层 - 自选股 + 分析缓存
零外部依赖，纯 Python 标准库 sqlite3
"""
import sqlite3
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any

logger = logging.getLogger("DB")

DB_PATH = Path(__file__).parent / "data" / "harness.db"


def _get_conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """初始化数据库表"""
    conn = _get_conn()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS watchlist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL UNIQUE,
                stock_name TEXT DEFAULT '',
                added_at TEXT DEFAULT (datetime('now', 'localtime')),
                notes TEXT DEFAULT '',
                last_price REAL,
                last_grade TEXT,
                last_score INTEGER,
                last_analysis_at TEXT
            );

            CREATE TABLE IF NOT EXISTS analysis_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                analyzed_at TEXT DEFAULT (datetime('now', 'localtime')),
                report_markdown TEXT NOT NULL DEFAULT '',
                reports_json TEXT NOT NULL DEFAULT '{}',
                chart_data_json TEXT DEFAULT '{}',
                overall_grade TEXT DEFAULT '',
                overall_score INTEGER DEFAULT 50,
                context_text TEXT DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_analysis_cache_stock
                ON analysis_cache(stock_code, analyzed_at DESC);
        """)
        conn.commit()
        logger.info(f"[DB] 数据库初始化完成: {DB_PATH}")
    finally:
        conn.close()


# ==================== 自选股 ====================

def watchlist_get_all() -> List[Dict]:
    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM watchlist ORDER BY added_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def watchlist_add(stock_code: str, stock_name: str = "", notes: str = "") -> bool:
    conn = _get_conn()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO watchlist (stock_code, stock_name, notes) VALUES (?, ?, ?)",
            (stock_code.strip(), stock_name, notes)
        )
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"[DB] 添加自选股失败: {e}")
        return False
    finally:
        conn.close()


def watchlist_remove(stock_code: str) -> bool:
    conn = _get_conn()
    try:
        conn.execute("DELETE FROM watchlist WHERE stock_code = ?", (stock_code.strip(),))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"[DB] 删除自选股失败: {e}")
        return False
    finally:
        conn.close()


def watchlist_update_snapshot(stock_code: str, stock_name: str = "",
                               last_price: float = None, last_grade: str = "",
                               last_score: int = None) -> bool:
    conn = _get_conn()
    try:
        parts = []
        params = []
        if stock_name:
            parts.append("stock_name = ?")
            params.append(stock_name)
        if last_price is not None:
            parts.append("last_price = ?")
            params.append(last_price)
        if last_grade:
            parts.append("last_grade = ?")
            params.append(last_grade)
        if last_score is not None:
            parts.append("last_score = ?")
            params.append(last_score)
        parts.append("last_analysis_at = datetime('now', 'localtime')")
        params.append(stock_code.strip())
        conn.execute(
            f"UPDATE watchlist SET {', '.join(parts)} WHERE stock_code = ?",
            params
        )
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"[DB] 更新自选股快照失败: {e}")
        return False
    finally:
        conn.close()


# ==================== 分析缓存 ====================

def cache_save(stock_code: str, report_markdown: str, reports: Dict,
               chart_data: Dict = None, overall_grade: str = "",
               overall_score: int = 50, context_text: str = "") -> bool:
    conn = _get_conn()
    try:
        conn.execute(
            """INSERT INTO analysis_cache
               (stock_code, report_markdown, reports_json, chart_data_json,
                overall_grade, overall_score, context_text)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (stock_code.strip(), report_markdown, json.dumps(reports, ensure_ascii=False),
             json.dumps(chart_data or {}, ensure_ascii=False),
             overall_grade, overall_score, context_text)
        )
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"[DB] 保存分析缓存失败: {e}")
        return False
    finally:
        conn.close()


def cache_get_latest(stock_code: str) -> Optional[Dict]:
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM analysis_cache WHERE stock_code = ? ORDER BY analyzed_at DESC LIMIT 1",
            (stock_code.strip(),)
        ).fetchone()
        if row:
            d = dict(row)
            d["reports"] = json.loads(d.get("reports_json", "{}"))
            d["chart_data"] = json.loads(d.get("chart_data_json", "{}"))
            return d
        return None
    finally:
        conn.close()


# ==================== 启动初始化 ====================
init_db()