"""
SQLite 数据库层 - 自选股 + 分析记录（统一存储）

设计说明（2026-08-18 重构）：
  原项目存在两套并行的"每次分析"存储：
    - db.py 的 analysis_cache（harness.db）
    - knowledge_base.py 的 analysis_snapshots（data/knowledge/stock_memory.db）
  二者写入的是同一份分析结果，却分属两个文件/两套函数，既重复又易失同步。
  现合并为单表 `analysis_records`（位于 harness.db），由本模块统一读写：
    - chat_engine 的"基于报告追问"与 knowledge_base 的"历史跟踪上下文/快照历史"
      都从同一张表读取，彻底消除双写。
    - 写入采用"追加 + 保留策略"（每股票最多保留 N 条 + 超过 TTL 天的清理），
      避免旧 analysis_cache 只 INSERT 不清理导致无限堆积。

零外部依赖，纯 Python 标准库 sqlite3。
"""
import sqlite3
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any

logger = logging.getLogger("DB")

DB_PATH = Path(__file__).parent / "data" / "harness.db"

# 分析记录保留策略
ANALYSIS_RETENTION_PER_STOCK = 30   # 每支股票最多保留的最近分析条数
ANALYSIS_TTL_DAYS = 180             # 超过该天数的分析记录将被清理


def _get_conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """初始化数据库表，并执行一次性的旧表迁移/清理。"""
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

            CREATE TABLE IF NOT EXISTS analysis_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                analyzed_at TEXT DEFAULT (datetime('now', 'localtime')),
                report_markdown TEXT NOT NULL DEFAULT '',
                reports_json TEXT NOT NULL DEFAULT '{}',
                chart_data_json TEXT DEFAULT '{}',
                overall_grade TEXT DEFAULT '',
                overall_score INTEGER DEFAULT 50,
                context_text TEXT DEFAULT '',
                dimension_scores TEXT DEFAULT '{}',
                key_conclusion TEXT DEFAULT '',
                top_signals TEXT DEFAULT '[]',
                top_risks TEXT DEFAULT '[]',
                price_at_analysis REAL,
                full_report_md TEXT DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_analysis_records_stock
                ON analysis_records(stock_code, analyzed_at DESC, id DESC);

            CREATE TABLE IF NOT EXISTS backtest_outcomes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                analysis_date TEXT,
                forecast_days INTEGER,
                predicted_grade TEXT DEFAULT '',
                predicted_score INTEGER DEFAULT 50,
                price_at_analysis REAL,
                price_at_future REAL,
                return_pct REAL,
                benchmark_return_pct REAL,
                excess_return_pct REAL,
                actual_direction TEXT,
                is_correct INTEGER DEFAULT 0,
                evaluated_at TEXT DEFAULT (datetime('now', 'localtime'))
            );
            CREATE INDEX IF NOT EXISTS idx_backtest_outcomes_stock
                ON backtest_outcomes(stock_code, analysis_date, forecast_days);
        """)
        conn.commit()

        # ---- 增量列迁移（幂等）：回测置信度校准所需字段 ----
        _ensure_column(conn, "analysis_records", "overall_confidence",
                       "ALTER TABLE analysis_records ADD COLUMN overall_confidence INTEGER DEFAULT 0")
        _ensure_column(conn, "backtest_outcomes", "predicted_confidence",
                       "ALTER TABLE backtest_outcomes ADD COLUMN predicted_confidence INTEGER DEFAULT 0")
        _ensure_column(conn, "backtest_outcomes", "dimension_results",
                       "ALTER TABLE backtest_outcomes ADD COLUMN dimension_results TEXT DEFAULT '{}'")

        # ---- 一次性迁移：旧的 analysis_cache / analysis_snapshots → analysis_records ----
        _migrate_legacy_caches(conn)

        conn.commit()
        logger.info(f"[DB] 数据库初始化完成: {DB_PATH}")
    finally:
        conn.close()


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    if "." in name:
        # 支持 ATTACH 的库，例如 "kb.analysis_snapshots"
        schema, tname = name.split(".", 1)
        row = conn.execute(
            f"SELECT 1 FROM {schema}.sqlite_master WHERE type='table' AND name=?",
            (tname,),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,),
        ).fetchone()
    return row is not None


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str):
    """幂等加列：列不存在时执行 ALTER TABLE（用于轻量 schema 迁移）。"""
    if not _table_exists(conn, table):
        return
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in cols:
        conn.execute(ddl)
        logger.info(f"[DB] 迁移加列: {table}.{column}")


def _migrate_legacy_caches(conn: sqlite3.Connection):
    """将历史 analysis_cache（本库）与 analysis_snapshots（knowledge 库）合并入 analysis_records。"""
    # 1) 本库旧表 analysis_cache → analysis_records
    if _table_exists(conn, "analysis_cache"):
        logger.info("[DB] 迁移 analysis_cache → analysis_records")
        conn.execute("""
            INSERT INTO analysis_records
                (stock_code, analyzed_at, report_markdown, reports_json, chart_data_json,
                 overall_grade, overall_score, context_text,
                 dimension_scores, key_conclusion, top_signals, top_risks,
                 price_at_analysis, full_report_md)
            SELECT stock_code, analyzed_at, report_markdown, reports_json, chart_data_json,
                   overall_grade, overall_score, context_text,
                   '{}', '', '[]', '[]',
                   NULL, report_markdown
            FROM analysis_cache
        """)
        conn.execute("DROP TABLE analysis_cache")

    # 2) 知识库旧表 analysis_snapshots（独立文件）→ analysis_records
    kb_path = Path(__file__).parent / "data" / "knowledge" / "stock_memory.db"
    if kb_path.exists():
        try:
            conn.execute(f"ATTACH DATABASE ? AS kb", (str(kb_path),))
            if _table_exists(conn, "kb.analysis_snapshots"):
                logger.info("[DB] 迁移 kb.analysis_snapshots → analysis_records")
                conn.execute("""
                    INSERT INTO analysis_records
                        (stock_code, analyzed_at, report_markdown, reports_json, chart_data_json,
                         overall_grade, overall_score, context_text,
                         dimension_scores, key_conclusion, top_signals, top_risks,
                         price_at_analysis, full_report_md)
                    SELECT stock_code, analysis_time,
                           COALESCE(full_report_md, ''), '{}', '{}',
                           overall_grade, overall_score, '',
                           dimension_scores, key_conclusion, top_signals, top_risks,
                           price_at_analysis, COALESCE(full_report_md, '')
                    FROM kb.analysis_snapshots
                """)
                conn.execute("DROP TABLE kb.analysis_snapshots")
            conn.execute("DETACH DATABASE kb")
        except Exception as e:
            logger.warning(f"[DB] 迁移 analysis_snapshots 失败（已跳过）: {e}")
            try:
                conn.execute("DETACH DATABASE kb")
            except Exception:
                pass


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


# ==================== 分析记录（统一存储） ====================

def save_analysis(
    stock_code: str,
    *,
    report_markdown: str = "",
    reports: Dict = None,
    chart_data: Dict = None,
    overall_grade: str = "",
    overall_score: int = 50,
    overall_confidence: int = 0,
    context_text: str = "",
    dimension_scores: Dict = None,
    key_conclusion: str = "",
    top_signals: List[str] = None,
    top_risks: List[str] = None,
    price_at_analysis: float = None,
    full_report_md: str = None,
) -> bool:
    """写入一条分析记录（全量字段，单次写入）。

    合并了原 analysis_cache 与 analysis_snapshots 的职责，是全站唯一的"分析落库"入口。
    写入后按保留策略裁剪（每股票最多 N 条）。
    """
    if full_report_md is None:
        full_report_md = report_markdown
    conn = _get_conn()
    try:
        conn.execute(
            """INSERT INTO analysis_records
               (stock_code, report_markdown, reports_json, chart_data_json,
                overall_grade, overall_score, overall_confidence, context_text, dimension_scores,
                key_conclusion, top_signals, top_risks, price_at_analysis, full_report_md)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                stock_code.strip(),
                report_markdown,
                json.dumps(reports or {}, ensure_ascii=False),
                json.dumps(chart_data or {}, ensure_ascii=False),
                overall_grade,
                overall_score,
                int(overall_confidence or 0),
                context_text,
                json.dumps(dimension_scores or {}, ensure_ascii=False),
                key_conclusion,
                json.dumps(top_signals or [], ensure_ascii=False),
                json.dumps(top_risks or [], ensure_ascii=False),
                price_at_analysis,
                full_report_md,
            ),
        )
        conn.commit()
        prune_analysis_records(stock_code)
        return True
    except Exception as e:
        logger.error(f"[DB] 保存分析记录失败: {e}")
        return False
    finally:
        conn.close()


def _row_to_dict(row) -> Dict:
    """把 analysis_records 的一行转换为对外统一的字典（JSON 字段已解析）。"""
    d = dict(row)
    for key in ("reports_json", "chart_data_json", "dimension_scores", "top_signals", "top_risks"):
        raw = d.get(key)
        if isinstance(raw, str):
            try:
                d[key] = json.loads(raw)
            except json.JSONDecodeError:
                d[key] = {} if key in ("reports_json", "chart_data_json", "dimension_scores") else []
    # 对外暴露解析后的字段名（兼容 chat_engine 的 reports/chart_data 用法）
    d["reports"] = d.pop("reports_json", {})
    d["chart_data"] = d.pop("chart_data_json", {})
    return d


def get_latest_analysis(stock_code: str) -> Optional[Dict]:
    """读取某股票最近一次分析记录（chat_engine 与 build_tracking_context 共用）。"""
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM analysis_records WHERE stock_code = ? "
            "ORDER BY analyzed_at DESC, id DESC LIMIT 1",
            (stock_code.strip(),)
        ).fetchone()
        if row:
            return _row_to_dict(row)
        return None
    finally:
        conn.close()


def get_analysis_history(stock_code: str, limit: int = 5) -> List[Dict]:
    """读取某股票最近 N 条分析记录（原 get_snapshot_history）。"""
    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM analysis_records WHERE stock_code = ? "
            "ORDER BY analyzed_at DESC, id DESC LIMIT ?",
            (stock_code.strip(), limit),
        ).fetchall()
        return [_row_to_dict(r) for r in rows]
    finally:
        conn.close()


def prune_analysis_records(stock_code: str = None):
    """按保留策略清理分析记录。

    - 指定 stock_code：仅保留该股票最近 ANALYSIS_RETENTION_PER_STOCK 条（写入后调用）。
    - 不指定：清理超过 ANALYSIS_TTL_DAYS 天的全部陈旧记录（运维/启动时使用）。
    """
    conn = _get_conn()
    try:
        if stock_code:
            conn.execute(
                """DELETE FROM analysis_records
                   WHERE stock_code = ?
                     AND id NOT IN (
                         SELECT id FROM analysis_records
                         WHERE stock_code = ?
                         ORDER BY analyzed_at DESC, id DESC
                         LIMIT ?
                     )""",
                (stock_code.strip(), stock_code.strip(), ANALYSIS_RETENTION_PER_STOCK),
            )
        else:
            conn.execute(
                "DELETE FROM analysis_records WHERE analyzed_at < datetime('now', 'localtime', ?)",
                (f"-{ANALYSIS_TTL_DAYS} days",),
            )
        conn.commit()
    except Exception as e:
        logger.warning(f"[DB] 清理分析记录失败: {e}")
    finally:
        conn.close()


# ==================== 回测结果（分析→N日验证闭环） ====================

def get_analysis_records_for_backtest(
    horizon_days: int,
    stock_codes: Optional[List[str]] = None,
) -> List[Dict]:
    """读取"已到期且尚未回测"的历史分析记录，供真实数据回测使用。

    筛选条件：
      - price_at_analysis 非空（才有收益基准）
      - analyzed_at 距今 >= horizon_days（未来时点已过去）
      - 同一 (stock_code, analysis_date, forecast_days) 在 backtest_outcomes 中无记录
    """
    conn = _get_conn()
    try:
        sql = """
            SELECT ar.stock_code, ar.analyzed_at, ar.overall_grade, ar.overall_score,
                   ar.overall_confidence, ar.dimension_scores, ar.price_at_analysis
            FROM analysis_records ar
            WHERE ar.price_at_analysis IS NOT NULL
              AND (julianday('now', 'localtime') - julianday(ar.analyzed_at)) >= ?
              AND NOT EXISTS (
                  SELECT 1 FROM backtest_outcomes bo
                  WHERE bo.stock_code = ar.stock_code
                    AND bo.analysis_date = substr(ar.analyzed_at, 1, 10)
                    AND bo.forecast_days = ?
              )
        """
        params: List[Any] = [horizon_days, horizon_days]
        if stock_codes:
            placeholders = ",".join("?" for _ in stock_codes)
            sql += f" AND ar.stock_code IN ({placeholders})"
            params.extend([c.strip() for c in stock_codes])
        sql += " ORDER BY ar.analyzed_at ASC"
        rows = conn.execute(sql, params).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            ds = d.get("dimension_scores")
            if isinstance(ds, str):
                try:
                    ds = json.loads(ds)
                except json.JSONDecodeError:
                    ds = {}
            d["dimension_scores"] = ds or {}
            d["analysis_date"] = str(d.get("analyzed_at", ""))[:10]
            out.append(d)
        return out
    finally:
        conn.close()


def save_backtest_outcome(result) -> bool:
    """落库单条回测结果（BacktestResult 对象）。

    predicted_confidence / dimension_results 供 confidence 校准与维度级校准上下文使用。
    """
    d = result.to_dict()
    dim_results = d.get("dimension_correct") or {}
    conn = _get_conn()
    try:
        conn.execute(
            """INSERT INTO backtest_outcomes
               (stock_code, analysis_date, forecast_days, predicted_grade, predicted_score,
                predicted_confidence, dimension_results,
                price_at_analysis, price_at_future, return_pct, benchmark_return_pct,
                excess_return_pct, actual_direction, is_correct)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                d["stock_code"], d["analysis_date"], d["forecast_days"],
                d["predicted_grade"], d["predicted_score"],
                int(getattr(result.snapshot, "overall_confidence", 0) or 0),
                json.dumps({k: bool(v) for k, v in dim_results.items()}, ensure_ascii=False),
                result.outcome.price_at_analysis, result.outcome.price_at_future,
                result.outcome.return_pct, result.outcome.benchmark_return_pct,
                result.outcome.excess_return_pct, result.outcome.direction,
                1 if result.is_correct_direction else 0,
            ),
        )
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"[DB] 保存回测结果失败: {e}")
        return False
    finally:
        conn.close()


def get_backtest_outcomes(stock_code: str) -> List[Dict]:
    """读取某股票全部已落库的回测结果（供校准上下文生成）。"""
    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM backtest_outcomes WHERE stock_code = ? "
            "ORDER BY analysis_date DESC, forecast_days ASC",
            (stock_code.strip(),),
        ).fetchall()
        return [_decode_outcome_row(dict(r)) for r in rows]
    finally:
        conn.close()


def get_all_backtest_outcomes(forecast_days: Optional[int] = None) -> List[Dict]:
    """读取全量回测结果（供回测报告/整体校准分析），可选按展望期过滤。"""
    conn = _get_conn()
    try:
        if forecast_days:
            rows = conn.execute(
                "SELECT * FROM backtest_outcomes WHERE forecast_days = ? "
                "ORDER BY analysis_date DESC",
                (forecast_days,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM backtest_outcomes ORDER BY analysis_date DESC"
            ).fetchall()
        return [_decode_outcome_row(dict(r)) for r in rows]
    finally:
        conn.close()


def _decode_outcome_row(d: Dict) -> Dict:
    """解析回测结果行中的 dimension_results JSON 字段。"""
    raw = d.get("dimension_results")
    if isinstance(raw, str) and raw:
        try:
            d["dimension_results"] = json.loads(raw)
        except json.JSONDecodeError:
            d["dimension_results"] = {}
    elif raw is None:
        d["dimension_results"] = {}
    return d


# ==================== 兼容别名（供 chat_engine 等旧调用） ====================

def cache_get_latest(stock_code: str) -> Optional[Dict]:
    """兼容别名：返回最近一次分析记录（含 reports / chart_data 解析字段）。"""
    return get_latest_analysis(stock_code)


def cache_save(
    stock_code: str,
    report_markdown: str,
    reports: Dict,
    chart_data: Dict = None,
    overall_grade: str = "",
    overall_score: int = 50,
    context_text: str = "",
) -> bool:
    """兼容别名：仅写入原 analysis_cache 维度的字段（建议新代码改用 save_analysis）。"""
    return save_analysis(
        stock_code,
        report_markdown=report_markdown,
        reports=reports,
        chart_data=chart_data,
        overall_grade=overall_grade,
        overall_score=overall_score,
        context_text=context_text,
    )


# ==================== 启动初始化 ====================
init_db()
