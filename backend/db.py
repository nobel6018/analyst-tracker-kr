"""
DB 연결 및 헬퍼
SQLite 사용 (개발). PostgreSQL 전환 시 connection 부분만 교체.
"""

import sqlite3
from pathlib import Path
from contextlib import contextmanager

DB_PATH = Path(__file__).parent / "analyst.db"
SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def transaction():
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with transaction() as conn:
        conn.executescript(SCHEMA_PATH.read_text())
    print(f"DB 초기화 완료: {DB_PATH}")


# ─── firm ────────────────────────────────────────────────────────────────────

def upsert_firm(conn: sqlite3.Connection, name: str) -> int:
    conn.execute(
        "INSERT OR IGNORE INTO firm (name) VALUES (?)",
        (name,),
    )
    row = conn.execute("SELECT id FROM firm WHERE name = ?", (name,)).fetchone()
    return row["id"]


# ─── stock ───────────────────────────────────────────────────────────────────

def upsert_stock(conn: sqlite3.Connection, naver_code: str, yahoo_ticker: str,
                 name: str, market: str | None = None) -> int:
    conn.execute(
        """
        INSERT INTO stock (naver_code, yahoo_ticker, name, market)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(naver_code) DO UPDATE SET
            yahoo_ticker = excluded.yahoo_ticker,
            name         = excluded.name,
            market       = excluded.market,
            updated_at   = datetime('now')
        """,
        (naver_code, yahoo_ticker, name, market),
    )
    row = conn.execute("SELECT id FROM stock WHERE naver_code = ?", (naver_code,)).fetchone()
    return row["id"]


def get_all_stocks(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute("SELECT * FROM stock").fetchall()


# ─── stock_price ─────────────────────────────────────────────────────────────

def upsert_price(conn: sqlite3.Connection, stock_id: int, price_date: str,
                 close_price: int, high_price: int | None = None,
                 low_price: int | None = None, open_price: int | None = None,
                 volume: int | None = None):
    conn.execute(
        """
        INSERT INTO stock_price (stock_id, price_date, open_price, high_price, low_price, close_price, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(stock_id, price_date) DO UPDATE SET
            close_price = excluded.close_price,
            high_price  = excluded.high_price,
            low_price   = excluded.low_price
        """,
        (stock_id, price_date, open_price, high_price, low_price, close_price, volume),
    )


def get_price(conn: sqlite3.Connection, stock_id: int, price_date: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM stock_price WHERE stock_id = ? AND price_date >= ? ORDER BY price_date ASC LIMIT 1",
        (stock_id, price_date),
    ).fetchone()


def get_max_high(conn: sqlite3.Connection, stock_id: int,
                 start_date: str, end_date: str) -> tuple[int | None, int | None]:
    """기간 내 최고가 + 마지막 종가"""
    row = conn.execute(
        """
        SELECT MAX(high_price) AS max_high,
               (SELECT close_price FROM stock_price
                WHERE stock_id = ? AND price_date <= ?
                ORDER BY price_date DESC LIMIT 1) AS last_close
        FROM stock_price
        WHERE stock_id = ? AND price_date BETWEEN ? AND ?
        """,
        (stock_id, end_date, stock_id, start_date, end_date),
    ).fetchone()
    return (row["max_high"], row["last_close"]) if row else (None, None)


# ─── report ──────────────────────────────────────────────────────────────────

OPINION_MAP = {
    "매수": "BUY", "buy": "BUY", "strong buy": "BUY", "outperform": "BUY",
    "비중확대": "BUY", "긍정적": "BUY", "적극매수": "BUY",
    "중립": "HOLD", "hold": "HOLD", "neutral": "HOLD", "marketperform": "HOLD",
    "보유": "HOLD", "없음": "HOLD",
    "매도": "SELL", "sell": "SELL", "underperform": "SELL", "비중축소": "SELL",
}


def normalize_opinion(raw: str | None) -> str | None:
    if not raw:
        return None
    return OPINION_MAP.get(raw.strip().lower(), "BUY")  # 미매핑 기본값 BUY (한국 시장 특성)


def upsert_report(conn: sqlite3.Connection, *, nid: str, stock_id: int, firm_id: int,
                  analyst_name: str | None, title: str | None, opinion_raw: str | None,
                  target_price: int | None, base_price: int | None,
                  report_date: str) -> int:
    opinion = normalize_opinion(opinion_raw)
    conn.execute(
        """
        INSERT INTO report
            (nid, stock_id, firm_id, analyst_name, title, opinion_raw, opinion,
             target_price, base_price, report_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(nid) DO UPDATE SET
            base_price  = COALESCE(excluded.base_price, report.base_price),
            target_price= COALESCE(excluded.target_price, report.target_price),
            opinion_raw = COALESCE(excluded.opinion_raw, report.opinion_raw),
            opinion     = COALESCE(excluded.opinion, report.opinion),
            updated_at  = datetime('now')
        """,
        (nid, stock_id, firm_id, analyst_name, title, opinion_raw, opinion,
         target_price, base_price, report_date),
    )
    row = conn.execute("SELECT id FROM report WHERE nid = ?", (nid,)).fetchone()
    return row["id"]


def get_reports_without_result(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT r.*, s.yahoo_ticker, s.name AS stock_name, f.name AS firm_name
        FROM report r
        JOIN stock s ON r.stock_id = s.id
        JOIN firm  f ON r.firm_id  = f.id
        LEFT JOIN report_result rr ON rr.report_id = r.id
        WHERE rr.id IS NULL
          AND r.target_price IS NOT NULL
          AND r.base_price IS NOT NULL
        """
    ).fetchall()


def get_reports_for_scoring(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT r.firm_id, r.analyst_name, r.opinion,
               rr.hit, rr.actual_return_pct, rr.implied_upside_pct
        FROM report r
        JOIN report_result rr ON rr.report_id = r.id
        WHERE rr.hit IS NOT NULL
        """
    ).fetchall()


# ─── report_result ────────────────────────────────────────────────────────────

def upsert_report_result(conn: sqlite3.Connection, *, report_id: int,
                         max_high_1y: int | None, last_close_1y: int | None,
                         hit: bool | None, implied_upside_pct: float | None,
                         actual_return_pct: float | None, evaluation_date: str,
                         is_final: bool):
    conn.execute(
        """
        INSERT INTO report_result
            (report_id, max_high_1y, last_close_1y, hit, implied_upside_pct,
             actual_return_pct, evaluation_date, is_final)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(report_id) DO UPDATE SET
            max_high_1y        = excluded.max_high_1y,
            last_close_1y      = excluded.last_close_1y,
            hit                = excluded.hit,
            implied_upside_pct = excluded.implied_upside_pct,
            actual_return_pct  = excluded.actual_return_pct,
            evaluation_date    = excluded.evaluation_date,
            is_final           = excluded.is_final,
            updated_at         = datetime('now')
        """,
        (report_id, max_high_1y, last_close_1y,
         1 if hit else 0 if hit is not None else None,
         implied_upside_pct, actual_return_pct, evaluation_date, 1 if is_final else 0),
    )


# ─── analyst_score ────────────────────────────────────────────────────────────

def upsert_analyst_score(conn: sqlite3.Connection, *, firm_id: int, analyst_name: str | None,
                         total_reports: int, evaluated_reports: int, hit_count: int,
                         success_rate: float | None, avg_return_pct: float | None,
                         z_score: float | None, star_rating: int | None,
                         is_stat_significant: bool, buy_total: int, buy_hit_count: int,
                         buy_hit_rate: float | None, calculated_at: str):
    conn.execute(
        """
        INSERT INTO analyst_score
            (firm_id, analyst_name, total_reports, evaluated_reports, hit_count,
             success_rate, avg_return_pct, z_score, star_rating, is_stat_significant,
             buy_total, buy_hit_count, buy_hit_rate, calculated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(firm_id, analyst_name) DO UPDATE SET
            total_reports       = excluded.total_reports,
            evaluated_reports   = excluded.evaluated_reports,
            hit_count           = excluded.hit_count,
            success_rate        = excluded.success_rate,
            avg_return_pct      = excluded.avg_return_pct,
            z_score             = excluded.z_score,
            star_rating         = excluded.star_rating,
            is_stat_significant = excluded.is_stat_significant,
            buy_total           = excluded.buy_total,
            buy_hit_count       = excluded.buy_hit_count,
            buy_hit_rate        = excluded.buy_hit_rate,
            calculated_at       = excluded.calculated_at,
            updated_at          = datetime('now')
        """,
        (firm_id, analyst_name, total_reports, evaluated_reports, hit_count,
         success_rate, avg_return_pct, z_score, star_rating,
         1 if is_stat_significant else 0,
         buy_total, buy_hit_count, buy_hit_rate, calculated_at),
    )


def get_leaderboard(conn: sqlite3.Connection, min_reports: int = 5,
                    limit: int = 20) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT s.*, f.name AS firm_name
        FROM analyst_score s
        JOIN firm f ON f.id = s.firm_id
        WHERE s.evaluated_reports >= ?
        ORDER BY
            s.star_rating DESC NULLS LAST,
            s.z_score DESC NULLS LAST,
            s.success_rate DESC NULLS LAST
        LIMIT ?
        """,
        (min_reports, limit),
    ).fetchall()
