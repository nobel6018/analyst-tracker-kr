-- 애널리스트 성적 추적 플랫폼 DB 스키마
-- SQLite (개발) → PostgreSQL (프로덕션) 마이그레이션 용이하도록 설계

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- ─── 증권사 ──────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS firm (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL UNIQUE,
    created_at  TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- ─── 종목 ────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS stock (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    naver_code   TEXT    NOT NULL UNIQUE,   -- '005930'
    yahoo_ticker TEXT    NOT NULL,           -- '005930.KS'
    name         TEXT    NOT NULL,
    market       TEXT,                       -- 'KOSPI' | 'KOSDAQ'
    created_at   TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at   TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- ─── 주가 캐시 ───────────────────────────────────────────────────────────────
-- Yahoo Finance 중복 호출 방지. (stock_id, price_date) 단위로 캐싱.

CREATE TABLE IF NOT EXISTS stock_price (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_id    INTEGER NOT NULL REFERENCES stock(id),
    price_date  TEXT    NOT NULL,   -- 'YYYY-MM-DD'
    open_price  INTEGER,
    high_price  INTEGER,
    low_price   INTEGER,
    close_price INTEGER NOT NULL,
    volume      INTEGER,
    created_at  TEXT    NOT NULL DEFAULT (datetime('now')),
    UNIQUE (stock_id, price_date)
);

-- ─── 리포트 ──────────────────────────────────────────────────────────────────
-- 네이버 금융 company_list + company_read 합산 데이터

CREATE TABLE IF NOT EXISTS report (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    nid                 TEXT    NOT NULL UNIQUE,    -- 네이버 리포트 ID
    stock_id            INTEGER NOT NULL REFERENCES stock(id),
    firm_id             INTEGER NOT NULL REFERENCES firm(id),
    analyst_name        TEXT,                        -- NULL 허용 (추후 확장)
    title               TEXT,
    opinion_raw         TEXT,                        -- 원본: '매수', 'Buy', 'BUY'
    opinion             TEXT,                        -- 정규화: 'BUY' | 'HOLD' | 'SELL'
    target_price        INTEGER,
    base_price          INTEGER,                     -- 발행일 당시 종가
    report_date         TEXT    NOT NULL,            -- 'YYYY-MM-DD'
    created_at          TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_report_stock   ON report(stock_id);
CREATE INDEX IF NOT EXISTS idx_report_firm    ON report(firm_id);
CREATE INDEX IF NOT EXISTS idx_report_date    ON report(report_date);

-- ─── 적중 결과 ───────────────────────────────────────────────────────────────
-- 발행일 + 1년 경과 후 자동 계산. is_final=0 이면 아직 1년 미경과 (중간 스냅샷).

CREATE TABLE IF NOT EXISTS report_result (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    report_id           INTEGER NOT NULL UNIQUE REFERENCES report(id),
    max_high_1y         INTEGER,            -- 발행일~1년 최고가
    last_close_1y       INTEGER,            -- 1년 후(or 현재) 종가
    hit                 INTEGER,            -- 1=적중, 0=미달 (target_price 도달 여부)
    implied_upside_pct  REAL,               -- (목표가 - 기준가) / 기준가 × 100
    actual_return_pct   REAL,               -- (1년후 종가 - 기준가) / 기준가 × 100
    evaluation_date     TEXT,               -- 평가 기준일 (발행일 + 365일 or 오늘)
    is_final            INTEGER NOT NULL DEFAULT 0,  -- 1년 완전 경과 여부
    created_at          TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- ─── 애널리스트 점수 ──────────────────────────────────────────────────────────
-- (firm_id, analyst_name) 단위. analyst_name=NULL 이면 증권사 전체 점수.
-- 정기 재계산 (배치). 최신 점수만 유지 (UPSERT).

CREATE TABLE IF NOT EXISTS analyst_score (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    firm_id             INTEGER NOT NULL REFERENCES firm(id),
    analyst_name        TEXT,                        -- NULL = 증권사 전체
    total_reports       INTEGER NOT NULL DEFAULT 0,  -- 전체 리포트 수
    evaluated_reports   INTEGER NOT NULL DEFAULT 0,  -- 적중 계산 완료 건수
    hit_count           INTEGER NOT NULL DEFAULT 0,
    success_rate        REAL,               -- hit_count / evaluated_reports (0~1)
    avg_return_pct      REAL,               -- 평균 실제 수익률
    z_score             REAL,               -- Z-test 값
    star_rating         INTEGER,            -- 1~5. NULL = 데이터 부족
    is_stat_significant INTEGER NOT NULL DEFAULT 0,  -- evaluated_reports >= 10
    -- 매수 편향 보정 지표 (한국 특화)
    buy_total           INTEGER NOT NULL DEFAULT 0,  -- 매수 추천 건수
    buy_hit_count       INTEGER NOT NULL DEFAULT 0,  -- 매수 추천 중 실제 상승
    buy_hit_rate        REAL,               -- buy_hit_count / buy_total (편향 보정률)
    calculated_at       TEXT    NOT NULL,
    created_at          TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT    NOT NULL DEFAULT (datetime('now')),
    UNIQUE (firm_id, analyst_name)
);

-- ─── 종목별 점수 (섹터 전문성 드릴다운용) ────────────────────────────────────

CREATE TABLE IF NOT EXISTS analyst_stock_score (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    firm_id             INTEGER NOT NULL REFERENCES firm(id),
    analyst_name        TEXT,
    stock_id            INTEGER NOT NULL REFERENCES stock(id),
    total_reports       INTEGER NOT NULL DEFAULT 0,
    evaluated_reports   INTEGER NOT NULL DEFAULT 0,
    hit_count           INTEGER NOT NULL DEFAULT 0,
    success_rate        REAL,
    avg_return_pct      REAL,
    calculated_at       TEXT    NOT NULL,
    created_at          TEXT    NOT NULL DEFAULT (datetime('now')),
    UNIQUE (firm_id, analyst_name, stock_id)
);
