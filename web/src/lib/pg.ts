/**
 * PostgreSQL 클라이언트
 * - 로컬: 환경변수 또는 기본값 (RDS 직접 연결)
 * - Vercel: DATABASE_URL 환경변수
 */

import { Pool } from "pg";

const pool = new Pool({
  connectionString:
    process.env.DATABASE_URL ||
    `postgresql://sejak:68fa7f1680765eae405657b5fdf8a1f1@sejak-db.chwac2k62kbf.ap-northeast-2.rds.amazonaws.com:5432/analyst`,
  max: 5,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
  ssl: { rejectUnauthorized: false },  // AWS RDS SSL 필수
});

export async function query<T = any>(sql: string, params?: any[]): Promise<T[]> {
  const res = await pool.query(sql, params);
  return res.rows as T[];
}

export async function queryOne<T = any>(sql: string, params?: any[]): Promise<T | null> {
  const rows = await query<T>(sql, params);
  return rows[0] ?? null;
}

// ─── 리더보드 ──────────────────────────────────────────────────────────────

export interface LeaderEntry {
  firm_name: string;
  analyst_name: string | null;
  total_reports: number;
  evaluated_reports: number;
  hit_count: number;
  success_rate: number;
  avg_return_pct: number;
  z_score: number;
  star_rating: number | null;
  is_stat_significant: boolean;
  buy_hit_rate: number | null;
  calculated_at: string;
}

export async function getLeaderboard(minReports = 5, limit = 30): Promise<LeaderEntry[]> {
  return query<LeaderEntry>(
    `SELECT f.name AS firm_name, s.*
     FROM analyst_score s
     JOIN firm f ON f.id = s.firm_id
     WHERE s.evaluated_reports >= $1
       AND s.analyst_name IS NULL
     ORDER BY s.z_score DESC NULLS LAST
     LIMIT $2`,
    [minReports, limit]
  );
}

export async function getIndividualLeaderboard(minReports = 3, limit = 30): Promise<LeaderEntry[]> {
  return query<LeaderEntry>(
    `SELECT f.name AS firm_name, s.*
     FROM analyst_score s
     JOIN firm f ON f.id = s.firm_id
     WHERE s.evaluated_reports >= $1
       AND s.analyst_name IS NOT NULL
     ORDER BY s.z_score DESC NULLS LAST
     LIMIT $2`,
    [minReports, limit]
  );
}

// ─── 컨센서스 ──────────────────────────────────────────────────────────────

export interface ConsensusEntry {
  stock_name: string;
  naver_code: string;
  simple_target: number;
  weighted_target: number;
  predicted_surprise_pct: number;
  analyst_count: number;
  coverage: number;
}

export async function getConsensus(): Promise<ConsensusEntry[]> {
  const rows = await query(
    `SELECT
       s.name AS stock_name, s.naver_code,
       r.target_price,
       EXTRACT(EPOCH FROM (NOW() - r.report_date::timestamptz)) / 86400 AS days_old,
       a.star_rating,
       r.firm_id
     FROM report r
     JOIN stock s ON s.id = r.stock_id
     LEFT JOIN analyst_score a ON a.firm_id = r.firm_id AND a.analyst_name IS NULL
     WHERE r.target_price IS NOT NULL
       AND r.report_date >= NOW() - INTERVAL '120 days'
     ORDER BY s.name, r.report_date DESC`
  );

  const byStock = new Map<string, any[]>();
  for (const row of rows) {
    if (!byStock.has(row.naver_code)) byStock.set(row.naver_code, []);
    byStock.get(row.naver_code)!.push(row);
  }

  const results: ConsensusEntry[] = [];
  for (const [code, items] of byStock.entries()) {
    if (items.length < 2) continue;
    const totalW = items.reduce((s, r) => s + weight(r.star_rating, r.days_old), 0);
    const simple = items.reduce((s, r) => s + Number(r.target_price), 0) / items.length;
    const weighted = totalW > 0
      ? items.reduce((s, r) => s + Number(r.target_price) * weight(r.star_rating, r.days_old), 0) / totalW
      : simple;

    results.push({
      stock_name: items[0].stock_name,
      naver_code: code,
      simple_target: Math.round(simple),
      weighted_target: Math.round(weighted),
      predicted_surprise_pct: +((weighted - simple) / Math.abs(simple) * 100).toFixed(2),
      analyst_count: items.length,
      coverage: new Set(items.map((r) => r.firm_id)).size,
    });
  }

  results.sort((a, b) => Math.abs(b.predicted_surprise_pct) - Math.abs(a.predicted_surprise_pct));
  return results;
}

function weight(starRating: number | null, daysOld: number): number {
  const MAX_AGE = 120;
  if (daysOld > MAX_AGE) return 0;
  const accuracy = starRating ? starRating / 5 : 0.2;
  return accuracy * (1 - daysOld / MAX_AGE);
}

// ─── 통계 ──────────────────────────────────────────────────────────────────

export interface Stats {
  total_stocks: number;
  total_reports: number;
  evaluated_reports: number;
  total_firms: number;
  total_analysts: number;
  avg_success_rate: number;
}

export async function getStats(): Promise<Stats> {
  const [stocks, reports, evaluated, firms, analysts, avgRate] = await Promise.all([
    queryOne<{ n: string }>("SELECT COUNT(*) AS n FROM stock"),
    queryOne<{ n: string }>("SELECT COUNT(*) AS n FROM report"),
    queryOne<{ n: string }>("SELECT COUNT(*) AS n FROM report_result WHERE hit IS NOT NULL"),
    queryOne<{ n: string }>("SELECT COUNT(*) AS n FROM firm"),
    queryOne<{ n: string }>("SELECT COUNT(DISTINCT analyst_name || firm_id::text) AS n FROM report WHERE analyst_name IS NOT NULL"),
    queryOne<{ v: string }>("SELECT AVG(success_rate)*100 AS v FROM analyst_score WHERE evaluated_reports >= 5 AND analyst_name IS NULL"),
  ]);
  return {
    total_stocks: Number(stocks?.n ?? 0),
    total_reports: Number(reports?.n ?? 0),
    evaluated_reports: Number(evaluated?.n ?? 0),
    total_firms: Number(firms?.n ?? 0),
    total_analysts: Number(analysts?.n ?? 0),
    avg_success_rate: avgRate?.v ? +Number(avgRate.v).toFixed(1) : 0,
  };
}

// ─── 스코어카드 ─────────────────────────────────────────────────────────────

export async function getAnalystScore(firmName: string, analystName: string | null) {
  return queryOne(
    `SELECT f.name AS firm_name, s.*
     FROM analyst_score s
     JOIN firm f ON f.id = s.firm_id
     WHERE f.name = $1
       AND (s.analyst_name = $2 OR ($2 IS NULL AND s.analyst_name IS NULL))`,
    [firmName, analystName]
  );
}

export async function getAnalystReports(firmName: string, analystName: string | null, limit = 50) {
  return query(
    `SELECT r.report_date, s.name AS stock_name, s.naver_code,
            r.opinion, r.target_price, r.base_price,
            rr.hit, rr.implied_upside_pct, rr.actual_return_pct, rr.max_high_1y
     FROM report r
     JOIN stock s ON s.id = r.stock_id
     JOIN firm  f ON f.id = r.firm_id
     LEFT JOIN report_result rr ON rr.report_id = r.id
     WHERE f.name = $1
       AND (r.analyst_name = $2 OR ($2 IS NULL AND r.analyst_name IS NULL))
     ORDER BY r.report_date DESC
     LIMIT $3`,
    [firmName, analystName, limit]
  );
}

export async function getAnalystSectorScores(firmName: string, analystName: string | null) {
  return query(
    `SELECT s.name AS stock_name, s.naver_code,
            COUNT(*) AS total_reports,
            COUNT(rr.hit) AS evaluated_reports,
            SUM(CASE WHEN rr.hit THEN 1 ELSE 0 END) AS hit_count,
            CASE WHEN COUNT(rr.hit) > 0 THEN AVG(CASE WHEN rr.hit THEN 1.0 ELSE 0.0 END) ELSE NULL END AS success_rate,
            CASE WHEN COUNT(rr.hit) > 0 THEN AVG(rr.actual_return_pct) ELSE NULL END AS avg_return_pct
     FROM report r
     JOIN stock s ON s.id = r.stock_id
     JOIN firm  f ON f.id = r.firm_id
     LEFT JOIN report_result rr ON rr.report_id = r.id
     WHERE f.name = $1
       AND (r.analyst_name = $2 OR ($2 IS NULL AND r.analyst_name IS NULL))
     GROUP BY s.id
     ORDER BY evaluated_reports DESC`,
    [firmName, analystName]
  );
}

// ─── 구독자 ─────────────────────────────────────────────────────────────────

export async function upsertSubscriber(email: string, firms: string[], analysts: string[]) {
  const row = await queryOne<{ id: number }>(
    `INSERT INTO subscriber (email, firms, analysts)
     VALUES ($1, $2, $3)
     ON CONFLICT (email) DO UPDATE SET
       firms    = EXCLUDED.firms,
       analysts = EXCLUDED.analysts
     RETURNING id`,
    [email, firms, analysts]
  );
  const total = await queryOne<{ n: string }>("SELECT COUNT(*) AS n FROM subscriber");
  return { id: row?.id, total: Number(total?.n ?? 0) };
}
