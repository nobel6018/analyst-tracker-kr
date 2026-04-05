import Database from "better-sqlite3";
import path from "path";

// web/ 기준: analyst-tracker-kr/web → analyst-tracker-kr → leedo → analyst-poc
// 실제 DB: ~/leedo/analyst-poc/analyst.db
const DB_PATH =
  process.env.DB_PATH ||
  path.join(process.cwd(), "../../analyst-poc/analyst.db");

let _db: Database.Database | null = null;

export function getDb(): Database.Database {
  if (!_db) {
    _db = new Database(DB_PATH, { readonly: true });
    _db.pragma("journal_mode = WAL");
  }
  return _db;
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
  is_stat_significant: number;
  buy_hit_rate: number | null;
  calculated_at: string;
}

export function getLeaderboard(minReports = 5, limit = 30): LeaderEntry[] {
  return getDb()
    .prepare(
      `SELECT f.name AS firm_name, s.*
       FROM analyst_score s
       JOIN firm f ON f.id = s.firm_id
       WHERE s.evaluated_reports >= ?
         AND s.analyst_name IS NULL       -- 증권사 전체 점수만
       ORDER BY s.z_score DESC NULLS LAST
       LIMIT ?`
    )
    .all(minReports, limit) as LeaderEntry[];
}

export function getIndividualLeaderboard(minReports = 3, limit = 30): LeaderEntry[] {
  return getDb()
    .prepare(
      `SELECT f.name AS firm_name, s.*
       FROM analyst_score s
       JOIN firm f ON f.id = s.firm_id
       WHERE s.evaluated_reports >= ?
         AND s.analyst_name IS NOT NULL   -- 개인 점수만
       ORDER BY s.z_score DESC NULLS LAST
       LIMIT ?`
    )
    .all(minReports, limit) as LeaderEntry[];
}

// ─── 종목 컨센서스 ─────────────────────────────────────────────────────────

export interface ConsensusEntry {
  stock_name: string;
  naver_code: string;
  simple_target: number;
  weighted_target: number;
  predicted_surprise_pct: number;
  analyst_count: number;
  coverage: number;
}

export function getConsensus(): ConsensusEntry[] {
  // 최근 120일 리포트 기준 가중 컨센서스
  const rows = getDb()
    .prepare(
      `SELECT
         s.name AS stock_name,
         s.naver_code,
         r.target_price,
         julianday('now') - julianday(r.report_date) AS days_old,
         a.star_rating,
         r.firm_id
       FROM report r
       JOIN stock s ON s.id = r.stock_id
       LEFT JOIN analyst_score a ON a.firm_id = r.firm_id AND a.analyst_name IS NULL
       WHERE r.target_price IS NOT NULL
         AND r.report_date >= date('now', '-120 days')
       ORDER BY s.name, r.report_date DESC`
    )
    .all() as any[];

  // 종목별 그룹화 후 가중 컨센서스 계산
  const byStock = new Map<string, any[]>();
  for (const row of rows) {
    const key = row.naver_code;
    if (!byStock.has(key)) byStock.set(key, []);
    byStock.get(key)!.push(row);
  }

  const results: ConsensusEntry[] = [];
  for (const [code, items] of byStock.entries()) {
    if (items.length < 2) continue;

    const stock_name = items[0].stock_name;
    const totalW = items.reduce((s, r) => s + weight(r.star_rating, r.days_old), 0);
    const simpleAvg = items.reduce((s, r) => s + r.target_price, 0) / items.length;
    const weightedAvg =
      totalW > 0
        ? items.reduce((s, r) => s + r.target_price * weight(r.star_rating, r.days_old), 0) / totalW
        : simpleAvg;

    results.push({
      stock_name,
      naver_code: code,
      simple_target: Math.round(simpleAvg),
      weighted_target: Math.round(weightedAvg),
      predicted_surprise_pct: +((weightedAvg - simpleAvg) / Math.abs(simpleAvg) * 100).toFixed(2),
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
  const recency = 1 - daysOld / MAX_AGE;
  return accuracy * recency;
}

// ─── 통계 요약 ─────────────────────────────────────────────────────────────

export interface Stats {
  total_stocks: number;
  total_reports: number;
  evaluated_reports: number;
  total_firms: number;
  total_analysts: number;
  avg_success_rate: number;
}

export function getStats(): Stats {
  const db = getDb();
  return {
    total_stocks: (db.prepare("SELECT COUNT(*) AS n FROM stock").get() as any).n,
    total_reports: (db.prepare("SELECT COUNT(*) AS n FROM report").get() as any).n,
    evaluated_reports: (db.prepare("SELECT COUNT(*) AS n FROM report_result WHERE hit IS NOT NULL").get() as any).n,
    total_firms: (db.prepare("SELECT COUNT(*) AS n FROM firm").get() as any).n,
    total_analysts: (db.prepare("SELECT COUNT(DISTINCT analyst_name || CAST(firm_id AS TEXT)) AS n FROM report WHERE analyst_name IS NOT NULL").get() as any).n,
    avg_success_rate: +(((db.prepare("SELECT AVG(success_rate) AS v FROM analyst_score WHERE evaluated_reports >= 5 AND analyst_name IS NULL").get() as any).v * 100).toFixed(1)),
  };
}
