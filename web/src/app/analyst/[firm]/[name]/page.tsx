"use client";

import { useEffect, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import Link from "next/link";

/* ─── Types ──────────────────────────────────────────────────────────────── */

interface AnalystDetail {
  firm_name: string;
  analyst_name: string | null;
  evaluated_reports: number;
  hit_count: number;
  success_rate: number;
  avg_return_pct: number;
  z_score: number;
  star_rating: number | null;
  is_stat_significant: number;
  buy_total: number;
  buy_hit_count: number;
  buy_hit_rate: number | null;
  calculated_at: string;
}

interface ReportRow {
  report_date: string;
  stock_name: string;
  naver_code: string;
  opinion: string | null;
  target_price: number | null;
  base_price: number | null;
  hit: number | null;
  implied_upside_pct: number | null;
  actual_return_pct: number | null;
  max_high_1y: number | null;
}

interface SectorScore {
  stock_name: string;
  naver_code: string;
  total_reports: number;
  evaluated_reports: number;
  hit_count: number;
  success_rate: number | null;
  avg_return_pct: number | null;
}

/* ─── Helpers ────────────────────────────────────────────────────────────── */

const STARS: Record<number, string> = { 5: "★★★★★", 4: "★★★★☆", 3: "★★★☆☆", 2: "★★☆☆☆", 1: "★☆☆☆☆" };
const STAR_COLOR: Record<number, string> = { 5: "text-yellow-400", 4: "text-yellow-400", 3: "text-yellow-500", 2: "text-gray-400", 1: "text-red-400" };

function fmt(n: number) { return n.toLocaleString("ko-KR"); }
function pct(n: number | null, decimals = 1) {
  if (n == null) return "–";
  return `${n >= 0 ? "+" : ""}${n.toFixed(decimals)}%`;
}

function HitBadge({ hit }: { hit: number | null }) {
  if (hit === null) return <span className="text-gray-300 text-xs">평가중</span>;
  return hit
    ? <span className="text-xs bg-green-50 text-green-700 border border-green-200 rounded px-1.5 py-0.5">적중</span>
    : <span className="text-xs bg-red-50 text-red-500 border border-red-200 rounded px-1.5 py-0.5">미달</span>;
}

/* ─── Scorecard Header ───────────────────────────────────────────────────── */

function ScorecardHeader({ score }: { score: AnalystDetail }) {
  const stars = score.star_rating ? STARS[score.star_rating] : null;
  const starColor = score.star_rating ? STAR_COLOR[score.star_rating] : "text-gray-300";
  const displayName = score.analyst_name ?? "(증권사 전체)";

  return (
    <div className="bg-white rounded-xl border border-gray-100 shadow-sm p-6">
      <div className="flex items-start justify-between">
        <div>
          <p className="text-sm text-gray-500 mb-1">{score.firm_name}</p>
          <h1 className="text-2xl font-bold text-gray-900">{displayName}</h1>
          {stars && (
            <p className={`text-lg mt-1 ${starColor}`}>{stars}</p>
          )}
        </div>
        <div className="text-right">
          {score.is_stat_significant ? (
            <span className="text-xs bg-blue-50 text-blue-700 border border-blue-200 rounded px-2 py-1">통계적으로 유의</span>
          ) : (
            <span className="text-xs bg-gray-50 text-gray-400 border border-gray-200 rounded px-2 py-1">데이터 부족 (n&lt;10)</span>
          )}
          <p className="text-xs text-gray-400 mt-2">{score.calculated_at.slice(0, 10)} 계산</p>
        </div>
      </div>

      {/* 핵심 지표 */}
      <div className="grid grid-cols-2 sm:grid-cols-5 gap-4 mt-6">
        <Metric label="평가 리포트" value={`${score.evaluated_reports}건`} />
        <Metric
          label="적중률"
          value={`${(score.success_rate * 100).toFixed(1)}%`}
          color={score.success_rate >= 0.6 ? "text-green-600" : score.success_rate < 0.4 ? "text-red-500" : undefined}
        />
        <Metric
          label="평균 수익률"
          value={pct(score.avg_return_pct)}
          color={score.avg_return_pct >= 0 ? "text-green-600" : "text-red-500"}
        />
        <Metric
          label="Z-score"
          value={`${score.z_score >= 0 ? "+" : ""}${score.z_score.toFixed(2)}`}
          color={score.z_score >= 1.96 ? "text-blue-600" : score.z_score < 0 ? "text-red-400" : undefined}
          sub="p<0.05 기준: ±1.96"
        />
        <Metric
          label="매수 보정률"
          value={score.buy_hit_rate != null ? `${(score.buy_hit_rate * 100).toFixed(1)}%` : "–"}
          sub={`${score.buy_hit_count}/${score.buy_total}건`}
        />
      </div>
    </div>
  );
}

function Metric({ label, value, color, sub }: { label: string; value: string; color?: string; sub?: string }) {
  return (
    <div className="bg-gray-50 rounded-lg p-4">
      <p className="text-xs text-gray-500 mb-1">{label}</p>
      <p className={`text-xl font-bold ${color ?? "text-gray-900"}`}>{value}</p>
      {sub && <p className="text-xs text-gray-400 mt-0.5">{sub}</p>}
    </div>
  );
}

/* ─── Sector Heatmap ─────────────────────────────────────────────────────── */

function SectorHeatmap({ sectors }: { sectors: SectorScore[] }) {
  if (!sectors.length) return null;

  const maxReports = Math.max(...sectors.map((s) => s.evaluated_reports), 1);

  return (
    <div className="bg-white rounded-xl border border-gray-100 shadow-sm p-6">
      <h2 className="text-sm font-semibold text-gray-700 mb-4">종목별 전문성</h2>
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3">
        {sectors.map((s) => {
          const rate = s.success_rate ?? 0;
          const hasData = s.evaluated_reports > 0;
          const bg = !hasData
            ? "bg-gray-50"
            : rate >= 0.7 ? "bg-green-50 border-green-200"
            : rate >= 0.5 ? "bg-yellow-50 border-yellow-200"
            : "bg-red-50 border-red-200";

          return (
            <div key={s.naver_code} className={`rounded-lg border p-3 ${bg}`}>
              <p className="text-xs font-medium text-gray-700 truncate">{s.stock_name}</p>
              {hasData ? (
                <>
                  <p className="text-lg font-bold mt-1">
                    {(rate * 100).toFixed(0)}%
                  </p>
                  <p className="text-xs text-gray-500">
                    {s.hit_count}/{s.evaluated_reports}건 · {pct(s.avg_return_pct, 0)}
                  </p>
                </>
              ) : (
                <p className="text-xs text-gray-400 mt-1">리포트 {s.total_reports}건 (미평가)</p>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

/* ─── Report History ─────────────────────────────────────────────────────── */

function ReportHistory({ reports }: { reports: ReportRow[] }) {
  return (
    <div className="bg-white rounded-xl border border-gray-100 shadow-sm overflow-hidden">
      <div className="px-6 py-4 border-b border-gray-50">
        <h2 className="text-sm font-semibold text-gray-700">리포트 이력 (최근 50건)</h2>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-50 text-xs text-gray-500">
              <th className="px-4 py-3 text-left">날짜</th>
              <th className="px-4 py-3 text-left">종목</th>
              <th className="px-4 py-3 text-center">의견</th>
              <th className="px-4 py-3 text-right">목표가</th>
              <th className="px-4 py-3 text-right">기준가</th>
              <th className="px-4 py-3 text-right">암시수익</th>
              <th className="px-4 py-3 text-right">1년최고</th>
              <th className="px-4 py-3 text-right">실제수익</th>
              <th className="px-4 py-3 text-center">결과</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-50">
            {reports.map((r, i) => (
              <tr key={i} className="hover:bg-gray-50 transition-colors">
                <td className="px-4 py-3 text-gray-500 font-mono text-xs">{r.report_date}</td>
                <td className="px-4 py-3 font-medium text-gray-900">{r.stock_name}</td>
                <td className="px-4 py-3 text-center">
                  <span className={`text-xs rounded px-1.5 py-0.5 ${
                    r.opinion === "BUY" ? "bg-blue-50 text-blue-700" :
                    r.opinion === "SELL" ? "bg-red-50 text-red-600" :
                    "bg-gray-100 text-gray-500"
                  }`}>
                    {r.opinion ?? "–"}
                  </span>
                </td>
                <td className="px-4 py-3 text-right font-mono text-gray-700">
                  {r.target_price ? fmt(r.target_price) : "–"}
                </td>
                <td className="px-4 py-3 text-right font-mono text-gray-500">
                  {r.base_price ? fmt(r.base_price) : "–"}
                </td>
                <td className="px-4 py-3 text-right font-mono text-gray-500">
                  {pct(r.implied_upside_pct)}
                </td>
                <td className="px-4 py-3 text-right font-mono text-gray-500">
                  {r.max_high_1y ? fmt(r.max_high_1y) : "–"}
                </td>
                <td className="px-4 py-3 text-right font-mono">
                  <span className={r.actual_return_pct != null && r.actual_return_pct >= 0 ? "text-green-600" : "text-red-500"}>
                    {pct(r.actual_return_pct)}
                  </span>
                </td>
                <td className="px-4 py-3 text-center">
                  <HitBadge hit={r.hit} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/* ─── Main Page ──────────────────────────────────────────────────────────── */

export default function AnalystPage() {
  const params = useParams();
  const router = useRouter();
  const firm = decodeURIComponent(params.firm as string);
  const name = decodeURIComponent(params.name as string);

  const [data, setData] = useState<{ score: AnalystDetail; reports: ReportRow[]; sectors: SectorScore[] } | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch(`/api/analyst/${encodeURIComponent(firm)}/${encodeURIComponent(name)}`)
      .then((r) => r.json())
      .then((d) => {
        if (d.error) setError(d.error);
        else setData(d);
      })
      .catch(() => setError("데이터를 불러올 수 없습니다."));
  }, [firm, name]);

  return (
    <main className="min-h-screen bg-gray-50">
      <header className="bg-white border-b border-gray-200 px-6 py-4">
        <div className="max-w-5xl mx-auto flex items-center gap-3">
          <Link href="/" className="text-gray-400 hover:text-gray-700 text-sm">← 전체 랭킹</Link>
          <span className="text-gray-200">/</span>
          <span className="text-sm text-gray-600">스코어카드</span>
        </div>
      </header>

      <div className="max-w-5xl mx-auto px-6 py-8 space-y-6">
        {error && (
          <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-red-600 text-sm">{error}</div>
        )}
        {!data && !error && (
          <div className="flex items-center justify-center h-40 text-gray-400 text-sm animate-pulse">
            불러오는 중...
          </div>
        )}
        {data && (
          <>
            <ScorecardHeader score={data.score} />
            <SectorHeatmap sectors={data.sectors} />
            <ReportHistory reports={data.reports} />
          </>
        )}
      </div>
    </main>
  );
}
