"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";

/* ─── Types ──────────────────────────────────────────────────────────────── */

interface Leader {
  firm_name: string;
  analyst_name: string | null;
  evaluated_reports: number;
  success_rate: number;
  avg_return_pct: number;
  z_score: number;
  star_rating: number | null;
  is_stat_significant: number;
  buy_hit_rate: number | null;
}

interface Consensus {
  stock_name: string;
  naver_code: string;
  simple_target: number;
  weighted_target: number;
  predicted_surprise_pct: number;
  analyst_count: number;
  coverage: number;
}

interface Stats {
  total_stocks: number;
  total_reports: number;
  evaluated_reports: number;
  total_firms: number;
  total_analysts: number;
  avg_success_rate: number;
}

/* ─── Helpers ────────────────────────────────────────────────────────────── */

const STARS: Record<number, string> = {
  5: "★★★★★",
  4: "★★★★☆",
  3: "★★★☆☆",
  2: "★★☆☆☆",
  1: "★☆☆☆☆",
};

const STAR_COLOR: Record<number, string> = {
  5: "text-yellow-400",
  4: "text-yellow-400",
  3: "text-yellow-500",
  2: "text-gray-400",
  1: "text-red-400",
};

function fmt(n: number): string {
  return n.toLocaleString("ko-KR");
}

function pct(n: number, decimals = 1): string {
  return `${n >= 0 ? "+" : ""}${n.toFixed(decimals)}%`;
}

function psColor(ps: number): string {
  if (ps >= 5) return "text-red-500 font-bold";
  if (ps >= 2) return "text-orange-500";
  if (ps <= -5) return "text-blue-500 font-bold";
  if (ps <= -2) return "text-blue-400";
  return "text-gray-500";
}

/* ─── Sub-components ─────────────────────────────────────────────────────── */

function StatCard({ label, value, sub }: { label: string; value: string | number; sub?: string }) {
  return (
    <div className="bg-white rounded-xl border border-gray-100 p-5 shadow-sm">
      <p className="text-xs text-gray-500 mb-1">{label}</p>
      <p className="text-2xl font-bold text-gray-900">{value}</p>
      {sub && <p className="text-xs text-gray-400 mt-1">{sub}</p>}
    </div>
  );
}

function StarBadge({ rating }: { rating: number | null }) {
  if (!rating) return <span className="text-gray-300 text-sm">–</span>;
  return (
    <span className={`text-sm font-medium ${STAR_COLOR[rating] ?? "text-gray-400"}`}>
      {STARS[rating]}
    </span>
  );
}

function SigBadge({ sig }: { sig: number }) {
  return sig ? (
    <span className="text-xs bg-green-50 text-green-700 border border-green-200 rounded px-1.5 py-0.5">유의</span>
  ) : (
    <span className="text-xs bg-gray-50 text-gray-400 border border-gray-200 rounded px-1.5 py-0.5">부족</span>
  );
}

/* ─── LeaderTable ────────────────────────────────────────────────────────── */

function LeaderTable({ data, showAnalyst }: { data: Leader[]; showAnalyst: boolean }) {
  const router = useRouter();
  if (!data.length) {
    return <div className="flex items-center justify-center h-32 text-gray-400 text-sm">데이터 없음</div>;
  }

  function toScorecard(row: Leader) {
    const name = row.analyst_name ?? "_";
    router.push(`/analyst/${encodeURIComponent(row.firm_name)}/${encodeURIComponent(name)}`);
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="bg-gray-50 text-xs text-gray-500 uppercase tracking-wide">
            <th className="px-4 py-3 text-left w-8">#</th>
            <th className="px-4 py-3 text-left">증권사</th>
            {showAnalyst && <th className="px-4 py-3 text-left">애널리스트</th>}
            <th className="px-4 py-3 text-center">등급</th>
            <th className="px-4 py-3 text-right">평가건수</th>
            <th className="px-4 py-3 text-right">적중률</th>
            <th className="px-4 py-3 text-right">평균수익</th>
            <th className="px-4 py-3 text-right">Z-score</th>
            <th className="px-4 py-3 text-right">매수보정</th>
            <th className="px-4 py-3 text-center">통계</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-50">
          {data.map((row, i) => (
            <tr
              key={i}
              onClick={() => toScorecard(row)}
              className="hover:bg-blue-50 cursor-pointer transition-colors"
            >
              <td className="px-4 py-3 text-gray-400 font-mono">{i + 1}</td>
              <td className="px-4 py-3 font-medium text-gray-900">{row.firm_name}</td>
              {showAnalyst && (
                <td className="px-4 py-3 text-gray-700">{row.analyst_name ?? "–"}</td>
              )}
              <td className="px-4 py-3 text-center">
                <StarBadge rating={row.star_rating} />
              </td>
              <td className="px-4 py-3 text-right text-gray-600">{row.evaluated_reports}건</td>
              <td className="px-4 py-3 text-right font-mono">
                <span className={row.success_rate >= 0.6 ? "text-green-600" : row.success_rate < 0.4 ? "text-red-500" : "text-gray-700"}>
                  {(row.success_rate * 100).toFixed(1)}%
                </span>
              </td>
              <td className="px-4 py-3 text-right font-mono">
                <span className={row.avg_return_pct >= 0 ? "text-green-600" : "text-red-500"}>
                  {pct(row.avg_return_pct)}
                </span>
              </td>
              <td className="px-4 py-3 text-right font-mono">
                <span className={row.z_score >= 1.96 ? "text-blue-600 font-semibold" : row.z_score < 0 ? "text-red-400" : "text-gray-600"}>
                  {row.z_score >= 0 ? "+" : ""}{row.z_score.toFixed(2)}
                </span>
              </td>
              <td className="px-4 py-3 text-right font-mono text-gray-500">
                {row.buy_hit_rate != null ? (row.buy_hit_rate * 100).toFixed(1) + "%" : "–"}
              </td>
              <td className="px-4 py-3 text-center">
                <SigBadge sig={row.is_stat_significant} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/* ─── ConsensusTable ──────────────────────────────────────────────────────── */

function ConsensusTable({ data }: { data: Consensus[] }) {
  if (!data.length) {
    return <div className="flex items-center justify-center h-32 text-gray-400 text-sm">데이터 없음</div>;
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="bg-gray-50 text-xs text-gray-500 uppercase tracking-wide">
            <th className="px-4 py-3 text-left">종목</th>
            <th className="px-4 py-3 text-right">단순 컨센서스</th>
            <th className="px-4 py-3 text-right">가중 컨센서스</th>
            <th className="px-4 py-3 text-right">PS%</th>
            <th className="px-4 py-3 text-right">애널리스트</th>
            <th className="px-4 py-3 text-right">커버리지</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-50">
          {data.map((row, i) => (
            <tr key={i} className="hover:bg-gray-50 transition-colors">
              <td className="px-4 py-3">
                <span className="font-medium text-gray-900">{row.stock_name}</span>
                <span className="text-xs text-gray-400 ml-2">{row.naver_code}</span>
              </td>
              <td className="px-4 py-3 text-right font-mono text-gray-500">
                {fmt(row.simple_target)}원
              </td>
              <td className="px-4 py-3 text-right font-mono font-semibold text-gray-900">
                {fmt(row.weighted_target)}원
              </td>
              <td className={`px-4 py-3 text-right font-mono font-semibold ${psColor(row.predicted_surprise_pct)}`}>
                {pct(row.predicted_surprise_pct)}
              </td>
              <td className="px-4 py-3 text-right text-gray-500">{row.analyst_count}명</td>
              <td className="px-4 py-3 text-right text-gray-500">{row.coverage}사</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/* ─── Main ───────────────────────────────────────────────────────────────── */

export default function Home() {
  const [tab, setTab] = useState<"firm" | "individual" | "consensus">("firm");
  const [firmLeaders, setFirmLeaders] = useState<Leader[]>([]);
  const [indivLeaders, setIndivLeaders] = useState<Leader[]>([]);
  const [consensus, setConsensus] = useState<Consensus[]>([]);
  const [stats, setStats] = useState<Stats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      fetch("/api/stats").then((r) => r.json()),
      fetch("/api/leaderboard?type=firm&min=5").then((r) => r.json()),
      fetch("/api/leaderboard?type=individual&min=3").then((r) => r.json()),
      fetch("/api/consensus").then((r) => r.json()),
    ]).then(([s, f, iv, c]) => {
      setStats(s);
      setFirmLeaders(f);
      setIndivLeaders(iv);
      setConsensus(c);
      setLoading(false);
    });
  }, []);

  const currentData = tab === "firm" ? firmLeaders : indivLeaders;

  return (
    <main className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 px-6 py-4">
        <div className="max-w-6xl mx-auto flex items-center justify-between">
          <div>
            <h1 className="text-xl font-bold text-gray-900">애널리스트 성적표</h1>
            <p className="text-xs text-gray-500 mt-0.5">
              한국 증권사 리서치 실적 추적 · Z-test 기반 등급 · I/B/E/S SmartEstimate 방식 컨센서스
            </p>
          </div>
          <span className="text-xs text-gray-400">{new Date().toLocaleDateString("ko-KR")} 기준</span>
        </div>
      </header>

      <div className="max-w-6xl mx-auto px-6 py-8 space-y-8">

        {/* Stats */}
        {stats && (
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-4">
            <StatCard label="추적 종목" value={`${stats.total_stocks}개`} />
            <StatCard label="수집 리포트" value={`${fmt(stats.total_reports)}건`} />
            <StatCard label="평가 완료" value={`${fmt(stats.evaluated_reports)}건`} />
            <StatCard label="증권사" value={`${stats.total_firms}곳`} />
            <StatCard label="개인 애널리스트" value={`${stats.total_analysts}명`} />
            <StatCard label="평균 적중률" value={`${stats.avg_success_rate}%`} sub="5건↑ 증권사 기준" />
          </div>
        )}

        {/* Main card */}
        <div className="bg-white rounded-xl border border-gray-100 shadow-sm overflow-hidden">
          {/* Tabs */}
          <div className="flex border-b border-gray-100">
            {(["firm", "individual", "consensus"] as const).map((t) => (
              <button
                key={t}
                onClick={() => setTab(t)}
                className={`px-5 py-3 text-sm font-medium transition-colors ${
                  tab === t
                    ? "text-blue-600 border-b-2 border-blue-600 bg-blue-50/40"
                    : "text-gray-500 hover:text-gray-700 hover:bg-gray-50"
                }`}
              >
                {t === "firm" ? "증권사 랭킹" : t === "individual" ? "개인 애널리스트" : "SmartEstimate"}
              </button>
            ))}
          </div>

          {loading ? (
            <div className="flex items-center justify-center h-40 text-gray-400 text-sm animate-pulse">
              데이터 로딩 중...
            </div>
          ) : tab === "consensus" ? (
            <ConsensusTable data={consensus} />
          ) : (
            <LeaderTable data={currentData} showAnalyst={tab === "individual"} />
          )}
        </div>

        {/* Legend */}
        <div className="text-xs text-gray-400 space-y-1 border-t border-gray-100 pt-4">
          <p>
            <strong>★ 등급</strong>: Z-test (최소 10건 필요) + 적중률 + 평균수익률 종합 ·{" "}
            <strong>유의</strong>: n≥10으로 통계적으로 유의미
          </p>
          <p>
            <strong>SmartEstimate PS%</strong>: 가중컨센서스 vs 단순컨센서스 괴리.
            |PS%| &gt; 2%면 실제 어닝 서프라이즈 방향과 70% 상관 (I/B/E/S 기반)
          </p>
          <p>
            <strong>매수보정</strong>: 매수 추천 중 실제 상승 비율 (92.9% 매수 시장에서 진짜 실력 측정)
          </p>
          <p>데이터: 네이버 금융 리서치 · Yahoo Finance KS · 적중 기준: 목표주가 1년 내 최고가 도달 여부</p>
        </div>
      </div>
    </main>
  );
}
