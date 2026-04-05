"""
Z-test 기반 애널리스트 점수 엔진

벤치마크:
  - TipRanks: success_rate + avg_return + Z-test (Cornell 대학 공동 개발)
  - I/B/E/S StarMine: 정확도×최신성 이중 가중 (Phase 2에서 추가)

핵심 공식:
  Z = (success_rate - 0.5) / sqrt(0.25 / n)
  H0: 이 애널리스트의 성공률은 랜덤(50%)과 같다
  → |Z| > 1.96 이면 p < 0.05, 즉 실력이 있다고 판단
"""

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

# 통계적 유의성 최소 요건 (TipRanks: 10~15건)
MIN_REPORTS_FOR_SIGNIFICANCE = 10


@dataclass
class ReportRow:
    firm_id: int
    analyst_name: Optional[str]
    opinion: Optional[str]   # 'BUY' | 'HOLD' | 'SELL'
    hit: Optional[bool]
    actual_return_pct: Optional[float]
    implied_upside_pct: Optional[float]


@dataclass
class AnalystScoreResult:
    firm_id: int
    analyst_name: Optional[str]
    total_reports: int
    evaluated_reports: int
    hit_count: int
    success_rate: Optional[float]
    avg_return_pct: Optional[float]
    z_score: Optional[float]
    star_rating: Optional[int]
    is_stat_significant: bool
    # 매수 편향 보정 (한국 특화)
    buy_total: int
    buy_hit_count: int
    buy_hit_rate: Optional[float]
    calculated_at: str


# ─── 핵심 계산 함수 ──────────────────────────────────────────────────────────

def calc_z_score(success_rate: float, n: int) -> float:
    """
    단측 Z-test (H0: p = 0.5)
    Z = (p̂ - p0) / sqrt(p0(1-p0)/n)
    p0 = 0.5이므로 분모는 sqrt(0.25/n)으로 단순화
    """
    if n == 0:
        return 0.0
    return (success_rate - 0.5) / math.sqrt(0.25 / n)


def assign_star_rating(success_rate: float, avg_return: float, z_score: float) -> int:
    """
    TipRanks 방식 참고, 한국 시장 조정값 적용.
    데이터가 충분히 쌓이면 분위수(percentile) 기반으로 전환 예정.

    5★  success_rate ≥ 70%  AND  z_score ≥ 2.0  AND  avg_return ≥ 10%
    4★  success_rate ≥ 60%  AND  z_score ≥ 1.5  AND  avg_return ≥  5%
    3★  success_rate ≥ 55%  AND  z_score ≥ 1.0
    2★  success_rate ≥ 45%
    1★  그 외
    """
    if success_rate >= 0.70 and z_score >= 2.0 and avg_return >= 10.0:
        return 5
    if success_rate >= 0.60 and z_score >= 1.5 and avg_return >= 5.0:
        return 4
    if success_rate >= 0.55 and z_score >= 1.0:
        return 3
    if success_rate >= 0.45:
        return 2
    return 1


def score_analyst(rows: list[ReportRow], firm_id: int,
                  analyst_name: Optional[str]) -> AnalystScoreResult:
    """
    단일 (firm, analyst) 단위의 점수 계산.
    rows: 해당 애널리스트의 모든 평가 완료 리포트.
    """
    now = datetime.now().isoformat(timespec="seconds")

    evaluated = [r for r in rows if r.hit is not None and r.actual_return_pct is not None]
    n = len(evaluated)
    is_sig = n >= MIN_REPORTS_FOR_SIGNIFICANCE

    # 매수 편향 보정 지표
    buy_rows = [r for r in evaluated if r.opinion == "BUY"]
    buy_total = len(buy_rows)
    buy_hit_count = sum(1 for r in buy_rows if r.hit)
    buy_hit_rate = round(buy_hit_count / buy_total, 4) if buy_total > 0 else None

    if n == 0:
        return AnalystScoreResult(
            firm_id=firm_id,
            analyst_name=analyst_name,
            total_reports=len(rows),
            evaluated_reports=0,
            hit_count=0,
            success_rate=None,
            avg_return_pct=None,
            z_score=None,
            star_rating=None,
            is_stat_significant=False,
            buy_total=buy_total,
            buy_hit_count=buy_hit_count,
            buy_hit_rate=buy_hit_rate,
            calculated_at=now,
        )

    hit_count = sum(1 for r in evaluated if r.hit)
    success_rate = hit_count / n
    avg_return = sum(r.actual_return_pct for r in evaluated) / n
    z = calc_z_score(success_rate, n)
    star = assign_star_rating(success_rate, avg_return, z) if is_sig else None

    return AnalystScoreResult(
        firm_id=firm_id,
        analyst_name=analyst_name,
        total_reports=len(rows),
        evaluated_reports=n,
        hit_count=hit_count,
        success_rate=round(success_rate, 4),
        avg_return_pct=round(avg_return, 2),
        z_score=round(z, 3),
        star_rating=star,
        is_stat_significant=is_sig,
        buy_total=buy_total,
        buy_hit_count=buy_hit_count,
        buy_hit_rate=buy_hit_rate,
        calculated_at=now,
    )


# ─── 배치 점수 계산 ──────────────────────────────────────────────────────────

def score_all(rows: list[ReportRow]) -> list[AnalystScoreResult]:
    """
    DB에서 가져온 전체 평가 완료 리포트를 (firm_id, analyst_name) 단위로 묶어
    일괄 점수 계산.
    """
    from collections import defaultdict

    groups: dict[tuple, list[ReportRow]] = defaultdict(list)
    for r in rows:
        groups[(r.firm_id, r.analyst_name)].append(r)

    results = []
    for (firm_id, analyst_name), group_rows in groups.items():
        result = score_analyst(group_rows, firm_id, analyst_name)
        results.append(result)

    # Z-score 내림차순 정렬
    results.sort(key=lambda r: (r.z_score or -999), reverse=True)
    return results


# ─── SmartEstimate 가중 컨센서스 (I/B/E/S Phase 2) ───────────────────────────

def smart_estimate_weight(star_rating: Optional[int], days_since_report: int,
                          max_age_days: int = 120) -> float:
    """
    I/B/E/S StarMine 방식: 정확도 점수 × 최신성 점수
    - 정확도: 스타등급(1~5) → 0.2~1.0 선형 매핑
    - 최신성: max_age_days(120일) 초과 시 0, 선형 감쇠
    """
    if days_since_report > max_age_days:
        return 0.0

    accuracy_score = (star_rating / 5.0) if star_rating else 0.2
    recency_score = 1.0 - (days_since_report / max_age_days)
    return round(accuracy_score * recency_score, 4)


def weighted_consensus(opinions: list[dict]) -> dict:
    """
    가중 컨센서스 목표주가 계산.
    opinions: [{"target_price": int, "star_rating": int, "days_since_report": int}]

    반환: {
        "weighted_target": float,   # 가중 컨센서스 목표주가
        "simple_target": float,     # 단순 평균
        "predicted_surprise_pct": float,  # I/B/E/S PS%
        "analyst_count": int,
    }
    """
    valid = [o for o in opinions if o.get("target_price") and o.get("days_since_report") is not None]
    if not valid:
        return {}

    weights = [
        smart_estimate_weight(o.get("star_rating"), o["days_since_report"])
        for o in valid
    ]
    total_weight = sum(weights)

    simple_avg = sum(o["target_price"] for o in valid) / len(valid)

    if total_weight == 0:
        return {
            "weighted_target": simple_avg,
            "simple_target": simple_avg,
            "predicted_surprise_pct": 0.0,
            "analyst_count": len(valid),
        }

    weighted_avg = sum(o["target_price"] * w for o, w in zip(valid, weights)) / total_weight
    predicted_surprise_pct = (weighted_avg - simple_avg) / abs(simple_avg) * 100

    return {
        "weighted_target": round(weighted_avg),
        "simple_target": round(simple_avg),
        "predicted_surprise_pct": round(predicted_surprise_pct, 2),
        "analyst_count": len(valid),
    }


# ─── 출력 헬퍼 ───────────────────────────────────────────────────────────────

STARS = {5: "★★★★★", 4: "★★★★☆", 3: "★★★☆☆", 2: "★★☆☆☆", 1: "★☆☆☆☆"}


def format_score(result: AnalystScoreResult, firm_name: str = "") -> str:
    stars = STARS.get(result.star_rating, "  N/A ") if result.star_rating else "  N/A "
    sig_tag = "[유의]" if result.is_stat_significant else "[부족]"
    analyst = result.analyst_name or "(증권사 전체)"
    name = f"{firm_name or result.firm_id} / {analyst}"

    if result.evaluated_reports == 0:
        return f"{sig_tag} {name:<30} | 평가 데이터 없음"

    return (
        f"{sig_tag} {stars} {name:<30} | "
        f"평가 {result.evaluated_reports:>3}건 | "
        f"적중률 {result.success_rate*100:>5.1f}% | "
        f"평균수익 {result.avg_return_pct:>+6.1f}% | "
        f"Z={result.z_score:>+5.2f} | "
        f"매수보정 {result.buy_hit_rate*100:.1f}%" if result.buy_hit_rate else
        f"{sig_tag} {stars} {name:<30} | "
        f"평가 {result.evaluated_reports:>3}건 | "
        f"적중률 {result.success_rate*100:>5.1f}% | "
        f"평균수익 {result.avg_return_pct:>+6.1f}% | "
        f"Z={result.z_score:>+5.2f}"
    )
