"""
KOSPI 200 전체 종목 수집 + SmartEstimate 가중 컨센서스

Step 1: KOSPI 200 종목 목록 자동 수집 (KRX 공개 데이터)
Step 4: SmartEstimate — 정확도×최신성 가중 컨센서스 계산
"""

import json
import time
import sqlite3
from datetime import datetime, date, timedelta
from pathlib import Path
import requests

import db
import scoring
from pipeline import ingest_stock, cache_prices, fill_base_prices, evaluate_reports

KRX_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://www.krx.co.kr/",
}


# ─── Step 1: KOSPI 200 종목 목록 수집 ────────────────────────────────────────

def fetch_kospi200() -> list[dict]:
    """KRX 공개 API로 KOSPI 200 구성 종목 조회"""
    url = "https://www.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    payload = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT00601",
        "mktId": "STK",
        "idxIndMktTpCd": "1",
        "idxIndTpCd": "1",
        "indIdx": "1028",   # KOSPI 200
        "indIdx2": "001",
        "cvsFileYn": "N",
    }
    try:
        resp = requests.post(url, data=payload, headers=KRX_HEADERS, timeout=15)
        data = resp.json()
        stocks = []
        for item in data.get("output", []):
            code = item.get("ISU_SRT_CD", "").zfill(6)
            name = item.get("ISU_ABBRV", "")
            if code and name:
                stocks.append({
                    "naver_code": code,
                    "yahoo_ticker": f"{code}.KS",
                    "name": name,
                    "market": "KOSPI",
                })
        return stocks
    except Exception as e:
        print(f"KRX API 오류: {e}")
        return []


def get_universe() -> list[dict]:
    """
    KOSPI 200 종목 목록. KRX API 실패 시 하드코딩 fallback.
    pipeline.py의 14개 기본 종목 + 추가 86개 = 100개 tier-1 유니버스
    """
    stocks = fetch_kospi200()
    if stocks:
        print(f"KRX에서 KOSPI 200 {len(stocks)}개 종목 조회 완료")
        return stocks

    # Fallback: 주요 KOSPI 종목 100개 (시가총액 순)
    print("KRX API 실패 — fallback 종목 목록 사용")
    return [
        ("005930","005930.KS","삼성전자","KOSPI"),
        ("000660","000660.KS","SK하이닉스","KOSPI"),
        ("207940","207940.KS","삼성바이오로직스","KOSPI"),
        ("005380","005380.KS","현대차","KOSPI"),
        ("000270","000270.KS","기아","KOSPI"),
        ("051910","051910.KS","LG화학","KOSPI"),
        ("006400","006400.KS","삼성SDI","KOSPI"),
        ("035420","035420.KS","NAVER","KOSPI"),
        ("035720","035720.KS","카카오","KOSPI"),
        ("068270","068270.KS","셀트리온","KOSPI"),
        ("105560","105560.KS","KB금융","KOSPI"),
        ("055550","055550.KS","신한지주","KOSPI"),
        ("086790","086790.KS","하나금융지주","KOSPI"),
        ("032830","032830.KS","삼성생명","KOSPI"),
        ("003550","003550.KS","LG","KOSPI"),
        ("066570","066570.KS","LG전자","KOSPI"),
        ("028260","028260.KS","삼성물산","KOSPI"),
        ("012330","012330.KS","현대모비스","KOSPI"),
        ("011170","011170.KS","롯데케미칼","KOSPI"),
        ("034730","034730.KS","SK","KOSPI"),
        ("017670","017670.KS","SK텔레콤","KOSPI"),
        ("030200","030200.KS","KT","KOSPI"),
        ("010950","010950.KS","S-Oil","KOSPI"),
        ("009150","009150.KS","삼성전기","KOSPI"),
        ("018260","018260.KS","삼성에스디에스","KOSPI"),
        ("009830","009830.KS","한화솔루션","KOSPI"),
        ("036570","036570.KS","엔씨소프트","KOSPI"),
        ("251270","251270.KS","넷마블","KOSPI"),
        ("259960","259960.KS","크래프톤","KOSPI"),
        ("047810","047810.KS","한국항공우주","KOSPI"),
        ("011200","011200.KS","HMM","KOSPI"),
        ("010130","010130.KS","고려아연","KOSPI"),
        ("003490","003490.KS","대한항공","KOSPI"),
        ("000810","000810.KS","삼성화재","KOSPI"),
        ("138040","138040.KS","메리츠금융지주","KOSPI"),
        ("024110","024110.KS","기업은행","KOSPI"),
        ("316140","316140.KS","우리금융지주","KOSPI"),
        ("033780","033780.KS","KT&G","KOSPI"),
        ("097950","097950.KS","CJ제일제당","KOSPI"),
        ("000100","000100.KS","유한양행","KOSPI"),
        ("128940","128940.KS","한미약품","KOSPI"),
        ("326030","326030.KS","SK바이오팜","KOSPI"),
        ("200130","200130.KS","코스모신소재","KOSPI"),
        ("042660","042660.KS","한화오션","KOSPI"),
        ("010620","010620.KS","현대미포조선","KOSPI"),
        ("009540","009540.KS","HD한국조선해양","KOSPI"),
        ("000880","000880.KS","한화","KOSPI"),
        ("002380","002380.KS","KCC","KOSPI"),
        ("004020","004020.KS","현대제철","KOSPI"),
        ("005490","005490.KS","POSCO홀딩스","KOSPI"),
    ]


# ─── Step 4: SmartEstimate 가중 컨센서스 ──────────────────────────────────────

def build_smart_consensus(stock_id: int, conn: sqlite3.Connection) -> dict | None:
    """
    특정 종목의 최근 4개월 리포트를 정확도×최신성 가중 평균으로 컨센서스 계산.

    반환:
      weighted_target    - 가중 컨센서스 목표주가
      simple_target      - 단순 평균
      predicted_surprise - (weighted - simple) / |simple| × 100  ← I/B/E/S PS%
      analyst_count      - 참여 애널리스트 수
      coverage           - 증권사 수
    """
    cutoff = (datetime.now() - timedelta(days=120)).strftime("%Y-%m-%d")

    rows = conn.execute(
        """
        SELECT
            r.nid,
            r.firm_id,
            r.target_price,
            r.report_date,
            a.star_rating,
            a.success_rate,
            julianday('now') - julianday(r.report_date) AS days_old
        FROM report r
        JOIN firm f ON f.id = r.firm_id
        LEFT JOIN analyst_score a
            ON a.firm_id = r.firm_id AND a.analyst_name IS r.analyst_name
        WHERE r.stock_id = ?
          AND r.target_price IS NOT NULL
          AND r.report_date >= ?
        ORDER BY r.report_date DESC
        """,
        (stock_id, cutoff),
    ).fetchall()

    if not rows:
        return None

    opinions = [
        {
            "target_price": r["target_price"],
            "star_rating": r["star_rating"],
            "days_since_report": max(0, int(r["days_old"])),
        }
        for r in rows
    ]

    result = scoring.weighted_consensus(opinions)
    result["coverage"] = len(set(r["firm_id"] for r in rows))
    return result


def compute_all_consensus() -> list[dict]:
    """전체 종목 SmartEstimate 컨센서스 계산 및 출력"""
    with db.get_connection() as conn:
        stocks = conn.execute(
            "SELECT s.id, s.naver_code, s.name FROM stock s"
        ).fetchall()

        results = []
        for stock in stocks:
            consensus = build_smart_consensus(stock["id"], conn)
            if consensus and consensus.get("analyst_count", 0) >= 2:
                results.append({
                    "stock": stock["name"],
                    "code": stock["naver_code"],
                    **consensus,
                })

    # predicted_surprise 절대값 기준 정렬
    results.sort(key=lambda x: abs(x.get("predicted_surprise_pct", 0)), reverse=True)
    return results


def print_consensus_table(results: list[dict]):
    print(f"\n{'─'*85}")
    print(f"  {'종목':<12} {'단순컨센서스':>10} {'가중컨센서스':>10} {'PS%':>7} {'애널수':>5} {'커버리지':>6}")
    print(f"{'─'*85}")
    for r in results:
        ps = r.get("predicted_surprise_pct", 0)
        ps_str = f"{ps:+.1f}%"
        print(
            f"  {r['stock']:<12} "
            f"{r['simple_target']:>10,}원 "
            f"{r['weighted_target']:>10,}원 "
            f"{ps_str:>7} "
            f"{r['analyst_count']:>5}명 "
            f"{r['coverage']:>5}사"
        )


# ─── 메인 실행 ────────────────────────────────────────────────────────────────

def run_expansion(max_stocks: int = 50, pages: int = 4):
    """
    KOSPI 200 종목 확장 수집.
    이미 DB에 있는 종목은 스킵.
    """
    db.init_db()

    universe = get_universe()
    if isinstance(universe[0], dict):
        candidates = [(s["naver_code"], s["yahoo_ticker"], s["name"], s["market"])
                      for s in universe]
    else:
        candidates = universe  # tuple list

    # 이미 수집된 종목 제외
    with db.get_connection() as conn:
        existing = {r["naver_code"] for r in conn.execute("SELECT naver_code FROM stock").fetchall()}

    new_stocks = [s for s in candidates if s[0] not in existing][:max_stocks]
    print(f"\n신규 수집 대상: {len(new_stocks)}개 (기존 {len(existing)}개 스킵)")

    stock_ids = {}
    for naver_code, yahoo_ticker, name, market in new_stocks:
        sid = ingest_stock(naver_code, yahoo_ticker, name, market, pages=pages)
        stock_ids[naver_code] = (sid, yahoo_ticker)
        time.sleep(0.5)

    # 주가 캐싱
    print("\n주가 캐싱...")
    for code, (sid, ticker) in stock_ids.items():
        n = cache_prices(sid, ticker)
        print(f"  {code}: {n}일치")

    # 기준가 + 적중 계산
    fill_base_prices()
    evaluate_reports()

    # SmartEstimate 재계산
    print("\n=== SmartEstimate 가중 컨센서스 ===")
    results = compute_all_consensus()
    print_consensus_table(results)

    return results


if __name__ == "__main__":
    import sys
    if "--consensus-only" in sys.argv:
        # 수집 없이 컨센서스만 출력
        db.init_db()
        results = compute_all_consensus()
        print_consensus_table(results)
    else:
        run_expansion(max_stocks=36, pages=4)  # 기존 14개 + 36개 = 50개
