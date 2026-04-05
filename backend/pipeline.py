"""
메인 파이프라인
1. 크롤링 (네이버 금융 리서치 리포트)
2. DB 저장
3. 주가 캐싱 (Yahoo Finance)
4. 적중 계산
5. 점수 계산 (Z-test)

실행:
  python pipeline.py             # 등록된 전체 종목
  python pipeline.py --stock 005930  # 특정 종목만
"""

import argparse
import re
import time
from datetime import datetime, timedelta

import requests
import yfinance as yf
from bs4 import BeautifulSoup

import db
import scoring
from scoring import ReportRow

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://finance.naver.com/",
}

# KOSPI 200 주요 종목 (PoC 확장용 - 단계적으로 추가)
STOCK_UNIVERSE = [
    # 반도체
    ("005930", "005930.KS", "삼성전자",  "KOSPI"),
    ("000660", "000660.KS", "SK하이닉스", "KOSPI"),
    # 플랫폼/IT
    ("035420", "035420.KS", "NAVER",     "KOSPI"),
    ("035720", "035720.KS", "카카오",     "KOSPI"),
    # 자동차
    ("005380", "005380.KS", "현대차",     "KOSPI"),
    ("000270", "000270.KS", "기아",       "KOSPI"),
    # 바이오
    ("207940", "207940.KS", "삼성바이오로직스", "KOSPI"),
    ("068270", "068270.KS", "셀트리온",   "KOSPI"),
    # 금융
    ("105560", "105560.KS", "KB금융",     "KOSPI"),
    ("055550", "055550.KS", "신한지주",   "KOSPI"),
    # 화학/소재
    ("051910", "051910.KS", "LG화학",     "KOSPI"),
    ("006400", "006400.KS", "삼성SDI",    "KOSPI"),
    # 엔터/게임
    ("036570", "036570.KS", "엔씨소프트", "KOSPI"),
    ("251270", "251270.KS", "넷마블",     "KOSPI"),
]


# ─── Step 1. 크롤링 ──────────────────────────────────────────────────────────

def crawl_report_list(naver_code: str, pages: int = 5) -> list[dict]:
    """네이버 금융 리서치 목록 수집 (여러 페이지)"""
    reports = []
    for page in range(1, pages + 1):
        url = (
            f"https://finance.naver.com/research/company_list.naver"
            f"?searchType=itemCode&itemCode={naver_code}&page={page}"
        )
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            soup = BeautifulSoup(resp.text, "html.parser")
            table = soup.find("table", class_="type_1")
            if not table:
                break

            page_has_data = False
            for row in table.find_all("tr"):
                cols = row.find_all("td")
                if len(cols) < 4:
                    continue

                nid = None
                for a in row.find_all("a", href=True):
                    m = re.search(r"nid=(\d+)", a["href"])
                    if m:
                        nid = m.group(1)
                        break
                if not nid:
                    continue

                date_raw = cols[4].get_text(strip=True) if len(cols) > 4 else ""
                date = _parse_date(date_raw)
                if not date:
                    continue

                reports.append({
                    "nid": nid,
                    "firm": cols[2].get_text(strip=True),
                    "title": cols[1].get_text(strip=True),
                    "date": date,
                })
                page_has_data = True

            if not page_has_data:
                break
        except Exception as e:
            print(f"  ⚠ 리스트 크롤링 오류 (page={page}): {e}")
            break

        time.sleep(0.3)
    return reports


def crawl_report_detail(nid: str, naver_code: str) -> dict:
    """네이버 금융 리포트 상세: 목표주가 + 투자의견"""
    url = (
        f"https://finance.naver.com/research/company_read.naver"
        f"?nid={nid}&searchType=itemCode&itemCode={naver_code}"
    )
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(resp.text, "html.parser")
        for td in soup.find_all("td"):
            text = td.get_text(strip=True)
            if "목표가" in text and "투자의견" in text:
                t = re.search(r"목표가([0-9,]+)", text)
                o = re.search(r"투자의견(.+)", text)
                return {
                    "target_price": int(t.group(1).replace(",", "")) if t else None,
                    "opinion_raw": o.group(1).strip() if o else None,
                }
    except Exception:
        pass
    return {"target_price": None, "opinion_raw": None}


def _parse_date(raw: str) -> str | None:
    m = re.match(r"(\d{2})\.(\d{2})\.(\d{2})", raw)
    if not m:
        return None
    yy, mm, dd = m.groups()
    return f"20{yy}-{mm}-{dd}"


# ─── Step 2. DB 저장 ──────────────────────────────────────────────────────────

def ingest_stock(naver_code: str, yahoo_ticker: str, name: str, market: str,
                 pages: int = 5):
    print(f"\n[{name}] 수집 중...")

    with db.transaction() as conn:
        stock_id = db.upsert_stock(conn, naver_code, yahoo_ticker, name, market)

    reports = crawl_report_list(naver_code, pages=pages)
    print(f"  리포트 목록: {len(reports)}건")

    stored = 0
    for r in reports:
        detail = crawl_report_detail(r["nid"], naver_code)
        time.sleep(0.15)

        with db.transaction() as conn:
            firm_id = db.upsert_firm(conn, r["firm"])
            db.upsert_report(
                conn,
                nid=r["nid"],
                stock_id=stock_id,
                firm_id=firm_id,
                analyst_name=None,
                title=r["title"],
                opinion_raw=detail.get("opinion_raw"),
                target_price=detail.get("target_price"),
                base_price=None,  # Step 3에서 주가 캐싱 후 채움
                report_date=r["date"],
            )
        stored += 1

    print(f"  DB 저장: {stored}건")
    return stock_id


# ─── Step 3. 주가 캐싱 + 기준가 채우기 ──────────────────────────────────────

def cache_prices(stock_id: int, yahoo_ticker: str,
                 start_date: str = "2022-01-01"):
    """Yahoo Finance 일별 OHLCV를 stock_price 테이블에 캐싱"""
    try:
        end = datetime.now().strftime("%Y-%m-%d")
        hist = yf.Ticker(yahoo_ticker).history(start=start_date, end=end)
        if hist.empty:
            return 0

        count = 0
        with db.transaction() as conn:
            for dt, row in hist.iterrows():
                db.upsert_price(
                    conn,
                    stock_id=stock_id,
                    price_date=dt.strftime("%Y-%m-%d"),
                    close_price=round(float(row["Close"])),
                    high_price=round(float(row["High"])),
                    low_price=round(float(row["Low"])),
                    open_price=round(float(row["Open"])),
                    volume=int(row["Volume"]),
                )
                count += 1
        return count
    except Exception as e:
        print(f"  ⚠ 주가 캐싱 오류: {e}")
        return 0


def fill_base_prices():
    """report.base_price가 NULL인 행을 stock_price 캐시로 채움"""
    with db.get_connection() as conn:
        rows = conn.execute(
            """
            SELECT r.id, r.stock_id, r.report_date
            FROM report r
            WHERE r.base_price IS NULL
              AND r.target_price IS NOT NULL
            """
        ).fetchall()

    updated = 0
    for row in rows:
        with db.transaction() as conn:
            price_row = db.get_price(conn, row["stock_id"], row["report_date"])
            if price_row:
                conn.execute(
                    "UPDATE report SET base_price = ?, updated_at = datetime('now') WHERE id = ?",
                    (price_row["close_price"], row["id"]),
                )
                updated += 1
    return updated


# ─── Step 4. 적중 계산 ──────────────────────────────────────────────────────

def evaluate_reports():
    """평가 미완료 리포트에 대해 적중 여부 + 수익률 계산"""
    with db.get_connection() as conn:
        pending = db.get_reports_without_result(conn)

    print(f"\n적중 계산 대상: {len(pending)}건")
    done = 0
    for r in pending:
        report_date = r["report_date"]
        start_dt = datetime.strptime(report_date, "%Y-%m-%d")
        end_dt = min(start_dt + timedelta(days=365), datetime.now())
        is_final = end_dt < datetime.now() - timedelta(days=1)
        eval_date = end_dt.strftime("%Y-%m-%d")

        with db.transaction() as conn:
            max_high, last_close = db.get_max_high(
                conn, r["stock_id"], report_date, eval_date
            )

        if max_high is None or last_close is None or r["base_price"] is None:
            continue

        target = r["target_price"]
        base = r["base_price"]
        hit = max_high >= target
        implied = round((target - base) / base * 100, 2)
        actual = round((last_close - base) / base * 100, 2)

        with db.transaction() as conn:
            db.upsert_report_result(
                conn,
                report_id=r["id"],
                max_high_1y=max_high,
                last_close_1y=last_close,
                hit=hit,
                implied_upside_pct=implied,
                actual_return_pct=actual,
                evaluation_date=eval_date,
                is_final=is_final,
            )
        done += 1

    print(f"  적중 계산 완료: {done}건")
    return done


# ─── Step 5. 점수 계산 ──────────────────────────────────────────────────────

def recalculate_scores():
    """전체 애널리스트 점수 재계산 후 DB 저장"""
    with db.get_connection() as conn:
        rows = db.get_reports_for_scoring(conn)

    report_rows = [
        ReportRow(
            firm_id=r["firm_id"],
            analyst_name=r["analyst_name"],
            opinion=r["opinion"],
            hit=bool(r["hit"]) if r["hit"] is not None else None,
            actual_return_pct=r["actual_return_pct"],
            implied_upside_pct=r["implied_upside_pct"],
        )
        for r in rows
    ]

    results = scoring.score_all(report_rows)

    with db.transaction() as conn:
        # firm_name 조회용 맵
        firm_map = {f["id"]: f["name"] for f in conn.execute("SELECT id, name FROM firm").fetchall()}

        for res in results:
            db.upsert_analyst_score(
                conn,
                firm_id=res.firm_id,
                analyst_name=res.analyst_name,
                total_reports=res.total_reports,
                evaluated_reports=res.evaluated_reports,
                hit_count=res.hit_count,
                success_rate=res.success_rate,
                avg_return_pct=res.avg_return_pct,
                z_score=res.z_score,
                star_rating=res.star_rating,
                is_stat_significant=res.is_stat_significant,
                buy_total=res.buy_total,
                buy_hit_count=res.buy_hit_count,
                buy_hit_rate=res.buy_hit_rate,
                calculated_at=res.calculated_at,
            )

    print(f"\n점수 계산 완료: {len(results)}개 (firm, analyst) 단위")

    # 리더보드 출력
    with db.get_connection() as conn:
        leaders = db.get_leaderboard(conn, min_reports=3, limit=15)
        firm_map = {f["id"]: f["name"] for f in conn.execute("SELECT id, name FROM firm").fetchall()}

    print(f"\n{'─'*80}")
    print("  리더보드 (평가 3건 이상)")
    print(f"{'─'*80}")
    for r in leaders:
        fmt = scoring.format_score(
            scoring.AnalystScoreResult(
                firm_id=r["firm_id"],
                analyst_name=r["analyst_name"],
                total_reports=r["total_reports"],
                evaluated_reports=r["evaluated_reports"],
                hit_count=r["hit_count"],
                success_rate=r["success_rate"],
                avg_return_pct=r["avg_return_pct"],
                z_score=r["z_score"],
                star_rating=r["star_rating"],
                is_stat_significant=bool(r["is_stat_significant"]),
                buy_total=r["buy_total"],
                buy_hit_count=r["buy_hit_count"],
                buy_hit_rate=r["buy_hit_rate"],
                calculated_at=r["calculated_at"],
            ),
            firm_name=firm_map.get(r["firm_id"], "?"),
        )
        print(f"  {fmt}")


# ─── CLI 진입점 ──────────────────────────────────────────────────────────────

def run(target_code: str | None = None, pages: int = 5):
    db.init_db()

    stocks = (
        [s for s in STOCK_UNIVERSE if s[0] == target_code]
        if target_code
        else STOCK_UNIVERSE
    )

    if not stocks:
        print(f"종목 코드 '{target_code}'를 찾을 수 없습니다.")
        return

    print(f"\n{'='*60}")
    print(f"파이프라인 시작: {len(stocks)}개 종목")
    print(f"{'='*60}")

    # 1+2. 크롤링 + DB 저장
    stock_ids = {}
    for naver_code, yahoo_ticker, name, market in stocks:
        sid = ingest_stock(naver_code, yahoo_ticker, name, market, pages=pages)
        stock_ids[naver_code] = (sid, yahoo_ticker)

    # 3. 주가 캐싱
    print(f"\n주가 캐싱 중...")
    for naver_code, (sid, ticker) in stock_ids.items():
        n = cache_prices(sid, ticker)
        print(f"  {naver_code}: {n}일치 캐싱")

    # 기준가 채우기
    filled = fill_base_prices()
    print(f"  기준가 채움: {filled}건")

    # 4. 적중 계산
    evaluate_reports()

    # 5. 점수 계산
    recalculate_scores()

    print(f"\n{'='*60}")
    print("파이프라인 완료")
    print(f"{'='*60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--stock", help="특정 종목 코드만 실행 (예: 005930)")
    parser.add_argument("--pages", type=int, default=5, help="종목당 수집 페이지 수")
    args = parser.parse_args()
    run(target_code=args.stock, pages=args.pages)
