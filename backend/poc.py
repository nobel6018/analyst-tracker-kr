"""
애널리스트 성적 추적 플랫폼 PoC v2

검증 가설:
  1. 네이버 금융 리서치에서 (날짜, 증권사, 목표주가, 투자의견) 자동 수집 가능
  2. Yahoo Finance에서 KS 실제 주가 자동 수집 가능
  3. 목표주가 적중률 + 수익률 자동 계산 가능

데이터 흐름:
  naver list page → (nid, date, firm, title)
      + naver detail page → (target_price, opinion)
      + yahoo finance → (base_price, max_high_1y, actual_return)
      → hit 판정 + 수익률
"""

import requests
import yfinance as yf
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import re
import time

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://finance.naver.com/",
}

TARGETS = [
    {"name": "삼성전자", "naver_code": "005930", "yahoo_ticker": "005930.KS"},
    {"name": "SK하이닉스", "naver_code": "000660", "yahoo_ticker": "000660.KS"},
    {"name": "NAVER",    "naver_code": "035420", "yahoo_ticker": "035420.KS"},
]


# ─── Step 1. 리포트 목록 수집 ──────────────────────────────────────────────

def fetch_report_list(naver_code: str, pages: int = 2) -> list[dict]:
    reports = []
    for page in range(1, pages + 1):
        url = (
            f"https://finance.naver.com/research/company_list.naver"
            f"?searchType=itemCode&itemCode={naver_code}&page={page}"
        )
        resp = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table", class_="type_1")
        if not table:
            break

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

            date_str = cols[4].get_text(strip=True) if len(cols) > 4 else ""
            reports.append({
                "nid": nid,
                "stock": cols[0].get_text(strip=True),
                "title": cols[1].get_text(strip=True),
                "firm": cols[2].get_text(strip=True),
                "date_raw": date_str,
                "date": _parse_date(date_str),
            })
        time.sleep(0.3)

    return reports


def _parse_date(date_raw: str) -> str | None:
    """'26.04.02' → '2026-04-02'"""
    m = re.match(r"(\d{2})\.(\d{2})\.(\d{2})", date_raw)
    if not m:
        return None
    yy, mm, dd = m.groups()
    year = "20" + yy
    return f"{year}-{mm}-{dd}"


# ─── Step 2. 개별 리포트 상세 (목표주가, 투자의견) ────────────────────────

def fetch_report_detail(nid: str, naver_code: str) -> dict:
    url = (
        f"https://finance.naver.com/research/company_read.naver"
        f"?nid={nid}&searchType=itemCode&itemCode={naver_code}"
    )
    resp = requests.get(url, headers=HEADERS, timeout=10)
    soup = BeautifulSoup(resp.text, "html.parser")

    target_price = None
    opinion = None

    for td in soup.find_all("td"):
        text = td.get_text(strip=True)
        if "목표가" in text and "투자의견" in text:
            t = re.search(r"목표가([0-9,]+)", text)
            o = re.search(r"투자의견(.+)", text)
            if t:
                target_price = int(t.group(1).replace(",", ""))
            if o:
                opinion = o.group(1).strip()
            break

    return {"target_price": target_price, "opinion": opinion}


# ─── Step 3. 주가 데이터 수집 ────────────────────────────────────────────

def fetch_base_price(yahoo_ticker: str, date: str) -> int | None:
    """발행일 당시 종가 (기준가)"""
    try:
        dt = datetime.strptime(date, "%Y-%m-%d")
        end = dt + timedelta(days=5)
        hist = yf.Ticker(yahoo_ticker).history(
            start=dt.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
        )
        if hist.empty:
            return None
        return round(float(hist["Close"].iloc[0]))
    except Exception:
        return None


def fetch_max_high(yahoo_ticker: str, start: str) -> dict:
    """발행일로부터 1년간 최고가 + 최종 종가"""
    try:
        start_dt = datetime.strptime(start, "%Y-%m-%d")
        end_dt = min(start_dt + timedelta(days=365), datetime.now())
        hist = yf.Ticker(yahoo_ticker).history(
            start=start_dt.strftime("%Y-%m-%d"),
            end=end_dt.strftime("%Y-%m-%d"),
        )
        if hist.empty:
            return {}
        return {
            "max_high": round(float(hist["High"].max())),
            "last_close": round(float(hist["Close"].iloc[-1])),
            "days_elapsed": (end_dt - start_dt).days,
        }
    except Exception:
        return {}


# ─── Step 4. 적중 판정 + 수익률 계산 ─────────────────────────────────────

def evaluate(report: dict, yahoo_ticker: str) -> dict:
    if not report["date"] or not report["target_price"]:
        return {"skip": True, "reason": "missing date or target_price"}

    base_price = fetch_base_price(yahoo_ticker, report["date"])
    if not base_price:
        return {"skip": True, "reason": "no base price"}

    price_data = fetch_max_high(yahoo_ticker, report["date"])
    if not price_data:
        return {"skip": True, "reason": "no price range data"}

    target = report["target_price"]
    max_high = price_data["max_high"]
    last_close = price_data["last_close"]

    hit = max_high >= target
    implied_upside = round((target - base_price) / base_price * 100, 1)
    actual_return = round((last_close - base_price) / base_price * 100, 1)

    return {
        "skip": False,
        "hit": hit,
        "base_price": base_price,
        "target_price": target,
        "max_high_1y": max_high,
        "implied_upside_pct": implied_upside,
        "actual_return_pct": actual_return,
        "days_elapsed": price_data["days_elapsed"],
    }


# ─── 실행 ─────────────────────────────────────────────────────────────────

def run():
    print("=" * 70)
    print("애널리스트 성적 추적 플랫폼 PoC v2")
    print("=" * 70)

    for target in TARGETS:
        name = target["name"]
        code = target["naver_code"]
        ticker = target["yahoo_ticker"]

        print(f"\n{'─'*70}")
        print(f"[{name}] ({code}) → Yahoo: {ticker}")
        print(f"{'─'*70}")

        # Step 1: 리포트 목록
        print("\n▶ 가설 1-A: 리포트 목록 수집")
        reports = fetch_report_list(code, pages=1)
        print(f"  수집된 리포트: {len(reports)}건")
        for r in reports[:3]:
            print(f"    {r['date']} | {r['firm']:<12} | {r['title'][:30]}")

        # Step 2: 상세 (목표주가, 투자의견)
        print("\n▶ 가설 1-B: 목표주가/투자의견 수집")
        detailed = []
        for r in reports[:8]:  # 상위 8건만 테스트 (속도)
            detail = fetch_report_detail(r["nid"], code)
            r.update(detail)
            has_target = r["target_price"] is not None
            print(f"    {r['date']} | {r['firm']:<12} | 목표가: {r['target_price']:>8,}원 | {r['opinion']}" if has_target
                  else f"    {r['date']} | {r['firm']:<12} | 목표가 없음")
            if has_target:
                detailed.append(r)
            time.sleep(0.2)

        print(f"\n  목표주가 있는 리포트: {len(detailed)}/{min(8, len(reports))}건")

        # Step 3+4: 주가 수집 + 적중 계산 (2025년 이전 리포트만 — 1년 경과)
        print("\n▶ 가설 2+3: 주가 수집 + 적중률 계산")
        evaluable = [r for r in detailed if r["date"] and r["date"] < "2025-04-01"]

        if not evaluable:
            print("  ⚠️  1년 경과 리포트 없음 (최신 리포트만 존재). 날짜 범위를 확장해야 함.")
            # 대신 최신 리포트로 기준가 + 현재 수익률만 계산
            if detailed:
                sample = detailed[0]
                base = fetch_base_price(ticker, sample["date"])
                print(f"\n  [기준가 수집 테스트]")
                print(f"    리포트: {sample['date']} | {sample['firm']}")
                print(f"    기준가: {base:,}원" if base else "    기준가: 수집 실패")
                if base and sample["target_price"]:
                    implied = round((sample["target_price"] - base) / base * 100, 1)
                    print(f"    목표주가: {sample['target_price']:,}원 (암시수익률: {implied}%)")
        else:
            hits = 0
            returns = []
            for r in evaluable[:5]:
                result = evaluate(r, ticker)
                if result.get("skip"):
                    print(f"  ⚠️  건너뜀: {result['reason']}")
                    continue
                status = "✅ 적중" if result["hit"] else "❌ 미달"
                print(f"  {status} | {r['date']} | {r['firm']:<10} | "
                      f"목표가 {result['target_price']:,} | 기준가 {result['base_price']:,} | "
                      f"1년최고 {result['max_high_1y']:,} | "
                      f"암시 {result['implied_upside_pct']:+.1f}% | 실제 {result['actual_return_pct']:+.1f}%")
                if result["hit"]:
                    hits += 1
                returns.append(result["actual_return_pct"])
                time.sleep(0.5)

            if returns:
                avg_return = round(sum(returns) / len(returns), 1)
                hit_rate = round(hits / len(returns) * 100)
                print(f"\n  ─ 요약: 적중률 {hit_rate}% ({hits}/{len(returns)}) | 평균 수익률 {avg_return:+.1f}%")

    print(f"\n{'='*70}")
    print("PoC 완료")
    print(f"{'='*70}")


if __name__ == "__main__":
    run()
