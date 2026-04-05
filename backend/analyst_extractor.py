"""
PDF 커버 페이지 → Tesseract OCR → 애널리스트 이름 추출

흐름:
  naver mobile API → PDF URL → pdftoppm → PIL(JPEG 변환) → Tesseract OCR → 이름 파싱 → DB 저장

발견된 패턴 (미래에셋 샘플):
  [반도체]
  김영건
  younggun.kim.a@miraeasset.com
"""

import os
import re
import subprocess
import tempfile
import time
from pathlib import Path

import requests
from PIL import Image

NAVER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15"
}
DOWNLOAD_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}

TESSERACT = "/opt/homebrew/bin/tesseract"
PDFTOPPM  = "/opt/homebrew/bin/pdftoppm"

TMP_DIR = Path(tempfile.gettempdir()) / "analyst_pdfs"
TMP_DIR.mkdir(exist_ok=True)


# ─── PDF URL 조회 ─────────────────────────────────────────────────────────────

def get_pdf_url(nid: str) -> str | None:
    try:
        r = requests.get(
            f"https://m.stock.naver.com/api/research/company/{nid}",
            headers=NAVER_HEADERS, timeout=8
        )
        return r.json().get("researchContent", {}).get("attachUrl")
    except Exception:
        return None


# ─── PDF → JPEG 변환 ──────────────────────────────────────────────────────────

def pdf_to_jpeg(pdf_url: str, nid: str) -> str | None:
    """PDF 1페이지 → JPEG. 파일 경로 반환."""
    pdf_path = TMP_DIR / f"{nid}.pdf"
    jpg_path = TMP_DIR / f"{nid}.jpg"

    if jpg_path.exists():
        return str(jpg_path)  # 캐시

    # PDF 다운로드
    if not pdf_path.exists():
        try:
            r = requests.get(pdf_url, headers=DOWNLOAD_HEADERS, timeout=20)
            if r.status_code != 200:
                return None
            pdf_path.write_bytes(r.content)
        except Exception:
            return None

    # pdftoppm: 1페이지 → PNG (출력 prefix는 하이픈 없이)
    png_prefix = str(TMP_DIR / f"p{nid}")
    result = subprocess.run(
        [PDFTOPPM, "-r", "200", "-png", "-f", "1", "-l", "1",
         str(pdf_path), png_prefix],
        capture_output=True, timeout=30
    )
    if result.returncode != 0:
        return None

    # 생성된 PNG 찾기
    candidates = sorted(TMP_DIR.glob(f"p{nid}-*.png")) + sorted(TMP_DIR.glob(f"p{nid}.png"))
    if not candidates:
        return None

    # JPEG로 변환 (tesseract가 하이픈 경로를 못 읽으므로)
    img = Image.open(str(candidates[0]))
    img.convert("RGB").save(str(jpg_path), "JPEG", quality=95)
    return str(jpg_path)


# ─── Tesseract OCR ───────────────────────────────────────────────────────────

def ocr_image(jpg_path: str, nid: str) -> str:
    """JPEG 이미지 → 한국어+영어 OCR 텍스트"""
    out_prefix = str(TMP_DIR / f"ocr{nid}")
    old_cwd = os.getcwd()
    try:
        os.chdir(str(TMP_DIR))
        fname = Path(jpg_path).name  # 상대 경로로 실행
        result = subprocess.run(
            [TESSERACT, fname, f"ocr{nid}", "-l", "kor+eng", "--psm", "3"],
            capture_output=True, timeout=60
        )
    finally:
        os.chdir(old_cwd)

    out_file = Path(out_prefix + ".txt")
    if out_file.exists():
        return out_file.read_text(encoding="utf-8", errors="ignore")
    return ""


# ─── 이름 파싱 ───────────────────────────────────────────────────────────────

# 이름 블랙리스트 (종목명, 섹터명, 일반 단어)
NAME_BLACKLIST = {
    # 종목명
    "삼성전자", "SK하이닉스", "하이닉스", "카카오", "현대차", "기아차",
    "삼성바이오", "셀트리온", "KB금융", "신한지주", "LG화학", "삼성SDI",
    "엔씨소프트", "넷마블", "미래에셋", "신한투자", "한화투자", "대신증권",
    # 섹터명
    "반도체", "바이오", "자동차", "금융", "소비재", "화학", "건설",
    "에너지", "통신", "유통", "게임", "엔터", "철강", "조선", "항공",
    "약바이오", "은행", "보험", "증권", "부동산", "리츠", "건강",
    # 일반 단어
    "증권사", "리서치", "분석가", "연구센터", "투자의", "영업이", "매출액", "순이익",
    "서울시", "한국어", "한국투자", "투자증권", "증권연구",
}

# 섹터 키워드
SECTOR_KEYWORDS = ["반도체", "IT", "바이오", "자동차", "금융", "소비재", "화학", "건설",
                   "에너지", "통신", "유통", "게임", "엔터", "철강", "조선", "항공"]


def _is_valid_name(name: str) -> bool:
    """한글 이름 유효성 검사"""
    if not name or len(name) < 2 or len(name) > 4:
        return False
    if name in NAME_BLACKLIST:
        return False
    if not re.match(r'^[가-힣]+$', name):
        return False
    return True


def parse_analyst_info(ocr_text: str) -> dict:
    """
    OCR 텍스트에서 애널리스트 이름, 이메일, 섹터 추출.

    탐지 전략 (우선순위 순):
      1. 이메일 같은 줄에 "Analyst 이름" 패턴
      2. 이메일 바로 위 1~2줄에 단독 한글 이름
      3. 직함 키워드 옆 한글 이름
      섹터: "[반도체" 또는 독립적인 섹터 키워드 줄 탐색
    """
    lines = [l.strip() for l in ocr_text.split("\n") if l.strip()]
    analyst_name = None
    email = None
    sector = None

    for i, line in enumerate(lines):
        # ── 이메일 찾기 ─────────────────────────────────────────────────────
        email_match = re.search(r'[\w.+-]+@[\w.-]+\.[a-z]{2,}', line, re.IGNORECASE)
        if email_match:
            if email is None:
                email = email_match.group().replace(",", ".")  # yuak,pak → yuak.pak

            if analyst_name is None:
                # 패턴1: 같은 줄에 "Analyst 이름" 또는 "이름 Analyst"
                same_line = re.search(
                    r'(?:Analyst|analyst)\s+([가-힣]{2,4})|([가-힣]{2,4})\s+(?:Analyst|analyst)',
                    line
                )
                if same_line:
                    name = same_line.group(1) or same_line.group(2)
                    if _is_valid_name(name):
                        analyst_name = name

                # 패턴2: 이메일 위 1줄에 "Analyst 이름" 패턴
                if analyst_name is None and i > 0:
                    above = lines[i - 1]
                    above_match = re.search(
                        r'(?:Analyst|analyst)\s+([가-힣]{2,4})|([가-힣]{2,4})\s+(?:Analyst|analyst)',
                        above
                    )
                    if above_match:
                        name = above_match.group(1) or above_match.group(2)
                        if _is_valid_name(name):
                            analyst_name = name

                # 패턴3: 이메일 가장 가까운 줄부터 역방향으로 "Analyst 이름" 또는 단독 이름 탐색
                if analyst_name is None:
                    # 1순위: ±8줄 안에서 "Analyst 이름" 패턴
                    for j in range(max(0, i - 8), min(len(lines), i + 3)):
                        if j == i:
                            continue
                        am = re.search(r'(?:Analyst|analyst)\s+([가-힣]{2,4})', lines[j])
                        if am and _is_valid_name(am.group(1)):
                            analyst_name = am.group(1)
                            break

                    # 2순위: 이메일 바로 위부터 역방향으로 단독 한글 이름 탐색
                    if analyst_name is None:
                        for j in range(i - 1, max(-1, i - 6), -1):
                            nm = re.match(r'^([가-힣]{2,4})$', lines[j])
                            if nm and _is_valid_name(nm.group(1)):
                                analyst_name = nm.group(1)
                                break

        # ── 직함 키워드로 탐지 ───────────────────────────────────────────────
        if analyst_name is None:
            title_match = re.search(
                r'([가-힣]{2,4})\s*(?:애널리스트|연구원|팀장|수석|부장)',
                line
            )
            if title_match and _is_valid_name(title_match.group(1)):
                analyst_name = title_match.group(1)

        # ── 섹터 탐지 ("[반도체" 또는 standalone 섹터 줄) ────────────────────
        if sector is None:
            # "[반도체" 처럼 대괄호 포함 또는 독립 줄
            sector_match = re.search(r'\[?(' + '|'.join(SECTOR_KEYWORDS) + r')\]?', line)
            if sector_match:
                # 종목코드+섹터 조합("005930 + 반도체")은 제외
                if not re.search(r'\d{5,6}\s*\+', line):
                    sector = sector_match.group(1)

    return {
        "analyst_name": analyst_name,
        "email": email,
        "sector": sector,
    }


# ─── 통합 추출 ────────────────────────────────────────────────────────────────

def extract_analyst(nid: str) -> dict:
    """nid 하나로 PDF 다운로드 → OCR → 파싱 전체 수행"""
    base = {"nid": nid, "analyst_name": None, "email": None, "sector": None}

    pdf_url = get_pdf_url(nid)
    if not pdf_url:
        return {**base, "source": "no_pdf"}

    jpg_path = pdf_to_jpeg(pdf_url, nid)
    if not jpg_path:
        return {**base, "source": "pdf_error"}

    ocr_text = ocr_image(jpg_path, nid)
    if not ocr_text.strip():
        return {**base, "source": "ocr_empty"}

    info = parse_analyst_info(ocr_text)
    return {**base, **info, "source": "ocr"}


# ─── DB 연동: 미추출 리포트 배치 처리 ────────────────────────────────────────

def update_db_analyst_names(limit: int = 200, delay: float = 0.3):
    """DB에서 analyst_name=NULL인 리포트를 가져와 OCR로 채움"""
    import db

    with db.get_connection() as conn:
        rows = conn.execute(
            """
            SELECT r.id, r.nid, f.name AS firm_name
            FROM report r
            JOIN firm f ON f.id = r.firm_id
            WHERE r.analyst_name IS NULL
            ORDER BY r.report_date DESC
            LIMIT ?
            """,
            (limit,)
        ).fetchall()

    print(f"\n애널리스트 이름 추출 대상: {len(rows)}건")

    found = 0
    for i, row in enumerate(rows, 1):
        result = extract_analyst(row["nid"])
        name = result.get("analyst_name")

        if name:
            with db.transaction() as conn:
                conn.execute(
                    "UPDATE report SET analyst_name = ?, updated_at = datetime('now') WHERE id = ?",
                    (name, row["id"])
                )
            found += 1
            print(f"  [{i:3d}/{len(rows)}] nid={row['nid']} {row['firm_name']:<12} → {name} ({result.get('email','')})")
        else:
            print(f"  [{i:3d}/{len(rows)}] nid={row['nid']} {row['firm_name']:<12} → 없음 ({result['source']})")

        time.sleep(delay)

    print(f"\n완료: {found}/{len(rows)}건 이름 추출")
    return found


if __name__ == "__main__":
    import sys

    if "--update-db" in sys.argv:
        update_db_analyst_names(limit=300)
    else:
        # 단일 테스트
        print("=== 애널리스트 이름 추출 테스트 ===\n")
        for nid in ["91191", "91153", "91107", "91020", "90944"]:
            result = extract_analyst(nid)
            print(f"nid={nid}: {result}")
            time.sleep(0.3)
