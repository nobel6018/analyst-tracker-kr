# analyst-tracker-kr

한국 증권사 애널리스트 실적 추적 플랫폼

TipRanks + I/B/E/S SmartEstimate 방식을 한국 시장에 적용한 PoC.

## 구조

```
backend/   Python 데이터 파이프라인 (크롤링 → DB → 점수 계산)
web/       Next.js 웹 대시보드
```

## 핵심 기능

- **네이버 금융 리서치** 크롤링 → 목표주가·투자의견 자동 수집
- **Z-test 기반 등급** (TipRanks 방식): 적중률 + 평균수익률 + 통계적 유의성
- **SmartEstimate 가중 컨센서스** (I/B/E/S 방식): 정확도×최신성 이중 가중
- **매수 편향 보정 지표**: 92.9% 매수 시장에서 진짜 실력 측정
- **개인 애널리스트 추적**: PDF OCR(Tesseract)로 커버 페이지에서 이름 추출

## 현황 (2025.04 기준)

- 추적 종목: KOSPI 50개
- 수집 리포트: 6,240건
- 평가 완료: 5,509건
- 증권사: 30개
- 개인 애널리스트: 34명

## 실행

```bash
# 백엔드 파이프라인
cd backend
python pipeline.py               # 전체 종목 수집 + 점수 계산
python pipeline.py --stock 005930  # 특정 종목만

# 웹 대시보드
cd web
npm install
npm run dev
# → http://localhost:3000
```

## 데이터 소스

- 리포트: 네이버 금융 리서치 (공개 페이지)
- 주가: Yahoo Finance KS
- 애널리스트 이름: 리포트 PDF OCR (Tesseract)

## 벤치마크

| 플랫폼 | 레퍼런스 |
|--------|---------|
| TipRanks | 적중률 + Z-test + 스타 등급 |
| Refinitiv I/B/E/S | SmartEstimate 가중 컨센서스 |
| Visible Alpha | 섹터별 KPI (로드맵) |
