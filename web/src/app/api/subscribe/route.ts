import { NextResponse } from "next/server";

// 로컬: 파일 기반 저장 (PoC)
// Vercel: 준비 중 응답 (추후 Resend + Neon으로 교체)

export async function POST(req: Request) {
  const body = await req.json();
  const { email, firms = [], analysts = [] } = body;

  if (!email || !email.includes("@")) {
    return NextResponse.json({ error: "유효한 이메일을 입력하세요." }, { status: 400 });
  }

  if (process.env.VERCEL) {
    // TODO: Resend + Neon으로 실제 구독 저장 구현
    console.log("[subscribe] Vercel 환경 — 이메일:", email, "firms:", firms, "analysts:", analysts);
    return NextResponse.json({
      ok: true,
      message: `${email} 사전 등록 완료! 서비스 오픈 시 알림을 드립니다.`,
    });
  }

  // 로컬: 파일 저장
  const fs = await import("fs");
  const path = await import("path");
  const SUBS_FILE = path.join(process.cwd(), "../../analyst-poc/subscribers.json");

  interface Subscriber { email: string; firms: string[]; analysts: string[]; registeredAt: string; }

  let subs: Subscriber[] = [];
  try { subs = JSON.parse(fs.readFileSync(SUBS_FILE, "utf-8")); } catch { /* 첫 실행 */ }

  const existing = subs.findIndex((s) => s.email === email);
  if (existing >= 0) {
    subs[existing].firms = firms;
    subs[existing].analysts = analysts;
  } else {
    subs.push({ email, firms, analysts, registeredAt: new Date().toISOString() });
  }
  fs.writeFileSync(SUBS_FILE, JSON.stringify(subs, null, 2));

  return NextResponse.json({
    ok: true,
    message: `${email} 등록 완료. 새 리포트 발행 시 알림을 보내드립니다.`,
    total_subscribers: subs.length,
  });
}

export async function GET() {
  if (process.env.VERCEL) {
    return NextResponse.json({ total: "–" });
  }
  const fs = await import("fs");
  const path = await import("path");
  const SUBS_FILE = path.join(process.cwd(), "../../analyst-poc/subscribers.json");
  try {
    const subs = JSON.parse(fs.readFileSync(SUBS_FILE, "utf-8"));
    return NextResponse.json({ total: subs.length });
  } catch {
    return NextResponse.json({ total: 0 });
  }
}
