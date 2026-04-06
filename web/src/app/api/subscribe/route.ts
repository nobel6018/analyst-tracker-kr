import { NextResponse } from "next/server";
import { upsertSubscriber } from "@/lib/pg";

export async function POST(req: Request) {
  const body = await req.json();
  const { email, firms = [], analysts = [] } = body;

  if (!email || !email.includes("@")) {
    return NextResponse.json({ error: "유효한 이메일을 입력하세요." }, { status: 400 });
  }

  try {
    const result = await upsertSubscriber(email, firms, analysts);
    return NextResponse.json({
      ok: true,
      message: `${email} 등록 완료. 새 리포트 발행 시 알림을 보내드립니다.`,
      total_subscribers: result.total,
    });
  } catch (e) {
    console.error("[subscribe]", e);
    return NextResponse.json({ error: "등록 중 오류가 발생했습니다." }, { status: 500 });
  }
}

export async function GET() {
  try {
    const { queryOne } = await import("@/lib/pg");
    const row = await queryOne<{ n: string }>("SELECT COUNT(*) AS n FROM subscriber");
    return NextResponse.json({ total: Number(row?.n ?? 0) });
  } catch {
    return NextResponse.json({ total: "–" });
  }
}
