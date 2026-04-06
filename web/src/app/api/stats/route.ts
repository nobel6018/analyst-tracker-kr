import { NextResponse } from "next/server";
import { getStats } from "@/lib/pg";

export async function GET() {
  try {
    return NextResponse.json(await getStats());
  } catch (e) {
    console.error("[stats]", e);
    // Vercel cold-start DB 연결 실패 시 스냅샷 fallback
    const data = await import("@/data/stats.json");
    return NextResponse.json(data.default);
  }
}
