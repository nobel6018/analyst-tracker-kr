import { NextResponse } from "next/server";

export async function GET() {
  if (process.env.VERCEL) {
    const data = await import("@/data/stats.json");
    return NextResponse.json(data.default);
  }

  const { getStats } = await import("@/lib/db");
  return NextResponse.json(getStats());
}
