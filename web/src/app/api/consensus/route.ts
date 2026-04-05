import { NextResponse } from "next/server";

export async function GET() {
  if (process.env.VERCEL) {
    const data = await import("@/data/consensus.json");
    return NextResponse.json(data.default);
  }

  const { getConsensus } = await import("@/lib/db");
  return NextResponse.json(getConsensus());
}
