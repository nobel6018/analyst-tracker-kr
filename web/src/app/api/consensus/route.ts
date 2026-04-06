import { NextResponse } from "next/server";
import { getConsensus } from "@/lib/pg";

export async function GET() {
  try {
    return NextResponse.json(await getConsensus());
  } catch (e) {
    console.error("[consensus]", e);
    const data = await import("@/data/consensus.json");
    return NextResponse.json(data.default);
  }
}
