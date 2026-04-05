import { NextResponse } from "next/server";
import { getConsensus } from "@/lib/db";

export async function GET() {
  try {
    return NextResponse.json(getConsensus());
  } catch (e) {
    console.error("[consensus]", e);
    return NextResponse.json({ error: "DB error" }, { status: 500 });
  }
}
