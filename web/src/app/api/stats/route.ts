import { NextResponse } from "next/server";
import { getStats } from "@/lib/db";

export async function GET() {
  try {
    return NextResponse.json(getStats());
  } catch (e) {
    console.error("[stats]", e);
    return NextResponse.json({ error: "DB error" }, { status: 500 });
  }
}
