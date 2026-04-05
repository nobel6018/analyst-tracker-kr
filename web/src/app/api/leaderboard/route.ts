import { NextResponse } from "next/server";
import { getLeaderboard, getIndividualLeaderboard } from "@/lib/db";

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const type = searchParams.get("type") || "firm";
  const minReports = Number(searchParams.get("min") || 5);

  try {
    const data =
      type === "individual"
        ? getIndividualLeaderboard(minReports)
        : getLeaderboard(minReports);
    return NextResponse.json(data);
  } catch (e) {
    console.error("[leaderboard]", e);
    return NextResponse.json({ error: "DB error" }, { status: 500 });
  }
}
