import { NextResponse } from "next/server";
import { getLeaderboard, getIndividualLeaderboard } from "@/lib/pg";

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const type = searchParams.get("type") || "firm";
  const minReports = Number(searchParams.get("min") || 5);

  try {
    const data = type === "individual"
      ? await getIndividualLeaderboard(minReports)
      : await getLeaderboard(minReports);
    return NextResponse.json(data);
  } catch (e) {
    console.error("[leaderboard]", e);
    const [leaders, individuals] = await Promise.all([
      import("@/data/leaders.json"),
      import("@/data/individuals.json"),
    ]);
    const data = type === "individual"
      ? individuals.default.filter((r) => r.evaluated_reports >= minReports)
      : leaders.default.filter((r) => r.evaluated_reports >= minReports);
    return NextResponse.json(data);
  }
}
