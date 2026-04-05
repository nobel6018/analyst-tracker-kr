import { NextResponse } from "next/server";

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const type = searchParams.get("type") || "firm";
  const minReports = Number(searchParams.get("min") || 5);

  if (process.env.VERCEL) {
    const [leaders, individuals] = await Promise.all([
      import("@/data/leaders.json"),
      import("@/data/individuals.json"),
    ]);
    const data =
      type === "individual"
        ? individuals.default.filter((r) => r.evaluated_reports >= minReports)
        : leaders.default.filter((r) => r.evaluated_reports >= minReports);
    return NextResponse.json(data);
  }

  const { getLeaderboard, getIndividualLeaderboard } = await import("@/lib/db");
  const data =
    type === "individual"
      ? getIndividualLeaderboard(minReports)
      : getLeaderboard(minReports);
  return NextResponse.json(data);
}
