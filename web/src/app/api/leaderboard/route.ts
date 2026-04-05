import { NextResponse } from "next/server";
import leadersData from "@/data/leaders.json";
import individualsData from "@/data/individuals.json";

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const type = searchParams.get("type") || "firm";
  const minReports = Number(searchParams.get("min") || 5);

  const data =
    type === "individual"
      ? individualsData.filter((r) => r.evaluated_reports >= minReports)
      : leadersData.filter((r) => r.evaluated_reports >= minReports);

  return NextResponse.json(data);
}
