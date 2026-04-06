import { NextResponse } from "next/server";

export async function GET(
  _req: Request,
  { params }: { params: Promise<{ firm: string; name: string }> }
) {
  const { firm, name } = await params;
  const firmName = decodeURIComponent(firm);
  const analystName = name === "_" ? null : decodeURIComponent(name);

  if (process.env.VERCEL) {
    return NextResponse.json({ error: "Vercel: use local dev for live data" }, { status: 503 });
  }

  const { getAnalystScore, getAnalystReports, getAnalystSectorScores } = await import("@/lib/db");
  const score = getAnalystScore(firmName, analystName);
  if (!score) return NextResponse.json({ error: "not found" }, { status: 404 });

  return NextResponse.json({
    score,
    reports: getAnalystReports(firmName, analystName),
    sectors: getAnalystSectorScores(firmName, analystName),
  });
}
