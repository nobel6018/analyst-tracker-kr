import { NextResponse } from "next/server";
import { getAnalystScore, getAnalystReports, getAnalystSectorScores } from "@/lib/pg";

export async function GET(
  _req: Request,
  { params }: { params: Promise<{ firm: string; name: string }> }
) {
  const { firm, name } = await params;
  const firmName = decodeURIComponent(firm);
  const analystName = name === "_" ? null : decodeURIComponent(name);

  try {
    const score = await getAnalystScore(firmName, analystName);
    if (!score) return NextResponse.json({ error: "not found" }, { status: 404 });

    const [reports, sectors] = await Promise.all([
      getAnalystReports(firmName, analystName),
      getAnalystSectorScores(firmName, analystName),
    ]);

    return NextResponse.json({ score, reports, sectors });
  } catch (e) {
    console.error("[analyst]", e);
    return NextResponse.json({ error: "DB error" }, { status: 500 });
  }
}
