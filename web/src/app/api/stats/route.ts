import { NextResponse } from "next/server";
import statsData from "@/data/stats.json";

export async function GET() {
  return NextResponse.json(statsData);
}
