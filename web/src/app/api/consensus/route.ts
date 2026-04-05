import { NextResponse } from "next/server";
import consensusData from "@/data/consensus.json";

export async function GET() {
  return NextResponse.json(consensusData);
}
