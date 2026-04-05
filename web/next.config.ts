import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // better-sqlite3는 native addon → 서버 사이드에서만 실행, 번들 제외
  serverExternalPackages: ["better-sqlite3"],
};

export default nextConfig;
