import "server-only";

import { promises as fs } from "fs";
import path from "path";
import type {
  PublicPicksFile,
  PlaysFile,
  SourceRecord,
  ComboRecord,
  ExpertRecord,
  TrendEntry,
} from "./types";

// Re-export format helpers so server pages can still use `import { formatWinPct } from "@/lib/data"`
export { formatOdds, formatWinPct, formatRecord, formatPickSelection } from "./format";

const PUBLIC_DATA_DIR = path.join(process.cwd(), "public", "data");
const PRIVATE_DATA_DIR = path.join(process.cwd(), "data", "private");

async function readJSON<T>(filePath: string): Promise<T | null> {
  try {
    const raw = await fs.readFile(filePath, "utf-8");
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
}

// Get available pick dates (sorted descending)
export async function getAvailableDates(): Promise<string[]> {
  try {
    const dir = path.join(PUBLIC_DATA_DIR, "picks");
    const files = await fs.readdir(dir);
    return files
      .filter((f) => f.startsWith("public_") && f.endsWith(".json"))
      .map((f) => f.replace("public_", "").replace(".json", ""))
      .sort()
      .reverse();
  } catch {
    return [];
  }
}

// Get today's date in YYYY-MM-DD format
export function getTodayDate(): string {
  return new Date().toISOString().slice(0, 10);
}

// Public picks (teaser + locked summaries)
export async function getPublicPicks(
  date: string
): Promise<PublicPicksFile | null> {
  return readJSON<PublicPicksFile>(
    path.join(PUBLIC_DATA_DIR, "picks", `public_${date}.json`)
  );
}

// Full picks (for authenticated subscribers only)
export async function getFullPicks(date: string): Promise<PlaysFile | null> {
  return readJSON<PlaysFile>(
    path.join(PRIVATE_DATA_DIR, `picks_${date}.json`)
  );
}

// Report data loaders — unwrap nested JSON structures from report_records.py

export async function getSourceRecords(): Promise<SourceRecord[]> {
  const data = await readJSON<{ rows: SourceRecord[] }>(
    path.join(PUBLIC_DATA_DIR, "reports", "by_source_record.json")
  );
  return data?.rows || [];
}

export async function getComboRecords(): Promise<ComboRecord[]> {
  const data = await readJSON<{ rows: ComboRecord[] }>(
    path.join(PUBLIC_DATA_DIR, "reports", "by_sources_combo_record.json")
  );
  return data?.rows || [];
}

export async function getExpertRecords(): Promise<ExpertRecord[]> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const data = await readJSON<{ rows_filtered: any[] }>(
    path.join(PUBLIC_DATA_DIR, "reports", "by_expert_record.json")
  );
  // Map "expert" key to "expert_name" for our type
  return (data?.rows_filtered || []).map((r) => ({
    ...r,
    expert_name: r.expert || r.expert_name,
  }));
}

export async function getTopTrends(): Promise<TrendEntry[]> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const data = await readJSON<{ trends: any[] }>(
    path.join(PUBLIC_DATA_DIR, "reports", "top_trends_summary.json")
  );
  // Map "report" → "dimension" and "description" → "key" for our type
  return (data?.trends || []).map((t) => ({
    ...t,
    dimension: t.report || t.dimension,
    key: t.description || t.key,
  }));
}

export interface TierPerformance {
  tier: string;
  wins: number;
  losses: number;
  n: number;
  win_pct: number;
}

export async function getTierPerformance(): Promise<TierPerformance[]> {
  const data = await readJSON<TierPerformance[]>(
    path.join(PUBLIC_DATA_DIR, "reports", "tier_performance.json")
  );
  return data || [];
}
