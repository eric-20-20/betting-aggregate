import "server-only";

import { promises as fs } from "fs";
import path from "path";
import type {
  PublicPicksFile,
  PlaysFile,
  ConsensusStrengthRecord,
  MarketTypeRecord,
  StatTypeRecord,
  TrendEntry,
  RecentTrendsReport,
  HistoryIndex,
  HistoryDayFile,
} from "./types";

// Re-export format helpers so server pages can still use `import { formatWinPct } from "@/lib/data"`
export { formatOdds, formatWinPct, formatRecord, formatPickSelection } from "./format";

const PUBLIC_DATA_DIR = path.join(process.cwd(), "public", "data");
const PRIVATE_DATA_DIR = path.join(process.cwd(), "data", "private");

// Known source/expert names that should never appear in public-facing labels
const SOURCE_NAMES = [
  "nukethebooks", "juicereel_nukethebooks", "juicereel_sxebets", "sxebets",
  "action", "betql", "covers", "dimers", "sportsline", "oddstrader",
  "vegasinsider", "juicereel",
];

function sanitizePatternLabel(label: string): string {
  let s = label;
  for (const src of SOURCE_NAMES) {
    s = s.replace(new RegExp(src, "gi"), "source");
  }
  // Collapse "source source" or "source_source" artifacts
  s = s.replace(/source[_ ]source/gi, "multi-source");
  return s.trim();
}

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
      .filter((d) => d >= "2025-10-01")
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

// Report data loaders — all source-anonymous, ROI-free

export async function getConsensusStrengthRecords(): Promise<ConsensusStrengthRecord[]> {
  const data = await readJSON<{ rows: ConsensusStrengthRecord[] }>(
    path.join(PUBLIC_DATA_DIR, "reports", "consensus_strength.json")
  );
  return data?.rows || [];
}

export async function getMarketTypeRecords(): Promise<MarketTypeRecord[]> {
  const data = await readJSON<{ by_market: MarketTypeRecord[] }>(
    path.join(PUBLIC_DATA_DIR, "reports", "market_type.json")
  );
  return data?.by_market || [];
}

export async function getStatTypeRecords(): Promise<StatTypeRecord[]> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const data = await readJSON<{ rows: any[] }>(
    path.join(PUBLIC_DATA_DIR, "reports", "by_stat_type.json")
  );
  return data?.rows || [];
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

export async function getRecentTrends(): Promise<RecentTrendsReport | null> {
  return readJSON<RecentTrendsReport>(
    path.join(PUBLIC_DATA_DIR, "reports", "recent_trends.json")
  );
}

// History data loaders (paid members only — reads from private dir)

export async function getHistoryIndex(): Promise<HistoryIndex | null> {
  return readJSON<HistoryIndex>(
    path.join(PRIVATE_DATA_DIR, "history", "index.json")
  );
}

export async function getHistoryDay(
  date: string
): Promise<HistoryDayFile | null> {
  return readJSON<HistoryDayFile>(
    path.join(PRIVATE_DATA_DIR, "history", `history_${date}.json`)
  );
}

export interface TimelineEntry {
  date: string;
  day_of_week: string;
  tier: string;
  result: string;
  selection: string;
  market_type: string;
  line: number | null;
  best_odds: number | null;
  pattern_label: string | null;
  pattern_record: string | null;
  matchup: string;
}

export async function getAggregatedTimeline(tiers: string[] = ["A"]): Promise<TimelineEntry[]> {
  const index = await getHistoryIndex();
  if (!index) return [];

  const entries: TimelineEntry[] = [];

  for (const day of index.dates) {
    const file = await getHistoryDay(day.date);
    if (!file) continue;

    for (const play of file.plays) {
      if (!tiers.includes(play.tier)) continue;
      const sig = play.signal;
      // Build matchup string
      let matchup = "";
      if (sig.away_team && sig.home_team) {
        matchup = `${sig.away_team} @ ${sig.home_team}`;
      } else {
        const ek = sig.event_key || "";
        const atIdx = ek.lastIndexOf("@");
        if (atIdx > 0) {
          const beforeAt = ek.substring(0, atIdx);
          const colonIdx = beforeAt.lastIndexOf(":");
          const away = colonIdx >= 0 ? beforeAt.substring(colonIdx + 1) : beforeAt;
          const home = ek.substring(atIdx + 1);
          matchup = `${away} @ ${home}`;
        }
      }

      entries.push({
        date: day.date,
        day_of_week: day.day_of_week,
        tier: play.tier,
        result: play.result || "PENDING",
        selection: sig.selection,
        market_type: sig.market_type,
        line: sig.line,
        best_odds: sig.best_odds,
        pattern_label: play.matched_pattern?.label
          ? sanitizePatternLabel(play.matched_pattern.label)
          : null,
        pattern_record: play.matched_pattern?.hist?.record ?? null,
        matchup,
      });
    }
  }

  // Newest first
  entries.reverse();
  return entries;
}

export interface PLDataPoint {
  date: string;
  cumulative_units: number;
  pick_count: number;
}

/**
 * Build cumulative P&L chart data for A-tier picks at flat -110 odds.
 * Each WIN = +0.909 units, each LOSS = -1 unit.
 * Returns oldest-first array of {date, cumulative_units, pick_count}.
 */
export async function getPLChartData(): Promise<PLDataPoint[]> {
  const index = await getHistoryIndex();
  if (!index) return [];

  const points: PLDataPoint[] = [];
  let cumulative = 0;
  let pickCount = 0;

  // index.dates is already oldest-first from the export pipeline
  const sorted = [...index.dates].sort((a, b) => a.date.localeCompare(b.date));

  for (const day of sorted) {
    const file = await getHistoryDay(day.date);
    if (!file) continue;

    for (const play of file.plays) {
      if (play.tier !== "A") continue;
      const result = play.result;
      if (result === "WIN") {
        cumulative += 0.909; // +$91 on $100 bet at -110
        pickCount += 1;
      } else if (result === "LOSS") {
        cumulative -= 1.0;
        pickCount += 1;
      }
      // PUSH/PENDING/VOID: no change
    }

    if (pickCount > 0) {
      points.push({
        date: day.date,
        cumulative_units: Math.round(cumulative * 100) / 100,
        pick_count: pickCount,
      });
    }
  }

  return points;
}
