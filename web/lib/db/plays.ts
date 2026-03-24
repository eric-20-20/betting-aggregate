/**
 * Supabase query functions for plays and signals.
 *
 * These replace the JSON file reads in lib/data.ts and can be used in
 * server components or API routes.
 */
import "server-only";
import { supabase, type DbPlay, type DbSignal, type DbGrade } from "../supabase";

export interface PlayWithSignalAndGrade {
  play: DbPlay;
  signal: DbSignal;
  grade: DbGrade | null;
}

/**
 * Get all plays for a date, joined with their signals and grades.
 * Ordered by rank ascending.
 */
export async function getPlaysForDate(
  date: string,
  tierFilter?: string[]
): Promise<PlayWithSignalAndGrade[]> {
  let query = supabase
    .from("plays")
    .select(
      `
      *,
      signal:signals(*),
      grade:grades(*)
    `
    )
    .eq("date", date)
    .order("rank", { ascending: true });

  if (tierFilter && tierFilter.length > 0) {
    query = query.in("tier", tierFilter);
  }

  const { data, error } = await query;
  if (error) throw new Error(`getPlaysForDate: ${error.message}`);

  return (data || []).map((row) => ({
    play: {
      id: row.id,
      date: row.date,
      signal_id: row.signal_id,
      tier: row.tier,
      rank: row.rank,
      wilson_score: row.wilson_score,
      composite_score: row.composite_score,
      matched_pattern: row.matched_pattern,
      summary: row.summary,
      factors: row.factors,
      expert_detail: row.expert_detail,
    },
    signal: row.signal as DbSignal,
    grade: (row.grade as DbGrade) || null,
  }));
}

/**
 * Get available dates that have plays data, sorted descending.
 */
export async function getAvailableDatesFromDb(): Promise<string[]> {
  const { data, error } = await supabase
    .from("plays")
    .select("date")
    .order("date", { ascending: false })
    .limit(365);

  if (error) throw new Error(`getAvailableDates: ${error.message}`);

  const unique = [...new Set((data || []).map((r) => r.date))];
  return unique.sort().reverse();
}

/**
 * Get tier performance summary across all graded plays.
 * Returns win/loss/n/win_pct per tier.
 */
export async function getTierPerformanceFromDb(): Promise<
  { tier: string; wins: number; losses: number; n: number; win_pct: number }[]
> {
  const { data, error } = await supabase
    .from("plays")
    .select(
      `
      tier,
      grade:grades(result)
    `
    )
    .not("tier", "is", null);

  if (error) throw new Error(`getTierPerformance: ${error.message}`);

  const acc: Record<string, { wins: number; losses: number }> = {};
  for (const row of data || []) {
    const tier = row.tier as string;
    const result = (row.grade as DbGrade | null)?.result;
    if (!acc[tier]) acc[tier] = { wins: 0, losses: 0 };
    if (result === "WIN") acc[tier].wins += 1;
    if (result === "LOSS") acc[tier].losses += 1;
  }

  return Object.entries(acc)
    .map(([tier, { wins, losses }]) => {
      const n = wins + losses;
      return { tier, wins, losses, n, win_pct: n > 0 ? wins / n : 0 };
    })
    .sort((a, b) => a.tier.localeCompare(b.tier));
}

/**
 * Build history index — one entry per day with aggregated win/loss counts.
 */
export async function getHistoryIndexFromDb(
  tiers: string[] = ["A", "B"]
): Promise<{ date: string; wins: number; losses: number; total_picks: number }[]> {
  const { data, error } = await supabase
    .from("plays")
    .select(
      `
      date,
      tier,
      grade:grades(result)
    `
    )
    .in("tier", tiers)
    .order("date", { ascending: false });

  if (error) throw new Error(`getHistoryIndex: ${error.message}`);

  const acc: Record<string, { wins: number; losses: number; total_picks: number }> = {};
  for (const row of data || []) {
    const date = row.date as string;
    const result = (row.grade as DbGrade | null)?.result;
    if (!acc[date]) acc[date] = { wins: 0, losses: 0, total_picks: 0 };
    acc[date].total_picks += 1;
    if (result === "WIN") acc[date].wins += 1;
    if (result === "LOSS") acc[date].losses += 1;
  }

  return Object.entries(acc)
    .map(([date, stats]) => ({ date, ...stats }))
    .sort((a, b) => b.date.localeCompare(a.date));
}

/**
 * Get cumulative P&L data for A-tier picks (oldest-first, for chart).
 */
export async function getPLChartDataFromDb(): Promise<
  { date: string; cumulative_units: number; pick_count: number }[]
> {
  const { data, error } = await supabase
    .from("plays")
    .select(
      `
      date,
      grade:grades(result)
    `
    )
    .eq("tier", "A")
    .order("date", { ascending: true });

  if (error) throw new Error(`getPLChartData: ${error.message}`);

  const byDate: Record<string, ("WIN" | "LOSS")[]> = {};
  for (const row of data || []) {
    const date = row.date as string;
    const result = (row.grade as DbGrade | null)?.result;
    if (result === "WIN" || result === "LOSS") {
      if (!byDate[date]) byDate[date] = [];
      byDate[date].push(result);
    }
  }

  const points: { date: string; cumulative_units: number; pick_count: number }[] = [];
  let cumulative = 0;
  let pickCount = 0;

  for (const date of Object.keys(byDate).sort()) {
    for (const result of byDate[date]) {
      if (result === "WIN") cumulative += 0.909;
      if (result === "LOSS") cumulative -= 1.0;
      pickCount += 1;
    }
    points.push({
      date,
      cumulative_units: Math.round(cumulative * 100) / 100,
      pick_count: pickCount,
    });
  }

  return points;
}
