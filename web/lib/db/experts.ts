/**
 * Supabase query functions for expert analysis.
 *
 * These use the expert_record and combo_record views created in supabase_schema.sql.
 * Requires SUPABASE_URL and SUPABASE_ANON_KEY env vars.
 */
import "server-only";
import { supabase, type DbExpertRecord } from "../supabase";

export interface ExpertStat {
  source_id: string;
  expert_slug: string | null;
  market_type: string | null;
  atomic_stat: string | null;
  n: number;
  wins: number;
  losses: number;
  win_pct: number | null;
}

export interface ComboStat {
  sources_combo: string;
  market_type: string | null;
  atomic_stat: string | null;
  direction: string | null;
  n: number;
  wins: number;
  losses: number;
  win_pct: number | null;
}

export interface ExpertRecency {
  source_id: string;
  expert_slug: string | null;
  win_pct_30d: number | null;
  win_pct_all: number | null;
  n_30d: number;
  n_all: number;
}

/**
 * Expert win rates from the expert_record view.
 * Optionally filter by source or minimum n.
 */
export async function getExpertStats(options?: {
  sourceId?: string;
  minN?: number;
  marketType?: string;
}): Promise<ExpertStat[]> {
  let query = supabase.from("expert_record").select("*");

  if (options?.sourceId) {
    query = query.eq("source_id", options.sourceId);
  }
  if (options?.marketType) {
    query = query.eq("market_type", options.marketType);
  }
  if (options?.minN) {
    query = query.gte("n", options.minN);
  }

  query = query.order("win_pct", { ascending: false });

  const { data, error } = await query;
  if (error) throw new Error(`getExpertStats: ${error.message}`);
  return (data || []) as ExpertStat[];
}

/**
 * Source combo win rates from the combo_record view.
 */
export async function getComboStats(options?: {
  minN?: number;
  marketType?: string;
  direction?: string;
}): Promise<ComboStat[]> {
  let query = supabase.from("combo_record").select("*");

  if (options?.marketType) {
    query = query.eq("market_type", options.marketType);
  }
  if (options?.direction) {
    query = query.eq("direction", options.direction);
  }
  if (options?.minN) {
    query = query.gte("n", options.minN);
  }

  query = query.order("win_pct", { ascending: false });

  const { data, error } = await query;
  if (error) throw new Error(`getComboStats: ${error.message}`);
  return (data || []) as ComboStat[];
}

/**
 * Expert recency: compare last 30 days vs all-time win rate.
 * Useful for detecting hot/cold streaks.
 */
export async function getExpertRecency(
  sinceDate: string  // "2026-02-17" for last 30 days
): Promise<ExpertRecency[]> {
  // We do this client-side by fetching signal_sources joined with grades
  // filtered by day_key range
  const { data: all, error: e1 } = await supabase
    .from("signal_sources")
    .select(
      `
      source_id,
      expert_slug,
      signal:signals(day_key),
      grade:grades(result)
    `
    )
    .not("signal_sources.source_id", "is", null);

  if (e1) throw new Error(`getExpertRecency: ${e1.message}`);

  const acc: Record<
    string,
    { n_all: number; wins_all: number; n_30d: number; wins_30d: number }
  > = {};

  for (const row of all || []) {
    const sourceId = row.source_id as string;
    const expertSlug = row.expert_slug as string | null;
    const dayKey = (row.signal as { day_key: string } | null)?.day_key;
    const result = (row.grade as { result: string } | null)?.result;
    if (!result || !["WIN", "LOSS"].includes(result)) continue;

    const key = `${sourceId}|${expertSlug || ""}`;
    if (!acc[key]) acc[key] = { n_all: 0, wins_all: 0, n_30d: 0, wins_30d: 0 };
    acc[key].n_all += 1;
    if (result === "WIN") acc[key].wins_all += 1;

    // Convert day_key "NBA:2026:02:17" → date "2026-02-17"
    if (dayKey) {
      const parts = dayKey.split(":");
      const dateFromKey = parts.length === 4 ? `${parts[1]}-${parts[2]}-${parts[3]}` : null;
      if (dateFromKey && dateFromKey >= sinceDate) {
        acc[key].n_30d += 1;
        if (result === "WIN") acc[key].wins_30d += 1;
      }
    }
  }

  return Object.entries(acc)
    .map(([key, stats]) => {
      const [sourceId, expertSlug] = key.split("|");
      return {
        source_id: sourceId,
        expert_slug: expertSlug || null,
        win_pct_all: stats.n_all > 0 ? Math.round((stats.wins_all / stats.n_all) * 1000) / 1000 : null,
        win_pct_30d: stats.n_30d > 0 ? Math.round((stats.wins_30d / stats.n_30d) * 1000) / 1000 : null,
        n_all: stats.n_all,
        n_30d: stats.n_30d,
      };
    })
    .filter((r) => r.n_all >= 20)
    .sort((a, b) => (b.win_pct_30d ?? 0) - (a.win_pct_30d ?? 0));
}
