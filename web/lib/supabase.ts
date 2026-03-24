import { createClient } from "@supabase/supabase-js";

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!;

// Public (anon) client — safe to use in browser and server components.
// Row-level security in Supabase controls what data is readable.
export const supabase = createClient(supabaseUrl, supabaseAnonKey);

// ── Database Types ──────────────────────────────────────────────────

export interface DbSignal {
  signal_id: string;
  day_key: string;
  event_key: string;
  sport: string;
  away_team: string | null;
  home_team: string | null;
  market_type: string | null;
  selection: string | null;
  direction: string | null;
  line: number | null;
  line_min: number | null;
  line_max: number | null;
  atomic_stat: string | null;
  player_key: string | null;
  sources_combo: string | null;
  sources_count: number | null;
  signal_type: string | null;
  score: number | null;
  run_id: string | null;
  observed_at_utc: string | null;
}

export interface DbGrade {
  signal_id: string;
  day_key: string;
  result: "WIN" | "LOSS" | "PUSH" | "PENDING" | "INELIGIBLE" | null;
  status: string | null;
  line: number | null;
  odds: number | null;
  stat_value: number | null;
  market_type: string | null;
  selection: string | null;
  direction: string | null;
  player_key: string | null;
  provider: string | null;
  graded_at_utc: string | null;
  notes: string | null;
}

export interface DbPlay {
  id: number;
  date: string;
  signal_id: string;
  tier: string | null;
  rank: number | null;
  wilson_score: number | null;
  composite_score: number | null;
  matched_pattern: string | null;
  summary: string | null;
  factors: object | null;
  expert_detail: object | null;
}

export interface DbSignalSource {
  id: number;
  signal_id: string;
  source_id: string | null;
  expert_id: number | null;
  expert_slug: string | null;
  expert_name: string | null;
  line: number | null;
  odds: number | null;
  raw_pick_text: string | null;
}

export interface DbExpertRecord {
  source_id: string;
  expert_slug: string | null;
  market_type: string | null;
  atomic_stat: string | null;
  n: number;
  wins: number;
  losses: number;
  win_pct: number | null;
}
