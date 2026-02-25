// TypeScript types matching the sanitized pipeline JSON output

export interface PlaysMeta {
  generated_at_utc: string;
  date: string;
  day_of_week: string;
  total_signals: number;
  scorable_signals: number;
  tier_counts: {
    A: number;
    B: number;
    C: number;
    D: number;
  };
}

export interface Signal {
  signal_id: string;
  selection: string;
  market_type: "player_prop" | "spread" | "total" | "moneyline";
  sources_count: number;        // number of agreeing sources (anonymized)
  consensus_strength: string;   // "1_source", "2_source", "3+_source"
  line: number | null;
  score: number;
  direction: string;
  atomic_stat: string | null;
  best_odds: number | null;
  away_team: string;
  home_team: string;
  event_key: string;
  day_key: string;
}

export interface Factor {
  dimension: string;
  lookup_key: string;
  edge: number;
  win_pct: number;
  wilson_lower: number;
  n: number;
  verdict: "positive" | "negative" | "neutral" | "no_data";
}

export interface HistoricalRecord {
  wins: number;
  losses: number;
  n: number;
  win_pct: number;
  source: string; // "exact" | "market" | "broad"
  label: string;
}

export interface Play {
  rank: number;
  tier: "A" | "B" | "C" | "D";
  composite_score: number;
  positive_dimensions: number;
  negative_dimensions: number;
  signal: Signal;
  factors: Factor[];
  summary: string;
  historical_record: HistoricalRecord;
}

export interface PlaysFile {
  meta: PlaysMeta;
  plays: Play[];
}

// Public picks structure (for non-subscribers)
export interface PublicPicksFile {
  meta: PlaysMeta;
  teaser_picks: Play[];
  locked_picks: LockedPick[];
}

export interface LockedPick {
  rank: number;
  tier: "A" | "B" | "C" | "D";
  market_type: string;
  matchup: string;
  positive_dimensions: number;
}

// Report types (no ROI — only win/loss/win_pct)
export interface ReportRow {
  n: number;
  wins: number;
  losses: number;
  pushes?: number;
  win_pct: number;
}

export interface ConsensusStrengthRecord extends ReportRow {
  consensus_strength: string;
}

export interface MarketTypeRecord extends ReportRow {
  market_type: string;
}

export interface StatTypeRecord extends ReportRow {
  stat_type: string;
}

export interface TrendEntry {
  dimension: string; // mapped from "report" in JSON
  key: string;       // mapped from "description" in JSON
  n: number;
  win_pct: number;
  wilson_lower: number;
  edge_over_50: number;
  sample_flag: string;
}

export type Tier = "A" | "B" | "C" | "D";

export const TIER_COLORS: Record<Tier, { bg: string; text: string; border: string }> = {
  A: { bg: "bg-emerald-500/20", text: "text-emerald-400", border: "border-emerald-500/40" },
  B: { bg: "bg-blue-500/20", text: "text-blue-400", border: "border-blue-500/40" },
  C: { bg: "bg-amber-500/20", text: "text-amber-400", border: "border-amber-500/40" },
  D: { bg: "bg-red-500/20", text: "text-red-400", border: "border-red-500/40" },
};

export const MARKET_LABELS: Record<string, string> = {
  spread: "Spreads",
  total: "Totals",
  moneyline: "Moneyline",
  player_prop: "Player Props",
};
