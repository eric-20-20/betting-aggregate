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
  line_min: number | null;
  line_max: number | null;
  market_line?: number | null;
  line_diff?: number | null;
  game_time_et?: string | null;
}

export interface Factor {
  dimension: string;
  lookup_key: string;
  win_pct: number;
  wilson_lower: number;
  n: number;
  edge?: number;
  verdict?: "positive" | "negative" | "neutral" | "no_data";
}

export interface PrimaryRecord {
  wins: number;
  losses: number;
  n: number;
  win_pct: number;
  wilson_lower: number;
  lookup_level: string;
  label: string;
}

export interface HistoricalRecord {
  wins: number;
  losses: number;
  n: number;
  win_pct: number;
  source: string; // "exact" | "market" | "broad"
  label: string;
}

export interface RecentTrend {
  wins: number;
  losses: number;
  n: number;
  win_pct: number;
  window: number;       // 7, 14, or 30
  source: string;       // cascade level that matched
  label: string;        // sanitized: "2_source / player_prop"
}

export interface ExpertDetail {
  win_pct: number;
  n: number;
  adjustment: number;
  role: "boost" | "penalty";
}

export interface MatchedPattern {
  label: string;
  tier_eligible: "A" | "B";
  hist?: {
    record: string;
    win_pct: number;
    wilson: number;
    n: number;
  };
}

export interface Play {
  rank: number;
  tier: "A" | "B" | "C" | "D";
  wilson_score: number;
  confidence: "high" | "medium" | "low";
  a_tier_eligible?: boolean;
  primary_record: PrimaryRecord;
  signal: Signal;
  factors: Factor[];
  supporting_factors?: Factor[];
  summary: string;
  historical_record: HistoricalRecord;
  recency_adjustment?: number;
  expert_adjustment?: number;
  stat_adjustment?: number;
  day_adjustment?: number;
  line_bucket_adjustment?: number;
  total_adjustment?: number;
  expert_detail?: ExpertDetail;
  recent_trend?: RecentTrend;
  matched_pattern?: MatchedPattern;
  // Backward compat
  composite_score?: number;
  positive_dimensions?: number;
  negative_dimensions?: number;
  small_sample?: boolean;
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
  confidence?: "high" | "medium" | "low";
  positive_dimensions?: number;
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

export interface RecentWindowRecord {
  wins: number;
  losses: number;
  n: number;
  win_pct: number;
}

export interface HotStreak {
  description: string;
  window: number;
  wins: number;
  losses: number;
  n: number;
  win_pct: number;
}

export interface RecentTrendsReport {
  meta: { ref_date: string; windows: number[] };
  by_consensus_strength: Array<{
    consensus_strength: string;
    [key: string]: RecentWindowRecord | string;
  }>;
  by_market_type: Array<{
    market_type: string;
    [key: string]: RecentWindowRecord | string;
  }>;
  top_hot_streaks: HotStreak[];
}

// History types (graded past picks — paid members only)

export interface GradedPlay extends Play {
  result: "WIN" | "LOSS" | "PUSH" | "PENDING";
}

export interface HistoryDay {
  date: string;
  day_of_week: string;
  wins: number;
  losses: number;
  pushes: number;
  pending: number;
  a_wins: number;
  a_losses: number;
  total_picks: number;
}

export interface HistoryDayFile {
  meta: PlaysMeta;
  plays: GradedPlay[];
  summary: { wins: number; losses: number; pushes: number; pending: number };
}

export interface HistoryIndex {
  dates: HistoryDay[];
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
