import type { Play, RecentTrend, ExpertDetail } from "@/lib/types";
import { formatOdds, formatPickSelection, formatWinPct } from "@/lib/format";
import TierBadge from "./TierBadge";
import ConsensusIndicator from "./ConsensusIndicator";

function ConfidenceBadge({ confidence }: { confidence: string }) {
  const config: Record<string, { label: string; color: string }> = {
    high: { label: "High confidence", color: "text-emerald-400" },
    medium: { label: "Medium confidence", color: "text-blue-400" },
    low: { label: "Limited data", color: "text-amber-400" },
  };
  const c = config[confidence] || config.low;
  return <span className={`text-xs ${c.color}`}>{c.label}</span>;
}

function parseMatchup(signal: {
  away_team: string | null;
  home_team: string | null;
  event_key: string;
}): string {
  if (signal.away_team && signal.home_team) {
    return `${signal.away_team} @ ${signal.home_team}`;
  }
  const ek = signal.event_key || "";
  const atIdx = ek.lastIndexOf("@");
  if (atIdx > 0) {
    const beforeAt = ek.substring(0, atIdx);
    const colonIdx = beforeAt.lastIndexOf(":");
    const away = colonIdx >= 0 ? beforeAt.substring(colonIdx + 1) : beforeAt;
    const home = ek.substring(atIdx + 1);
    return `${away} @ ${home}`;
  }
  return "";
}

const STAT_LABELS: Record<string, string> = {
  points: "points",
  rebounds: "rebounds",
  assists: "assists",
  threes: "threes",
  steals: "steals",
  blocks: "blocks",
  pts_reb: "pts+reb",
  pts_ast: "pts+ast",
  reb_ast: "reb+ast",
  pts_reb_ast: "pts+reb+ast",
  turnovers: "turnovers",
};

const MARKET_LABELS: Record<string, string> = {
  player_prop: "player prop",
  spread: "spread",
  total: "total",
  moneyline: "moneyline",
};

function formatLine(val: number, market: string): string {
  if (market === "spread") {
    return val > 0 ? `+${val}` : `${val}`;
  }
  return `${val}`;
}

/** Show the range of lines across sources when they disagree. */
function LineRange({ signal }: { signal: Play["signal"] }) {
  const { line, line_min, line_max, market_type } = signal;
  if (line_min == null || line_max == null || line == null) return null;
  if (line_min === line_max) return null; // all sources agree

  const spread = Math.abs(line_max - line_min);

  // Skip ranges that are too wide (likely data quality issues)
  if (spread > 5) return null;

  const isTight = spread <= 1.5;

  return (
    <span
      className={`text-xs ml-1 ${isTight ? "text-gray-500" : "text-amber-400/80"}`}
      title={`Sources range: ${formatLine(line_min, market_type)} to ${formatLine(line_max, market_type)}`}
    >
      ({formatLine(line_min, market_type)} to {formatLine(line_max, market_type)})
    </span>
  );
}

/** Show market line vs pick line when available. */
function MarketLineTag({ signal }: { signal: Play["signal"] }) {
  const { market_line, line, line_diff, market_type, direction } = signal;
  if (market_line == null || line == null || line_diff == null) return null;
  if (line_diff === 0) return null;

  // Determine if pick line is "better" (more favorable) than market
  let isBetter: boolean;
  if (market_type === "spread") {
    // For spreads: getting more points (higher line) is better for underdogs
    // Getting a smaller negative number is better for favorites
    // In both cases: pick_line > market_line means better for the bettor
    isBetter = line > market_line;
  } else if (market_type === "total") {
    // For overs: lower line is better (easier to go over)
    // For unders: higher line is better (easier to stay under)
    const isOver = direction === "OVER" || direction === "over";
    isBetter = isOver ? line < market_line : line > market_line;
  } else {
    return null;
  }

  const mktStr = formatLine(market_line, market_type);

  return (
    <span
      className={`text-xs ${isBetter ? "text-emerald-400/80" : "text-amber-400/80"}`}
      title={`Market line: ${mktStr}. Pick line ${isBetter ? "is better" : "has moved"}.`}
    >
      mkt {mktStr} {isBetter ? "\u2714" : "\u26A0"}
    </span>
  );
}

/** Turn the internal label into a user-friendly description. */
function describeRecord(hr: { label: string }): string {
  const parts = hr.label.split(" / ");

  if (parts.length >= 3) {
    const stat = STAT_LABELS[parts[2]] || parts[2];
    if (stat === "all") {
      const market = MARKET_LABELS[parts[1]] || parts[1];
      return `similar ${market} picks`;
    }
    return `similar ${stat} props`;
  }

  if (parts.length === 2) {
    const market = MARKET_LABELS[parts[1]] || parts[1];
    return `similar ${market} picks`;
  }

  return "similar picks";
}

function RecentTrendTag({ trend }: { trend: RecentTrend }) {
  const decided = trend.wins + trend.losses;
  if (decided < 5) return null;

  const isHot = trend.win_pct >= 0.60;
  const isCold = trend.win_pct < 0.45;

  return (
    <div className="flex items-baseline gap-1.5">
      <span className={`text-xs font-medium ${
        isHot ? "text-emerald-400" : isCold ? "text-red-400" : "text-gray-400"
      }`}>
        Last {trend.window}d: {trend.wins}-{trend.losses} ({formatWinPct(trend.win_pct)})
      </span>
      <span className="text-gray-600 text-xs">
        {describeRecord(trend)}
      </span>
    </div>
  );
}

function AdjustmentBar({ play }: { play: Play }) {
  const items: { label: string; value: number }[] = [];
  if (play.recency_adjustment && play.recency_adjustment !== 0)
    items.push({ label: "Trend", value: play.recency_adjustment });
  if (play.expert_adjustment && play.expert_adjustment !== 0)
    items.push({ label: "Analyst", value: play.expert_adjustment });
  if (play.stat_adjustment && play.stat_adjustment !== 0)
    items.push({ label: "Stat", value: play.stat_adjustment });
  if (play.day_adjustment && play.day_adjustment !== 0)
    items.push({ label: "Day", value: play.day_adjustment });
  if (play.line_bucket_adjustment && play.line_bucket_adjustment !== 0)
    items.push({ label: "Line", value: play.line_bucket_adjustment });

  if (items.length === 0) return null;

  return (
    <div className="flex flex-wrap gap-2 text-xs">
      {items.map((item) => (
        <span
          key={item.label}
          className={item.value > 0 ? "text-emerald-400/70" : "text-red-400/70"}
        >
          {item.label} {item.value > 0 ? "+" : ""}{(item.value * 100).toFixed(1)}%
        </span>
      ))}
    </div>
  );
}

function ResultBadge({ result }: { result: string }) {
  const config: Record<string, { label: string; bg: string; text: string }> = {
    WIN: { label: "W", bg: "bg-emerald-500/20", text: "text-emerald-400" },
    LOSS: { label: "L", bg: "bg-red-500/20", text: "text-red-400" },
    PUSH: { label: "P", bg: "bg-gray-500/20", text: "text-gray-400" },
    PENDING: { label: "?", bg: "bg-amber-500/20", text: "text-amber-400" },
    VOID: { label: "N/A", bg: "bg-gray-500/20", text: "text-gray-500" },
  };
  const c = config[result] || config.PENDING;
  return (
    <span className={`${c.bg} ${c.text} text-xs font-bold px-2 py-0.5 rounded`}>
      {c.label}
    </span>
  );
}

export default function PickCard({ play, result }: { play: Play; result?: string }) {
  const { signal, historical_record } = play;
  const pr = play.primary_record || historical_record;
  const { main, detail } = formatPickSelection(signal);
  const matchup = parseMatchup(signal);
  const odds = formatOdds((signal.expert_odds ?? (signal as any).best_odds) as number | null);
  const hasRecord = pr && pr.n > 0;
  const isATier = play.tier === "A";

  return (
    <div className={`${
      isATier
        ? "bg-emerald-900/20 border-emerald-500/30 hover:border-emerald-400/50"
        : "bg-gray-800/60 border-gray-700/50 hover:border-gray-600/60"
    } border rounded-lg p-3 transition-colors flex flex-col gap-2`}>
      {/* Top: tier + result badge + matchup + game time */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <TierBadge tier={play.tier} confidenceScore={play.confidence_score} />
          {result && <ResultBadge result={result} />}
        </div>
        <div className="flex items-center gap-1.5">
          {matchup && (
            <span className="text-gray-500 text-xs">{matchup}</span>
          )}
          {signal.game_time_et && (
            <span className="text-gray-600 text-xs">· {signal.game_time_et}</span>
          )}
        </div>
      </div>

      {/* Pick name + detail + line range */}
      <div className="flex flex-wrap items-baseline gap-x-1.5">
        <span className="text-white font-semibold text-lg">{main}</span>
        <span className="text-gray-400">{detail}</span>
        <LineRange signal={signal} />
      </div>

      {/* Consensus + odds + market line */}
      <div className="flex items-center gap-2">
        <ConsensusIndicator count={signal.sources_count} />
        {odds && (
          <span className="text-gray-500 text-sm">({odds})</span>
        )}
        <MarketLineTag signal={signal} />
      </div>

      {/* Pattern match badge for A-tier */}
      {play.matched_pattern && play.tier === "A" && (
        <div className="text-xs text-emerald-400/80 bg-emerald-900/20 rounded px-1.5 py-0.5 w-fit">
          {play.matched_pattern.label}
          {play.matched_pattern.hist && (
            <span className="text-emerald-400/60 ml-1">
              ({play.matched_pattern.hist.record})
            </span>
          )}
        </div>
      )}

      {/* Primary record — the reason this pick is ranked here */}
      {hasRecord && (
        <div className="flex flex-wrap items-baseline gap-1.5 border-t border-gray-700/40 pt-2">
          <span
            className={`text-sm font-semibold ${
              pr.win_pct >= 0.55
                ? "text-emerald-400"
                : pr.win_pct >= 0.5
                  ? "text-gray-300"
                  : "text-red-400"
            }`}
          >
            {formatWinPct(pr.win_pct)}
          </span>
          <span className="text-gray-500 text-xs">
            ({pr.wins}-{pr.losses}) in {pr.n} {describeRecord(pr)}
          </span>
          <ConfidenceBadge confidence={play.confidence || (pr.n < 30 ? "low" : "medium")} />
        </div>
      )}

      {/* Recent trend */}
      {play.recent_trend && play.recent_trend.wins + play.recent_trend.losses >= 5 && (
        <div className="border-t border-gray-700/40 pt-1.5">
          <RecentTrendTag trend={play.recent_trend} />
        </div>
      )}

      {/* Adjustment modifiers */}
      <AdjustmentBar play={play} />
    </div>
  );
}
