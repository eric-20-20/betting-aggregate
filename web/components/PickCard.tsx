import type { Play, Factor } from "@/lib/types";
import { formatOdds, formatPickSelection, formatWinPct } from "@/lib/format";
import TierBadge from "./TierBadge";
import ConsensusIndicator from "./ConsensusIndicator";

const KEY_DIMENSIONS = new Set([
  "consensus",
  "line_bucket",
  "stat_type",
  "best_expert",
]);

function FactorDots({ factors }: { factors: Factor[] }) {
  const key = factors.filter((f) => KEY_DIMENSIONS.has(f.dimension));
  const positive = key.filter((f) => f.verdict === "positive").length;
  const total = key.length;

  if (total === 0) return null;

  return (
    <div className="flex items-center gap-1.5" title={`${positive} of ${total} key factors positive`}>
      {key.map((f) => (
        <span
          key={f.dimension}
          className={`w-1.5 h-1.5 rounded-full ${
            f.verdict === "positive"
              ? "bg-emerald-500"
              : f.verdict === "negative"
                ? "bg-red-500"
                : "bg-gray-600"
          }`}
        />
      ))}
    </div>
  );
}

export default function PickCard({ play }: { play: Play }) {
  const { signal, factors, historical_record } = play;
  const { main, detail } = formatPickSelection(signal);
  const matchup = `${signal.away_team} @ ${signal.home_team}`;
  const odds = formatOdds(signal.best_odds);

  return (
    <div className="bg-gray-800/60 border border-gray-700/50 rounded-xl p-4 hover:border-gray-600/60 transition-colors">
      <div className="flex items-start gap-3">
        <TierBadge tier={play.tier} />

        <div className="flex-1 min-w-0">
          {/* Top row: pick + matchup */}
          <div className="flex items-baseline justify-between gap-2">
            <div>
              <span className="text-white font-semibold text-lg">{main}</span>
              <span className="text-gray-400 ml-2 text-sm">{detail}</span>
            </div>
            <span className="text-gray-500 text-sm shrink-0">{matchup}</span>
          </div>

          {/* Consensus + odds + win rate row */}
          <div className="flex items-center gap-2 mt-2 flex-wrap">
            <ConsensusIndicator count={signal.sources_count} />
            {odds && (
              <span className="text-gray-400 text-sm ml-1">({odds})</span>
            )}
            {historical_record && historical_record.n > 0 && (
              <span className="text-sm ml-auto flex items-center gap-2">
                <span
                  className={`font-semibold ${
                    historical_record.win_pct >= 0.55
                      ? "text-emerald-400"
                      : historical_record.win_pct >= 0.5
                        ? "text-gray-300"
                        : "text-red-400"
                  }`}
                >
                  {formatWinPct(historical_record.win_pct)} win rate
                </span>
                <span className="text-gray-500">
                  · {historical_record.n} similar picks
                </span>
              </span>
            )}
          </div>

          {/* Compact factor dots */}
          {factors.length > 0 && (
            <div className="mt-2">
              <FactorDots factors={factors} />
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
