import type { Play } from "@/lib/types";
import { formatOdds, formatPickSelection, formatWinPct } from "@/lib/format";
import TierBadge from "./TierBadge";
import SourceBadge from "./SourceBadge";
import FactorBar from "./FactorBar";

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

          {/* Sources + odds row */}
          <div className="flex items-center gap-2 mt-2 flex-wrap">
            {signal.sources_present.map((src) => (
              <SourceBadge key={src} source={src} />
            ))}
            {odds && (
              <span className="text-gray-400 text-sm ml-1">({odds})</span>
            )}
            {historical_record && historical_record.n > 0 && (
              <span className="text-gray-500 text-sm ml-auto">
                {historical_record.wins}-{historical_record.losses}{" "}
                <span
                  className={
                    historical_record.win_pct >= 0.55
                      ? "text-emerald-400"
                      : historical_record.win_pct >= 0.5
                        ? "text-gray-300"
                        : "text-red-400"
                  }
                >
                  ({formatWinPct(historical_record.win_pct)})
                </span>
              </span>
            )}
          </div>

          {/* Factor analysis */}
          <div className="mt-3">
            <FactorBar factors={factors} />
          </div>
        </div>
      </div>
    </div>
  );
}
