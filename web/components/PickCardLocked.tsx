import type { LockedPick } from "@/lib/types";
import { MARKET_LABELS } from "@/lib/types";
import TierBadge from "./TierBadge";

export default function PickCardLocked({ pick }: { pick: LockedPick }) {
  const marketLabel = MARKET_LABELS[pick.market_type] || pick.market_type;

  return (
    <div className="bg-gray-800/40 border border-gray-700/30 rounded-xl p-4 relative overflow-hidden">
      <div className="flex items-start gap-3">
        <TierBadge tier={pick.tier} />

        <div className="flex-1 min-w-0">
          <div className="flex items-baseline justify-between gap-2">
            <div className="flex items-center gap-2">
              <span className="text-gray-400 font-medium">{marketLabel}</span>
              <span className="text-gray-600">|</span>
              <span className="text-gray-500 text-sm">{pick.matchup}</span>
            </div>
            {pick.positive_dimensions > 0 && (
              <span className="text-emerald-500/60 text-xs">
                {pick.positive_dimensions} positive factors
              </span>
            )}
          </div>

          {/* Blurred placeholder */}
          <div className="mt-2 flex gap-2">
            <div className="h-5 w-20 bg-gray-700/40 rounded-full" />
            <div className="h-5 w-16 bg-gray-700/40 rounded-full" />
            <div className="h-5 w-12 bg-gray-700/40 rounded-full" />
          </div>
          <div className="mt-2 flex gap-1.5">
            <div className="h-3 w-8 bg-gray-700/30 rounded" />
            <div className="h-3 w-10 bg-gray-700/30 rounded" />
            <div className="h-3 w-6 bg-gray-700/30 rounded" />
            <div className="h-3 w-12 bg-gray-700/30 rounded" />
          </div>
        </div>
      </div>

      {/* Lock overlay */}
      <div className="absolute inset-0 bg-gradient-to-r from-transparent via-transparent to-gray-900/60 pointer-events-none" />
    </div>
  );
}
