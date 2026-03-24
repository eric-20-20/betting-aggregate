import { TIER_COLORS, type Tier } from "@/lib/types";

export default function TierBadge({ tier, confidenceScore }: { tier: Tier; confidenceScore?: number }) {
  const colors = TIER_COLORS[tier];

  if (confidenceScore !== undefined) {
    return (
      <div className="flex items-center gap-1">
        <span
          className={`inline-flex items-center justify-center w-6 h-6 rounded text-xs font-bold ${colors.bg} ${colors.text} border ${colors.border}`}
        >
          {tier}
        </span>
        <span className={`text-sm font-bold tabular-nums ${colors.text}`}>
          {confidenceScore}
        </span>
      </div>
    );
  }

  return (
    <span
      className={`inline-flex items-center justify-center w-8 h-8 rounded-md text-sm font-bold ${colors.bg} ${colors.text} border ${colors.border}`}
    >
      {tier}
    </span>
  );
}
