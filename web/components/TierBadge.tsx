import { TIER_COLORS, type Tier } from "@/lib/types";

export default function TierBadge({ tier }: { tier: Tier }) {
  const colors = TIER_COLORS[tier];
  return (
    <span
      className={`inline-flex items-center justify-center w-8 h-8 rounded-md text-sm font-bold ${colors.bg} ${colors.text} border ${colors.border}`}
    >
      {tier}
    </span>
  );
}
