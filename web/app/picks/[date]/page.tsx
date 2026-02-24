import { getServerSession } from "next-auth";
import { authOptions, isAuthEnabled } from "@/lib/auth";
import { getPublicPicks, getFullPicks } from "@/lib/data";
import { hasAccess } from "@/lib/whop";
import type { Tier, Play } from "@/lib/types";
import PicksClientWrapper from "./PicksClientWrapper";

export default async function PicksDatePage({
  params,
}: {
  params: Promise<{ date: string }>;
}) {
  const { date } = await params;

  // Determine if this is a subscriber
  let isSubscriber = false;
  if (isAuthEnabled) {
    const session = await getServerSession(authOptions);
    const whopUserId = (session as any)?.whopUserId as string | undefined;
    if (whopUserId) {
      isSubscriber = await hasAccess(whopUserId);
    }
  }

  // Subscriber view: load full picks from private JSON
  if (isSubscriber) {
    const fullPicks = await getFullPicks(date);
    if (!fullPicks) {
      return <NoPicks date={date} />;
    }

    const { meta, plays } = fullPicks;
    const marketCounts: Record<string, number> = {};
    for (const p of plays) {
      const mt = p.signal.market_type;
      marketCounts[mt] = (marketCounts[mt] || 0) + 1;
    }

    const tierOrder: Tier[] = ["A", "B", "C", "D"];
    const playsByTier: Record<string, Play[]> = {};
    for (const tier of tierOrder) {
      playsByTier[tier] = plays.filter((p) => p.tier === tier);
    }

    return (
      <div className="max-w-6xl mx-auto px-4 py-8">
        <PicksHeader meta={meta} />
        <PicksClientWrapper
          allPlays={plays}
          playsByTier={playsByTier}
          marketCounts={marketCounts}
          tierOrder={tierOrder}
          isSubscriber={true}
        />
      </div>
    );
  }

  // Free / non-subscriber view
  const picks = await getPublicPicks(date);
  if (!picks) {
    return <NoPicks date={date} />;
  }

  const { meta, teaser_picks, locked_picks } = picks;
  const marketCounts: Record<string, number> = {};
  for (const p of teaser_picks) {
    const mt = p.signal.market_type;
    marketCounts[mt] = (marketCounts[mt] || 0) + 1;
  }
  for (const p of locked_picks) {
    const mt = p.market_type;
    marketCounts[mt] = (marketCounts[mt] || 0) + 1;
  }

  const tierOrder: Tier[] = ["A", "B", "C", "D"];
  const lockedByTier: Record<string, typeof locked_picks> = {};
  for (const tier of tierOrder) {
    lockedByTier[tier] = locked_picks.filter((p) => p.tier === tier);
  }

  return (
    <div className="max-w-6xl mx-auto px-4 py-8">
      <PicksHeader meta={meta} />
      <PicksClientWrapper
        teaserPicks={teaser_picks}
        lockedPicks={locked_picks}
        lockedByTier={lockedByTier}
        marketCounts={marketCounts}
        tierOrder={tierOrder}
        isSubscriber={false}
      />
    </div>
  );
}

function PicksHeader({
  meta,
}: {
  meta: {
    date: string;
    day_of_week: string;
    total_signals: number;
    tier_counts: { A: number };
  };
}) {
  return (
    <div className="flex items-center justify-between mb-6">
      <div>
        <h1 className="text-2xl font-bold text-white">Daily Picks</h1>
        <p className="text-gray-500 text-sm mt-1">
          {meta.date} &middot; {meta.day_of_week}
        </p>
      </div>
      <div className="flex gap-3 text-sm">
        <span className="bg-gray-800 px-3 py-1 rounded-lg text-gray-400">
          {meta.total_signals} picks
        </span>
        <span className="bg-emerald-500/20 text-emerald-400 px-3 py-1 rounded-lg">
          {meta.tier_counts.A} A-tier
        </span>
      </div>
    </div>
  );
}

function NoPicks({ date }: { date: string }) {
  return (
    <div className="max-w-6xl mx-auto px-4 py-16 text-center">
      <h1 className="text-2xl font-bold text-white mb-4">
        No picks available for {date}
      </h1>
      <p className="text-gray-400">
        Picks are generated daily when the pipeline runs. Check back later.
      </p>
    </div>
  );
}
