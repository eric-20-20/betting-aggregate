import { getServerSession } from "next-auth";
import { authOptions, isAuthEnabled } from "@/lib/auth";
import { getPublicPicks, getFullPicks } from "@/lib/data";
import { hasAccess } from "@/lib/whop";
import { isAdminRequest } from "@/lib/admin";
import { notFound } from "next/navigation";
import type { Metadata } from "next";
import type { Tier, Play } from "@/lib/types";
import { TIER_COLORS } from "@/lib/types";
import PicksClientWrapper from "./PicksClientWrapper";

export async function generateMetadata({
  params,
}: {
  params: Promise<{ date: string }>;
}): Promise<Metadata> {
  const { date } = await params;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    return { title: "Invalid Date | The Aggregate" };
  }
  const picks = await getPublicPicks(date);
  const total = picks?.meta.total_signals ?? 0;
  const aTier = picks?.meta.tier_counts.A ?? 0;
  const title = total > 0
    ? `${date} NBA Picks (${total} picks, ${aTier} A-tier) | The Aggregate`
    : `${date} NBA Picks | The Aggregate`;
  const description = total > 0
    ? `${total} consensus NBA picks for ${date}. ${aTier} top-tier selections backed by multi-source agreement.`
    : `Consensus NBA picks for ${date}. Aggregated from 7+ expert sources.`;
  return {
    title,
    description,
    openGraph: { title, description },
    alternates: { canonical: `/picks/${date}` },
  };
}

export default async function PicksDatePage({
  params,
}: {
  params: Promise<{ date: string }>;
}) {
  const { date } = await params;

  // Validate date format
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    notFound();
  }

  // Determine if this is a subscriber
  let isSubscriber = await isAdminRequest();

  if (!isSubscriber && isAuthEnabled) {
    const session = await getServerSession(authOptions);
    const whopUserId = session?.whopUserId;
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
        <span className={`${TIER_COLORS.A.bg} ${TIER_COLORS.A.text} px-3 py-1 rounded-lg`}>
          {meta.tier_counts.A} A-tier
        </span>
      </div>
    </div>
  );
}

function NoPicks({ date }: { date: string }) {
  const checkoutUrl = process.env.NEXT_PUBLIC_WHOP_CHECKOUT_URL;
  return (
    <div className="max-w-6xl mx-auto px-4 py-16 text-center">
      <h1 className="text-2xl font-bold text-white mb-4">
        No picks available for {date}
      </h1>
      <p className="text-gray-400 mb-6">
        Picks are generated daily when the pipeline runs. Check back later.
      </p>
      {checkoutUrl && (
        <a
          href={checkoutUrl}
          className="text-emerald-400 hover:text-emerald-300 text-sm transition-colors"
        >
          Subscribe to get notified when picks drop
        </a>
      )}
    </div>
  );
}
