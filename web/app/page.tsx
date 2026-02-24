import Link from "next/link";
import { getPublicPicks, getTodayDate } from "@/lib/data";
import PickCard from "@/components/PickCard";
import TierBadge from "@/components/TierBadge";

export default async function HomePage() {
  const today = getTodayDate();
  const picks = await getPublicPicks(today);

  const totalPicks = picks?.meta.total_signals ?? 0;
  const tierCounts = picks?.meta.tier_counts ?? { A: 0, B: 0, C: 0, D: 0 };

  return (
    <div className="max-w-6xl mx-auto px-4">
      {/* Hero */}
      <section className="py-16 text-center">
        <h1 className="text-4xl md:text-5xl font-bold text-white mb-4">
          Multi-Source Consensus{" "}
          <span className="text-emerald-400">NBA Picks</span>
        </h1>
        <p className="text-gray-400 text-lg max-w-2xl mx-auto mb-8">
          We aggregate picks from 7+ expert sources, find where they agree, and
          score each pick against 8 historical dimensions. No gut feelings —
          just data.
        </p>
        <div className="flex justify-center gap-4">
          <Link
            href="/picks"
            className="bg-emerald-600 hover:bg-emerald-500 text-white font-semibold px-6 py-3 rounded-lg transition-colors"
          >
            View Today&apos;s Picks
          </Link>
          <Link
            href="/track-record"
            className="bg-gray-800 hover:bg-gray-700 text-gray-300 font-semibold px-6 py-3 rounded-lg transition-colors border border-gray-700"
          >
            See Track Record
          </Link>
        </div>
      </section>

      {/* Quick Stats */}
      {picks && (
        <section className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-12">
          <StatCard label="Picks Today" value={String(totalPicks)} />
          <StatCard
            label="A-Tier"
            value={String(tierCounts.A)}
            accent="emerald"
          />
          <StatCard label="Sources" value="7+" />
          <StatCard label="Scoring Factors" value="8" />
        </section>
      )}

      {/* How It Works */}
      <section className="mb-12">
        <h2 className="text-2xl font-bold text-white mb-6 text-center">
          How It Works
        </h2>
        <div className="grid md:grid-cols-4 gap-4">
          <StepCard
            step="1"
            title="Aggregate"
            desc="We scrape 7+ expert sources daily — Action Network, BetQL, Covers, Dimers, SportsLine, OddsTrader, and more."
          />
          <StepCard
            step="2"
            title="Consensus"
            desc="When multiple sources agree on the same pick, we flag it as a consensus signal with higher confidence."
          />
          <StepCard
            step="3"
            title="Score"
            desc="Each pick is scored against 8 historical dimensions — combo win rate, stat type, line bucket, expert record, and more."
          />
          <StepCard
            step="4"
            title="Tier"
            desc="Picks are ranked A through D based on their composite score and number of positive factors."
          />
        </div>
      </section>

      {/* Today's Free Picks */}
      {picks && picks.teaser_picks.length > 0 && (
        <section className="mb-12">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-2xl font-bold text-white">
              Today&apos;s Top Picks
            </h2>
            <span className="text-gray-500 text-sm">{picks.meta.date}</span>
          </div>
          <div className="space-y-3">
            {picks.teaser_picks.map((play) => (
              <PickCard key={play.signal.signal_id} play={play} />
            ))}
          </div>
          <div className="mt-6 text-center">
            <Link
              href="/picks"
              className="text-emerald-400 hover:text-emerald-300 font-medium transition-colors"
            >
              View all {totalPicks} picks →
            </Link>
          </div>
        </section>
      )}

      {/* Tier Legend */}
      <section className="mb-12 bg-gray-800/40 rounded-xl p-6">
        <h3 className="text-lg font-semibold text-white mb-4">
          Tier Breakdown
        </h3>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <TierExplain
            tier="A"
            label="Strong Edge"
            desc="6+ positive scoring factors with significant historical edge"
          />
          <TierExplain
            tier="B"
            label="Solid Pick"
            desc="3-5 positive factors with moderate historical support"
          />
          <TierExplain
            tier="C"
            label="Mixed Signal"
            desc="Some positive factors but limited or conflicting data"
          />
          <TierExplain
            tier="D"
            label="Speculative"
            desc="Few positive factors or limited historical data"
          />
        </div>
      </section>
    </div>
  );
}

function StatCard({
  label,
  value,
  accent,
}: {
  label: string;
  value: string;
  accent?: string;
}) {
  const valueColor = accent === "emerald" ? "text-emerald-400" : "text-white";
  return (
    <div className="bg-gray-800/60 border border-gray-700/40 rounded-xl p-4 text-center">
      <div className={`text-3xl font-bold ${valueColor}`}>{value}</div>
      <div className="text-gray-500 text-sm mt-1">{label}</div>
    </div>
  );
}

function StepCard({
  step,
  title,
  desc,
}: {
  step: string;
  title: string;
  desc: string;
}) {
  return (
    <div className="bg-gray-800/40 border border-gray-700/30 rounded-xl p-5">
      <div className="text-emerald-400 font-bold text-sm mb-2">
        Step {step}
      </div>
      <h3 className="text-white font-semibold mb-2">{title}</h3>
      <p className="text-gray-400 text-sm">{desc}</p>
    </div>
  );
}

function TierExplain({
  tier,
  label,
  desc,
}: {
  tier: "A" | "B" | "C" | "D";
  label: string;
  desc: string;
}) {
  return (
    <div className="flex items-start gap-3">
      <TierBadge tier={tier} />
      <div>
        <div className="text-white font-medium text-sm">{label}</div>
        <div className="text-gray-500 text-xs mt-0.5">{desc}</div>
      </div>
    </div>
  );
}
