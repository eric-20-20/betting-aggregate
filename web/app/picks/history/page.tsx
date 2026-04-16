import { getHistoryIndex, getAggregatedTimeline } from "@/lib/data";
import { requireSubscriber } from "@/lib/access";
import HistoryClientWrapper from "./HistoryClientWrapper";
import Link from "next/link";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Pick History | The Aggregate",
  description: "See how our past consensus picks performed — full graded results for every pick.",
  alternates: { canonical: "/picks/history" },
};

export default async function HistoryPage() {
  const isSubscriber = await requireSubscriber();

  if (!isSubscriber) {
    return (
      <div className="max-w-6xl mx-auto px-4 py-16 text-center">
        <h1 className="text-2xl font-bold text-white mb-4">Pick History</h1>
        <p className="text-gray-400 mb-4">
          Full graded results for every past pick — available to subscribers.
        </p>
        <ul className="text-gray-500 text-sm mb-6 space-y-1">
          <li>Every A-tier pick graded against final scores</li>
          <li>Win/loss records and cumulative P&L tracking</li>
          <li>Full factor breakdown for every historical pick</li>
        </ul>
        <div className="flex justify-center gap-4">
          {process.env.NEXT_PUBLIC_WHOP_CHECKOUT_URL && (
            <a
              href={process.env.NEXT_PUBLIC_WHOP_CHECKOUT_URL}
              className="bg-emerald-600 hover:bg-emerald-500 text-white px-6 py-2 rounded-lg transition-colors"
            >
              Subscribe Now
            </a>
          )}
          <Link
            href="/picks"
            className="bg-gray-700 hover:bg-gray-600 text-white px-6 py-2 rounded-lg transition-colors"
          >
            View Free Picks
          </Link>
        </div>
      </div>
    );
  }

  const [historyIndex, timeline] = await Promise.all([
    getHistoryIndex(),
    getAggregatedTimeline(["A"]),
  ]);

  if (!historyIndex || historyIndex.dates.length === 0) {
    return (
      <div className="max-w-6xl mx-auto px-4 py-16 text-center">
        <h1 className="text-2xl font-bold text-white mb-4">Pick History</h1>
        <p className="text-gray-400">No history data available yet.</p>
      </div>
    );
  }

  // Compute running totals
  const totals = historyIndex.dates.reduce(
    (acc, d) => ({
      wins: acc.wins + d.wins,
      losses: acc.losses + d.losses,
      pushes: acc.pushes + d.pushes,
      pending: acc.pending + d.pending,
      a_wins: acc.a_wins + d.a_wins,
      a_losses: acc.a_losses + d.a_losses,
      picks: acc.picks + d.total_picks,
    }),
    { wins: 0, losses: 0, pushes: 0, pending: 0, a_wins: 0, a_losses: 0, picks: 0 }
  );

  const decided = totals.wins + totals.losses;
  const winPct = decided > 0 ? ((totals.wins / decided) * 100).toFixed(1) : "N/A";
  const aDecided = totals.a_wins + totals.a_losses;
  const aWinPct = aDecided > 0 ? ((totals.a_wins / aDecided) * 100).toFixed(1) : "N/A";

  return (
    <div className="max-w-6xl mx-auto px-4 py-8">
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-white">Pick History</h1>
        <p className="text-gray-500 text-sm mt-1">
          How our consensus picks performed
        </p>
      </div>

      {/* Summary stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-8">
        <div className="bg-gray-800/60 border border-gray-700/50 rounded-lg p-3">
          <div className="text-gray-500 text-xs uppercase tracking-wide">Overall</div>
          <div className="text-white text-lg font-bold">
            {totals.wins}W-{totals.losses}L
          </div>
          <div className="text-gray-400 text-sm">{winPct}%</div>
        </div>
        <div className="bg-emerald-900/20 border border-emerald-500/30 rounded-lg p-3">
          <div className="text-emerald-400/70 text-xs uppercase tracking-wide">A-Tier</div>
          <div className="text-emerald-400 text-lg font-bold">
            {totals.a_wins}W-{totals.a_losses}L
          </div>
          <div className="text-emerald-400/70 text-sm">{aWinPct}%</div>
        </div>
        <div className="bg-gray-800/60 border border-gray-700/50 rounded-lg p-3">
          <div className="text-gray-500 text-xs uppercase tracking-wide">Total Picks</div>
          <div className="text-white text-lg font-bold">{totals.picks}</div>
          <div className="text-gray-400 text-sm">{historyIndex.dates.length} days</div>
        </div>
        <div className="bg-gray-800/60 border border-gray-700/50 rounded-lg p-3">
          <div className="text-gray-500 text-xs uppercase tracking-wide">Pending</div>
          <div className="text-amber-400 text-lg font-bold">{totals.pending}</div>
          <div className="text-gray-400 text-sm">awaiting results</div>
        </div>
      </div>

      <HistoryClientWrapper
        dates={historyIndex.dates}
        timeline={timeline}
      />
    </div>
  );
}
