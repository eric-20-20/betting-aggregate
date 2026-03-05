import {
  getConsensusStrengthRecords,
  getMarketTypeRecords,
  getStatTypeRecords,
  getTopTrends,
  getTierPerformance,
  getRecentTrends,
  formatWinPct,
  formatRecord,
} from "@/lib/data";
import type {
  ConsensusStrengthRecord,
  MarketTypeRecord,
  StatTypeRecord,
  TrendEntry,
  RecentTrendsReport,
  RecentWindowRecord,
  HotStreak,
} from "@/lib/types";
import type { TierPerformance } from "@/lib/data";

export default async function TrackRecordPage() {
  const [consensus, markets, stats, trends, tiers, recentTrends] = await Promise.all([
    getConsensusStrengthRecords(),
    getMarketTypeRecords(),
    getStatTypeRecords(),
    getTopTrends(),
    getTierPerformance(),
    getRecentTrends(),
  ]);

  // Compute headline stats
  const totalGraded = consensus.reduce((sum, r) => sum + r.n, 0);
  const multiSource = consensus.filter(
    (r) => r.consensus_strength !== "1_source"
  );
  const multiSourceWins = multiSource.reduce((s, r) => s + r.wins, 0);
  const multiSourceTotal = multiSource.reduce(
    (s, r) => s + r.wins + r.losses,
    0
  );
  const multiSourceWinPct =
    multiSourceTotal > 0 ? multiSourceWins / multiSourceTotal : 0;

  const bestTierWinPct =
    tiers.length > 0 ? Math.max(...tiers.map((t) => t.win_pct)) : 0;

  return (
    <div className="max-w-6xl mx-auto px-4 py-8">
      <h1 className="text-3xl font-bold text-white mb-2">Track Record</h1>
      <p className="text-gray-400 mb-8">
        Full transparency on our historical performance. All picks are graded
        against final scores.
      </p>

      {/* Headline Stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-10">
        <BigStat
          label="Total Graded Picks"
          value={totalGraded > 0 ? totalGraded.toLocaleString() : "—"}
        />
        <BigStat
          label="Multi-Source Win Rate"
          value={multiSourceTotal > 0 ? formatWinPct(multiSourceWinPct) : "—"}
          accent
        />
        <BigStat
          label="Market Types"
          value={String(markets.length)}
        />
        <BigStat
          label="Best Tier Win Rate"
          value={bestTierWinPct > 0 ? formatWinPct(bestTierWinPct) : "—"}
          accent
        />
      </div>

      {/* Recent Performance */}
      {recentTrends && (
        <RecentPerformanceSection report={recentTrends} />
      )}

      {/* Tier Performance */}
      {tiers.length > 0 && (
        <section className="mb-10">
          <h2 className="text-xl font-bold text-white mb-4">
            Performance by Tier
          </h2>
          <p className="text-gray-500 text-sm mb-4">
            Picks are assigned tiers based on composite score. Higher tiers
            have stronger historical support.
          </p>
          <div className="bg-gray-800/40 rounded-xl overflow-hidden">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-700/50">
                  <th className="text-left p-3 text-gray-400 font-medium">
                    Tier
                  </th>
                  <th className="text-right p-3 text-gray-400 font-medium">
                    Record
                  </th>
                  <th className="text-right p-3 text-gray-400 font-medium">
                    Win %
                  </th>
                  <th className="text-right p-3 text-gray-400 font-medium">
                    Picks
                  </th>
                </tr>
              </thead>
              <tbody>
                {tiers
                  .sort((a, b) => a.tier.localeCompare(b.tier))
                  .map((t) => (
                    <TierRow key={t.tier} tier={t} />
                  ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {/* Consensus Strength Performance */}
      {consensus.length > 0 && (
        <section className="mb-10">
          <h2 className="text-xl font-bold text-white mb-4">
            Consensus Strength
          </h2>
          <p className="text-gray-500 text-sm mb-4">
            When multiple independent sources agree on a pick, the win rate
            improves. More agreement = stronger signal.
          </p>
          <div className="bg-gray-800/40 rounded-xl overflow-hidden">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-700/50">
                  <th className="text-left p-3 text-gray-400 font-medium">
                    Strength
                  </th>
                  <th className="text-right p-3 text-gray-400 font-medium">
                    Record
                  </th>
                  <th className="text-right p-3 text-gray-400 font-medium">
                    Win %
                  </th>
                  <th className="text-right p-3 text-gray-400 font-medium">
                    Picks
                  </th>
                </tr>
              </thead>
              <tbody>
                {consensus.map((row) => (
                  <ConsensusRow key={row.consensus_strength} row={row} />
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {/* Market Type Performance */}
      {markets.length > 0 && (
        <section className="mb-10">
          <h2 className="text-xl font-bold text-white mb-4">
            Performance by Market Type
          </h2>
          <div className="bg-gray-800/40 rounded-xl overflow-hidden">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-700/50">
                  <th className="text-left p-3 text-gray-400 font-medium">
                    Market
                  </th>
                  <th className="text-right p-3 text-gray-400 font-medium">
                    Record
                  </th>
                  <th className="text-right p-3 text-gray-400 font-medium">
                    Win %
                  </th>
                  <th className="text-right p-3 text-gray-400 font-medium">
                    Picks
                  </th>
                </tr>
              </thead>
              <tbody>
                {markets
                  .sort((a, b) => b.n - a.n)
                  .map((row) => (
                    <MarketRow key={row.market_type} row={row} />
                  ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {/* Stat Type Performance */}
      {stats.length > 0 && (
        <section className="mb-10">
          <h2 className="text-xl font-bold text-white mb-4">
            Performance by Stat Type
          </h2>
          <p className="text-gray-500 text-sm mb-4">
            How our picks perform across different statistical categories.
          </p>
          <div className="bg-gray-800/40 rounded-xl overflow-hidden">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-700/50">
                  <th className="text-left p-3 text-gray-400 font-medium">
                    Stat Type
                  </th>
                  <th className="text-right p-3 text-gray-400 font-medium">
                    Record
                  </th>
                  <th className="text-right p-3 text-gray-400 font-medium">
                    Win %
                  </th>
                  <th className="text-right p-3 text-gray-400 font-medium">
                    Picks
                  </th>
                </tr>
              </thead>
              <tbody>
                {stats
                  .filter((s) => s.n >= 10)
                  .sort((a, b) => b.n - a.n)
                  .map((row) => (
                    <StatRow key={row.stat_type} row={row} />
                  ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {/* Top Trends */}
      {trends.length > 0 && (
        <section className="mb-10">
          <h2 className="text-xl font-bold text-white mb-4">
            Strongest Trends
          </h2>
          <p className="text-gray-500 text-sm mb-4">
            Ranked by conservative lower-bound win rate (Wilson score interval).
          </p>
          <div className="grid md:grid-cols-2 gap-3">
            {trends.slice(0, 10).map((t, i) => (
              <div
                key={i}
                className="bg-gray-800/40 border border-gray-700/30 rounded-lg p-4 flex items-center justify-between"
              >
                <div>
                  <span className="text-white font-medium text-sm">
                    {t.key}
                  </span>
                  <span className="text-gray-600 text-xs ml-2">
                    {t.dimension}
                  </span>
                </div>
                <div className="text-right">
                  <span
                    className={`font-semibold ${t.win_pct >= 0.55 ? "text-emerald-400" : t.win_pct >= 0.5 ? "text-gray-300" : "text-red-400"}`}
                  >
                    {formatWinPct(t.win_pct)}
                  </span>
                  <span className="text-gray-600 text-xs ml-2">
                    (n={t.n})
                  </span>
                </div>
              </div>
            ))}
          </div>
        </section>
      )}
    </div>
  );
}

function BigStat({
  label,
  value,
  accent,
}: {
  label: string;
  value: string;
  accent?: boolean;
}) {
  return (
    <div className="bg-gray-800/60 border border-gray-700/40 rounded-xl p-4 text-center">
      <div
        className={`text-2xl font-bold ${accent ? "text-emerald-400" : "text-white"}`}
      >
        {value}
      </div>
      <div className="text-gray-500 text-sm mt-1">{label}</div>
    </div>
  );
}

function TierRow({ tier }: { tier: TierPerformance }) {
  const tierColors: Record<string, string> = {
    A: "text-emerald-400",
    B: "text-blue-400",
    C: "text-amber-400",
    D: "text-red-400",
  };
  return (
    <tr className="border-b border-gray-700/20 hover:bg-gray-700/20">
      <td className="p-3">
        <span className={`font-semibold ${tierColors[tier.tier] || "text-gray-300"}`}>
          Tier {tier.tier}
        </span>
      </td>
      <td className="text-right p-3 text-gray-400">
        {formatRecord(tier.wins, tier.losses)}
      </td>
      <td className="text-right p-3">
        <WinPctCell value={tier.win_pct} />
      </td>
      <td className="text-right p-3 text-gray-500">{tier.n}</td>
    </tr>
  );
}

function ConsensusRow({ row }: { row: ConsensusStrengthRecord }) {
  const labels: Record<string, string> = {
    "1_source": "Single Source",
    "2_source": "2-Source Consensus",
    "3+_source": "3+ Source Consensus",
  };
  return (
    <tr className="border-b border-gray-700/20 hover:bg-gray-700/20">
      <td className="p-3 text-gray-300">
        {labels[row.consensus_strength] || row.consensus_strength}
      </td>
      <td className="text-right p-3 text-gray-400">
        {formatRecord(row.wins, row.losses)}
      </td>
      <td className="text-right p-3">
        <WinPctCell value={row.win_pct} />
      </td>
      <td className="text-right p-3 text-gray-500">{row.n}</td>
    </tr>
  );
}

function MarketRow({ row }: { row: MarketTypeRecord }) {
  const labels: Record<string, string> = {
    spread: "Spreads",
    total: "Totals",
    moneyline: "Moneyline",
    player_prop: "Player Props",
  };
  return (
    <tr className="border-b border-gray-700/20 hover:bg-gray-700/20">
      <td className="p-3 text-gray-300">
        {labels[row.market_type] || row.market_type}
      </td>
      <td className="text-right p-3 text-gray-400">
        {formatRecord(row.wins, row.losses)}
      </td>
      <td className="text-right p-3">
        <WinPctCell value={row.win_pct} />
      </td>
      <td className="text-right p-3 text-gray-500">{row.n}</td>
    </tr>
  );
}

function StatRow({ row }: { row: StatTypeRecord }) {
  return (
    <tr className="border-b border-gray-700/20 hover:bg-gray-700/20">
      <td className="p-3 text-gray-300 capitalize">
        {row.stat_type.replace(/_/g, " ")}
      </td>
      <td className="text-right p-3 text-gray-400">
        {formatRecord(row.wins, row.losses)}
      </td>
      <td className="text-right p-3">
        <WinPctCell value={row.win_pct} />
      </td>
      <td className="text-right p-3 text-gray-500">{row.n}</td>
    </tr>
  );
}

function WinPctCell({ value }: { value: number }) {
  return (
    <span
      className={
        value >= 0.55
          ? "text-emerald-400"
          : value >= 0.5
            ? "text-gray-300"
            : "text-red-400"
      }
    >
      {formatWinPct(value)}
    </span>
  );
}

const CONSENSUS_LABELS: Record<string, string> = {
  "1_source": "Single Source",
  "2_source": "2-Source",
  "3_source": "3-Source",
  "4+_sources": "4+ Sources",
  "3+_source": "3+ Sources",
};

const MARKET_LABELS: Record<string, string> = {
  spread: "Spreads",
  total: "Totals",
  moneyline: "Moneyline",
  player_prop: "Player Props",
};

function RecentPerformanceSection({ report }: { report: RecentTrendsReport }) {
  const { by_consensus_strength, by_market_type, top_hot_streaks, meta } = report;

  // Determine available windows from the data
  const windows = meta?.windows || [14, 30];
  const displayWindows = windows.filter((w) => w >= 14); // skip 7d — too sparse

  return (
    <section className="mb-10">
      <h2 className="text-xl font-bold text-white mb-1">Recent Performance</h2>
      <p className="text-gray-500 text-sm mb-4">
        How picks have performed in recent windows.
        {meta?.ref_date && (
          <span className="text-gray-600"> As of {meta.ref_date}.</span>
        )}
      </p>

      {/* Consensus strength across windows */}
      {by_consensus_strength.length > 0 && (
        <div className="mb-6">
          <h3 className="text-sm font-semibold text-gray-400 uppercase tracking-wider mb-3">
            By Agreement Level
          </h3>
          <div className="bg-gray-800/40 rounded-xl overflow-hidden">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-700/50">
                  <th className="text-left p-3 text-gray-400 font-medium">
                    Strength
                  </th>
                  {displayWindows.map((w) => (
                    <th key={w} className="text-right p-3 text-gray-400 font-medium">
                      Last {w}d
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {by_consensus_strength.map((row) => {
                  const label = CONSENSUS_LABELS[row.consensus_strength] || row.consensus_strength;
                  return (
                    <tr key={row.consensus_strength} className="border-b border-gray-700/20 hover:bg-gray-700/20">
                      <td className="p-3 text-gray-300">{label}</td>
                      {displayWindows.map((w) => {
                        const data = row[`window_${w}`] as RecentWindowRecord | undefined;
                        if (!data || data.n === 0) {
                          return <td key={w} className="text-right p-3 text-gray-600">—</td>;
                        }
                        return (
                          <td key={w} className="text-right p-3">
                            <span className="text-gray-400 text-xs mr-1.5">
                              {formatRecord(data.wins, data.losses)}
                            </span>
                            <WinPctCell value={data.win_pct} />
                          </td>
                        );
                      })}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Market type across windows */}
      {by_market_type.length > 0 && (
        <div className="mb-6">
          <h3 className="text-sm font-semibold text-gray-400 uppercase tracking-wider mb-3">
            By Market Type
          </h3>
          <div className="bg-gray-800/40 rounded-xl overflow-hidden">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-700/50">
                  <th className="text-left p-3 text-gray-400 font-medium">
                    Market
                  </th>
                  {displayWindows.map((w) => (
                    <th key={w} className="text-right p-3 text-gray-400 font-medium">
                      Last {w}d
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {by_market_type.map((row) => {
                  const label = MARKET_LABELS[row.market_type] || row.market_type;
                  return (
                    <tr key={row.market_type} className="border-b border-gray-700/20 hover:bg-gray-700/20">
                      <td className="p-3 text-gray-300">{label}</td>
                      {displayWindows.map((w) => {
                        const data = row[`window_${w}`] as RecentWindowRecord | undefined;
                        if (!data || data.n === 0) {
                          return <td key={w} className="text-right p-3 text-gray-600">—</td>;
                        }
                        return (
                          <td key={w} className="text-right p-3">
                            <span className="text-gray-400 text-xs mr-1.5">
                              {formatRecord(data.wins, data.losses)}
                            </span>
                            <WinPctCell value={data.win_pct} />
                          </td>
                        );
                      })}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Hot Streaks */}
      {top_hot_streaks.length > 0 && (
        <div>
          <h3 className="text-sm font-semibold text-gray-400 uppercase tracking-wider mb-3">
            Hot Streaks
          </h3>
          <div className="grid md:grid-cols-2 gap-3">
            {top_hot_streaks.slice(0, 6).map((streak, i) => (
              <HotStreakCard key={i} streak={streak} />
            ))}
          </div>
        </div>
      )}
    </section>
  );
}

function HotStreakCard({ streak }: { streak: HotStreak }) {
  // Use anonymous_label if available, otherwise description
  const label = (streak as { anonymous_label?: string }).anonymous_label || streak.description;

  return (
    <div className="bg-gray-800/40 border border-gray-700/30 rounded-lg p-4 flex items-center justify-between">
      <div>
        <span className="text-white font-medium text-sm">{label}</span>
        <span className="text-gray-600 text-xs ml-2">last {streak.window}d</span>
      </div>
      <div className="text-right">
        <span className="text-gray-400 text-xs mr-1.5">
          {formatRecord(streak.wins, streak.losses)}
        </span>
        <span
          className={`font-semibold ${
            streak.win_pct >= 0.60
              ? "text-emerald-400"
              : streak.win_pct >= 0.5
                ? "text-gray-300"
                : "text-red-400"
          }`}
        >
          {formatWinPct(streak.win_pct)}
        </span>
      </div>
    </div>
  );
}
