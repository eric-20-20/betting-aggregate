import {
  getComboRecords,
  getSourceRecords,
  getExpertRecords,
  getTopTrends,
  formatWinPct,
  formatRecord,
} from "@/lib/data";
import type { ComboRecord, SourceRecord, ExpertRecord } from "@/lib/types";

export default async function TrackRecordPage() {
  const [combos, sources, experts, trends] = await Promise.all([
    getComboRecords(),
    getSourceRecords(),
    getExpertRecords(),
    getTopTrends(),
  ]);

  // Sort combos by sample size descending, take top 15
  const topCombos = [...combos]
    .filter((c) => c.n >= 50)
    .sort((a, b) => b.n - a.n)
    .slice(0, 15);

  // Sort sources by sample size
  const sortedSources = [...sources]
    .filter((s) => s.n >= 10)
    .sort((a, b) => b.n - a.n);

  // Sort experts by win rate (min 20 picks)
  const topExperts = [...experts]
    .filter((e) => e.n >= 20)
    .sort((a, b) => b.win_pct - a.win_pct)
    .slice(0, 20);

  // Multi-source combos (2+ sources)
  const multiSourceCombos = topCombos.filter(
    (c) => (c.sources_combo || "").includes("|")
  );

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
          value={sources
            .reduce((sum, s) => sum + s.n, 0)
            .toLocaleString()}
        />
        <BigStat
          label="Multi-Source Win Rate"
          value={
            multiSourceCombos.length > 0
              ? formatWinPct(
                  multiSourceCombos.reduce((s, c) => s + c.wins, 0) /
                    (multiSourceCombos.reduce(
                      (s, c) => s + c.wins + c.losses,
                      0
                    ) || 1)
                )
              : "—"
          }
          accent
        />
        <BigStat label="Expert Sources" value={String(sources.length)} />
        <BigStat
          label="Best Combo Win Rate"
          value={
            multiSourceCombos.length > 0
              ? formatWinPct(
                  Math.max(...multiSourceCombos.map((c) => c.win_pct))
                )
              : "—"
          }
          accent
        />
      </div>

      {/* Source Combo Performance */}
      <section className="mb-10">
        <h2 className="text-xl font-bold text-white mb-4">
          Source Combination Performance
        </h2>
        <p className="text-gray-500 text-sm mb-4">
          When multiple sources agree on a pick, the win rate improves. Here are
          the most common source combinations and their records.
        </p>
        <div className="bg-gray-800/40 rounded-xl overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-700/50">
                <th className="text-left p-3 text-gray-400 font-medium">
                  Sources
                </th>
                <th className="text-right p-3 text-gray-400 font-medium">
                  Record
                </th>
                <th className="text-right p-3 text-gray-400 font-medium">
                  Win %
                </th>
                <th className="text-right p-3 text-gray-400 font-medium">
                  ROI
                </th>
                <th className="text-right p-3 text-gray-400 font-medium">
                  Picks
                </th>
              </tr>
            </thead>
            <tbody>
              {topCombos.map((combo) => (
                <ComboRow key={combo.sources_combo} combo={combo} />
              ))}
            </tbody>
          </table>
        </div>
      </section>

      {/* Per-Source Performance */}
      <section className="mb-10">
        <h2 className="text-xl font-bold text-white mb-4">
          Per-Source Performance
        </h2>
        <div className="bg-gray-800/40 rounded-xl overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-700/50">
                <th className="text-left p-3 text-gray-400 font-medium">
                  Source
                </th>
                <th className="text-right p-3 text-gray-400 font-medium">
                  Record
                </th>
                <th className="text-right p-3 text-gray-400 font-medium">
                  Win %
                </th>
                <th className="text-right p-3 text-gray-400 font-medium">
                  ROI
                </th>
                <th className="text-right p-3 text-gray-400 font-medium">
                  Picks
                </th>
              </tr>
            </thead>
            <tbody>
              {sortedSources.map((src) => (
                <SourceRow key={src.source_id} source={src} />
              ))}
            </tbody>
          </table>
        </div>
      </section>

      {/* Expert Leaderboard */}
      <section className="mb-10">
        <h2 className="text-xl font-bold text-white mb-4">
          Top Expert Leaderboard
        </h2>
        <p className="text-gray-500 text-sm mb-4">
          Individual experts ranked by win percentage (minimum 20 picks).
        </p>
        <div className="bg-gray-800/40 rounded-xl overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-700/50">
                <th className="text-left p-3 text-gray-400 font-medium">
                  #
                </th>
                <th className="text-left p-3 text-gray-400 font-medium">
                  Expert
                </th>
                <th className="text-right p-3 text-gray-400 font-medium">
                  Record
                </th>
                <th className="text-right p-3 text-gray-400 font-medium">
                  Win %
                </th>
                <th className="text-right p-3 text-gray-400 font-medium">
                  ROI
                </th>
              </tr>
            </thead>
            <tbody>
              {topExperts.map((expert, i) => (
                <ExpertRow
                  key={expert.expert_name}
                  expert={expert}
                  rank={i + 1}
                />
              ))}
            </tbody>
          </table>
        </div>
      </section>

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

function ComboRow({ combo }: { combo: ComboRecord }) {
  const isMulti = combo.sources_combo.includes("|");
  return (
    <tr className="border-b border-gray-700/20 hover:bg-gray-700/20">
      <td className="p-3">
        <span className={`${isMulti ? "text-emerald-400" : "text-gray-300"}`}>
          {combo.sources_combo}
        </span>
      </td>
      <td className="text-right p-3 text-gray-400">
        {formatRecord(combo.wins, combo.losses)}
      </td>
      <td className="text-right p-3">
        <span
          className={
            combo.win_pct >= 0.55
              ? "text-emerald-400"
              : combo.win_pct >= 0.5
                ? "text-gray-300"
                : "text-red-400"
          }
        >
          {formatWinPct(combo.win_pct)}
        </span>
      </td>
      <td className="text-right p-3">
        <span
          className={
            combo.roi > 0 ? "text-emerald-400" : "text-red-400"
          }
        >
          {combo.roi > 0 ? "+" : ""}
          {(combo.roi * 100).toFixed(1)}%
        </span>
      </td>
      <td className="text-right p-3 text-gray-500">{combo.n}</td>
    </tr>
  );
}

function SourceRow({ source }: { source: SourceRecord }) {
  return (
    <tr className="border-b border-gray-700/20 hover:bg-gray-700/20">
      <td className="p-3 text-gray-300 capitalize">{source.source_id}</td>
      <td className="text-right p-3 text-gray-400">
        {formatRecord(source.wins, source.losses)}
      </td>
      <td className="text-right p-3">
        <span
          className={
            source.win_pct >= 0.55
              ? "text-emerald-400"
              : source.win_pct >= 0.5
                ? "text-gray-300"
                : "text-red-400"
          }
        >
          {formatWinPct(source.win_pct)}
        </span>
      </td>
      <td className="text-right p-3">
        <span
          className={source.roi > 0 ? "text-emerald-400" : "text-red-400"}
        >
          {source.roi > 0 ? "+" : ""}
          {(source.roi * 100).toFixed(1)}%
        </span>
      </td>
      <td className="text-right p-3 text-gray-500">{source.n}</td>
    </tr>
  );
}

function ExpertRow({
  expert,
  rank,
}: {
  expert: ExpertRecord;
  rank: number;
}) {
  return (
    <tr className="border-b border-gray-700/20 hover:bg-gray-700/20">
      <td className="p-3 text-gray-600 w-8">{rank}</td>
      <td className="p-3 text-gray-300">{expert.expert_name}</td>
      <td className="text-right p-3 text-gray-400">
        {formatRecord(expert.wins, expert.losses)}
      </td>
      <td className="text-right p-3">
        <span
          className={
            expert.win_pct >= 0.55
              ? "text-emerald-400"
              : expert.win_pct >= 0.5
                ? "text-gray-300"
                : "text-red-400"
          }
        >
          {formatWinPct(expert.win_pct)}
        </span>
      </td>
      <td className="text-right p-3">
        <span
          className={expert.roi > 0 ? "text-emerald-400" : "text-red-400"}
        >
          {expert.roi > 0 ? "+" : ""}
          {(expert.roi * 100).toFixed(1)}%
        </span>
      </td>
    </tr>
  );
}
