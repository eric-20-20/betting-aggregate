import type { Factor } from "@/lib/types";

const DIMENSION_LABELS: Record<string, string> = {
  combo_x_market: "Signal",
  combo_x_stat: "Stat Match",
  combo_x_market_x_stat: "Full Match",
  consensus: "Consensus",
  line_bucket: "Line",
  stat_type: "Stat",
  day_of_week: "Day",
  best_expert: "Analyst",
};

function verdictColor(verdict: string): string {
  if (verdict === "positive") return "bg-emerald-500";
  if (verdict === "negative") return "bg-red-500";
  return "bg-gray-600";
}

function friendlyLookupKey(key: string): string {
  return key
    .replace("1_source", "single-source")
    .replace("2_source", "2-source consensus")
    .replace("3+_source", "3+ source consensus")
    .replace("top analyst", "top-performing analyst")
    .replace(" / ", " + ");
}

export default function FactorBar({ factors }: { factors: Factor[] }) {
  return (
    <div className="flex flex-wrap gap-1.5">
      {factors.map((f) => {
        const label = DIMENSION_LABELS[f.dimension] || f.dimension;
        const edge = f.edge ?? 0;
        const edgeStr =
          f.verdict === "no_data"
            ? "—"
            : `${edge > 0 ? "+" : ""}${(edge * 100).toFixed(1)}%`;
        const tooltip =
          f.verdict === "no_data"
            ? `${label}: Insufficient data`
            : `${friendlyLookupKey(f.lookup_key)}: ${(f.win_pct * 100).toFixed(1)}% win rate (n=${f.n})`;
        return (
          <div
            key={f.dimension}
            className="flex items-center gap-1 text-xs"
            title={tooltip}
          >
            <span
              className={`w-2 h-2 rounded-full ${verdictColor(f.verdict ?? "neutral")}`}
            />
            <span className="text-gray-400">{label}</span>
            <span
              className={
                (f.verdict ?? "neutral") === "positive"
                  ? "text-emerald-400"
                  : (f.verdict ?? "neutral") === "negative"
                    ? "text-red-400"
                    : "text-gray-500"
              }
            >
              {edgeStr}
            </span>
          </div>
        );
      })}
    </div>
  );
}
