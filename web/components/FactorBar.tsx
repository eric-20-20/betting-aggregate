import type { Factor } from "@/lib/types";

const DIMENSION_LABELS: Record<string, string> = {
  combo_x_market: "Combo",
  combo_x_stat: "Combo+Stat",
  combo_x_market_x_stat: "Full Match",
  consensus: "Consensus",
  line_bucket: "Line",
  stat_type: "Stat",
  day_of_week: "Day",
  best_expert: "Expert",
};

function verdictColor(verdict: string): string {
  if (verdict === "positive") return "bg-emerald-500";
  if (verdict === "negative") return "bg-red-500";
  return "bg-gray-600";
}

export default function FactorBar({ factors }: { factors: Factor[] }) {
  return (
    <div className="flex flex-wrap gap-1.5">
      {factors.map((f) => {
        const label = DIMENSION_LABELS[f.dimension] || f.dimension;
        const edgeStr =
          f.verdict === "no_data"
            ? "—"
            : `${f.edge > 0 ? "+" : ""}${(f.edge * 100).toFixed(1)}%`;
        return (
          <div
            key={f.dimension}
            className="flex items-center gap-1 text-xs"
            title={`${f.lookup_key}: ${(f.win_pct * 100).toFixed(1)}% win rate (n=${f.n})`}
          >
            <span
              className={`w-2 h-2 rounded-full ${verdictColor(f.verdict)}`}
            />
            <span className="text-gray-400">{label}</span>
            <span
              className={
                f.verdict === "positive"
                  ? "text-emerald-400"
                  : f.verdict === "negative"
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
