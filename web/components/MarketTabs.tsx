"use client";

import { MARKET_LABELS } from "@/lib/types";

const TABS = ["all", "spread", "total", "moneyline", "player_prop"] as const;

export default function MarketTabs({
  active,
  onChange,
  counts,
}: {
  active: string;
  onChange: (tab: string) => void;
  counts: Record<string, number>;
}) {
  return (
    <div className="flex gap-1 bg-gray-800/60 rounded-lg p-1 overflow-x-auto">
      {TABS.map((tab) => {
        const label = tab === "all" ? "All" : MARKET_LABELS[tab] || tab;
        const count = tab === "all"
          ? Object.values(counts).reduce((a, b) => a + b, 0)
          : counts[tab] || 0;
        const isActive = active === tab;

        return (
          <button
            key={tab}
            onClick={() => onChange(tab)}
            className={`px-3 py-1.5 rounded-md text-sm font-medium transition-colors whitespace-nowrap ${
              isActive
                ? "bg-gray-700 text-white"
                : "text-gray-400 hover:text-gray-300"
            }`}
          >
            {label}
            <span
              className={`ml-1.5 text-xs ${isActive ? "text-gray-400" : "text-gray-600"}`}
            >
              {count}
            </span>
          </button>
        );
      })}
    </div>
  );
}
