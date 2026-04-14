"use client";

import { useRef } from "react";
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
  const tabsRef = useRef<(HTMLButtonElement | null)[]>([]);

  function handleKeyDown(e: React.KeyboardEvent, idx: number) {
    let nextIdx = idx;
    if (e.key === "ArrowRight") {
      nextIdx = (idx + 1) % TABS.length;
    } else if (e.key === "ArrowLeft") {
      nextIdx = (idx - 1 + TABS.length) % TABS.length;
    } else {
      return;
    }
    e.preventDefault();
    const nextTab = TABS[nextIdx];
    onChange(nextTab);
    tabsRef.current[nextIdx]?.focus();
  }

  return (
    <div role="tablist" aria-label="Filter by market type" className="flex gap-1 bg-gray-800/60 rounded-lg p-1 overflow-x-auto">
      {TABS.map((tab, idx) => {
        const label = tab === "all" ? "All" : MARKET_LABELS[tab] || tab;
        const count = tab === "all"
          ? Object.values(counts).reduce((a, b) => a + b, 0)
          : counts[tab] || 0;
        const isActive = active === tab;

        return (
          <button
            key={tab}
            ref={(el) => { tabsRef.current[idx] = el; }}
            role="tab"
            aria-selected={isActive}
            tabIndex={isActive ? 0 : -1}
            onClick={() => onChange(tab)}
            onKeyDown={(e) => handleKeyDown(e, idx)}
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
