"use client";

import { useState } from "react";
import type { HistoryDay, GradedPlay } from "@/lib/types";
import PickCard from "@/components/PickCard";

interface Props {
  dates: HistoryDay[];
  adminParam?: string;
}

interface ExpandedDay {
  plays: GradedPlay[];
  loading: boolean;
}

export default function HistoryClientWrapper({ dates, adminParam }: Props) {
  const [expanded, setExpanded] = useState<Record<string, ExpandedDay>>({});

  async function toggleDate(date: string) {
    // Collapse if already expanded
    if (expanded[date] && !expanded[date].loading) {
      setExpanded((prev) => {
        const next = { ...prev };
        delete next[date];
        return next;
      });
      return;
    }

    // Start loading
    setExpanded((prev) => ({
      ...prev,
      [date]: { plays: [], loading: true },
    }));

    try {
      const url = adminParam
        ? `/api/history/${date}?admin=${adminParam}`
        : `/api/history/${date}`;
      const res = await fetch(url);
      if (!res.ok) throw new Error("Failed to load");
      const data = await res.json();
      setExpanded((prev) => ({
        ...prev,
        [date]: { plays: data.plays || [], loading: false },
      }));
    } catch {
      setExpanded((prev) => ({
        ...prev,
        [date]: { plays: [], loading: false },
      }));
    }
  }

  return (
    <div className="space-y-2">
      {dates.map((day) => {
        const decided = day.wins + day.losses;
        const winPct = decided > 0 ? (day.wins / decided) * 100 : null;
        const isWinning = winPct !== null && winPct >= 50;
        const exp = expanded[day.date];

        return (
          <div key={day.date}>
            {/* Day row */}
            <button
              onClick={() => toggleDate(day.date)}
              className={`w-full text-left px-4 py-3 rounded-lg border transition-colors ${
                exp
                  ? "bg-gray-800/80 border-gray-600/60"
                  : "bg-gray-800/40 border-gray-700/40 hover:border-gray-600/50"
              }`}
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <span className="text-white font-medium text-sm min-w-[90px]">
                    {day.date}
                  </span>
                  <span className="text-gray-500 text-xs min-w-[80px]">
                    {day.day_of_week}
                  </span>
                </div>

                <div className="flex items-center gap-4">
                  {/* Record */}
                  {decided > 0 ? (
                    <span
                      className={`text-sm font-semibold ${
                        isWinning ? "text-emerald-400" : "text-red-400"
                      }`}
                    >
                      {day.wins}W-{day.losses}L
                      <span className="text-gray-500 font-normal ml-1">
                        ({winPct!.toFixed(0)}%)
                      </span>
                    </span>
                  ) : day.pending > 0 ? (
                    <span className="text-amber-400 text-sm">
                      {day.pending} pending
                    </span>
                  ) : (
                    <span className="text-gray-500 text-sm">no picks</span>
                  )}

                  {/* A-tier record */}
                  {(day.a_wins > 0 || day.a_losses > 0) && (
                    <span className="text-emerald-400/70 text-xs">
                      A: {day.a_wins}-{day.a_losses}
                    </span>
                  )}

                  {/* Total picks */}
                  <span className="text-gray-600 text-xs">
                    {day.total_picks} picks
                  </span>

                  {/* Expand indicator */}
                  <span className="text-gray-500 text-xs">
                    {exp ? "▼" : "▶"}
                  </span>
                </div>
              </div>
            </button>

            {/* Expanded picks */}
            {exp && (
              <div className="mt-2 ml-4 mb-4">
                {exp.loading ? (
                  <div className="text-gray-500 text-sm py-4 text-center">
                    Loading picks...
                  </div>
                ) : exp.plays.length === 0 ? (
                  <div className="text-gray-500 text-sm py-4 text-center">
                    No graded picks for this date
                  </div>
                ) : (
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                    {exp.plays.map((play) => (
                      <PickCard
                        key={play.signal.signal_id}
                        play={play}
                        result={play.result}
                      />
                    ))}
                  </div>
                )}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}
