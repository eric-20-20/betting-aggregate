"use client";

import { useState } from "react";
import type { HistoryDay, GradedPlay } from "@/lib/types";
import type { TimelineEntry } from "@/lib/data";
import PickCard from "@/components/PickCard";
import { formatOdds, formatPickSelection } from "@/lib/format";

interface Props {
  dates: HistoryDay[];
  timeline: TimelineEntry[];
}

interface ExpandedDay {
  plays: GradedPlay[];
  loading: boolean;
  error?: string;
  visible: boolean;
}

function ResultBadge({ result }: { result: string }) {
  const config: Record<string, { label: string; cls: string }> = {
    WIN:     { label: "W", cls: "bg-emerald-500/20 text-emerald-400" },
    LOSS:    { label: "L", cls: "bg-red-500/20 text-red-400" },
    PUSH:    { label: "P", cls: "bg-gray-500/20 text-gray-400" },
    PENDING: { label: "?", cls: "bg-amber-500/20 text-amber-400" },
    VOID:    { label: "N/A", cls: "bg-gray-500/20 text-gray-500" },
  };
  const c = config[result] || config.PENDING;
  return (
    <span className={`${c.cls} text-xs font-bold px-2 py-0.5 rounded min-w-[28px] text-center inline-block`}>
      {c.label}
    </span>
  );
}

function TimelineRow({ entry }: { entry: TimelineEntry }) {
  const fakeSignal = {
    selection: entry.selection,
    market_type: entry.market_type as "player_prop" | "spread" | "total" | "moneyline",
    line: entry.line,
    direction: entry.selection,
    atomic_stat: null,
    away_team: "",
    home_team: "",
    event_key: "",
    day_key: "",
    line_min: null,
    line_max: null,
    signal_id: "",
    score: 0,
    expert_odds: entry.best_odds as number | null,
    sources_count: 0,
    consensus_strength: "",
  };
  const { main, detail } = formatPickSelection(fakeSignal);
  const odds = formatOdds(entry.best_odds);

  return (
    <div className="grid grid-cols-[80px_1fr_auto] md:grid-cols-[100px_72px_1fr_auto] items-center gap-2 px-3 py-2.5 rounded-lg border border-gray-700/40 bg-gray-800/40 hover:bg-gray-800/60 transition-colors">
      <span className="text-gray-500 text-xs">{entry.date}</span>
      <span className="hidden md:block text-gray-600 text-xs">{entry.day_of_week?.slice(0, 3)}</span>
      <div className="min-w-0">
        <div className="flex items-baseline gap-1.5 flex-wrap">
          <span className="text-white text-sm font-medium">{main}</span>
          <span className="text-gray-400 text-xs">{detail}</span>
          {odds && <span className="text-gray-600 text-xs">({odds})</span>}
        </div>
        <div className="flex items-center gap-1.5 flex-wrap mt-0.5">
          <span className="text-gray-600 text-xs">{entry.matchup}</span>
          {entry.pattern_label && (
            <span className="text-emerald-400/60 text-xs">· {entry.pattern_label}</span>
          )}
        </div>
      </div>
      <ResultBadge result={entry.result} />
    </div>
  );
}

function TimelineView({ entries }: { entries: TimelineEntry[] }) {
  const [filter, setFilter] = useState<"all" | "WIN" | "LOSS">("all");

  const graded = entries.filter((e) => e.result === "WIN" || e.result === "LOSS");
  const wins = graded.filter((e) => e.result === "WIN").length;
  const winPct = graded.length > 0 ? ((wins / graded.length) * 100).toFixed(1) : null;

  const filtered =
    filter === "all" ? entries : entries.filter((e) => e.result === filter);

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between flex-wrap gap-2">
        <div className="text-sm">
          {graded.length > 0 && (
            <>
              <span className="text-emerald-400 font-semibold">{wins}W-{graded.length - wins}L</span>
              <span className="text-gray-500 ml-1.5">({winPct}%) across {graded.length} graded A-tier picks</span>
            </>
          )}
        </div>
        <div className="flex gap-1">
          {(["all", "WIN", "LOSS"] as const).map((f) => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              className={`text-xs px-2.5 py-1 rounded transition-colors ${
                filter === f
                  ? "bg-gray-600 text-white"
                  : "bg-gray-800/60 text-gray-500 hover:text-gray-300"
              }`}
            >
              {f === "all" ? "All" : f === "WIN" ? "Wins" : "Losses"}
            </button>
          ))}
        </div>
      </div>

      <div className="grid grid-cols-[80px_1fr_auto] md:grid-cols-[100px_72px_1fr_auto] gap-2 px-3 text-gray-600 text-xs uppercase tracking-wide">
        <span>Date</span>
        <span className="hidden md:block">Day</span>
        <span>Pick</span>
        <span className="text-right">Result</span>
      </div>

      <div className="space-y-1.5">
        {filtered.length === 0 ? (
          <div className="text-gray-500 text-sm text-center py-8">No picks match this filter.</div>
        ) : (
          filtered.map((entry, i) => <TimelineRow key={i} entry={entry} />)
        )}
      </div>
    </div>
  );
}

export default function HistoryClientWrapper({ dates, timeline }: Props) {
  const [tab, setTab] = useState<"timeline" | "byDay">("timeline");
  const [expanded, setExpanded] = useState<Record<string, ExpandedDay>>({});

  async function toggleDate(date: string) {
    const existing = expanded[date];
    // If already loaded, just toggle visibility
    if (existing && !existing.loading && !existing.error && existing.plays.length > 0) {
      setExpanded((prev) => ({
        ...prev,
        [date]: { ...prev[date], visible: !prev[date].visible },
      }));
      return;
    }
    // If collapsed with no data or has error, (re)fetch
    setExpanded((prev) => ({ ...prev, [date]: { plays: [], loading: true, visible: true } }));
    try {
      const res = await fetch(`/api/history/${date}`);
      if (res.status === 401) {
        setExpanded((prev) => ({
          ...prev,
          [date]: { plays: [], loading: false, error: "Your session has expired. Please sign in again.", visible: true },
        }));
        return;
      }
      if (!res.ok) throw new Error("Failed to load");
      const data = await res.json();
      setExpanded((prev) => ({
        ...prev,
        [date]: { plays: data.plays || [], loading: false, visible: true },
      }));
    } catch {
      setExpanded((prev) => ({
        ...prev,
        [date]: { plays: [], loading: false, error: "Failed to load picks. Tap to retry.", visible: true },
      }));
    }
  }

  return (
    <div>
      {/* Tabs */}
      <div className="flex gap-1 mb-5 border-b border-gray-700/50">
        {[
          { key: "timeline" as const, label: "A-Tier Timeline" },
          { key: "byDay" as const, label: "By Day" },
        ].map(({ key, label }) => (
          <button
            key={key}
            onClick={() => setTab(key)}
            className={`text-sm px-3 pb-2 transition-colors border-b-2 -mb-px ${
              tab === key
                ? "text-white border-emerald-500"
                : "text-gray-500 border-transparent hover:text-gray-300"
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      {tab === "timeline" ? (
        <TimelineView entries={timeline} />
      ) : (
        <div className="space-y-2">
          {dates.map((day) => {
            const decided = day.wins + day.losses;
            const winPct = decided > 0 ? (day.wins / decided) * 100 : null;
            const isWinning = winPct !== null && winPct >= 50;
            const exp = expanded[day.date];

            const isOpen = exp?.visible;

            return (
              <div key={day.date}>
                <button
                  onClick={() => toggleDate(day.date)}
                  aria-expanded={!!isOpen}
                  className={`w-full text-left px-4 py-3 rounded-lg border transition-colors ${
                    isOpen
                      ? "bg-gray-800/80 border-gray-600/60"
                      : "bg-gray-800/40 border-gray-700/40 hover:border-gray-600/50"
                  }`}
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3">
                      <span className="text-white font-medium text-sm min-w-[90px]">{day.date}</span>
                      <span className="text-gray-500 text-xs min-w-[80px]">{day.day_of_week}</span>
                    </div>
                    <div className="flex items-center gap-4">
                      {decided > 0 ? (
                        <span className={`text-sm font-semibold ${isWinning ? "text-emerald-400" : "text-red-400"}`}>
                          {day.wins}W-{day.losses}L
                          <span className="text-gray-500 font-normal ml-1">({winPct!.toFixed(0)}%)</span>
                        </span>
                      ) : day.pending > 0 ? (
                        <span className="text-amber-400 text-sm">{day.pending} pending</span>
                      ) : (
                        <span className="text-gray-500 text-sm">no picks</span>
                      )}
                      {(day.a_wins > 0 || day.a_losses > 0) && (
                        <span className="text-emerald-400/70 text-xs">A: {day.a_wins}-{day.a_losses}</span>
                      )}
                      <span className="text-gray-600 text-xs">{day.total_picks} picks</span>
                      <span className="text-gray-500 text-xs">{isOpen ? "▼" : "▶"}</span>
                    </div>
                  </div>
                </button>
                {isOpen && exp && (
                  <div className="mt-2 ml-4 mb-4" role="region" aria-label={`Picks for ${day.date}`}>
                    {exp.loading ? (
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                        {Array.from({ length: 4 }).map((_, i) => (
                          <div key={i} className="bg-gray-800/40 border border-gray-700/30 rounded-lg p-3 space-y-3">
                            <div className="flex justify-between">
                              <div className="h-5 w-16 bg-gray-700/40 rounded animate-pulse" />
                              <div className="h-4 w-24 bg-gray-700/40 rounded animate-pulse" />
                            </div>
                            <div className="h-6 w-3/4 bg-gray-700/40 rounded animate-pulse" />
                            <div className="h-4 w-1/2 bg-gray-700/40 rounded animate-pulse" />
                          </div>
                        ))}
                      </div>
                    ) : exp.error ? (
                      <div className="text-center py-4">
                        <p className="text-red-400 text-sm mb-2">{exp.error}</p>
                        <button
                          onClick={() => {
                            setExpanded((prev) => { const next = { ...prev }; delete next[day.date]; return next; });
                            toggleDate(day.date);
                          }}
                          className="text-sm text-gray-400 hover:text-white transition-colors underline"
                        >
                          Retry
                        </button>
                      </div>
                    ) : exp.plays.length === 0 ? (
                      <div className="text-gray-500 text-sm py-4 text-center">No graded picks for this date</div>
                    ) : (
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                        {exp.plays.map((play) => (
                          <PickCard key={play.signal.signal_id} play={play} result={play.result} />
                        ))}
                      </div>
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
