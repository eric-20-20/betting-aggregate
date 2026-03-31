"""
Consensus Overlap Analysis

Surfaces two types of hidden signal agreement not captured by the existing
hard/soft consensus system:

  Module 1 — Fuzzy Line Agreement
    Same player + stat + direction from two different sources, lines within ±1.0.
    E.g. Curry OVER 24.5 pts (Action) + Curry OVER 23.5 pts (OddsTrader).

  Module 2 — Cross-Stat Component Overlap
    Different stats on the same player/direction that share a component.
    E.g. Kawhi OVER 23.5 pts (Action) + Kawhi OVER 27.5 pts_reb (BetQL).
    Both are bullish on Kawhi scoring — pts is a component of pts_reb.

Analysis only — does not modify the pipeline or write to the ledger.

Usage:
  python3 scripts/consensus_overlap_analysis.py
  python3 scripts/consensus_overlap_analysis.py --module 1
  python3 scripts/consensus_overlap_analysis.py --module 2
  python3 scripts/consensus_overlap_analysis.py --min-n 20
  python3 scripts/consensus_overlap_analysis.py --date 2026-03-17
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Dict, FrozenSet, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent
SIGNALS_FILE = REPO_ROOT / "data/ledger/signals_latest.jsonl"
GRADES_FILE  = REPO_ROOT / "data/ledger/grades_latest.jsonl"
OUT_FILE     = REPO_ROOT / "data/reports/consensus_overlap_analysis.json"

FUZZY_TOLERANCE = 1.0   # lines within ±1.0 count as fuzzy agreement
DEFAULT_MIN_N   = 20

# Component map — mirrors consensus_nba.py COMBO_TO_ATOMIC
STAT_COMPONENTS: Dict[str, FrozenSet[str]] = {
    "points":      frozenset(["points"]),
    "rebounds":    frozenset(["rebounds"]),
    "assists":     frozenset(["assists"]),
    "threes":      frozenset(["threes"]),
    "threes_made": frozenset(["threes"]),
    "steals":      frozenset(["steals"]),
    "blocks":      frozenset(["blocks"]),
    "pts_reb":     frozenset(["points", "rebounds"]),
    "pts_ast":     frozenset(["points", "assists"]),
    "reb_ast":     frozenset(["rebounds", "assists"]),
    "pts_reb_ast": frozenset(["points", "rebounds", "assists"]),
}


# ──────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────

def wilson_lb(wins: int, n: int, z: float = 1.645) -> float:
    if n == 0:
        return 0.0
    p = wins / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    spread = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return (centre - spread) / denom


def sources_from_record(r: dict) -> List[str]:
    """Return list of individual source_ids for a signal."""
    sc = r.get("sources_combo") or ""
    if "|" in sc:
        return sc.split("|")
    sp = r.get("sources_present") or r.get("sources") or []
    if sp:
        return list(sp)
    if sc:
        return [sc]
    return []


def primary_source(r: dict) -> Optional[str]:
    """Single source_id for solo signals."""
    srcs = sources_from_record(r)
    return srcs[0] if len(srcs) == 1 else (r.get("sources_combo") or None)


def sources_count(r: dict) -> int:
    return len(set(sources_from_record(r))) or 1


def make_sources_combo(srcs: List[str]) -> str:
    return "|".join(sorted(set(srcs)))


# ──────────────────────────────────────────────────────────────────
# Data loading
# ──────────────────────────────────────────────────────────────────

def load_joined(filter_date: Optional[str] = None) -> Tuple[List[dict], int]:
    """Load signals + grades joined on signal_id. Returns (records, skipped_no_line)."""
    signals: Dict[str, dict] = {}
    with open(SIGNALS_FILE) as f:
        for line in f:
            if not line.strip():
                continue
            r = json.loads(line)
            sid = r.get("signal_id")
            if sid:
                signals[sid] = r

    joined: List[dict] = []
    with open(GRADES_FILE) as f:
        for line in f:
            if not line.strip():
                continue
            g = json.loads(line)
            if g.get("result") not in ("WIN", "LOSS"):
                continue
            sid = g.get("signal_id")
            if not sid:
                continue
            s = signals.get(sid)
            if not s:
                continue
            rec = {**s, **g, "signal_id": sid}
            if filter_date:
                dk = rec.get("day_key") or ""
                if filter_date not in dk:
                    continue
            joined.append(rec)

    # Count skipped (no line) separately
    skipped = sum(
        1 for r in joined
        if r.get("market_type") == "player_prop"
        and r.get("player_key")
        and not (r.get("line") or r.get("line_min"))
    )
    return joined, skipped


# ──────────────────────────────────────────────────────────────────
# Module 1 — Fuzzy Line Agreement
# ──────────────────────────────────────────────────────────────────

def run_fuzzy_line(joined: List[dict], min_n: int) -> dict:
    """Find cross-source pairs where same player+stat+direction have lines within ±1.0."""

    # Index by (day, player, stat, direction) → list of records
    idx: Dict[tuple, List[dict]] = defaultdict(list)
    for r in joined:
        if r.get("market_type") != "player_prop":
            continue
        pk = r.get("player_key")
        if not pk:
            continue
        line = r.get("line") or r.get("line_min")
        if line is None:
            continue
        stat = r.get("atomic_stat") or ""
        direction = r.get("direction") or ""
        key = (r.get("day_key", ""), pk, stat, direction)
        idx[key].append(r)

    # For each group, look for cross-source fuzzy matches
    # Accumulate: (sources_combo, stat, direction) → wins, losses, already_hard_count
    class Bucket:
        def __init__(self):
            self.wins = self.losses = self.already_hard = self.total = 0
        def add(self, result: str, already_hard: bool):
            self.total += 1
            if result == "WIN":
                self.wins += 1
            else:
                self.losses += 1
            if already_hard:
                self.already_hard += 1
        @property
        def n(self): return self.wins + self.losses
        @property
        def win_pct(self): return self.wins / self.n if self.n else 0.0
        @property
        def wilson(self): return wilson_lb(self.wins, self.n)

    agg: Dict[tuple, Bucket] = defaultdict(Bucket)
    total_clusters = 0
    total_already_hard = 0

    for key, records in idx.items():
        day_key, player_key, stat, direction = key
        if len(records) < 2:
            continue

        # Build source→record map (use first record per source if dupes)
        src_records: Dict[str, dict] = {}
        for r in records:
            srcs = sources_from_record(r)
            for src in srcs:
                if src not in src_records:
                    src_records[src] = r

        # Need 2+ distinct sources
        if len(src_records) < 2:
            continue

        # Sort sources by their line
        src_list = sorted(src_records.items(), key=lambda x: float(x[1].get("line") or x[1].get("line_min") or 0))

        # Find cross-source pairs within ±1.0
        fuzzy_pairs: List[Tuple[str, str]] = []
        for (src_a, rec_a), (src_b, rec_b) in combinations(src_list, 2):
            if src_a == src_b:
                continue
            line_a = float(rec_a.get("line") or rec_a.get("line_min") or 0)
            line_b = float(rec_b.get("line") or rec_b.get("line_min") or 0)
            if abs(line_a - line_b) <= FUZZY_TOLERANCE:
                fuzzy_pairs.append((src_a, src_b))

        if not fuzzy_pairs:
            continue

        # Collect all sources in the fuzzy cluster
        cluster_sources: set = set()
        for a, b in fuzzy_pairs:
            cluster_sources.add(a)
            cluster_sources.add(b)

        combo = make_sources_combo(list(cluster_sources))
        already_hard = any(sources_count(src_records[s]) >= 2 for s in cluster_sources)
        total_clusters += 1
        if already_hard:
            total_already_hard += 1

        # Pick the "best" graded signal from the cluster to count (avoid double-counting)
        # Prefer highest sources_count, then highest score
        best_rec = max(
            (src_records[s] for s in cluster_sources),
            key=lambda r: (sources_count(r), r.get("score") or 0)
        )
        result = best_rec.get("result")
        if result in ("WIN", "LOSS"):
            agg_key = (combo, stat, direction)
            agg[agg_key].add(result, already_hard)

    # Build output rows
    rows = []
    for (combo, stat, direction), b in sorted(
        agg.items(), key=lambda x: -x[1].wilson
    ):
        if b.n < min_n:
            continue
        rows.append({
            "sources_combo": combo,
            "stat": stat,
            "direction": direction,
            "wins": b.wins,
            "losses": b.losses,
            "n": b.n,
            "win_pct": round(b.win_pct, 4),
            "wilson": round(b.wilson, 4),
            "already_hard_pct": round(b.already_hard / b.total, 3) if b.total else 0,
        })

    return {
        "total_clusters": total_clusters,
        "total_already_hard": total_already_hard,
        "already_hard_pct": round(total_already_hard / total_clusters, 3) if total_clusters else 0,
        "rows": rows,
    }


# ──────────────────────────────────────────────────────────────────
# Module 2 — Cross-Stat Component Overlap
# ──────────────────────────────────────────────────────────────────

def run_cross_stat(joined: List[dict], min_n: int) -> dict:
    """Find cross-source pairs where different stats share a component."""

    # Index by (day, player, direction) → list of records
    idx: Dict[tuple, List[dict]] = defaultdict(list)
    for r in joined:
        if r.get("market_type") != "player_prop":
            continue
        pk = r.get("player_key")
        if not pk:
            continue
        stat = r.get("atomic_stat") or ""
        if stat not in STAT_COMPONENTS:
            continue
        direction = r.get("direction") or ""
        key = (r.get("day_key", ""), pk, direction)
        idx[key].append(r)

    class Bucket:
        def __init__(self):
            self.wins = self.losses = 0
        def add(self, result: str):
            if result == "WIN": self.wins += 1
            else: self.losses += 1
        @property
        def n(self): return self.wins + self.losses
        @property
        def win_pct(self): return self.wins / self.n if self.n else 0.0
        @property
        def wilson(self): return wilson_lb(self.wins, self.n)

    # (stat_a, stat_b, component, direction) → Bucket for atomic signal (stat_a)
    agg: Dict[tuple, Bucket] = defaultdict(Bucket)
    conflicts_agg: Dict[tuple, Bucket] = defaultdict(Bucket)
    total_overlaps = 0
    total_conflicts = 0

    # For score boost simulation: track B-tier signals with/without overlap
    # B-tier = score 30-44
    boost_with_overlap:    Dict[str, Bucket] = defaultdict(Bucket)  # stat → bucket
    boost_without_overlap: Dict[str, Bucket] = defaultdict(Bucket)

    # Track which signal_ids have a cross-stat overlap (for boost sim)
    signal_has_overlap: Dict[str, str] = {}  # signal_id → stat

    for key, records in idx.items():
        day_key, player_key, direction = key
        if len(records) < 2:
            continue

        # Build source→record map (one record per source — pick best)
        src_records: Dict[str, dict] = {}
        for r in records:
            srcs = sources_from_record(r)
            for src in srcs:
                if src not in src_records:
                    src_records[src] = r
                else:
                    # Keep higher sources_count
                    if sources_count(r) > sources_count(src_records[src]):
                        src_records[src] = r

        # Need 2+ distinct sources
        if len(src_records) < 2:
            continue

        src_list = list(src_records.items())

        # Check all pairs with different stats
        for (src_a, rec_a), (src_b, rec_b) in combinations(src_list, 2):
            if src_a == src_b:
                continue
            stat_a = rec_a.get("atomic_stat") or ""
            stat_b = rec_b.get("atomic_stat") or ""
            if stat_a == stat_b:
                continue  # same stat → fuzzy line module handles it
            if stat_a not in STAT_COMPONENTS or stat_b not in STAT_COMPONENTS:
                continue

            # Check for component overlap
            comps_a = STAT_COMPONENTS[stat_a]
            comps_b = STAT_COMPONENTS[stat_b]
            shared = comps_a & comps_b
            if not shared:
                continue

            # Ensure atomic stat is stat_a (the simpler one)
            # If both are combo, pick the one with fewer components as stat_a
            if len(comps_a) > len(comps_b):
                stat_a, stat_b = stat_b, stat_a
                rec_a, rec_b = rec_b, rec_a
                src_a, src_b = src_b, src_a
                shared = STAT_COMPONENTS[stat_a] & STAT_COMPONENTS[stat_b]

            component = sorted(shared)[0]  # primary shared component

            dir_a = rec_a.get("direction") or ""
            dir_b = rec_b.get("direction") or ""

            # Same direction on the shared component = overlap; opposite = conflict
            is_conflict = (dir_a != dir_b)

            result_a = rec_a.get("result")
            if result_a not in ("WIN", "LOSS"):
                continue

            agg_key = (stat_a, stat_b, component, direction)
            if is_conflict:
                total_conflicts += 1
                conflicts_agg[agg_key].add(result_a)
            else:
                total_overlaps += 1
                agg[agg_key].add(result_a)
                signal_has_overlap[rec_a.get("signal_id", "")] = stat_a

    # Score boost simulation
    # Among B-tier signals (score 30-44) on player props: with vs without a cross-stat overlap
    b_tier_with    = Bucket()
    b_tier_without = Bucket()
    for r in joined:
        if r.get("market_type") != "player_prop":
            continue
        score = r.get("score") or 0
        if not (30 <= score <= 44):
            continue
        result = r.get("result")
        if result not in ("WIN", "LOSS"):
            continue
        sid = r.get("signal_id", "")
        if sid in signal_has_overlap:
            b_tier_with.add(result)
        else:
            b_tier_without.add(result)

    # Build output rows
    rows = []
    for (stat_a, stat_b, component, direction), b in sorted(
        agg.items(), key=lambda x: -x[1].wilson
    ):
        if b.n < min_n:
            continue
        cb = conflicts_agg.get((stat_a, stat_b, component, direction))
        conflict_n = cb.n if cb else 0
        rows.append({
            "stat_a":    stat_a,
            "stat_b":    stat_b,
            "component": component,
            "direction": direction,
            "wins":      b.wins,
            "losses":    b.losses,
            "n":         b.n,
            "win_pct":   round(b.win_pct, 4),
            "wilson":    round(b.wilson, 4),
            "conflict_n": conflict_n,
            "conflict_pct": round(conflict_n / (b.n + conflict_n), 3) if (b.n + conflict_n) else 0,
        })

    score_boost_sim = {
        "b_tier_with_overlap": {
            "n": b_tier_with.n,
            "wins": b_tier_with.wins,
            "win_pct": round(b_tier_with.win_pct, 4),
            "wilson": round(b_tier_with.wilson, 4),
            "note": "B-tier signals (score 30-44) that have a cross-stat partner",
        },
        "b_tier_without_overlap": {
            "n": b_tier_without.n,
            "wins": b_tier_without.wins,
            "win_pct": round(b_tier_without.win_pct, 4),
            "wilson": round(b_tier_without.wilson, 4),
            "note": "B-tier signals (score 30-44) without cross-stat partner (baseline)",
        },
        "overlap_lift": round(b_tier_with.win_pct - b_tier_without.win_pct, 4),
    }

    return {
        "total_overlaps":  total_overlaps,
        "total_conflicts": total_conflicts,
        "rows":            rows,
        "score_boost_simulation": score_boost_sim,
    }


# ──────────────────────────────────────────────────────────────────
# Printing
# ──────────────────────────────────────────────────────────────────

W = 120

def _header(title: str):
    print("\n" + "═" * W)
    print(f"  {title}")
    print("═" * W)


def print_fuzzy(result: dict, min_n: int):
    _header(f"MODULE 1 — FUZZY LINE AGREEMENT  (±{FUZZY_TOLERANCE}, n≥{min_n})")
    print(f"  Total fuzzy clusters found: {result['total_clusters']}")
    print(f"  Already captured by hard consensus: {result['total_already_hard']} ({result['already_hard_pct']:.0%})")
    rows = result["rows"]
    if not rows:
        print("  (no groups meet min-n threshold)")
        return
    print()
    print(f"  {'SOURCES_COMBO':35} {'STAT':12} {'DIR':6} {'WIN%':>6} {'WILSON':>7} {'N':>5}  ALREADY_HARD%")
    print("  " + "-" * 85)
    for r in rows:
        star = "★" if r["win_pct"] >= 0.60 and r["wilson"] >= 0.50 else " "
        print(f"  {star} {r['sources_combo']:35} {r['stat']:12} {r['direction']:6} "
              f"{r['win_pct']*100:5.1f}%  {r['wilson']*100:5.1f}%  {r['n']:5}   {r['already_hard_pct']*100:.0f}%")


def print_cross_stat(result: dict, min_n: int):
    _header(f"MODULE 2 — CROSS-STAT COMPONENT OVERLAP  (n≥{min_n})")
    print(f"  Total overlapping pairs: {result['total_overlaps']}")
    print(f"  Total conflicting pairs:  {result['total_conflicts']}")
    rows = result["rows"]
    if not rows:
        print("  (no groups meet min-n threshold)")
    else:
        print()
        print(f"  {'STAT_A':12} {'STAT_B':12} {'COMPONENT':12} {'DIR':6} {'WIN%':>6} {'WILSON':>7} {'N':>5}  CONFLICT%")
        print("  " + "-" * 90)
        for r in rows:
            star = "★" if r["win_pct"] >= 0.60 and r["wilson"] >= 0.50 else " "
            print(f"  {star} {r['stat_a']:12} {r['stat_b']:12} {r['component']:12} {r['direction']:6} "
                  f"{r['win_pct']*100:5.1f}%  {r['wilson']*100:5.1f}%  {r['n']:5}   {r['conflict_pct']*100:.0f}%")

    # Score boost simulation
    sim = result["score_boost_simulation"]
    print()
    print("  ── Score boost simulation (B-tier signals, score 30-44) ──")
    w = sim["b_tier_with_overlap"]
    wo = sim["b_tier_without_overlap"]
    lift = sim["overlap_lift"]
    print(f"  WITH cross-stat partner:    {w['win_pct']*100:.1f}% win  (n={w['n']}, wilson={w['wilson']*100:.1f}%)")
    print(f"  WITHOUT cross-stat partner: {wo['win_pct']*100:.1f}% win  (n={wo['n']}, wilson={wo['wilson']*100:.1f}%)")
    direction = "+" if lift >= 0 else ""
    print(f"  Overlap lift: {direction}{lift*100:.1f}pp")
    if w["n"] > 0:
        would_promote = round(w["n"] * (w["win_pct"] - wo["win_pct"]), 0)
        print(f"  → If +10 score boost applied: estimated {int(abs(would_promote))} additional "
              f"{'correct' if lift >= 0 else 'incorrect'} promotions to A-tier eligible")


# ──────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Consensus overlap analysis")
    parser.add_argument("--module", type=int, choices=[1, 2], help="Run single module (1=fuzzy, 2=cross-stat)")
    parser.add_argument("--min-n", type=int, default=DEFAULT_MIN_N, help=f"Minimum sample size (default {DEFAULT_MIN_N})")
    parser.add_argument("--date", help="Filter to a single date (YYYY-MM-DD)")
    args = parser.parse_args()

    print(f"Loading data...")
    joined, skipped_no_line = load_joined(filter_date=args.date)
    print(f"Loaded {len(joined)} WIN/LOSS graded records  ({skipped_no_line} props skipped — no line)")

    output: dict = {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_records": len(joined),
            "min_n": args.min_n,
            "fuzzy_tolerance": FUZZY_TOLERANCE,
            "filter_date": args.date,
        }
    }

    run_all = args.module is None

    if run_all or args.module == 1:
        fuzzy_result = run_fuzzy_line(joined, args.min_n)
        print_fuzzy(fuzzy_result, args.min_n)
        output["fuzzy_line"] = fuzzy_result

    if run_all or args.module == 2:
        cross_result = run_cross_stat(joined, args.min_n)
        print_cross_stat(cross_result, args.min_n)
        output["cross_stat"] = cross_result

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_FILE, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n  Results → {OUT_FILE.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
