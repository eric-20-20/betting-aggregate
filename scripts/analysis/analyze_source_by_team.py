#!/usr/bin/env python3
"""Analyze source × team performance for spread picks.

For each source, compute win rate when that source picked each team to cover.
Helps identify which sources are reliably accurate on specific teams.

Usage:
    python3 scripts/analyze_source_by_team.py
    python3 scripts/analyze_source_by_team.py --min-n 20
    python3 scripts/analyze_source_by_team.py --market spread
    python3 scripts/analyze_source_by_team.py --market spread moneyline
    python3 scripts/analyze_source_by_team.py --source action betql
"""
from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

OCC_PATH = Path("data/analysis/graded_occurrences_latest.jsonl")


def wilson_lower(wins: int, n: int, z: float = 1.96) -> float:
    if n == 0:
        return 0.0
    p = wins / n
    denom = 1 + z * z / n
    center = p + z * z / (2 * n)
    spread = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)
    return (center - spread) / denom


def load_occurrences(path: Path) -> List[Dict[str, Any]]:
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def get_team(row: Dict[str, Any]) -> Optional[str]:
    """Extract the picked team from a spread/moneyline signal."""
    sel = row.get("selection") or ""
    # For spreads/MLs, selection is a 2-3 char team abbreviation
    if sel and len(sel) <= 4 and sel.isupper():
        return sel
    # Fallback: try direction field
    d = row.get("direction") or ""
    if d and len(d) <= 4 and d.isupper() and d not in ("OVER", "UNDER", "TEAM"):
        return d
    return None


def analyze(
    rows: List[Dict[str, Any]],
    markets: List[str],
    min_n: int,
    source_filter: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Aggregate win rates by (source_id, team, market_type)."""
    # key: (source_id, team, market_type) -> metrics
    agg: Dict[Tuple[str, str, str], Dict[str, Any]] = defaultdict(
        lambda: {"wins": 0, "losses": 0, "pushes": 0, "n": 0}
    )

    for row in rows:
        if row.get("grade_status") != "GRADED":
            continue
        result = row.get("result")
        if result not in ("WIN", "LOSS", "PUSH"):
            continue
        market = row.get("market_type") or ""
        if market not in markets:
            continue

        source_id = row.get("occ_source_id") or ""
        if not source_id:
            continue
        if source_filter and source_id not in source_filter:
            continue

        team = get_team(row)
        if not team:
            continue

        key = (source_id, team, market)
        m = agg[key]
        m["n"] += 1
        if result == "WIN":
            m["wins"] += 1
        elif result == "LOSS":
            m["losses"] += 1
        elif result == "PUSH":
            m["pushes"] += 1

    results = []
    for (source_id, team, market), m in agg.items():
        n = m["wins"] + m["losses"]  # exclude pushes from win rate
        if n < min_n:
            continue
        wp = m["wins"] / n
        wl = wilson_lower(m["wins"], n)
        results.append({
            "source_id": source_id,
            "team": team,
            "market_type": market,
            "wins": m["wins"],
            "losses": m["losses"],
            "pushes": m["pushes"],
            "n": n,
            "win_pct": round(wp, 4),
            "wilson_lower": round(wl, 4),
        })

    # Sort by wilson_lower descending
    results.sort(key=lambda r: (-r["wilson_lower"], -r["n"]))
    return results


def print_report(results: List[Dict[str, Any]], min_wilson: float = 0.0) -> None:
    filtered = [r for r in results if r["wilson_lower"] >= min_wilson]

    print(f"\n{'Source':<22} {'Team':<6} {'Market':<12} {'W-L':>8}  {'Win%':>6}  {'Wilson':>7}  {'n':>5}")
    print("-" * 72)

    current_source = None
    for r in filtered:
        src = r["source_id"]
        if src != current_source:
            if current_source is not None:
                print()
            current_source = src

        record = f"{r['wins']}-{r['losses']}"
        marker = " ★" if r["wilson_lower"] >= 0.52 else ("  " if r["wilson_lower"] >= 0.48 else "")
        print(
            f"  {src:<20} {r['team']:<6} {r['market_type']:<12} {record:>8}  "
            f"{r['win_pct']*100:>5.1f}%  {r['wilson_lower']:>7.4f}{marker}"
        )


def main():
    parser = argparse.ArgumentParser(description="Source × team performance analysis")
    parser.add_argument("--min-n", type=int, default=15, help="Minimum graded picks (default: 15)")
    parser.add_argument("--min-wilson", type=float, default=0.48, help="Minimum Wilson lower bound to show (default: 0.48)")
    parser.add_argument(
        "--market",
        nargs="+",
        default=["spread"],
        choices=["spread", "moneyline", "total", "player_prop"],
        help="Markets to analyze (default: spread)",
    )
    parser.add_argument("--source", nargs="+", default=None, help="Filter to specific sources")
    parser.add_argument("--show-all", action="store_true", help="Show all results (ignore --min-wilson)")
    args = parser.parse_args()

    print(f"Loading occurrences from {OCC_PATH}...")
    rows = load_occurrences(OCC_PATH)
    print(f"  {len(rows):,} total rows loaded")

    results = analyze(rows, args.market, args.min_n, args.source)
    print(f"  {len(results):,} (source, team, market) combos with n >= {args.min_n}")

    min_wilson = 0.0 if args.show_all else args.min_wilson
    print(f"\n=== Source × Team Performance (market={args.market}, min_n={args.min_n}, min_wilson={min_wilson}) ===")
    print_report(results, min_wilson=min_wilson)

    # Summary: top entries per source
    print("\n\n=== TOP ENTRIES BY SOURCE (Wilson >= 0.50) ===")
    by_source: Dict[str, List[Dict]] = defaultdict(list)
    for r in results:
        if r["wilson_lower"] >= 0.50:
            by_source[r["source_id"]].append(r)

    for src in sorted(by_source):
        top = by_source[src][:8]
        print(f"\n  {src} ({len(by_source[src])} team combos above 0.50 Wilson):")
        for r in top:
            record = f"{r['wins']}-{r['losses']}"
            print(f"    {r['team']:<6} {r['market_type']:<10} {record:>8}  {r['win_pct']*100:.1f}%  Wilson={r['wilson_lower']:.4f}")


if __name__ == "__main__":
    main()
