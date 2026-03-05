#!/usr/bin/env python3
"""
Grade daily plays against actual game results.

Loads plays files and matches them against the grades ledger to show
how our exported picks actually performed.

Usage:
    python3 scripts/grade_plays.py                    # All dates
    python3 scripts/grade_plays.py --date 2026-02-24  # Specific date
    python3 scripts/grade_plays.py --tier A           # Only A-tier
    python3 scripts/grade_plays.py --rescore          # Re-run scorer and grade
"""

import argparse
import json
import glob
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

PLAYS_DIR = Path("data/plays")
GRADES_PATH = Path("data/ledger/grades_latest.jsonl")


def load_grades_index() -> Dict[str, dict]:
    """Index grades by signal_id for matching."""
    index: Dict[str, dict] = {}
    if not GRADES_PATH.exists():
        return index
    with open(GRADES_PATH) as f:
        for line in f:
            g = json.loads(line)
            sid = g.get("signal_id", "")
            if not sid or sid in index:
                continue
            index[sid] = g
    return index


def grade_plays_file(
    path: Path,
    grades: Dict[str, dict],
    tier_filter: Optional[str] = None,
) -> dict:
    """Grade all plays in a file against the grades ledger."""
    with open(path) as f:
        data = json.load(f)

    plays = data.get("plays", [])
    date_str = data.get("meta", {}).get("date", path.stem.replace("plays_", ""))
    day_of_week = data.get("meta", {}).get("day_of_week", "")

    results = []
    for p in plays:
        tier = p.get("tier", "D")
        if tier not in ("A", "B"):
            continue
        if tier_filter and tier != tier_filter:
            continue

        sig = p.get("signal", {})
        sid = sig.get("signal_id", "")

        grade = grades.get(sid)
        result = grade.get("result") if grade else "UNGRADED"
        if result not in ("WIN", "LOSS", "PUSH"):
            result = "UNGRADED"

        wilson = p.get("wilson_score") or p.get("composite_score", 0) or 0
        results.append({
            "tier": tier,
            "selection": sig.get("selection", "?"),
            "market_type": sig.get("market_type", "?"),
            "line": sig.get("line"),
            "wilson_score": wilson,
            "confidence": p.get("confidence", "?"),
            "a_tier_eligible": p.get("a_tier_eligible", False),
            "result": result,
            "sources_combo": sig.get("sources_combo", ""),
        })

    wins = sum(1 for r in results if r["result"] == "WIN")
    losses = sum(1 for r in results if r["result"] == "LOSS")
    pushes = sum(1 for r in results if r["result"] == "PUSH")
    ungraded = sum(1 for r in results if r["result"] == "UNGRADED")
    decided = wins + losses
    win_pct = wins / decided if decided > 0 else None

    return {
        "date": date_str,
        "day_of_week": day_of_week,
        "total_picks": len(results),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "ungraded": ungraded,
        "decided": decided,
        "win_pct": win_pct,
        "results": results,
    }


def format_selection_short(sel: str, market: str) -> str:
    """Shorten selection for display."""
    sel = sel.replace("NBA:", "")
    if "::" in sel:
        parts = sel.split("::")
        player = parts[0]
        stat = parts[1] if len(parts) > 1 else ""
        direction = parts[2][0] if len(parts) > 2 else ""
        return f"{player} {stat} {direction}"
    return sel


def main():
    parser = argparse.ArgumentParser(description="Grade daily plays")
    parser.add_argument("--date", type=str, help="Specific date (YYYY-MM-DD)")
    parser.add_argument("--tier", choices=["A", "B"], help="Filter by tier")
    parser.add_argument("--verbose", action="store_true", help="Show individual picks")
    args = parser.parse_args()

    grades = load_grades_index()
    print(f"Loaded {len(grades)} graded signals\n")

    if args.date:
        files = [PLAYS_DIR / f"plays_{args.date}.json"]
    else:
        files = sorted(PLAYS_DIR.glob("plays_*.json"))

    all_wins = 0
    all_losses = 0
    all_pushes = 0
    all_ungraded = 0
    daily_results = []

    for path in files:
        if not path.exists():
            print(f"  File not found: {path}")
            continue

        day = grade_plays_file(path, grades, tier_filter=args.tier)
        daily_results.append(day)

        all_wins += day["wins"]
        all_losses += day["losses"]
        all_pushes += day["pushes"]
        all_ungraded += day["ungraded"]

        # Print daily summary
        wp_str = f"{day['win_pct']*100:.1f}%" if day["win_pct"] is not None else "N/A"
        tier_counts = defaultdict(int)
        for r in day["results"]:
            tier_counts[r["tier"]] += 1
        tier_str = " ".join(f"{k}={v}" for k, v in sorted(tier_counts.items()))

        print(f"  {day['date']} ({day['day_of_week']:3s})  "
              f"{day['wins']}W-{day['losses']}L-{day['pushes']}P  "
              f"({day['ungraded']} ungraded)  "
              f"Win: {wp_str:>6s}  "
              f"[{tier_str}]")

        if args.verbose or args.date:
            for r in day["results"]:
                sel_short = format_selection_short(r["selection"], r["market_type"])
                result_color = {"WIN": "+", "LOSS": "-", "PUSH": "=", "UNGRADED": "?"}
                mark = result_color.get(r["result"], "?")
                a_tag = " ★" if r.get("a_tier_eligible") else ""
                line_str = f" {r['line']}" if r.get("line") is not None else ""
                print(f"      {mark} {r['tier']} {sel_short:35s}{line_str:>7s}  "
                      f"W={r['wilson_score']*100:.1f}%  {r['result']}{a_tag}")
            print()

    # Overall summary
    total_decided = all_wins + all_losses
    if total_decided > 0 or all_ungraded > 0:
        print()
        print("  " + "=" * 60)
        overall_wp = f"{all_wins/total_decided*100:.1f}%" if total_decided > 0 else "N/A"
        print(f"  OVERALL: {all_wins}W-{all_losses}L-{all_pushes}P "
              f"({all_ungraded} ungraded)  "
              f"Win: {overall_wp}")
        print(f"  Sample: {total_decided} decided picks across {len(daily_results)} days")
        if total_decided > 0 and total_decided < 30:
            print(f"  ⚠ Small sample — need 30+ decided picks for meaningful results")


if __name__ == "__main__":
    main()
