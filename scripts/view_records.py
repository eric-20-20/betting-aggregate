#!/usr/bin/env python3
"""View betting records by source combinations and filters.

Usage:
    # Show all source combo records
    python3 scripts/view_records.py

    # Filter by specific sources (must include these sources)
    python3 scripts/view_records.py --sources action,betql

    # Filter by exact source combo
    python3 scripts/view_records.py --exact action|betql

    # Filter by market type
    python3 scripts/view_records.py --market player_prop

    # Combine filters
    python3 scripts/view_records.py --sources action,betql --market spread

    # Minimum sample size
    python3 scripts/view_records.py --min-bets 10

    # Sport selection
    python3 scripts/view_records.py --sport NCAAB

    # Show individual source records (not combos)
    python3 scripts/view_records.py --individual

    # Sort by different criteria
    python3 scripts/view_records.py --sort win_pct
    python3 scripts/view_records.py --sort roi
    python3 scripts/view_records.py --sort n

Examples:
    # What's the record when Action and BetQL agree?
    python3 scripts/view_records.py --exact action|betql

    # What's the record when Action and BetQL agree on player props?
    python3 scripts/view_records.py --exact action|betql --market player_prop

    # Show all combos with at least 5 bets, sorted by win rate
    python3 scripts/view_records.py --min-bets 5 --sort win_pct
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional


def get_report_path(report_name: str, sport: str = "NBA") -> Path:
    base = Path("data/reports")
    if sport.upper() == "NCAAB":
        return base / "ncaab" / report_name
    return base / report_name


def load_report(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"rows": [], "meta": {}}
    with path.open() as f:
        return json.load(f)


def filter_by_sources_contain(rows: List[Dict], sources: List[str]) -> List[Dict]:
    """Filter to rows where sources_combo contains ALL specified sources."""
    filtered = []
    source_set = set(s.lower().strip() for s in sources)
    for row in rows:
        combo = row.get("sources_combo") or row.get("source_id") or ""
        row_sources = set(s.lower().strip() for s in combo.split("|") if s)
        if source_set.issubset(row_sources):
            filtered.append(row)
    return filtered


def filter_by_exact_combo(rows: List[Dict], exact: str) -> List[Dict]:
    """Filter to rows matching exact sources_combo."""
    exact_normalized = "|".join(sorted(s.lower().strip() for s in exact.split("|") if s))
    filtered = []
    for row in rows:
        combo = row.get("sources_combo") or row.get("source_id") or ""
        combo_normalized = "|".join(sorted(s.lower().strip() for s in combo.split("|") if s))
        if combo_normalized == exact_normalized:
            filtered.append(row)
    return filtered


def filter_by_market(rows: List[Dict], market: str) -> List[Dict]:
    """Filter to rows matching market type."""
    return [r for r in rows if (r.get("market_type") or "").lower() == market.lower()]


def format_pct(val: Optional[float]) -> str:
    if val is None:
        return "N/A"
    return f"{val*100:.1f}%"


def format_roi(val: Optional[float]) -> str:
    if val is None:
        return "N/A"
    return f"{val*100:+.1f}%"


def format_units(val: Optional[float]) -> str:
    if val is None:
        return "N/A"
    return f"{val:+.2f}u"


def print_record_table(rows: List[Dict], title: str = "Records", show_market: bool = False) -> None:
    """Print records in a formatted table."""
    if not rows:
        print("No records match your filters.")
        return

    # Header
    print(f"\n{'='*90}")
    print(f"  {title}")
    print(f"{'='*90}")

    if show_market:
        header = f"{'Sources':<25} {'Market':<12} {'Bets':>5} {'W':>4} {'L':>4} {'P':>3} {'Win%':>7} {'ROI':>8} {'Net':>9}"
    else:
        header = f"{'Sources':<30} {'Bets':>5} {'W':>4} {'L':>4} {'P':>3} {'Win%':>7} {'ROI':>8} {'Net':>9}"

    print(header)
    print("-" * 90)

    for row in rows:
        combo = row.get("sources_combo") or row.get("source_id") or "unknown"
        market = row.get("market_type") or ""
        n = row.get("n", 0)
        wins = row.get("wins", 0)
        losses = row.get("losses", 0)
        pushes = row.get("pushes", 0)
        win_pct = row.get("win_pct")
        roi = row.get("roi")
        net = row.get("net_units")

        win_str = format_pct(win_pct)
        roi_str = format_roi(roi)
        net_str = format_units(net)

        if show_market:
            print(f"{combo:<25} {market:<12} {n:>5} {wins:>4} {losses:>4} {pushes:>3} {win_str:>7} {roi_str:>8} {net_str:>9}")
        else:
            print(f"{combo:<30} {n:>5} {wins:>4} {losses:>4} {pushes:>3} {win_str:>7} {roi_str:>8} {net_str:>9}")

    print("-" * 90)
    print(f"  {len(rows)} record(s) displayed")
    print()


def print_summary(rows: List[Dict]) -> None:
    """Print aggregate summary of filtered rows."""
    if not rows:
        return

    total_n = sum(r.get("n", 0) for r in rows)
    total_wins = sum(r.get("wins", 0) for r in rows)
    total_losses = sum(r.get("losses", 0) for r in rows)
    total_pushes = sum(r.get("pushes", 0) for r in rows)
    total_net = sum(r.get("net_units", 0) or 0 for r in rows)

    decided = total_wins + total_losses
    agg_win_pct = total_wins / decided if decided > 0 else None

    print(f"  Aggregate: {total_n} bets, {total_wins}W-{total_losses}L-{total_pushes}P, {format_pct(agg_win_pct)} win rate, {format_units(total_net)} net")


def main():
    parser = argparse.ArgumentParser(
        description="View betting records by source combinations and filters",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        "--sources", type=str,
        help="Comma-separated sources to filter (records must include ALL these sources). E.g., 'action,betql'"
    )
    parser.add_argument(
        "--exact", type=str,
        help="Exact source combo to match (pipe-separated). E.g., 'action|betql'"
    )
    parser.add_argument(
        "--market", type=str,
        help="Market type filter: spread, total, player_prop, moneyline"
    )
    parser.add_argument(
        "--min-bets", type=int, default=1,
        help="Minimum sample size (default: 1)"
    )
    parser.add_argument(
        "--sport", type=str, default="NBA", choices=["NBA", "NCAAB", "nba", "ncaab"],
        help="Sport to view (default: NBA)"
    )
    parser.add_argument(
        "--individual", action="store_true",
        help="Show individual source records instead of combos"
    )
    parser.add_argument(
        "--sort", type=str, default="n", choices=["n", "win_pct", "roi", "net_units"],
        help="Sort by: n (sample size), win_pct, roi, net_units (default: n)"
    )
    parser.add_argument(
        "--asc", action="store_true",
        help="Sort ascending instead of descending"
    )

    args = parser.parse_args()
    sport = args.sport.upper()

    # Determine which report to load
    if args.market:
        if args.individual:
            report_name = "by_source_record_market.json"
        else:
            report_name = "by_sources_combo_market.json"
        show_market = True
    else:
        if args.individual:
            report_name = "by_source_record.json"
        else:
            report_name = "by_sources_combo_record.json"
        show_market = False

    report_path = get_report_path(report_name, sport)

    if not report_path.exists():
        print(f"Report not found: {report_path}")
        print("You may need to run: python3 scripts/report_records.py")
        return

    report = load_report(report_path)
    rows = report.get("rows", [])
    meta = report.get("meta", {})

    if not rows:
        print(f"No data in report: {report_path}")
        return

    # Apply filters
    if args.exact:
        rows = filter_by_exact_combo(rows, args.exact)
    elif args.sources:
        source_list = [s.strip() for s in args.sources.split(",")]
        rows = filter_by_sources_contain(rows, source_list)

    if args.market:
        rows = filter_by_market(rows, args.market)

    # Filter by minimum bets
    rows = [r for r in rows if r.get("n", 0) >= args.min_bets]

    # Sort
    sort_key = args.sort
    reverse = not args.asc

    def get_sort_val(r):
        val = r.get(sort_key)
        if val is None:
            return float('-inf') if reverse else float('inf')
        return val

    rows.sort(key=get_sort_val, reverse=reverse)

    # Build title
    title_parts = [f"{sport} Records"]
    if args.individual:
        title_parts.append("(Individual Sources)")
    else:
        title_parts.append("(Source Combinations)")
    if args.exact:
        title_parts.append(f"| Exact: {args.exact}")
    elif args.sources:
        title_parts.append(f"| Contains: {args.sources}")
    if args.market:
        title_parts.append(f"| Market: {args.market}")
    if args.min_bets > 1:
        title_parts.append(f"| Min {args.min_bets} bets")

    title = " ".join(title_parts)

    # Display
    print_record_table(rows, title=title, show_market=show_market)
    print_summary(rows)

    # Show report metadata
    if meta:
        graded = meta.get("graded_rows") or meta.get("graded_unique_signals", 0)
        total = meta.get("total_rows", 0)
        gen_at = meta.get("generated_at_utc", "unknown")[:19]
        print(f"  Report: {report_path.name}")
        print(f"  Generated: {gen_at} | {graded} graded of {total} total rows")
        print()


if __name__ == "__main__":
    main()
