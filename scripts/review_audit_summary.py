#!/usr/bin/env python3
"""Build an audit summary across review slates for a date range.

Outputs a summary table showing the review status of each date: whether
a slate exists, whether it's been approved, override counts, tier breakdowns,
and reconciliation status.

Usage:
    python3 scripts/review_audit_summary.py
    python3 scripts/review_audit_summary.py --date 2026-03-01 --to 2026-03-31
    python3 scripts/review_audit_summary.py --sport NCAAB
    python3 scripts/review_audit_summary.py --json
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from review_paths import (
    approved_path,
    find_all_plays_dates,
    overrides_path,
    parse_date_range,
    plays_path,
    private_export_path,
    slate_json_path,
    validate_date,
)


def build_date_summary(date: str, sport: str) -> Dict[str, Any]:
    """Build a summary row for a single date."""
    pp = plays_path(sport, date)
    sp = slate_json_path(sport, date)
    op = overrides_path(sport, date)
    ap = approved_path(sport, date)
    ep = private_export_path(sport, date)

    row: Dict[str, Any] = {
        "date": date,
        "sport": sport,
        "plays_file_exists": pp.exists(),
        "slate_exists": sp.exists(),
        "overrides_exist": op.exists(),
        "approved_exists": ap.exists(),
        "export_exists": ep.exists(),
        "total_candidates": 0,
        "auto_included": 0,
        "near_misses": 0,
        "published_included": 0,
        "published_excluded": 0,
        "override_count": 0,
        "tier_a": 0,
        "tier_b": 0,
        "tier_c": 0,
        "tier_d": 0,
        "promoted_count": 0,
        "demoted_count": 0,
        "excluded_by_override": 0,
        "included_by_override": 0,
        "reconcile_status": "n/a",
        "notes": "",
    }

    if not pp.exists():
        row["notes"] = "no plays file"
        return row

    # Read slate if it exists
    if sp.exists():
        try:
            with open(sp) as f:
                slate = json.load(f)
            meta = slate.get("meta", {})
            row["total_candidates"] = meta.get("total_candidates", 0)
            row["auto_included"] = meta.get("auto_included_count", 0)
            row["near_misses"] = meta.get("near_miss_count", 0)

            # Count auto tiers
            for c in slate.get("candidates", []):
                t = c.get("auto_tier", "D")
                row[f"tier_{t.lower()}"] = row.get(f"tier_{t.lower()}", 0) + 1
        except (json.JSONDecodeError, KeyError) as e:
            row["notes"] = f"slate parse error: {e}"

    # Read approved if it exists
    if ap.exists():
        try:
            with open(ap) as f:
                approved = json.load(f)
            meta = approved.get("meta", {})
            row["published_included"] = meta.get("published_included_count", 0)
            row["published_excluded"] = meta.get("published_excluded_count", 0)
            row["override_count"] = meta.get("overrides_applied", 0)

            # Compute promotion/demotion/inclusion/exclusion counts
            for c in approved.get("candidates", []):
                auto_tier = c.get("auto_tier", "D")
                pub_tier = c.get("published_tier", auto_tier)
                auto_incl = c.get("auto_included", False)
                pub_incl = c.get("published_included", auto_incl)

                tier_order = {"A": 0, "B": 1, "C": 2, "D": 3}
                if tier_order.get(pub_tier, 9) < tier_order.get(auto_tier, 9):
                    row["promoted_count"] += 1
                elif tier_order.get(pub_tier, 9) > tier_order.get(auto_tier, 9):
                    row["demoted_count"] += 1

                if auto_incl and not pub_incl:
                    row["excluded_by_override"] += 1
                elif not auto_incl and pub_incl:
                    row["included_by_override"] += 1
        except (json.JSONDecodeError, KeyError) as e:
            row["notes"] = f"approved parse error: {e}"

    # Quick reconcile check
    if ap.exists() and ep.exists():
        try:
            with open(ap) as f:
                approved = json.load(f)
            with open(ep) as f:
                exported = json.load(f)

            approved_ids = {c["pick_id"] for c in approved["candidates"]
                           if c.get("published_included", False)}
            exported_ids = {p["signal"]["signal_id"] for p in exported.get("plays", [])}

            if approved_ids == exported_ids:
                row["reconcile_status"] = "ok"
            else:
                diff = len(approved_ids.symmetric_difference(exported_ids))
                row["reconcile_status"] = f"mismatch ({diff} diff)"
        except Exception:
            row["reconcile_status"] = "error"
    elif ap.exists() and not ep.exists():
        row["reconcile_status"] = "no export"
    elif not ap.exists():
        row["reconcile_status"] = "no approved"

    return row


def build_summary(dates: List[str], sport: str) -> List[Dict[str, Any]]:
    """Build summary rows for a list of dates."""
    return [build_date_summary(d, sport) for d in dates]


def print_cli_table(rows: List[Dict[str, Any]]) -> None:
    """Print a human-readable CLI summary table."""
    if not rows:
        print("  No dates to summarize")
        return

    # Header
    print(f"{'Date':<12} {'Slate':>5} {'Appr':>5} {'Ovrd':>4} "
          f"{'Incl':>4} {'Excl':>4} {'A':>3} {'B':>3} {'C':>3} {'D':>3} "
          f"{'Prom':>4} {'Demo':>4} {'Reconcile':<14} {'Notes'}")
    print("-" * 100)

    totals = {"incl": 0, "excl": 0, "overrides": 0, "promoted": 0, "demoted": 0}

    for r in rows:
        slate_mark = "Y" if r["slate_exists"] else "-"
        appr_mark = "Y" if r["approved_exists"] else "-"
        ovrd_count = str(r["override_count"]) if r["override_count"] else "-"
        incl = str(r["published_included"]) if r["approved_exists"] else "-"
        excl = str(r["published_excluded"]) if r["approved_exists"] else "-"
        prom = str(r["promoted_count"]) if r["promoted_count"] else "-"
        demo = str(r["demoted_count"]) if r["demoted_count"] else "-"

        print(f"{r['date']:<12} {slate_mark:>5} {appr_mark:>5} {ovrd_count:>4} "
              f"{incl:>4} {excl:>4} "
              f"{r['tier_a']:>3} {r['tier_b']:>3} {r['tier_c']:>3} {r['tier_d']:>3} "
              f"{prom:>4} {demo:>4} {r['reconcile_status']:<14} {r['notes']}")

        if r["approved_exists"]:
            totals["incl"] += r["published_included"]
            totals["excl"] += r["published_excluded"]
            totals["overrides"] += r["override_count"]
            totals["promoted"] += r["promoted_count"]
            totals["demoted"] += r["demoted_count"]

    print("-" * 100)
    dates_with_plays = sum(1 for r in rows if r["plays_file_exists"])
    dates_with_slates = sum(1 for r in rows if r["slate_exists"])
    dates_with_approved = sum(1 for r in rows if r["approved_exists"])
    dates_reconciled = sum(1 for r in rows if r["reconcile_status"] == "ok")
    dates_mismatched = sum(1 for r in rows if "mismatch" in r["reconcile_status"])

    print(f"\nSummary: {len(rows)} dates checked")
    print(f"  Plays files: {dates_with_plays}")
    print(f"  Slates:      {dates_with_slates}")
    print(f"  Approved:    {dates_with_approved}")
    print(f"  Reconciled:  {dates_reconciled} ok, {dates_mismatched} mismatch")
    if totals["overrides"]:
        print(f"  Overrides:   {totals['overrides']} total "
              f"({totals['promoted']} promoted, {totals['demoted']} demoted)")


def main():
    ap = argparse.ArgumentParser(description="Review audit summary across dates")
    ap.add_argument("--date", type=str, default=None,
                    help="Start date (default: all available)")
    ap.add_argument("--to", type=str, default=None, dest="end_date",
                    help="End date for range (requires --date)")
    ap.add_argument("--sport", type=str, default="NBA",
                    choices=["NBA", "NCAAB"],
                    help="Sport (default: NBA)")
    ap.add_argument("--json", action="store_true",
                    help="Output as JSON instead of table")
    ap.add_argument("--csv", action="store_true",
                    help="Output as CSV instead of table")
    args = ap.parse_args()

    if args.date and args.end_date:
        validate_date(args.date)
        validate_date(args.end_date)
        dates = parse_date_range(args.date, args.end_date)
    elif args.date:
        validate_date(args.date)
        dates = [args.date]
    else:
        # Default: all dates with plays files
        dates = find_all_plays_dates(args.sport)
        if not dates:
            print("[audit] No plays files found")
            sys.exit(0)

    print(f"[audit] Scanning {len(dates)} dates ({args.sport})")
    rows = build_summary(dates, args.sport)

    if args.json:
        print(json.dumps(rows, indent=2))
    elif args.csv:
        if rows:
            writer = csv.DictWriter(sys.stdout, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
    else:
        print()
        print_cli_table(rows)


if __name__ == "__main__":
    main()
