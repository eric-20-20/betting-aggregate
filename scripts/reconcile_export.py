#!/usr/bin/env python3
"""Verify the approved slate matches the exported web data.

Compares pick IDs and tiers between the approved slate and the private
web export file. Reports any discrepancies.

Usage:
    python3 scripts/reconcile_export.py
    python3 scripts/reconcile_export.py --date 2026-04-20
    python3 scripts/reconcile_export.py --date 2026-03-01 --to 2026-03-31
    python3 scripts/reconcile_export.py --sport NCAAB
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from review_paths import (
    approved_path,
    find_dates_with_approved,
    find_latest_slate_date,
    parse_date_range,
    private_export_path,
    validate_date,
)


def reconcile(date: str, sport: str = "NBA", quiet: bool = False) -> bool:
    """Compare approved slate with exported web data.

    Returns True if they match, False with printed discrepancies.
    """
    ap = approved_path(sport, date)
    ep = private_export_path(sport, date)

    if not ap.exists():
        if not quiet:
            print(f"[reconcile] No approved slate: {ap}")
            print(f"[reconcile] Skipping reconciliation (no editorial review for this date)")
        return True  # Not an error — just no review workflow for this date

    if not ep.exists():
        if not quiet:
            print(f"[reconcile] MISMATCH: approved slate exists but no export: {ep}")
        return False

    with open(ap) as f:
        approved = json.load(f)

    with open(ep) as f:
        exported = json.load(f)

    # Build approved included set
    approved_picks: Dict[str, str] = {}  # pick_id -> published_tier
    for c in approved["candidates"]:
        if c.get("published_included", False):
            approved_picks[c["pick_id"]] = c["published_tier"]

    # Build exported set
    exported_picks: Dict[str, str] = {}  # signal_id -> tier
    for p in exported.get("plays", []):
        sid = p["signal"]["signal_id"]
        exported_picks[sid] = p["tier"]

    ok = True
    issues = []

    # Check counts
    if len(approved_picks) != len(exported_picks):
        issues.append(
            f"Count mismatch: approved={len(approved_picks)}, exported={len(exported_picks)}"
        )
        ok = False

    # Check for picks in approved but not exported
    approved_ids = set(approved_picks.keys())
    exported_ids = set(exported_picks.keys())

    missing_from_export = approved_ids - exported_ids
    if missing_from_export:
        issues.append(f"{len(missing_from_export)} pick(s) in approved but not in export")
        for pid in list(missing_from_export)[:5]:
            issues.append(f"  missing: {pid[:16]}... (tier={approved_picks[pid]})")
        ok = False

    extra_in_export = exported_ids - approved_ids
    if extra_in_export:
        issues.append(f"{len(extra_in_export)} pick(s) in export but not in approved")
        for pid in list(extra_in_export)[:5]:
            issues.append(f"  extra: {pid[:16]}... (tier={exported_picks[pid]})")
        ok = False

    # Check tier matches for common picks
    tier_mismatches = []
    for pid in approved_ids & exported_ids:
        if approved_picks[pid] != exported_picks[pid]:
            tier_mismatches.append(
                f"  {pid[:16]}...: approved={approved_picks[pid]}, "
                f"exported={exported_picks[pid]}"
            )
    if tier_mismatches:
        issues.append(f"{len(tier_mismatches)} tier mismatch(es)")
        issues.extend(tier_mismatches[:5])
        ok = False

    if not quiet:
        if ok:
            print(f"[reconcile] OK: {len(approved_picks)} picks match between "
                  f"approved slate and export ({sport}, {date})")
        else:
            print(f"[reconcile] MISMATCH ({sport}, {date}):")
            for issue in issues:
                print(f"  {issue}")

    return ok


def main():
    ap = argparse.ArgumentParser(description="Reconcile approved slate vs web export")
    ap.add_argument("--date", type=str, default=None,
                    help="Date to reconcile (default: latest)")
    ap.add_argument("--to", type=str, default=None, dest="end_date",
                    help="End date for range (requires --date as start)")
    ap.add_argument("--sport", type=str, default="NBA",
                    choices=["NBA", "NCAAB"],
                    help="Sport (default: NBA)")
    args = ap.parse_args()

    if args.end_date and not args.date:
        print("[reconcile] ERROR: --to requires --date as start date")
        sys.exit(1)

    if args.end_date:
        # Date range mode
        validate_date(args.date)
        validate_date(args.end_date)
        dates = parse_date_range(args.date, args.end_date)
        print(f"[reconcile] Reconciling {len(dates)} dates "
              f"({args.date} to {args.end_date}, {args.sport})")
        passed = 0
        failed = 0
        skipped = 0
        for d in dates:
            ap_path = approved_path(args.sport, d)
            if not ap_path.exists():
                skipped += 1
                continue
            if reconcile(d, args.sport, quiet=True):
                passed += 1
            else:
                failed += 1
                reconcile(d, args.sport, quiet=False)  # print details for failures
        print(f"[reconcile] Done: {passed} passed, {failed} failed, {skipped} skipped")
        sys.exit(0 if failed == 0 else 1)
    else:
        # Single date mode
        date = args.date
        if date:
            validate_date(date)
        else:
            date = find_latest_slate_date(args.sport)
        if not date:
            print("[reconcile] No approved slates found")
            sys.exit(0)

        ok = reconcile(date, args.sport)
        sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
