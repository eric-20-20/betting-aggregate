#!/usr/bin/env python3
"""
Export pipeline data for the web application.

Reads the daily plays file and report JSONs, splits picks into
public (teaser) and private (full) datasets, and copies everything
to the web/ directory structure.

Usage:
    python3 scripts/export_web_data.py
    python3 scripts/export_web_data.py --date 2026-02-24
"""

import argparse
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path

PLAYS_DIR = Path("data/plays")
REPORTS_DIR = Path("data/reports")
WEB_PUBLIC_DIR = Path("web/public/data")
WEB_PRIVATE_DIR = Path("web/data/private")

NUM_TEASER_PICKS = 3


def find_latest_plays_date() -> str:
    """Find the most recent plays file date."""
    plays_files = sorted(PLAYS_DIR.glob("plays_*.json"), reverse=True)
    if not plays_files:
        print("[export] No plays files found in data/plays/")
        sys.exit(1)
    # plays_2026-02-24.json -> 2026-02-24
    return plays_files[0].stem.replace("plays_", "")


def export_picks(date: str) -> None:
    """Split plays into public teaser + locked summaries and private full data."""
    plays_path = PLAYS_DIR / f"plays_{date}.json"
    if not plays_path.exists():
        print(f"[export] Missing {plays_path}")
        sys.exit(1)

    with open(plays_path) as f:
        data = json.load(f)

    meta = data["meta"]
    plays = data["plays"]

    # Sort by tier (A first) then composite_score descending
    tier_order = {"A": 0, "B": 1, "C": 2, "D": 3}
    plays.sort(key=lambda p: (tier_order.get(p["tier"], 9), -p["composite_score"]))

    # Teaser picks: top N A-tier picks with full details
    teaser_picks = []
    for p in plays:
        if p["tier"] == "A" and len(teaser_picks) < NUM_TEASER_PICKS:
            teaser_picks.append(p)

    teaser_ids = {p["signal"]["signal_id"] for p in teaser_picks}

    # Locked picks: everything else, stripped of details
    locked_picks = []
    for p in plays:
        if p["signal"]["signal_id"] in teaser_ids:
            continue
        sig = p["signal"]
        matchup = f"{sig['away_team']}@{sig['home_team']}"
        locked_picks.append({
            "rank": p["rank"],
            "tier": p["tier"],
            "market_type": sig["market_type"],
            "matchup": matchup,
            "positive_dimensions": p["positive_dimensions"],
        })

    public_data = {
        "meta": meta,
        "teaser_picks": teaser_picks,
        "locked_picks": locked_picks,
    }

    # Write public picks
    public_dir = WEB_PUBLIC_DIR / "picks"
    public_dir.mkdir(parents=True, exist_ok=True)
    public_path = public_dir / f"public_{date}.json"
    with open(public_path, "w") as f:
        json.dump(public_data, f, indent=2)
    print(f"[export] Public picks: {public_path} ({len(teaser_picks)} teaser, {len(locked_picks)} locked)")

    # Write private full picks
    WEB_PRIVATE_DIR.mkdir(parents=True, exist_ok=True)
    private_path = WEB_PRIVATE_DIR / f"picks_{date}.json"
    shutil.copy2(plays_path, private_path)
    print(f"[export] Private picks: {private_path} ({len(plays)} total)")


def export_reports() -> None:
    """Copy report JSONs to web public directory."""
    reports_dir = WEB_PUBLIC_DIR / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    report_files = [
        "by_source_record.json",
        "by_sources_combo_record.json",
        "by_expert_record.json",
        "by_source_record_market.json",
        "by_stat_type.json",
        "coverage_summary.json",
    ]

    trend_files = [
        "trends/top_trends_summary.json",
        "trends/consensus_strength.json",
        "trends/market_type.json",
    ]

    copied = 0
    for rf in report_files:
        src = REPORTS_DIR / rf
        if src.exists():
            dst = reports_dir / Path(rf).name
            shutil.copy2(src, dst)
            copied += 1

    for tf in trend_files:
        src = REPORTS_DIR / tf
        if src.exists():
            dst = reports_dir / Path(tf).name
            shutil.copy2(src, dst)
            copied += 1

    print(f"[export] Reports: copied {copied} files to {reports_dir}")


def export_tier_performance() -> None:
    """Generate tier performance summary from grades data."""
    grades_path = Path("data/ledger/grades_latest.jsonl")
    if not grades_path.exists():
        print("[export] No grades file found, skipping tier performance")
        return

    # We need the plays files to map signal_ids to tiers
    # Instead, compute from the cross_tabulation which has combo performance
    # For now, generate a placeholder that can be filled in later
    reports_dir = WEB_PUBLIC_DIR / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    # Check if score_bucket data exists (tiers map to score ranges)
    score_bucket_path = REPORTS_DIR / "by_score_bucket.json"
    if score_bucket_path.exists():
        with open(score_bucket_path) as f:
            buckets = json.load(f)
        # Transform score buckets into tier-like display
        tier_data = []
        for row in buckets:
            if isinstance(row, dict) and "n" in row:
                tier_data.append(row)
        dst = reports_dir / "tier_performance.json"
        with open(dst, "w") as f:
            json.dump(tier_data, f, indent=2)
        print(f"[export] Tier performance: {dst}")
    else:
        print("[export] No score bucket data found, skipping tier performance")


def main():
    ap = argparse.ArgumentParser(description="Export pipeline data for web")
    ap.add_argument("--date", type=str, default=None,
                    help="Date to export (default: latest)")
    args = ap.parse_args()

    date = args.date or find_latest_plays_date()
    print(f"[export] Exporting data for {date}")

    export_picks(date)
    export_reports()
    export_tier_performance()

    print(f"[export] Done. Web data ready in web/public/data/ and web/data/private/")


if __name__ == "__main__":
    main()
