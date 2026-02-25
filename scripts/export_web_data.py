#!/usr/bin/env python3
"""
Export pipeline data for the web application.

Reads the daily plays file and report JSONs, sanitizes source/expert names
and ROI data, then writes public (teaser) and private (full) datasets
to the web/ directory structure.

Usage:
    python3 scripts/export_web_data.py
    python3 scripts/export_web_data.py --date 2026-02-24
"""

import argparse
import copy
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

# Source names that must never appear in web-facing data
SOURCE_NAMES = {
    "action", "betql", "covers", "dimers", "sportsline",
    "oddstrader", "vegasinsider", "sportscapping",
}

# Fields to strip from all exported data (incomplete ROI data)
ROI_FIELDS = {"roi", "net_units", "bets_with_units", "units_missing_count",
              "avg_odds", "odds_count", "odds_sum"}


# ── Sanitization helpers ────────────────────────────────────────────

def _consensus_label(count: int) -> str:
    if count >= 3:
        return "3+_source"
    return f"{count}_source"


def _sanitize_lookup_key(dimension: str, lookup_key: str, strength_label: str) -> str:
    """Replace source combo portion in factor lookup_key with strength label."""
    if dimension == "best_expert":
        return "top analyst"
    if dimension in ("combo_x_market", "combo_x_stat", "combo_x_market_x_stat"):
        parts = lookup_key.split(" / ")
        if len(parts) > 1:
            parts[0] = strength_label
            return " / ".join(parts)
    return lookup_key


def _strip_roi(d: dict) -> dict:
    """Remove ROI-related fields from a dict."""
    return {k: v for k, v in d.items() if k not in ROI_FIELDS}


def sanitize_play(play: dict) -> dict:
    """Strip source names, expert names, and ROI from a play for web display."""
    play = copy.deepcopy(play)
    sig = play["signal"]

    # Replace named sources with anonymous count
    source_count = len(sig.get("sources_present", []))
    strength_label = _consensus_label(source_count)
    sig["sources_count"] = source_count
    sig["consensus_strength"] = strength_label
    sig.pop("sources_combo", None)
    sig.pop("sources_present", None)
    sig["experts"] = []

    # Sanitize factor lookup_keys
    for factor in play.get("factors", []):
        factor["lookup_key"] = _sanitize_lookup_key(
            factor["dimension"], factor["lookup_key"], strength_label
        )
        # Strip ROI from factors too
        for k in list(factor.keys()):
            if k in ROI_FIELDS:
                del factor[k]

    # Sanitize historical_record label
    hr = play.get("historical_record")
    if hr and "label" in hr:
        label = hr["label"]
        # Check if label contains any source name
        label_lower = label.lower()
        if any(src in label_lower for src in SOURCE_NAMES):
            # Replace source portion: "covers overall" → "1_source overall"
            # "action|sportsline / player_prop" → "2_source / player_prop"
            parts = label.split(" / ")
            parts[0] = strength_label + (" overall" if "overall" in parts[0] else "")
            hr["label"] = " / ".join(parts)

    # Strip summary if it contains source names
    summary = play.get("summary", "")
    for src in SOURCE_NAMES:
        if src in summary.lower():
            # Replace source references in summary with generic text
            play["summary"] = ""
            break

    return play


def sanitize_report_rows(rows: list) -> list:
    """Strip ROI fields from report rows."""
    return [_strip_roi(row) for row in rows]


def sanitize_trends(trends_data: dict) -> dict:
    """Remove trends that reveal source or expert names, strip ROI."""
    BLOCKED_REPORTS = {"by_expert", "by_source_surface"}

    safe_trends = []
    for t in trends_data.get("trends", []):
        report = t.get("report", "")
        desc = t.get("description", "").lower()

        # Skip expert/source-revealing trends
        if report in BLOCKED_REPORTS:
            continue
        if any(src in desc for src in SOURCE_NAMES):
            continue

        safe_trends.append(_strip_roi(t))

    trends_data["trends"] = safe_trends
    trends_data["count"] = len(safe_trends)
    return trends_data


# ── Export functions ─────────────────────────────────────────────────

def find_latest_plays_date() -> str:
    """Find the most recent plays file date."""
    plays_files = sorted(PLAYS_DIR.glob("plays_*.json"), reverse=True)
    if not plays_files:
        print("[export] No plays files found in data/plays/")
        sys.exit(1)
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

    # Sanitize ALL plays
    sanitized_plays = [sanitize_play(p) for p in plays]

    # Teaser picks: top N A-tier picks with full details
    teaser_picks = []
    for p in sanitized_plays:
        if p["tier"] == "A" and len(teaser_picks) < NUM_TEASER_PICKS:
            teaser_picks.append(p)

    teaser_ids = {p["signal"]["signal_id"] for p in teaser_picks}

    # Locked picks: everything else, stripped of details
    locked_picks = []
    for p in sanitized_plays:
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

    # Write private full picks (also sanitized — subscribers see consensus strength, not source names)
    private_data = {
        "meta": meta,
        "plays": sanitized_plays,
    }
    WEB_PRIVATE_DIR.mkdir(parents=True, exist_ok=True)
    private_path = WEB_PRIVATE_DIR / f"picks_{date}.json"
    with open(private_path, "w") as f:
        json.dump(private_data, f, indent=2)
    print(f"[export] Private picks: {private_path} ({len(sanitized_plays)} total)")


def export_reports() -> None:
    """Export sanitized report JSONs to web public directory."""
    reports_dir = WEB_PUBLIC_DIR / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    copied = 0

    # Consensus strength — already anonymous, just strip ROI
    src = REPORTS_DIR / "trends" / "consensus_strength.json"
    if src.exists():
        with open(src) as f:
            data = json.load(f)
        if "rows" in data:
            data["rows"] = sanitize_report_rows(data["rows"])
        dst = reports_dir / "consensus_strength.json"
        with open(dst, "w") as f:
            json.dump(data, f, indent=2)
        copied += 1

    # Market type — already clean, just strip ROI
    src = REPORTS_DIR / "trends" / "market_type.json"
    if src.exists():
        with open(src) as f:
            data = json.load(f)
        for key in ("by_market", "by_market_x_consensus"):
            if key in data:
                data[key] = sanitize_report_rows(data[key])
        dst = reports_dir / "market_type.json"
        with open(dst, "w") as f:
            json.dump(data, f, indent=2)
        copied += 1

    # Stat type — already clean, just strip ROI
    src = REPORTS_DIR / "by_stat_type.json"
    if src.exists():
        with open(src) as f:
            data = json.load(f)
        if "rows" in data:
            data["rows"] = sanitize_report_rows(data["rows"])
        dst = reports_dir / "by_stat_type.json"
        with open(dst, "w") as f:
            json.dump(data, f, indent=2)
        copied += 1

    # Top trends — sanitize out source/expert trends + strip ROI
    src = REPORTS_DIR / "trends" / "top_trends_summary.json"
    if src.exists():
        with open(src) as f:
            data = json.load(f)
        data = sanitize_trends(data)
        dst = reports_dir / "top_trends_summary.json"
        with open(dst, "w") as f:
            json.dump(data, f, indent=2)
        copied += 1

    print(f"[export] Reports: exported {copied} sanitized files to {reports_dir}")


def export_tier_performance() -> None:
    """Generate tier performance summary from score bucket data."""
    reports_dir = WEB_PUBLIC_DIR / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    score_bucket_path = REPORTS_DIR / "by_score_bucket.json"
    if score_bucket_path.exists():
        with open(score_bucket_path) as f:
            buckets = json.load(f)
        tier_data = []
        rows = buckets.get("rows", buckets) if isinstance(buckets, dict) else buckets
        for row in rows:
            if isinstance(row, dict) and "n" in row:
                tier_data.append(_strip_roi(row))
        dst = reports_dir / "tier_performance.json"
        with open(dst, "w") as f:
            json.dump(tier_data, f, indent=2)
        print(f"[export] Tier performance: {dst}")
    else:
        # Write empty array as placeholder
        dst = reports_dir / "tier_performance.json"
        with open(dst, "w") as f:
            json.dump([], f)
        print("[export] Tier performance: placeholder (no score bucket data)")


def cleanup_stale_reports() -> None:
    """Remove report files that contain source/expert names."""
    reports_dir = WEB_PUBLIC_DIR / "reports"
    stale_files = [
        "by_source_record.json",
        "by_sources_combo_record.json",
        "by_expert_record.json",
        "by_source_record_market.json",
        "coverage_summary.json",
    ]
    removed = 0
    for f in stale_files:
        p = reports_dir / f
        if p.exists():
            p.unlink()
            removed += 1
    if removed:
        print(f"[export] Cleaned up {removed} stale report files")


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
    cleanup_stale_reports()

    print(f"[export] Done. Web data ready in web/public/data/ and web/data/private/")


if __name__ == "__main__":
    main()
