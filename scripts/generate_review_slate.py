#!/usr/bin/env python3
"""Generate a daily review slate for editorial review of picks.

Reads the plays file produced by score_signals.py and builds a review
dataset with one row per candidate pick (including near-miss D-tier
entries). The output is used for editorial review before website export.

Usage:
    python3 scripts/generate_review_slate.py
    python3 scripts/generate_review_slate.py --date 2026-04-20
    python3 scripts/generate_review_slate.py --date 2026-03-01 --to 2026-03-31
    python3 scripts/generate_review_slate.py --sport NCAAB
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from review_paths import (
    approved_path,
    ensure_review_dir,
    find_all_plays_dates,
    find_latest_plays_date,
    overrides_path,
    parse_date_range,
    plays_path,
    slate_csv_path,
    slate_json_path,
    validate_date,
)

NEAR_MISS_WILSON_FLOOR = 0.42  # D-tier entries with Wilson >= this are included


# ── Bet label helpers ──────────────────────────────────────────────────

def _player_name_from_selection(selection: str) -> Optional[str]:
    """Extract and format player name from selection key.

    Selection format: 'NBA:first_last::stat::DIR' or team-level like 'ATL'.
    """
    parts = selection.split("::")
    if len(parts) < 3:
        return None
    # prefix is 'NBA:first_last' or 'NCAAB:first_last'
    prefix = parts[0]
    name_part = prefix.split(":", 1)[1] if ":" in prefix else prefix
    if not name_part or name_part in ("", "undefined_undefined"):
        return None
    return name_part.replace("_", " ").title()


def _stat_label(atomic_stat: str) -> str:
    """Human-readable stat label."""
    labels = {
        "points": "points",
        "rebounds": "rebounds",
        "assists": "assists",
        "steals": "steals",
        "blocks": "blocks",
        "threes": "3-pointers",
        "pts_reb": "pts+reb",
        "pts_ast": "pts+ast",
        "reb_ast": "reb+ast",
        "pts_reb_ast": "pts+reb+ast",
        "turnovers": "turnovers",
    }
    return labels.get(atomic_stat, atomic_stat)


def build_bet_label(signal: Dict[str, Any], market: str) -> str:
    """Build a human-readable bet label from signal data."""
    away = signal.get("away_team") or "?"
    home = signal.get("home_team") or "?"
    line = signal.get("line")
    direction = signal.get("direction", "")

    if market == "player_prop":
        player = _player_name_from_selection(signal.get("selection", ""))
        stat = _stat_label(signal.get("atomic_stat", ""))
        line_str = f" {line}" if line is not None else ""
        return f"{player or '?'} {direction}{line_str} {stat}"

    if market == "spread":
        line_str = f" {line:+g}" if line is not None else ""
        return f"{direction}{line_str} ({away}@{home})"

    if market == "total":
        line_str = f" {line}" if line is not None else ""
        return f"{direction}{line_str} ({away}@{home})"

    if market == "moneyline":
        return f"{direction} ML ({away}@{home})"

    return f"{direction} {market} ({away}@{home})"


def build_game_label(signal: Dict[str, Any]) -> str:
    """Build game label like 'ATL @ NYK'."""
    away = signal.get("away_team") or "?"
    home = signal.get("home_team") or "?"
    return f"{away} @ {home}"


# ── Tier reason ────────────────────────────────────────────────────────

def build_tier_reason(play: Dict[str, Any], source: str = "passed") -> str:
    """Build a human-readable explanation of the tier assignment.

    Args:
        play: Full play dict or filtered entry.
        source: 'passed' for plays that passed quality gate, 'filtered' for D-tier.
    """
    tier = play.get("tier", "?")
    wilson = play.get("wilson_score", 0)
    pattern = play.get("matched_pattern")
    signal = play.get("signal", {})
    sources_present = signal.get("sources_present") or []
    sources_count = signal.get("sources_count") or len(sources_present)

    parts = []

    if source == "filtered":
        gate_reason = play.get("gate_reason", "unknown")
        parts.append(f"D(near-miss): Wilson {wilson:.3f}, gated -- {gate_reason}")
        if sources_count:
            parts.append(f"{sources_count} source{'s' if sources_count != 1 else ''}")
        return ", ".join(parts)

    # Passed plays
    parts.append(f"{tier}:")

    if pattern:
        pat_label = pattern.get("label", "unnamed")
        hist = pattern.get("hist", {})
        record = hist.get("record", "")
        win_pct = hist.get("win_pct", 0)
        if record:
            parts.append(f"pattern '{pat_label}' ({record}, {win_pct:.1%})")
        else:
            parts.append(f"pattern '{pat_label}'")
    else:
        parts.append("no pattern match")

    parts.append(f"Wilson {wilson:.3f}")
    parts.append(f"{sources_count} source{'s' if sources_count != 1 else ''}")

    # Primary record context
    primary = play.get("primary_record")
    if primary and primary.get("n", 0) > 0:
        lookup = primary.get("lookup_level", "")
        n = primary["n"]
        wp = primary.get("win_pct", 0)
        parts.append(f"record {primary.get('wins',0)}-{primary.get('losses',0)} "
                      f"({wp:.1%}, n={n}, {lookup})")

    # Confidence
    conf = play.get("confidence_score")
    if conf is not None:
        parts.append(f"confidence {conf}/100")

    # Adjustments
    adjustments = []
    if play.get("recency_adjustment", 0) != 0:
        adjustments.append(f"recency {play['recency_adjustment']:+.3f}")
    if play.get("expert_adjustment", 0) != 0:
        adjustments.append(f"expert {play['expert_adjustment']:+.3f}")
    if play.get("stat_adjustment", 0) != 0:
        adjustments.append(f"stat {play['stat_adjustment']:+.3f}")
    if play.get("line_bucket_adjustment", 0) != 0:
        adjustments.append(f"line_bucket {play['line_bucket_adjustment']:+.3f}")
    if play.get("day_adjustment", 0) != 0:
        adjustments.append(f"day {play['day_adjustment']:+.3f}")
    if adjustments:
        parts.append(f"adj: {', '.join(adjustments)}")

    return ", ".join(parts)


# ── Near-miss check ───────────────────────────────────────────────────

def is_near_miss(filtered_entry: Dict[str, Any]) -> bool:
    """Check if a filtered D-tier entry is close enough for editorial review."""
    wilson = filtered_entry.get("wilson_score", 0)
    return wilson >= NEAR_MISS_WILSON_FLOOR


# ── Review row builders ───────────────────────────────────────────────

def build_review_row(play: Dict[str, Any], date: str, sport: str,
                     source: str = "passed") -> Dict[str, Any]:
    """Convert a play dict into a review row."""
    signal = play.get("signal", {})
    market = signal.get("market_type", "")
    tier = play.get("tier", "D")
    auto_included = tier in ("A", "B", "C")

    pattern = play.get("matched_pattern")
    pattern_name = pattern.get("label") if pattern else None
    sources_present = signal.get("sources_present") or []
    support_count = signal.get("sources_count") or len(sources_present)

    row: Dict[str, Any] = {
        "pick_id": signal.get("signal_id", ""),
        "date": date,
        "sport": sport,
        "game_label": build_game_label(signal),
        "bet_label": build_bet_label(signal, market),
        "market": market,
        "selection": signal.get("selection", ""),
        "line": signal.get("line"),
        "odds": play.get("odds"),
        "auto_tier": tier,
        "tier_reason": build_tier_reason(play, source),
        "support_count": support_count,
        "pattern_name": pattern_name,
        "source_list": signal.get("sources_present", []),
        "auto_included": auto_included,
        "website_eligible": source == "passed",
        "published_tier": None,
        "published_included": None,
        "override_note": None,
        "wilson_score": play.get("wilson_score", 0),
        "confidence_score": play.get("confidence_score", 0),
        "gate_reason": play.get("gate_reason") if source == "filtered" else None,
        "primary_record": play.get("primary_record"),
        "matched_pattern": play.get("matched_pattern"),
    }

    # For near-miss filtered entries, store full play data so overrides
    # that promote them have the data needed for web export.
    # For passed plays, this is redundant with the plays file so we skip it.
    if source == "filtered":
        row["_play_data"] = play

    return row


# ── Slate generation ──────────────────────────────────────────────────

def generate_review_slate(date: str, sport: str = "NBA") -> Dict[str, Any]:
    """Generate a review slate from the plays file.

    Returns the slate dict ready to be written to JSON.
    """
    pp = plays_path(sport, date)

    if not pp.exists():
        print(f"[review] Missing plays file: {pp}")
        sys.exit(1)

    with open(pp) as f:
        data = json.load(f)

    plays = data.get("plays", [])
    filtered = data.get("filtered", [])

    candidates = []

    # Add all passed plays (A/B/C tier)
    for play in plays:
        row = build_review_row(play, date, sport, source="passed")
        candidates.append(row)

    # Add near-miss filtered entries (D-tier close to threshold)
    near_miss_count = 0
    for entry in filtered:
        if is_near_miss(entry):
            row = build_review_row(entry, date, sport, source="filtered")
            near_miss_count += 1
            candidates.append(row)

    auto_included_count = sum(1 for c in candidates if c["auto_included"])

    slate = {
        "meta": {
            "date": date,
            "sport": sport,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_plays_file": str(pp),
            "total_candidates": len(candidates),
            "auto_included_count": auto_included_count,
            "near_miss_count": near_miss_count,
        },
        "candidates": candidates,
    }

    return slate


def write_slate(slate: Dict[str, Any], sport: str) -> tuple:
    """Write slate to JSON and CSV in the dated directory. Returns (json_path, csv_path)."""
    date = slate["meta"]["date"]
    ensure_review_dir(sport, date)

    json_path = slate_json_path(sport, date)
    with open(json_path, "w") as f:
        json.dump(slate, f, indent=2, ensure_ascii=False)

    csv_path = slate_csv_path(sport, date)
    csv_fields = [
        "pick_id", "date", "sport", "game_label", "bet_label", "market",
        "selection", "line", "auto_tier", "tier_reason", "support_count",
        "pattern_name", "auto_included", "wilson_score", "confidence_score",
        "gate_reason",
    ]

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields + ["source_list"],
                                extrasaction="ignore")
        writer.writeheader()
        for c in slate["candidates"]:
            row = {k: c.get(k) for k in csv_fields}
            row["source_list"] = "|".join(c.get("source_list", []))
            writer.writerow(row)

    return json_path, csv_path


def _process_single_date(date: str, sport: str) -> bool:
    """Generate slate for one date. Returns True on success."""
    pp = plays_path(sport, date)
    if not pp.exists():
        print(f"[review] Skipping {date} — no plays file")
        return False

    slate = generate_review_slate(date, sport)
    json_path, csv_path = write_slate(slate, sport)

    meta = slate["meta"]
    print(f"[review] {date}: {meta['total_candidates']} candidates "
          f"({meta['auto_included_count']} auto-included, "
          f"{meta['near_miss_count']} near-misses)")
    return True


# ── Main ──────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Generate daily review slate")
    ap.add_argument("--date", type=str, default=None,
                    help="Date to generate slate for (default: latest)")
    ap.add_argument("--to", type=str, default=None, dest="end_date",
                    help="End date for range (requires --date as start)")
    ap.add_argument("--sport", type=str, default="NBA",
                    choices=["NBA", "NCAAB"],
                    help="Sport (default: NBA)")
    args = ap.parse_args()

    if args.end_date and not args.date:
        print("[review] ERROR: --to requires --date as start date")
        sys.exit(1)

    if args.end_date:
        # Date range mode
        validate_date(args.date)
        validate_date(args.end_date)
        dates = parse_date_range(args.date, args.end_date)
        print(f"[review] Generating slates for {len(dates)} dates "
              f"({args.date} to {args.end_date}, {args.sport})")
        success = 0
        for d in dates:
            if _process_single_date(d, args.sport):
                success += 1
        print(f"[review] Done: {success}/{len(dates)} slates generated")
    else:
        # Single date mode
        date = args.date
        if date:
            validate_date(date)
        else:
            date = find_latest_plays_date(args.sport)
        if not date:
            print("[review] No plays files found")
            sys.exit(1)

        print(f"[review] Generating slate for {date} ({args.sport})")
        slate = generate_review_slate(date, args.sport)
        json_path, csv_path = write_slate(slate, args.sport)

        meta = slate["meta"]
        print(f"[review] Slate: {meta['total_candidates']} candidates "
              f"({meta['auto_included_count']} auto-included, "
              f"{meta['near_miss_count']} near-misses)")
        print(f"[review] JSON: {json_path}")
        print(f"[review] CSV:  {csv_path}")

        # Print tier breakdown
        tiers: Dict[str, int] = {}
        for c in slate["candidates"]:
            t = c["auto_tier"]
            tiers[t] = tiers.get(t, 0) + 1
        for t in ("A", "B", "C", "D"):
            if t in tiers:
                print(f"  {t}-tier: {tiers[t]}")


if __name__ == "__main__":
    main()
