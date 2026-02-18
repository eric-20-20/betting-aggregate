#!/usr/bin/env python3
"""
Convert BetQL backfill grades from flat schema to normalized schema.

Input:  out/betql_backfill_grades.jsonl (flat schema)
Output: out/normalized_betql_backfill.jsonl (normalized schema matching consensus pipeline)
"""

import json
import hashlib
from pathlib import Path
from typing import Dict, Any, Optional
from collections import Counter
from datetime import datetime

INPUT_FILE = Path("out/betql_backfill_grades.jsonl")
OUTPUT_FILE = Path("out/normalized_betql_backfill.jsonl")


def generate_fingerprint(r: Dict[str, Any]) -> str:
    """Generate a unique fingerprint for deduplication."""
    key_parts = [
        r.get("event_key", ""),
        r.get("player_slug", ""),
        r.get("stat_key", ""),
        r.get("direction", ""),
        str(r.get("line", "")),
    ]
    key_str = "|".join(key_parts)
    return hashlib.sha256(key_str.encode()).hexdigest()


def convert_record(r: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Convert a flat BetQL backfill record to normalized schema."""

    # Extract fields
    event_key = r.get("event_key", "")
    home_team = r.get("home_team_norm", "")
    away_team = r.get("away_team_norm", "")
    player_name = r.get("player_name", "")
    player_slug = r.get("player_slug", "")
    stat_key = r.get("stat_key", "")
    direction = r.get("direction", "")
    line = r.get("line")
    odds = r.get("odds")
    status = r.get("status")  # WIN, LOSS, ERROR
    stat_value = r.get("stat_value")
    game_date = r.get("game_date", "")

    # FIX: day_key in backfill is observation date (1 day before game)
    # We need day_key to match the GAME date for consensus pipeline
    # Derive from game_date instead of using stored day_key
    if game_date:
        # game_date format: 2025-02-10 -> NBA:2025:02:10
        parts = game_date.split("-")
        if len(parts) == 3:
            day_key = f"NBA:{parts[0]}:{parts[1]}:{parts[2]}"
        else:
            day_key = r.get("day_key", "")
    else:
        day_key = r.get("day_key", "")

    # Skip records without essential fields
    if not player_slug or not stat_key or not direction:
        return None

    # Build player_key
    player_key = f"NBA:{player_slug}"

    # Build selection in canonical format
    selection = f"NBA:{player_slug}::{stat_key}::{direction}"

    # Map status to result
    result_map = {"WIN": "WIN", "LOSS": "LOSS", "PUSH": "PUSH"}
    result_status = result_map.get(status)

    # Build matchup_key from event_key
    # event_key format: NBA:2025:02:10:MIN@CLE
    matchup_key = None
    if event_key and home_team and away_team:
        parts = event_key.split(":")
        if len(parts) >= 4:
            matchup_key = f"{parts[0]}:{parts[1]}:{parts[2]}:{parts[3]}:{home_team}-{away_team}"

    # Generate fingerprint for deduplication
    fingerprint = generate_fingerprint(r)

    # Build normalized record
    normalized = {
        "provenance": {
            "source_id": "betql",
            "source_surface": "betql_backfill_props",
            "sport": "NBA",
            "observed_at_utc": None,
            "canonical_url": r.get("canonical_url"),
            "raw_fingerprint": fingerprint,
            "rating_stars": r.get("rating_stars"),
            "backfill_game_date": game_date,
        },
        "event": {
            "sport": "NBA",
            "event_key": event_key,
            "day_key": day_key,
            "matchup_key": matchup_key,
            "away_team": away_team,
            "home_team": home_team,
            "event_start_time_utc": None,
        },
        "market": {
            "market_type": "player_prop",
            "market_family": "prop",
            "selection": selection,
            "side": direction,
            "line": line,
            "odds": odds,
            "player_key": player_key,
            "player_name": player_name,
            "stat_key": stat_key,
        },
        "result": {
            "status": result_status,
            "stat_value": stat_value,
            "graded_by": "betql_backfill",
            "provider": r.get("provider"),
            "player_match": r.get("player_match"),
        } if result_status else None,
        "eligible_for_consensus": True,
        "ineligibility_reason": None,
    }

    return normalized


def main():
    print(f"[convert] Reading from {INPUT_FILE}")

    if not INPUT_FILE.exists():
        print(f"[convert] ERROR: Input file not found: {INPUT_FILE}")
        return

    # Read input
    with open(INPUT_FILE, "r") as f:
        input_rows = [json.loads(line) for line in f if line.strip()]

    print(f"[convert] Loaded {len(input_rows)} input records")

    # Convert
    converted = []
    skipped = 0
    status_counts = Counter()
    stat_counts = Counter()
    day_key_issues = []

    for r in input_rows:
        status_counts[r.get("status", "UNKNOWN")] += 1
        stat_counts[r.get("stat_key", "unknown")] += 1

        # Track day_key vs game_date discrepancies
        day_key = r.get("day_key", "")
        game_date = r.get("game_date", "")
        event_key = r.get("event_key", "")

        if day_key and game_date:
            # day_key format: NBA:2025:02:09
            # game_date format: 2025-02-10
            day_key_date = day_key.replace("NBA:", "").replace(":", "-")
            if day_key_date != game_date:
                day_key_issues.append({
                    "day_key": day_key,
                    "game_date": game_date,
                    "event_key": event_key,
                })

        result = convert_record(r)
        if result:
            converted.append(result)
        else:
            skipped += 1

    print(f"[convert] Converted: {len(converted)}, Skipped: {skipped}")
    print(f"[convert] Status distribution: {dict(status_counts)}")
    print(f"[convert] Stat types: {dict(stat_counts.most_common(10))}")

    # Report day_key issues
    if day_key_issues:
        print(f"\n[convert] DAY_KEY DISCREPANCIES: {len(day_key_issues)} records")
        print("[convert] Sample discrepancies:")
        for issue in day_key_issues[:5]:
            print(f"  day_key={issue['day_key']} game_date={issue['game_date']} event_key={issue['event_key']}")

    # Write output
    with open(OUTPUT_FILE, "w") as f:
        for record in converted:
            f.write(json.dumps(record) + "\n")

    print(f"\n[convert] Wrote {len(converted)} records to {OUTPUT_FILE}")

    # Summary stats for graded records
    graded = [r for r in converted if r.get("result") and r["result"].get("status") in ["WIN", "LOSS", "PUSH"]]
    wins = sum(1 for r in graded if r["result"]["status"] == "WIN")
    losses = sum(1 for r in graded if r["result"]["status"] == "LOSS")
    pushes = sum(1 for r in graded if r["result"]["status"] == "PUSH")

    print(f"\n[convert] GRADED SUMMARY:")
    print(f"  Total graded: {len(graded)}")
    print(f"  WIN: {wins} ({100*wins/len(graded):.1f}%)" if graded else "  WIN: 0")
    print(f"  LOSS: {losses} ({100*losses/len(graded):.1f}%)" if graded else "  LOSS: 0")
    print(f"  PUSH: {pushes}")

    # Date range
    dates = set()
    for r in converted:
        dk = r.get("event", {}).get("day_key", "")
        if dk:
            dates.add(dk)

    if dates:
        sorted_dates = sorted(dates)
        print(f"\n[convert] Date range: {sorted_dates[0]} to {sorted_dates[-1]} ({len(dates)} days)")


if __name__ == "__main__":
    main()
