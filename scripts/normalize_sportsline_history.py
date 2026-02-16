#!/usr/bin/env python3
"""
Normalize SportsLine historical picks from JSONL to normalized JSONL format.

Usage:
    python3 scripts/normalize_sportsline_history.py
"""

from __future__ import annotations

import argparse
import json
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.normalizer_sportsline_nba import normalize_sportsline_record


def main():
    parser = argparse.ArgumentParser(description="Normalize SportsLine historical picks")
    parser.add_argument(
        "--input",
        default="out/raw_sportsline_nba_history.jsonl",
        help="Input raw JSONL file"
    )
    parser.add_argument(
        "--output",
        default="out/normalized_sportsline_nba_history.jsonl",
        help="Output normalized JSONL file"
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    args = parser.parse_args()

    input_path = os.path.join(REPO_ROOT, args.input)
    output_path = os.path.join(REPO_ROOT, args.output)

    if not os.path.exists(input_path):
        print(f"Input file not found: {input_path}")
        return 1

    # Read raw records
    raw_records = []
    with open(input_path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    raw_records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

    print(f"Read {len(raw_records)} raw records from {args.input}")

    # Normalize each record
    normalized = []
    inelig_reasons = {}

    for raw in raw_records:
        # Adapt raw history format to normalizer expected format
        adapted = {
            "source_id": raw.get("source_id", "sportsline"),
            "source_surface": raw.get("source_surface", "sportsline_nba_history"),
            "observed_at_utc": raw.get("observed_at_utc"),
            "canonical_url": raw.get("article_url"),
            "raw_fingerprint": raw.get("raw_fingerprint"),
            "raw_pick_text": raw.get("raw_pick_text"),
            "raw_block": raw.get("raw_block"),
            "matchup_hint": raw.get("matchup_hint"),
            "away_team": raw.get("away_team"),
            "home_team": raw.get("home_team"),
            "market_type": raw.get("market_type"),
            "selection": raw.get("selection"),
            "line": raw.get("line"),
            "odds": raw.get("odds"),
            # Use article_date as event date
            "event_start_time_utc": f"{raw.get('article_date')}T19:00:00Z" if raw.get("article_date") else None,
        }

        rec = normalize_sportsline_record(adapted)

        if rec.get("eligible_for_consensus"):
            normalized.append(rec)
        else:
            reason = rec.get("ineligibility_reason") or "unknown"
            inelig_reasons[reason] = inelig_reasons.get(reason, 0) + 1

    print(f"Normalized {len(normalized)} eligible records")
    if inelig_reasons:
        print(f"Ineligibility reasons: {inelig_reasons}")

    # Deduplicate
    seen = set()
    deduped = []
    for rec in normalized:
        ev = rec.get("event") or {}
        mk = rec.get("market") or {}
        key = (
            ev.get("event_key"),
            mk.get("market_type"),
            mk.get("selection"),
            mk.get("line"),
        )
        if key not in seen:
            seen.add(key)
            deduped.append(rec)

    removed = len(normalized) - len(deduped)
    if removed > 0:
        print(f"Removed {removed} duplicate records")

    # Write output
    with open(output_path, "w") as f:
        for rec in deduped:
            f.write(json.dumps(rec) + "\n")

    print(f"Wrote {len(deduped)} normalized records to {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
