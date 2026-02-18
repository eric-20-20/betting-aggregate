#!/usr/bin/env python3
"""Merge backfill data from Action and BetQL into unified files for consensus.

This script combines:
- Action backfill: out/normalized_action_nba_backfill.jsonl
- BetQL backfill: data/history/betql_normalized/{props,spreads,totals}/*.jsonl

Output:
- out/merged_action_nba.json (Action backfill in JSON format)
- out/merged_betql_props_nba.json (BetQL props backfill)
- out/merged_betql_spreads_nba.json (BetQL spreads backfill)
- out/merged_betql_totals_nba.json (BetQL totals backfill)

These can then be added to the consensus input file list.

Usage:
    python3 scripts/merge_backfill_data.py
    python3 scripts/merge_backfill_data.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    """Load JSONL file."""
    rows = []
    if not path.exists():
        return rows
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return rows


def load_json(path: Path) -> List[Dict[str, Any]]:
    """Load JSON array file."""
    if not path.exists():
        return []
    with path.open() as f:
        data = json.load(f)
    return data if isinstance(data, list) else []


def save_json(path: Path, data: List[Dict[str, Any]]) -> None:
    """Save to JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def normalize_event_key(event_key: str) -> str:
    """
    Normalize event key to standard format: NBA:YYYY:MM:DD:AWAY@HOME

    Handles:
    - NBA:20260206:MIA@BOS:2130 -> NBA:2026:02:06:MIA@BOS
    - NBA:2026:01:15:TIMBERWOLVES@PISTONS -> NBA:2026:01:15:MIN@DET
    """
    if not event_key:
        return event_key

    # Pattern 1: NBA:20260206:MIA@BOS:2130
    match = re.match(r'NBA:(\d{4})(\d{2})(\d{2}):([A-Z]+)@([A-Z]+)(?::\d+)?', event_key)
    if match:
        year, month, day, away, home = match.groups()
        return f"NBA:{year}:{month}:{day}:{away}@{home}"

    # Pattern 2: Already in good format NBA:2026:01:15:AWAY@HOME
    # Just normalize team names
    match = re.match(r'NBA:(\d{4}):(\d{2}):(\d{2}):([A-Z]+)@([A-Z]+)', event_key)
    if match:
        return event_key

    return event_key


def normalize_team_code(team: str) -> str:
    """Normalize team name to 3-letter code."""
    team = (team or "").upper().strip()

    # Full name to code mapping
    team_map = {
        "TIMBERWOLVES": "MIN",
        "PISTONS": "DET",
        "CELTICS": "BOS",
        "HEAT": "MIA",
        "LAKERS": "LAL",
        "CLIPPERS": "LAC",
        "WARRIORS": "GSW",
        "SUNS": "PHX",
        "NUGGETS": "DEN",
        "JAZZ": "UTA",
        "TRAIL BLAZERS": "POR",
        "BLAZERS": "POR",
        "THUNDER": "OKC",
        "MAVERICKS": "DAL",
        "ROCKETS": "HOU",
        "SPURS": "SAS",
        "GRIZZLIES": "MEM",
        "PELICANS": "NOP",
        "KINGS": "SAC",
        "HAWKS": "ATL",
        "HORNETS": "CHA",
        "BULLS": "CHI",
        "CAVALIERS": "CLE",
        "PACERS": "IND",
        "BUCKS": "MIL",
        "NETS": "BKN",
        "KNICKS": "NYK",
        "76ERS": "PHI",
        "SIXERS": "PHI",
        "RAPTORS": "TOR",
        "WIZARDS": "WAS",
        "MAGIC": "ORL",
        # Already codes
        "MIN": "MIN",
        "DET": "DET",
        "BOS": "BOS",
        "MIA": "MIA",
        "LAL": "LAL",
        "LAC": "LAC",
        "GSW": "GSW",
        "GS": "GSW",
        "PHX": "PHX",
        "PHO": "PHX",
        "DEN": "DEN",
        "UTA": "UTA",
        "POR": "POR",
        "OKC": "OKC",
        "DAL": "DAL",
        "HOU": "HOU",
        "SAS": "SAS",
        "SA": "SAS",
        "MEM": "MEM",
        "NOP": "NOP",
        "NO": "NOP",
        "SAC": "SAC",
        "ATL": "ATL",
        "CHA": "CHA",
        "CHO": "CHA",
        "CHI": "CHI",
        "CLE": "CLE",
        "IND": "IND",
        "MIL": "MIL",
        "BKN": "BKN",
        "BRK": "BKN",
        "NYK": "NYK",
        "NY": "NYK",
        "PHI": "PHI",
        "TOR": "TOR",
        "WAS": "WAS",
        "ORL": "ORL",
    }

    return team_map.get(team, team[:3] if len(team) >= 3 else team)


def normalize_day_key(day_key: str) -> str:
    """Normalize day key to NBA:YYYY:MM:DD format."""
    if not day_key:
        return day_key

    # Already good format
    if re.match(r'NBA:\d{4}:\d{2}:\d{2}$', day_key):
        return day_key

    # NBA:20260206 format
    match = re.match(r'NBA:(\d{4})(\d{2})(\d{2})', day_key)
    if match:
        year, month, day = match.groups()
        return f"NBA:{year}:{month}:{day}"

    return day_key


def extract_day_key_from_event_key(event_key: str) -> Optional[str]:
    """Extract day_key from event_key like NBA:20260206:MIA@BOS:2130."""
    if not event_key:
        return None

    # Pattern: NBA:20260206:MIA@BOS:2130
    match = re.match(r'NBA:(\d{4})(\d{2})(\d{2}):', event_key)
    if match:
        year, month, day = match.groups()
        return f"NBA:{year}:{month}:{day}"

    # Pattern: NBA:2026:01:15:...
    match = re.match(r'(NBA:\d{4}:\d{2}:\d{2}):', event_key)
    if match:
        return match.group(1)

    return None


def normalize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a record for consensus matching."""
    record = dict(record)  # Don't mutate original

    event = record.get("event", {})
    if isinstance(event, dict):
        event = dict(event)  # Don't mutate original

        # Normalize teams
        if "away_team" in event:
            event["away_team"] = normalize_team_code(event["away_team"])
        if "home_team" in event:
            event["home_team"] = normalize_team_code(event["home_team"])

        # Extract day_key from event_key if not present
        if not event.get("day_key") and event.get("event_key"):
            extracted = extract_day_key_from_event_key(event["event_key"])
            if extracted:
                event["day_key"] = extracted

        # Normalize day_key
        if "day_key" in event:
            event["day_key"] = normalize_day_key(event["day_key"])

        # Normalize event_key - rebuild with normalized components
        day = event.get("day_key", "")
        away = event.get("away_team", "")
        home = event.get("home_team", "")
        if day and away and home:
            event["event_key"] = f"{day}:{away}@{home}"
        elif "event_key" in event:
            event["event_key"] = normalize_event_key(event["event_key"])

        # Normalize matchup_key
        if day and away and home:
            event["matchup_key"] = f"{day}:{home}-{away}"

        record["event"] = event

    return record


def load_action_backfill() -> List[Dict[str, Any]]:
    """Load Action backfill data."""
    paths = [
        Path("out/normalized_action_nba_backfill.jsonl"),
        Path("out_action_hist/normalized_action_nba_backfill.jsonl"),
    ]

    all_records = []
    seen_fingerprints = set()

    for path in paths:
        if path.exists():
            records = load_jsonl(path)
            for r in records:
                fp = r.get("provenance", {}).get("raw_fingerprint")
                if fp and fp in seen_fingerprints:
                    continue
                if fp:
                    seen_fingerprints.add(fp)
                all_records.append(normalize_record(r))

    return all_records


def load_betql_backfill(category: str) -> List[Dict[str, Any]]:
    """Load BetQL backfill data for a category (props, spreads, totals)."""
    base_dir = Path(f"data/history/betql_normalized/{category}")

    if not base_dir.exists():
        return []

    all_records = []
    seen_fingerprints = set()

    for jsonl_file in sorted(base_dir.glob("*.jsonl")):
        records = load_jsonl(jsonl_file)
        for r in records:
            fp = r.get("provenance", {}).get("raw_fingerprint")
            if fp and fp in seen_fingerprints:
                continue
            if fp:
                seen_fingerprints.add(fp)
            all_records.append(normalize_record(r))

    return all_records


def load_graded_backfill() -> List[Dict[str, Any]]:
    """Load graded BetQL backfill from converted schema file.

    This file contains pre-graded player prop picks with results.
    Source: out/normalized_betql_backfill.jsonl (converted from betql_backfill_grades.jsonl)
    """
    graded_file = Path("out/normalized_betql_backfill.jsonl")

    if not graded_file.exists():
        return []

    all_records = []
    seen_fingerprints = set()

    records = load_jsonl(graded_file)
    for r in records:
        fp = r.get("provenance", {}).get("raw_fingerprint")
        if fp and fp in seen_fingerprints:
            continue
        if fp:
            seen_fingerprints.add(fp)
        all_records.append(normalize_record(r))

    return all_records


def merge_with_graded_priority(
    history_records: List[Dict[str, Any]],
    graded_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Merge history and graded records, preferring graded when duplicates exist.

    Duplicates are identified by: day_key + selection + line
    """
    # Build lookup for graded records
    graded_by_bet = {}
    for r in graded_records:
        event = r.get("event", {})
        market = r.get("market", {})
        day_key = event.get("day_key", "")
        selection = market.get("selection", "")
        line = market.get("line")
        bet_key = f"{day_key}|{selection}|{line}"

        # Only keep if it has a result
        result = r.get("result")
        if result and result.get("status"):
            graded_by_bet[bet_key] = r

    # Process history, replacing with graded when available
    merged = []
    replaced = 0

    for r in history_records:
        event = r.get("event", {})
        market = r.get("market", {})
        day_key = event.get("day_key", "")
        selection = market.get("selection", "")
        line = market.get("line")
        bet_key = f"{day_key}|{selection}|{line}"

        if bet_key in graded_by_bet:
            # Use graded version
            merged.append(graded_by_bet.pop(bet_key))
            replaced += 1
        else:
            merged.append(r)

    # Add remaining graded records (not in history)
    new_from_graded = len(graded_by_bet)
    merged.extend(graded_by_bet.values())

    print(f"    Merged: {replaced} replaced with graded, {new_from_graded} new from graded")

    return merged


def build_player_alias_map(
    action_records: List[Dict[str, Any]],
    betql_props: List[Dict[str, Any]],
) -> Dict[str, str]:
    """
    Build a comprehensive player alias map by matching abbreviated Action slugs
    to full BetQL slugs based on initials + last name matching.

    Action format: a_caruso (first initial + last name)
    BetQL format: alex_caruso (full first + last name)
    """
    # Extract unique player slugs from each source
    action_slugs = set()
    for r in action_records:
        mk = r.get("market", {})
        if mk.get("market_type") == "player_prop":
            pk = mk.get("player_key", "")
            if pk:
                slug = pk.split(":", 1)[1] if pk.lower().startswith("nba:") else pk
                # Clean up suffixes like "_to_score_yes"
                slug = re.sub(r"_to_(score|record|make|have)_yes$", "", slug)
                action_slugs.add(slug.lower())

    betql_slugs = set()
    for r in betql_props:
        mk = r.get("market", {})
        pk = mk.get("player_key", "")
        if pk:
            slug = pk.split(":", 1)[1] if pk.lower().startswith("nba:") else pk
            betql_slugs.add(slug.lower())

    # Build alias map: abbreviated -> full
    alias_map: Dict[str, str] = {}

    for short in action_slugs:
        if short in betql_slugs:
            # Already matches, no alias needed
            continue

        parts = short.split("_")
        if len(parts) < 2:
            continue

        # Check if first part is a single letter (initial)
        first_part = parts[0]
        last_part = parts[-1]

        if len(first_part) == 1:
            # Pattern: a_caruso -> match alex_caruso
            candidates = []
            for full in betql_slugs:
                full_parts = full.split("_")
                if len(full_parts) < 2:
                    continue
                # Last name must match
                if full_parts[-1] != last_part:
                    continue
                # First name must start with the initial
                if full_parts[0].startswith(first_part):
                    candidates.append(full)

            if len(candidates) == 1:
                alias_map[short] = candidates[0]
            elif len(candidates) > 1:
                # Ambiguous - try middle name matching if present
                if len(parts) == 3:
                    # e.g., k_a_towns -> karl_anthony_towns
                    middle_initial = parts[1]
                    refined = [c for c in candidates
                               if len(c.split("_")) >= 3 and c.split("_")[1].startswith(middle_initial)]
                    if len(refined) == 1:
                        alias_map[short] = refined[0]

    return alias_map


def main():
    parser = argparse.ArgumentParser(description="Merge backfill data for consensus")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without writing files")
    parser.add_argument("--out-dir", type=str, default="out", help="Output directory (default: out)")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)

    print("Loading backfill data...")

    # Load Action backfill
    action_records = load_action_backfill()
    print(f"  Action: {len(action_records)} records")

    # Load BetQL backfill by category
    betql_props = load_betql_backfill("props")
    print(f"  BetQL props (history): {len(betql_props)} records")

    # Load graded backfill and merge
    graded_props = load_graded_backfill()
    print(f"  BetQL props (graded backfill): {len(graded_props)} records")

    if graded_props:
        betql_props = merge_with_graded_priority(betql_props, graded_props)
        print(f"  BetQL props (after merge): {len(betql_props)} records")

    betql_spreads = load_betql_backfill("spreads")
    print(f"  BetQL spreads: {len(betql_spreads)} records")

    betql_totals = load_betql_backfill("totals")
    print(f"  BetQL totals: {len(betql_totals)} records")

    # Count overlapping dates
    action_dates = set()
    for r in action_records:
        day = r.get("event", {}).get("day_key", "")
        if day:
            action_dates.add(day)

    betql_dates = set()
    for records in [betql_props, betql_spreads, betql_totals]:
        for r in records:
            day = r.get("event", {}).get("day_key", "")
            if day:
                betql_dates.add(day)

    overlap = action_dates & betql_dates
    print(f"\nDate overlap: {len(overlap)} days with both Action + BetQL data")

    # Build player alias map
    print("\nBuilding player alias map...")
    alias_map = build_player_alias_map(action_records, betql_props)
    print(f"  Generated {len(alias_map)} player aliases")

    if args.dry_run:
        print("\n[DRY RUN] Would write:")
        print(f"  {out_dir}/merged_action_nba.json ({len(action_records)} records)")
        print(f"  {out_dir}/merged_betql_props_nba.json ({len(betql_props)} records)")
        print(f"  {out_dir}/merged_betql_spreads_nba.json ({len(betql_spreads)} records)")
        print(f"  {out_dir}/merged_betql_totals_nba.json ({len(betql_totals)} records)")
        print(f"  {out_dir}/player_alias_map_nba.json ({len(alias_map)} aliases)")
        # Show sample aliases
        print("\nSample aliases:")
        for k, v in list(alias_map.items())[:10]:
            print(f"    {k} -> {v}")
        return

    # Write output files
    print(f"\nWriting merged files to {out_dir}/...")

    save_json(out_dir / "merged_action_nba.json", action_records)
    print(f"  Wrote merged_action_nba.json ({len(action_records)} records)")

    save_json(out_dir / "merged_betql_props_nba.json", betql_props)
    print(f"  Wrote merged_betql_props_nba.json ({len(betql_props)} records)")

    save_json(out_dir / "merged_betql_spreads_nba.json", betql_spreads)
    print(f"  Wrote merged_betql_spreads_nba.json ({len(betql_spreads)} records)")

    save_json(out_dir / "merged_betql_totals_nba.json", betql_totals)
    print(f"  Wrote merged_betql_totals_nba.json ({len(betql_totals)} records)")

    # Write player alias map
    save_json(out_dir / "player_alias_map_nba.json", alias_map)
    print(f"  Wrote player_alias_map_nba.json ({len(alias_map)} aliases)")

    print("\nDone! Now update consensus_nba.py INPUT_FILES_BY_SPORT to include these files.")
    print("Or copy them to data/latest/ with the expected filenames.")


if __name__ == "__main__":
    main()
