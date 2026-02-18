#!/usr/bin/env python3
"""
Convert Dimers backfill data from flat schema to normalized schema.

Reads the original backfill JSONL (with raw_fingerprint, observed_at_utc, etc.)
and the graded JSONL (with WIN/LOSS/PUSH status), then merges them into the
normalized {event, market, provenance, result} schema expected by
build_signal_ledger.py --include-normalized-jsonl.

Usage:
    python3 scripts/convert_dimers_backfill_schema.py
    python3 scripts/convert_dimers_backfill_schema.py --input out/backfill_dimers_bets_repaired.jsonl --grades out/dimers_backfill_grades.jsonl
"""

import json
import hashlib
import re
from pathlib import Path
from collections import Counter
from typing import Dict, Any, Optional, Tuple

INPUT_FILE = Path("out/backfill_dimers_bets_repaired.jsonl")
GRADES_FILE = Path("out/dimers_backfill_grades.jsonl")
OUTPUT_FILE = Path("out/normalized_dimers_backfill.jsonl")

# Team name mapping (same as normalizer_dimers_nba.py)
TEAM_MAP = {
    "hawks": "ATL", "celtics": "BOS", "nets": "BKN", "hornets": "CHA",
    "bulls": "CHI", "cavaliers": "CLE", "cavs": "CLE",
    "mavericks": "DAL", "mavs": "DAL", "nuggets": "DEN",
    "pistons": "DET", "warriors": "GSW", "rockets": "HOU", "pacers": "IND",
    "clippers": "LAC", "lakers": "LAL", "grizzlies": "MEM", "heat": "MIA",
    "bucks": "MIL", "timberwolves": "MIN", "wolves": "MIN",
    "pelicans": "NOP", "knicks": "NYK",
    "thunder": "OKC", "magic": "ORL", "76ers": "PHI", "sixers": "PHI",
    "suns": "PHX", "blazers": "POR", "trail blazers": "POR", "kings": "SAC",
    "spurs": "SAS", "raptors": "TOR", "jazz": "UTA", "wizards": "WAS",
}


def normalize_team(name: str) -> Optional[str]:
    if not name:
        return None
    low = name.lower().strip()
    if low in TEAM_MAP:
        return TEAM_MAP[low]
    if len(name) == 3 and name.upper() == name:
        return name
    return None


def parse_matchup(matchup: str) -> Tuple[Optional[str], Optional[str]]:
    """Parse 'Away vs. Home' into (away_code, home_code)."""
    if not matchup:
        return None, None
    m = re.match(r"(\w+(?:\s+\w+)?)\s+vs\.?\s+(\w+(?:\s+\w+)?)", matchup, re.IGNORECASE)
    if m:
        away = normalize_team(m.group(1))
        home = normalize_team(m.group(2))
        return away, home
    return None, None


def build_grade_key(row: Dict[str, Any]) -> str:
    """Build a key for matching original records to graded records."""
    return f"{row.get('game_date')}|{row.get('matchup')}|{row.get('market_type')}|{row.get('pick_text')}"


def convert_record(orig: Dict[str, Any], grade: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Convert a flat dimers backfill record to normalized schema."""
    game_date = orig.get("game_date", "")
    matchup = orig.get("matchup", "")
    market_type = orig.get("market_type", "")
    pick_text = orig.get("pick_text", "")

    # Parse teams
    away_team, home_team = parse_matchup(matchup)
    if not away_team or not home_team:
        return None

    # Build day_key from game_date (YYYY-MM-DD -> NBA:YYYY:MM:DD)
    parts = game_date.split("-")
    if len(parts) != 3:
        return None
    day_key = f"NBA:{parts[0]}:{parts[1]}:{parts[2]}"

    # Sorted matchup_key
    t1, t2 = sorted([away_team, home_team])
    matchup_key = f"{day_key}:{t1}-{t2}"

    # Event key (away@home)
    event_key = f"{day_key}:{away_team}@{home_team}"

    # Build selection, side, and line based on market type
    selection = None
    side = None
    line = None
    team_code = None

    if market_type == "moneyline":
        # pick_text: "Rockets win"
        m = re.match(r"(\w+)\s+win", pick_text, re.IGNORECASE)
        if m:
            team_code = normalize_team(m.group(1))
        if team_code:
            selection = team_code
            side = team_code

    elif market_type == "spread":
        # pick_text: "Nets +14.5"
        m = re.match(r"(\w+)\s+([+-]?\d+\.?\d*)", pick_text, re.IGNORECASE)
        if m:
            team_code = normalize_team(m.group(1))
            try:
                line = float(m.group(2))
            except ValueError:
                pass
        if team_code:
            selection = team_code
            side = team_code

    elif market_type == "total":
        # pick_text: "Over 240.5" or "Under 240.5"
        m = re.match(r"(over|under)\s+([\d.]+)", pick_text, re.IGNORECASE)
        if m:
            side = m.group(1).upper()
            try:
                line = float(m.group(2))
            except ValueError:
                pass
            selection = side

    if not selection:
        return None

    # Parse odds
    odds = None
    best_odds = orig.get("best_odds")
    if best_odds:
        odds_match = re.search(r"([+-]?\d+)", str(best_odds))
        if odds_match:
            try:
                odds = int(odds_match.group(1))
            except ValueError:
                pass

    # Build result block from grade
    result_block = None
    if grade and grade.get("status") in ("WIN", "LOSS", "PUSH"):
        result_block = {
            "status": grade["status"],
            "graded_by": "dimers_backfill",
            "away_score": grade.get("away_score"),
            "home_score": grade.get("home_score"),
        }

    normalized = {
        "provenance": {
            "source_id": "dimers",
            "source_surface": orig.get("source_surface", "dimers_schedule_backfill"),
            "sport": "NBA",
            "observed_at_utc": orig.get("observed_at_utc"),
            "canonical_url": orig.get("game_url"),
            "raw_fingerprint": orig.get("raw_fingerprint"),
            "raw_pick_text": pick_text,
            "expert_name": "Dimers AI Model",
            "probability_pct": orig.get("probability_pct"),
            "edge_pct": orig.get("edge_pct"),
            "best_odds": best_odds,
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
            "market_type": market_type,
            "market_family": "standard",
            "side": side,
            "selection": selection,
            "line": line,
            "odds": odds,
            "team_code": team_code,
        },
        "result": result_block,
        "eligible_for_consensus": True,
        "ineligibility_reason": None,
    }

    return normalized


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Convert Dimers backfill to normalized schema")
    parser.add_argument("--input", default=str(INPUT_FILE), help="Original backfill JSONL")
    parser.add_argument("--grades", default=str(GRADES_FILE), help="Graded backfill JSONL")
    parser.add_argument("--output", default=str(OUTPUT_FILE), help="Output normalized JSONL")
    args = parser.parse_args()

    input_path = Path(args.input)
    grades_path = Path(args.grades)
    output_path = Path(args.output)

    # Load original records
    originals = []
    with open(input_path) as f:
        for line in f:
            if line.strip():
                originals.append(json.loads(line))
    print(f"[convert] Loaded {len(originals)} original backfill records from {input_path}")

    # Load grades and index by key
    grade_index = {}
    if grades_path.exists():
        with open(grades_path) as f:
            for line in f:
                if line.strip():
                    g = json.loads(line)
                    key = build_grade_key(g)
                    grade_index[key] = g
        print(f"[convert] Loaded {len(grade_index)} graded records from {grades_path}")
    else:
        print(f"[convert] No grades file found at {grades_path}, converting without grades")

    # Convert
    converted = []
    skipped = 0
    matched_grades = 0
    status_counts = Counter()

    for orig in originals:
        key = build_grade_key(orig)
        grade = grade_index.get(key)
        if grade:
            matched_grades += 1

        result = convert_record(orig, grade)
        if result:
            converted.append(result)
            if result.get("result"):
                status_counts[result["result"]["status"]] += 1
            else:
                status_counts["UNGRADED"] += 1
        else:
            skipped += 1

    print(f"[convert] Converted: {len(converted)}, Skipped: {skipped}")
    print(f"[convert] Matched grades: {matched_grades}/{len(originals)}")
    print(f"[convert] Status distribution: {dict(status_counts)}")

    # Market type distribution
    market_counts = Counter(r["market"]["market_type"] for r in converted)
    print(f"[convert] Market types: {dict(market_counts)}")

    # Write output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        for record in converted:
            f.write(json.dumps(record) + "\n")

    print(f"\n[convert] Wrote {len(converted)} records to {output_path}")

    # Date range
    dates = set()
    for r in converted:
        dk = r.get("event", {}).get("day_key", "")
        if dk:
            dates.add(dk)
    if dates:
        sorted_dates = sorted(dates)
        print(f"[convert] Date range: {sorted_dates[0]} to {sorted_dates[-1]} ({len(dates)} days)")

    # Sample output
    if converted:
        print(f"\n[convert] Sample record:")
        print(json.dumps(converted[0], indent=2))


if __name__ == "__main__":
    main()
