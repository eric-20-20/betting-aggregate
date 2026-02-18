#!/usr/bin/env python3
"""Inject backfill grades into the consensus/grading pipeline with proper source tracking.

This script takes grades from backfill scripts and converts them into the signal/grade
format used by the main pipeline, enabling proper source attribution and edge mining.

The "better fix" - instead of grading backfill data separately, this routes it through
the same signal creation process as live data.

Usage:
    python3 scripts/inject_backfill_into_pipeline.py
    python3 scripts/inject_backfill_into_pipeline.py --dry-run
    python3 scripts/inject_backfill_into_pipeline.py --source betql
    python3 scripts/inject_backfill_into_pipeline.py --source dimers
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

# Output paths
SIGNALS_BACKFILL_PATH = Path("data/ledger/signals_backfill.jsonl")
GRADES_BACKFILL_PATH = Path("data/ledger/grades_backfill.jsonl")
SIGNALS_OCCURRENCES_BACKFILL_PATH = Path("data/ledger/signals_occurrences_backfill.jsonl")

# Input paths for merged/normalized backfill data
MERGED_BETQL_SPREADS = Path("data/latest/merged_betql_spreads_nba.json")
MERGED_BETQL_TOTALS = Path("data/latest/merged_betql_totals_nba.json")
MERGED_BETQL_PROPS = Path("data/latest/merged_betql_props_nba.json")
MERGED_ACTION = Path("data/latest/merged_action_nba.json")

# Additional sources
DIMERS_BACKFILL = Path("out/backfill_dimers_bets.jsonl")
NORMALIZED_SPORTSLINE = Path("data/latest/normalized_sportsline_nba.json")
NORMALIZED_SPORTSCAPPING = Path("data/latest/normalized_sportscapping_nba.json")
NORMALIZED_DIMERS = Path("data/latest/normalized_dimers_nba.json")

# Team name mapping for dimers
TEAM_MAP = {
    "hawks": "ATL", "celtics": "BOS", "nets": "BKN", "hornets": "CHA",
    "bulls": "CHI", "cavaliers": "CLE", "mavericks": "DAL", "nuggets": "DEN",
    "pistons": "DET", "warriors": "GSW", "rockets": "HOU", "pacers": "IND",
    "clippers": "LAC", "lakers": "LAL", "grizzlies": "MEM", "heat": "MIA",
    "bucks": "MIL", "timberwolves": "MIN", "pelicans": "NOP", "knicks": "NYK",
    "thunder": "OKC", "magic": "ORL", "76ers": "PHI", "sixers": "PHI",
    "suns": "PHX", "blazers": "POR", "trail blazers": "POR", "kings": "SAC",
    "spurs": "SAS", "raptors": "TOR", "jazz": "UTA", "wizards": "WAS",
}


def sha256_json(obj: Any) -> str:
    """Generate SHA256 hash of a JSON-serializable object."""
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def compute_signal_id(sig: Dict[str, Any]) -> Optional[str]:
    """Compute unique signal ID (matching consensus_nba.py logic)."""
    sig_type = sig.get("signal_type")
    if not sig_type:
        return None
    key = {
        "signal_type": sig_type,
        "day_key": sig.get("day_key"),
        "event_key": sig.get("event_key"),
        "market_type": sig.get("market_type"),
        "selection": sig.get("selection"),
        "player_id": sig.get("player_id"),
        "atomic_stat": sig.get("atomic_stat"),
        "direction": sig.get("direction"),
    }
    return sha256_json(key)


def normalize_direction(market: Dict[str, Any]) -> Optional[str]:
    """Normalize direction from market data."""
    side = (market.get("side") or "").upper()
    selection = (market.get("selection") or "").upper()

    if side in ("OVER", "UNDER"):
        return side
    if selection in ("OVER", "UNDER"):
        return selection
    if "OVER" in selection:
        return "OVER"
    if "UNDER" in selection:
        return "UNDER"
    return None


def load_merged_data(path: Path) -> List[Dict[str, Any]]:
    """Load merged JSON data."""
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
        return data if isinstance(data, list) else []


def load_jsonl_data(path: Path) -> List[Dict[str, Any]]:
    """Load JSONL data."""
    if not path.exists():
        return []
    records = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return records


def normalize_team(name: str) -> Optional[str]:
    """Normalize team name to 3-letter code."""
    if not name:
        return None
    name_lower = name.lower().strip()
    if name_lower in TEAM_MAP:
        return TEAM_MAP[name_lower]
    if len(name) == 3 and name.upper() == name:
        return name
    return None


def parse_dimers_matchup(matchup: str) -> Tuple[Optional[str], Optional[str]]:
    """Parse matchup like 'Nets vs. Cavaliers' into team codes."""
    if not matchup:
        return None, None
    m = re.match(r"(\w+)\s+vs\.?\s+(\w+)", matchup, re.IGNORECASE)
    if m:
        away = normalize_team(m.group(1))
        home = normalize_team(m.group(2))
        return away, home
    return None, None


def convert_dimers_record_to_signal(
    record: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Convert a dimers backfill record to signal format."""
    game_date = record.get("game_date")
    matchup = record.get("matchup", "")
    market_type = record.get("market_type")
    pick_text = record.get("pick_text", "")

    if not game_date or not market_type:
        return None

    # Parse teams from matchup
    away_team, home_team = parse_dimers_matchup(matchup)
    if not away_team or not home_team:
        return None

    # Build event_key and day_key
    # game_date format: 2025-10-22
    parts = game_date.split("-")
    if len(parts) != 3:
        return None

    day_key = f"NBA:{parts[0]}:{parts[1]}:{parts[2]}"
    event_key = f"NBA:{parts[0]}:{parts[1]}:{parts[2]}:{away_team}@{home_team}"

    # Parse pick based on market type
    selection = None
    line = None
    direction = None

    if market_type == "spread":
        # Pattern: "TeamName +/-X.X"
        m = re.match(r"(\w+)\s+([+-]?\d+\.?\d*)", pick_text, re.IGNORECASE)
        if m:
            picked_team = normalize_team(m.group(1))
            selection = picked_team
            try:
                line = float(m.group(2))
            except ValueError:
                pass
    elif market_type == "total":
        # Pattern: "Over/Under X.X"
        m = re.match(r"(over|under)\s+(\d+\.?\d*)", pick_text, re.IGNORECASE)
        if m:
            direction = m.group(1).upper()
            selection = direction
            try:
                line = float(m.group(2))
            except ValueError:
                pass
    elif market_type == "moneyline":
        # Pattern: "TeamName win"
        m = re.match(r"(\w+)\s+win", pick_text, re.IGNORECASE)
        if m:
            picked_team = normalize_team(m.group(1))
            selection = f"{picked_team}_ml" if picked_team else None

    if not selection:
        return None

    # Build signal
    signal: Dict[str, Any] = {
        "signal_type": "agree",
        "day_key": day_key,
        "event_key": event_key,
        "matchup_key": f"NBA:{parts[0]}:{parts[1]}:{parts[2]}:{home_team}-{away_team}",
        "market_type": market_type,
        "away_team": away_team,
        "home_team": home_team,
        "sources": ["dimers"],
        "sources_present": ["dimers"],
        "selection": selection,
        "line": line,
        "direction": direction,
        "supports": [{
            "source_id": "dimers",
            "line": line,
            "odds": record.get("best_odds"),
            "selection": selection,
            "probability_pct": record.get("probability_pct"),
            "edge_pct": record.get("edge_pct"),
        }],
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "count_total": 1,
        "score": 50,
    }

    signal["signal_id"] = compute_signal_id(signal)
    if not signal["signal_id"]:
        return None

    return signal


def convert_record_to_signal(
    record: Dict[str, Any],
    source_id: str,
) -> Optional[Dict[str, Any]]:
    """Convert a normalized record to signal format."""
    event = record.get("event", {})
    market = record.get("market", {})
    provenance = record.get("provenance", {})

    # Skip ineligible records
    if not record.get("eligible_for_consensus", False):
        return None

    event_key = event.get("event_key")
    day_key = event.get("day_key")
    market_type = market.get("market_type")

    if not event_key or not day_key or not market_type:
        return None

    # Build signal based on market type
    signal: Dict[str, Any] = {
        "signal_type": "agree",
        "day_key": day_key,
        "event_key": event_key,
        "matchup_key": event.get("matchup_key"),
        "market_type": market_type,
        "away_team": event.get("away_team"),
        "home_team": event.get("home_team"),
        "sources": [source_id],
        "sources_present": [source_id],
        "supports": [{
            "source_id": source_id,
            "line": market.get("line"),
            "odds": market.get("odds"),
            "selection": market.get("selection"),
            "canonical_url": provenance.get("canonical_url"),
            "expert_name": provenance.get("expert_name"),
            "observed_at_utc": provenance.get("observed_at_utc"),
        }],
        "observed_at_utc": provenance.get("observed_at_utc") or datetime.now(timezone.utc).isoformat(),
        "count_total": 1,
        "score": 50,  # Default score for single-source
    }

    if market_type == "player_prop":
        signal["player_key"] = market.get("player_key")
        signal["player_id"] = market.get("player_key")
        signal["atomic_stat"] = market.get("stat_key")
        signal["direction"] = normalize_direction(market)
        signal["selection"] = market.get("selection")
        signal["line"] = market.get("line")
    elif market_type in ("spread", "total"):
        signal["selection"] = market.get("selection")
        signal["line"] = market.get("line")
        if market_type == "total":
            signal["direction"] = normalize_direction(market)
    elif market_type == "moneyline":
        signal["selection"] = market.get("selection")

    # Compute signal_id
    signal["signal_id"] = compute_signal_id(signal)
    if not signal["signal_id"]:
        return None

    return signal


def convert_signal_to_occurrence(
    signal: Dict[str, Any],
    source_id: str,
) -> Dict[str, Any]:
    """Convert a signal to occurrence format for signals_occurrences."""
    occurrence_id = sha256_json({
        "signal_id": signal.get("signal_id"),
        "source_id": source_id,
        "observed_at_utc": signal.get("observed_at_utc"),
    })

    return {
        "occurrence_id": occurrence_id,
        "signal_id": signal.get("signal_id"),
        "source_id": source_id,
        "source_surface": f"{source_id}_backfill",
        "sources_present": signal.get("sources_present", [source_id]),
        "day_key": signal.get("day_key"),
        "event_key": signal.get("event_key"),
        "matchup_key": signal.get("matchup_key"),
        "market_type": signal.get("market_type"),
        "selection": signal.get("selection"),
        "line": signal.get("line"),
        "direction": signal.get("direction"),
        "player_key": signal.get("player_key"),
        "player_id": signal.get("player_id"),
        "atomic_stat": signal.get("atomic_stat"),
        "away_team": signal.get("away_team"),
        "home_team": signal.get("home_team"),
        "score": signal.get("score"),
        "supports": signal.get("supports", []),
        "observed_at_utc": signal.get("observed_at_utc"),
        "count_total": signal.get("count_total", 1),
    }


def create_grade_stub(
    signal: Dict[str, Any],
    source_id: str,
) -> Dict[str, Any]:
    """Create a grade stub that will be filled by the grading script."""
    return {
        "signal_id": signal.get("signal_id"),
        "sources_combo": source_id,
        "day_key": signal.get("day_key"),
        "event_key": signal.get("event_key"),
        "market_type": signal.get("market_type"),
        "selection": signal.get("selection"),
        "line": signal.get("line"),
        "direction": signal.get("direction"),
        "player_key": signal.get("player_key"),
        "atomic_stat": signal.get("atomic_stat"),
        "away_team": signal.get("away_team"),
        "home_team": signal.get("home_team"),
        "status": "PENDING",
        "result": None,
        "units": None,
        "graded_at_utc": None,
        "notes": "backfill_injected",
    }


def process_source(
    records: List[Dict[str, Any]],
    source_id: str,
    seen_signal_ids: Set[str],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Process records from a single source."""
    signals = []
    occurrences = []
    grades = []

    for record in records:
        signal = convert_record_to_signal(record, source_id)
        if not signal:
            continue

        sid = signal.get("signal_id")
        if not sid or sid in seen_signal_ids:
            continue

        seen_signal_ids.add(sid)
        signals.append(signal)
        occurrences.append(convert_signal_to_occurrence(signal, source_id))
        grades.append(create_grade_stub(signal, source_id))

    return signals, occurrences, grades


def process_dimers_backfill(
    records: List[Dict[str, Any]],
    seen_signal_ids: Set[str],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Process dimers backfill records."""
    signals = []
    occurrences = []
    grades = []

    for record in records:
        signal = convert_dimers_record_to_signal(record)
        if not signal:
            continue

        sid = signal.get("signal_id")
        if not sid or sid in seen_signal_ids:
            continue

        seen_signal_ids.add(sid)
        signals.append(signal)
        occurrences.append(convert_signal_to_occurrence(signal, "dimers"))
        grades.append(create_grade_stub(signal, "dimers"))

    return signals, occurrences, grades


def main():
    parser = argparse.ArgumentParser(
        description="Inject backfill data into the consensus/grading pipeline"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't write files, just show what would be done",
    )
    parser.add_argument(
        "--source",
        choices=["betql", "action", "dimers", "sportsline", "sportscapping", "all"],
        default="all",
        help="Which source to process (default: all)",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append to existing files instead of overwriting",
    )
    args = parser.parse_args()

    all_signals: List[Dict[str, Any]] = []
    all_occurrences: List[Dict[str, Any]] = []
    all_grades: List[Dict[str, Any]] = []
    seen_signal_ids: Set[str] = set()
    stats: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

    # Load existing signal IDs to avoid duplicates
    existing_signals_path = Path("data/ledger/signals_latest.jsonl")
    if existing_signals_path.exists():
        with open(existing_signals_path, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    try:
                        sig = json.loads(line)
                        if sig.get("signal_id"):
                            seen_signal_ids.add(sig["signal_id"])
                    except json.JSONDecodeError:
                        pass
        print(f"Loaded {len(seen_signal_ids)} existing signal IDs to avoid duplicates")

    # Process BetQL spreads
    if args.source in ("betql", "all"):
        print(f"\nProcessing BetQL spreads from {MERGED_BETQL_SPREADS}")
        records = load_merged_data(MERGED_BETQL_SPREADS)
        print(f"  Loaded {len(records)} records")
        signals, occs, grades = process_source(records, "betql", seen_signal_ids)
        all_signals.extend(signals)
        all_occurrences.extend(occs)
        all_grades.extend(grades)
        stats["betql"]["spreads"] = len(signals)
        print(f"  Created {len(signals)} signals")

        # Process BetQL totals
        print(f"\nProcessing BetQL totals from {MERGED_BETQL_TOTALS}")
        records = load_merged_data(MERGED_BETQL_TOTALS)
        print(f"  Loaded {len(records)} records")
        signals, occs, grades = process_source(records, "betql", seen_signal_ids)
        all_signals.extend(signals)
        all_occurrences.extend(occs)
        all_grades.extend(grades)
        stats["betql"]["totals"] = len(signals)
        print(f"  Created {len(signals)} signals")

        # Process BetQL props
        print(f"\nProcessing BetQL props from {MERGED_BETQL_PROPS}")
        records = load_merged_data(MERGED_BETQL_PROPS)
        print(f"  Loaded {len(records)} records")
        signals, occs, grades = process_source(records, "betql", seen_signal_ids)
        all_signals.extend(signals)
        all_occurrences.extend(occs)
        all_grades.extend(grades)
        stats["betql"]["props"] = len(signals)
        print(f"  Created {len(signals)} signals")

    # Process Action
    if args.source in ("action", "all"):
        print(f"\nProcessing Action from {MERGED_ACTION}")
        records = load_merged_data(MERGED_ACTION)
        print(f"  Loaded {len(records)} records")
        signals, occs, grades = process_source(records, "action", seen_signal_ids)
        all_signals.extend(signals)
        all_occurrences.extend(occs)
        all_grades.extend(grades)
        stats["action"]["all"] = len(signals)
        print(f"  Created {len(signals)} signals")

    # Process Dimers backfill
    if args.source in ("dimers", "all"):
        print(f"\nProcessing Dimers backfill from {DIMERS_BACKFILL}")
        records = load_jsonl_data(DIMERS_BACKFILL)
        print(f"  Loaded {len(records)} records")
        signals, occs, grades = process_dimers_backfill(records, seen_signal_ids)
        all_signals.extend(signals)
        all_occurrences.extend(occs)
        all_grades.extend(grades)
        stats["dimers"]["backfill"] = len(signals)
        print(f"  Created {len(signals)} signals")

        # Also try normalized dimers
        print(f"\nProcessing Dimers normalized from {NORMALIZED_DIMERS}")
        records = load_merged_data(NORMALIZED_DIMERS)
        print(f"  Loaded {len(records)} records")
        signals, occs, grades = process_source(records, "dimers", seen_signal_ids)
        all_signals.extend(signals)
        all_occurrences.extend(occs)
        all_grades.extend(grades)
        stats["dimers"]["normalized"] = len(signals)
        print(f"  Created {len(signals)} signals")

    # Process Sportsline
    if args.source in ("sportsline", "all"):
        print(f"\nProcessing Sportsline from {NORMALIZED_SPORTSLINE}")
        records = load_merged_data(NORMALIZED_SPORTSLINE)
        print(f"  Loaded {len(records)} records")
        signals, occs, grades = process_source(records, "sportsline", seen_signal_ids)
        all_signals.extend(signals)
        all_occurrences.extend(occs)
        all_grades.extend(grades)
        stats["sportsline"]["normalized"] = len(signals)
        print(f"  Created {len(signals)} signals")

    # Process Sportscapping
    if args.source in ("sportscapping", "all"):
        print(f"\nProcessing Sportscapping from {NORMALIZED_SPORTSCAPPING}")
        records = load_merged_data(NORMALIZED_SPORTSCAPPING)
        print(f"  Loaded {len(records)} records")
        signals, occs, grades = process_source(records, "sportscapping", seen_signal_ids)
        all_signals.extend(signals)
        all_occurrences.extend(occs)
        all_grades.extend(grades)
        stats["sportscapping"]["normalized"] = len(signals)
        print(f"  Created {len(signals)} signals")

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Total new signals: {len(all_signals)}")
    print(f"Total occurrences: {len(all_occurrences)}")
    print(f"Total grade stubs: {len(all_grades)}")

    for source, markets in stats.items():
        total = sum(markets.values())
        print(f"\n{source.upper()}: {total} signals")
        for market, count in markets.items():
            print(f"  {market}: {count}")

    # Market type breakdown
    market_counts = Counter(s.get("market_type") for s in all_signals)
    print(f"\nBy market type:")
    for mkt, cnt in market_counts.most_common():
        print(f"  {mkt}: {cnt}")

    if args.dry_run:
        print("\n[DRY RUN] No files written")
        return

    # Write output files
    mode = "a" if args.append else "w"

    print(f"\nWriting signals to {SIGNALS_BACKFILL_PATH}")
    SIGNALS_BACKFILL_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(SIGNALS_BACKFILL_PATH, mode, encoding="utf-8") as f:
        for sig in all_signals:
            f.write(json.dumps(sig, ensure_ascii=False, sort_keys=True) + "\n")

    print(f"Writing occurrences to {SIGNALS_OCCURRENCES_BACKFILL_PATH}")
    with open(SIGNALS_OCCURRENCES_BACKFILL_PATH, mode, encoding="utf-8") as f:
        for occ in all_occurrences:
            f.write(json.dumps(occ, ensure_ascii=False, sort_keys=True) + "\n")

    print(f"Writing grade stubs to {GRADES_BACKFILL_PATH}")
    with open(GRADES_BACKFILL_PATH, mode, encoding="utf-8") as f:
        for grade in all_grades:
            f.write(json.dumps(grade, ensure_ascii=False, sort_keys=True) + "\n")

    print("\nDone! Next steps:")
    print("  1. Run: python3 scripts/grade_signals_nba.py --input data/ledger/grades_backfill.jsonl")
    print("  2. Merge outputs into main ledger files")
    print("  3. Rebuild research dataset: python3 scripts/build_research_dataset_nba_occurrences.py")


if __name__ == "__main__":
    main()
