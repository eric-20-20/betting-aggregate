#!/usr/bin/env python3
"""Integrate SportsLine expert picks into the ledger pipeline.

SportsLine expert picks come pre-graded (WIN/LOSS/PUSH), so we:
1. Convert them to ledger signal occurrence format
2. Generate pre-graded entries based on source-provided results

Usage:
    python3 scripts/integrate_sportsline_expert_picks.py
    python3 scripts/integrate_sportsline_expert_picks.py --debug
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.signal_keys import build_selection_key, build_offer_key

INPUT_PATH = REPO_ROOT / "out" / "normalized_sportsline_expert_picks.jsonl"
SIGNALS_OCC_PATH = REPO_ROOT / "data" / "ledger" / "signals_occurrences.jsonl"
GRADES_OCC_PATH = REPO_ROOT / "data" / "ledger" / "grades_occurrences.jsonl"


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not path.exists():
        return out
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def append_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
            f.write("\n")


def build_signal_key(signal: Dict[str, Any]) -> str:
    """Build signal_key hash from signal components."""
    market_type = (signal.get("market_type") or "").lower()
    game_key = signal.get("event_key") or signal.get("day_key") or ""
    selection = signal.get("selection") or ""
    line = signal.get("line") or ""
    atomic_stat = signal.get("atomic_stat") or ""
    direction = signal.get("direction") or ""
    if market_type == "player_prop":
        raw = f"{game_key}|player_prop|{selection}|{line}"
    else:
        raw = f"{game_key}|{market_type}|{selection}|{direction}|{atomic_stat}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def build_occurrence_id(record: Dict[str, Any]) -> str:
    """Build unique occurrence ID."""
    run_id = record.get("run_id") or ""
    observed = record.get("observed_at_utc") or ""
    source_id = record.get("source_id") or record.get("sources_combo") or ""
    source_surface = record.get("source_surface") or ""
    canonical_url = record.get("canonical_url") or ""
    event_key = record.get("event_key") or record.get("day_key") or ""
    selection = record.get("selection") or ""
    line = record.get("line") or ""
    raw = f"{run_id}|{observed}|{source_id}|{source_surface}|{canonical_url}|{event_key}|{selection}|{line}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def canonicalize_game_keys(
    day_key: Optional[str],
    away_team: Optional[str],
    home_team: Optional[str],
    sport: str = "NBA"
) -> Tuple[Optional[str], Optional[str], Optional[Tuple[str, str, str, str]]]:
    """Build canonical game keys from day_key and teams."""
    if not day_key:
        return None, None, None

    away = away_team.upper() if away_team else None
    home = home_team.upper() if home_team else None

    if not away or not home:
        return None, None, None

    teams_sorted = sorted([away, home])
    event_key = f"{day_key}:{away}@{home}"
    matchup_key = f"{day_key}:{home}-{away}"
    canonical_game_key = (sport, day_key, teams_sorted[0], teams_sorted[1])
    return event_key, matchup_key, canonical_game_key


def normalized_to_signal_occurrence(row: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a normalized SportsLine expert pick to signal occurrence format."""
    ev = row.get("event") or {}
    mk = row.get("market") or {}
    prov = row.get("provenance") or {}
    result = row.get("result") or {}

    day_key = ev.get("day_key")
    event_key = ev.get("event_key")
    away = ev.get("away_team")
    home = ev.get("home_team")

    ev_key, mu_key, cgk = canonicalize_game_keys(day_key, away, home)
    if ev_key:
        event_key = ev_key

    # Extract player info for props
    player_id = mk.get("player_id")
    player_name = mk.get("player_name")
    stat_key = mk.get("stat_key")

    # Build player key for props
    player_key = None
    if player_name and mk.get("market_type") == "player_prop":
        # Normalize player name to slug format
        slug = player_name.lower().replace(" ", "_").replace(".", "").replace("'", "")
        player_key = f"NBA:{slug}"

    # Build selection for player props
    selection = mk.get("selection")
    if mk.get("market_type") == "player_prop" and player_key and stat_key:
        direction = mk.get("side") or mk.get("selection")
        if direction:
            selection = f"{player_key}::{stat_key}::{direction.upper()}"

    source_id = prov.get("source_id") or "sportsline"

    signal = {
        "event_key": event_key,
        "day_key": day_key,
        "matchup_key": mu_key or ev.get("matchup_key"),
        "market_type": mk.get("market_type"),
        "selection": selection,
        "direction": mk.get("side"),
        "atomic_stat": stat_key,
        "line": mk.get("line"),
        "odds": mk.get("odds"),
        "player_id": player_id,
        "player_key": player_key,
        "player_name": player_name,
        "observed_at_utc": prov.get("observed_at_utc"),
        "sources_combo": source_id,
        "sources": [source_id],
        "sources_present": [source_id],
        "signal_type": "sportsline_expert_picks",
        "source_id": source_id,
        "source_surface": prov.get("source_surface"),
        "run_id": "sportsline_expert_backfill",
        "away_team": away,
        "home_team": home,
        "expert_name": prov.get("expert_name"),
        "expert_id": prov.get("expert_id"),
        "writeup": prov.get("writeup"),
        "raw_pick_text": prov.get("raw_pick_text"),
        "urls": [],
        "supports": [
            {
                "source_id": source_id,
                "source_surface": prov.get("source_surface"),
                "selection": selection,
                "direction": mk.get("side"),
                "line": mk.get("line"),
                "line_hint": mk.get("line"),
                "raw_pick_text": prov.get("raw_pick_text"),
                "canonical_url": None,
            }
        ],
        # Pre-graded result from SportsLine
        "pregraded_result": result.get("status"),
        "pregraded_by": result.get("graded_by"),
        # Scores for verification
        "away_score": ev.get("away_score"),
        "home_score": ev.get("home_score"),
    }

    signal["canonical_game_key"] = cgk
    signal["signal_key"] = build_signal_key(signal)

    # Build selection_key and offer_key
    selection_key = build_selection_key(
        day_key=day_key or "",
        market_type=mk.get("market_type") or "",
        selection=selection,
        player_id=player_id,
        atomic_stat=stat_key,
        direction=mk.get("side"),
        team=selection if mk.get("market_type") in ("spread", "moneyline") else None,
    )
    signal["selection_key"] = selection_key
    signal["offer_key"] = build_offer_key(selection_key, mk.get("line"), mk.get("odds"))
    signal["occurrence_id"] = build_occurrence_id(signal)
    signal["signal_id"] = signal.get("signal_key")

    return signal


def signal_to_grade(signal: Dict[str, Any], graded_at: str) -> Optional[Dict[str, Any]]:
    """Convert a signal with pre-graded result to a grade entry."""
    pregraded = signal.get("pregraded_result")
    if not pregraded or pregraded == "PENDING":
        return None

    status = pregraded.upper()
    if status not in {"WIN", "LOSS", "PUSH"}:
        return None

    # Calculate units from odds
    odds = signal.get("odds")
    units = None
    if odds is not None and status in {"WIN", "LOSS", "PUSH"}:
        try:
            o = float(odds)
            if status == "PUSH":
                units = 0.0
            elif status == "WIN":
                units = (o / 100.0) if o > 0 else (100.0 / abs(o))
                units = round(units, 3)
            elif status == "LOSS":
                units = -1.0
        except (TypeError, ValueError):
            pass

    return {
        "signal_id": signal.get("signal_id"),
        "graded_at_utc": graded_at,
        "status": status,
        "result": status,
        "units": units,
        "market_type": signal.get("market_type"),
        "selection": signal.get("selection"),
        "event_key": signal.get("event_key"),
        "day_key": signal.get("day_key"),
        "urls": [],
        "notes": f"pregraded_by_{signal.get('pregraded_by', 'sportsline')}",
        "selection_key": signal.get("selection_key"),
        "offer_key": signal.get("offer_key"),
        "roi_eligible": odds is not None,
        "source_graded": True,
        "pregraded_by": signal.get("pregraded_by"),
        "expert_name": signal.get("expert_name"),
        "away_score": signal.get("away_score"),
        "home_score": signal.get("home_score"),
    }


def main():
    parser = argparse.ArgumentParser(description="Integrate SportsLine expert picks into ledger")
    parser.add_argument("--input", default=str(INPUT_PATH), help="Input normalized JSONL file")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    parser.add_argument("--dry-run", action="store_true", help="Don't write output files")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        return 1

    # Read normalized picks
    normalized = read_jsonl(input_path)
    print(f"[integrate] Read {len(normalized)} normalized expert picks from {args.input}")

    # Load existing occurrence IDs to avoid duplicates
    existing_signal_ids = set()
    existing_grade_ids = set()

    if SIGNALS_OCC_PATH.exists():
        for row in read_jsonl(SIGNALS_OCC_PATH):
            oid = row.get("occurrence_id")
            if oid:
                existing_signal_ids.add(oid)
    print(f"[integrate] Existing signal occurrences: {len(existing_signal_ids)}")

    if GRADES_OCC_PATH.exists():
        for row in read_jsonl(GRADES_OCC_PATH):
            sid = row.get("signal_id")
            if sid:
                existing_grade_ids.add(sid)
    print(f"[integrate] Existing grade entries: {len(existing_grade_ids)}")

    # Convert to signal occurrences
    new_signals: List[Dict[str, Any]] = []
    new_grades: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc).isoformat()

    counters: Counter = Counter()
    market_counts: Counter = Counter()
    result_counts: Counter = Counter()

    for row in normalized:
        signal = normalized_to_signal_occurrence(row)
        oid = signal.get("occurrence_id")

        if oid in existing_signal_ids:
            counters["skipped_duplicate_signal"] += 1
            continue

        new_signals.append(signal)
        existing_signal_ids.add(oid)
        counters["added_signal"] += 1
        market_counts[signal.get("market_type")] += 1

        # Generate grade entry
        sid = signal.get("signal_id")
        if sid not in existing_grade_ids:
            grade = signal_to_grade(signal, now)
            if grade:
                new_grades.append(grade)
                existing_grade_ids.add(sid)
                counters["added_grade"] += 1
                result_counts[grade.get("result")] += 1
        else:
            counters["skipped_duplicate_grade"] += 1

    print(f"\n[integrate] Conversion summary:")
    print(f"  New signals: {counters['added_signal']}")
    print(f"  New grades: {counters['added_grade']}")
    print(f"  Skipped duplicate signals: {counters['skipped_duplicate_signal']}")
    print(f"  Skipped duplicate grades: {counters['skipped_duplicate_grade']}")
    print(f"\n[integrate] Markets: {dict(market_counts)}")
    print(f"[integrate] Results: {dict(result_counts)}")

    if new_signals:
        # Win rate calculation
        wins = result_counts.get("WIN", 0)
        losses = result_counts.get("LOSS", 0)
        total = wins + losses
        if total > 0:
            win_rate = wins / total * 100
            print(f"\n[integrate] Win rate: {wins}/{total} = {win_rate:.1f}%")

    if args.debug and new_signals:
        print("\n[debug] Sample signal occurrence:")
        print(json.dumps(new_signals[0], indent=2))
        if new_grades:
            print("\n[debug] Sample grade entry:")
            print(json.dumps(new_grades[0], indent=2))

    if args.dry_run:
        print("\n[integrate] Dry run - no files written")
        return 0

    # Write new entries
    if new_signals:
        append_jsonl(SIGNALS_OCC_PATH, new_signals)
        print(f"\n[integrate] Appended {len(new_signals)} signals to {SIGNALS_OCC_PATH}")

    if new_grades:
        append_jsonl(GRADES_OCC_PATH, new_grades)
        print(f"[integrate] Appended {len(new_grades)} grades to {GRADES_OCC_PATH}")

    print("\n[integrate] Done! Run the following to update latest files:")
    print("  python3 scripts/build_signal_ledger.py")
    print("  python3 scripts/report_records.py")

    return 0


if __name__ == "__main__":
    sys.exit(main())
