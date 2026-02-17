#!/usr/bin/env python3
"""Spot-check grading accuracy by sampling random graded picks.

This script selects random GRADED signals and displays:
- The pick details (market, selection, line)
- The actual game result (scores)
- The computed grade (WIN/LOSS/PUSH)
- Manual verification of the math

Usage:
    python3 scripts/audit_grade_spot_check.py
    python3 scripts/audit_grade_spot_check.py --sample 20
    python3 scripts/audit_grade_spot_check.py --market spread
    python3 scripts/audit_grade_spot_check.py --sport NCAAB
"""

from __future__ import annotations

import argparse
import json
import random
from pathlib import Path
from typing import Any, Dict, List, Optional


def get_dataset_path(sport: str) -> Path:
    if sport.upper() == "NCAAB":
        return Path("data/analysis/ncaab/graded_occurrences_latest.jsonl")
    return Path("data/analysis/graded_occurrences_latest.jsonl")


def get_grades_path(sport: str) -> Path:
    """Get path to grades ledger which has stat_value for player props."""
    if sport.upper() == "NCAAB":
        return Path("data/ledger/ncaab/grades_latest.jsonl")
    return Path("data/ledger/grades_latest.jsonl")


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows = []
    if not path.exists():
        return rows
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def verify_spread_grade(row: Dict[str, Any]) -> Dict[str, Any]:
    """Verify spread grading is correct."""
    home_score = row.get("final_home_score")
    away_score = row.get("final_away_score")
    line = row.get("line") or row.get("spread_line")
    selection = row.get("selection") or row.get("spread_pick") or ""
    home_team = row.get("home_team", "")
    away_team = row.get("away_team", "")
    stored_result = row.get("result")

    if home_score is None or away_score is None or line is None:
        return {
            "valid": False,
            "reason": "missing_data",
            "home_score": home_score,
            "away_score": away_score,
            "line": line
        }

    # Determine which team was picked
    selection_upper = selection.upper()
    picked_team = None

    # Check if selection contains team code
    if home_team.upper() in selection_upper or "_HOME" in selection_upper:
        picked_team = "home"
    elif away_team.upper() in selection_upper or "_AWAY" in selection_upper:
        picked_team = "away"
    else:
        # Try to extract from selection
        for part in selection_upper.replace("_SPREAD", "").replace("::", ":").split(":"):
            if part == home_team.upper():
                picked_team = "home"
                break
            elif part == away_team.upper():
                picked_team = "away"
                break

    if not picked_team:
        return {
            "valid": False,
            "reason": "cannot_determine_pick",
            "selection": selection,
            "home_team": home_team,
            "away_team": away_team
        }

    # Calculate margin from perspective of picked team
    if picked_team == "home":
        margin = home_score - away_score
    else:
        margin = away_score - home_score

    # Add line to margin (spread is from picked team's perspective)
    adjusted = margin + line

    # Determine expected result
    if adjusted > 0:
        expected = "WIN"
    elif adjusted < 0:
        expected = "LOSS"
    else:
        expected = "PUSH"

    return {
        "valid": True,
        "picked_team": picked_team,
        "home_score": home_score,
        "away_score": away_score,
        "line": line,
        "margin": margin,
        "adjusted": adjusted,
        "expected_result": expected,
        "stored_result": stored_result,
        "match": expected == stored_result,
        "calculation": f"{picked_team} margin={margin:+.1f}, +line({line:+.1f}) = {adjusted:+.1f} => {expected}"
    }


def verify_total_grade(row: Dict[str, Any]) -> Dict[str, Any]:
    """Verify total (over/under) grading is correct."""
    home_score = row.get("final_home_score")
    away_score = row.get("final_away_score")
    line = row.get("line")
    selection = row.get("selection") or ""
    direction = row.get("direction") or ""
    stored_result = row.get("result")

    if home_score is None or away_score is None or line is None:
        return {
            "valid": False,
            "reason": "missing_data"
        }

    total_pts = home_score + away_score

    # Determine direction from selection or direction field
    sel_upper = (selection + direction).upper()
    if "OVER" in sel_upper:
        pick_direction = "OVER"
    elif "UNDER" in sel_upper:
        pick_direction = "UNDER"
    else:
        return {
            "valid": False,
            "reason": "cannot_determine_direction",
            "selection": selection,
            "direction": direction
        }

    # Calculate expected result
    if pick_direction == "OVER":
        if total_pts > line:
            expected = "WIN"
        elif total_pts < line:
            expected = "LOSS"
        else:
            expected = "PUSH"
    else:  # UNDER
        if total_pts < line:
            expected = "WIN"
        elif total_pts > line:
            expected = "LOSS"
        else:
            expected = "PUSH"

    return {
        "valid": True,
        "direction": pick_direction,
        "home_score": home_score,
        "away_score": away_score,
        "total_pts": total_pts,
        "line": line,
        "expected_result": expected,
        "stored_result": stored_result,
        "match": expected == stored_result,
        "calculation": f"Total={total_pts}, Line={line}, {pick_direction} => {expected}"
    }


def verify_player_prop_grade(row: Dict[str, Any]) -> Dict[str, Any]:
    """Verify player prop grading is correct."""
    line = row.get("line")
    direction = row.get("direction") or ""
    selection = row.get("selection") or ""
    stored_result = row.get("result")
    player_meta = row.get("player_meta") or {}
    # stat_value can be at top level (grades_latest) or in player_meta
    stat_value = row.get("stat_value") or player_meta.get("stat_value")
    player_id = row.get("player_id") or row.get("player_key") or ""
    atomic_stat = row.get("atomic_stat") or row.get("stat_key") or ""

    if stat_value is None or line is None:
        return {
            "valid": False,
            "reason": "missing_stat_value_or_line",
            "stat_value": stat_value,
            "line": line,
            "player_meta": player_meta
        }

    # Determine direction
    sel_upper = (selection + direction).upper()
    if "OVER" in sel_upper:
        pick_direction = "OVER"
    elif "UNDER" in sel_upper:
        pick_direction = "UNDER"
    else:
        return {
            "valid": False,
            "reason": "cannot_determine_direction",
            "selection": selection,
            "direction": direction
        }

    # Calculate expected result
    if pick_direction == "OVER":
        if stat_value > line:
            expected = "WIN"
        elif stat_value < line:
            expected = "LOSS"
        else:
            expected = "PUSH"
    else:  # UNDER
        if stat_value < line:
            expected = "WIN"
        elif stat_value > line:
            expected = "LOSS"
        else:
            expected = "PUSH"

    return {
        "valid": True,
        "player": player_id,
        "stat": atomic_stat,
        "stat_value": stat_value,
        "line": line,
        "direction": pick_direction,
        "expected_result": expected,
        "stored_result": stored_result,
        "match": expected == stored_result,
        "calculation": f"{player_id} {atomic_stat}={stat_value}, Line={line}, {pick_direction} => {expected}"
    }


def verify_moneyline_grade(row: Dict[str, Any]) -> Dict[str, Any]:
    """Verify moneyline grading is correct."""
    home_score = row.get("final_home_score")
    away_score = row.get("final_away_score")
    selection = row.get("selection") or ""
    home_team = row.get("home_team", "")
    away_team = row.get("away_team", "")
    stored_result = row.get("result")

    if home_score is None or away_score is None:
        return {
            "valid": False,
            "reason": "missing_scores"
        }

    # Determine which team was picked
    selection_upper = selection.upper()
    picked_team = None

    if home_team.upper() in selection_upper or "_HOME" in selection_upper:
        picked_team = "home"
    elif away_team.upper() in selection_upper or "_AWAY" in selection_upper:
        picked_team = "away"
    else:
        for part in selection_upper.replace("_ML", "").replace("::", ":").split(":"):
            if part == home_team.upper():
                picked_team = "home"
                break
            elif part == away_team.upper():
                picked_team = "away"
                break

    if not picked_team:
        return {
            "valid": False,
            "reason": "cannot_determine_pick",
            "selection": selection
        }

    # Determine winner
    if home_score > away_score:
        winner = "home"
    elif away_score > home_score:
        winner = "away"
    else:
        winner = "tie"

    # Calculate expected result
    if winner == "tie":
        expected = "PUSH"
    elif picked_team == winner:
        expected = "WIN"
    else:
        expected = "LOSS"

    return {
        "valid": True,
        "picked_team": picked_team,
        "home_score": home_score,
        "away_score": away_score,
        "winner": winner,
        "expected_result": expected,
        "stored_result": stored_result,
        "match": expected == stored_result,
        "calculation": f"Picked {picked_team}, Winner={winner} ({home_score}-{away_score}) => {expected}"
    }


def verify_grade(row: Dict[str, Any]) -> Dict[str, Any]:
    """Verify a grade based on market type."""
    market_type = (row.get("market_type") or "").lower()

    if market_type == "spread":
        return verify_spread_grade(row)
    elif market_type == "total":
        return verify_total_grade(row)
    elif market_type == "player_prop":
        return verify_player_prop_grade(row)
    elif market_type == "moneyline":
        return verify_moneyline_grade(row)
    else:
        return {"valid": False, "reason": f"unknown_market_type: {market_type}"}


def print_audit_result(idx: int, row: Dict[str, Any], verification: Dict[str, Any]) -> None:
    """Print a single audit result."""
    market_type = row.get("market_type", "unknown")
    sources_combo = row.get("sources_combo") or row.get("signal_sources_combo") or "unknown"
    day_key = row.get("day_key", "")
    selection = row.get("selection", "")[:60]
    stored_result = row.get("result")
    stored_units = row.get("units")

    print(f"\n{'='*80}")
    print(f"Sample {idx}: {market_type.upper()} | {sources_combo}")
    print(f"{'='*80}")
    print(f"  Day:       {day_key}")
    print(f"  Selection: {selection}")
    print(f"  Matchup:   {row.get('away_team', '?')} @ {row.get('home_team', '?')}")

    if verification.get("valid"):
        print(f"\n  Verification:")
        print(f"    {verification.get('calculation', 'N/A')}")
        print(f"\n  Stored Result: {stored_result}")
        print(f"  Expected:      {verification.get('expected_result')}")

        if verification.get("match"):
            print(f"  Status:        CORRECT")
        else:
            print(f"  Status:        MISMATCH")

        if stored_units is not None:
            print(f"  Units:         {stored_units:+.3f}")
    else:
        print(f"\n  Verification FAILED: {verification.get('reason')}")
        for k, v in verification.items():
            if k not in ("valid", "reason"):
                print(f"    {k}: {v}")


def main():
    parser = argparse.ArgumentParser(description="Spot-check grading accuracy")
    parser.add_argument("--sample", type=int, default=10, help="Number of samples to check (default: 10)")
    parser.add_argument("--market", type=str, help="Filter by market type: spread, total, player_prop, moneyline")
    parser.add_argument("--sport", type=str, default="NBA", choices=["NBA", "NCAAB"])
    parser.add_argument("--seed", type=int, help="Random seed for reproducibility")
    parser.add_argument("--show-all", action="store_true", help="Show all samples, not just mismatches")
    parser.add_argument("--source", type=str, help="Filter by source combo (e.g., 'sportsline')")
    parser.add_argument("--require-scores", action="store_true", help="Only include rows with final scores")

    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    # Use grades_latest for player props (has stat_value), research dataset for others
    if args.market and args.market.lower() == "player_prop":
        dataset_path = get_grades_path(args.sport)
        if not dataset_path.exists():
            print(f"Grades not found: {dataset_path}")
            print("You may need to run the grading scripts first.")
            return
        rows = load_jsonl(dataset_path)
        print(f"Loaded {len(rows)} rows from {dataset_path} (grades ledger)")
        graded = [r for r in rows if r.get("status") in ("WIN", "LOSS", "PUSH") or r.get("result") in ("WIN", "LOSS", "PUSH")]
        # Normalize result field
        for r in graded:
            if r.get("status") and not r.get("result"):
                r["result"] = r["status"]
    else:
        dataset_path = get_dataset_path(args.sport)
        if not dataset_path.exists():
            print(f"Dataset not found: {dataset_path}")
            print("You may need to run the grading and research dataset scripts first.")
            return
        rows = load_jsonl(dataset_path)
        print(f"Loaded {len(rows)} rows from {dataset_path}")
        graded = [r for r in rows if r.get("grade_status") == "GRADED" and r.get("result") in ("WIN", "LOSS", "PUSH")]

    print(f"Found {len(graded)} GRADED rows with terminal result")

    if args.market:
        graded = [r for r in graded if (r.get("market_type") or "").lower() == args.market.lower()]
        print(f"Filtered to {len(graded)} {args.market} rows")

    if args.source:
        graded = [r for r in graded if args.source.lower() in (r.get("sources_combo") or r.get("source_id") or "").lower()]
        print(f"Filtered to {len(graded)} rows with source '{args.source}'")

    if args.require_scores:
        graded = [r for r in graded if r.get("final_home_score") is not None and r.get("final_away_score") is not None]
        print(f"Filtered to {len(graded)} rows with final scores")

    if not graded:
        print("No graded rows to audit.")
        return

    # Sample
    sample_size = min(args.sample, len(graded))
    samples = random.sample(graded, sample_size)

    print(f"\nAuditing {sample_size} random samples...")

    # Verify each
    correct = 0
    mismatches = []
    invalid = []

    for idx, row in enumerate(samples, 1):
        verification = verify_grade(row)

        if verification.get("valid"):
            if verification.get("match"):
                correct += 1
                if args.show_all:
                    print_audit_result(idx, row, verification)
            else:
                mismatches.append((idx, row, verification))
                print_audit_result(idx, row, verification)
        else:
            invalid.append((idx, row, verification))
            if args.show_all:
                print_audit_result(idx, row, verification)

    # Summary
    print(f"\n{'='*80}")
    print("AUDIT SUMMARY")
    print(f"{'='*80}")
    print(f"  Samples checked:    {sample_size}")
    print(f"  Correct grades:     {correct}")
    print(f"  Mismatches:         {len(mismatches)}")
    print(f"  Invalid (no data):  {len(invalid)}")

    if mismatches:
        print(f"\n  MISMATCHES FOUND - Review required!")
        for idx, row, v in mismatches:
            print(f"    Sample {idx}: {row.get('market_type')} - stored={v.get('stored_result')}, expected={v.get('expected_result')}")
    else:
        print(f"\n  All grades verified correct!")

    if invalid:
        print(f"\n  Invalid samples (missing data):")
        for idx, row, v in invalid:
            print(f"    Sample {idx}: {v.get('reason')}")

    # Market type breakdown
    print(f"\n  By Market Type:")
    market_counts = {}
    for r in samples:
        mt = r.get("market_type", "unknown")
        market_counts[mt] = market_counts.get(mt, 0) + 1
    for mt, count in sorted(market_counts.items()):
        print(f"    {mt}: {count}")


if __name__ == "__main__":
    main()
