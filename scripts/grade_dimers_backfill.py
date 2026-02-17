#!/usr/bin/env python3
"""Grade Dimers backfill data.

This script grades Dimers spread, total, and moneyline picks from the backfill.

Usage:
    python3 scripts/grade_dimers_backfill.py
    python3 scripts/grade_dimers_backfill.py --market spread
    python3 scripts/grade_dimers_backfill.py --min-edge 5.0
    python3 scripts/grade_dimers_backfill.py --output out/dimers_grades.jsonl
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.results import nba_provider

# Default input file
DEFAULT_INPUT = Path("out/backfill_dimers_bets.jsonl")

# Team name mapping
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


def normalize_team(name: str) -> Optional[str]:
    """Normalize team name to 3-letter code."""
    if not name:
        return None
    name_lower = name.lower().strip()
    if name_lower in TEAM_MAP:
        return TEAM_MAP[name_lower]
    # Already a code
    if len(name) == 3 and name.upper() == name:
        return name
    return None


def parse_matchup(matchup: str) -> Tuple[Optional[str], Optional[str]]:
    """Parse matchup like 'Nets vs. Cavaliers' into team codes."""
    if not matchup:
        return None, None

    # Try "Away vs. Home" pattern
    m = re.match(r"(\w+)\s+vs\.?\s+(\w+)", matchup, re.IGNORECASE)
    if m:
        away = normalize_team(m.group(1))
        home = normalize_team(m.group(2))
        return away, home

    return None, None


def parse_pick_text(pick_text: str, market_type: str) -> Dict[str, Any]:
    """Parse pick text into structured data."""
    result = {
        "picked_team": None,
        "line": None,
        "direction": None,
    }

    if not pick_text:
        return result

    pick_lower = pick_text.lower()

    if market_type == "moneyline":
        # Pattern: "TeamName win"
        m = re.match(r"(\w+)\s+win", pick_text, re.IGNORECASE)
        if m:
            result["picked_team"] = normalize_team(m.group(1))

    elif market_type == "spread":
        # Pattern: "TeamName +/-X.X"
        m = re.match(r"(\w+)\s+([+-]?\d+\.?\d*)", pick_text, re.IGNORECASE)
        if m:
            result["picked_team"] = normalize_team(m.group(1))
            try:
                result["line"] = float(m.group(2))
            except ValueError:
                pass

    elif market_type == "total":
        # Pattern: "Over/Under X.X"
        m = re.match(r"(over|under)\s+(\d+\.?\d*)", pick_text, re.IGNORECASE)
        if m:
            result["direction"] = m.group(1).upper()
            try:
                result["line"] = float(m.group(2))
            except ValueError:
                pass

    return result


def parse_final_score(final_score: str) -> Tuple[Optional[int], Optional[int]]:
    """Parse final score like '108-120' into away_score, home_score.

    Note: Dimers shows scores in order, typically away-home.
    """
    if not final_score:
        return None, None

    m = re.match(r"(\d+)-(\d+)", final_score)
    if m:
        try:
            s1, s2 = int(m.group(1)), int(m.group(2))
            return s1, s2
        except ValueError:
            return None, None

    return None, None


def is_valid_nba_score(away_score: Optional[int], home_score: Optional[int]) -> bool:
    """Check if scores look like valid NBA final scores (typically 70-170)."""
    if away_score is None or home_score is None:
        return False
    # NBA scores are typically between 70 and 170
    return 70 <= away_score <= 170 and 70 <= home_score <= 170


def grade_spread(
    row: Dict[str, Any],
    away_team: str,
    home_team: str,
    away_score: int,
    home_score: int,
) -> Dict[str, Any]:
    """Grade a spread pick."""
    pick_info = parse_pick_text(row.get("pick_text", ""), "spread")
    picked_team = pick_info.get("picked_team")
    line = pick_info.get("line")

    result = {
        "game_date": row.get("game_date"),
        "matchup": row.get("matchup"),
        "market_type": "spread",
        "pick_text": row.get("pick_text"),
        "picked_team": picked_team,
        "line": line,
        "away_team": away_team,
        "home_team": home_team,
        "away_score": away_score,
        "home_score": home_score,
        "probability_pct": row.get("probability_pct"),
        "edge_pct": row.get("edge_pct"),
        "best_odds": row.get("best_odds"),
        "status": "PENDING",
        "error": None,
    }

    if not picked_team:
        result["status"] = "ERROR"
        result["error"] = "cannot_parse_picked_team"
        return result

    if line is None:
        result["status"] = "ERROR"
        result["error"] = "cannot_parse_line"
        return result

    # Calculate spread result
    if picked_team == away_team:
        adjusted_score = away_score + line
        opponent_score = home_score
    elif picked_team == home_team:
        adjusted_score = home_score + line
        opponent_score = away_score
    else:
        result["status"] = "ERROR"
        result["error"] = f"picked_team_not_in_matchup:{picked_team}"
        return result

    result["adjusted_score"] = adjusted_score
    result["opponent_score"] = opponent_score

    if adjusted_score > opponent_score:
        result["status"] = "WIN"
    elif adjusted_score == opponent_score:
        result["status"] = "PUSH"
    else:
        result["status"] = "LOSS"

    return result


def grade_total(
    row: Dict[str, Any],
    away_team: str,
    home_team: str,
    away_score: int,
    home_score: int,
) -> Dict[str, Any]:
    """Grade a total (over/under) pick."""
    pick_info = parse_pick_text(row.get("pick_text", ""), "total")
    direction = pick_info.get("direction")
    line = pick_info.get("line")

    result = {
        "game_date": row.get("game_date"),
        "matchup": row.get("matchup"),
        "market_type": "total",
        "pick_text": row.get("pick_text"),
        "direction": direction,
        "line": line,
        "away_team": away_team,
        "home_team": home_team,
        "away_score": away_score,
        "home_score": home_score,
        "probability_pct": row.get("probability_pct"),
        "edge_pct": row.get("edge_pct"),
        "best_odds": row.get("best_odds"),
        "status": "PENDING",
        "error": None,
    }

    if not direction:
        result["status"] = "ERROR"
        result["error"] = "cannot_parse_direction"
        return result

    if line is None:
        result["status"] = "ERROR"
        result["error"] = "cannot_parse_line"
        return result

    total_score = away_score + home_score
    result["total_score"] = total_score

    if direction == "OVER":
        if total_score > line:
            result["status"] = "WIN"
        elif total_score == line:
            result["status"] = "PUSH"
        else:
            result["status"] = "LOSS"
    else:  # UNDER
        if total_score < line:
            result["status"] = "WIN"
        elif total_score == line:
            result["status"] = "PUSH"
        else:
            result["status"] = "LOSS"

    return result


def grade_moneyline(
    row: Dict[str, Any],
    away_team: str,
    home_team: str,
    away_score: int,
    home_score: int,
) -> Dict[str, Any]:
    """Grade a moneyline pick."""
    pick_info = parse_pick_text(row.get("pick_text", ""), "moneyline")
    picked_team = pick_info.get("picked_team")

    result = {
        "game_date": row.get("game_date"),
        "matchup": row.get("matchup"),
        "market_type": "moneyline",
        "pick_text": row.get("pick_text"),
        "picked_team": picked_team,
        "away_team": away_team,
        "home_team": home_team,
        "away_score": away_score,
        "home_score": home_score,
        "probability_pct": row.get("probability_pct"),
        "edge_pct": row.get("edge_pct"),
        "best_odds": row.get("best_odds"),
        "status": "PENDING",
        "error": None,
    }

    if not picked_team:
        result["status"] = "ERROR"
        result["error"] = "cannot_parse_picked_team"
        return result

    # Determine winner
    if away_score > home_score:
        winner = away_team
    elif home_score > away_score:
        winner = home_team
    else:
        # Tie (shouldn't happen in NBA but handle it)
        result["status"] = "PUSH"
        return result

    if picked_team == winner:
        result["status"] = "WIN"
    elif picked_team in (away_team, home_team):
        result["status"] = "LOSS"
    else:
        result["status"] = "ERROR"
        result["error"] = f"picked_team_not_in_matchup:{picked_team}"

    return result


def american_odds_to_units(odds_str: Optional[str], outcome: str) -> float:
    """Calculate units won/lost based on American odds."""
    if outcome == "PUSH":
        return 0.0
    if outcome == "LOSS":
        return -1.0
    if outcome != "WIN":
        return 0.0

    # Parse odds
    if not odds_str:
        odds = -110  # Default
    else:
        try:
            odds = int(odds_str.replace("+", ""))
        except ValueError:
            odds = -110

    if odds > 0:
        return odds / 100.0
    else:
        return 100.0 / abs(odds)


def load_dimers_backfill(input_file: Path) -> List[Dict[str, Any]]:
    """Load Dimers backfill data from JSONL file."""
    rows = []
    if not input_file.exists():
        return rows

    with open(input_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    return rows


def main():
    parser = argparse.ArgumentParser(description="Grade Dimers backfill data")
    parser.add_argument("--input", type=str, default=str(DEFAULT_INPUT),
                       help=f"Input JSONL file (default: {DEFAULT_INPUT})")
    parser.add_argument("--market", choices=["spread", "total", "moneyline", "all"],
                       default="all", help="Market type to grade")
    parser.add_argument("--min-edge", type=float, default=None,
                       help="Minimum edge percentage to include")
    parser.add_argument("--min-prob", type=float, default=None,
                       help="Minimum probability percentage to include")
    parser.add_argument("--output", type=str, default=None,
                       help="Output file for detailed grades (JSONL)")
    parser.add_argument("--by-edge-bucket", action="store_true",
                       help="Show results broken down by edge bucket")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    input_file = Path(args.input)
    rows = load_dimers_backfill(input_file)

    print(f"Loaded {len(rows)} Dimers backfill records from {input_file}")

    if not rows:
        print("No data to grade.")
        return

    # Filter by market type
    if args.market != "all":
        rows = [r for r in rows if r.get("market_type") == args.market]
        print(f"Filtered to {len(rows)} {args.market} records")

    # Filter by edge
    if args.min_edge is not None:
        rows = [r for r in rows if (r.get("edge_pct") or 0) >= args.min_edge]
        print(f"Filtered to {len(rows)} records with edge >= {args.min_edge}%")

    # Filter by probability
    if args.min_prob is not None:
        rows = [r for r in rows if (r.get("probability_pct") or 0) >= args.min_prob]
        print(f"Filtered to {len(rows)} records with probability >= {args.min_prob}%")

    # Grade each record
    all_grades = []

    for row in rows:
        matchup = row.get("matchup", "")
        away_team, home_team = parse_matchup(matchup)

        # Parse final score from backfill data
        final_score = row.get("final_score", "")
        away_score, home_score = parse_final_score(final_score)

        market_type = row.get("market_type", "")

        # Check if scores look valid (detect truncated scores like "3-06" instead of "103-106")
        scores_valid = is_valid_nba_score(away_score, home_score)

        # If we don't have valid scores from backfill, try to fetch them
        if not scores_valid:
            game_date_str = row.get("game_date", "")
            if game_date_str and away_team and home_team:
                try:
                    game_date = datetime.strptime(game_date_str, "%Y-%m-%d").date()
                    if game_date <= date.today():
                        event_key = f"NBA:{game_date.year:04d}:{game_date.month:02d}:{game_date.day:02d}:{away_team}@{home_team}"
                        game_result = nba_provider.fetch_game_result(
                            event_key=event_key,
                            away=away_team,
                            home=home_team,
                            date_str=game_date_str,
                        )
                        if game_result and game_result.get("status") != "ERROR":
                            away_score = game_result.get("away_score")
                            home_score = game_result.get("home_score")
                except Exception:
                    pass

        # Skip if we still don't have valid scores
        if not is_valid_nba_score(away_score, home_score):
            # Determine specific error reason
            if away_score is None or home_score is None:
                error_reason = "missing_scores"
            else:
                error_reason = f"invalid_scores:{away_score}-{home_score}"

            grade = {
                "game_date": row.get("game_date"),
                "matchup": matchup,
                "market_type": market_type,
                "pick_text": row.get("pick_text"),
                "probability_pct": row.get("probability_pct"),
                "edge_pct": row.get("edge_pct"),
                "raw_final_score": final_score,
                "status": "ERROR",
                "error": error_reason,
            }
            all_grades.append(grade)
            continue

        # Grade based on market type
        if market_type == "spread":
            grade = grade_spread(row, away_team, home_team, away_score, home_score)
        elif market_type == "total":
            grade = grade_total(row, away_team, home_team, away_score, home_score)
        elif market_type == "moneyline":
            grade = grade_moneyline(row, away_team, home_team, away_score, home_score)
        else:
            grade = {
                "game_date": row.get("game_date"),
                "matchup": matchup,
                "market_type": market_type,
                "pick_text": row.get("pick_text"),
                "status": "ERROR",
                "error": f"unknown_market_type:{market_type}",
            }

        all_grades.append(grade)

        if args.verbose and grade["status"] in ("WIN", "LOSS", "PUSH"):
            print(f"  {grade['game_date']} {grade['matchup']}: {grade['pick_text']} -> {grade['status']}")

    # Summary
    print(f"\n{'='*60}")
    print("DIMERS BACKFILL GRADING SUMMARY")
    print(f"{'='*60}")

    # By market type
    market_types = ["spread", "total", "moneyline"] if args.market == "all" else [args.market]

    for market in market_types:
        market_grades = [g for g in all_grades if g.get("market_type") == market]
        if not market_grades:
            continue

        status_counts = Counter(g["status"] for g in market_grades)
        wins = status_counts.get("WIN", 0)
        losses = status_counts.get("LOSS", 0)
        pushes = status_counts.get("PUSH", 0)
        errors = status_counts.get("ERROR", 0)
        graded = wins + losses + pushes

        print(f"\n{market.upper()}:")
        print(f"  Total: {len(market_grades)} | Graded: {graded} | Errors: {errors}")

        if graded > 0:
            win_pct = 100.0 * wins / (wins + losses) if (wins + losses) > 0 else 0
            total_units = sum(
                american_odds_to_units(g.get("best_odds"), g["status"])
                for g in market_grades if g["status"] in ("WIN", "LOSS", "PUSH")
            )
            roi = 100.0 * total_units / graded if graded > 0 else 0

            print(f"  Record: {wins}W - {losses}L - {pushes}P")
            print(f"  Win Rate: {win_pct:.1f}%")
            print(f"  Net Units: {total_units:+.2f}u")
            print(f"  ROI: {roi:+.1f}%")

        # Error breakdown
        if errors > 0:
            error_counts = Counter(g.get("error") for g in market_grades if g["status"] == "ERROR")
            print(f"  Error breakdown:")
            for err, count in error_counts.most_common(5):
                print(f"    {err}: {count}")

    # Overall
    print(f"\n{'='*60}")
    print("OVERALL (all markets)")
    print(f"{'='*60}")

    status_counts = Counter(g["status"] for g in all_grades)
    wins = status_counts.get("WIN", 0)
    losses = status_counts.get("LOSS", 0)
    pushes = status_counts.get("PUSH", 0)
    errors = status_counts.get("ERROR", 0)
    graded = wins + losses + pushes

    print(f"Total: {len(all_grades)} | Graded: {graded} | Errors: {errors}")

    if graded > 0:
        win_pct = 100.0 * wins / (wins + losses) if (wins + losses) > 0 else 0
        total_units = sum(
            american_odds_to_units(g.get("best_odds"), g["status"])
            for g in all_grades if g["status"] in ("WIN", "LOSS", "PUSH")
        )
        roi = 100.0 * total_units / graded if graded > 0 else 0

        print(f"Record: {wins}W - {losses}L - {pushes}P")
        print(f"Win Rate: {win_pct:.1f}%")
        print(f"Net Units: {total_units:+.2f}u")
        print(f"ROI: {roi:+.1f}%")

    # By edge bucket
    if args.by_edge_bucket:
        print(f"\n{'='*60}")
        print("RESULTS BY EDGE BUCKET")
        print(f"{'='*60}")

        buckets = [
            (2.0, 3.0, "2-3%"),
            (3.0, 5.0, "3-5%"),
            (5.0, 7.5, "5-7.5%"),
            (7.5, 10.0, "7.5-10%"),
            (10.0, 100.0, "10%+"),
        ]

        for min_e, max_e, label in buckets:
            bucket_grades = [
                g for g in all_grades
                if g.get("status") in ("WIN", "LOSS", "PUSH")
                and min_e <= (g.get("edge_pct") or 0) < max_e
            ]

            if not bucket_grades:
                continue

            wins = sum(1 for g in bucket_grades if g["status"] == "WIN")
            losses = sum(1 for g in bucket_grades if g["status"] == "LOSS")
            pushes = sum(1 for g in bucket_grades if g["status"] == "PUSH")
            graded = wins + losses + pushes

            if graded > 0:
                win_pct = 100.0 * wins / (wins + losses) if (wins + losses) > 0 else 0
                total_units = sum(
                    american_odds_to_units(g.get("best_odds"), g["status"])
                    for g in bucket_grades
                )
                roi = 100.0 * total_units / graded

                print(f"\nEdge {label} (n={graded}):")
                print(f"  Record: {wins}W - {losses}L - {pushes}P")
                print(f"  Win Rate: {win_pct:.1f}% | ROI: {roi:+.1f}%")

    # Write output
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            for grade in all_grades:
                f.write(json.dumps(grade, ensure_ascii=False, sort_keys=True) + "\n")
        print(f"\nDetailed grades written to: {output_path}")


if __name__ == "__main__":
    main()
