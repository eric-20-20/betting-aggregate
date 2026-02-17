#!/usr/bin/env python3
"""Grade BetQL spreads/totals and Action backfill data directly.

This script grades:
1. BetQL spreads from data/history/betql_normalized/spreads/
2. BetQL totals from data/history/betql_normalized/totals/
3. Action spreads/totals from out/normalized_action_nba_backfill.jsonl

Usage:
    python3 scripts/grade_backfill_spreads_totals.py
    python3 scripts/grade_backfill_spreads_totals.py --source betql --market spread
    python3 scripts/grade_backfill_spreads_totals.py --source action --market total
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import re

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.results import nba_provider


def load_betql_spreads_totals(market_type: str = "spread") -> List[Dict[str, Any]]:
    """Load BetQL spreads or totals from normalized history."""
    if market_type == "spread":
        data_dir = Path("data/history/betql_normalized/spreads")
    else:
        data_dir = Path("data/history/betql_normalized/totals")

    all_rows = []
    for jsonl_file in sorted(data_dir.glob("*.jsonl")):
        if jsonl_file.stat().st_size == 0:
            continue
        with open(jsonl_file, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                    row["_source_file"] = jsonl_file.name
                    row["_source_type"] = "betql"
                    all_rows.append(row)
                except json.JSONDecodeError:
                    continue
    return all_rows


def load_action_backfill(market_type: str = "spread") -> List[Dict[str, Any]]:
    """Load Action backfill data for a specific market type."""
    paths = [
        Path("out/normalized_action_nba_backfill.jsonl"),
        Path("out_action_hist/normalized_action_nba_backfill.jsonl"),
    ]

    all_rows = []
    for path in paths:
        if not path.exists():
            continue
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                    row_market = row.get("market", {}).get("market_type", "")
                    if row_market == market_type:
                        row["_source_type"] = "action"
                        all_rows.append(row)
                except json.JSONDecodeError:
                    continue
    return all_rows


def normalize_team_code(team: str) -> str:
    """Normalize team name to 3-letter code."""
    team = (team or "").upper().strip()
    team_map = {
        "TIMBERWOLVES": "MIN", "PISTONS": "DET", "CELTICS": "BOS", "HEAT": "MIA",
        "LAKERS": "LAL", "CLIPPERS": "LAC", "WARRIORS": "GSW", "SUNS": "PHX",
        "NUGGETS": "DEN", "JAZZ": "UTA", "TRAIL BLAZERS": "POR", "BLAZERS": "POR",
        "THUNDER": "OKC", "MAVERICKS": "DAL", "ROCKETS": "HOU", "SPURS": "SAS",
        "GRIZZLIES": "MEM", "PELICANS": "NOP", "KINGS": "SAC", "HAWKS": "ATL",
        "HORNETS": "CHA", "BULLS": "CHI", "CAVALIERS": "CLE", "PACERS": "IND",
        "BUCKS": "MIL", "NETS": "BKN", "KNICKS": "NYK", "76ERS": "PHI",
        "SIXERS": "PHI", "RAPTORS": "TOR", "WIZARDS": "WAS", "MAGIC": "ORL",
        "MINNESOTA": "MIN", "DETROIT": "DET", "BOSTON": "BOS", "MIAMI": "MIA",
        "LOS ANGELES": "LAL", "GOLDEN STATE": "GSW", "PHOENIX": "PHX",
        "DENVER": "DEN", "UTAH": "UTA", "PORTLAND": "POR", "OKLAHOMA CITY": "OKC",
        "DALLAS": "DAL", "HOUSTON": "HOU", "SAN ANTONIO": "SAS", "MEMPHIS": "MEM",
        "NEW ORLEANS": "NOP", "SACRAMENTO": "SAC", "ATLANTA": "ATL",
        "CHARLOTTE": "CHA", "CHICAGO": "CHI", "CLEVELAND": "CLE", "INDIANA": "IND",
        "MILWAUKEE": "MIL", "BROOKLYN": "BKN", "NEW YORK": "NYK",
        "PHILADELPHIA": "PHI", "TORONTO": "TOR", "WASHINGTON": "WAS", "ORLANDO": "ORL",
        "MIN": "MIN", "DET": "DET", "BOS": "BOS", "MIA": "MIA", "LAL": "LAL",
        "LAC": "LAC", "GSW": "GSW", "GS": "GSW", "PHX": "PHX", "PHO": "PHX",
        "DEN": "DEN", "UTA": "UTA", "POR": "POR", "OKC": "OKC", "DAL": "DAL",
        "HOU": "HOU", "SAS": "SAS", "SA": "SAS", "MEM": "MEM", "NOP": "NOP",
        "NO": "NOP", "SAC": "SAC", "ATL": "ATL", "CHA": "CHA", "CHO": "CHA",
        "CHI": "CHI", "CLE": "CLE", "IND": "IND", "MIL": "MIL", "BKN": "BKN",
        "BRK": "BKN", "NYK": "NYK", "NY": "NYK", "PHI": "PHI", "TOR": "TOR",
        "WAS": "WAS", "ORL": "ORL",
    }
    return team_map.get(team, team[:3] if len(team) >= 3 else team)


def parse_date_from_day_key(day_key: str) -> Optional[date]:
    """Parse date from day_key like NBA:2025:02:09."""
    m = re.match(r'NBA:(\d{4}):(\d{2}):(\d{2})', day_key)
    if m:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    return None


def parse_date_from_event_key(event_key: str) -> Optional[date]:
    """Parse date from event_key like NBA:20260206:MIA@BOS:2130."""
    # Pattern: NBA:20260206:MIA@BOS:2130
    m = re.match(r'NBA:(\d{4})(\d{2})(\d{2}):', event_key)
    if m:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    return None


def extract_teams_from_event_key(event_key: str) -> Tuple[Optional[str], Optional[str]]:
    """Extract away and home teams from event_key like NBA:20260206:MIA@BOS:2130."""
    m = re.match(r'NBA:\d{8}:([A-Z]+)@([A-Z]+)', event_key)
    if m:
        return m.group(1), m.group(2)
    return None, None


def grade_spread(
    row: Dict[str, Any],
    refresh_cache: bool = False,
) -> Dict[str, Any]:
    """Grade a spread pick."""
    # Extract fields from normalized format
    event = row.get("event", {})
    market = row.get("market", {})

    day_key = event.get("day_key", "")
    event_key = event.get("event_key", "")
    away_team = normalize_team_code(event.get("away_team", ""))
    home_team = normalize_team_code(event.get("home_team", ""))

    # Fallback: extract teams from event_key if not in event
    if not away_team or not home_team:
        ek_away, ek_home = extract_teams_from_event_key(event_key)
        if not away_team and ek_away:
            away_team = normalize_team_code(ek_away)
        if not home_team and ek_home:
            home_team = normalize_team_code(ek_home)

    line = market.get("line")
    direction = market.get("direction", "").upper()
    selection = market.get("selection", "")  # e.g., "MIN -3.5" or team name
    odds = market.get("odds")

    # Convert string odds to int if needed
    if isinstance(odds, str):
        try:
            odds = int(odds)
        except ValueError:
            odds = None

    result = {
        "day_key": day_key,
        "event_key": event_key,
        "away_team": away_team,
        "home_team": home_team,
        "line": line,
        "direction": direction,
        "selection": selection,
        "odds": odds,
        "source": row.get("_source_type"),
        "status": "PENDING",
        "error": None,
    }

    # Try to parse game date from day_key or event_key
    game_date = parse_date_from_day_key(day_key)
    if not game_date:
        game_date = parse_date_from_event_key(event_key)
    if not game_date:
        result["status"] = "ERROR"
        result["error"] = "invalid_day_key"
        return result

    if game_date > date.today():
        result["status"] = "PENDING"
        result["error"] = "future_game"
        return result

    if not away_team or not home_team:
        result["status"] = "ERROR"
        result["error"] = "missing_teams"
        return result

    if line is None:
        result["status"] = "ERROR"
        result["error"] = "missing_line"
        return result

    # Determine which team was picked
    # Selection could be "MIN -3.5", "TIMBERWOLVES", or direction like "AWAY"
    picked_team = None
    if direction in ("AWAY", "VISITOR"):
        picked_team = away_team
    elif direction in ("HOME",):
        picked_team = home_team
    else:
        # Try to parse from selection
        sel_upper = (selection or "").upper()
        if away_team in sel_upper:
            picked_team = away_team
        elif home_team in sel_upper:
            picked_team = home_team
        else:
            # Try normalizing
            for word in sel_upper.split():
                normalized = normalize_team_code(word)
                if normalized == away_team:
                    picked_team = away_team
                    break
                elif normalized == home_team:
                    picked_team = home_team
                    break

    if not picked_team:
        result["status"] = "ERROR"
        result["error"] = f"cannot_determine_picked_team:{selection}"
        return result

    result["picked_team"] = picked_team

    # Get game result
    event_key = f"NBA:{game_date.year:04d}:{game_date.month:02d}:{game_date.day:02d}:{away_team}@{home_team}"
    date_str = game_date.strftime("%Y-%m-%d")

    try:
        game_result = nba_provider.fetch_game_result(
            event_key=event_key,
            away=away_team,
            home=home_team,
            date_str=date_str,
            refresh_cache=refresh_cache,
        )

        if game_result is None:
            result["status"] = "ERROR"
            result["error"] = "no_game_result"
            return result

        if game_result.get("status") == "ERROR":
            result["status"] = "ERROR"
            result["error"] = f"game_error:{game_result.get('notes', 'unknown')}"
            return result

        away_score = game_result.get("away_score")
        home_score = game_result.get("home_score")

        if away_score is None or home_score is None:
            result["status"] = "ERROR"
            result["error"] = "missing_scores"
            return result

        result["away_score"] = away_score
        result["home_score"] = home_score

        # Calculate spread result
        # Line is from picked team's perspective
        # If picked_team is away: away_score + line vs home_score
        # If picked_team is home: home_score + line vs away_score
        if picked_team == away_team:
            adjusted_score = away_score + line
            opponent_score = home_score
        else:
            adjusted_score = home_score + line
            opponent_score = away_score

        result["adjusted_score"] = adjusted_score
        result["opponent_score"] = opponent_score

        if adjusted_score > opponent_score:
            result["status"] = "WIN"
        elif adjusted_score == opponent_score:
            result["status"] = "PUSH"
        else:
            result["status"] = "LOSS"

        return result

    except Exception as e:
        result["status"] = "ERROR"
        result["error"] = f"exception:{str(e)[:50]}"
        return result


def grade_total(
    row: Dict[str, Any],
    refresh_cache: bool = False,
) -> Dict[str, Any]:
    """Grade a total (over/under) pick."""
    event = row.get("event", {})
    market = row.get("market", {})

    day_key = event.get("day_key", "")
    event_key = event.get("event_key", "")
    away_team = normalize_team_code(event.get("away_team", ""))
    home_team = normalize_team_code(event.get("home_team", ""))

    # Fallback: extract teams from event_key if not in event
    if not away_team or not home_team:
        ek_away, ek_home = extract_teams_from_event_key(event_key)
        if not away_team and ek_away:
            away_team = normalize_team_code(ek_away)
        if not home_team and ek_home:
            home_team = normalize_team_code(ek_home)

    line = market.get("line")
    # Direction can be in 'direction', 'side', or 'selection' field
    # Action uses 'side' field with values like "over" or "under"
    # BetQL uses 'selection' field with values like "OVER" or "UNDER"
    direction = market.get("direction", "").upper()
    if not direction or direction in ("GAME_TOTAL", ""):
        direction = market.get("side", "").upper()
    if not direction or direction not in ("OVER", "UNDER"):
        direction = market.get("selection", "").upper()
    odds = market.get("odds")

    # Convert string odds to int if needed
    if isinstance(odds, str):
        try:
            odds = int(odds)
        except ValueError:
            odds = None

    result = {
        "day_key": day_key,
        "event_key": event_key,
        "away_team": away_team,
        "home_team": home_team,
        "line": line,
        "direction": direction,
        "odds": odds,
        "source": row.get("_source_type"),
        "status": "PENDING",
        "error": None,
    }

    # Try to parse game date from day_key or event_key
    game_date = parse_date_from_day_key(day_key)
    if not game_date:
        game_date = parse_date_from_event_key(event_key)
    if not game_date:
        result["status"] = "ERROR"
        result["error"] = "invalid_day_key"
        return result

    if game_date > date.today():
        result["status"] = "PENDING"
        result["error"] = "future_game"
        return result

    if not away_team or not home_team:
        result["status"] = "ERROR"
        result["error"] = "missing_teams"
        return result

    if line is None:
        result["status"] = "ERROR"
        result["error"] = "missing_line"
        return result

    if direction not in ("OVER", "UNDER"):
        result["status"] = "ERROR"
        result["error"] = f"invalid_direction:{direction}"
        return result

    # Get game result
    event_key = f"NBA:{game_date.year:04d}:{game_date.month:02d}:{game_date.day:02d}:{away_team}@{home_team}"
    date_str = game_date.strftime("%Y-%m-%d")

    try:
        game_result = nba_provider.fetch_game_result(
            event_key=event_key,
            away=away_team,
            home=home_team,
            date_str=date_str,
            refresh_cache=refresh_cache,
        )

        if game_result is None:
            result["status"] = "ERROR"
            result["error"] = "no_game_result"
            return result

        if game_result.get("status") == "ERROR":
            result["status"] = "ERROR"
            result["error"] = f"game_error:{game_result.get('notes', 'unknown')}"
            return result

        away_score = game_result.get("away_score")
        home_score = game_result.get("home_score")

        if away_score is None or home_score is None:
            result["status"] = "ERROR"
            result["error"] = "missing_scores"
            return result

        result["away_score"] = away_score
        result["home_score"] = home_score

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

    except Exception as e:
        result["status"] = "ERROR"
        result["error"] = f"exception:{str(e)[:50]}"
        return result


def american_odds_to_units(odds: Optional[int], outcome: str) -> float:
    """Calculate units won/lost based on American odds."""
    if odds is None:
        odds = -110  # Default assumption
    if outcome == "PUSH":
        return 0.0
    if outcome == "LOSS":
        return -1.0
    if outcome == "WIN":
        if odds > 0:
            return odds / 100.0
        else:
            return 100.0 / abs(odds)
    return 0.0


def main():
    parser = argparse.ArgumentParser(description="Grade BetQL spreads/totals and Action backfill")
    parser.add_argument("--source", choices=["betql", "action", "all"], default="all",
                       help="Data source to grade")
    parser.add_argument("--market", choices=["spread", "total", "all"], default="all",
                       help="Market type to grade")
    parser.add_argument("--limit", type=int, default=None,
                       help="Limit number of picks to grade (for testing)")
    parser.add_argument("--refresh-cache", action="store_true",
                       help="Refresh provider cache")
    parser.add_argument("--output", type=str, default=None,
                       help="Output file for detailed grades (JSONL)")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    all_grades = []

    # Load and grade data based on source and market
    sources_to_process = ["betql", "action"] if args.source == "all" else [args.source]
    markets_to_process = ["spread", "total"] if args.market == "all" else [args.market]

    for source in sources_to_process:
        for market in markets_to_process:
            print(f"\n{'='*60}")
            print(f"Processing {source.upper()} {market.upper()}s")
            print(f"{'='*60}")

            # Load data
            if source == "betql":
                rows = load_betql_spreads_totals(market)
            else:
                rows = load_action_backfill(market)

            if args.limit:
                rows = rows[:args.limit]

            print(f"Loaded {len(rows)} rows")

            if not rows:
                continue

            # Grade
            grades = []
            for i, row in enumerate(rows):
                if (i + 1) % 100 == 0:
                    print(f"  Progress: {i+1}/{len(rows)}", end="\r")

                if market == "spread":
                    grade = grade_spread(row, refresh_cache=args.refresh_cache)
                else:
                    grade = grade_total(row, refresh_cache=args.refresh_cache)

                grade["market_type"] = market
                grades.append(grade)

                if args.verbose and grade["status"] in ("WIN", "LOSS", "PUSH"):
                    if market == "spread":
                        print(f"  {grade['picked_team']} {grade['line']:+.1f}: "
                              f"{grade['away_team']} {grade.get('away_score')} @ "
                              f"{grade['home_team']} {grade.get('home_score')} -> {grade['status']}")
                    else:
                        print(f"  {grade['direction']} {grade['line']}: "
                              f"actual={grade.get('total_score')} -> {grade['status']}")

            print(f"  Graded {len(grades)} {source} {market}s")
            all_grades.extend(grades)

            # Summary for this batch
            status_counts = Counter(g["status"] for g in grades)
            wins = status_counts.get("WIN", 0)
            losses = status_counts.get("LOSS", 0)
            pushes = status_counts.get("PUSH", 0)
            graded = wins + losses + pushes

            if graded > 0:
                win_pct = 100.0 * wins / (wins + losses) if (wins + losses) > 0 else 0
                total_units = sum(american_odds_to_units(g.get("odds"), g["status"])
                                 for g in grades if g["status"] in ("WIN", "LOSS", "PUSH"))
                roi = 100.0 * total_units / graded if graded > 0 else 0

                print(f"\n  {source.upper()} {market.upper()} Record: {wins}W-{losses}L-{pushes}P")
                print(f"  Win Rate: {win_pct:.1f}%")
                print(f"  Net Units: {total_units:+.2f}u")
                print(f"  ROI: {roi:+.1f}%")

            # Error breakdown
            errors = [g for g in grades if g["status"] == "ERROR"]
            if errors:
                error_counts = Counter(g.get("error", "unknown") for g in errors)
                print(f"\n  Errors ({len(errors)}):")
                for err, count in error_counts.most_common(5):
                    print(f"    {err}: {count}")

    # Overall summary
    print(f"\n{'='*60}")
    print("OVERALL SUMMARY")
    print(f"{'='*60}")

    for market in markets_to_process:
        market_grades = [g for g in all_grades if g.get("market_type") == market]
        if not market_grades:
            continue

        status_counts = Counter(g["status"] for g in market_grades)
        wins = status_counts.get("WIN", 0)
        losses = status_counts.get("LOSS", 0)
        pushes = status_counts.get("PUSH", 0)
        graded = wins + losses + pushes

        if graded > 0:
            win_pct = 100.0 * wins / (wins + losses) if (wins + losses) > 0 else 0
            total_units = sum(american_odds_to_units(g.get("odds"), g["status"])
                             for g in market_grades if g["status"] in ("WIN", "LOSS", "PUSH"))
            roi = 100.0 * total_units / graded if graded > 0 else 0

            print(f"\n{market.upper()}S (all sources combined):")
            print(f"  Record: {wins}W - {losses}L - {pushes}P")
            print(f"  Win Rate: {win_pct:.1f}%")
            print(f"  Net Units: {total_units:+.2f}u")
            print(f"  ROI: {roi:+.1f}%")
            print(f"  Total Bets: {graded}")

    # Output detailed grades
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            for grade in all_grades:
                f.write(json.dumps(grade, ensure_ascii=False, sort_keys=True) + "\n")
        print(f"\nDetailed grades written to: {output_path}")


if __name__ == "__main__":
    main()
