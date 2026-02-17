#!/usr/bin/env python3
"""Analyze consensus between Action and BetQL in backfill data.

This script finds picks where Action and BetQL agree on:
- Same game (day + teams)
- Same market type (spread, total, player_prop)
- Same direction (for totals: over/under, for spreads: same team)

Then grades those consensus picks to see combined record.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.results import nba_provider


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


def parse_date_from_event_key(event_key: str) -> Optional[date]:
    """Parse date from event_key like NBA:20260206:MIA@BOS:2130."""
    m = re.match(r'NBA:(\d{4})(\d{2})(\d{2}):', event_key)
    if m:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    return None


def extract_teams_from_event_key(event_key: str) -> Tuple[Optional[str], Optional[str]]:
    """Extract away and home teams from event_key."""
    m = re.match(r'NBA:\d{8}:([A-Z]+)@([A-Z]+)', event_key)
    if m:
        return m.group(1), m.group(2)
    return None, None


def load_action_backfill() -> List[Dict[str, Any]]:
    """Load Action backfill data."""
    path = Path("out/normalized_action_nba_backfill.jsonl")
    rows = []
    if not path.exists():
        return rows
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return rows


def load_betql_data(market_type: str) -> List[Dict[str, Any]]:
    """Load BetQL data for a specific market type."""
    if market_type == "spread":
        data_dir = Path("data/history/betql_normalized/spreads")
    elif market_type == "total":
        data_dir = Path("data/history/betql_normalized/totals")
    else:
        return []

    rows = []
    for jsonl_file in sorted(data_dir.glob("*.jsonl")):
        if jsonl_file.stat().st_size == 0:
            continue
        with open(jsonl_file) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
    return rows


def extract_game_key(row: Dict[str, Any]) -> Optional[str]:
    """Extract a normalized game key for matching."""
    event = row.get("event", {})
    event_key = event.get("event_key", "")

    # Try to parse from event_key
    game_date = parse_date_from_event_key(event_key)
    away, home = extract_teams_from_event_key(event_key)

    if not game_date:
        # Try from day_key
        day_key = event.get("day_key", "")
        m = re.match(r'NBA:(\d{4}):(\d{2}):(\d{2})', day_key)
        if m:
            game_date = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))

    if not away or not home:
        away = normalize_team_code(event.get("away_team", ""))
        home = normalize_team_code(event.get("home_team", ""))

    if game_date and away and home:
        return f"{game_date.isoformat()}:{away}@{home}"
    return None


def extract_spread_pick(row: Dict[str, Any]) -> Optional[Tuple[str, float]]:
    """Extract (picked_team, line) from a spread row."""
    market = row.get("market", {})
    event = row.get("event", {})

    line = market.get("line")
    if line is None:
        return None

    # Get team codes
    away = normalize_team_code(event.get("away_team", ""))
    home = normalize_team_code(event.get("home_team", ""))

    if not away or not home:
        event_key = event.get("event_key", "")
        ek_away, ek_home = extract_teams_from_event_key(event_key)
        if ek_away:
            away = normalize_team_code(ek_away)
        if ek_home:
            home = normalize_team_code(ek_home)

    # Determine picked team
    direction = (market.get("direction") or "").upper()
    side = (market.get("side") or "").upper()
    selection = market.get("selection") or ""

    picked_team = None
    if direction in ("AWAY", "VISITOR") or side in ("AWAY", "VISITOR"):
        picked_team = away
    elif direction in ("HOME",) or side in ("HOME",):
        picked_team = home
    else:
        # Try selection
        sel_upper = (selection or "").upper()
        if away in sel_upper:
            picked_team = away
        elif home in sel_upper:
            picked_team = home
        else:
            for word in sel_upper.split():
                normalized = normalize_team_code(word)
                if normalized == away:
                    picked_team = away
                    break
                elif normalized == home:
                    picked_team = home
                    break

    if picked_team:
        return (picked_team, float(line))
    return None


def extract_total_pick(row: Dict[str, Any]) -> Optional[Tuple[str, float]]:
    """Extract (direction, line) from a total row."""
    market = row.get("market", {})

    line = market.get("line")
    if line is None:
        return None

    # Get direction
    direction = market.get("direction", "").upper()
    if not direction or direction in ("GAME_TOTAL", ""):
        direction = market.get("side", "").upper()
    if not direction or direction not in ("OVER", "UNDER"):
        direction = market.get("selection", "").upper()

    if direction in ("OVER", "UNDER"):
        return (direction, float(line))
    return None


def american_odds_to_units(odds: Optional[int], outcome: str) -> float:
    """Calculate units won/lost based on American odds."""
    if odds is None:
        odds = -110
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


def grade_spread_consensus(
    game_key: str,
    picked_team: str,
    line: float,
    refresh_cache: bool = False,
) -> Dict[str, Any]:
    """Grade a spread consensus pick."""
    parts = game_key.split(":")
    date_str = parts[0]
    matchup = parts[1]
    away, home = matchup.split("@")

    game_date = date.fromisoformat(date_str)

    result = {
        "game_key": game_key,
        "picked_team": picked_team,
        "line": line,
        "away_team": away,
        "home_team": home,
        "date": date_str,
        "status": "PENDING",
        "error": None,
    }

    if game_date > date.today():
        result["status"] = "PENDING"
        result["error"] = "future_game"
        return result

    event_key = f"NBA:{game_date.year:04d}:{game_date.month:02d}:{game_date.day:02d}:{away}@{home}"

    try:
        game_result = nba_provider.fetch_game_result(
            event_key=event_key,
            away=away,
            home=home,
            date_str=date_str,
            refresh_cache=refresh_cache,
        )

        if game_result is None or game_result.get("status") == "ERROR":
            result["status"] = "ERROR"
            result["error"] = "no_game_result"
            return result

        away_score = game_result.get("away_score")
        home_score = game_result.get("home_score")

        if away_score is None or home_score is None:
            result["status"] = "ERROR"
            result["error"] = "missing_scores"
            return result

        result["away_score"] = away_score
        result["home_score"] = home_score

        # Grade
        if picked_team == away:
            adjusted = away_score + line
            opponent = home_score
        else:
            adjusted = home_score + line
            opponent = away_score

        if adjusted > opponent:
            result["status"] = "WIN"
        elif adjusted == opponent:
            result["status"] = "PUSH"
        else:
            result["status"] = "LOSS"

        return result

    except Exception as e:
        result["status"] = "ERROR"
        result["error"] = str(e)[:50]
        return result


def grade_total_consensus(
    game_key: str,
    direction: str,
    line: float,
    refresh_cache: bool = False,
) -> Dict[str, Any]:
    """Grade a total consensus pick."""
    parts = game_key.split(":")
    date_str = parts[0]
    matchup = parts[1]
    away, home = matchup.split("@")

    game_date = date.fromisoformat(date_str)

    result = {
        "game_key": game_key,
        "direction": direction,
        "line": line,
        "away_team": away,
        "home_team": home,
        "date": date_str,
        "status": "PENDING",
        "error": None,
    }

    if game_date > date.today():
        result["status"] = "PENDING"
        result["error"] = "future_game"
        return result

    event_key = f"NBA:{game_date.year:04d}:{game_date.month:02d}:{game_date.day:02d}:{away}@{home}"

    try:
        game_result = nba_provider.fetch_game_result(
            event_key=event_key,
            away=away,
            home=home,
            date_str=date_str,
            refresh_cache=refresh_cache,
        )

        if game_result is None or game_result.get("status") == "ERROR":
            result["status"] = "ERROR"
            result["error"] = "no_game_result"
            return result

        away_score = game_result.get("away_score")
        home_score = game_result.get("home_score")

        if away_score is None or home_score is None:
            result["status"] = "ERROR"
            result["error"] = "missing_scores"
            return result

        result["away_score"] = away_score
        result["home_score"] = home_score
        total = away_score + home_score
        result["total"] = total

        # Grade
        if direction == "OVER":
            if total > line:
                result["status"] = "WIN"
            elif total == line:
                result["status"] = "PUSH"
            else:
                result["status"] = "LOSS"
        else:  # UNDER
            if total < line:
                result["status"] = "WIN"
            elif total == line:
                result["status"] = "PUSH"
            else:
                result["status"] = "LOSS"

        return result

    except Exception as e:
        result["status"] = "ERROR"
        result["error"] = str(e)[:50]
        return result


def main():
    parser = argparse.ArgumentParser(description="Analyze consensus between Action and BetQL")
    parser.add_argument("--market", choices=["spread", "total", "all"], default="all")
    parser.add_argument("--refresh-cache", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--line-tolerance", type=float, default=1.0,
                       help="Max difference in lines to still count as consensus (default: 1.0)")
    args = parser.parse_args()

    markets = ["spread", "total"] if args.market == "all" else [args.market]

    # Load Action data
    print("Loading Action backfill data...")
    action_rows = load_action_backfill()
    print(f"  Loaded {len(action_rows)} Action rows")

    for market in markets:
        print(f"\n{'='*60}")
        print(f"ANALYZING {market.upper()} CONSENSUS")
        print(f"{'='*60}")

        # Filter Action by market type
        action_market = [r for r in action_rows
                        if r.get("market", {}).get("market_type") == market]
        print(f"Action {market}s: {len(action_market)}")

        # Load BetQL data
        betql_market = load_betql_data(market)
        print(f"BetQL {market}s: {len(betql_market)}")

        if not action_market or not betql_market:
            print("  Not enough data to find consensus")
            continue

        # Index by game key
        if market == "spread":
            # action_by_game: game_key -> list of (picked_team, line, row)
            action_by_game = defaultdict(list)
            for row in action_market:
                gk = extract_game_key(row)
                pick = extract_spread_pick(row)
                if gk and pick:
                    action_by_game[gk].append((pick[0], pick[1], row))

            betql_by_game = defaultdict(list)
            for row in betql_market:
                gk = extract_game_key(row)
                pick = extract_spread_pick(row)
                if gk and pick:
                    betql_by_game[gk].append((pick[0], pick[1], row))

            # Find consensus
            consensus = []
            for gk in action_by_game:
                if gk not in betql_by_game:
                    continue

                for a_team, a_line, a_row in action_by_game[gk]:
                    for b_team, b_line, b_row in betql_by_game[gk]:
                        # Same team picked?
                        if a_team == b_team:
                            # Lines within tolerance?
                            if abs(a_line - b_line) <= args.line_tolerance:
                                avg_line = (a_line + b_line) / 2
                                consensus.append({
                                    "game_key": gk,
                                    "picked_team": a_team,
                                    "action_line": a_line,
                                    "betql_line": b_line,
                                    "avg_line": avg_line,
                                })

            print(f"\nFound {len(consensus)} consensus spread picks")

            if not consensus:
                continue

            # Grade consensus
            grades = []
            for i, c in enumerate(consensus):
                if (i + 1) % 20 == 0:
                    print(f"  Grading {i+1}/{len(consensus)}...", end="\r")

                grade = grade_spread_consensus(
                    c["game_key"],
                    c["picked_team"],
                    c["avg_line"],
                    refresh_cache=args.refresh_cache,
                )
                grade["action_line"] = c["action_line"]
                grade["betql_line"] = c["betql_line"]
                grades.append(grade)

            print()

        else:  # total
            action_by_game = defaultdict(list)
            for row in action_market:
                gk = extract_game_key(row)
                pick = extract_total_pick(row)
                if gk and pick:
                    action_by_game[gk].append((pick[0], pick[1], row))

            betql_by_game = defaultdict(list)
            for row in betql_market:
                gk = extract_game_key(row)
                pick = extract_total_pick(row)
                if gk and pick:
                    betql_by_game[gk].append((pick[0], pick[1], row))

            # Find consensus
            consensus = []
            for gk in action_by_game:
                if gk not in betql_by_game:
                    continue

                for a_dir, a_line, a_row in action_by_game[gk]:
                    for b_dir, b_line, b_row in betql_by_game[gk]:
                        # Same direction?
                        if a_dir == b_dir:
                            # Lines within tolerance?
                            if abs(a_line - b_line) <= args.line_tolerance:
                                avg_line = (a_line + b_line) / 2
                                consensus.append({
                                    "game_key": gk,
                                    "direction": a_dir,
                                    "action_line": a_line,
                                    "betql_line": b_line,
                                    "avg_line": avg_line,
                                })

            print(f"\nFound {len(consensus)} consensus total picks")

            if not consensus:
                continue

            # Grade consensus
            grades = []
            for i, c in enumerate(consensus):
                if (i + 1) % 20 == 0:
                    print(f"  Grading {i+1}/{len(consensus)}...", end="\r")

                grade = grade_total_consensus(
                    c["game_key"],
                    c["direction"],
                    c["avg_line"],
                    refresh_cache=args.refresh_cache,
                )
                grade["action_line"] = c["action_line"]
                grade["betql_line"] = c["betql_line"]
                grades.append(grade)

            print()

        # Summarize
        status_counts = Counter(g["status"] for g in grades)
        wins = status_counts.get("WIN", 0)
        losses = status_counts.get("LOSS", 0)
        pushes = status_counts.get("PUSH", 0)
        errors = status_counts.get("ERROR", 0)
        pending = status_counts.get("PENDING", 0)
        graded = wins + losses + pushes

        print(f"\n{market.upper()} CONSENSUS RESULTS:")
        print(f"  Total found: {len(grades)}")
        print(f"  Graded: {graded}")
        print(f"  Errors: {errors}")
        print(f"  Pending: {pending}")

        if graded > 0:
            win_pct = 100.0 * wins / (wins + losses) if (wins + losses) > 0 else 0
            total_units = sum(american_odds_to_units(-110, g["status"])
                             for g in grades if g["status"] in ("WIN", "LOSS", "PUSH"))
            roi = 100.0 * total_units / graded if graded > 0 else 0

            print(f"\n  RECORD: {wins}W - {losses}L - {pushes}P")
            print(f"  Win Rate: {win_pct:.1f}%")
            print(f"  Net Units: {total_units:+.2f}u (assuming -110 odds)")
            print(f"  ROI: {roi:+.1f}%")

        # Show sample of graded picks
        if args.verbose:
            print("\n  Sample picks:")
            for g in grades[:10]:
                if g["status"] in ("WIN", "LOSS", "PUSH"):
                    if market == "spread":
                        print(f"    {g['date']} {g['away_team']}@{g['home_team']}: "
                              f"{g['picked_team']} {g.get('line', 0):+.1f} -> "
                              f"{g.get('away_score')}-{g.get('home_score')} = {g['status']}")
                    else:
                        print(f"    {g['date']} {g['away_team']}@{g['home_team']}: "
                              f"{g['direction']} {g.get('line', 0)} -> "
                              f"total={g.get('total')} = {g['status']}")

        # Error breakdown
        if errors > 0:
            error_reasons = Counter(g.get("error") for g in grades if g["status"] == "ERROR")
            print(f"\n  Error breakdown:")
            for err, cnt in error_reasons.most_common(5):
                print(f"    {err}: {cnt}")


if __name__ == "__main__":
    main()
