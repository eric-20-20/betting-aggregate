#!/usr/bin/env python3
"""Grade BetQL prop backfill data directly without going through consensus pipeline.

This script:
1. Reads raw BetQL prop data from data/history/betql_game_props_raw/
2. Filters to 4+ star picks (eligible)
3. Grades each pick against actual player stats
4. Outputs a record summary

Usage:
    python3 scripts/grade_betql_backfill_props.py
    python3 scripts/grade_betql_backfill_props.py --min-stars 5
    python3 scripts/grade_betql_backfill_props.py --limit 100
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ensure repo root on path for src package imports
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.results import nba_provider


def load_backfill_props(
    data_dir: Path,
    min_stars: int = 4,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Load BetQL prop backfill data, filtered by star rating."""
    all_props = []

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
                    stars = row.get("rating_stars", 0)
                    if stars >= min_stars:
                        row["_source_file"] = jsonl_file.name
                        all_props.append(row)
                        if limit and len(all_props) >= limit:
                            return all_props
                except json.JSONDecodeError:
                    continue

    return all_props


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


def normalize_stat_key(stat: str) -> Optional[str]:
    """Normalize stat name to canonical key."""
    stat = (stat or "").lower().strip()
    stat_map = {
        "points": "points",
        "pts": "points",
        "rebounds": "rebounds",
        "reb": "rebounds",
        "rebs": "rebounds",
        "assists": "assists",
        "ast": "assists",
        "asts": "assists",
        "threes": "threes",
        "3pm": "threes",
        "three pointers made": "threes",
        "3-pointers made": "threes",
        "made threes": "threes",
        "steals": "steals",
        "stl": "steals",
        "blocks": "blocks",
        "blk": "blocks",
        "turnovers": "turnovers",
        "to": "turnovers",
        "tov": "turnovers",
        "pts + reb + ast": "pra",
        "pts+reb+ast": "pra",
        "points, rebounds & assists": "pra",
        "pts + ast": "pa",
        "pts+ast": "pa",
        "points + assists": "pa",
        "pts + reb": "pr",
        "pts+reb": "pr",
        "points + rebounds": "pr",
        "reb + ast": "ra",
        "reb+ast": "ra",
        "rebounds + assists": "ra",
    }
    return stat_map.get(stat)


def make_player_slug(player_name: str) -> str:
    """Convert player name to slug format."""
    import re
    name = player_name.lower().strip()
    # Remove Jr., Sr., III, etc.
    name = re.sub(r'\s+(jr\.?|sr\.?|iii|ii|iv)$', '', name, flags=re.IGNORECASE)
    # Replace spaces/hyphens with underscores
    name = re.sub(r'[\s\-]+', '_', name)
    # Remove non-alphanumeric except underscores
    name = re.sub(r'[^a-z0-9_]', '', name)
    return name


def parse_date_from_day_key(day_key: str) -> Optional[date]:
    """Parse date from day_key like NBA:2025:02:09."""
    import re
    m = re.match(r'NBA:(\d{4}):(\d{2}):(\d{2})', day_key)
    if m:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    return None


def extract_matchup_from_url(url: str) -> Tuple[Optional[str], Optional[str], Optional[date]]:
    """Extract teams and date from BetQL canonical URL.

    URL format: https://betql.co/nba/game-predictions/cleveland-cavaliers-vs-minnesota-timberwolves-02-10-2025
    """
    import re

    # Extract the game portion after /game-predictions/
    m = re.search(r'/game-predictions/([a-z0-9\-]+)-(\d{2})-(\d{2})-(\d{4})', url.lower())
    if not m:
        return None, None, None

    teams_slug = m.group(1)
    month = int(m.group(2))
    day = int(m.group(3))
    year = int(m.group(4))

    game_date = date(year, month, day)

    # Split by -vs-
    if '-vs-' not in teams_slug:
        return None, None, game_date

    parts = teams_slug.split('-vs-')
    if len(parts) != 2:
        return None, None, game_date

    # Team name to code mapping (handle multi-word names)
    team_slug_map = {
        "cleveland-cavaliers": "CLE",
        "minnesota-timberwolves": "MIN",
        "boston-celtics": "BOS",
        "miami-heat": "MIA",
        "los-angeles-lakers": "LAL",
        "la-lakers": "LAL",
        "los-angeles-clippers": "LAC",
        "la-clippers": "LAC",
        "golden-state-warriors": "GSW",
        "phoenix-suns": "PHX",
        "denver-nuggets": "DEN",
        "utah-jazz": "UTA",
        "portland-trail-blazers": "POR",
        "oklahoma-city-thunder": "OKC",
        "dallas-mavericks": "DAL",
        "houston-rockets": "HOU",
        "san-antonio-spurs": "SAS",
        "memphis-grizzlies": "MEM",
        "new-orleans-pelicans": "NOP",
        "sacramento-kings": "SAC",
        "atlanta-hawks": "ATL",
        "charlotte-hornets": "CHA",
        "chicago-bulls": "CHI",
        "indiana-pacers": "IND",
        "milwaukee-bucks": "MIL",
        "brooklyn-nets": "BKN",
        "new-york-knicks": "NYK",
        "philadelphia-76ers": "PHI",
        "toronto-raptors": "TOR",
        "washington-wizards": "WAS",
        "orlando-magic": "ORL",
        "detroit-pistons": "DET",
    }

    # In BetQL URLs, format is "home-vs-away"
    home_slug = parts[0].strip()
    away_slug = parts[1].strip()

    home_team = team_slug_map.get(home_slug)
    away_team = team_slug_map.get(away_slug)

    return away_team, home_team, game_date


def grade_prop(
    prop: Dict[str, Any],
    refresh_cache: bool = False,
) -> Dict[str, Any]:
    """Grade a single prop pick against actual player stats."""
    result = {
        "player_name": prop.get("player_name"),
        "stat": prop.get("stat"),
        "line": prop.get("line"),
        "direction": prop.get("direction"),
        "odds": prop.get("odds"),
        "rating_stars": prop.get("rating_stars"),
        "day_key": prop.get("day_key"),
        "away_team": prop.get("away_team"),
        "home_team": prop.get("home_team"),
        "canonical_url": prop.get("canonical_url"),
        "status": "PENDING",
        "stat_value": None,
        "error": None,
    }

    # Try to extract correct matchup from canonical URL first
    canonical_url = prop.get("canonical_url", "")
    away_team, home_team, game_date = extract_matchup_from_url(canonical_url)

    if not game_date:
        # Fallback to day_key
        day_key = prop.get("day_key", "")
        game_date = parse_date_from_day_key(day_key)

    if not game_date:
        result["status"] = "ERROR"
        result["error"] = "invalid_date"
        return result

    # Skip future games
    if game_date > date.today():
        result["status"] = "PENDING"
        result["error"] = "future_game"
        return result

    # Get teams from URL or fallback to raw fields
    if not away_team or not home_team:
        away_team = normalize_team_code(prop.get("away_team", ""))
        home_team = normalize_team_code(prop.get("home_team", ""))

    result["away_team_norm"] = away_team
    result["home_team_norm"] = home_team
    result["game_date"] = str(game_date)

    if not away_team or not home_team:
        result["status"] = "ERROR"
        result["error"] = "missing_teams"
        return result

    # Normalize stat
    stat_key = normalize_stat_key(prop.get("stat", ""))
    if not stat_key:
        result["status"] = "ERROR"
        result["error"] = f"unknown_stat:{prop.get('stat')}"
        return result
    result["stat_key"] = stat_key

    # Player slug
    player_name = prop.get("player_name", "")
    player_slug = make_player_slug(player_name)
    result["player_slug"] = player_slug

    # Direction
    direction = (prop.get("direction") or "").upper()
    if direction not in ("OVER", "UNDER"):
        result["status"] = "ERROR"
        result["error"] = f"invalid_direction:{direction}"
        return result

    # Line
    line = prop.get("line")
    if line is None:
        result["status"] = "ERROR"
        result["error"] = "missing_line"
        return result

    # Build event_key for provider using the extracted/normalized date
    event_key = f"NBA:{game_date.year:04d}:{game_date.month:02d}:{game_date.day:02d}:{away_team}@{home_team}"
    date_str = game_date.strftime("%Y-%m-%d")
    result["event_key"] = event_key

    # Try to get player stat
    try:
        stat_result = nba_provider.fetch_player_statline(
            event_key=event_key,
            player_id=player_slug,
            stat_key=stat_key,
            date_str=date_str,
            away=away_team,
            home=home_team,
            refresh_cache=refresh_cache,
        )

        if stat_result is None:
            result["status"] = "ERROR"
            result["error"] = "no_stat_result"
            return result

        # Check status from provider
        provider_status = stat_result.get("status", "")
        if provider_status == "ERROR":
            result["status"] = "ERROR"
            result["error"] = f"provider_error:{stat_result.get('notes', 'unknown')}"
            return result

        # The provider returns 'value' not 'stat_value'
        stat_value = stat_result.get("value")
        result["stat_value"] = stat_value
        result["provider"] = stat_result.get("provider")
        result["player_match"] = stat_result.get("match_method")
        result["player_name_matched"] = stat_result.get("player_name")

        if stat_value is None:
            result["status"] = "ERROR"
            result["error"] = "stat_value_none"
            return result

        # Grade it
        if direction == "OVER":
            if stat_value > line:
                result["status"] = "WIN"
            elif stat_value == line:
                result["status"] = "PUSH"
            else:
                result["status"] = "LOSS"
        else:  # UNDER
            if stat_value < line:
                result["status"] = "WIN"
            elif stat_value == line:
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
        return 0.0
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
    parser = argparse.ArgumentParser(description="Grade BetQL prop backfill data")
    parser.add_argument("--data-dir", default="data/history/betql_game_props_raw",
                       help="Directory with raw backfill JSONL files")
    parser.add_argument("--min-stars", type=int, default=4,
                       help="Minimum star rating to include (default: 4)")
    parser.add_argument("--limit", type=int, default=None,
                       help="Limit number of props to grade (for testing)")
    parser.add_argument("--refresh-cache", action="store_true",
                       help="Refresh provider cache")
    parser.add_argument("--output", type=str, default=None,
                       help="Output file for detailed grades (JSONL)")
    parser.add_argument("--verbose", action="store_true",
                       help="Show individual grades")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        print(f"Error: Data directory not found: {data_dir}")
        return

    print(f"Loading BetQL props from {data_dir} (min stars: {args.min_stars})...")
    props = load_backfill_props(data_dir, min_stars=args.min_stars, limit=args.limit)
    print(f"Loaded {len(props)} eligible props")

    if not props:
        print("No props to grade")
        return

    # Group by date for progress tracking
    by_date = defaultdict(list)
    for p in props:
        day_key = p.get("day_key", "unknown")
        by_date[day_key].append(p)

    print(f"Props span {len(by_date)} dates")

    # Grade all props
    print("\nGrading props...")
    all_grades = []
    status_counts = Counter()
    error_counts = Counter()

    total = len(props)
    for i, prop in enumerate(props):
        if (i + 1) % 100 == 0 or i == 0:
            print(f"  Progress: {i+1}/{total} ({100*(i+1)/total:.1f}%)", end="\r")

        grade = grade_prop(prop, refresh_cache=args.refresh_cache)
        all_grades.append(grade)
        status_counts[grade["status"]] += 1

        if grade["status"] == "ERROR":
            error_counts[grade.get("error", "unknown")] += 1

        if args.verbose and grade["status"] in ("WIN", "LOSS", "PUSH"):
            print(f"  {grade['player_name']} {grade['direction']} {grade['line']} {grade['stat']}: "
                  f"actual={grade['stat_value']} -> {grade['status']}")

    print(f"\n\nGrading complete!")

    # Summary
    print("\n" + "="*60)
    print("STATUS SUMMARY")
    print("="*60)
    for status, count in sorted(status_counts.items(), key=lambda x: -x[1]):
        pct = 100.0 * count / total
        print(f"  {status:12s}: {count:6d} ({pct:5.1f}%)")

    # Record calculation
    wins = status_counts.get("WIN", 0)
    losses = status_counts.get("LOSS", 0)
    pushes = status_counts.get("PUSH", 0)
    graded_total = wins + losses + pushes

    if graded_total > 0:
        win_pct = 100.0 * wins / (wins + losses) if (wins + losses) > 0 else 0

        # Calculate ROI
        total_units = 0.0
        unit_count = 0
        for grade in all_grades:
            if grade["status"] in ("WIN", "LOSS", "PUSH"):
                units = american_odds_to_units(grade.get("odds"), grade["status"])
                total_units += units
                unit_count += 1

        roi = 100.0 * total_units / unit_count if unit_count > 0 else 0

        print("\n" + "="*60)
        print("RECORD SUMMARY (BetQL 4+ Star Props)")
        print("="*60)
        print(f"  Record:     {wins}W - {losses}L - {pushes}P")
        print(f"  Win Rate:   {win_pct:.1f}%")
        print(f"  Net Units:  {total_units:+.2f}u")
        print(f"  ROI:        {roi:+.1f}%")
        print(f"  Bets:       {graded_total}")

    # Error breakdown
    if error_counts:
        print("\n" + "="*60)
        print("ERROR BREAKDOWN")
        print("="*60)
        for error, count in sorted(error_counts.items(), key=lambda x: -x[1])[:10]:
            print(f"  {error}: {count}")

    # Star breakdown
    print("\n" + "="*60)
    print("RECORD BY STAR RATING")
    print("="*60)
    by_stars = defaultdict(lambda: {"wins": 0, "losses": 0, "pushes": 0, "units": 0.0})
    for grade in all_grades:
        stars = grade.get("rating_stars", 0)
        status = grade.get("status", "")
        if status == "WIN":
            by_stars[stars]["wins"] += 1
            by_stars[stars]["units"] += american_odds_to_units(grade.get("odds"), "WIN")
        elif status == "LOSS":
            by_stars[stars]["losses"] += 1
            by_stars[stars]["units"] -= 1
        elif status == "PUSH":
            by_stars[stars]["pushes"] += 1

    for stars in sorted(by_stars.keys()):
        data = by_stars[stars]
        w, l, p = data["wins"], data["losses"], data["pushes"]
        total_bets = w + l + p
        if total_bets == 0:
            continue
        win_pct = 100.0 * w / (w + l) if (w + l) > 0 else 0
        roi = 100.0 * data["units"] / total_bets if total_bets > 0 else 0
        print(f"  {stars}-star: {w}W-{l}L-{p}P ({win_pct:.1f}% win, {roi:+.1f}% ROI, {total_bets} bets)")

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
