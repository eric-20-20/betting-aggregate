"""Alternative NBA results provider for historical games.

This provider uses the nba_api LeagueGameFinder endpoint which has better
coverage for historical games than the standard scoreboard endpoint.

The standard scoreboard endpoint sometimes returns null team names for
older games, but LeagueGameFinder provides complete game data.

Features:
  - Date-based game fetch with caching per day
  - Game resolution by team abbreviations
  - Score lookup with FINAL status
  - Caches in data/cache/results/lgf_YYYY-MM-DD.json
"""
from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from nba_api.stats.endpoints import leaguegamefinder
except ImportError:
    leaguegamefinder = None

CACHE_DIR = Path("data/cache/results")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

SLEEP_BETWEEN_CALLS = 0.5

# In-memory cache for current session
CACHE_MEMO: Dict[str, List[Dict[str, Any]]] = {}

# Team abbreviation normalization
TEAM_NORM = {
    "BRK": "BKN",
    "PHO": "PHX",
    "CHO": "CHA",
    "NJN": "BKN",
    "NOH": "NOP",
    "NOK": "NOP",
}


def _norm_team(team: str) -> str:
    """Normalize team abbreviation."""
    team = (team or "").upper().strip()
    return TEAM_NORM.get(team, team)


def _cache_path(date_str: str) -> Path:
    """Get cache file path for a date."""
    return CACHE_DIR / f"lgf_{date_str}.json"


def _read_cache(date_str: str) -> Optional[List[Dict[str, Any]]]:
    """Read cached games for a date."""
    if date_str in CACHE_MEMO:
        return CACHE_MEMO[date_str]

    path = _cache_path(date_str)
    if path.exists():
        try:
            data = json.loads(path.read_text())
            CACHE_MEMO[date_str] = data
            return data
        except Exception:
            pass
    return None


def _write_cache(date_str: str, games: List[Dict[str, Any]]) -> None:
    """Write games to cache."""
    CACHE_MEMO[date_str] = games
    path = _cache_path(date_str)
    try:
        path.write_text(json.dumps(games, indent=2))
    except Exception:
        pass


def _parse_matchup(matchup: str) -> Tuple[str, str, bool]:
    """
    Parse matchup string to extract teams.

    Returns (team1, team2, is_home) where is_home indicates if team1 is home.
    """
    matchup = (matchup or "").strip()
    if " vs. " in matchup:
        parts = matchup.split(" vs. ")
        return parts[0].strip(), parts[1].strip(), True  # team1 is home
    elif " @ " in matchup:
        parts = matchup.split(" @ ")
        return parts[0].strip(), parts[1].strip(), False  # team1 is away
    return "", "", False


def get_games_for_date(date_str: str, refresh_cache: bool = False) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Fetch all games for a date using LeagueGameFinder.

    Args:
        date_str: Date in YYYY-MM-DD format
        refresh_cache: If True, ignore cache and fetch fresh data

    Returns:
        Tuple of (games list, metadata dict)
    """
    meta: Dict[str, Any] = {
        "date": date_str,
        "provider": "league_game_finder",
        "used_cache": False,
    }

    if leaguegamefinder is None:
        meta["error"] = "nba_api_not_installed"
        return [], meta

    # Check cache first
    if not refresh_cache:
        cached = _read_cache(date_str)
        if cached is not None:
            meta["used_cache"] = True
            meta["games_count"] = len(cached)
            return cached, meta

    # Convert date format
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        date_formatted = dt.strftime("%m/%d/%Y")  # LeagueGameFinder uses MM/DD/YYYY
    except ValueError:
        meta["error"] = "invalid_date_format"
        return [], meta

    # Rate limiting
    time.sleep(SLEEP_BETWEEN_CALLS)

    try:
        finder = leaguegamefinder.LeagueGameFinder(
            date_from_nullable=date_formatted,
            date_to_nullable=date_formatted,
            league_id_nullable="00",  # NBA
        )
        df = finder.get_data_frames()[0]
    except Exception as e:
        meta["error"] = f"api_error: {str(e)[:100]}"
        return [], meta

    if df.empty:
        meta["games_count"] = 0
        _write_cache(date_str, [])
        return [], meta

    # Group by game_id to get both teams
    games_by_id: Dict[str, Dict[str, Any]] = {}

    for _, row in df.iterrows():
        game_id = str(row.get("GAME_ID", ""))
        if not game_id:
            continue

        team, opponent, is_home = _parse_matchup(row.get("MATCHUP", ""))
        team = _norm_team(team)
        opponent = _norm_team(opponent)
        pts = int(row.get("PTS", 0) or 0)

        if game_id not in games_by_id:
            games_by_id[game_id] = {
                "game_id": game_id,
                "game_date": str(row.get("GAME_DATE", date_str)),
                "status": "FINAL",
            }

        game = games_by_id[game_id]
        if is_home:
            game["home_team"] = team
            game["away_team"] = opponent
            game["home_score"] = pts
        else:
            game["away_team"] = team
            game["home_team"] = opponent
            game["away_score"] = pts

    # Convert to list and filter complete games
    games = []
    for game in games_by_id.values():
        if all(k in game for k in ["home_team", "away_team", "home_score", "away_score"]):
            games.append(game)

    meta["games_count"] = len(games)
    meta["fetched_at"] = datetime.utcnow().isoformat()

    # Cache results
    _write_cache(date_str, games)

    return games, meta


def fetch_game_result(
    event_key: str,
    away: str,
    home: str,
    date_str: str,
    refresh_cache: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Fetch game result using LeagueGameFinder.

    Args:
        event_key: Event key (not used, for API compatibility)
        away: Away team abbreviation
        home: Home team abbreviation
        date_str: Date in YYYY-MM-DD format
        refresh_cache: If True, ignore cache

    Returns:
        Dict with away_score, home_score, status, or None if not found
    """
    away_n = _norm_team(away)
    home_n = _norm_team(home)

    games, meta = get_games_for_date(date_str, refresh_cache=refresh_cache)

    if not games:
        return {
            "status": "ERROR",
            "notes": meta.get("error", "no_games_for_date"),
            "games_info": meta,
        }

    # Find matching game
    for game in games:
        game_away = _norm_team(game.get("away_team", ""))
        game_home = _norm_team(game.get("home_team", ""))

        # Direct match
        if game_away == away_n and game_home == home_n:
            return {
                "away_score": game["away_score"],
                "home_score": game["home_score"],
                "status": "FINAL",
                "game_id": game.get("game_id"),
                "provider": "league_game_finder",
                "games_info": meta,
            }

        # Flipped match (in case away/home are swapped in our data)
        if game_away == home_n and game_home == away_n:
            return {
                "away_score": game["home_score"],
                "home_score": game["away_score"],
                "status": "FINAL",
                "game_id": game.get("game_id"),
                "provider": "league_game_finder",
                "match_orientation": "flipped",
                "games_info": meta,
            }

    # Not found
    return {
        "status": "ERROR",
        "notes": "game_not_found",
        "games_info": {
            **meta,
            "search": {"away": away, "home": home, "away_norm": away_n, "home_norm": home_n},
            "available_games": [f"{g['away_team']}@{g['home_team']}" for g in games],
        },
    }
