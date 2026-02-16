"""NCAAB (College Basketball) results provider using API-Sports.

This provider fetches game scores and player stats for college basketball grading.
It reuses the API-Sports infrastructure from the NBA provider.

Env vars:
  API_SPORTS_KEY                # required for API-Sports access
  NCAAB_LEAGUE_ID              # optional, auto-discovered if not set
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

STATS = {"cache_hits": 0, "fetches": 0, "refreshes": 0, "bad_cache": 0}

try:
    import requests  # type: ignore
except ImportError:  # pragma: no cover
    requests = None  # type: ignore

CACHE_DIR = Path("data/cache/results/ncaab")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_BASE = "https://v1.basketball.api-sports.io"
TEAM_MAP_PATH = CACHE_DIR / "ncaab_team_map.json"
LEAGUE_MAP_PATH = CACHE_DIR / "ncaab_league_map.json"


def _require_requests():
    if requests is None:
        raise RuntimeError("requests not installed; cannot call API-Sports")


def _headers() -> Dict[str, str]:
    key = os.environ.get("API_SPORTS_KEY")
    if not key:
        raise RuntimeError("env API_SPORTS_KEY is required for NCAAB results provider")
    return {
        "x-rapidapi-key": key,
        "x-rapidapi-host": os.environ.get("API_SPORTS_HOST", "v1.basketball.api-sports.io"),
        "Accept": "application/json",
    }


def _base_url() -> str:
    raw = os.environ.get("API_SPORTS_BASE", DEFAULT_BASE)
    if raw.startswith("http"):
        base = raw
    else:
        base = f"https://{raw}"
    return base.rstrip("/")


def _cache_read(path: Path) -> Optional[Any]:
    if path.exists():
        try:
            STATS["cache_hits"] += 1
            return json.loads(path.read_text())
        except Exception:
            return None
    return None


def _cache_write(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.write_text(json.dumps(payload))
    except Exception:
        pass


def is_bad_cached_response(payload: Any, expect_list: bool = False) -> bool:
    if payload is None:
        return True
    if isinstance(payload, dict) and {"home_score", "away_score"} <= set(payload.keys()):
        return False
    if expect_list:
        if isinstance(payload, list):
            return len(payload) == 0
    if not isinstance(payload, dict) and not isinstance(payload, list):
        return True
    if isinstance(payload, dict):
        if "errors" in payload and payload.get("errors"):
            return True
        if "message" in payload and "unauthorized" in str(payload["message"]).lower():
            return True
        if "response" not in payload and "results" not in payload:
            return True
        resp = payload.get("response")
        res = payload.get("results")
        errs = payload.get("errors")
        if resp == [] and (res == 0 or res is None):
            if errs:
                return True
    return False


def _get(url_path: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    _require_requests()
    url = f"{_base_url()}/{url_path.lstrip('/')}"
    for attempt in range(2):  # 1 retry
        try:
            resp = requests.get(url, params=params, headers=_headers(), timeout=10)
            if resp.status_code != 200:
                time.sleep(0.5)
                continue
            data = resp.json()
            if isinstance(data, dict):
                STATS["fetches"] += 1
                return data
        except Exception:
            time.sleep(0.5)
            continue
    return None


def _season_for_date(date_str: str) -> int:
    """College basketball season spans fall-spring, e.g. 2025-2026 season."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    year = dt.year
    # NCAAB season starts in November, ends in April
    if dt.month >= 10:
        return year
    return year - 1


def _normalize_team_code(code: str) -> str:
    return code.upper().replace(" ", "")


def get_ncaab_league_id() -> Optional[int]:
    """Get the NCAA Basketball league ID from API-Sports.

    Returns None if not found, as NCAAB coverage varies by API-Sports subscription.
    """
    # Check for explicit env var override
    env_id = os.environ.get("NCAAB_LEAGUE_ID")
    if env_id:
        return int(env_id)

    cached = _cache_read(LEAGUE_MAP_PATH) or {}
    if "NCAAB" in cached:
        return cached["NCAAB"]

    # Try to discover NCAAB league
    data = _get("leagues", {})
    league_id = None
    if data and isinstance(data.get("response"), list):
        for lg in data["response"]:
            name = (lg.get("name") or "").lower()
            country = (lg.get("country", {}).get("name") or lg.get("country") or "").lower()
            # Look for NCAA/College Basketball leagues
            if any(term in name for term in ["ncaa", "college", "ncaab"]) and country in {"usa", "united states", "us"}:
                league_id = lg.get("id")
                break

    if league_id:
        cached["NCAAB"] = league_id
        _cache_write(LEAGUE_MAP_PATH, cached)

    return league_id


def _load_team_map(season: int) -> Dict[str, Dict[str, Any]]:
    """Load team mapping for NCAAB season."""
    cache = _cache_read(TEAM_MAP_PATH) or {}
    season_map = cache.get(str(season))
    if season_map:
        return season_map

    league_id = get_ncaab_league_id()
    if not league_id:
        return {}

    data = _get("teams", {"league": league_id, "season": season})
    season_map = {}
    if data and isinstance(data.get("response"), list):
        for team in data["response"]:
            code = _normalize_team_code(team.get("code") or team.get("name") or "")
            if code:
                season_map[code] = {"id": team.get("id"), "name": team.get("name"), "code": team.get("code")}

    cache[str(season)] = season_map
    _cache_write(TEAM_MAP_PATH, cache)
    return season_map


def _team_id_for(code: str, season: int) -> Optional[int]:
    code_norm = _normalize_team_code(code)
    season_map = _load_team_map(season)
    if code_norm in season_map:
        return season_map[code_norm].get("id")
    return None


def get_games_by_date(date_yyyy_mm_dd: str, refresh_cache: bool = False) -> Tuple[List[Dict[str, Any]], bool]:
    """Get all NCAAB games for a given date."""
    cache_path = CACHE_DIR / f"ncaab_games_{date_yyyy_mm_dd}.json"
    cached = None if refresh_cache else _cache_read(cache_path)
    if cached is not None and not is_bad_cached_response(cached, expect_list=True):
        if isinstance(cached, list):
            return cached, True
    elif cached is not None:
        STATS["bad_cache"] += 1

    league_id = get_ncaab_league_id()
    if not league_id:
        return [], False

    season = _season_for_date(date_yyyy_mm_dd)
    data = _get("games", {"date": date_yyyy_mm_dd, "league": league_id, "season": season})
    games = []
    if isinstance(data, dict) and isinstance(data.get("response"), list):
        games = data["response"]

    _cache_write(cache_path, games or [])
    STATS["refreshes"] += 1
    return games or [], False


LAST_GAMES_INFO: Dict[str, Any] = {}


def resolve_game_id(date_yyyy_mm_dd: str, away_team_code: str, home_team_code: str, refresh_cache: bool = False) -> Optional[int]:
    """Resolve a game ID given date and team codes."""
    season = _season_for_date(date_yyyy_mm_dd)
    away_id = _team_id_for(away_team_code, season)
    home_id = _team_id_for(home_team_code, season)

    # If we can't map team IDs, try matching by team name
    games, used_cache = get_games_by_date(date_yyyy_mm_dd, refresh_cache=refresh_cache)
    global LAST_GAMES_INFO
    LAST_GAMES_INFO = {
        "date": date_yyyy_mm_dd,
        "games_count": len(games),
        "used_cache": used_cache,
    }

    for g in games:
        teams = g.get("teams") or {}
        away = teams.get("visitors") or teams.get("away") or {}
        home = teams.get("home") or {}

        # Try ID match first
        if away_id and home_id:
            if away.get("id") == away_id and home.get("id") == home_id:
                return g.get("id")

        # Fallback to name matching
        away_name = (away.get("name") or "").upper()
        home_name = (home.get("name") or "").upper()
        away_code_api = (away.get("code") or "").upper()
        home_code_api = (home.get("code") or "").upper()

        if (away_team_code.upper() in away_name or away_team_code.upper() == away_code_api) and \
           (home_team_code.upper() in home_name or home_team_code.upper() == home_code_api):
            return g.get("id")

    return None


def get_game_score(game_id: int) -> Optional[Dict[str, Any]]:
    """Get final score for a game."""
    cache_path = CACHE_DIR / f"ncaab_game_{game_id}.json"
    cached = _cache_read(cache_path)
    if cached is not None and not is_bad_cached_response(cached):
        return cached
    elif cached is not None:
        STATS["bad_cache"] += 1

    data = _get("games", {"id": game_id})
    if not data or not isinstance(data.get("response"), list) or not data["response"]:
        return None

    game = data["response"][0]
    scores = game.get("scores") or game.get("score") or {}
    home_score = scores.get("home", {}).get("total") if isinstance(scores.get("home"), dict) else scores.get("home")
    away_score = scores.get("visitors", {}).get("total") if isinstance(scores.get("visitors"), dict) else scores.get("away")

    result = {
        "home_score": home_score,
        "away_score": away_score,
        "status": (game.get("status") or {}).get("long") if isinstance(game.get("status"), dict) else game.get("status"),
        "date": game.get("date", {}).get("start") if isinstance(game.get("date"), dict) else game.get("date"),
    }
    _cache_write(cache_path, result)
    return result


def get_player_stats(game_id: int) -> List[Dict[str, Any]]:
    """Get player stats for a game."""
    cache_path = CACHE_DIR / f"ncaab_player_stats_{game_id}.json"
    cached = _cache_read(cache_path)
    if cached is not None and not is_bad_cached_response(cached, expect_list=True):
        return cached
    elif cached is not None:
        STATS["bad_cache"] += 1

    data = _get("players", {"game": game_id})
    stats = data.get("response") if isinstance(data, dict) else []
    _cache_write(cache_path, stats or [])
    return stats or []


def _normalize_name(name: str) -> str:
    name = name.lower()
    name = re.sub(r"[^a-z0-9 ]+", "", name)
    name = name.replace(" iii", "").replace(" jr", "").replace(" ii", "").replace(" sr", "")
    name = " ".join(name.split())
    return name


def build_player_index_for_game(game_id: int, season: str) -> Dict[str, int]:
    """Build player name -> ID index for a game."""
    idx_path = CACHE_DIR / f"ncaab_player_index_{season}.json"
    cache = _cache_read(idx_path) or {}
    if str(game_id) in cache.get("_built_for", []):
        return cache.get("index", {})

    stats = get_player_stats(game_id)
    index = cache.get("index", {})
    for entry in stats:
        player = entry.get("player") or {}
        pid = player.get("id")
        name = player.get("name")
        if pid and name:
            index[_normalize_name(name)] = pid

    cache["index"] = index
    built_for = cache.get("_built_for", [])
    if game_id not in built_for:
        built_for.append(game_id)
    cache["_built_for"] = built_for
    _cache_write(idx_path, cache)
    return index


def find_player_id(game_id: int, season: str, player_hint: str) -> Optional[int]:
    """Find player ID by name hint."""
    index = build_player_index_for_game(game_id, season)
    key = _normalize_name(player_hint.replace("NCAAB:", ""))
    return index.get(key)


# High-level grading functions

def fetch_game_result(
    event_key: str,
    away: str,
    home: str,
    date_str: str,
    refresh_cache: bool = False
) -> Optional[Dict[str, Any]]:
    """Fetch game result for grading.

    Args:
        event_key: Event key like NCAAB:2026:02:15:DUKE@UNC
        away: Away team code
        home: Home team code
        date_str: Date string YYYY-MM-DD
        refresh_cache: Force refresh cached data

    Returns:
        Dict with home_score, away_score, status or None
    """
    game_id = resolve_game_id(date_str, away, home, refresh_cache=refresh_cache)
    if not game_id:
        return None
    return get_game_score(game_id)


def fetch_player_statline(
    event_key: str,
    player_id: str,
    stat_key: str,
    date_str: str,
    away: str,
    home: str,
) -> Optional[Dict[str, Any]]:
    """Fetch player stats for prop grading.

    Args:
        event_key: Event key
        player_id: Player identifier
        stat_key: Stat to fetch (points, rebounds, etc.)
        date_str: Game date
        away: Away team code
        home: Home team code

    Returns:
        Dict with stat value or None
    """
    game_id = resolve_game_id(date_str, away, home)
    if not game_id:
        return None

    season = str(_season_for_date(date_str))
    api_player_id = find_player_id(game_id, season, player_id)
    if not api_player_id:
        return None

    stats = get_player_stats(game_id)
    for entry in stats:
        player = entry.get("player") or {}
        if player.get("id") == api_player_id:
            # Map stat_key to API field
            stat_mapping = {
                "points": "points",
                "rebounds": "totReb",
                "assists": "assists",
                "steals": "steals",
                "blocks": "blocks",
                "threes_made": "tpm",
            }
            api_stat = stat_mapping.get(stat_key)
            if api_stat:
                return {"value": entry.get(api_stat)}

    return None


def provider_info() -> Dict[str, Any]:
    """Return provider metadata for logging/debugging."""
    key = os.environ.get("API_SPORTS_KEY")
    return {
        "provider": "ncaab_api_sports",
        "base": _base_url(),
        "key_present": bool(key),
    }
