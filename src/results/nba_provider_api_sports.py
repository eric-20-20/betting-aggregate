"""API-Sports (API-NBA) results provider with caching, team and player mapping.

Env vars:
  RESULTS_PROVIDER=api_sports   # optional selector
  API_SPORTS_KEY                # required
  API_SPORTS_BASE               # default https://v1.basketball.api-sports.io (scheme added if missing)
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

import re

STATS = {"cache_hits": 0, "fetches": 0, "refreshes": 0, "bad_cache": 0}

try:
    import requests  # type: ignore
except ImportError:  # pragma: no cover
    requests = None  # type: ignore

CACHE_DIR = Path("data/cache/results")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_BASE = "https://v1.basketball.api-sports.io"
TEAM_MAP_PATH = CACHE_DIR / "team_map.json"
LEAGUE_MAP_PATH = CACHE_DIR / "league_map.json"


def _require_requests():
    if requests is None:
        raise RuntimeError("requests not installed; cannot call API-Sports")


def _headers() -> Dict[str, str]:
    key = os.environ.get("API_SPORTS_KEY")
    if not key:
        raise RuntimeError("env API_SPORTS_KEY is required for RESULTS_PROVIDER=api_sports")
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
        # if accidentally stored dict, inspect
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
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    year = dt.year
    if dt.month >= 10:
        return year
    return year - 1


def _normalize_team_code(code: str) -> str:
    return code.upper().replace("LA ", "LA").replace(" ", "")


def _load_team_map(season: int) -> Dict[str, Dict[str, Any]]:
    cache = _cache_read(TEAM_MAP_PATH) or {}
    season_map = cache.get(str(season))
    if season_map:
        return season_map
    league_id = get_nba_league_id()
    data = _get("teams", {"league": league_id, "season": season})
    season_map = {}
    if data and isinstance(data.get("response"), list):
        for team in data["response"]:
            code = _normalize_team_code(team.get("code") or team.get("name") or "")
            if code:
                season_map[code] = {"id": team.get("id"), "name": team.get("name"), "code": team.get("code")}
    # persist
    cache[str(season)] = season_map
    _cache_write(TEAM_MAP_PATH, cache)
    return season_map


def _team_id_for(code: str, season: int) -> Optional[int]:
    code_norm = _normalize_team_code(code)
    season_map = _load_team_map(season)
    if code_norm in season_map:
        return season_map[code_norm].get("id")
    # fallback aliases
    aliases = {
        "NY": "NYK",
        "GS": "GSW",
        "SA": "SAS",
        "NO": "NOP",
        "UTAH": "UTA",
        "PHO": "PHX",
        "LALAKERS": "LAL",
        "LACLIPPERS": "LAC",
    }
    if code_norm in aliases and aliases[code_norm] in season_map:
        return season_map[aliases[code_norm]].get("id")
    return None


def get_nba_league_id() -> int:
    cached = _cache_read(LEAGUE_MAP_PATH) or {}
    if "NBA" in cached:
        return cached["NBA"]
    data = _get("leagues", {})
    league_id = 12
    if data and isinstance(data.get("response"), list):
        for lg in data["response"]:
            name = (lg.get("name") or "").lower()
            country = (lg.get("country", {}).get("name") or lg.get("country") or "").lower()
            if "nba" in name and country in {"usa", "united states", "united states of america", "us"}:
                league_id = lg.get("id", league_id)
                break
    cached["NBA"] = league_id
    _cache_write(LEAGUE_MAP_PATH, cached)
    return league_id


def get_games_by_date(date_yyyy_mm_dd: str, refresh_cache: bool = False) -> Tuple[List[Dict[str, Any]], bool]:
    cache_path = CACHE_DIR / f"games_{date_yyyy_mm_dd}.json"
    cached = None if refresh_cache else _cache_read(cache_path)
    if cached is not None and not is_bad_cached_response(cached, expect_list=True):
        if isinstance(cached, list):
            return cached, True
    else:
        if cached is not None:
            STATS["bad_cache"] += 1
    season = _season_for_date(date_yyyy_mm_dd)
    league_id = get_nba_league_id()
    data = _get("games", {"date": date_yyyy_mm_dd, "league": league_id, "season": season})
    games = []
    if isinstance(data, dict) and isinstance(data.get("response"), list):
        games = data["response"]
    _cache_write(cache_path, games or [])
    STATS["refreshes"] += 1
    return games or [], False


LAST_GAMES_INFO: Dict[str, Any] = {}


def resolve_game_id(date_yyyy_mm_dd: str, away_team_code: str, home_team_code: str, refresh_cache: bool = False) -> Optional[int]:
    season = _season_for_date(date_yyyy_mm_dd)
    away_id = _team_id_for(away_team_code, season)
    home_id = _team_id_for(home_team_code, season)
    if not away_id or not home_id:
        return None
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
        if away.get("id") == away_id and home.get("id") == home_id:
            return g.get("id")
    return None


def get_game_score(game_id: int) -> Optional[Dict[str, Any]]:
    cache_path = CACHE_DIR / f"game_{game_id}.json"
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
    cache_path = CACHE_DIR / f"player_stats_{game_id}.json"
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
    idx_path = CACHE_DIR / f"player_index_{season}.json"
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
    index = build_player_index_for_game(game_id, season)
    key = _normalize_name(player_hint.replace("NBA:", ""))
    return index.get(key)
