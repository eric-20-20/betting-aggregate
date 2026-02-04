"""data.nba.net provider for game scores and player stats with caching."""

from __future__ import annotations

import json
import time
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .http_utils import http_get_with_retry  # type: ignore

CACHE_DIR = Path("data/cache/results")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

STATS = {"cache_hits": 0, "fetches": 0, "bad_cache": 0}
SLEEP = 0.3

TEAM_NORM = {
    "BRK": "BKN",
    "PHO": "PHX",
    "CHO": "CHA",
    "SAS": "SA",
}


def _sleep():
    time.sleep(SLEEP)


def _cache(path: Path, refresh: bool = False) -> Optional[Any]:
    if not refresh and path.exists():
        try:
            STATS["cache_hits"] += 1
            return json.loads(path.read_text())
        except Exception:
            STATS["bad_cache"] += 1
            return None
    return None


def _write(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.write_text(json.dumps(payload))
    except Exception:
        pass


def _fetch_json(url: str, debug: bool = False) -> Tuple[Optional[Dict[str, Any]], Optional[int], Optional[str]]:
    resp, err = http_get_with_retry(url, timeout=25, retries=2, backoff=0.6, allow_redirects=True)
    if resp is None:
        return None, None, str(err)
    status = resp.status_code
    try:
        data = resp.json()
    except Exception as exc:
        return None, status, f"json_error:{exc}"
    return data, status, None


def _yyyymmdd(date_str: str) -> str:
    if "-" in date_str:
        return date_str.replace("-", "")
    return date_str


def _norm_team(code: str) -> str:
    c = code.upper()
    return TEAM_NORM.get(c, c)


def _valid_scoreboard(data: Any) -> bool:
    return isinstance(data, dict) and "games" in data and isinstance(data.get("games"), list)


def fetch_game_result(event_key: str, away: str, home: str, date_str: str, refresh_cache: bool = False, debug: bool = False) -> Optional[Dict[str, Any]]:
    day = _yyyymmdd(date_str)
    sb_path = CACHE_DIR / f"nba_net_scoreboard_{date_str}.json"
    data = _cache(sb_path, refresh_cache)
    used_cache = data is not None
    if not _valid_scoreboard(data):
        urls = [
            f"https://data.nba.net/prod/v2/{day}/scoreboard.json",
            f"https://data.nba.net/10s/prod/v2/{day}/scoreboard.json",
        ]
        data = None
        last_status = None
        last_err = None
        for u in urls:
            payload, status, err = _fetch_json(u, debug=debug)
            last_status, last_err = status, err
            if _valid_scoreboard(payload):
                data = payload
                _write(sb_path, data)
                break
        if data is None:
            if debug:
                print(f"[data.nba.net] scoreboard fetch failed date={date_str} last_status={last_status} err={last_err}")
            return None
    games = data.get("games") or []
    away_n = _norm_team(away)
    home_n = _norm_team(home)
    target = None
    for g in games:
        if _norm_team(g.get("vTeam", {}).get("triCode", "")) == away_n and _norm_team(g.get("hTeam", {}).get("triCode", "")) == home_n:
            target = g
            break
    if not target:
        return None
    status_num = int(target.get("statusNum", 0) or 0)
    game_id = target.get("gameId")
    home_score = int(target.get("hTeam", {}).get("score", 0) or 0)
    away_score = int(target.get("vTeam", {}).get("score", 0) or 0)
    status = "FINAL" if status_num >= 3 else "PENDING"
    return {"game_id": game_id, "home_score": home_score, "away_score": away_score, "status": status, "used_cache": used_cache}


def _build_slug(first: str, last: str) -> str:
    nm = f"{first} {last}".lower()
    nm = re.sub(r"[^a-z0-9 ]+", "", nm)
    return nm.replace(" ", "_")


def _player_index_path() -> Path:
    return CACHE_DIR / "nba_net_player_index.json"


def _load_player_index() -> Dict[str, str]:
    path = _player_index_path()
    data = _cache(path, refresh=False)
    if isinstance(data, dict):
        return data
    return {}


def _save_player_index(idx: Dict[str, str]) -> None:
    _write(_player_index_path(), idx)


STAT_MAP = {
    "points": "points",
    "rebounds": "totReb",
    "assists": "assists",
    "threes": "tpm",
    "steals": "steals",
    "blocks": "blocks",
    "turnovers": "turnovers",
}


def fetch_scoreboard_with_meta(date_str: str, refresh_cache: bool = False, verbose: bool = False):
    day = _yyyymmdd(date_str)
    sb_path = CACHE_DIR / f"nba_net_scoreboard_{date_str}.json"
    data = _cache(sb_path, refresh_cache)
    used_cache = data is not None and _valid_scoreboard(data)
    tried = 0
    last_status = None
    last_err = None
    urls = [
        f"https://data.nba.net/prod/v2/{day}/scoreboard.json",
        f"https://data.nba.net/10s/prod/v2/{day}/scoreboard.json",
    ]
    if not _valid_scoreboard(data):
        for u in urls:
            tried += 1
            payload, status, err = _fetch_json(u, debug=verbose)
            last_status, last_err = status, err
            if verbose:
                print(f"[http] url={u} status={status} err={err}")
                if status and 200 <= status < 300:
                    print(f"[http] content-type={payload.get('contentType') if isinstance(payload, dict) else 'n/a'}")
            if _valid_scoreboard(payload):
                data = payload
                _write(sb_path, data)
                STATS["fetches"] += 1
                used_cache = False
                break
    meta = {"used_cache": used_cache, "tried": tried or (1 if used_cache else 0), "last_status": last_status, "error": last_err}
    if not _valid_scoreboard(data):
        return None, meta
    return data, meta


def fetch_player_statline(event_key: str, player_id: str, stat_key: str, date_str: str, away: str, home: str, refresh_cache: bool = False) -> Optional[Dict[str, Any]]:
    day = _yyyymmdd(date_str)
    game_id = None
    # require game result first to get game_id
    game_res = fetch_game_result(event_key, away, home, date_str, refresh_cache)
    if not game_res or not game_res.get("game_id"):
        return None
    game_id = game_res["game_id"]
    bx_path = CACHE_DIR / f"nba_net_boxscore_{date_str}_{game_id}.json"
    data = _cache(bx_path, refresh_cache)
    if data is None:
        _sleep()
        data = _get(f"https://data.nba.net/prod/v1/{day}/{game_id}_boxscore.json")
        if data is None:
            return None
        _write(bx_path, data)
    stats = data.get("stats") or {}
    players: List[Dict[str, Any]] = stats.get("activePlayers") or []
    idx = _load_player_index()
    target_slug = player_id.replace("NBA:", "").lower()
    target_slug = target_slug.replace("-", "_")
    match_pid = idx.get(target_slug)
    def norm_name(p):
        return _build_slug(p.get("firstName", ""), p.get("lastName", ""))
    if not match_pid:
        for p in players:
            if norm_name(p) == target_slug:
                match_pid = p.get("personId")
                break
    if not match_pid:
        # try fuzzy contains
        for p in players:
            if target_slug in norm_name(p):
                match_pid = p.get("personId")
                break
    if match_pid:
        idx[target_slug] = str(match_pid)
        _save_player_index(idx)
    target_player = None
    for p in players:
        if str(p.get("personId")) == str(match_pid):
            target_player = p
            break
    if not target_player:
        return None
    nba_field = STAT_MAP.get(stat_key)
    if nba_field is None:
        return None
    if nba_field not in target_player:
        return None
    try:
        val = float(target_player.get(nba_field))
    except Exception:
        return None
    return {"value": val, "player_id": str(match_pid), "game_id": game_id}
