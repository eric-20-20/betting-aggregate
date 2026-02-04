"""NBA Stats provider using nba_api with caching (LEGACY; prefer nba_api+nba_cdn)."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import re

try:
    from nba_api.stats.endpoints import scoreboardv2, boxscoretraditionalv2, commonallplayers
    from nba_api.stats.library.parameters import LeagueID
except ImportError as exc:  # pragma: no cover
    raise RuntimeError("nba_api is required for RESULTS_PROVIDER=nba_stats") from exc

CACHE_DIR = Path("data/cache/results")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

SLEEP_BETWEEN_CALLS = 0.35
STATS = {"cache_hits": 0, "fetches": 0}

TEAM_ABBR_MAP = {
    "BRK": "BKN",
    "PHO": "PHX",
    "CHO": "CHA",
    "SAS": "SA",
    "GSW": "GS",
    "NYK": "NY",
}


def _sleep():
    time.sleep(SLEEP_BETWEEN_CALLS)


def _cache_path(name: str) -> Path:
    return CACHE_DIR / name


def _read_cache(path: Path) -> Optional[Any]:
    if path.exists():
        try:
            STATS["cache_hits"] += 1
            return json.loads(path.read_text())
        except Exception:
            return None
    return None


def _write_cache(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.write_text(json.dumps(payload))
    except Exception:
        pass


def normalize_team(code: str) -> str:
    c = code.upper()
    return TEAM_ABBR_MAP.get(c, c)


def _fetch_scoreboard(date_str: str, refresh: bool = False) -> Dict[str, Any]:
    path = _cache_path(f"nba_scoreboard_{date_str}.json")
    if not refresh:
        cached = _read_cache(path)
        if cached:
            return cached
    _sleep()
    sb = scoreboardv2.ScoreboardV2(game_date=date_str, league_id=LeagueID.nba)
    data = sb.get_dict()
    _write_cache(path, data)
    STATS["fetches"] += 1
    return data


def _games_from_scoreboard(date_str: str, refresh: bool = False) -> List[Dict[str, Any]]:
    data = _fetch_scoreboard(date_str, refresh=refresh)
    games = data.get("resultSets", [])[0] if data.get("resultSets") else {}
    headers = games.get("headers", [])
    rows = games.get("rowSet", [])
    out = []
    for row in rows:
        rec = dict(zip(headers, row))
        out.append(rec)
    return out


def resolve_game_id(date_str: str, away_team: str, home_team: str, refresh_cache: bool = False) -> Optional[str]:
    away_n = normalize_team(away_team)
    home_n = normalize_team(home_team)
    games = _games_from_scoreboard(date_str, refresh=refresh_cache)
    for g in games:
        if normalize_team(str(g.get("VISITORTEAMABBREVIATION", ""))) == away_n and normalize_team(
            str(g.get("HOMETEAMABBREVIATION", ""))
        ) == home_n:
            return str(g.get("GAME_ID"))
    return None


def get_game_score(game_id: str, date_str: str, refresh_cache: bool = False) -> Optional[Dict[str, Any]]:
    path = _cache_path(f"nba_boxscore_{game_id}.json")
    if not refresh_cache:
        cached = _read_cache(path)
        if cached and "game" in cached:
            return cached["game"]
    _sleep()
    box = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
    data = box.get_dict()
    STATS["fetches"] += 1
    _write_cache(path, data)
    info_sets = {rs["name"]: rs for rs in data.get("resultSets", [])}
    line = info_sets.get("GameInfo", {})
    if line.get("rowSet"):
        headers = line.get("headers", [])
        row = line["rowSet"][0]
        rec = dict(zip(headers, row))
        return {"home_score": rec.get("PTS_HOME"), "away_score": rec.get("PTS_AWAY")}
    # fallback: sum team box rows
    teams = info_sets.get("TeamStats", {})
    pts = {}
    for r in teams.get("rowSet", []):
        rec = dict(zip(teams.get("headers", []), r))
        pts[normalize_team(rec.get("TEAM_ABBREVIATION", ""))] = rec.get("PTS")
    if pts:
        # cannot assign home/away confidently; skip
        return None
    return None


def _build_player_index(refresh: bool = False) -> Dict[str, str]:
    path = _cache_path("nba_player_index.json")
    if not refresh:
        cached = _read_cache(path)
        if cached:
            return cached
    _sleep()
    allp = commonallplayers.CommonAllPlayers(is_only_current_season=0, league_id=LeagueID.nba)
    data = allp.get_dict()
    STATS["fetches"] += 1
    headers = data["resultSets"][0]["headers"]
    rows = data["resultSets"][0]["rowSet"]
    idx = {}
    for row in rows:
        rec = dict(zip(headers, row))
        pid = str(rec.get("PERSON_ID"))
        name = rec.get("DISPLAY_FIRST_LAST") or ""
        idx[_normalize_name(name)] = pid
    _write_cache(path, idx)
    return idx


def _normalize_name(name: str) -> str:
    name = name.lower()
    name = re.sub(r"[^a-z0-9 ]+", "", name)
    name = name.replace(" iii", "").replace(" jr", "").replace(" ii", "").replace(" sr", "")
    name = " ".join(name.split())
    return name


def _player_id_from_signal(player_id: str) -> Optional[str]:
    # expect NBA:slug or slug_like
    slug = player_id.replace("NBA:", "")
    slug = slug.replace("_", " ")
    return _normalize_name(slug)


def get_player_stats(game_id: str, player_hint: str, refresh_cache: bool = False) -> Optional[Dict[str, Any]]:
    path = _cache_path(f"nba_boxscore_{game_id}.json")
    data = _read_cache(path) if not refresh_cache else None
    if data is None:
        _sleep()
        box = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
        data = box.get_dict()
        STATS["fetches"] += 1
        _write_cache(path, data)
    rs_list = {rs["name"]: rs for rs in data.get("resultSets", [])}
    player_stats = rs_list.get("PlayerStats", {})
    headers = player_stats.get("headers", [])
    rows = player_stats.get("rowSet", [])
    idx = _build_player_index(refresh=False)
    target_norm = _player_id_from_signal(player_hint)
    if not target_norm:
        return None
    # attempt direct id match first
    if target_norm in idx:
        target_pid = idx[target_norm]
    else:
        target_pid = None
    for row in rows:
        rec = dict(zip(headers, row))
        if target_pid and str(rec.get("PLAYER_ID")) == str(target_pid):
            return rec
        name_norm = _normalize_name(rec.get("PLAYER_NAME", ""))
        if name_norm == target_norm:
            return rec
    return None
