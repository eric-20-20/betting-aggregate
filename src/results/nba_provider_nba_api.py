"""NBA results provider using nba_api.stats endpoints (ScoreboardV2/BoxScoreTraditionalV2).

Features:
  - Date-based scoreboard fetch with caching per day.
  - Game resolution by team abbreviations.
  - Score lookup with FINAL / IN_PROGRESS / SCHEDULED status.
  - Player prop stats via boxscore with per-game player index.
  - Cache files live in data/cache/results and honor --refresh-cache semantics.
"""

from __future__ import annotations

import json
import re
import time
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

try:
    from nba_api.stats.endpoints import scoreboardv2, boxscoretraditionalv2
    from nba_api.stats.library.parameters import LeagueID
except ImportError as exc:  # pragma: no cover
    raise RuntimeError("nba_api is required for RESULTS_PROVIDER=nba_api") from exc

CACHE_DIR = Path("data/cache/results")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

SLEEP_BETWEEN_CALLS = 0.25

STATS: Dict[str, int] = {
    "cache_hits_file": 0,
    "cache_hits_mem": 0,
    "fetches": 0,
    "refreshes": 0,
    "poison_retry": 0,
    "box_cache_hits_file": 0,
    "box_cache_hits_mem": 0,
    "box_fetches": 0,
    "box_refreshes": 0,
    # detailed telemetry
    "cache_hit_mem": 0,
    "cache_hit_file": 0,
    "http_fetch_scoreboard": 0,
    "refresh_attempt_scoreboard": 0,
    "http_error_scoreboard": 0,
}

TEAM_ABBR_MAP = {
    "BRK": "BKN",
    "PHO": "PHX",
    "CHO": "CHA",
    "SA": "SAS",
}

# in-memory caches per run
SCOREBOARD_MEMO: Dict[str, Dict[str, Any]] = {}
BOX_MEMO: Dict[str, Dict[str, Any]] = {}
CACHE_CDN: Dict[str, Dict[str, Any]] = {}
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"


def _verify_ssl() -> bool:
    return os.environ.get("NBA_SSL_NO_VERIFY") != "1"


def _http_headers() -> Dict[str, str]:
    return {
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://stats.nba.com/",
        "Origin": "https://stats.nba.com",
        "x-nba-stats-origin": "stats",
        "x-nba-stats-token": "true",
        "Connection": "keep-alive",
    }


def _http_get(url: str, params: Dict[str, Any], meta: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    meta.update({"url": url, "params": params})
    try:
        start = time.time()
        resp = requests.get(
            url,
            params=params,
            headers=_http_headers(),
            timeout=(5, 15),  # connect, read
            verify=_verify_ssl(),
        )
        meta["elapsed_sec"] = round(time.time() - start, 3)
        meta["status_code"] = resp.status_code
        meta["applied_headers"] = True
        meta["body_preview"] = resp.text[:200] if resp.text else ""
        meta["body_is_json"] = False
        if resp.status_code != 200:
            return None
        try:
            data = resp.json()
            meta["body_is_json"] = True
            return data
        except Exception as exc:
            meta["json_error"] = str(exc)
            return None
    except Exception as exc:
        meta["exc_type"] = type(exc).__name__
        meta["exc_msg"] = str(exc)
        return None


def _sleep() -> None:
    time.sleep(SLEEP_BETWEEN_CALLS)


def _norm_team(code: str) -> str:
    return TEAM_ABBR_MAP.get(code.upper(), code.upper())


def _format_date(date_str: str) -> str:
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%m/%d/%Y")
    except Exception:
        return date_str


def _scoreboard_cache_path(date_str: str) -> Path:
    return CACHE_DIR / f"nba_api_scoreboard_{date_str}.json"


def _boxscore_cache_path(date_str: str, game_id: str) -> Path:
    safe_gid = str(game_id)
    return CACHE_DIR / f"nba_api_boxscore_{date_str}_{safe_gid}.json"


def _read_cache(path: Path) -> Optional[Any]:
    if path.exists():
        try:
            payload = json.loads(path.read_text())
            STATS["cache_hits_file"] += 1
            return payload
        except Exception:
            return None
    return None


def _write_cache_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp.write_text(json.dumps(payload))
        tmp.replace(path)
    except Exception:
        try:
            tmp.unlink(missing_ok=True)  # type: ignore[arg-type]
        except Exception:
            pass


def _is_poison_scoreboard(payload: Any) -> bool:
    if payload is None:
        return True
    if isinstance(payload, dict) and payload.get("message"):
        return True
    if isinstance(payload, dict) and payload.get("resultSets"):
        # valid schema, even if zero games
        return False
    return True


def _is_poison_boxscore(payload: Any) -> bool:
    if payload is None:
        return True
    if isinstance(payload, dict) and payload.get("message"):
        return True
    if isinstance(payload, dict) and payload.get("resultSets"):
        return False
    return True


def get_scoreboard(date_str: str, refresh_cache: bool = False) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    """
    Return scoreboard payload and meta. Uses in-memory memo and file cache.
    """
    meta = {
        "date": date_str,
        "used_cache_mem": False,
        "used_cache_file": False,
        "fetch_failed": False,
    }
    # memo always wins if present, even when refresh_cache is True (avoid repeat fetches in one run)
    if date_str in SCOREBOARD_MEMO:
        STATS["cache_hits_mem"] += 1
        STATS["cache_hit_mem"] += 1
        meta["used_cache_mem"] = True
        return SCOREBOARD_MEMO[date_str], meta

    path = _scoreboard_cache_path(date_str)
    data: Optional[Dict[str, Any]] = None

    if not refresh_cache:
        cached = _read_cache(path)
        if cached is not None and not _is_poison_scoreboard(cached):
            meta["used_cache_file"] = True
            data = cached
            STATS["cache_hits_file"] += 1
            STATS["cache_hit_file"] += 1

    if data is None:
        if refresh_cache:
            STATS["refreshes"] += 1
            STATS["refresh_attempt_scoreboard"] += 1
        try:
            _sleep()
            STATS["fetches"] += 1
            STATS["http_fetch_scoreboard"] += 1
            sb = scoreboardv2.ScoreboardV2(game_date=_format_date(date_str), league_id=LeagueID.nba, timeout=3)
            data = sb.get_dict()
        except Exception:
            meta["fetch_failed"] = True
            data = None

        if data is not None and not _is_poison_scoreboard(data):
            _write_cache_atomic(path, data)
        elif data is None:
            meta["fetch_failed"] = True
            data = {"resultSets": []}

        if meta.get("fetch_failed"):
            STATS["http_error_scoreboard"] += 1

    if data is not None:
        SCOREBOARD_MEMO[date_str] = data
    return data, meta


def _parse_games(scoreboard: Dict[str, Any]) -> List[Dict[str, Any]]:
    result_sets = scoreboard.get("resultSets") or []
    rs_by_name = {rs.get("name"): rs for rs in result_sets if isinstance(rs, dict)}
    header = rs_by_name.get("GameHeader") or {}
    linescore = rs_by_name.get("LineScore") or {}
    header_rows = header.get("rowSet") or []
    header_cols = header.get("headers") or []
    line_rows = linescore.get("rowSet") or []
    line_cols = linescore.get("headers") or []

    home_map: Dict[str, Dict[str, Any]] = {}
    for row in header_rows:
        rec = dict(zip(header_cols, row))
        gid = str(rec.get("GAME_ID"))
        home_map[gid] = {
            "game_id": gid,
            "home_team_id": rec.get("HOME_TEAM_ID"),
            "away_team_id": rec.get("VISITOR_TEAM_ID"),
            "status_text": rec.get("GAME_STATUS_TEXT"),
            "status_id": rec.get("GAME_STATUS_ID"),
            "live_period": rec.get("LIVE_PERIOD"),
        }
    for row in line_rows:
        rec = dict(zip(line_cols, row))
        gid = str(rec.get("GAME_ID"))
        if gid not in home_map:
            continue
        if rec.get("TEAM_ID") == home_map[gid].get("home_team_id"):
            home_map[gid]["home_team_abbrev"] = _norm_team(rec.get("TEAM_ABBREVIATION", ""))
            home_map[gid]["home_score"] = rec.get("PTS")
        elif rec.get("TEAM_ID") == home_map[gid].get("away_team_id"):
            home_map[gid]["away_team_abbrev"] = _norm_team(rec.get("TEAM_ABBREVIATION", ""))
            home_map[gid]["away_score"] = rec.get("PTS")
    games: List[Dict[str, Any]] = []
    for gid, rec in home_map.items():
        status_id = int(rec.get("status_id") or 0)
        if status_id == 3:
            status = "FINAL"
        elif status_id == 2:
            status = "IN_PROGRESS"
        else:
            status = "SCHEDULED"
        games.append(
            {
                "game_id": gid,
                "home_team_abbrev": rec.get("home_team_abbrev"),
                "away_team_abbrev": rec.get("away_team_abbrev"),
                "home_score": rec.get("home_score"),
                "away_score": rec.get("away_score"),
                "status": status,
                "status_text": rec.get("status_text"),
                "live_period": rec.get("live_period"),
            }
        )
    return games


def get_games_for_date(date_str: str, refresh_cache: bool = False) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    data, meta = get_scoreboard(date_str, refresh_cache=refresh_cache)
    games = _parse_games(data) if isinstance(data, dict) else []
    meta.update({"games_count": len(games)})
    return games, meta


def resolve_game_id(date_str: str, away: str, home: str, refresh_cache: bool = False) -> Optional[str]:
    games, meta = get_games_for_date(date_str, refresh_cache=refresh_cache)
    away_n = _norm_team(away)
    home_n = _norm_team(home)
    for g in games:
        if _norm_team(str(g.get("away_team_abbrev") or "")) == away_n and _norm_team(str(g.get("home_team_abbrev") or "")) == home_n:
            return str(g.get("game_id"))
    return None


def get_game_score(game_id: str, date_str: str, refresh_cache: bool = False) -> Optional[Dict[str, Any]]:
    games, meta = get_games_for_date(date_str, refresh_cache=refresh_cache)
    for g in games:
        if str(g.get("game_id")) == str(game_id):
            res = {
                "home_score": g.get("home_score"),
                "away_score": g.get("away_score"),
                "status": g.get("status"),
                "games_info": {
                    "date": date_str,
                    "games_count": len(games),
                    "used_cache_mem": meta.get("used_cache_mem"),
                    "used_cache_file": meta.get("used_cache_file"),
                    "fetch_failed": meta.get("fetch_failed"),
                },
                "provider": "nba_api",
            }
            return res
    return None


STAT_KEY_MAP = {
    "points": "PTS",
    "rebounds": "REB",
    "assists": "AST",
    "threes": "FG3M",
    "steals": "STL",
    "blocks": "BLK",
    "turnovers": "TOV",
}


def _slugify_player(val: str) -> str:
    val = val.replace("NBA:", "")
    val = val.lower()
    val = re.sub(r"[^a-z0-9]+", "_", val)
    val = re.sub(r"_+", "_", val)
    return val.strip("_")


def _resultset_to_dict_rows(payload: Dict[str, Any], wanted_names: Optional[List[str]] = None) -> Dict[str, List[Dict[str, Any]]]:
    """
    Convert nba_api resultSets payload to {name: [dict rows]} for easy access.
    Handles cases where resultSets is list, dict, or singular resultSet.
    """
    out: Dict[str, List[Dict[str, Any]]] = {}
    rs_container = payload.get("resultSets") or payload.get("resultSet")
    if isinstance(rs_container, dict):
        rs_iter = [rs_container]
    elif isinstance(rs_container, list):
        rs_iter = rs_container
    else:
        return out
    for rs in rs_iter:
        if not isinstance(rs, dict):
            continue
        name = rs.get("name")
        if wanted_names and name not in wanted_names:
            continue
        headers = rs.get("headers") or []
        rows = rs.get("rowSet") or []
        dict_rows: List[Dict[str, Any]] = []
        for row in rows:
            try:
                dict_rows.append(dict(zip(headers, row)))
            except Exception:
                continue
        if name:
            out[name] = dict_rows
    return out


def _fetch_boxscore(game_id: str, date_str: str, refresh_cache: bool = False) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    meta = {
        "game_id": game_id,
        "date": date_str,
        "used_cache_mem": False,
        "used_cache_file": False,
        "fetch_failed": False,
        "box_url": f"https://stats.nba.com/stats/boxscoretraditionalv2",
    }
    memo_key = f"{date_str}:{game_id}"
    if memo_key in BOX_MEMO:
        STATS["box_cache_hits_mem"] += 1
        meta["used_cache_mem"] = True
        return BOX_MEMO[memo_key], meta

    path = _boxscore_cache_path(date_str, game_id)
    data: Optional[Dict[str, Any]] = None
    if not refresh_cache:
        cached = _read_cache(path)
        if cached is not None and not _is_poison_boxscore(cached):
            # ensure player rows present
            rs = _resultset_to_dict_rows(cached, ["PlayerStats"])
            if rs.get("PlayerStats"):
                STATS["box_cache_hits_file"] += 1
                meta["used_cache_file"] = True
                data = cached
            else:
                meta["empty_playerstats_cache"] = True
        elif _is_poison_boxscore(cached):
            STATS["poison_retry"] += 1

    attempts = 0
    while data is None and attempts < 2:
        if refresh_cache or attempts > 0:
            STATS["box_refreshes"] += 1
        attempts += 1
        try:
            _sleep()
            params = {
                "GameID": game_id,
                "StartPeriod": 0,
                "EndPeriod": 10,
                "StartRange": 0,
                "EndRange": 2147483647,
                "RangeType": 0,
            }
            payload = _http_get(meta["box_url"], params, meta)
            STATS["box_fetches"] += 1
            rs = _resultset_to_dict_rows(payload or {}, ["PlayerStats"])
            if rs.get("PlayerStats"):
                data = payload
                _write_cache_atomic(path, payload)
                break
            else:
                meta["empty_playerstats_rowset"] = True
                data = None
        except Exception as exc:
            meta["fetch_failed"] = True
            meta["exc_type"] = type(exc).__name__
            meta["exc_msg"] = str(exc)
            data = None

    if data is None:
        meta["fetch_failed"] = True
        data = {"resultSets": []}

    BOX_MEMO[memo_key] = data
    return data, meta


def _player_rows_from_box(data: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[str]]:
    rs_dict = _resultset_to_dict_rows(data)
    player_rows = rs_dict.get("PlayerStats") or []
    headers = []
    if player_rows:
        headers = list(player_rows[0].keys())
    return player_rows, headers


def get_boxscore_player_rows(game_id: str, date_str: str, refresh_cache: bool = False) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    raw, meta = _fetch_boxscore(game_id, date_str, refresh_cache=refresh_cache)
    if not isinstance(raw, dict):
        meta["player_rows_count"] = 0
        return [], meta
    rs_dict = _resultset_to_dict_rows(raw)
    meta["resultset_names"] = list(rs_dict.keys())
    player_rows = rs_dict.get("PlayerStats") or []
    meta["player_rows_count"] = len(player_rows)
    if not player_rows:
        meta["player_notes"] = "boxscore_parse_failed"
    else:
        meta["sample_players"] = [r.get("PLAYER_NAME") for r in player_rows[:15]]
    return player_rows, meta


def _normalize_player_name(name: str) -> Dict[str, Any]:
    raw = name
    name = name.lower()
    name = re.sub(r"[^a-z0-9 ]+", " ", name)
    parts = [p for p in name.split() if p]
    suffixes = {"jr", "sr", "ii", "iii", "iv", "v"}
    suffix = parts[-1] if parts and parts[-1] in suffixes else None
    if suffix:
        parts = parts[:-1]
    first = parts[0] if parts else ""
    last = parts[-1] if parts else ""
    return {"first": first, "last": last, "suffix": suffix, "raw": raw}


def _parse_slug(player_slug: str) -> Dict[str, Any]:
    slug = player_slug.replace("NBA:", "").lower()
    slug = slug.replace(".", " ").replace("-", " ").replace("_", " ")
    parts = [p for p in slug.split() if p]
    suffixes = {"jr", "sr", "ii", "iii", "iv", "v"}
    suffix = parts[-1] if parts and parts[-1] in suffixes else None
    if suffix:
        parts = parts[:-1]
    first = parts[0] if parts else ""
    last = parts[-1] if parts else ""
    first_initial = first[:1]
    return {"first": first, "first_initial": first_initial, "last": last, "suffix": suffix, "raw": slug}


def resolve_player_in_game(game_id: str, player_slug: str, date_str: str, refresh_cache: bool = False) -> Tuple[Optional[str], Dict[str, Any], Optional[List[Dict[str, Any]]]]:
    rows, meta = get_boxscore_player_rows(game_id, date_str, refresh_cache=refresh_cache)
    if not rows:
        return None, meta, rows

    slug_info = _parse_slug(player_slug)
    if not slug_info.get("last"):
        meta["player_notes"] = "bad_slug"
        return None, meta, rows

    candidates: List[Dict[str, Any]] = []
    for rec in rows:
        pname = str(rec.get("PLAYER_NAME", ""))
        norm = _normalize_player_name(pname)
        if not norm["last"]:
            continue
        last_match = norm["last"] == slug_info["last"]
        if not last_match:
            continue
        # rule 1: full first match
        if slug_info["first"] and norm["first"] == slug_info["first"]:
            candidates.append({"name": pname, "player_id": rec.get("PLAYER_ID"), "reason": "first_last", "row": rec})
            continue
        # rule 2: initial match
        if slug_info["first_initial"] and norm["first"].startswith(slug_info["first_initial"]):
            candidates.append({"name": pname, "player_id": rec.get("PLAYER_ID"), "reason": "initial_last", "row": rec})
            continue
        # rule 4: last only unique handled later
        candidates.append({"name": pname, "player_id": rec.get("PLAYER_ID"), "reason": "last_only", "row": rec})

    # handle suffix constraint
    if slug_info.get("suffix"):
        candidates = [c for c in candidates if slug_info["suffix"] in _normalize_player_name(c["name"]).values()]

    # prefer first_last > initial_last > last_only
    def rank(c):
        r = c["reason"]
        return {"first_last": 0, "initial_last": 1, "last_only": 2}.get(r, 3)

    if not candidates:
        meta["player_notes"] = "player_not_found"
        meta["player_candidates"] = []
        return None, meta, rows

    candidates.sort(key=rank)
    best_reason = rank(candidates[0])
    top = [c for c in candidates if rank(c) == best_reason]
    if len(top) == 1:
        choice = top[0]
        meta["player_match"] = {"name": choice["name"], "player_id": choice["player_id"], "reason": choice["reason"]}
        return str(choice["player_id"]), meta, rows
    # ambiguous
    meta["player_notes"] = "player_ambiguous"
    meta["player_candidates"] = [c["name"] for c in top[:5]]
    return None, meta, rows


def get_player_stat(game_id: str, player_key: str, stat_key: str, date_str: str, refresh_cache: bool = False) -> Optional[Dict[str, Any]]:
    player_id, meta, rows = resolve_player_in_game(game_id, player_key, date_str, refresh_cache=refresh_cache)
    if rows is None or player_id is None:
        # try CDN fallback
        cdn_row, cdn_meta = fetch_cdn_player_row(game_id, player_key, refresh_cache=refresh_cache)
        if cdn_row:
            val = extract_stat_from_row(cdn_row, stat_key)
            if val is None:
                cdn_meta["player_notes"] = "stat_missing"
                return {"player_not_found": True, "meta": cdn_meta}
            return {"value": val, "player_id": cdn_row.get("personId"), "stat_key": STAT_KEY_MAP.get(stat_key.lower(), stat_key), "meta": cdn_meta}
        if meta is None:
            meta = {}
        meta["cdn_fallback"] = cdn_meta
        return {"player_not_found": True, "meta": meta}

    stat_col = STAT_KEY_MAP.get(stat_key.lower())
    if not stat_col:
        meta["player_notes"] = "stat_unknown"
        return {"player_not_found": True, "meta": meta}

    target_row = None
    for rec in rows:
        if str(rec.get("PLAYER_ID")) == str(player_id):
            target_row = rec
            break
    if not target_row:
        meta["player_notes"] = "player_not_found_in_stats"
        return {"player_not_found": True, "meta": meta}

    def _val(col):
        if col in target_row:
            return target_row.get(col)
        return None

    val = _val(stat_col)
    if val is None and stat_col == "TOV":
        val = _val("TO")

    # combo handling
    if val is None and stat_key.lower() in {"pts_reb", "pts_ast", "reb_ast"}:
        pts = _val("PTS") or 0
        reb = _val("REB") or 0
        ast = _val("AST") or 0
        if stat_key.lower() == "pts_reb":
            val = pts + reb
        elif stat_key.lower() == "pts_ast":
            val = pts + ast
        elif stat_key.lower() == "reb_ast":
            val = reb + ast

    if val is None:
        meta["player_notes"] = "stat_missing"
        return {"player_not_found": True, "meta": meta}

    return {
        "value": val,
        "player_id": player_id,
        "stat_key": stat_col,
        "meta": meta,
    }


def fetch_cdn_player_row(game_id: str, player_key: str, refresh_cache: bool = False) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    meta: Dict[str, Any] = {"cdn": True, "game_id": game_id}
    url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
    meta["url"] = url
    if not refresh_cache and game_id in CACHE_CDN:
        meta["used_cache_mem"] = True
        data = CACHE_CDN[game_id]
    else:
        try:
            start = time.time()
            resp = requests.get(url, headers=_http_headers(), timeout=10, verify=_verify_ssl())
            meta["status_code"] = resp.status_code
            meta["elapsed_sec"] = round(time.time() - start, 3)
            meta["body_preview"] = resp.text[:200]
            if resp.status_code != 200:
                meta["fetch_failed"] = True
                return None, meta
            data = resp.json()
            CACHE_CDN[game_id] = data
        except Exception as exc:
            meta["fetch_failed"] = True
            meta["exc_type"] = type(exc).__name__
            meta["exc_msg"] = str(exc)
            return None, meta
    try:
        players = data["game"]["players"]
    except Exception:
        meta["fetch_failed"] = True
        return None, meta
    slug_info = _parse_slug(player_key)
    last = slug_info.get("last")
    first = slug_info.get("first")
    first_initial = slug_info.get("first_initial")
    cand = []
    for p in players:
        name = p.get("name", {}).get("first", "") + " " + p.get("name", {}).get("last", "")
        norm = _normalize_player_name(name)
        if not norm["last"]:
            continue
        if norm["last"] != last:
            continue
        if first and norm["first"] == first:
            cand.append((0, p, name))
        elif first_initial and norm["first"].startswith(first_initial):
            cand.append((1, p, name))
        else:
            cand.append((2, p, name))
    if not cand:
        meta["player_notes"] = "player_not_found_cdn"
        return None, meta
    cand.sort(key=lambda x: x[0])
    if len([c for c in cand if c[0] == cand[0][0]]) > 1:
        meta["player_notes"] = "player_ambiguous_cdn"
        meta["player_candidates"] = [c[2] for c in cand[:5]]
        return None, meta
    chosen = cand[0][1]
    meta["player_match"] = {"name": cand[0][2], "reason": "cdn"}
    return chosen, meta


def extract_stat_from_row(row: Dict[str, Any], stat_key: str) -> Optional[Any]:
    key = stat_key.lower()
    stats = row.get("statistics", {})
    stat_map = {
        "points": stats.get("points"),
        "rebounds": stats.get("reboundsTotal") or stats.get("rebounds"),
        "assists": stats.get("assists"),
        "threes": stats.get("threePointersMade") or stats.get("threePointers"),
        "steals": stats.get("steals"),
        "blocks": stats.get("blocks"),
        "turnovers": stats.get("turnovers"),
        "pts_reb": None,
        "pts_ast": None,
        "reb_ast": None,
    }
    if key == "pts_reb":
        return (stats.get("points") or 0) + (stats.get("reboundsTotal") or stats.get("rebounds") or 0)
    if key == "pts_ast":
        return (stats.get("points") or 0) + (stats.get("assists") or 0)
    if key == "reb_ast":
        return (stats.get("reboundsTotal") or stats.get("rebounds") or 0) + (stats.get("assists") or 0)
    return stat_map.get(key)
