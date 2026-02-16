from __future__ import annotations

import json
import re
import unicodedata
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List

import requests
import subprocess
import json as _json
import signal
import contextlib
import signal
from contextlib import contextmanager

class _TimeoutError(Exception):
    pass

@contextmanager
def _alarm(seconds: int):
    def handler(signum, frame):
        raise _TimeoutError(f"timed out after {seconds}s")
    old = signal.signal(signal.SIGALRM, handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old)

CACHE_DIR = Path("data/cache/results")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

STATS = {
    "cache_hits": 0,
    "fetches": 0,
    # detailed telemetry
    "cache_hit_file": 0,
    "http_fetch_boxscore_cdn": 0,
    "refresh_attempt_boxscore_cdn": 0,
    "http_error_boxscore_cdn": 0,
}

def _cache_path(game_id: str) -> Path:
    return CACHE_DIR / f"nba_cdn_boxscore_{game_id}.json"

def _fetch_boxscore_json(game_id: str, refresh_cache: bool = False, timeout: int = 20) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    meta: Dict[str, Any] = {
        "provider": "nba_cdn",
        "game_id": game_id,
        "used_cache_file": False,
        "fetch_failed": False,
        "status": None,
        "elapsed_sec": None,
        "url": None,
        "path": "requests",
    }

    cp = _cache_path(game_id)
    if cp.exists() and not refresh_cache:
        try:
            STATS["cache_hits"] += 1
            STATS["cache_hit_file"] += 1
            meta["used_cache_file"] = True
            return json.loads(cp.read_text()), meta
        except Exception:
            pass

    if refresh_cache:
        STATS["refresh_attempt_boxscore_cdn"] += 1

    url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
    meta["url"] = url
    @contextlib.contextmanager
    def _deadline(seconds: int):
        def handler(signum, frame):
            raise TimeoutError("deadline exceeded")

        old_handler = signal.signal(signal.SIGALRM, handler)
        signal.alarm(seconds)
        try:
            yield
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)

    session = requests.Session()
    session.trust_env = False

    for attempt in range(2):  # one retry
        t0 = time.time()
        meta["path"] = "requests" if attempt == 0 else "requests_retry"
        try:
            with _deadline(5):
                STATS["fetches"] += 1
                STATS["http_fetch_boxscore_cdn"] += 1
                resp = session.get(url, timeout=(3, 20), verify=True)
            meta["status"] = resp.status_code
            meta["elapsed_sec"] = round(time.time() - t0, 3)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, dict) and data.get("game"):
                    cp.write_text(json.dumps(data))
                    return data, meta
            meta["body_preview"] = resp.text[:200]
            if resp.status_code != 200:
                STATS["http_error_boxscore_cdn"] += 1
        except Exception as e:
            meta.setdefault("exceptions", []).append(repr(e))
            meta["elapsed_sec"] = round(time.time() - t0, 3)
            STATS["http_error_boxscore_cdn"] += 1

    # curl fallback with python-enforced timeout
    meta["path"] = "curl_fallback"
    try:
        t0 = time.time()
        STATS["http_fetch_boxscore_cdn"] += 1
        output = subprocess.check_output(["curl", "-sL", url], timeout=20)
        meta["elapsed_sec"] = round(time.time() - t0, 3)
        meta["status"] = 0
        if output:
            try:
                data = _json.loads(output)
                if isinstance(data, dict) and data.get("game"):
                    cp.write_text(json.dumps(data))
                    return data, meta
                else:
                    meta.setdefault("exceptions", []).append("curl_json_invalid")
                    STATS["http_error_boxscore_cdn"] += 1
            except Exception as e:
                meta.setdefault("exceptions", []).append(f"curl_json_error:{e}")
                STATS["http_error_boxscore_cdn"] += 1
        else:
            STATS["http_error_boxscore_cdn"] += 1
    except subprocess.TimeoutExpired as e:
        meta.setdefault("exceptions", []).append(f"curl_timeout:{e}")
        meta["elapsed_sec"] = round(time.time() - t0, 3)
        STATS["http_error_boxscore_cdn"] += 1
    except Exception as e:
        meta.setdefault("exceptions", []).append(f"curl_error:{e}")
        STATS["http_error_boxscore_cdn"] += 1

    meta["fetch_failed"] = True
    return None, meta

def _stat_from_player(player: Dict[str, Any], stat_key: str) -> Optional[float]:
    s = (player.get("statistics") or {})
    # map canonical keys -> CDN fields
    mapping = {
        "points": "points",
        "rebounds": "reboundsTotal",
        "assists": "assists",
        "threes": "threePointersMade",
        "steals": "steals",
        "blocks": "blocks",
        "turnovers": "turnovers",
    }
    # combos
    if stat_key in {"pa", "pts_ast", "pts_ast"}:
        pts = s.get("points") or 0
        ast = s.get("assists") or 0
        return float(pts + ast)
    if stat_key in {"ra", "reb_ast"}:
        reb = s.get("reboundsTotal") or s.get("rebounds") or 0
        ast = s.get("assists") or 0
        return float(reb + ast)
    if stat_key in {"pr", "pts_reb"}:
        pts = s.get("points") or 0
        reb = s.get("reboundsTotal") or s.get("rebounds") or 0
        return float(pts + reb)
    if stat_key in {"pra", "pts_reb_ast", "pts_reb_assists"}:
        pts = s.get("points") or 0
        reb = s.get("reboundsTotal") or s.get("rebounds") or 0
        ast = s.get("assists") or 0
        return float(pts + reb + ast)

    k = mapping.get(stat_key, stat_key)
    val = s.get(k)
    if val is None:
        return None
    try:
        return float(val)
    except Exception:
        return None


SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}


def _last_non_suffix(tokens: list[str]) -> str:
    for t in reversed(tokens):
        if t and t not in SUFFIXES:
            return t
    return ""


def _normalize_key(s: str) -> str:
    s = s.replace("NBA:", "")
    s = s.lower()
    s = s.replace("_", " ").replace("-", " ").replace("/", " ")
    # remove apostrophes/smart quotes entirely so de'aaron -> deaaron
    s = re.sub(r"[’'`´]", "", s)
    # remove remaining punctuation with space
    s = re.sub(r"[\\.,]", " ", s)
    # unicode normalize then strip non-ascii
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    parts = [p for p in re.split(r"\\s+", s) if p]
    if parts and parts[-1] in SUFFIXES:
        parts = parts[:-1]
    return " ".join(parts)


def _resolve_player(players: List[Dict[str, Any]], player_key: str) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    meta: Dict[str, Any] = {"requested_player_key_raw": player_key}
    if player_key.isdigit():
        for p in players:
            if str(p.get("personId")) == player_key:
                meta.update(
                    {
                        "match_method": "personId",
                        "matched_personId": p.get("personId"),
                        "matched_name_raw": p.get("name"),
                        "matched_name_norm": _normalize_key(p.get("name", "")),
                    }
                )
                return p, meta

    key_clean = _normalize_key(player_key)
    meta["requested_player_key_norm"] = key_clean
    parts = key_clean.split()
    key_is_initial_last = len(parts) == 2 and len(parts[0]) == 1

    def norm_player(p: Dict[str, Any]) -> Tuple[str, str, str]:
        name_str = p.get("name") or ""
        fn = p.get("firstName") or ""
        ln = p.get("familyName") or ""
        full_name = _normalize_key(name_str if name_str else f"{fn} {ln}")
        norm_fn = _normalize_key(fn)
        norm_ln = _normalize_key(ln)
        ln_tokens = norm_ln.split()
        ln_core = _last_non_suffix(ln_tokens) if ln_tokens else norm_ln
        fn_core = norm_fn.split()[0] if norm_fn else norm_fn
        return full_name, fn_core, ln_core

    # Precompute
    norm_players = []
    for p in players:
        full, fn, ln = norm_player(p)
        norm_players.append((p, full, fn, ln))

    # Exact full name match
    for p, full, fn, ln in norm_players:
        if key_clean == full:
            meta.update(
                {
                    "match_method": "full_norm",
                    "matched_personId": p.get("personId"),
                    "matched_name_raw": p.get("name"),
                    "matched_name_norm": full,
                }
            )
            return p, meta

    # initial + last
    if key_is_initial_last:
        init, last = parts
        cands = [p for p, full, fn, ln in norm_players if ln == last and fn and fn.startswith(init)]
        meta["candidates_count"] = len(cands)
        if len(cands) == 1:
            p = cands[0]
            meta.update(
                {
                    "match_method": "initial_last",
                    "matched_personId": p.get("personId"),
                    "matched_name_raw": p.get("name"),
                    "matched_name_norm": _normalize_key(p.get("name", "")),
                }
            )
            return p, meta

    # last-only unique
    if len(parts) == 1:
        last = parts[0]
        cands = [p for p, full, fn, ln in norm_players if ln == last]
        meta["candidates_count"] = len(cands)
        if len(cands) == 1:
            p = cands[0]
            meta.update(
                {
                    "match_method": "last_only_unique",
                    "matched_personId": p.get("personId"),
                    "matched_name_raw": p.get("name"),
                    "matched_name_norm": _normalize_key(p.get("name", "")),
                }
            )
            return p, meta

    meta["player_notes"] = "player_not_found"
    meta["players_sample"] = [p.get("name") for p in players[:15]]
    meta["players_norm_sample"] = [_normalize_key(p.get("name", "")) for p in players[:15]]
    return None, meta

def fetch_player_statline(
    game_id: str,
    player_id: Optional[str],
    stat_key: str,
    refresh_cache: bool = False,
) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    """
    Return canonical statline dict: {"status": "OK", "value": <float>, "player_id":..., "player_name":...}
    """
    data, meta = _fetch_boxscore_json(game_id, refresh_cache=refresh_cache)
    if not data or meta.get("fetch_failed"):
        return None, meta

    game = data.get("game") or {}
    home_team = game.get("homeTeam") or {}
    away_team = game.get("awayTeam") or {}
    meta["home_tricode"] = home_team.get("teamTricode")
    meta["away_tricode"] = away_team.get("teamTricode")
    players: List[Dict[str, Any]] = []
    home_players = []
    away_players = []
    for p in home_team.get("players") or []:
        p2 = dict(p)
        p2["teamTricode"] = home_team.get("teamTricode")
        p2["_team_tricode"] = p2["teamTricode"]
        home_players.append(p2)
    for p in away_team.get("players") or []:
        p2 = dict(p)
        p2["teamTricode"] = away_team.get("teamTricode")
        p2["_team_tricode"] = p2["teamTricode"]
        away_players.append(p2)
    players.extend(home_players)
    players.extend(away_players)
    meta["home_players_count"] = len(home_players)
    meta["away_players_count"] = len(away_players)
    meta["total_players_count"] = len(players)
    if len(home_players) == 0 or len(away_players) == 0:
        meta["player_notes"] = meta.get("player_notes") or "one_side_players_empty"

    target, match_meta = _resolve_player(players, str(player_id) if player_id is not None else "")
    meta.update(match_meta)
    if not target:
        meta["player_not_found"] = True
        meta["players_count"] = len(players)
        meta["player_ids_sample"] = [p.get("personId") for p in players[:15]]
        # distinguish not in game vs bad fetch
        if meta.get("status") == 200 or meta.get("path") == "curl_fallback":
            meta["player_notes"] = "player_not_in_game"
        return None, meta

    val = _stat_from_player(target, stat_key)
    if val is None:
        meta["stat_missing"] = True
        meta["stat_key"] = stat_key
        return None, meta

    return {
        "status": "OK",
        "value": val,
        "player_id": str(target.get("personId")),
        "player_name": target.get("name"),
        "team": target.get("teamTricode"),
        "team_tricode": target.get("_team_tricode"),
        "provider": "nba_cdn",
        "match_method": meta.get("match_method"),
        "stat_field_used": stat_key,
    }, meta
