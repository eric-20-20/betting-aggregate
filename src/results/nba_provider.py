"""Unified NBA results provider wrapper.

Selects backend via RESULTS_PROVIDER (api_sports, nba_api) and exposes:
  fetch_game_result(event_key, away, home, date_str, refresh_cache=False) -> dict or None
  fetch_player_statline(event_key, player_id, stat_key, date_str, away, home, refresh_cache=False) -> dict or None
"""

from __future__ import annotations

import os
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple, Iterable, List

from . import nba_provider_api_sports as api_sports
from . import nba_provider_nba_api as nba_api
from . import nba_provider_nba_cdn as nba_cdn
from . import nba_provider_nba_stats as nba_stats
from . import nba_provider_data_nba_net as nba_net
from . import nba_provider_bball_ref as lgf_provider  # LeagueGameFinder fallback

# Days in past after which we force refresh cache for SCHEDULED games
STALE_SCHEDULED_DAYS = 3


def _should_force_refresh_stale(date_str: str, game_status: Optional[str]) -> bool:
    """Return True if game is SCHEDULED but date is old enough to warrant cache refresh."""
    if game_status != "SCHEDULED":
        return False
    try:
        game_date = datetime.strptime(date_str, "%Y-%m-%d")
        days_ago = (datetime.now() - game_date).days
        return days_ago >= STALE_SCHEDULED_DAYS
    except Exception:
        return False

def _provider() -> str:
    prov = os.environ.get("RESULTS_PROVIDER")
    if prov:
        return prov.lower()
    if os.environ.get("NBA_API") == "1":
        return "nba_api"
    if os.environ.get("NBA_STATS") == "1":
        return "nba_stats"
    # default: use LGF (LeagueGameFinder) to resolve game IDs, CDN for stats
    return "auto"


def provider_info() -> Dict[str, Any]:
    prov = _provider()
    if prov == "auto":
        return {"provider": prov, "base": "lgf+cdn", "key_present": True}
    if prov == "nba_api":
        return {"provider": prov, "base": "nba_api.stats", "key_present": True}
    if prov == "nba_cdn":
        return {"provider": "nba_cdn", "base": "cdn.nba.com", "key_present": True}
    if prov == "api_sports":
        return {
            "provider": prov,
            "base": api_sports._base_url(),
            "key_present": bool(os.environ.get("API_SPORTS_KEY")),
        }
    if prov == "nba_stats":
        return {"provider": "nba_stats", "base": "nba_api", "key_present": True, "legacy": True}
    if prov == "data_nba_net":
        return {"provider": "data_nba_net", "base": "data.nba.net", "key_present": True}
    return {"provider": prov, "base": "unknown", "key_present": False}


def _games_diagnostics(games: List[Dict[str, Any]]) -> Tuple[List[str], List[Dict[str, Any]]]:
    matchups_full: List[str] = []
    games_full: List[Dict[str, Any]] = []
    for g in games:
        away = g.get("away_team_abbrev")
        home = g.get("home_team_abbrev")
        matchups_full.append(f"{away}@{home}")
        games_full.append(
            {
                "game_id": str(g.get("game_id")),
                "away": away,
                "home": home,
                "status": g.get("status"),
            }
        )
    return matchups_full, games_full


def _parse_event_key_date(event_key: Optional[str]) -> Optional[str]:
    if not isinstance(event_key, str):
        return None
    try:
        parts = event_key.split(":")
        if len(parts) >= 4 and all(p.isdigit() for p in parts[1:4]):
            return f"{parts[1]}-{parts[2]}-{parts[3]}"
    except Exception:
        return None
    return None


def _candidate_dates(primary_date: str, event_key: Optional[str]) -> List[Tuple[str, int, str]]:
    """
    Return list of (date_str, shift_from_primary, strategy) preserving order and deduping dates.
    strategy in {primary, shifted, event_key_date, event_key_shifted}
    """
    seen = set()
    out: List[Tuple[str, int, str]] = []

    def add(ds: str, shift: int, strat: str) -> None:
        if ds not in seen:
            seen.add(ds)
            out.append((ds, shift, strat))

    add(primary_date, 0, "primary")
    try:
        base = datetime.strptime(primary_date, "%Y-%m-%d")
        add((base - timedelta(days=1)).strftime("%Y-%m-%d"), -1, "shifted")
        add((base + timedelta(days=1)).strftime("%Y-%m-%d"), 1, "shifted")
    except Exception:
        pass

    ev_date = _parse_event_key_date(event_key)
    if ev_date:
        try:
            base_ev = datetime.strptime(ev_date, "%Y-%m-%d")
            add(ev_date, (base_ev - datetime.strptime(primary_date, "%Y-%m-%d")).days, "event_key_date")
            add((base_ev - timedelta(days=1)).strftime("%Y-%m-%d"), (base_ev - timedelta(days=1) - datetime.strptime(primary_date, "%Y-%m-%d")).days, "event_key_shifted")
            add((base_ev + timedelta(days=1)).strftime("%Y-%m-%d"), (base_ev + timedelta(days=1) - datetime.strptime(primary_date, "%Y-%m-%d")).days, "event_key_shifted")
        except Exception:
            pass

    return out


def _prepare_games_meta(games: List[Dict[str, Any]], meta: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    meta_copy = json.loads(json.dumps(meta))
    matchups_full, games_full = _games_diagnostics(games)
    actual_count = len(games)
    meta_count_raw = meta_copy.get("games_count") or 0
    try:
        meta_count = int(meta_count_raw)
    except Exception:
        meta_count = 0
    mismatch = bool(meta_count and actual_count and meta_count != actual_count)
    partial = mismatch or (meta_count == 0 and actual_count > 0)
    meta_copy.update(
        {
            "games_count_actual": actual_count,
            "games_count_meta": meta_count,
            "matchups_full": matchups_full,
            "games_full": games_full,
        }
    )
    if partial:
        meta_copy["suspect_partial"] = True
    return games, meta_copy


def fetch_game_result(event_key: str, away: str, home: str, date_str: str, refresh_cache: bool = False) -> Optional[Dict[str, Any]]:
    provider = _provider()

    if provider == "auto":
        # Primary path: LGF (LeagueGameFinder) resolves game_id, CDN fetches scores/stats
        # LGF cache is fast and reliable; CDN has complete boxscores including player stats
        lgf_result = lgf_provider.fetch_game_result(event_key, away, home, date_str, refresh_cache=refresh_cache)
        if lgf_result and lgf_result.get("status") not in (None, "ERROR"):
            # If it's a FINAL result with scores, return it directly
            if lgf_result.get("status") == "FINAL" and lgf_result.get("game_id"):
                # Try CDN for richer data (needed for player stats grading)
                game_id = str(lgf_result["game_id"])
                cdn_result, cdn_meta = nba_cdn.fetch_game_result(game_id, refresh_cache=refresh_cache)
                if cdn_result and cdn_result.get("status") == "FINAL":
                    cdn_result["game_id"] = game_id
                    cdn_result["fallback_from"] = "auto_lgf_cdn"
                    return cdn_result
                # CDN unavailable — return LGF scores directly
                lgf_result["fallback_from"] = "auto_lgf"
                return lgf_result
        # LGF failed or returned non-final — fall through to nba_api path
        # (handles SCHEDULED games, missing LGF entries, etc.)

    if provider in {"nba_api", "auto"}:
        try:
            away_n = nba_api._norm_team(away)
            home_n = nba_api._norm_team(home)
            meta_copy: Dict[str, Any] = {}
            game_id = None
            used_date = date_str
            for ds_attempt, shift, strat in _candidate_dates(date_str, event_key):
                games, meta_raw = nba_api.get_games_for_date(ds_attempt, refresh_cache=refresh_cache)
                games, meta = _prepare_games_meta(games, meta_raw)
                if not refresh_cache and meta.get("suspect_partial") and (meta.get("used_cache_file") or meta.get("used_cache_mem")):
                    games, meta_raw = nba_api.get_games_for_date(ds_attempt, refresh_cache=True)
                    games, meta = _prepare_games_meta(games, meta_raw)
                    meta["partial_refetch"] = True
                meta["date_used"] = ds_attempt
                meta["date_shifted"] = shift
                meta["date_strategy"] = strat
                meta["search"] = {"away": away, "home": home, "away_norm": away_n, "home_norm": home_n}
                meta["search_attempted"] = [
                    {"away": away, "home": home, "away_norm": away_n, "home_norm": home_n},
                    {"away": home, "home": away, "away_norm": home_n, "home_norm": away_n},
                ]
                meta.setdefault("match_orientation", "none")
                # keep first meta as baseline
                if not meta_copy:
                    meta_copy = json.loads(json.dumps(meta))
                for g in games:
                    away_game = nba_api._norm_team(str(g.get("away_team_abbrev") or ""))
                    home_game = nba_api._norm_team(str(g.get("home_team_abbrev") or ""))
                    orientation = None
                    if away_game == away_n and home_game == home_n:
                        orientation = "direct"
                    elif away_game == home_n and home_game == away_n:
                        orientation = "flipped"
                    if orientation:
                        game_id = str(g.get("game_id"))
                        meta_copy = json.loads(json.dumps(meta))
                        meta_copy["matched_game_id"] = game_id
                        meta_copy["matched_status"] = g.get("status")
                        meta_copy["match_orientation"] = orientation
                        if orientation == "flipped":
                            meta_copy["search_flipped"] = {"away": home, "home": away, "away_norm": home_n, "home_norm": away_n}
                        used_date = ds_attempt
                        break
                if game_id:
                    break
            if meta_copy.get("fetch_failed"):
                return {"status": "ERROR", "notes": "fetch_failed", "games_info": meta_copy}
            actual_count = meta_copy.get("games_count_actual", meta_copy.get("games_count", 0))
            meta_count = meta_copy.get("games_count_meta", meta_copy.get("games_count", 0))
            if actual_count == 0 and meta_count == 0:
                # NBA API scoreboard is empty — try LGF before giving up (LGF cache may have scores)
                lgf_result = lgf_provider.fetch_game_result(event_key, away, home, date_str, refresh_cache=refresh_cache)
                if lgf_result and lgf_result.get("status") not in (None, "ERROR"):
                    lgf_result["fallback_from"] = "empty_scoreboard_lgf"
                    lgf_result["games_info"] = meta_copy
                    return lgf_result
                return {"status": "NO_GAMES", "notes": "no_games_for_date", "games_info": meta_copy}
            if game_id:
                # Check if game is stale SCHEDULED and needs refresh
                matched_status = meta_copy.get("matched_status")
                force_refresh = _should_force_refresh_stale(used_date, matched_status)
                if force_refresh and not refresh_cache:
                    # Re-fetch scoreboard with refresh to get updated status
                    games_fresh, meta_fresh = nba_api.get_games_for_date(used_date, refresh_cache=True)
                    games_fresh, meta_fresh = _prepare_games_meta(games_fresh, meta_fresh)
                    meta_copy["heal_meta"] = {"healed": False, "path": "stale_refresh", "reason": "stale_scheduled_refresh"}
                    # Find the game again in refreshed data
                    for g in games_fresh:
                        if str(g.get("game_id")) == game_id:
                            meta_copy["matched_status"] = g.get("status")
                            meta_copy["heal_meta"]["healed"] = meta_copy["matched_status"] != "SCHEDULED"
                            break
                # If still SCHEDULED after refresh, try CDN fallback first (has player stats), then LGF
                if meta_copy.get("matched_status") == "SCHEDULED" and force_refresh:
                    # Try CDN - it has complete data including player stats
                    cdn_result, cdn_meta = nba_cdn.fetch_game_result(game_id, refresh_cache=refresh_cache)
                    if cdn_result and cdn_result.get("status") == "FINAL":
                        # Swap scores when flipped so they align with signal's away/home
                        if meta_copy.get("match_orientation") == "flipped":
                            cdn_result["home_score"], cdn_result["away_score"] = cdn_result.get("away_score"), cdn_result.get("home_score")
                        cdn_result["fallback_from"] = "stale_scheduled_cdn"
                        cdn_result["games_info"] = meta_copy
                        cdn_result["cdn_meta"] = cdn_meta
                        return cdn_result
                    # If CDN failed, try LGF as last resort
                    lgf_result = lgf_provider.fetch_game_result(event_key, away, home, date_str, refresh_cache=refresh_cache)
                    if lgf_result and lgf_result.get("status") != "ERROR" and lgf_result.get("status") == "FINAL":
                        lgf_result["fallback_from"] = "stale_scheduled_lgf"
                        lgf_result["games_info"] = meta_copy
                        return lgf_result
                score = nba_api.get_game_score(game_id, used_date, refresh_cache=refresh_cache or force_refresh) or {}
                # Swap scores when flipped so they align with signal's away/home
                if meta_copy.get("match_orientation") == "flipped":
                    score["home_score"], score["away_score"] = score.get("away_score"), score.get("home_score")
                info = score.get("games_info") or {}
                info.update(meta_copy)
                score["games_info"] = info
                return {"game_id": game_id, **score}
            # include scoreboard list for debugging
            meta_copy["match_orientation"] = meta_copy.get("match_orientation") or "none"
            matchups_full, games_full = _games_diagnostics(games)
            meta_copy["matchups_full"] = matchups_full
            meta_copy["games_full"] = games_full
            meta_copy["matchups"] = matchups_full[:10]
            # Fallback to LeagueGameFinder if standard scoreboard failed
            lgf_result = lgf_provider.fetch_game_result(event_key, away, home, date_str, refresh_cache=refresh_cache)
            if lgf_result and lgf_result.get("status") != "ERROR":
                lgf_result["fallback_from"] = "nba_api_scoreboard"
                return lgf_result
            return {"status": "ERROR", "notes": "game_not_found", "games_info": meta_copy}
        except Exception as e:
            # Try LeagueGameFinder as last resort
            try:
                lgf_result = lgf_provider.fetch_game_result(event_key, away, home, date_str, refresh_cache=refresh_cache)
                if lgf_result and lgf_result.get("status") != "ERROR":
                    lgf_result["fallback_from"] = "nba_api_exception"
                    return lgf_result
            except Exception:
                pass
            return {"status": "ERROR", "notes": "nba_api_exception", "games_info": {"provider": "nba_api", "date": date_str, "fetch_failed": True, "exception": repr(e)}}

    if provider == "api_sports":
        try:
            game_id = api_sports.resolve_game_id(date_str, away, home, refresh_cache=refresh_cache)
            if game_id:
                score = api_sports.get_game_score(game_id)
                if score:
                    return {"game_id": game_id, **score}
        except Exception as e:
            return {"status": "ERROR", "notes": "api_sports_exception", "games_info": {"provider": "api_sports", "date": date_str, "fetch_failed": True, "exception": repr(e)}}
        # fallback to nba_api if API-Sports blank
        try:
            games, meta_raw = nba_api.get_games_for_date(date_str, refresh_cache=refresh_cache)
            games, meta = _prepare_games_meta(games, meta_raw)
            if not refresh_cache and meta.get("suspect_partial") and (meta.get("used_cache_file") or meta.get("used_cache_mem")):
                games, meta_raw = nba_api.get_games_for_date(date_str, refresh_cache=True)
                games, meta = _prepare_games_meta(games, meta_raw)
                meta["partial_refetch"] = True
            game_id = None
            away_n = nba_api._norm_team(away)
            home_n = nba_api._norm_team(home)
            meta["search"] = {"away": away, "home": home, "away_norm": away_n, "home_norm": home_n}
            meta["search_attempted"] = [
                {"away": away, "home": home, "away_norm": away_n, "home_norm": home_n},
                {"away": home, "home": away, "away_norm": home_n, "home_norm": away_n},
            ]
            meta.setdefault("match_orientation", "none")
            for g in games:
                away_game = nba_api._norm_team(str(g.get("away_team_abbrev") or ""))
                home_game = nba_api._norm_team(str(g.get("home_team_abbrev") or ""))
                orientation = None
                if away_game == away_n and home_game == home_n:
                    orientation = "direct"
                elif away_game == home_n and home_game == away_n:
                    orientation = "flipped"
                if orientation:
                    game_id = str(g.get("game_id"))
                    meta["matched_game_id"] = game_id
                    meta["matched_status"] = g.get("status")
                    meta["match_orientation"] = orientation
                    if orientation == "flipped":
                        meta["search_flipped"] = {"away": home, "home": away, "away_norm": home_n, "home_norm": away_n}
                    break
            if meta.get("fetch_failed"):
                return {"status": "ERROR", "notes": "fetch_failed", "games_info": meta}
            actual_count = meta.get("games_count_actual", meta.get("games_count", 0))
            meta_count = meta.get("games_count_meta", meta.get("games_count", 0))
            if actual_count == 0 and meta_count == 0:
                return {"status": "NO_GAMES", "notes": "no_games_for_date", "games_info": meta}
            if game_id:
                score = nba_api.get_game_score(game_id, date_str, refresh_cache=refresh_cache) or {}
                info = score.get("games_info") or {}
                info.update(meta)
                score["games_info"] = info
                return {"game_id": game_id, **score}
            meta["matchups"] = meta.get("matchups_full", [])[:10]
            return {"status": "ERROR", "notes": "game_not_found", "games_info": meta}
        except Exception as e:
            return {"status": "ERROR", "notes": "nba_api_exception", "games_info": {"provider": "nba_api", "date": date_str, "fetch_failed": True, "exception": repr(e), "fallback_from": "api_sports"}}

    if provider == "nba_stats":
        try:
            game_id = nba_stats.resolve_game_id(date_str, away, home, refresh_cache=refresh_cache)
            if game_id:
                score = nba_stats.get_game_score(game_id, date_str, refresh_cache=refresh_cache)
                if score:
                    return {"game_id": game_id, **score}
        except Exception:
            # fallback
            pass
    # fallback to data.nba.net
    res = nba_net.fetch_game_result(event_key, away, home, date_str, refresh_cache=refresh_cache)
    if res:
        return res
    return None


def fetch_player_statline(event_key: str, player_id: str, stat_key: str, date_str: str, away: str, home: str, refresh_cache: bool = False) -> Optional[Dict[str, Any]]:
    provider = _provider()

    def resolve_game_id() -> Tuple[Optional[str], Dict[str, Any]]:
        try:
            game_id = None
            meta_best: Dict[str, Any] = {}
            away_n = nba_api._norm_team(away)
            home_n = nba_api._norm_team(home)
            for ds_attempt, shift, strat in _candidate_dates(date_str, event_key):
                games, meta_raw = nba_api.get_games_for_date(ds_attempt, refresh_cache=refresh_cache)
                games, meta = _prepare_games_meta(games, meta_raw)
                if not refresh_cache and meta.get("suspect_partial") and (meta.get("used_cache_file") or meta.get("used_cache_mem")):
                    games, meta_raw = nba_api.get_games_for_date(ds_attempt, refresh_cache=True)
                    games, meta = _prepare_games_meta(games, meta_raw)
                    meta["partial_refetch"] = True
                meta["date_used"] = ds_attempt
                meta["date_shifted"] = shift
                meta["date_strategy"] = strat
                meta["search"] = {"away": away, "home": home, "away_norm": away_n, "home_norm": home_n}
                meta["search_attempted"] = [
                    {"away": away, "home": home, "away_norm": away_n, "home_norm": home_n},
                    {"away": home, "home": away, "away_norm": home_n, "home_norm": away_n},
                ]
                meta.setdefault("match_orientation", "none")
                if not meta_best:
                    meta_best = json.loads(json.dumps(meta))
                for g in games:
                    away_game = nba_api._norm_team(str(g.get("away_team_abbrev") or ""))
                    home_game = nba_api._norm_team(str(g.get("home_team_abbrev") or ""))
                    orientation = None
                    if away_game == away_n and home_game == home_n:
                        orientation = "direct"
                    elif away_game == home_n and home_game == away_n:
                        orientation = "flipped"
                    if orientation:
                        game_id = str(g.get("game_id"))
                        meta_best = json.loads(json.dumps(meta))
                        meta_best["matched_game_id"] = game_id
                        meta_best["matched_status"] = g.get("status")
                        meta_best["match_orientation"] = orientation
                        if orientation == "flipped":
                            meta_best["search_flipped"] = {"away": home, "home": away, "away_norm": home_n, "home_norm": away_n}
                        break
                if game_id:
                    break
            if game_id:
                return game_id, meta_best
            games_last = games if "games" in locals() else []
            matchups_full, games_full = _games_diagnostics(games_last)
            meta_best["matchups_full"] = matchups_full
            meta_best["games_full"] = games_full
            meta_best["matchups"] = matchups_full[:10]
            meta_best["match_orientation"] = meta_best.get("match_orientation") or "none"
            return None, meta_best
        except Exception:
            return None, {"error": "resolve_game_id_exception"}

    def try_cdn(game_id: Optional[str], fallback_from: Optional[str] = None) -> Optional[Dict[str, Any]]:
        if not game_id:
            return {"status": "ERROR", "notes": "game_id_not_found_for_prop"}
        stat, meta = nba_cdn.fetch_player_statline(game_id, player_id, stat_key, refresh_cache=refresh_cache)
        if stat:
            stat["provider"] = "nba_cdn"
            stat["fallback_from"] = fallback_from
            return {"game_id": game_id, **stat, "meta": meta}
        if meta:
            return {"status": "ERROR", "notes": meta.get("player_notes") or "player_not_found", "meta": {**meta, "requested_player_id": player_id, "requested_stat_key": stat_key, "game_id": game_id}}
        return None

    # routing
    if provider == "nba_cdn":
        game_id, _meta = resolve_game_id()
        return try_cdn(game_id, fallback_from=None)

    if provider == "auto":
        # Try LGF first to get game_id (faster, more reliable than nba_api scoreboard)
        lgf_result = lgf_provider.fetch_game_result(event_key, away, home, date_str, refresh_cache=refresh_cache)
        lgf_game_id = lgf_result.get("game_id") if lgf_result else None
        if lgf_game_id:
            res = try_cdn(str(lgf_game_id), fallback_from="auto_lgf")
            if isinstance(res, dict) and res.get("status") != "ERROR":
                return res
        # LGF didn't yield a game_id or CDN failed — fall back to nba_api scoreboard
        game_id, meta = resolve_game_id()
        res = try_cdn(game_id, fallback_from=None)
        if isinstance(res, dict) and res.get("status") != "ERROR":
            if "games_info" not in res:
                res["games_info"] = meta
            return res
        if isinstance(res, dict) and res.get("status") == "ERROR":
            if "games_info" not in res:
                res["games_info"] = meta
            return res
        if os.environ.get("DEBUG_CDN_PROPS") == "1":
            print("[props][cdn] failed", {"event_key": event_key, "game_id": game_id, "player_id": player_id, "stat_key": stat_key, "res": res})
        return {"status": "ERROR", "notes": "cdn_failed_unknown", "games_info": meta}

    if provider == "nba_api":
        game_id, meta = resolve_game_id()
        if game_id:
            stats = nba_api.get_player_stat(game_id, player_id, stat_key, date_str, refresh_cache=refresh_cache)
            if stats:
                stats["games_info"] = meta
                stats["provider"] = "nba_api"
                return {"game_id": game_id, **stats}
        # fallback to cdn
        res = try_cdn(game_id, fallback_from="nba_api")
        if res:
            return res

    if provider == "api_sports":
        try:
            game_id = api_sports.resolve_game_id(date_str, away, home, refresh_cache=refresh_cache)
            if game_id:
                season = api_sports._season_for_date(date_str)
                pid = api_sports.find_player_id(game_id, season, player_id)
                if pid:
                    stats_list = api_sports.get_player_stats(game_id)
                    for entry in stats_list:
                        player = entry.get("player") or {}
                        if player.get("id") == pid:
                            statistics = (entry.get("statistics") or entry.get("stats") or entry.get("stat") or [])
                            if isinstance(statistics, list) and statistics:
                                stat_block = statistics[0]
                            else:
                                stat_block = statistics
                            if isinstance(stat_block, dict) and stat_key in stat_block:
                                return {"value": stat_block.get(stat_key), "player_id": pid, "game_id": game_id, "provider": "api_sports"}
                            for sec in ("points", "rebounds", "offense", "defense", "misc"):
                                if isinstance(stat_block, dict) and sec in stat_block and stat_key in stat_block[sec]:
                                    return {"value": stat_block[sec][stat_key], "player_id": pid, "game_id": game_id, "provider": "api_sports"}
            # fallback to cdn if game id known
            if game_id:
                res = try_cdn(game_id, fallback_from="api_sports")
                if res:
                    return res
        except Exception:
            pass

    if provider == "nba_stats":
        try:
            game_id = nba_stats.resolve_game_id(date_str, away, home, refresh_cache=refresh_cache)
            if game_id:
                stats = nba_stats.get_player_stats(game_id, player_id, refresh_cache=refresh_cache)
                if stats:
                    sk = stat_key.upper()
                    if sk in stats:
                        return {"value": stats[sk], "player_id": stats.get("PLAYER_ID"), "game_id": game_id, "provider": "nba_stats"}
        except Exception:
            pass
    # fallback to data.nba.net
    res = nba_net.fetch_player_statline(event_key, player_id, stat_key, date_str, away, home, refresh_cache=refresh_cache)
    if res:
        return res
    return None


def search_all_games_for_player_statline(
    player_id: str,
    stat_key: str,
    date_str: str,
    refresh_cache: bool = False,
) -> Optional[Dict[str, Any]]:
    """Search all games on a date for a player's statline (fallback for wrong matchup).

    Returns dict with value, game_id, matched_away, matched_home, provider keys,
    or None if player not found in any game.
    """
    try:
        games, meta_raw = nba_api.get_games_for_date(date_str, refresh_cache=refresh_cache)
    except Exception:
        return None
    if not games:
        return None
    for g in games:
        game_id = str(g.get("game_id") or "")
        if not game_id:
            continue
        g_status = g.get("status")
        if g_status and g_status not in ("FINAL", "Final", "final", "Final/OT"):
            continue
        try:
            stat, cdn_meta = nba_cdn.fetch_player_statline(
                game_id, player_id, stat_key, refresh_cache=refresh_cache
            )
        except Exception:
            continue
        if stat and not stat.get("player_not_found"):
            away_g = str(g.get("away_team_abbrev") or "").upper()
            home_g = str(g.get("home_team_abbrev") or "").upper()
            stat["provider"] = "nba_cdn"
            stat["fallback_from"] = "all_games_search"
            return {
                "game_id": game_id,
                "matched_away": away_g,
                "matched_home": home_g,
                **stat,
                "meta": cdn_meta,
            }
    return None
