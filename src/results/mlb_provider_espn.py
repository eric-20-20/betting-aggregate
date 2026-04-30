"""MLB score + player stat provider using ESPN's free public API.

No API key required.  Provides game scores and player boxscores for
Major League Baseball.

Endpoints:
  Scoreboard: site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard
  Summary:    site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import requests  # type: ignore
except ImportError:  # pragma: no cover
    requests = None  # type: ignore

CACHE_DIR = Path("data/cache/results/mlb")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb"

STATS = {"cache_hits": 0, "fetches": 0, "refreshes": 0, "bad_cache": 0}

# ── Team code mapping ──────────────────────────────────────────────────────
# Our canonical codes (data_mlb.py) → ESPN abbreviations where they differ.
ABBREV_MAP: Dict[str, str] = {
    "CHW": "CWS",   # Chicago White Sox (we use CHW, ESPN uses CWS)
    "KCR": "KC",    # Kansas City Royals
    "SDP": "SD",    # San Diego Padres
    "SFG": "SF",    # San Francisco Giants
    "TBR": "TB",    # Tampa Bay Rays
    "WSN": "WSH",   # Washington Nationals
}

REVERSE_ABBREV_MAP = {v: k for k, v in ABBREV_MAP.items()}

# ── Stat key mapping ───────────────────────────────────────────────────────
# ESPN MLB boxscore uses a different stat layout than basketball.
# The summary endpoint returns per-player stat lines in a "statistics" array.
# Unlike basketball, baseball boxscore structure varies (batting vs pitching).
#
# Batting stat labels (typical): ['AB', 'R', 'H', 'RBI', 'HR', 'BB', 'K', 'AVG', 'OBP', 'SLG']
# Pitching stat labels (typical): ['IP', 'H', 'R', 'ER', 'BB', 'K', 'HR', 'PC-ST', 'ERA']
#
# We map our stat_key to (category, espn_label) pairs.
BATTING_STAT_MAP: Dict[str, str] = {
    "hits": "H",
    "runs": "R",
    "rbi": "RBI",
    "home_runs": "HR",
    "singles": "H",       # Singles require special calc: H - 2B - 3B - HR
    "doubles": "2B",
    "steals": "SB",
    "walks": "BB",
    "strikeouts": "K",     # Batter strikeouts (not pitcher)
    "at_bats": "AB",
}

PITCHING_STAT_MAP: Dict[str, str] = {
    "strikeouts": "K",       # Pitcher Ks (shared key, disambiguated by player role)
    "hits_allowed": "H",
    "earned_runs_allowed": "ER",
    "walks_allowed": "BB",
    "outs_recorded": "IP",   # Needs special conversion: IP to outs
}

# Combo stat keys — sum of component batting stats
COMBO_STATS: Dict[str, List[str]] = {
    "total_bases": [],       # Computed: 1*1B + 2*2B + 3*3B + 4*HR
    "runs_hits_rbis": ["R", "H", "RBI"],
}

# All supported stat keys (for eligibility checks)
SUPPORTED_STATS = set(BATTING_STAT_MAP) | set(PITCHING_STAT_MAP) | set(COMBO_STATS)

LAST_GAMES_INFO: Dict[str, Any] = {}


# ── Cache helpers ──────────────────────────────────────────────────────────

def _cache_read(path: Path) -> Optional[Any]:
    if path.exists():
        try:
            STATS["cache_hits"] += 1
            return json.loads(path.read_text())
        except Exception:
            STATS["bad_cache"] += 1
            return None
    return None


def _cache_write(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.write_text(json.dumps(payload))
    except Exception:
        pass


def _get_json(url: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    if requests is None:
        raise RuntimeError("requests not installed")
    for attempt in range(2):
        try:
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code != 200:
                time.sleep(0.5)
                continue
            STATS["fetches"] += 1
            return resp.json()
        except Exception:
            time.sleep(0.5)
    return None


# ── Team code conversion ──────────────────────────────────────────────────

def _to_espn_code(our_code: str) -> str:
    return ABBREV_MAP.get(our_code.upper(), our_code.upper())


def _from_espn_code(espn_code: str) -> str:
    return REVERSE_ABBREV_MAP.get(espn_code.upper(), espn_code.upper())


# ── Scoreboard ────────────────────────────────────────────────────────────

def get_scoreboard(date_str: str, refresh_cache: bool = False) -> List[Dict[str, Any]]:
    """Fetch all MLB games for a date."""
    date_compact = date_str.replace("-", "")
    cache_path = CACHE_DIR / f"espn_scoreboard_{date_str}.json"

    if not refresh_cache:
        cached = _cache_read(cache_path)
        if cached is not None and isinstance(cached, list):
            return cached

    data = _get_json(f"{BASE_URL}/scoreboard", {"dates": date_compact, "limit": 50})
    events: List[Dict[str, Any]] = []
    if data and isinstance(data.get("events"), list):
        events = data["events"]

    _cache_write(cache_path, events)
    STATS["refreshes"] += 1
    return events


def _match_game(events: List[Dict[str, Any]], away: str, home: str) -> Optional[Dict[str, Any]]:
    """Find a game matching team codes (tries both orientations)."""
    away_espn = _to_espn_code(away)
    home_espn = _to_espn_code(home)

    for ev in events:
        comps = ev.get("competitions", [])
        if not comps:
            continue
        competitors = comps[0].get("competitors", [])
        if len(competitors) < 2:
            continue

        espn_home = None
        espn_away = None
        for c in competitors:
            abbr = c.get("team", {}).get("abbreviation", "").upper()
            if c.get("homeAway") == "home":
                espn_home = abbr
            elif c.get("homeAway") == "away":
                espn_away = abbr

        if espn_away == away_espn and espn_home == home_espn:
            return ev
        if espn_away == home_espn and espn_home == away_espn:
            return ev

    return None


def _extract_scores(event: Dict[str, Any], away: str, home: str) -> Optional[Dict[str, Any]]:
    """Extract scores and status from an ESPN event."""
    comp = event.get("competitions", [{}])[0]
    competitors = comp.get("competitors", [])
    status_obj = comp.get("status", {}).get("type", {})
    status = status_obj.get("name", "UNKNOWN")

    away_espn = _to_espn_code(away)
    home_espn = _to_espn_code(home)

    home_score = None
    away_score = None
    match_orientation = "direct"

    for c in competitors:
        abbr = c.get("team", {}).get("abbreviation", "").upper()
        score = c.get("score")
        if score is not None:
            try:
                score = int(score)
            except (ValueError, TypeError):
                score = None

        if abbr == home_espn and c.get("homeAway") == "home":
            home_score = score
        elif abbr == away_espn and c.get("homeAway") == "away":
            away_score = score
        elif abbr == home_espn and c.get("homeAway") == "away":
            away_score = score
            match_orientation = "flipped"
        elif abbr == away_espn and c.get("homeAway") == "home":
            home_score = score
            match_orientation = "flipped"

    if home_score is None and away_score is None:
        for c in competitors:
            score = c.get("score")
            if score is not None:
                try:
                    score = int(score)
                except (ValueError, TypeError):
                    score = None
            if c.get("homeAway") == "home":
                home_score = score
            elif c.get("homeAway") == "away":
                away_score = score

    return {
        "home_score": home_score,
        "away_score": away_score,
        "status": status,
        "match_orientation": match_orientation,
        "espn_event_id": event.get("id"),
        "espn_name": event.get("shortName"),
    }


# ── Boxscore / Player Stats ──────────────────────────────────────────────

def get_boxscore(espn_event_id: str, refresh_cache: bool = False) -> Optional[Dict[str, Any]]:
    """Fetch game summary/boxscore."""
    cache_path = CACHE_DIR / f"espn_boxscore_{espn_event_id}.json"

    if not refresh_cache:
        cached = _cache_read(cache_path)
        if cached is not None and isinstance(cached, dict):
            return cached

    data = _get_json(f"{BASE_URL}/summary", {"event": espn_event_id})
    if data and isinstance(data, dict):
        _cache_write(cache_path, data)
        STATS["refreshes"] += 1
        return data

    return None


def _normalize_name(name: str) -> str:
    """Normalize player name for matching."""
    name = name.lower()
    name = re.sub(r"[^a-z0-9 ]+", "", name)
    name = name.replace(" iii", "").replace(" jr", "").replace(" ii", "").replace(" sr", "")
    name = " ".join(name.split())
    return name


def _parse_stat_value(raw_val: str) -> Optional[float]:
    """Parse a stat value. Handles 'X-Y' format and plain numbers."""
    if not raw_val or raw_val == "--" or raw_val == "":
        return None
    if "-" in raw_val:
        parts = raw_val.split("-")
        try:
            return float(parts[0])
        except (ValueError, IndexError):
            return None
    try:
        return float(raw_val)
    except ValueError:
        return None


def _ip_to_outs(ip_str: str) -> Optional[float]:
    """Convert innings pitched (e.g. '6.2') to outs (e.g. 20).

    ESPN uses decimal notation where .1 = 1/3 inning, .2 = 2/3 inning.
    """
    try:
        val = float(ip_str)
    except (ValueError, TypeError):
        return None
    whole = int(val)
    frac = round((val - whole) * 10)
    return float(whole * 3 + frac)


def _compute_total_bases(player_stats: Dict[str, str], labels: List[str]) -> Optional[float]:
    """Compute total bases from boxscore stats: 1B*1 + 2B*2 + 3B*3 + HR*4.

    Singles = H - 2B - 3B - HR.
    """
    def _get(label: str) -> Optional[float]:
        if label in labels:
            idx = labels.index(label)
            raw = player_stats.get(idx)
            if raw is not None:
                return _parse_stat_value(str(raw))
        return None

    h = _get("H")
    doubles = _get("2B") or 0.0
    triples = _get("3B") or 0.0
    hr = _get("HR") or 0.0

    if h is None:
        return None

    singles = h - doubles - triples - hr
    if singles < 0:
        singles = 0

    return singles + (doubles * 2) + (triples * 3) + (hr * 4)


def _is_pitching_stat(stat_key: str) -> bool:
    return stat_key in PITCHING_STAT_MAP


def _find_player_stat(
    summary: Dict[str, Any],
    player_hint: str,
    stat_key: str,
) -> Optional[Dict[str, Any]]:
    """Search boxscore for a player's stat value.

    Handles batting and pitching stats separately since they appear in
    different stat groups in the ESPN boxscore.
    """
    player_norm = _normalize_name(player_hint)

    boxscore = summary.get("boxscore", {})
    team_stats_list = boxscore.get("players", [])

    for team_block in team_stats_list:
        stat_groups = team_block.get("statistics", [])

        for stat_group in stat_groups:
            group_name = (stat_group.get("name") or stat_group.get("type") or "").lower()
            labels = stat_group.get("labels", [])

            # Determine if this group matches the stat type we need
            is_pitching_group = "pitch" in group_name
            need_pitching = _is_pitching_stat(stat_key)

            # For stats that exist in both (like strikeouts), match the right group
            if need_pitching and not is_pitching_group:
                continue
            if not need_pitching and is_pitching_group and stat_key != "strikeouts":
                continue

            athletes = stat_group.get("athletes", [])
            for athlete_entry in athletes:
                athlete = athlete_entry.get("athlete", {})
                display_name = athlete.get("displayName", "")
                name_norm = _normalize_name(display_name)

                if name_norm != player_norm and player_norm not in name_norm and name_norm not in player_norm:
                    continue

                stats_arr = athlete_entry.get("stats", [])
                stats_dict = {i: v for i, v in enumerate(stats_arr)}

                # Handle combo/computed stats
                if stat_key == "total_bases":
                    value = _compute_total_bases(stats_dict, labels)
                    if value is not None:
                        return {
                            "value": value,
                            "player_id": str(athlete.get("id", "")),
                            "meta": {
                                "match_method": "name_match",
                                "espn_name": display_name,
                                "search_name": player_hint,
                                "stat_label": "total_bases (computed)",
                                "stat_key": stat_key,
                            },
                        }
                    continue

                if stat_key == "runs_hits_rbis":
                    component_labels = ["R", "H", "RBI"]
                    total = 0.0
                    all_found = True
                    for cl in component_labels:
                        if cl in labels:
                            idx = labels.index(cl)
                            raw = stats_dict.get(idx)
                            val = _parse_stat_value(str(raw)) if raw is not None else None
                            if val is not None:
                                total += val
                            else:
                                all_found = False
                        else:
                            all_found = False
                    if all_found:
                        return {
                            "value": total,
                            "player_id": str(athlete.get("id", "")),
                            "meta": {
                                "match_method": "name_match",
                                "espn_name": display_name,
                                "search_name": player_hint,
                                "stat_label": "runs_hits_rbis (computed)",
                                "stat_key": stat_key,
                            },
                        }
                    continue

                # Simple stat lookup
                if need_pitching:
                    espn_label = PITCHING_STAT_MAP.get(stat_key)
                else:
                    espn_label = BATTING_STAT_MAP.get(stat_key)

                if not espn_label or espn_label not in labels:
                    continue

                idx = labels.index(espn_label)
                if idx >= len(stats_arr):
                    continue

                raw_val = stats_arr[idx]

                # Special handling for outs_recorded (IP → outs)
                if stat_key == "outs_recorded":
                    value = _ip_to_outs(str(raw_val))
                else:
                    value = _parse_stat_value(str(raw_val))

                if value is not None:
                    return {
                        "value": value,
                        "player_id": str(athlete.get("id", "")),
                        "meta": {
                            "match_method": "name_match",
                            "espn_name": display_name,
                            "search_name": player_hint,
                            "stat_label": espn_label,
                            "stat_key": stat_key,
                            "raw_value": raw_val,
                        },
                    }

    return None


# ── High-level grading interface ──────────────────────────────────────────

def fetch_game_result(
    event_key: str,
    away: str,
    home: str,
    date_str: str,
    refresh_cache: bool = False,
) -> Optional[Dict[str, Any]]:
    """Fetch game result for MLB grading."""
    events = get_scoreboard(date_str, refresh_cache=refresh_cache)

    global LAST_GAMES_INFO
    LAST_GAMES_INFO = {
        "date": date_str,
        "games_count": len(events),
        "used_cache": not refresh_cache,
        "provider": "espn_mlb",
    }

    matched = _match_game(events, away, home)
    if not matched:
        return None

    result = _extract_scores(matched, away, home)
    if not result:
        return None

    result["games_info"] = {
        "date": date_str,
        "games_count": len(events),
        "provider": "espn_mlb",
        "espn_event_id": result.get("espn_event_id"),
        "match_orientation": result.get("match_orientation", "direct"),
    }

    return result


def fetch_player_statline(
    event_key: str,
    player_id: str,
    stat_key: str,
    date_str: str,
    away: str,
    home: str,
    refresh_cache: bool = False,
) -> Optional[Dict[str, Any]]:
    """Fetch player stat for MLB prop grading."""
    if stat_key not in SUPPORTED_STATS:
        return None

    events = get_scoreboard(date_str, refresh_cache=refresh_cache)
    matched = _match_game(events, away, home)
    if not matched:
        return None

    espn_event_id = matched.get("id")
    if not espn_event_id:
        return None

    summary = get_boxscore(str(espn_event_id), refresh_cache=refresh_cache)
    if not summary:
        return None

    player_hint = player_id.replace("MLB:", "").replace("_", " ")
    return _find_player_stat(summary, player_hint, stat_key)


def is_stat_supported(stat_key: str) -> bool:
    """Check if a stat key is supported for grading."""
    return stat_key in SUPPORTED_STATS
