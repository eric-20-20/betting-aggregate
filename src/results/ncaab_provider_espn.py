"""NCAAB score provider using ESPN's free public API.

No API key required. Provides game scores and player boxscores for
NCAA Division I Men's Basketball.

Endpoints:
  Scoreboard: site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard
  Summary:    site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary
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

CACHE_DIR = Path("data/cache/results/ncaab")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball"

STATS = {"cache_hits": 0, "fetches": 0, "refreshes": 0, "bad_cache": 0}

# ESPN stat label → index in the stats array (0-based)
# Labels: ['MIN', 'PTS', 'FG', '3PT', 'FT', 'REB', 'AST', 'TO', 'STL', 'BLK']
STAT_LABEL_INDEX = {
    "MIN": 0,
    "PTS": 1,
    "FG": 2,
    "3PT": 3,
    "FT": 4,
    "REB": 5,
    "AST": 6,
    "TO": 7,
    "STL": 8,
    "BLK": 9,
}

# Map our stat_key names to ESPN label names
STAT_KEY_TO_LABEL = {
    "points": "PTS",
    "rebounds": "REB",
    "assists": "AST",
    "steals": "STL",
    "blocks": "BLK",
    "threes_made": "3PT",
    "turnovers": "TO",
    "minutes": "MIN",
    "field_goals_made": "FG",
    "free_throws_made": "FT",
}

# Map known abbreviation mismatches: our_code → ESPN code
# Derived by comparing event_key team codes against ESPN scoreboard abbreviations.
ABBREV_MAP: Dict[str, str] = {
    "AC": "AMER",    # American University
    "AFA": "AF",     # Air Force
    "ALBY": "UALB",  # UAlbany
    "BSU": "BOIS",   # Boise State
    "BUFF": "BUF",   # Buffalo
    "CAMP": "CAM",   # Campbell
    "CCAR": "CCU",   # Coastal Carolina
    "CHAR": "COFC",  # College of Charleston
    "CHAT": "UTC",   # Chattanooga
    "CLEV": "CLE",   # Cleveland State
    "CLMB": "COLU",  # Columbia
    "CSB": "CSUB",   # Cal State Bakersfield
    "CSN": "CSUN",   # Cal State Northridge
    "DET": "DETM",   # Detroit Mercy
    "EKY": "EKU",    # Eastern Kentucky
    "GTOWN": "GTWN", # Georgetown
    "IND": "IU",     # Indiana
    "IW": "UIW",     # Incarnate Word
    "JVST": "JXST",  # Jacksonville State
    "KAN": "KU",     # Kansas
    "LINW": "LIN",   # Lindenwood
    "MASSL": "UML",  # UMass Lowell
    "MCNS": "MCN",   # McNeese
    "MIL": "MILW",   # Milwaukee
    "MIO": "M-OH",   # Miami (Ohio)
    "MIZZ": "MIZ",   # Missouri
    "MOSU": "MOST",  # Missouri State
    "MTU": "MTSU",   # Middle Tennessee
    "MURR": "MUR",   # Murray State
    "NCST": "NCSU",  # NC State
    "NHC": "UNH",    # New Hampshire (NHC = NH Cats)
    "NIAG": "NIA",   # Niagara
    "NW": "NU",      # Northwestern
    "OH": "OHIO",    # Ohio (Bobcats)
    "OKLA": "OU",    # Oklahoma
    "SBON": "SBU",   # St. Bonaventure
    "SCAR": "SC",    # South Carolina
    "SPC": "SPU",    # Saint Peter's
    "STLO": "SLU",   # Saint Louis
    "STON": "STO",   # Stonehill
    "TARL": "TAR",   # Tarleton State
    "TXAM": "TA&M",  # Texas A&M
    "UALR": "LR",    # Little Rock (Arkansas-Little Rock → ESPN: LR)
    "UCRV": "UCR",   # UC Riverside
    "ULL": "UL",     # Louisiana (Lafayette)
    "UMKC": "KC",    # Kansas City (UMKC)
    "UTRGV": "RGV",  # UT Rio Grande Valley
    "UWGA": "WGA",   # West Georgia
    "WAS": "WASH",   # Washington
}

# Reverse map: ESPN code → our code
REVERSE_ABBREV_MAP = {v: k for k, v in ABBREV_MAP.items()}

LAST_GAMES_INFO: Dict[str, Any] = {}


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
            continue
    return None


def _normalize_code(code: str) -> str:
    """Normalize a team code for comparison."""
    return code.upper().strip()


def _to_espn_code(our_code: str) -> str:
    """Convert our team code to ESPN's abbreviation."""
    return ABBREV_MAP.get(our_code.upper(), our_code.upper())


def _from_espn_code(espn_code: str) -> str:
    """Convert ESPN abbreviation back to our team code."""
    return REVERSE_ABBREV_MAP.get(espn_code.upper(), espn_code.upper())


def get_scoreboard(date_str: str, refresh_cache: bool = False) -> List[Dict[str, Any]]:
    """Fetch all D1 NCAAB games for a date.

    Args:
        date_str: Date in YYYY-MM-DD format
        refresh_cache: Force fresh fetch

    Returns:
        List of ESPN event objects
    """
    date_compact = date_str.replace("-", "")
    cache_path = CACHE_DIR / f"espn_scoreboard_{date_str}.json"

    if not refresh_cache:
        cached = _cache_read(cache_path)
        if cached is not None and isinstance(cached, list):
            return cached

    data = _get_json(f"{BASE_URL}/scoreboard", {
        "dates": date_compact,
        "groups": 50,  # Division I
        "limit": 400,
    })

    events = []
    if data and isinstance(data.get("events"), list):
        events = data["events"]

    _cache_write(cache_path, events)
    STATS["refreshes"] += 1
    return events


def _match_game(events: List[Dict[str, Any]], away: str, home: str) -> Optional[Dict[str, Any]]:
    """Find a game in the scoreboard matching the team codes.

    Tries both orientations (in case our away/home doesn't match ESPN's).
    """
    away_espn = _to_espn_code(away)
    home_espn = _to_espn_code(home)

    for ev in events:
        comps = ev.get("competitions", [])
        if not comps:
            continue
        comp = comps[0]
        competitors = comp.get("competitors", [])
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

        # Direct match
        if espn_away == away_espn and espn_home == home_espn:
            return ev
        # Flipped orientation
        if espn_away == home_espn and espn_home == away_espn:
            return ev

    return None


def _extract_scores(event: Dict[str, Any], away: str, home: str) -> Optional[Dict[str, Any]]:
    """Extract scores from an ESPN event object."""
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
        # Fallback: match by position regardless of code
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


def get_boxscore(espn_event_id: str, refresh_cache: bool = False) -> Optional[Dict[str, Any]]:
    """Fetch the game summary/boxscore from ESPN.

    Args:
        espn_event_id: ESPN event ID (string)
        refresh_cache: Force fresh fetch

    Returns:
        Full summary dict or None
    """
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


def _parse_stat_value(raw_val: str, label: str) -> Optional[float]:
    """Parse ESPN stat value. Handles 'X-Y' format (made-attempted) and plain numbers.

    For made-attempted stats (FG, 3PT, FT), returns the 'made' count.
    """
    if not raw_val or raw_val == "--":
        return None

    if "-" in raw_val and label in ("FG", "3PT", "FT"):
        parts = raw_val.split("-")
        try:
            return float(parts[0])
        except (ValueError, IndexError):
            return None

    try:
        return float(raw_val)
    except ValueError:
        return None


# ── High-level grading interface ──────────────────────────────────────────

def fetch_game_result(
    event_key: str,
    away: str,
    home: str,
    date_str: str,
    refresh_cache: bool = False,
) -> Optional[Dict[str, Any]]:
    """Fetch game result for NCAAB grading.

    Args:
        event_key: Event key like NCAAB:2026:02:15:DUKE@UNC
        away: Away team code
        home: Home team code
        date_str: Date string YYYY-MM-DD
        refresh_cache: Force refresh cached data

    Returns:
        Dict with home_score, away_score, status, games_info or None
    """
    events = get_scoreboard(date_str, refresh_cache=refresh_cache)

    global LAST_GAMES_INFO
    LAST_GAMES_INFO = {
        "date": date_str,
        "games_count": len(events),
        "used_cache": not refresh_cache,
        "provider": "espn",
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
        "provider": "espn",
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
    """Fetch player stats for NCAAB prop grading.

    Args:
        event_key: Event key
        player_id: Player identifier (e.g., "NCAAB:j_smith" or "john smith")
        stat_key: Stat to fetch (points, rebounds, etc.)
        date_str: Game date YYYY-MM-DD
        away: Away team code
        home: Home team code
        refresh_cache: Force refresh

    Returns:
        Dict with value, player info, or None
    """
    # Map our stat key to ESPN label
    espn_label = STAT_KEY_TO_LABEL.get(stat_key)
    if not espn_label:
        return None
    label_idx = STAT_LABEL_INDEX.get(espn_label)
    if label_idx is None:
        return None

    # Find the game
    events = get_scoreboard(date_str, refresh_cache=refresh_cache)
    matched = _match_game(events, away, home)
    if not matched:
        return None

    espn_event_id = matched.get("id")
    if not espn_event_id:
        return None

    # Fetch boxscore
    summary = get_boxscore(str(espn_event_id), refresh_cache=refresh_cache)
    if not summary:
        return None

    boxscore = summary.get("boxscore", {})
    teams_stats = boxscore.get("players", [])

    # Normalize player name for matching
    player_hint = player_id.replace("NCAAB:", "").replace("_", " ")
    player_norm = _normalize_name(player_hint)

    # Search all teams' players
    for team_block in teams_stats:
        stat_groups = team_block.get("statistics", [])
        if not stat_groups:
            continue

        # Find the labels array to verify index
        labels = stat_groups[0].get("labels", [])
        actual_idx = None
        for i, lbl in enumerate(labels):
            if lbl == espn_label:
                actual_idx = i
                break
        if actual_idx is None:
            actual_idx = label_idx  # fall back to hardcoded

        athletes = stat_groups[0].get("athletes", [])
        for athlete_entry in athletes:
            athlete = athlete_entry.get("athlete", {})
            display_name = athlete.get("displayName", "")
            name_norm = _normalize_name(display_name)

            if name_norm == player_norm or player_norm in name_norm or name_norm in player_norm:
                stats_arr = athlete_entry.get("stats", [])
                if actual_idx < len(stats_arr):
                    raw_val = stats_arr[actual_idx]
                    value = _parse_stat_value(raw_val, espn_label)
                    if value is not None:
                        return {
                            "value": value,
                            "player_id": str(athlete.get("id", "")),
                            "meta": {
                                "match_method": "name_match",
                                "espn_name": display_name,
                                "search_name": player_hint,
                                "stat_label": espn_label,
                                "raw_value": raw_val,
                            },
                        }

    return None


def provider_info() -> Dict[str, Any]:
    """Return provider metadata for logging."""
    return {
        "provider": "ncaab_espn",
        "base": BASE_URL,
        "key_present": True,  # No key needed — free API
    }
