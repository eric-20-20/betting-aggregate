"""
Dimers NBA pick normalizer.

Normalizes raw Dimers records into the consensus-compatible schema.
"""

from __future__ import annotations

import os
import re
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from betql_utils import slugify_player_name
from src.player_aliases_nba import normalize_player_key
from store import data_store
from utils import normalize_text

# Stat type mapping to canonical keys
STAT_MAP = {
    # Points
    "points": "points",
    "total points": "points",
    "pts": "points",
    # Rebounds
    "rebounds": "rebounds",
    "total rebounds": "rebounds",
    "reb": "rebounds",
    "rebs": "rebounds",
    # Assists
    "assists": "assists",
    "total assists": "assists",
    "ast": "assists",
    "asts": "assists",
    # Threes
    "threes": "threes",
    "3-pointers": "threes",
    "3-pointers made": "threes",
    "three pointers": "threes",
    "3pt": "threes",
    "3pm": "threes",
    "made threes": "threes",
    # Steals
    "steals": "steals",
    "stl": "steals",
    # Blocks
    "blocks": "blocks",
    "blk": "blocks",
    # Turnovers
    "turnovers": "turnovers",
    "to": "turnovers",
    # Combos
    "pts+reb+ast": "pts_reb_ast",
    "points + rebounds + assists": "pts_reb_ast",
    "pra": "pts_reb_ast",
    "pts+reb": "pts_reb",
    "points + rebounds": "pts_reb",
    "pts+ast": "pts_ast",
    "points + assists": "pts_ast",
    "reb+ast": "reb_ast",
    "rebounds + assists": "reb_ast",
}

# Team name to code fallback mapping
TEAM_FALLBACK = {
    "hawks": "ATL",
    "celtics": "BOS",
    "nets": "BKN",
    "hornets": "CHA",
    "bulls": "CHI",
    "cavaliers": "CLE",
    "cavs": "CLE",
    "mavericks": "DAL",
    "mavs": "DAL",
    "nuggets": "DEN",
    "pistons": "DET",
    "warriors": "GSW",
    "rockets": "HOU",
    "pacers": "IND",
    "clippers": "LAC",
    "lakers": "LAL",
    "grizzlies": "MEM",
    "heat": "MIA",
    "bucks": "MIL",
    "timberwolves": "MIN",
    "wolves": "MIN",
    "pelicans": "NOP",
    "knicks": "NYK",
    "thunder": "OKC",
    "magic": "ORL",
    "76ers": "PHI",
    "sixers": "PHI",
    "suns": "PHX",
    "blazers": "POR",
    "trail blazers": "POR",
    "kings": "SAC",
    "spurs": "SAS",
    "raptors": "TOR",
    "jazz": "UTA",
    "wizards": "WAS",
}


def _parse_dt(val: Any) -> Optional[datetime]:
    """Parse datetime from various formats."""
    if isinstance(val, datetime):
        dt = val
    elif isinstance(val, str):
        try:
            dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _map_team(alias: Optional[str]) -> Optional[str]:
    """Map team name/alias to canonical code."""
    if not alias:
        return None

    # Try data_store lookup first
    codes = data_store.lookup_team_code(alias)
    if len(codes) == 1:
        return next(iter(codes))

    # Fallback to manual mapping
    norm = normalize_text(alias).lower()
    if norm in TEAM_FALLBACK:
        return TEAM_FALLBACK[norm]

    # If it's already a short code
    if len(alias) <= 3:
        return alias.upper()

    return None


def _map_stat_key(stat_type: Optional[str]) -> Optional[str]:
    """Map stat type string to canonical stat key."""
    if not stat_type:
        return None

    # Normalize and lookup
    norm = normalize_text(stat_type).lower()

    # Direct match
    if norm in STAT_MAP:
        return STAT_MAP[norm]

    # Partial match
    for key, value in STAT_MAP.items():
        if key in norm:
            return value

    return None


def _parse_matchup(matchup_hint: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """Parse matchup hint into away and home team codes."""
    if not matchup_hint:
        return None, None

    # Try common patterns
    patterns = [
        r"(\w+(?:\s+\w+)?)\s+(?:vs\.?|@|at)\s+(\w+(?:\s+\w+)?)",
        r"(\w+)\s+(?:vs\.?|@)\s+(\w+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, matchup_hint, re.IGNORECASE)
        if match:
            away_alias = match.group(1).strip()
            home_alias = match.group(2).strip()
            away_code = _map_team(away_alias)
            home_code = _map_team(home_alias)
            if away_code and home_code:
                return away_code, home_code

    return None, None


def _parse_odds(odds_str: Optional[str]) -> Optional[int]:
    """Parse odds string to integer."""
    if not odds_str:
        return None
    match = re.search(r"([+-]?\d+)", odds_str)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None


def _parse_event_date(date_hint: Optional[str], observed_at: str) -> datetime:
    """
    Parse event date hint into datetime.
    Falls back to observed_at if parsing fails.
    """
    if date_hint:
        # Try to parse "Feb 19" style dates
        try:
            # Assume current year if not specified
            current_year = datetime.now().year
            # Try various formats
            for fmt in ["%b %d %Y", "%B %d %Y", "%b %d", "%B %d"]:
                try:
                    if "%Y" in fmt:
                        dt = datetime.strptime(date_hint, fmt)
                    else:
                        dt = datetime.strptime(f"{date_hint} {current_year}", f"{fmt} %Y")
                    return dt.replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
        except Exception:
            pass

    # Fallback to observed timestamp
    return _parse_dt(observed_at) or datetime.now(timezone.utc)


def normalize_dimers_prop(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a Dimers player prop record."""
    # Extract basic fields
    player_name = raw.get("player_name")
    stat_type = raw.get("stat_type")
    direction = raw.get("direction")
    line = raw.get("line")

    # Map stat key
    stat_key = _map_stat_key(stat_type)

    # Build player key
    player_slug = slugify_player_name(player_name) if player_name else None
    player_key = normalize_player_key(f"NBA:{player_slug}") if player_slug else None

    # Parse teams from matchup
    away_team, home_team = _parse_matchup(raw.get("matchup_hint"))

    # Build event keys
    event_dt = _parse_event_date(raw.get("event_date_hint"), raw.get("observed_at_utc", ""))
    day_key = f"NBA:{event_dt.year:04d}:{event_dt.month:02d}:{event_dt.day:02d}"
    event_key = f"{day_key}:{away_team}@{home_team}" if away_team and home_team else None
    matchup_key = f"{day_key}:{home_team}-{away_team}" if home_team and away_team else None

    # Build selection and side
    direction_upper = direction.upper() if direction else None
    selection = None
    side = None

    if player_key and stat_key and direction_upper:
        selection = f"{player_key}::{stat_key}::{direction_upper}"
        side = f"player_{direction_upper.lower()}"

    # Check eligibility
    eligible = True
    ineligibility_reason = None

    if not player_key:
        eligible, ineligibility_reason = False, "unparsed_player"
    elif not stat_key:
        eligible, ineligibility_reason = False, "unparsed_stat"
    elif not direction_upper:
        eligible, ineligibility_reason = False, "unparsed_direction"
    elif line is None:
        eligible, ineligibility_reason = False, "unparsed_line"

    # Parse odds
    odds = _parse_odds(raw.get("best_odds"))

    return {
        "provenance": {
            "source_id": "dimers",
            "source_surface": raw.get("source_surface", "dimers_best_props"),
            "sport": "NBA",
            "observed_at_utc": raw.get("observed_at_utc"),
            "canonical_url": raw.get("canonical_url"),
            "raw_fingerprint": raw.get("raw_fingerprint"),
            "raw_pick_text": raw.get("raw_pick_text"),
            "raw_block": raw.get("raw_block"),
            "expert_name": "Dimers AI Model",
            "expert_handle": None,
            "expert_profile": None,
            "expert_slug": None,
            "tailing_handle": None,
            "matchup_hint": raw.get("matchup_hint"),
            # Dimers-specific fields
            "probability_pct": raw.get("probability_pct"),
            "edge_pct": raw.get("edge_pct"),
            "best_odds": raw.get("best_odds"),
            "badge": raw.get("badge"),
        },
        "event": {
            "sport": "NBA",
            "event_key": event_key,
            "day_key": day_key,
            "matchup_key": matchup_key,
            "away_team": away_team,
            "home_team": home_team,
            "event_start_time_utc": event_dt.isoformat() if event_dt else None,
        },
        "market": {
            "market_type": "player_prop",
            "market_family": "player_prop",
            "side": side,
            "selection": selection,
            "line": line,
            "odds": odds,
            "stat_key": stat_key,
            "player_key": player_key,
            "player_name": player_name,
        },
        "eligible_for_consensus": eligible,
        "ineligibility_reason": ineligibility_reason,
        "eligibility": {
            "eligible_for_consensus": eligible,
            "ineligibility_reason": ineligibility_reason,
        },
    }


def normalize_dimers_game_bet(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a Dimers game bet record (moneyline, spread, total)."""
    # Parse teams from matchup
    away_team, home_team = _parse_matchup(raw.get("matchup_hint"))

    # Try to extract team from pick
    team_pick = raw.get("team_pick")
    if team_pick:
        team_code = _map_team(team_pick)
    else:
        team_code = None

    # Build event keys
    event_dt = _parse_event_date(raw.get("event_date_hint"), raw.get("observed_at_utc", ""))
    day_key = f"NBA:{event_dt.year:04d}:{event_dt.month:02d}:{event_dt.day:02d}"
    event_key = f"{day_key}:{away_team}@{home_team}" if away_team and home_team else None
    matchup_key = f"{day_key}:{home_team}-{away_team}" if home_team and away_team else None

    # Determine market type and build selection
    market_type = raw.get("market_type", "unknown")
    pick_text = raw.get("raw_pick_text", "")

    selection = None
    side = None
    line = None

    if market_type == "moneyline":
        if team_code:
            selection = f"{team_code}_ml"
            side = "team"
    elif market_type == "spread":
        # Extract spread line from pick text
        spread_match = re.search(r"([+-]\d+\.?\d*)", pick_text)
        if spread_match:
            try:
                line = float(spread_match.group(1))
            except ValueError:
                line = None
        if team_code:
            selection = f"{team_code}_spread"
            side = "team"
    elif market_type == "total":
        # Extract total line and direction
        total_match = re.search(r"(over|under)\s+([\d.]+)", pick_text, re.IGNORECASE)
        if total_match:
            side = total_match.group(1).lower()
            try:
                line = float(total_match.group(2))
            except ValueError:
                line = None
            selection = "game_total"

    # Parse odds
    odds = _parse_odds(raw.get("best_odds"))

    # Check eligibility
    eligible = True
    ineligibility_reason = None

    if not away_team or not home_team:
        eligible, ineligibility_reason = False, "unparsed_team"
    elif market_type == "unknown":
        eligible, ineligibility_reason = False, "unknown_market_type"
    elif market_type in ["moneyline", "spread"] and not team_code:
        eligible, ineligibility_reason = False, "unparsed_team_pick"
    elif market_type == "spread" and line is None:
        eligible, ineligibility_reason = False, "unparsed_line"

    return {
        "provenance": {
            "source_id": "dimers",
            "source_surface": raw.get("source_surface", "dimers_best_bets"),
            "sport": "NBA",
            "observed_at_utc": raw.get("observed_at_utc"),
            "canonical_url": raw.get("canonical_url"),
            "raw_fingerprint": raw.get("raw_fingerprint"),
            "raw_pick_text": raw.get("raw_pick_text"),
            "raw_block": raw.get("raw_block"),
            "expert_name": "Dimers AI Model",
            "expert_handle": None,
            "expert_profile": None,
            "expert_slug": None,
            "tailing_handle": None,
            "matchup_hint": raw.get("matchup_hint"),
            # Dimers-specific fields
            "probability_pct": raw.get("probability_pct"),
            "edge_pct": raw.get("edge_pct"),
            "best_odds": raw.get("best_odds"),
            "badge": raw.get("badge"),
        },
        "event": {
            "sport": "NBA",
            "event_key": event_key,
            "day_key": day_key,
            "matchup_key": matchup_key,
            "away_team": away_team,
            "home_team": home_team,
            "event_start_time_utc": event_dt.isoformat() if event_dt else None,
        },
        "market": {
            "market_type": market_type,
            "market_family": "standard",
            "side": side,
            "selection": selection,
            "line": line,
            "odds": odds,
            "team_code": team_code,
        },
        "eligible_for_consensus": eligible,
        "ineligibility_reason": ineligibility_reason,
        "eligibility": {
            "eligible_for_consensus": eligible,
            "ineligibility_reason": ineligibility_reason,
        },
    }


def normalize_dimers_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize a single Dimers record based on market family.
    """
    market_family = raw.get("market_family", "standard")

    if market_family == "player_prop":
        return normalize_dimers_prop(raw)
    else:
        return normalize_dimers_game_bet(raw)


def normalize_dimers_records(
    raw_records: List[Any],
    sport: str = "NBA",
    debug: bool = False,
) -> List[Dict[str, Any]]:
    """
    Normalize a list of raw Dimers records.

    Args:
        raw_records: List of RawDimersRecord objects or dicts
        sport: Sport identifier (currently only NBA supported)
        debug: Enable debug output

    Returns:
        List of normalized record dicts
    """
    normalized = []

    for i, raw in enumerate(raw_records):
        try:
            # Convert dataclass to dict if needed
            if hasattr(raw, "__dataclass_fields__"):
                raw_dict = asdict(raw)
            else:
                raw_dict = raw

            result = normalize_dimers_record(raw_dict)
            normalized.append(result)

            if debug and i < 5:
                market = result.get("market", {})
                event = result.get("event", {})
                print(
                    f"[dimers-normalize] Record {i}: "
                    f"type={market.get('market_type')} "
                    f"eligible={result.get('eligible_for_consensus')} "
                    f"event={event.get('event_key')}"
                )

        except Exception as e:
            if debug:
                print(f"[dimers-normalize] Error normalizing record {i}: {e}")
            continue

    if debug:
        eligible_count = sum(1 for r in normalized if r.get("eligible_for_consensus"))
        print(f"[dimers-normalize] Normalized {len(normalized)} records, {eligible_count} eligible")

    return normalized
