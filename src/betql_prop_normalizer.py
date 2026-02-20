from __future__ import annotations

import logging
from typing import Dict, Optional

from betql_utils import slugify_player_name
from src.player_aliases_nba import normalize_player_key
from utils import normalize_text

logger = logging.getLogger(__name__)

STAT_MARKET_MAP = {
    "POINTS": "player_points",
    "POINT": "player_points",
    "REB": "player_rebounds",
    "REBOUNDS": "player_rebounds",
    "ASSISTS": "player_assists",
    "AST": "player_assists",
    "MADE THREES": "player_threes",
    "THREES": "player_threes",
    "3PT": "player_threes",
}


def _map_market(stat_raw: Optional[str]) -> str:
    if not stat_raw:
        return "player_prop_other"
    key = stat_raw.upper().strip()
    for token, market in STAT_MARKET_MAP.items():
        if token in key:
            return market
    return "player_prop_other"


def normalize_betql_prop_pick(raw: Dict[str, object]) -> Dict[str, object]:
    """
    Normalize BetQL prop-picks record into a cross-source friendly schema.
    """
    # Extract nested event/market objects if present (merged BetQL format)
    event = raw.get("event", {}) or {}
    market_obj = raw.get("market", {}) or {}

    player_name_raw = raw.get("player_name")
    player_name_norm = normalize_text(player_name_raw or "") if player_name_raw else None
    player_slug = slugify_player_name(player_name_raw) if player_name_raw else None
    player_key = normalize_player_key(f"NBA:{player_slug}" if player_slug else None)

    # Also check market object for player_key if not found
    if not player_key:
        player_key_from_market = market_obj.get("player_key")
        if player_key_from_market:
            player_key = normalize_player_key(player_key_from_market)

    stat_raw = raw.get("stat_raw") or raw.get("stat") or market_obj.get("stat_key")
    market = _map_market(stat_raw)

    # Get direction from raw or market object
    direction_raw = raw.get("direction") or market_obj.get("side", "")
    # Convert 'player_over' / 'player_under' to 'OVER' / 'UNDER'
    if "over" in direction_raw.lower():
        direction = "OVER"
    elif "under" in direction_raw.lower():
        direction = "UNDER"
    else:
        direction = direction_raw.upper() if direction_raw else None

    # Get line from raw or market object
    line = raw.get("line") or market_obj.get("line")
    try:
        line = float(line) if line is not None else None
    except (TypeError, ValueError):
        line = None

    # Get odds from raw or market object
    odds = raw.get("odds") or market_obj.get("odds")
    try:
        odds = int(odds) if odds is not None else None
    except (TypeError, ValueError):
        odds = None

    # Extract team info - check nested event object first (merged format), then top-level
    away_team = raw.get("away_team") or event.get("away_team")
    home_team = raw.get("home_team") or event.get("home_team")
    matchup_hint = raw.get("matchup_hint") or event.get("matchup_key")
    day_key = raw.get("day_key") or event.get("day_key")
    event_key = raw.get("event_key") or event.get("event_key")

    # Build match_key using extracted fields
    match_key_parts = [
        matchup_hint or event_key or "",
        player_name_norm or (player_key or ""),
        market or "",
        str(line) if line is not None else "",
        direction or "",
    ]
    match_key = "|".join(match_key_parts)

    # Build selection in canonical format: player_key::stat::DIRECTION
    stat_key = market.replace("player_", "") if market else None
    selection = f"{player_key}::{stat_key}::{direction}" if player_key and stat_key and direction else None

    # Build matchup_key from teams (sorted order for cross-source matching)
    matchup_key = None
    if home_team and away_team and day_key:
        t1, t2 = sorted([str(home_team).upper(), str(away_team).upper()])
        matchup_key = f"{day_key}:{t1}-{t2}"

    # Rating eligibility
    rating_stars = raw.get("rating_stars")
    is_eligible = rating_stars is not None and rating_stars >= 3
    ineligibility_reason = None if is_eligible else "rating_below_threshold"

    # Build normalized record in nested schema format
    normalized = {
        "provenance": {
            "source_id": raw.get("source_id") or "betql",
            "source_surface": raw.get("source_surface") or "betql_prop_picks",
            "sport": raw.get("sport") or event.get("sport") or "NBA",
            "observed_at_utc": raw.get("observed_at_utc"),
            "canonical_url": raw.get("canonical_url"),
            "raw_fingerprint": raw.get("raw_fingerprint"),
            "rating_stars": rating_stars,
            "raw_pick_text": raw.get("raw_pick_text"),
        },
        "event": {
            "sport": raw.get("sport") or event.get("sport") or "NBA",
            "event_key": event_key,
            "day_key": day_key,
            "matchup_key": matchup_key,
            "away_team": away_team,
            "home_team": home_team,
            "event_start_time_utc": raw.get("event_start_time_utc") or event.get("event_start_time_utc"),
            "player_team": raw.get("player_team") or raw.get("team_panel_abbrev") or event.get("player_team"),
        },
        "market": {
            "market_type": "player_prop",
            "market_family": "prop",
            "selection": selection,
            "side": direction,
            "line": line,
            "odds": odds,
            "player_key": player_key,
            "player_name": player_name_norm,
            "stat_key": stat_key,
        },
        "eligibility": {
            "is_eligible": is_eligible,
            "reason": ineligibility_reason,
        },
        "eligible_for_consensus": is_eligible,
        "ineligibility_reason": ineligibility_reason,
    }

    return normalized
