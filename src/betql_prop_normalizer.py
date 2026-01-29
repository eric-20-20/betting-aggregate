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
    player_name_raw = raw.get("player_name")
    player_name_norm = normalize_text(player_name_raw or "") if player_name_raw else None
    player_slug = slugify_player_name(player_name_raw) if player_name_raw else None
    player_key = normalize_player_key(f"NBA:{player_slug}" if player_slug else None)
    stat_raw = raw.get("stat_raw") or raw.get("stat")
    market = _map_market(stat_raw)

    direction = (raw.get("direction") or "").upper() or None
    line = raw.get("line")
    try:
        line = float(line) if line is not None else None
    except (TypeError, ValueError):
        line = None
    odds = raw.get("odds")
    try:
        odds = int(odds) if odds is not None else None
    except (TypeError, ValueError):
        odds = None

    match_key_parts = [
        raw.get("matchup_hint") or "",
        player_name_norm or "",
        market or "",
        str(line) if line is not None else "",
        direction or "",
    ]
    match_key = "|".join(match_key_parts)

    normalized = {
        "sport": raw.get("sport") or "NBA",
        "source_id": raw.get("source_id") or "betql",
        "source_surface": raw.get("source_surface") or "betql_prop_picks",
        "observed_at_utc": raw.get("observed_at_utc"),
        "away_team": raw.get("away_team"),
        "home_team": raw.get("home_team"),
        "matchup_hint": raw.get("matchup_hint"),
        "player_name_raw": player_name_raw,
        "player_name_norm": player_name_norm,
        "player_team": raw.get("player_team"),
        "player_team_side": raw.get("player_team_side"),
        "player_slug": player_slug,
        "player_key": player_key,
        "direction": direction,
        "line": line,
        "odds": odds,
        "rating_stars": raw.get("rating_stars"),
        "stat_raw": stat_raw,
        "market": market,
        "selection": f"{player_key}::{market.replace('player_', '')}::{direction}" if player_key and direction else None,
        "match_key": match_key,
        "raw_pick_text": raw.get("raw_pick_text"),
        "raw_fingerprint": raw.get("raw_fingerprint"),
        "canonical_url": raw.get("canonical_url"),
    }

    return normalized
