"""Deterministic key generation for signals and offers.

This module provides stable, reproducible key generation for:
- selection_key: Identifies the semantic bet (day + market + player/team + stat + direction)
- offer_key: Identifies the specific offer (selection + line + odds if available)

These keys are the foundation for deterministic signal tracking and grading.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Optional


def _stable_hash(data: Dict[str, Any]) -> str:
    """Generate a stable SHA256 hash from a dictionary."""
    raw = json.dumps(data, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def build_selection_key(
    day_key: str,
    market_type: str,
    selection: Optional[str] = None,
    player_id: Optional[str] = None,
    atomic_stat: Optional[str] = None,
    direction: Optional[str] = None,
    team: Optional[str] = None,
    event_key: Optional[str] = None,
) -> str:
    """Build a selection_key that identifies the semantic bet identity.

    The selection_key is independent of line/odds variations - it identifies
    WHAT is being bet on, not at what price.

    Args:
        day_key: Game date in NBA:YYYY:MM:DD format
        market_type: One of player_prop, spread, total, moneyline
        selection: Full selection string (e.g., "NBA:p_watson::points::UNDER")
        player_id: Player identifier for props (e.g., "p_watson")
        atomic_stat: Stat type (e.g., "points", "rebounds")
        direction: OVER/UNDER for props/totals, or team code for spreads
        team: Team code for spread/moneyline (e.g., "BOS")
        event_key: Full event key (e.g., "NBA:2026:03:12:IND@PHX") — required
            for totals/spreads/moneylines to disambiguate multiple games per day.

    Returns:
        A stable SHA256 hash identifying this selection
    """
    # Normalize day_key format
    day_key_norm = day_key.strip() if day_key else ""

    # For player props, use structured components
    if market_type == "player_prop":
        # Try to extract from selection if components not provided
        if selection and "::" in selection and not all([player_id, atomic_stat, direction]):
            parts = selection.split("::")
            if len(parts) >= 3:
                # Handle NBA:player_id::stat::direction
                player_part = parts[0]
                if player_part.upper().startswith("NBA:"):
                    player_id = player_id or player_part.split(":", 1)[1]
                else:
                    player_id = player_id or player_part
                atomic_stat = atomic_stat or parts[1]
                direction = direction or parts[2]

        payload = {
            "day_key": day_key_norm,
            "market_type": "player_prop",
            "player_id": (player_id or "").lower().strip(),
            "atomic_stat": (atomic_stat or "").lower().strip(),
            "direction": (direction or "").upper().strip(),
        }
    elif market_type == "spread":
        # For spreads, the selection is which team is being backed
        team_code = team or selection or ""
        if "_spread" in team_code.lower():
            team_code = team_code.replace("_spread", "").replace("_SPREAD", "")
        payload = {
            "day_key": day_key_norm,
            "market_type": "spread",
            "team": team_code.upper().strip(),
            "event_key": (event_key or "").strip(),
        }
    elif market_type == "total":
        # event_key is critical here — without it every UNDER/OVER on the same day
        # gets the same hash, causing cross-game source merges.
        payload = {
            "day_key": day_key_norm,
            "market_type": "total",
            "direction": (direction or "").upper().strip(),
            "event_key": (event_key or "").strip(),
        }
    elif market_type == "moneyline":
        team_code = team or selection or ""
        if "_ml" in team_code.lower():
            team_code = team_code.replace("_ml", "").replace("_ML", "")
        payload = {
            "day_key": day_key_norm,
            "market_type": "moneyline",
            "team": team_code.upper().strip(),
            "event_key": (event_key or "").strip(),
        }
    else:
        # Fallback for unknown market types
        payload = {
            "day_key": day_key_norm,
            "market_type": market_type or "unknown",
            "selection": (selection or "").strip(),
        }

    return _stable_hash(payload)


def build_offer_key(
    selection_key: str,
    line: Optional[float] = None,
    odds: Optional[int] = None,
) -> str:
    """Build an offer_key that identifies a specific offer (selection + price).

    Args:
        selection_key: The selection_key from build_selection_key()
        line: The betting line (e.g., 18.5)
        odds: American odds (e.g., -110). Can be None.

    Returns:
        A stable SHA256 hash identifying this specific offer
    """
    payload = {
        "selection_key": selection_key,
        "line": line if line is not None else "null",
        "odds": odds if odds is not None else "null",
    }
    return _stable_hash(payload)


def player_match_confidence(match_method: Optional[str]) -> float:
    """Convert player match method to a confidence score.

    Args:
        match_method: The method used to match the player (from provider)

    Returns:
        Confidence score 0.0-1.0
    """
    if not match_method:
        return 0.0

    confidence_map = {
        # Best matches - full name match
        "exact": 1.0,
        "full_name": 1.0,
        "personid": 1.0,
        # Very good - initial + last name
        "initial_last": 0.95,
        "first_last": 0.95,
        # Good - initials matching
        "initial_initials": 0.85,
        "initials": 0.85,
        # Moderate - partial matches
        "last_only": 0.75,
        "last_name": 0.75,
        "fuzzy": 0.70,
        # Low confidence
        "fallback": 0.5,
        "guess": 0.4,
    }

    return confidence_map.get(match_method.lower(), 0.6)


def is_roi_eligible(
    status: str,
    odds: Optional[int],
    player_match_confidence_val: Optional[float] = None,
    market_type: Optional[str] = None,
) -> bool:
    """Determine if a graded signal should contribute to ROI calculations.

    A signal is ROI-eligible if:
    - Has a terminal status (WIN/LOSS/PUSH)
    - Has valid odds (we never assume -110)
    - For player props: has high player match confidence (>= 0.75)

    Args:
        status: The grade status (WIN/LOSS/PUSH/ERROR/INELIGIBLE/PENDING)
        odds: American odds (can be None)
        player_match_confidence_val: Player match confidence 0-1 (for props)
        market_type: The market type

    Returns:
        True if this grade should be included in ROI calculations
    """
    # Must have terminal status
    terminal_statuses = {"WIN", "LOSS", "PUSH"}
    if status not in terminal_statuses:
        return False

    # Must have odds - we never silently assume -110
    if odds is None:
        return False

    # For player props, require high confidence on player matching
    if market_type == "player_prop":
        if player_match_confidence_val is None:
            return False
        if player_match_confidence_val < 0.75:
            return False

    return True
