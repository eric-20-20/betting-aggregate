"""
Normalizer for SportsLine NBA picks.

Converts raw SportsLine pick records to the standard normalized schema
used by consensus_nba.py.
"""

from __future__ import annotations

import os
import re
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from store import data_store, write_json
from utils import normalize_text

ALLOWED_MARKETS = {"spread", "total", "moneyline"}


def _parse_dt(val: Any) -> Optional[datetime]:
    """Parse datetime from string or datetime object."""
    if isinstance(val, datetime):
        dt = val
    elif isinstance(val, str):
        try:
            dt = datetime.fromisoformat(val)
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _map_team(alias: Optional[str]) -> Optional[str]:
    """Map team name or abbreviation to standard team code."""
    if not alias:
        return None
    codes = data_store.lookup_team_code(alias)
    if len(codes) == 1:
        return next(iter(codes))
    fallback = {
        "bucks": "MIL",
        "76ers": "PHI",
        "sixers": "PHI",
        "knicks": "NYK",
        "nets": "BKN",
        "trail blazers": "POR",
        "blazers": "POR",
        "suns": "PHX",
        "spurs": "SAS",
        "warriors": "GSW",
        "pelicans": "NOP",
        "lakers": "LAL",
        "clippers": "LAC",
        "celtics": "BOS",
        "heat": "MIA",
        "bulls": "CHI",
        "cavaliers": "CLE",
        "cavs": "CLE",
        "pistons": "DET",
        "raptors": "TOR",
        "mavericks": "DAL",
        "mavs": "DAL",
        "wolves": "MIN",
        "timberwolves": "MIN",
        "thunder": "OKC",
        "magic": "ORL",
        "rockets": "HOU",
        "jazz": "UTA",
        "kings": "SAC",
        "hawks": "ATL",
        "hornets": "CHA",
        "grizzlies": "MEM",
        "nuggets": "DEN",
        "pacers": "IND",
        "wizards": "WAS",
    }
    norm = normalize_text(alias)
    if norm in fallback:
        return fallback[norm]
    if len(alias) <= 3:
        return alias.upper()
    return None


def _build_event_keys(
    observed_dt: Optional[datetime],
    event_dt: Optional[datetime],
    away_team: Optional[str],
    home_team: Optional[str]
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Build event_key, day_key, and matchup_key from datetime and teams."""
    dt = event_dt or observed_dt
    if not dt:
        return None, None, None

    day_key = f"NBA:{dt.year:04d}:{dt.month:02d}:{dt.day:02d}"
    event_key = day_key
    matchup_key = None

    if away_team and home_team:
        event_key = f"{day_key}:{away_team}@{home_team}"
        t1, t2 = sorted([str(home_team).upper(), str(away_team).upper()])
        matchup_key = f"{day_key}:{t1}-{t2}"

    return event_key, day_key, matchup_key


def normalize_sportsline_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize a single SportsLine pick record.

    Args:
        raw: Raw pick record from sportsline_ingest.py

    Returns:
        Normalized record with provenance, event, market, and eligibility fields
    """
    # Parse timestamps
    observed_dt = _parse_dt(raw.get("observed_at_utc"))
    event_dt = _parse_dt(raw.get("event_start_time_utc")) or observed_dt

    # Map teams
    away_team = _map_team(raw.get("away_team"))
    home_team = _map_team(raw.get("home_team"))

    # Try to extract teams from matchup_hint if not already set
    if not (away_team and home_team) and raw.get("matchup_hint"):
        hint = raw["matchup_hint"]
        parts = re.split(r"@|vs|v", hint, flags=re.IGNORECASE)
        parts = [p.strip() for p in parts if p.strip()]
        if len(parts) >= 2:
            away_team = away_team or _map_team(parts[0])
            home_team = home_team or _map_team(parts[1])

    # Build event keys
    event_key, day_key, matchup_key = _build_event_keys(
        observed_dt, event_dt, away_team, home_team
    )

    # Market parsing
    market_type = raw.get("market_type")
    selection = raw.get("selection")
    line = raw.get("line")
    odds = raw.get("odds")
    side = None

    # Determine eligibility
    eligible = True
    inelig_reason = None

    if not market_type or market_type not in ALLOWED_MARKETS:
        eligible = False
        inelig_reason = "unsupported_market"
    elif not away_team or not home_team:
        eligible = False
        inelig_reason = "unparsed_team"
    elif market_type == "spread":
        if not selection:
            eligible = False
            inelig_reason = "missing_selection"
        else:
            selection = _map_team(selection) or selection
            side = selection
    elif market_type == "total":
        if not selection:
            eligible = False
            inelig_reason = "missing_direction"
        else:
            selection = selection.upper() if isinstance(selection, str) else None
            side = selection
            if selection not in {"OVER", "UNDER"}:
                eligible = False
                inelig_reason = "invalid_direction"
    elif market_type == "moneyline":
        if not selection:
            eligible = False
            inelig_reason = "missing_selection"
        else:
            selection = _map_team(selection) or selection
            side = selection

    # Build provenance
    provenance = {
        "source_id": "sportsline",
        "source_surface": raw.get("source_surface") or "sportsline_nba_picks",
        "sport": "NBA",
        "observed_at_utc": raw.get("observed_at_utc"),
        "canonical_url": raw.get("canonical_url"),
        "raw_fingerprint": raw.get("raw_fingerprint"),
        "raw_pick_text": raw.get("raw_pick_text"),
        "raw_block": raw.get("raw_block"),
        "matchup_hint": raw.get("matchup_hint"),
        "expert_name": raw.get("expert_name"),
        "expert_profile": raw.get("expert_profile"),
        "expert_slug": raw.get("expert_slug"),
        "expert_handle": raw.get("expert_handle"),
    }

    # Build event
    event = {
        "sport": "NBA",
        "event_key": event_key,
        "day_key": day_key,
        "matchup_key": matchup_key,
        "away_team": away_team,
        "home_team": home_team,
        "event_start_time_utc": raw.get("event_start_time_utc"),
    }

    # Build market
    market = {
        "market_type": market_type,
        "market_family": "standard",
        "selection": selection,
        "side": side,
        "line": line,
        "odds": odds,
        "player_key": None,
        "stat_key": None,
    }

    # Build eligibility
    eligibility = {
        "eligible_for_consensus": eligible,
        "ineligibility_reason": inelig_reason,
    }

    return {
        "provenance": provenance,
        "event": event,
        "market": market,
        "eligible_for_consensus": eligible,
        "ineligibility_reason": inelig_reason,
        "eligibility": eligibility,
    }


def normalize_sportsline_records(
    raw_records: List[Dict[str, Any]],
    debug: bool = False
) -> List[Dict[str, Any]]:
    """
    Normalize a list of SportsLine raw records.

    Args:
        raw_records: List of raw pick records
        debug: Enable debug output

    Returns:
        List of normalized records
    """
    normalized: List[Dict[str, Any]] = []
    inelig_reasons: Dict[str, int] = {}

    for raw in raw_records:
        if not isinstance(raw, dict):
            continue

        rec = normalize_sportsline_record(raw)

        if rec.get("eligible_for_consensus"):
            normalized.append(rec)
        else:
            reason = rec.get("ineligibility_reason") or "unknown"
            inelig_reasons[reason] = inelig_reasons.get(reason, 0) + 1

    # Deduplicate by (event_key, market_type, selection, line)
    deduped: Dict[Tuple, Dict[str, Any]] = {}
    removed = 0

    for rec in normalized:
        ev = rec.get("event") or {}
        mk = rec.get("market") or {}
        key = (
            ev.get("event_key"),
            mk.get("market_type"),
            mk.get("selection"),
            mk.get("line"),
        )

        if key in deduped:
            removed += 1
            existing = deduped[key]
            # Keep better odds if present
            ex_odds = (existing.get("market") or {}).get("odds")
            new_odds = mk.get("odds")
            if ex_odds is None and new_odds is not None:
                existing["market"]["odds"] = new_odds
            continue

        deduped[key] = rec

    normalized = list(deduped.values())

    if debug:
        total = len(raw_records)
        elig = len(normalized)
        inelig_total = sum(inelig_reasons.values())
        print(f"[DEBUG] SportsLine normalized: total={total} eligible={elig} ineligible={inelig_total} removed_dupes={removed}")
        if inelig_reasons:
            print(f"[DEBUG] SportsLine ineligibility reasons: {inelig_reasons}")

    return normalized


def normalize_file(raw_path: str, out_path: str, debug: bool = False) -> List[Dict[str, Any]]:
    """
    Normalize a raw SportsLine JSON file and write output.

    Args:
        raw_path: Path to raw JSON file
        out_path: Path to write normalized JSON
        debug: Enable debug output

    Returns:
        List of normalized records
    """
    import json

    if not os.path.exists(raw_path):
        write_json(out_path, [])
        return []

    try:
        with open(raw_path, "r", encoding="utf-8") as f:
            raw_records = json.load(f)
    except json.JSONDecodeError:
        raw_records = []

    if not isinstance(raw_records, list):
        raw_records = []

    normalized = normalize_sportsline_records(raw_records, debug=debug)
    write_json(out_path, normalized)

    return normalized


__all__ = ["normalize_sportsline_record", "normalize_sportsline_records", "normalize_file"]
