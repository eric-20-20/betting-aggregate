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

from store import data_store, get_data_store, write_json
from utils import normalize_text

ALLOWED_MARKETS = {"spread", "total", "moneyline", "player_prop", "1st_half_spread", "team_total"}


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


def _map_team(alias: Optional[str], store=None, sport: str = "NBA") -> Optional[str]:
    """Map team name or abbreviation to standard team code."""
    if not alias:
        return None
    store = store or data_store
    norm = normalize_text(alias)
    codes = store.lookup_team_code(norm)
    if len(codes) == 1:
        return next(iter(codes))
    if norm != alias:
        codes = store.lookup_team_code(alias)
        if len(codes) == 1:
            return next(iter(codes))
    # NBA-specific city/nickname fallbacks — only for NBA sport
    if sport != "NBA":
        if len(alias) <= 4:
            upper = alias.upper()
            if upper in store.teams:
                return upper
        return None
    nba_fallback = {
        "bucks": "MIL", "76ers": "PHI", "sixers": "PHI", "knicks": "NYK",
        "nets": "BKN", "trail blazers": "POR", "blazers": "POR", "suns": "PHX",
        "spurs": "SAS", "warriors": "GSW", "pelicans": "NOP", "lakers": "LAL",
        "clippers": "LAC", "celtics": "BOS", "heat": "MIA", "bulls": "CHI",
        "cavaliers": "CLE", "cavs": "CLE", "pistons": "DET", "raptors": "TOR",
        "mavericks": "DAL", "mavs": "DAL", "wolves": "MIN",
        "timberwolves": "MIN", "thunder": "OKC", "magic": "ORL",
        "rockets": "HOU", "jazz": "UTA", "kings": "SAC", "hawks": "ATL",
        "hornets": "CHA", "grizzlies": "MEM", "nuggets": "DEN",
        "pacers": "IND", "wizards": "WAS",
        "milwaukee": "MIL", "philadelphia": "PHI", "new york": "NYK",
        "brooklyn": "BKN", "portland": "POR", "phoenix": "PHX",
        "san antonio": "SAS", "golden state": "GSW", "golden st.": "GSW",
        "golden st": "GSW", "new orleans": "NOP",
        "l.a. lakers": "LAL", "l a lakers": "LAL", "los angeles lakers": "LAL",
        "l.a. clippers": "LAC", "l a clippers": "LAC",
        "los angeles clippers": "LAC", "boston": "BOS", "miami": "MIA",
        "chicago": "CHI", "cleveland": "CLE", "detroit": "DET",
        "toronto": "TOR", "dallas": "DAL", "minnesota": "MIN",
        "oklahoma city": "OKC", "orlando": "ORL", "houston": "HOU",
        "utah": "UTA", "sacramento": "SAC", "atlanta": "ATL",
        "charlotte": "CHA", "memphis": "MEM", "denver": "DEN",
        "indiana": "IND", "washington": "WAS",
    }
    if norm in nba_fallback:
        return nba_fallback[norm]
    if len(alias) <= 4:
        upper = alias.upper()
        if upper in store.teams:
            return upper
    return None


def _build_event_keys(
    observed_dt: Optional[datetime],
    event_dt: Optional[datetime],
    away_team: Optional[str],
    home_team: Optional[str],
    sport: str = "NBA",
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Build event_key, day_key, and matchup_key from datetime and teams."""
    from zoneinfo import ZoneInfo

    dt = event_dt or observed_dt
    if not dt:
        return None, None, None

    # Use Eastern time for day_key (NBA convention)
    dt_eastern = dt.astimezone(ZoneInfo("America/New_York"))
    day_key = f"{sport}:{dt_eastern.year:04d}:{dt_eastern.month:02d}:{dt_eastern.day:02d}"
    event_key = day_key
    matchup_key = None

    if away_team and home_team:
        event_key = f"{day_key}:{away_team}@{home_team}"
        t1, t2 = sorted([str(home_team).upper(), str(away_team).upper()])
        matchup_key = f"{day_key}:{t1}-{t2}"

    return event_key, day_key, matchup_key


def normalize_sportsline_record(raw: Dict[str, Any], sport: str = "NBA") -> Dict[str, Any]:
    """
    Normalize a single SportsLine pick record.

    Args:
        raw: Raw pick record from sportsline_ingest.py
        sport: Sport identifier ("NBA" or "NCAAB")

    Returns:
        Normalized record with provenance, event, market, and eligibility fields
    """
    store = get_data_store(sport)

    # Parse timestamps
    observed_dt = _parse_dt(raw.get("observed_at_utc"))
    event_dt = _parse_dt(raw.get("event_start_time_utc")) or observed_dt

    # Map teams
    away_team = _map_team(raw.get("away_team"), store=store, sport=sport)
    home_team = _map_team(raw.get("home_team"), store=store, sport=sport)

    # Try to extract teams from matchup_hint if not already set
    if not (away_team and home_team) and raw.get("matchup_hint"):
        hint = raw["matchup_hint"]
        parts = re.split(r"@|vs|v", hint, flags=re.IGNORECASE)
        parts = [p.strip() for p in parts if p.strip()]
        if len(parts) >= 2:
            away_team = away_team or _map_team(parts[0], store=store, sport=sport)
            home_team = home_team or _map_team(parts[1], store=store, sport=sport)

    # Build event keys
    event_key, day_key, matchup_key = _build_event_keys(
        observed_dt, event_dt, away_team, home_team, sport=sport
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
    elif market_type in ("spread", "1st_half_spread"):
        if not selection:
            eligible = False
            inelig_reason = "missing_selection"
        else:
            selection = _map_team(selection, store=store, sport=sport) or selection
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
    elif market_type == "team_total":
        if not selection:
            eligible = False
            inelig_reason = "missing_selection"
        else:
            side = raw.get("side")
            # selection comes pre-built as "TEAM_OVER" or "TEAM_UNDER"
    elif market_type == "moneyline":
        if not selection:
            eligible = False
            inelig_reason = "missing_selection"
        else:
            selection = _map_team(selection, store=store, sport=sport) or selection
            side = selection
    elif market_type == "player_prop":
        player_name = raw.get("player_name")
        stat_key = raw.get("stat_key")
        side = raw.get("side")
        if not player_name:
            eligible = False
            inelig_reason = "unparsed_player"
        elif not stat_key:
            eligible = False
            inelig_reason = "unparsed_stat"
        elif side not in ("OVER", "UNDER"):
            eligible = False
            inelig_reason = "unparsed_direction"
        elif line is None:
            eligible = False
            inelig_reason = "unparsed_line"
        else:
            player_slug = re.sub(r"[^a-z0-9]+", "_", player_name.lower()).strip("_")
            player_key = f"{sport}:{player_slug}"
            selection = f"{player_key}::{stat_key}::{side}"

    # Build provenance
    provenance = {
        "source_id": "sportsline",
        "source_surface": raw.get("source_surface") or f"sportsline_{sport.lower()}_picks",
        "sport": sport,
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
        "sport": sport,
        "event_key": event_key,
        "day_key": day_key,
        "matchup_key": matchup_key,
        "away_team": away_team,
        "home_team": home_team,
        "event_start_time_utc": raw.get("event_start_time_utc"),
    }

    # Build market
    pp_player_key = None
    pp_stat_key = None
    pp_market_family = "standard"
    if market_type == "player_prop" and eligible:
        player_slug = re.sub(r"[^a-z0-9]+", "_", raw.get("player_name", "").lower()).strip("_")
        pp_player_key = f"{sport}:{player_slug}"
        pp_stat_key = raw.get("stat_key")
        pp_market_family = "player_prop"

    market = {
        "market_type": market_type,
        "market_family": pp_market_family,
        "selection": selection,
        "side": side,
        "line": line,
        "odds": odds,
        "player_key": pp_player_key,
        "stat_key": pp_stat_key,
        "unit_size": raw.get("unit_size"),
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
    debug: bool = False,
    sport: str = "NBA",
) -> List[Dict[str, Any]]:
    """
    Normalize a list of SportsLine raw records.

    Args:
        raw_records: List of raw pick records
        debug: Enable debug output
        sport: Sport identifier ("NBA" or "NCAAB")

    Returns:
        List of normalized records
    """
    normalized: List[Dict[str, Any]] = []
    inelig_reasons: Dict[str, int] = {}

    for raw in raw_records:
        if not isinstance(raw, dict):
            continue

        rec = normalize_sportsline_record(raw, sport=sport)

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
