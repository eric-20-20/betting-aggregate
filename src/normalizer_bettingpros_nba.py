"""
Normalizer for BettingPros expert NBA picks.

Converts raw BettingPros pick records to the standard normalized schema
used by consensus_nba.py.
"""

from __future__ import annotations

import json
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

ALLOWED_MARKETS = {"spread", "total", "moneyline", "player_prop"}

# Markets that should be ingested but marked ineligible for consensus
INELIGIBLE_MARKETS = {"parlay", "futures", "unknown"}


def _parse_dt(val: Any) -> Optional[datetime]:
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
        # BettingPros uses PHO (historical NBA abbrev) instead of PHX
        "pho": "PHX",
        # BettingPros sometimes uses UTH instead of UTA for Utah Jazz
        "uth": "UTA",
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
    from zoneinfo import ZoneInfo

    dt = event_dt or observed_dt
    if not dt:
        return None, None, None

    dt_eastern = dt.astimezone(ZoneInfo("America/New_York"))
    day_key = f"{sport}:{dt_eastern.year:04d}:{dt_eastern.month:02d}:{dt_eastern.day:02d}"
    event_key = day_key
    matchup_key = None

    if away_team and home_team:
        event_key = f"{day_key}:{away_team}@{home_team}"
        t1, t2 = sorted([str(home_team).upper(), str(away_team).upper()])
        matchup_key = f"{day_key}:{t1}-{t2}"

    return event_key, day_key, matchup_key


def _snap_spread(line: Optional[float]) -> Optional[float]:
    """Snap spread to nearest 0.5 (NBA spreads are .0 or .5)."""
    if line is None:
        return None
    return round(line * 2) / 2


def _parse_matchup(matchup_hint: Optional[str], store=None, sport: str = "NBA") -> Tuple[Optional[str], Optional[str]]:
    """Parse matchup like 'PHX @ LAL' or 'Phoenix Suns vs Los Angeles Lakers'."""
    if not matchup_hint:
        return None, None
    # "Team @ Team" (away @ home)
    if "@" in matchup_hint:
        parts = matchup_hint.split("@", 1)
        if len(parts) == 2:
            away = _map_team(parts[0].strip(), store=store, sport=sport)
            home = _map_team(parts[1].strip(), store=store, sport=sport)
            if away and home:
                return away, home
    # "Team vs Team"
    parts = re.split(r"\s+vs\.?\s+", matchup_hint, flags=re.IGNORECASE)
    if len(parts) == 2:
        away = _map_team(parts[0].strip(), store=store, sport=sport)
        home = _map_team(parts[1].strip(), store=store, sport=sport)
        return away, home
    return None, None


def _parse_game_date(raw: Dict[str, Any]) -> Optional[datetime]:
    """Try to parse game date from game_date field or event_start_time_utc."""
    game_date_str = raw.get("game_date")
    if game_date_str:
        # "2026-03-10" → datetime
        try:
            from zoneinfo import ZoneInfo
            d = datetime.strptime(game_date_str, "%Y-%m-%d")
            # Use noon ET as proxy for game day
            return d.replace(hour=12, tzinfo=ZoneInfo("America/New_York"))
        except (ValueError, TypeError):
            pass
    return _parse_dt(raw.get("event_start_time_utc"))


def normalize_bettingpros_record(raw: Dict[str, Any], sport: str = "NBA") -> Dict[str, Any]:
    """Normalize a single BettingPros pick record."""
    store = get_data_store(sport)

    observed_dt = _parse_dt(raw.get("observed_at_utc"))
    event_dt = _parse_game_date(raw) or observed_dt

    # Map teams from matchup_hint
    away_team = _map_team(raw.get("away_team"), store=store, sport=sport)
    home_team = _map_team(raw.get("home_team"), store=store, sport=sport)

    if not (away_team and home_team) and raw.get("matchup_hint"):
        away_team, home_team = _parse_matchup(raw["matchup_hint"], store=store, sport=sport)

    event_key, day_key, matchup_key = _build_event_keys(
        observed_dt, event_dt, away_team, home_team, sport=sport
    )

    market_type = raw.get("market_type")
    selection = raw.get("selection")
    line = raw.get("line")
    odds = raw.get("odds")
    side = None

    eligible = True
    inelig_reason = None

    # Parlay legs: ingest but mark ineligible
    if raw.get("parlay_id"):
        eligible = False
        inelig_reason = "parlay_leg"
        market_type = market_type or "parlay"

    # Futures: ingest but mark ineligible
    elif market_type == "futures":
        eligible = False
        inelig_reason = "futures"

    elif not market_type or market_type not in ALLOWED_MARKETS:
        eligible = False
        inelig_reason = "unsupported_market"

    elif (not away_team or not home_team) and market_type != "player_prop":
        eligible = False
        inelig_reason = "unparsed_team"

    elif market_type == "spread":
        if not selection:
            eligible = False
            inelig_reason = "missing_direction"
        elif line is None:
            eligible = False
            inelig_reason = "missing_line"
        else:
            # Map team name in selection to 3-letter code
            mapped = _map_team(selection, store=store, sport=sport)
            selection = mapped or selection.upper()
            side = selection
            line = _snap_spread(line)

    elif market_type == "total":
        direction = raw.get("side") or selection
        if direction not in ("OVER", "UNDER"):
            eligible = False
            inelig_reason = "missing_direction"
        elif line is None:
            eligible = False
            inelig_reason = "missing_line"
        else:
            selection = direction
            side = direction

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
        prop_direction = raw.get("side") or raw.get("direction")

        if not player_name:
            eligible = False
            inelig_reason = "unparsed_player"
        elif not stat_key:
            eligible = False
            inelig_reason = "unparsed_stat"
        elif prop_direction not in ("OVER", "UNDER"):
            eligible = False
            inelig_reason = "unparsed_direction"
        elif line is None:
            eligible = False
            inelig_reason = "unparsed_line"
        else:
            player_slug = re.sub(r"[^a-z0-9]+", "_", player_name.lower()).strip("_")
            player_key = f"{sport}:{player_slug}"
            selection = f"{player_key}::{stat_key}::{prop_direction}"
            side = prop_direction

    # Build provenance
    # Only 5-star picks are eligible for consensus; 3-4 star are ingested but marked ineligible
    bet_rating = raw.get("bet_rating")
    if bet_rating == 5:
        source_id = "bettingpros_5star"
    else:
        source_id = raw.get("source_id") or "bettingpros"

    # Mark non-5-star model picks ineligible (bet_rating present but < 5)
    if bet_rating is not None and bet_rating < 5 and eligible:
        eligible = False
        inelig_reason = "below_5star_rating"
    provenance = {
        "source_id": source_id,
        "source_surface": raw.get("source_surface") or "bettingpros_expert",
        "sport": sport,
        "observed_at_utc": raw.get("observed_at_utc"),
        "canonical_url": raw.get("canonical_url"),
        "raw_fingerprint": raw.get("raw_fingerprint"),
        "raw_pick_text": raw.get("raw_pick_text"),
        "raw_block": raw.get("raw_block"),
        "matchup_hint": raw.get("matchup_hint"),
        "expert_name": raw.get("expert_name"),
        "expert_slug": raw.get("expert_slug"),
        "wager": raw.get("wager"),
        "to_win": raw.get("to_win"),
        "parlay_id": raw.get("parlay_id"),
        "bet_rating": bet_rating,
    }

    event = {
        "sport": sport,
        "event_key": event_key,
        "day_key": day_key,
        "matchup_key": matchup_key,
        "away_team": away_team,
        "home_team": home_team,
        "event_start_time_utc": raw.get("event_start_time_utc"),
    }

    pp_player_key = None
    pp_stat_key = None
    pp_market_family = "standard"
    if market_type == "player_prop" and eligible:
        player_name_raw = raw.get("player_name") or ""
        player_slug = re.sub(r"[^a-z0-9]+", "_", player_name_raw.lower()).strip("_")
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
    }

    # Pre-graded result (backfill may have W/L/P)
    pregraded_result = raw.get("pregraded_result")
    result_block = None
    if pregraded_result and pregraded_result in {"WIN", "LOSS", "PUSH"}:
        result_block = {
            "status": pregraded_result,
            "graded_by": source_id,
            "graded_at_utc": raw.get("observed_at_utc"),
        }

    rec = {
        "provenance": provenance,
        "event": event,
        "market": market,
        "eligible_for_consensus": eligible,
        "ineligibility_reason": inelig_reason,
        "eligibility": {"eligible_for_consensus": eligible, "ineligibility_reason": inelig_reason},
    }
    if result_block:
        rec["result"] = result_block

    return rec


def normalize_bettingpros_records(
    raw_records: List[Dict[str, Any]],
    debug: bool = False,
    sport: str = "NBA",
) -> List[Dict[str, Any]]:
    """Normalize a list of BettingPros raw records."""
    normalized: List[Dict[str, Any]] = []
    inelig_reasons: Dict[str, int] = {}

    for raw in raw_records:
        if not isinstance(raw, dict):
            continue
        rec = normalize_bettingpros_record(raw, sport=sport)
        if rec.get("eligible_for_consensus"):
            normalized.append(rec)
        else:
            reason = rec.get("ineligibility_reason") or "unknown"
            inelig_reasons[reason] = inelig_reasons.get(reason, 0) + 1

    # Deduplicate by (event_key, source_id, market_type, selection, line)
    deduped: Dict[Tuple, Dict[str, Any]] = {}
    removed = 0

    for rec in normalized:
        ev = rec.get("event") or {}
        mk = rec.get("market") or {}
        prov = rec.get("provenance") or {}
        key = (
            ev.get("event_key"),
            prov.get("source_id"),
            mk.get("market_type"),
            mk.get("selection"),
            mk.get("line"),
        )
        if key in deduped:
            removed += 1
            continue
        deduped[key] = rec

    normalized = list(deduped.values())

    if debug:
        total = len(raw_records)
        elig = len(normalized)
        inelig_total = sum(inelig_reasons.values())
        print(f"[DEBUG] BettingPros normalized: total={total} eligible={elig} ineligible={inelig_total} removed_dupes={removed}")
        if inelig_reasons:
            print(f"[DEBUG] BettingPros ineligibility: {inelig_reasons}")

    return normalized


def normalize_file(raw_path: str, out_path: str, debug: bool = False, sport: str = "NBA") -> List[Dict[str, Any]]:
    """Normalize a raw BettingPros JSON file and write output."""
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

    normalized = normalize_bettingpros_records(raw_records, debug=debug, sport=sport)
    write_json(out_path, normalized)
    return normalized


__all__ = ["normalize_bettingpros_record", "normalize_bettingpros_records", "normalize_file"]
