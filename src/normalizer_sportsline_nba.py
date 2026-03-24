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

_PROFIT_RE = re.compile(r'^[+\-]?\d[\d,]*(\.\d+)?$')

# Canonical name for experts whose display name varies across page surfaces.
# Format: lowercased variant → canonical display name.
# "Prop Bet Guy" is canonical (72/76 historical records use it); map spaceless variant to match.
_EXPERT_NAME_ALIASES: Dict[str, str] = {
    "prop betguy": "Prop Bet Guy",
}


def _clean_expert_name(name: Optional[str], canonical_url: Optional[str]) -> Optional[str]:
    """Normalize expert_name: fix profit figures and merge known aliases.

    SportsLine expert pages sometimes render the running profit figure before the expert
    name in the DOM, causing the scraper to capture '+654' instead of 'Micah Roberts'.
    The canonical_url always contains the real slug: /experts/7950/micah-roberts/

    Also canonicalizes known variant spellings (e.g. 'Prop Betguy' -> 'Prop Bet Guy').
    """
    if not name:
        return name
    # Strip trailing record stats that sometimes appear after a newline
    # e.g. "Bruce Marshall\n102-88 Last 14 Days" → "Bruce Marshall"
    name = name.split("\n")[0].strip()
    if not name:
        return None
    # Resolve profit figure via canonical_url
    if _PROFIT_RE.match(name.strip()):
        if not canonical_url:
            return name
        m = re.search(r'/experts/\d+/([^/?]+)', canonical_url)
        if not m:
            return name
        slug = m.group(1)  # e.g. "micah-roberts"
        name = " ".join(w.capitalize() for w in slug.split("-"))
    # Apply alias normalization
    alias = _EXPERT_NAME_ALIASES.get(name.lower())
    if alias:
        return alias
    return name


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

    # For NCAAB expert-page picks, try to extract nicknames from raw_block
    # Format: "{Date}\n{AwayNickname}\n{Score?}\n@ {HomeNickname}\n{Score?}|..."
    if sport == "NCAAB" and not (away_team and home_team):
        raw_block = raw.get("raw_block") or ""
        matchup_section = raw_block.split("|")[0] if "|" in raw_block else raw_block
        lines = [l.strip() for l in matchup_section.split("\n") if l.strip()]
        # Lines after date: away_nickname, (score?), @ home_nickname, (score?)
        # Format: [Date, AwayNick, AwayScore?, @ HomeNick, HomeScore?, ...]
        # Scores appear as digit-only lines — skip them when searching backwards.
        nick_away = None
        nick_home = None
        for i, line in enumerate(lines[1:], 1):  # skip date line
            if line.startswith("@"):
                nick_home = line.lstrip("@ ").strip()
                # Walk backwards from the @ line to find the away nickname, skipping scores
                j = i - 1
                while j >= 1 and re.match(r"^\d+$", lines[j]):
                    j -= 1
                if j >= 1:
                    nick_away = lines[j]
                break
        if nick_away and not away_team:
            away_team = _map_team(nick_away, store=store, sport=sport)
        if nick_home and not home_team:
            home_team = _map_team(nick_home, store=store, sport=sport)
        # Fallback: use the selection team name (from raw_pick_text, already resolved) to fill
        # the correct slot.  For spread/moneyline/1st_half_spread the selection IS one of the
        # two teams.  Try two steps:
        #   1. If the selection code is in the ambiguous nick's candidate set, slot it there.
        #   2. If one slot is still unknown, fill it with sel_code if it doesn't conflict with
        #      the already-known slot — enough for derive_spread_fields to pass.
        if (not away_team or not home_team) and raw.get("selection"):
            sel_code = _map_team(raw["selection"], store=store, sport=sport)
            if sel_code:
                from utils import normalize_text as _nt
                if not away_team and nick_away:
                    candidates = store.lookup_team_code(_nt(nick_away))
                    if sel_code in candidates:
                        away_team = sel_code
                if not home_team and nick_home:
                    candidates = store.lookup_team_code(_nt(nick_home))
                    if sel_code in candidates:
                        home_team = sel_code
                # If still one slot unknown: slot sel_code there (opponent stays unknown/None).
                # The event_key won't have a full matchup but spread eligibility will pass
                # since selection == one of the two team slots.
                if not away_team and away_team != sel_code and home_team != sel_code:
                    away_team = sel_code
                elif not home_team and home_team != sel_code and away_team != sel_code:
                    home_team = sel_code

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
        if sport == "NBA":
            eligible = False
            inelig_reason = "unparsed_team"
        # NCAAB: allow partial team resolution — eligibility checked per-market below
    if eligible and market_type in ("spread", "1st_half_spread"):
        if not selection:
            # Try to parse team from raw_pick_text
            # Format: "1st Half Spread\n1st Half {TeamName} {line} ..."
            pick_text = raw.get("raw_pick_text") or ""
            hs_match = re.match(r"1st Half Spread\n1st Half ([A-Za-z][A-Za-z\s\.\'-]+?)\s+[+-]?\d", pick_text)
            if hs_match:
                team_name = hs_match.group(1).strip()
                team_code = _map_team(team_name, store=store, sport=sport)
                if team_code:
                    selection = team_code
                else:
                    eligible = False
                    inelig_reason = "unparsed_team"
            else:
                eligible = False
                inelig_reason = "missing_selection"
        if eligible and selection:
            selection = _map_team(selection, store=store, sport=sport) or selection
            side = selection
            # NCAAB: if we couldn't resolve either team AND the selection itself
            # doesn't resolve, we can't identify the game — mark ineligible.
            # But if we parsed the team from raw_pick_text (team_code already set),
            # we trust that resolution and skip this guard.
            if sport == "NCAAB" and not away_team and not home_team:
                resolved = _map_team(selection, store=store, sport=sport)
                if not resolved:
                    eligible = False
                    inelig_reason = "unparsed_team"
    elif eligible and market_type == "total":
        if not selection:
            eligible = False
            inelig_reason = "missing_direction"
        else:
            selection = selection.upper() if isinstance(selection, str) else None
            side = selection
            if selection not in {"OVER", "UNDER"}:
                eligible = False
                inelig_reason = "invalid_direction"
    elif eligible and market_type == "team_total":
        side = raw.get("side") or (selection.split("_")[-1] if selection else None)
        if not selection:
            # Try to parse team name from raw_pick_text
            # Format: "Home/Away Team Total\n{TeamName} Over/Under {line}..."
            pick_text = raw.get("raw_pick_text") or ""
            # Extract team name from second line of raw_pick_text
            tt_match = re.match(r"(?:Home|Away) Team Total\n([A-Za-z][A-Za-z\s\.\'\-\(\)]+?)\s+(?:Over|Under)\s", pick_text)
            if tt_match:
                team_name = tt_match.group(1).strip()
                team_code = _map_team(team_name, store=store, sport=sport)
                if team_code:
                    # Infer direction from text
                    dir_match = re.search(r"\b(Over|Under)\b", pick_text, re.IGNORECASE)
                    direction = dir_match.group(1).upper() if dir_match else (side or "")
                    selection = f"{team_code}_{direction}"
                    side = direction
                else:
                    eligible = False
                    inelig_reason = "unparsed_team"
            else:
                eligible = False
                inelig_reason = "missing_selection"
        else:
            side = raw.get("side")
            # selection comes pre-built as "TEAM_OVER" or "TEAM_UNDER"
    elif eligible and market_type == "moneyline":
        if not selection:
            eligible = False
            inelig_reason = "missing_selection"
        else:
            selection = _map_team(selection, store=store, sport=sport) or selection
            side = selection
            # NCAAB: moneyline selection must resolve to a known team code
            if sport == "NCAAB" and not away_team and not home_team:
                if not _map_team(raw.get("side") or "", store=store, sport=sport):
                    eligible = False
                    inelig_reason = "unparsed_team"
    elif eligible and market_type == "player_prop":
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
        "expert_name": _clean_expert_name(raw.get("expert_name"), raw.get("canonical_url")),
        "expert_profile": raw.get("expert_profile"),
        "expert_slug": raw.get("expert_slug"),
        "expert_handle": raw.get("expert_handle"),
        "pregraded_result": raw.get("pregraded_result"),
        "pregraded_by": raw.get("pregraded_by") or ("sportsline" if raw.get("pregraded_result") else None),
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

    # Build result block if pre-graded (matches JuiceReel format for ledger builder)
    pregraded = raw.get("pregraded_result")
    result_block = None
    if pregraded in {"WIN", "LOSS", "PUSH"}:
        result_block = {
            "status": pregraded,
            "graded_by": raw.get("pregraded_by") or "sportsline",
            "graded_at_utc": raw.get("observed_at_utc"),
        }

    out = {
        "provenance": provenance,
        "event": event,
        "market": market,
        "eligible_for_consensus": eligible,
        "ineligibility_reason": inelig_reason,
        "eligibility": eligibility,
    }
    if result_block:
        out["result"] = result_block
    return out


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

    # Deduplicate by (expert_name, event_key, market_type, selection, line)
    # Include expert_name so different experts' picks on the same game are preserved.
    deduped: Dict[Tuple, Dict[str, Any]] = {}
    removed = 0

    for rec in normalized:
        ev = rec.get("event") or {}
        mk = rec.get("market") or {}
        prov = rec.get("provenance") or {}
        key = (
            prov.get("expert_name") or prov.get("expert_slug") or "",
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
