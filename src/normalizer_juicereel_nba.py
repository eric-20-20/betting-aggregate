"""
Normalizer for JuiceReel expert NBA picks (sXeBets + nukethebooks).

Converts raw JuiceReel pick records to the standard normalized schema
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

ALLOWED_MARKETS = {"spread", "total", "moneyline", "player_prop"}

# Stat keyword → canonical stat_key (must match ALLOWED_ATOMIC in consensus_nba.py)
STAT_KEY_MAP: Dict[str, str] = {
    "points": "points",
    "point": "points",
    "pts": "points",
    "rebounds": "rebounds",
    "rebound": "rebounds",
    "reb": "rebounds",
    "assists": "assists",
    "assist": "assists",
    "ast": "assists",
    "blocks": "blocks",
    "block": "blocks",
    "blk": "blocks",
    "steals": "steals",
    "steal": "steals",
    "stl": "steals",
    "threes": "threes",
    "three pointers": "threes",
    "three-pointers": "threes",
    "3-pointers": "threes",
    "3 pointers": "threes",
    "3pt": "threes",
    "3s": "threes",
    "pts+reb+ast": "pts_reb_ast",
    "pts + reb + ast": "pts_reb_ast",
    "points + rebounds + assists": "pts_reb_ast",
    "points+rebounds+assists": "pts_reb_ast",
    "pra": "pts_reb_ast",
    "pts+reb": "pts_reb",
    "pts + reb": "pts_reb",
    "points + rebounds": "pts_reb",
    "points+rebounds": "pts_reb",
    "pr": "pts_reb",
    "reb+ast": "reb_ast",
    "reb + ast": "reb_ast",
    "rebounds + assists": "reb_ast",
    "rebounds+assists": "reb_ast",
    "ra": "reb_ast",
    "pts+ast": "pts_ast",
    "pts + ast": "pts_ast",
    "points + assists": "pts_ast",
    "points+assists": "pts_ast",
    "pa": "pts_ast",
    "double doubles": "double_doubles",
    "double double": "double_doubles",
    "dd": "double_doubles",
    "triple doubles": "triple_doubles",
    "triple double": "triple_doubles",
    "td": "triple_doubles",
    "turnovers": "turnovers",
    "turnover": "turnovers",
    "tov": "turnovers",
    "free throws made": "free_throws_made",
    "free throws": "free_throws_made",
    "ftm": "free_throws_made",
    "minutes": "minutes",
    "min": "minutes",
}


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
    """Snap spread to nearest 0.5 (NBA spreads are always .0 or .5)."""
    if line is None:
        return None
    rounded = round(line * 2) / 2
    return rounded


def _parse_prop_text(pick_text: str) -> Tuple[Optional[str], Optional[str], Optional[float], Optional[str]]:
    """
    Parse a player prop pick text like:
      "Darius Garland Over 12.5 Points"
      "Payton Pritchard Over 5.5 Assists"
      "Baylor Scheierman Over 0.5 Double Doubles"

    Returns: (player_name, stat_key, line, direction)
    """
    if not pick_text:
        return None, None, None, None

    text = pick_text.strip()

    # Match: "<Name> Over/Under <line> <stat>"
    # or:    "<Name> Over/Under <stat>" (no line shown)
    m = re.match(
        r"^(.+?)\s+(over|under)\s+([\d.]+)\s+(.+)$",
        text,
        re.IGNORECASE,
    )
    if m:
        player_name = m.group(1).strip()
        direction = m.group(2).upper()
        try:
            line = float(m.group(3))
        except ValueError:
            line = None
        stat_raw = m.group(4).strip().lower()
        stat_key = STAT_KEY_MAP.get(stat_raw)
        # Try partial match if exact fails
        if not stat_key:
            for k, v in STAT_KEY_MAP.items():
                if stat_raw.startswith(k) or k.startswith(stat_raw):
                    stat_key = v
                    break
        return player_name, stat_key, line, direction

    return None, None, None, None


def _parse_matchup(matchup_hint: Optional[str], store=None, sport: str = "NBA") -> Tuple[Optional[str], Optional[str]]:
    """
    Parse matchup like "Golden State Warriors vs Los Angeles Clippers"
    or "Clippers @ Pacers" (away @ home).
    Returns (away_team, home_team).
    """
    if not matchup_hint:
        return None, None
    # Try "Team @ Team" (away @ home)
    if "@" in matchup_hint:
        parts = matchup_hint.split("@", 1)
        if len(parts) == 2:
            away = _map_team(parts[0].strip(), store=store, sport=sport)
            home = _map_team(parts[1].strip(), store=store, sport=sport)
            if away and home:
                return away, home
    # Try "Team vs Team"
    parts = re.split(r"\s+vs\.?\s+", matchup_hint, flags=re.IGNORECASE)
    if len(parts) == 2:
        away = _map_team(parts[0].strip(), store=store, sport=sport)
        home = _map_team(parts[1].strip(), store=store, sport=sport)
        return away, home
    return None, None


def _validate_matchup_order(
    away_team: Optional[str],
    home_team: Optional[str],
    day_key: str,
    sport: str = "NBA",
) -> Tuple[Optional[str], Optional[str]]:
    """
    Validate that (away_team, home_team) matches the canonical schedule order.
    Uses NBA API scoreboard for NBA; ESPN provider for NCAAB.
    Returns (away_team, home_team) — possibly swapped — after comparing to schedule.
    Logs a warning on swap or on lookup failure.
    """
    if not away_team or not home_team:
        return away_team, home_team
    try:
        if sport == "NBA":
            from src.results.nba_provider_nba_api import get_games_for_date
            games, _ = get_games_for_date(day_key)
        else:
            # NCAAB: no canonical schedule check available; trust parsed order
            return away_team, home_team
        team_set = {away_team.upper(), home_team.upper()}
        for g in games:
            sched_away = (g.get("away_team_abbrev") or "").upper()
            sched_home = (g.get("home_team_abbrev") or "").upper()
            if {sched_away, sched_home} == team_set:
                if sched_away == away_team.upper() and sched_home == home_team.upper():
                    # Order matches — nothing to do
                    return away_team, home_team
                else:
                    # Order is reversed — swap
                    print(
                        f"⚠️  JuiceReel matchup order corrected: "
                        f"{away_team}@{home_team} → {sched_away}@{sched_home}"
                    )
                    return sched_away, sched_home
        # Game not found in schedule
        print(
            f"⚠️  JuiceReel matchup not found in schedule for {day_key}: "
            f"{away_team}@{home_team} — keeping parsed order"
        )
    except Exception as exc:
        print(
            f"⚠️  JuiceReel schedule lookup failed for {day_key} "
            f"{away_team}@{home_team}: {exc} — keeping parsed order"
        )
    return away_team, home_team


def normalize_juicereel_record(raw: Dict[str, Any], sport: str = "NBA") -> Dict[str, Any]:
    """
    Normalize a single JuiceReel pick record.
    """
    store = get_data_store(sport)

    observed_dt = _parse_dt(raw.get("observed_at_utc"))
    event_dt = _parse_dt(raw.get("event_start_time_utc")) or observed_dt

    # Map teams — try direct fields first, fall back to matchup_hint
    away_team = _map_team(raw.get("away_team"), store=store, sport=sport)
    home_team = _map_team(raw.get("home_team"), store=store, sport=sport)

    if not (away_team and home_team) and raw.get("matchup_hint"):
        away_team, home_team = _parse_matchup(raw["matchup_hint"], store=store, sport=sport)

    # Validate canonical team order against schedule
    if away_team and home_team:
        _day_key_for_validation = (
            event_dt or observed_dt
        ).strftime("%Y-%m-%d") if (event_dt or observed_dt) else None
        if _day_key_for_validation:
            away_team, home_team = _validate_matchup_order(
                away_team, home_team, _day_key_for_validation, sport=sport
            )

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

    if not market_type or market_type not in ALLOWED_MARKETS:
        eligible = False
        inelig_reason = "unsupported_market"
    elif (not away_team or not home_team) and market_type != "player_prop":
        # Player props can proceed without team matchup — consensus is on player/stat/line
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
            # Spread selection may be "CLE CAVALIERS" — try full string first, then first token
            sel_norm = selection.upper() if isinstance(selection, str) else None
            mapped = _map_team(sel_norm, store=store, sport=sport) if sel_norm else None
            if not mapped and sel_norm:
                first_token = sel_norm.split()[0]
                mapped = _map_team(first_token, store=store, sport=sport)
            selection = mapped or sel_norm
            side = selection
            if not selection:
                eligible = False
                inelig_reason = "invalid_direction"
    elif market_type == "total":
        if not selection or selection.upper() not in {"OVER", "UNDER"}:
            eligible = False
            inelig_reason = "missing_direction"
        elif line is not None and line < (100 if sport == "NCAAB" else 150):
            # NBA full-game totals are always 200+; NCAAB totals run 120-165
            # Lines under threshold are 1st-half or quarter totals
            eligible = False
            inelig_reason = "half_game_total"
        else:
            selection = selection.upper()
            side = selection
    elif market_type == "moneyline":
        if not selection:
            eligible = False
            inelig_reason = "missing_selection"
        else:
            # Detect unsigned spread disguised as moneyline: "St Louis 2.5", "TCU 3", "Siena 28"
            # Pattern: selection ends with a numeric value (the spread line)
            _spread_suffix = re.match(r"^(.+?)\s+([+-]?[\d.]+)\s*$", selection)
            _reclassified = False
            if _spread_suffix:
                _team_part = _spread_suffix.group(1).strip()
                _line_val = float(_spread_suffix.group(2))
                _mapped = _map_team(_team_part, store=store, sport=sport)
                if not _mapped:
                    _mapped = _map_team(_team_part.upper().split()[0], store=store, sport=sport)
                if _mapped:
                    # Re-classify as spread with the numeric suffix as line
                    market_type = "spread"
                    selection = _mapped
                    line = _snap_spread(_line_val)
                    side = selection
                    _reclassified = True
            if not _reclassified:
                # Standard moneyline: "MIN TIMBERWOLVES" — try full string, then first token
                mapped = _map_team(selection, store=store, sport=sport)
                if not mapped:
                    first_token = selection.upper().split()[0]
                    mapped = _map_team(first_token, store=store, sport=sport)
                selection = mapped or selection
                side = selection
    elif market_type == "player_prop":
        player_name = raw.get("player_name")
        stat_key = raw.get("stat_key")
        prop_direction = raw.get("side") or raw.get("direction")

        # If fields aren't pre-parsed, try to parse from pick_text
        if not (player_name and stat_key and prop_direction and line is not None):
            pick_text = raw.get("raw_pick_text") or raw.get("pick_text") or ""
            parsed_name, parsed_stat, parsed_line, parsed_dir = _parse_prop_text(pick_text)
            player_name = player_name or parsed_name
            stat_key = stat_key or parsed_stat
            line = line if line is not None else parsed_line
            prop_direction = prop_direction or parsed_dir

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
    source_id = raw.get("source_id") or "juicereel"
    # Infer expert_name from canonical_url if not explicitly set
    # e.g. https://app.juicereel.com/users/sXeBets -> "sXeBets"
    _expert_name = raw.get("expert_name")
    if not _expert_name:
        _canon = raw.get("canonical_url") or ""
        if "/users/" in _canon:
            _expert_name = _canon.split("/users/")[-1].split("/")[0].split("?")[0]
    provenance = {
        "source_id": source_id,
        "source_surface": raw.get("source_surface") or f"juicereel_{sport.lower()}_picks",
        "sport": sport,
        "observed_at_utc": raw.get("observed_at_utc"),
        "canonical_url": raw.get("canonical_url"),
        "raw_fingerprint": raw.get("raw_fingerprint"),
        "raw_pick_text": raw.get("raw_pick_text"),
        "raw_block": raw.get("raw_block"),
        "matchup_hint": raw.get("matchup_hint"),
        "expert_name": _expert_name,
        "expert_profile": raw.get("expert_profile"),
        "expert_slug": raw.get("expert_slug"),
        "expert_handle": raw.get("expert_handle"),
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
        player_slug = re.sub(r"[^a-z0-9]+", "_", (raw.get("player_name") or "").lower()).strip("_")
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

    # Pre-graded result (from historical table)
    pregraded_result = raw.get("pregraded_result")
    result_block = None
    if pregraded_result and pregraded_result in {"WIN", "LOSS", "PUSH"}:
        result_block = {
            "status": pregraded_result,
            "graded_by": source_id,
            "graded_at_utc": raw.get("observed_at_utc"),
        }

    eligibility = {
        "eligible_for_consensus": eligible,
        "ineligibility_reason": inelig_reason,
    }

    rec = {
        "provenance": provenance,
        "event": event,
        "market": market,
        "eligible_for_consensus": eligible,
        "ineligibility_reason": inelig_reason,
        "eligibility": eligibility,
    }
    if result_block:
        rec["result"] = result_block

    return rec


def normalize_juicereel_records(
    raw_records: List[Dict[str, Any]],
    debug: bool = False,
    sport: str = "NBA",
) -> List[Dict[str, Any]]:
    """Normalize a list of JuiceReel raw records. Includes all eligible records."""
    normalized: List[Dict[str, Any]] = []
    inelig_reasons: Dict[str, int] = {}

    for raw in raw_records:
        if not isinstance(raw, dict):
            continue
        rec = normalize_juicereel_record(raw, sport=sport)
        if rec.get("eligible_for_consensus"):
            normalized.append(rec)
        else:
            reason = rec.get("ineligibility_reason") or "unknown"
            inelig_reasons[reason] = inelig_reasons.get(reason, 0) + 1
            # Also include ineligible records that have a pre-graded result —
            # they can't contribute to consensus but can be graded for expert win-rate tracking.
            # Require both teams to confirm it's an actual NBA/NCAAB game (filters NHL, UFC, etc.)
            if (rec.get("result")
                    and rec.get("event", {}).get("away_team")
                    and rec.get("event", {}).get("home_team")):
                normalized.append(rec)

    # Deduplicate by (event_key, source_id, market_type, selection, line)
    # Include source_id so both experts' picks on same game are preserved
    deduped: Dict[Tuple, Dict[str, Any]] = {}
    removed = 0

    for rec in normalized:
        ev = rec.get("event") or {}
        mk = rec.get("market") or {}
        prov = rec.get("provenance") or {}
        # For ineligible records without a selection, use fingerprint to avoid collapsing
        # distinct picks (e.g. expert bets same game twice with different results across days)
        sel = mk.get("selection")
        fp = prov.get("raw_fingerprint") if sel is None else None
        key = (
            ev.get("event_key"),
            prov.get("source_id"),
            mk.get("market_type"),
            sel,
            mk.get("line"),
            fp,
        )
        if key in deduped:
            removed += 1
            existing = deduped[key]
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
        print(f"[DEBUG] JuiceReel normalized: total={total} eligible={elig} ineligible={inelig_total} removed_dupes={removed}")
        if inelig_reasons:
            print(f"[DEBUG] JuiceReel ineligibility reasons: {inelig_reasons}")

    return normalized


def normalize_file(raw_path: str, out_path: str, debug: bool = False, sport: str = "NBA") -> List[Dict[str, Any]]:
    """Normalize a raw JuiceReel JSON file and write output."""
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

    normalized = normalize_juicereel_records(raw_records, debug=debug, sport=sport)
    write_json(out_path, normalized)
    return normalized


__all__ = ["normalize_juicereel_record", "normalize_juicereel_records", "normalize_file"]
