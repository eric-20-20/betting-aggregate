from __future__ import annotations

import json
import os, sys
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from betql_utils import extract_player_name_from_text, slugify_player_name
from src.player_aliases_nba import normalize_player_key

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from store import data_store, write_json
from utils import normalize_text

ALLOWED_STANDARD = {"spread", "total"}
MIN_PROP_STARS = 3
AMBIGUOUS_SLUGS = {"m_bridge", "m_bridges"}

PROP_STAT_MAP = {
    "points": "points",
    "point": "points",
    "rebounds": "rebounds",
    "rebound": "rebounds",
    "reb": "rebounds",
    "assists": "assists",
    "assist": "assists",
    "ast": "assists",
    "threes": "threes",
    "made threes": "threes",
    "threes made": "threes",
    "3-pointers made": "threes",
    "3 pointers made": "threes",
    "three_pointers": "threes",
    "three pointers": "threes",
    "3pt": "threes",
    "3ptm": "threes",
    "3pm": "threes",
    "pts+reb+ast": "pts_reb_ast",
    "points_rebounds_assists": "pts_reb_ast",
    "pra": "pts_reb_ast",
    "points_rebounds": "pts_reb",
    "pts+reb": "pts_reb",
    "points_assists": "pts_ast",
    "pts+ast": "pts_ast",
    "rebounds_assists": "reb_ast",
    "reb+ast": "reb_ast",
    "ra": "reb_ast",
    "steals": "steals",
    "stl": "steals",
    "blocks": "blocks",
    "blk": "blocks",
    "turnovers": "turnovers",
    "to": "turnovers",
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


def _build_event_keys(raw: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    dt = _parse_dt(raw.get("event_start_time_utc")) or _parse_dt(raw.get("observed_at_utc"))
    if not dt:
        return None, None
    day_key = f"NBA:{dt.year:04d}:{dt.month:02d}:{dt.day:02d}"
    event_key = day_key
    return event_key, day_key


def _parse_matchup_hint(hint: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not hint:
        return None, None, None
    parts = re.split(r"@|vs|v", hint, flags=re.IGNORECASE)
    parts = [p.strip() for p in parts if p.strip()]
    if len(parts) < 2:
        return None, None, None
    away_alias, home_alias = parts[0], parts[1]

    def resolve(alias: str) -> Optional[str]:
        codes = data_store.lookup_team_code(alias)
        if len(codes) == 1:
            return next(iter(codes))
        return None

    away = resolve(away_alias)
    home = resolve(home_alias)
    return away, home, f"{away}@{home}" if away and home else None


def _parse_standard_pick(text: str) -> Tuple[Optional[str], Optional[float], Optional[int]]:
    odds = None
    m_odds = re.search(r"([+-]\d{2,4})", text)
    if m_odds:
        odds = int(m_odds.group(1))
    line = None
    m_line = re.search(r"([+-]?\d+(?:\.\d+)?)", text)
    if m_line:
        try:
            line = float(m_line.group(1))
        except ValueError:
            line = None
    selection = None
    tokens = normalize_text(text).split()
    for tok in tokens:
        codes = data_store.lookup_team_code(tok)
        if len(codes) == 1:
            selection = next(iter(codes))
            break
    return selection, line, odds


def _safe_float(val: Any) -> Optional[float]:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _map_team(alias: Optional[str]) -> Optional[str]:
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
        "pelicans": "NOP",
        "pelican": "NOP",
    }
    norm = normalize_text(alias)
    if norm in fallback:
        return fallback[norm]
    if len(alias) <= 3:
        return alias.upper()
    return None


def _parse_prop(text: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[float], Optional[int]]:
    cleaned = re.sub(r"^\s*\d+\s*stars?\s*", "", text, flags=re.IGNORECASE)
    odds = None
    m_odds = re.search(r"([+-]\d{2,4})", cleaned)
    if m_odds:
        odds = int(m_odds.group(1))
    line = None
    m_line = re.search(r"(\d+(?:\.\d+)?)", cleaned)
    if m_line:
        try:
            line = float(m_line.group(1))
        except ValueError:
            line = None
    direction = None
    m_dir = re.search(r"\b(over|under)\b", cleaned, re.IGNORECASE)
    if m_dir:
        direction = m_dir.group(1).upper()
    stat_key = None
    for key in [
        "points",
        "rebounds",
        "assists",
        "threes",
        "pts",
        "reb",
        "ast",
        "pra",
        "pts+reb+ast",
        "points_rebounds_assists",
        "points_rebounds",
        "points_assists",
        "rebounds_assists",
    ]:
        if key in normalize_text(cleaned):
            stat_key = key
            break
    player_name = extract_player_name_from_text(cleaned)
    return player_name, stat_key, direction, line, odds


def _stat_from_raw(stat_raw: Optional[str]) -> Optional[str]:
    if not stat_raw:
        return None
    canonical = {"points", "rebounds", "assists", "threes", "steals", "blocks", "turnovers", "pts_reb", "pts_ast", "reb_ast", "pts_reb_ast"}
    raw_lower = str(stat_raw).lower()
    if raw_lower in canonical:
        return raw_lower
    key = normalize_text(stat_raw).replace(" ", "_")
    if key in PROP_STAT_MAP:
        return PROP_STAT_MAP[key]
    for token, mapped in PROP_STAT_MAP.items():
        if token in key:
            return mapped
    return None


def normalize_betql_prop_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize BetQL prop picks into Action/Covers schema envelope.
    """
    rating_val = raw.get("rating_stars") if raw.get("rating_stars") is not None else raw.get("rating")
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

    stat_key = _stat_from_raw(raw.get("stat_raw") or raw.get("stat"))
    player_name = raw.get("player_name") or raw.get("player_name_raw")

    # fallback parse
    if not (player_name and stat_key and direction and line is not None):
        parsed_player, parsed_stat, parsed_dir, parsed_line, parsed_odds = _parse_prop(raw.get("raw_pick_text") or raw.get("raw_block") or "")
        player_name = player_name or parsed_player
        stat_key = stat_key or parsed_stat
        direction = direction or parsed_dir
        line = line if line is not None else parsed_line
        odds = odds if odds is not None else parsed_odds

    stat_key = _stat_from_raw(stat_key)
    player_slug = slugify_player_name(player_name)
    if player_slug in AMBIGUOUS_SLUGS:
        player_slug = None
    player_key = normalize_player_key(f"NBA:{player_slug}" if player_slug else None)

    # Extract nested event object if present (merged BetQL format)
    event = raw.get("event", {}) or {}

    # Check top-level first, then fall back to nested event object
    away_team = raw.get("away_team") or event.get("away_team")
    home_team = raw.get("home_team") or event.get("home_team")

    ineligibility_reason = None
    eligible = True

    if not away_team or not home_team:
        eligible, ineligibility_reason = False, "unparsed_team"

    if not player_key:
        eligible, ineligibility_reason = False, ineligibility_reason or "unparsed_player"

    if not stat_key:
        eligible, ineligibility_reason = False, ineligibility_reason or "unparsed_stat"
    if not direction:
        eligible, ineligibility_reason = False, ineligibility_reason or "unparsed_direction"
    if line is None:
        eligible, ineligibility_reason = False, ineligibility_reason or "unparsed_line"
    if player_slug is None:
        eligible, ineligibility_reason = False, ineligibility_reason or "ambiguous_player"

    # Filter by star rating - only picks with MIN_PROP_STARS or more are eligible
    if rating_val is None:
        eligible, ineligibility_reason = False, ineligibility_reason or "missing_rating"
    elif rating_val < MIN_PROP_STARS:
        eligible, ineligibility_reason = False, ineligibility_reason or "rating_below_threshold"

    observed_dt = _parse_dt(raw.get("observed_at_utc"))
    event_dt = _parse_dt(raw.get("event_start_time_utc") or event.get("event_start_time_utc")) or observed_dt

    # Check if nested event has pre-computed keys
    day_key = raw.get("day_key") or event.get("day_key")
    event_key = raw.get("event_key") or event.get("event_key")
    matchup_key = raw.get("matchup_key") or event.get("matchup_key")

    # Fall back to computing from event_dt if not available
    if not day_key and event_dt:
        day_key = f"NBA:{event_dt.year:04d}:{event_dt.month:02d}:{event_dt.day:02d}"
    if day_key and away_team and home_team:
        if not event_key:
            event_key = f"{day_key}:{away_team}@{home_team}"
        if not matchup_key:
            t1, t2 = sorted([str(home_team).upper(), str(away_team).upper()])
            matchup_key = f"{day_key}:{t1}-{t2}"
    matchup_hint = raw.get("matchup_hint") or event.get("matchup_key")
    if not matchup_key and day_key and matchup_hint and "@" in str(matchup_hint):
        parts = str(matchup_hint).split("@")
        if len(parts) == 2:
            a, h = parts[0].upper(), parts[1].upper()
            t1, t2 = sorted([a, h])
            matchup_key = f"{day_key}:{t1}-{t2}"

    selection = None
    side = None
    if player_slug and stat_key and direction:
        full_player_key = normalize_player_key(f"NBA:{player_slug}")
        selection = f"{full_player_key}::{stat_key}::{direction}"
        side = "player_over" if direction == "OVER" else "player_under" if direction == "UNDER" else direction

    provenance = {
        "source_id": "betql",
        "source_surface": raw.get("source_surface") or "betql_prop_picks",
        "sport": "NBA",
        "observed_at_utc": raw.get("observed_at_utc"),
        "canonical_url": raw.get("canonical_url"),
        "raw_fingerprint": raw.get("raw_fingerprint"),
        "raw_pick_text": raw.get("raw_pick_text"),
        "raw_block": raw.get("raw_block"),
        "matchup_hint": raw.get("matchup_hint"),
        "expert_name": raw.get("expert_name") or "BetQL Model",
        "rating_stars": rating_val,
    }

    event = {
        "sport": "NBA",
        "event_key": event_key,
        "day_key": day_key,
        "matchup_key": matchup_key,
        "away_team": away_team,
        "home_team": home_team,
        "event_start_time_utc": raw.get("event_start_time_utc"),
    }

    market = {
        "market_type": "player_prop",
        "side": side,
        "stat_key": stat_key,
        "line": line,
        "odds": odds,
        "player_key": player_key,
        "selection": selection,
    }

    eligibility = {"eligible_for_consensus": eligible, "ineligibility_reason": ineligibility_reason}

    # Prop grouping aid
    match_key = None
    if day_key and matchup_key and player_slug and stat_key and direction:
        match_key = f"{day_key}|{matchup_key}|{player_slug}|{stat_key}|{direction}"

    return {
        "provenance": provenance,
        "event": event,
        "market": market,
        "match_key": match_key,
        "eligible_for_consensus": eligible,
        "ineligibility_reason": ineligibility_reason,
        "eligibility": eligibility,
    }


def normalize_props_file(raw_path: str, out_path: str, debug: bool = False) -> List[Dict[str, Any]]:
    if not os.path.exists(raw_path):
        write_json(out_path, [])
        return []
    try:
        with open(raw_path, "r", encoding="utf-8") as f:
            raw_records = json.load(f)
    except json.JSONDecodeError:
        raw_records = []

    normalized: List[Dict[str, Any]] = []
    ineligible_reasons: Dict[str, int] = {}
    if isinstance(raw_records, list):
        for item in raw_records:
            if isinstance(item, dict):
                rec = normalize_betql_prop_record(item)
                if rec.get("eligible_for_consensus"):
                    normalized.append(rec)
                else:
                    reason = rec.get("ineligibility_reason") or rec.get("eligibility", {}).get("ineligibility_reason") or "unknown"
                    ineligible_reasons[reason] = ineligible_reasons.get(reason, 0) + 1

    # Deduplicate by (day_key, player_key, stat_key, direction)
    deduped: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    removed = 0
    for rec in normalized:
        ev = rec.get("event") or {}
        mk = rec.get("market") or {}
        dk = ev.get("day_key")
        key = (dk, mk.get("player_key"), mk.get("stat_key"), mk.get("selection").split("::")[-1] if mk.get("selection") else mk.get("side"))
        if key in deduped:
            removed += 1
            existing = deduped[key]
            # keep better odds if present (higher absolute value)
            ex_odds = (existing.get("market") or {}).get("odds")
            new_odds = mk.get("odds")
            if ex_odds is None and new_odds is not None:
                existing["market"]["odds"] = new_odds
            elif ex_odds is not None and new_odds is not None:
                if abs(new_odds) > abs(ex_odds):
                    existing["market"]["odds"] = new_odds
            # keep line if missing
            if (existing.get("market") or {}).get("line") is None and mk.get("line") is not None:
                existing["market"]["line"] = mk.get("line")
            continue
        deduped[key] = rec

    normalized = list(deduped.values())
    write_json(out_path, normalized)
    if debug:
        total = len(raw_records) if isinstance(raw_records, list) else 0
        elig = len(normalized)
        inelig_total = sum(ineligible_reasons.values())
        print(f"[DEBUG] BetQL props total={total} eligible={elig} ineligible={inelig_total} removed_dupes={removed}")
        if ineligible_reasons:
            print(f"[DEBUG] BetQL prop ineligibility reasons: {ineligible_reasons}")
    return normalized


def normalize_raw_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    # Safe defaults to avoid unbound locals across market branches
    player_slug: Optional[str] = None
    player_key: Optional[str] = None
    stat_key: Optional[str] = None
    direction: Optional[str] = None
    selection: Optional[str] = None
    side: Optional[str] = None
    line: Optional[float] = None
    odds: Optional[int] = None
    inelig_reason: Optional[str] = None
    eligible = True

    surface = raw.get("source_surface")
    raw_pick_text = raw.get("raw_pick_text") or ""
    market_family = raw.get("market_family") or "unknown"
    market_type = market_family

    event_key, day_key = _build_event_keys(raw)
    away, home, matchup = _parse_matchup_hint(raw.get("matchup_hint"))
    away = _map_team(away or raw.get("away_team"))
    home = _map_team(home or raw.get("home_team"))
    matchup_key = None
    if day_key and away and home:
        event_key = f"{day_key}:{away}@{home}"
        t1, t2 = sorted([str(home).upper(), str(away).upper()])
        matchup_key = f"{day_key}:{t1}-{t2}"

    if market_family == "player_prop":
        market_type = "player_prop"
        player_name, stat_key, direction, line, odds = _parse_prop(raw_pick_text)
        side = direction
        player_slug = slugify_player_name(player_name)
        if player_slug in AMBIGUOUS_SLUGS:
            player_slug = None
        player_key = normalize_player_key(f"NBA:{player_slug}" if player_slug else None)
        if not player_key:
            eligible, inelig_reason = False, "unparsed_player"
        stat_key = _stat_from_raw(stat_key)
        direction = direction.upper() if isinstance(direction, str) else direction
        if player_slug and stat_key and direction and player_key:
            selection = f"{player_key}::{stat_key}::{direction}"
            side = "player_over" if direction == "OVER" else "player_under" if direction == "UNDER" else direction
    else:
        selection_raw = raw.get("selection_hint") or raw.get("side_hint")
        selection = _map_team(selection_raw)
        if market_type == "total":
            selection = (raw.get("side_hint") or selection or "").upper() or None
            line = _safe_float(raw.get("line_hint"))
            odds = raw.get("odds_hint")
            side = selection
        else:
            parsed_selection, parsed_line, odds = _parse_standard_pick(raw_pick_text)
            selection = selection or _map_team(parsed_selection)
            if line is None:
                line = _safe_float(raw.get("line_hint"))
            if line is None:
                line = parsed_line
            side = selection
        if surface in {"betql_probet_spread", "betql_probet_total", "betql_sharp_spread", "betql_sharp_total"}:
            pct = raw.get("pro_pct") or 0
            if pct < 5:
                eligible, inelig_reason = False, "sharp_pct_below_threshold"
        if surface in {"betql_model_spread", "betql_model_total"}:
            rating = raw.get("rating_stars")
            if rating is None:
                eligible, inelig_reason = False, "missing_rating"
            elif rating < 3:
                eligible, inelig_reason = False, "rating_below_threshold"
        if selection is None and market_type == "spread":
            eligible, inelig_reason = False, "unparsed_team"
        if selection is None and market_type == "total":
            eligible, inelig_reason = False, "unparsed_direction"

    event = {
        "sport": "NBA",
        "event_key": event_key,
        "day_key": day_key,
        "matchup_key": matchup_key,
        "away_team": away,
        "home_team": home,
        "event_start_time_utc": raw.get("event_start_time_utc"),
    }

    market = {
        "market_type": market_type,
        "market_family": market_family,
        "selection": selection,
        "side": side,
        "line": line,
        "line_hint": raw.get("line_hint") if line is None else line,
        "odds": odds,
        "player_key": player_key,
        "stat_key": stat_key,
    }

    prov_fields = [
        "source_id",
        "source_surface",
        "observed_at_utc",
        "canonical_url",
        "raw_pick_text",
        "raw_block",
        "raw_fingerprint",
        "expert_name",
        "expert_profile",
        "expert_slug",
        "expert_handle",
        "tailing_handle",
        "source_updated_at_utc",
        "matchup_hint",
        "rating_stars",
        "pro_pct",
    ]
    provenance = {k: raw.get(k) for k in prov_fields}

    eligible = eligible and market_type in (ALLOWED_STANDARD | {"player_prop"})
    if market_type == "player_prop":
        if not (player_key and stat_key and side):
            eligible, inelig_reason = False, "missing_required_fields"
    if market_type not in ALLOWED_STANDARD and market_type != "player_prop":
        eligible, inelig_reason = False, "unsupported_market"
    if market_type in {"spread", "total"} and (not away or not home):
        eligible, inelig_reason = False, "unparsed_team"
    if not eligible and not inelig_reason:
        inelig_reason = "unsupported_market"

    return {
        "eligible_for_consensus": eligible,
        "ineligibility_reason": inelig_reason,
        "event": event,
        "market": market,
        "provenance": provenance,
        "bet_kind": (
            "model"
            if isinstance(surface, str) and "betql_model_" in surface
            else (
                "probet"
                if isinstance(surface, str) and ("betql_probet_" in surface or "betql_sharp_" in surface)
                else None
            )
        ),
    }


def normalize_file(raw_path: str, out_path: str, debug: bool = False) -> List[Dict[str, Any]]:
    if not os.path.exists(raw_path):
        write_json(out_path, [])
        return []
    try:
        with open(raw_path, "r", encoding="utf-8") as f:
            raw_records = json.load(f)
    except json.JSONDecodeError:
        raw_records = []
    normalized: List[Dict[str, Any]] = []
    skipped = 0
    if isinstance(raw_records, list):
        for item in raw_records:
            if isinstance(item, dict):
                rec = normalize_raw_record(item)
                if rec:
                    normalized.append(rec)
                else:
                    skipped += 1

    # Backfill BetQL probet lines from matching model lines
    model_lines: Dict[Tuple[str, str, str], float] = {}
    for rec in normalized:
        prov = rec.get("provenance") or {}
        mk = rec.get("market") or {}
        ev = rec.get("event") or {}
        if prov.get("source_id") == "betql" and rec.get("bet_kind") == "model" and mk.get("line") is not None:
            key = (ev.get("event_key"), mk.get("market_type"), mk.get("selection"))
            model_lines[key] = mk.get("line")

    for rec in normalized:
        prov = rec.get("provenance") or {}
        mk = rec.get("market") or {}
        ev = rec.get("event") or {}
        if prov.get("source_id") == "betql" and rec.get("bet_kind") == "probet" and mk.get("line") is None:
            key = (ev.get("event_key"), mk.get("market_type"), mk.get("selection"))
            if key in model_lines:
                mk["line"] = model_lines[key]
                if debug:
                    print(f"[DEBUG] BetQL probet line backfilled: {key[0]} {key[1]} {key[2]} line={model_lines[key]}")
    write_json(out_path, normalized)
    if debug and skipped:
        print(f"[DEBUG] normalize_file skipped={skipped} records (unknown/invalid shape)")
    return normalized


__all__ = ["normalize_raw_record", "normalize_file"]
