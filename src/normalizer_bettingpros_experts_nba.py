"""
Normalizer for BettingPros user-expert NBA picks.

Converts raw BettingPros expert pick records (from scripts/bettingpros_experts_ingest.py)
to the standard normalized schema used by consensus_nba.py.

Reuses helper functions from normalizer_bettingpros_nba.py.
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

# Import shared helpers from model normalizer
from src.normalizer_bettingpros_nba import (
    _parse_dt,
    _map_team,
    _build_event_keys,
    _snap_spread,
    _parse_matchup,
    _parse_game_date,
)

ALLOWED_MARKETS = {"spread", "total", "moneyline", "player_prop"}

# Stat keyword → canonical stat_key mapping (longest match wins)
_STAT_MAP: Dict[str, str] = {
    # Combos first (must match before single-stat keywords)
    "points + rebounds + assists": "pts_reb_ast",
    "pts + reb + ast": "pts_reb_ast",
    "points rebounds assists": "pts_reb_ast",
    "alt pts + reb + ast": "pts_reb_ast",
    "alt points rebounds assists": "pts_reb_ast",
    "pra": "pts_reb_ast",
    "p+r+a": "pts_reb_ast",
    "points + rebounds": "pts_reb",
    "pts + reb": "pts_reb",
    "points rebounds": "pts_reb",
    "alt pts + reb": "pts_reb",
    "points + assists": "pts_ast",
    "pts + ast": "pts_ast",
    "points assists": "pts_ast",
    "alt pts + ast": "pts_ast",
    "rebounds + assists": "reb_ast",
    "reb + ast": "reb_ast",
    "rebounds assists": "reb_ast",
    "alt reb + ast": "reb_ast",
    # Threes
    "three pointers made": "threes",
    "three pointer made": "threes",
    "3 pointers made": "threes",
    "3-pointers made": "threes",
    "made threes": "threes",
    "threes": "threes",
    "3pm": "threes",
    # Singles
    "points": "points",
    "pts": "points",
    "point": "points",
    "alt points": "points",
    "rebounds": "rebounds",
    "reb": "rebounds",
    "rebound": "rebounds",
    "alt rebounds": "rebounds",
    "assists": "assists",
    "ast": "assists",
    "assist": "assists",
    "alt assists": "assists",
    "steals": "steals",
    "stl": "steals",
    "steal": "steals",
    "blocks": "blocks",
    "blk": "blocks",
    "block": "blocks",
}

# Pre-sorted by length descending for greedy matching
_STAT_MAP_SORTED = sorted(_STAT_MAP.keys(), key=len, reverse=True)

# Stat keyword regex for player name boundary detection
_STAT_KW_RE = re.compile(
    r"(?:Points?|Rebounds?|Assists?|Steals?|Blocks?|"
    r"Three\s+Pointers?\s+Made|Made\s+Threes?|Threes?|"
    r"Pts?|Reb|Ast|Stl|Blk|PRA|Alt\s+\w+)",
    re.IGNORECASE,
)


def _extract_prop_stat(text: str) -> Optional[str]:
    """Extract canonical stat_key from a player prop raw_pick_text."""
    t = text.lower()
    # Strip matchup prefix (everything up to first ' - ' after the '@' or 'At'/'vs')
    if " @ " in t or " at " in t or " vs " in t:
        m = re.split(r"\s+-\s+", text, maxsplit=1)
        t = m[1].lower() if len(m) > 1 else t
    # Strip trailing direction/line
    t = re.sub(r"\s*[-–]\s*(over|under|o/u).*$", "", t)
    t = re.sub(r"\s+(over|under)\s*$", "", t)
    t = re.sub(r"\s*[\d.]+\+?\s*(over|under)?\s*$", "", t)
    t = re.sub(r"\s*(o/u)\s*$", "", t).strip()

    # Pattern: "To Record/Score X+ STAT - Player"
    m = re.search(r"to (?:record|score)\s+[\d.]+\+?\s+(.+?)(?:\s*-\s*\w[\w\s]*?)?\s*$", t, re.I)
    if m:
        stat_raw = m.group(1).strip().rstrip("-").strip()
        if stat_raw in _STAT_MAP:
            return _STAT_MAP[stat_raw]

    # Pattern: "Player - STAT_LABEL - ..." (dash-delimited)
    parts = [p.strip() for p in t.split(" - ")]
    if len(parts) >= 2:
        for part in parts[1:]:
            part_clean = re.sub(r"\s+alt\s+", " ", part).strip()
            if part_clean in _STAT_MAP:
                return _STAT_MAP[part_clean]

    # Greedy keyword scan
    for phrase in _STAT_MAP_SORTED:
        if phrase in t:
            return _STAT_MAP[phrase]

    return None


def _extract_prop_player(text: str) -> Optional[str]:
    """Extract player name from a player prop raw_pick_text."""
    if not text or text.strip() == "player_prop":
        return None

    # Strip matchup prefix
    core = text
    if " @ " in text or " At " in text or re.search(r"\s+vs\.?\s+", text, re.I):
        m = re.split(r"\s+-\s+", text, maxsplit=1)
        core = m[1] if len(m) > 1 else text
    core = core.strip()

    # Pattern: "To Record/Score X+ STAT - PLAYER [OVER/UNDER]"
    m = re.match(
        r"To (?:Record|Score)\s+[\d.]+\+?\s+[\w\s+]+?\s*-\s*(.+?)(?:\s+(?:OVER|UNDER))?\s*$",
        core, re.IGNORECASE,
    )
    if m:
        name = re.sub(r"\s+(OVER|UNDER)\s*$", "", m.group(1), flags=re.IGNORECASE).strip()
        if name and len(name) > 2:
            return name

    # Pattern: "PLAYER - STAT - PLAYER Over/Under" (player name repeated as last segment)
    parts = [p.strip() for p in re.split(r"\s+-\s+", core)]
    if len(parts) >= 3:
        last = re.sub(
            r"\s+(OVER|UNDER|Over\s+[\d.]+|Under\s+[\d.]+)\s*$", "", parts[-1], flags=re.IGNORECASE
        ).strip()
        if len(last) > 2 and not re.match(r"^(Alt|Made|Player|To\s)", last, re.IGNORECASE):
            return parts[0]

    # Pattern: "PLAYER STAT_KEYWORD ..."
    m = _STAT_KW_RE.search(core)
    if m and m.start() > 0:
        name = core[: m.start()].strip().rstrip("-").strip()
        name = re.sub(r"^Alt\s+", "", name, flags=re.IGNORECASE).strip()
        if name and len(name) > 2 and not re.match(r"^(To\s|Player\s|Made\s)", name, re.IGNORECASE):
            return name

    # Pattern: "STAT_LABEL - PLAYER Over X.X" (stat before player, e.g. 'vs' format)
    if len(parts) >= 2:
        last_part = re.sub(r"\s+(Over|Under)\s+[\d.]+\s*$", "", parts[-1], flags=re.IGNORECASE).strip()
        last_part = re.sub(r"\s+(OVER|UNDER)\s*$", "", last_part, flags=re.IGNORECASE).strip()
        if last_part and len(last_part) > 3 and not re.match(r"^(Alt|Made)\s", last_part, re.IGNORECASE):
            return last_part

    return None


def _parse_teams_from_text(text: str, store=None, sport: str = "NBA") -> Tuple[Optional[str], Optional[str]]:
    """
    Try to extract away/home team codes from raw_pick_text.
    Handles format: 'DET Pistons @ BKN Nets - Total - Under 216.5'
    or 'Dallas Mavericks @ Atlanta Hawks - Spread - DAL -5.5'
    """
    if not text or " @ " not in text:
        return None, None
    parts = text.split(" @ ", 1)
    away_raw = parts[0].strip()
    # home is everything before the first ' - '
    remainder = parts[1]
    home_raw = remainder.split(" - ")[0].strip() if " - " in remainder else remainder.strip()

    # Short abbreviation overrides not covered by _map_team's fallback dict
    _SHORT_ABBREV: Dict[str, str] = {
        "sa": "SAS", "gs": "GSW", "ny": "NYK", "la": "LAL",
        "okc": "OKC", "nop": "NOP", "no": "NOP",
    }

    def _try_map(raw: str) -> Optional[str]:
        result = _map_team(raw, store=store, sport=sport)
        if result:
            return result
        # Fallback: try just the first token (handles "DET Pistons" → "DET",
        # "SA Spurs" → "SA" → "SAS", "GS Warriors" → "GS" → "GSW")
        first_token = raw.split()[0].lower() if raw else ""
        if first_token and first_token != raw.lower():
            if first_token in _SHORT_ABBREV:
                return _SHORT_ABBREV[first_token]
            result = _map_team(first_token, store=store, sport=sport)
        return result

    away = _try_map(away_raw)
    home = _try_map(home_raw)
    return away, home


def normalize_bettingpros_experts_record(
    raw: Dict[str, Any],
    sport: str = "NBA",
    target_date: Optional[str] = None,
    include_parlays: bool = False,
) -> Dict[str, Any]:
    """Normalize a single BettingPros expert pick record.

    Args:
        raw: Raw pick record dict.
        sport: Sport code (default "NBA").
        target_date: If provided (YYYY-MM-DD), records whose game_date differs are
                     marked ineligible with reason "wrong_date". Used to filter out
                     stale unsettled picks from an expert's open portfolio.
        include_parlays: If True, parlay legs are treated as standalone picks (for
                         history/ledger use). Default False blocks them from consensus.
    """
    store = get_data_store(sport)

    observed_dt = _parse_dt(raw.get("observed_at_utc"))
    event_dt = _parse_game_date(raw) or observed_dt

    # Map teams — these are already team codes from BettingPros API
    away_team = _map_team(raw.get("away_team"), store=store, sport=sport)
    home_team = _map_team(raw.get("home_team"), store=store, sport=sport)

    if not (away_team and home_team) and raw.get("matchup_hint"):
        away_team, home_team = _parse_matchup(raw["matchup_hint"], store=store, sport=sport)

    # Last resort: parse teams from raw_pick_text
    if not (away_team and home_team):
        away_team, home_team = _parse_teams_from_text(
            raw.get("raw_pick_text", ""), store=store, sport=sport
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

    # Date gate: reject records for wrong game date (stale unsettled portfolio picks).
    # Compare the constructed day_key against the target to catch both:
    #   - records with explicit game_date (set in raw)
    #   - records without game_date but with event_start_time_utc (day inferred from event time)
    if target_date and day_key:
        target_day_key = f"{sport}:{target_date.replace('-', ':')}"
        if day_key != target_day_key:
            eligible = False
            inelig_reason = "wrong_date"

    # Parlay legs: block from consensus but allow through for history/ledger grading
    if raw.get("parlay_id") and not include_parlays:
        eligible = False
        inelig_reason = "parlay_leg"
        market_type = market_type or "parlay"

    elif not market_type or market_type not in ALLOWED_MARKETS:
        eligible = False
        inelig_reason = "unsupported_market"

    elif (not away_team or not home_team) and market_type != "player_prop":
        eligible = False
        inelig_reason = "unparsed_team"

    elif market_type == "spread":
        # Quarter/half markets: block from consensus — no grading infrastructure.
        # Detect by text keywords (most reliable) or known quarter market IDs.
        _QUARTER_MARKET_IDS = {137, 143, 153, 163, 164, 165, 167, 168}
        _raw_block_pick = {}
        try:
            _raw_block_pick = json.loads(raw.get("raw_block") or "{}").get("pick") or {}
        except (json.JSONDecodeError, AttributeError):
            pass
        _raw_text_lower = (raw.get("raw_pick_text") or "").lower()
        _market_id = _raw_block_pick.get("market_id")
        _is_quarter_half = (
            any(kw in _raw_text_lower for kw in (
                "1st quarter", "2nd quarter", "3rd quarter", "4th quarter",
                "1st half", "2nd half", "halftime",
            ))
            or _market_id in _QUARTER_MARKET_IDS
        )
        if _is_quarter_half:
            eligible = False
            inelig_reason = "quarter_market_unsupported"
        elif not selection:
            eligible = False
            inelig_reason = "missing_direction"
        elif line is None:
            eligible = False
            inelig_reason = "missing_line"
        else:
            # Alternate spread fix: when stored line=±1 AND raw_pick_text contains
            # "Alternate Spread", the API returns line.line=1 (integer placeholder).
            # The real line appears as the last signed number in raw_pick_text,
            # e.g. "...Alternate Spread - Washington Wizards +15.5" → +15.5
            _is_alternate = "alternate spread" in _raw_text_lower
            if _is_alternate and abs(line) == 1.0:
                _tail_m = re.search(r"([+-]\d+\.?\d*)\s*$", (raw.get("raw_pick_text") or "").strip())
                if _tail_m:
                    _extracted = float(_tail_m.group(1))
                    if abs(_extracted) != 1.0:
                        line = _extracted
                    else:
                        eligible = False
                        inelig_reason = "unparsed_alternate_line"
                else:
                    eligible = False
                    inelig_reason = "unparsed_alternate_line"

            if eligible:
                mapped = _map_team(selection, store=store, sport=sport)
                selection = mapped or selection.upper()
                side = selection
                line = _snap_spread(line)

    elif market_type == "total":
        direction = (raw.get("side") or selection or "").upper()
        if direction not in ("OVER", "UNDER"):
            eligible = False
            inelig_reason = "missing_direction"
        elif line is None:
            eligible = False
            inelig_reason = "missing_line"
        elif line < 150:
            # Line too low to be a real NBA/NCAAB game total — likely a novelty prop
            # (player SGP, alternate-line special, etc.) mis-classified as a game total.
            # Marked ineligible for now; may be supported in future with proper parsing.
            eligible = False
            inelig_reason = "unsupported_line_type"
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
        prop_direction = (raw.get("side") or "").upper()
        prop_line = line  # already set from raw.get("line")
        text = raw.get("raw_pick_text") or raw.get("selection") or ""

        player_name = _extract_prop_player(text)
        stat_key_parsed = _extract_prop_stat(text)

        if not player_name:
            eligible = False
            inelig_reason = "unparsed_player"
        elif not stat_key_parsed:
            eligible = False
            inelig_reason = "unparsed_stat"
        elif prop_direction not in ("OVER", "UNDER"):
            eligible = False
            inelig_reason = "unparsed_direction"
        elif prop_line is None or prop_line == 1.0:
            eligible = False
            inelig_reason = "unparsed_line"
        else:
            player_slug = re.sub(r"[^a-z0-9]+", "_", player_name.lower()).strip("_")
            player_key = f"{sport}:{player_slug}"
            selection = f"{player_key}::{stat_key_parsed}::{prop_direction}"
            side = prop_direction
            line = prop_line

    # Provenance
    source_id = raw.get("source_id") or "bettingpros_experts"
    provenance = {
        "source_id": source_id,
        "source_surface": raw.get("source_surface") or "bettingpros_user",
        "sport": sport,
        "observed_at_utc": raw.get("observed_at_utc"),
        "canonical_url": raw.get("canonical_url"),
        "raw_fingerprint": raw.get("raw_fingerprint"),
        "raw_pick_text": raw.get("raw_pick_text"),
        "raw_block": raw.get("raw_block"),
        "matchup_hint": raw.get("matchup_hint"),
        "expert_name": raw.get("expert_name"),
        "expert_slug": raw.get("expert_slug"),
        "wager": raw.get("wager_units"),
        "to_win": None,
        "parlay_id": raw.get("parlay_id"),
    }

    # Build player_key and stat_key for market block when prop is eligible
    pp_player_key = None
    pp_stat_key = None
    pp_market_family = raw.get("market_family") or "standard"
    if market_type == "player_prop" and eligible:
        # player_name and stat_key_parsed are set in the player_prop branch above
        pp_player_key = selection.split("::")[0] if "::" in (selection or "") else None
        pp_stat_key = selection.split("::")[1] if selection and selection.count("::") >= 2 else None
        pp_market_family = "player_prop"

    event = {
        "sport": sport,
        "event_key": event_key,
        "day_key": day_key,
        "matchup_key": matchup_key,
        "away_team": away_team,
        "home_team": home_team,
        "event_start_time_utc": raw.get("event_start_time_utc"),
    }

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

    # Pre-graded result
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


def normalize_bettingpros_experts_records(
    raw_records: List[Dict[str, Any]],
    debug: bool = False,
    sport: str = "NBA",
    target_date: Optional[str] = None,
    include_parlays: bool = False,
) -> List[Dict[str, Any]]:
    """Normalize a list of BettingPros expert raw records.

    Args:
        target_date: If provided (YYYY-MM-DD), records for other dates are dropped.
                     Pass today's date for daily consensus; omit for backfill.
        include_parlays: If True, parlay legs are normalized as standalone picks.
                         Use for history/ledger grading; keep False for consensus.
    """
    normalized: List[Dict[str, Any]] = []
    inelig_reasons: Dict[str, int] = {}

    for raw in raw_records:
        if not isinstance(raw, dict):
            continue
        rec = normalize_bettingpros_experts_record(raw, sport=sport, target_date=target_date, include_parlays=include_parlays)
        if rec.get("eligible_for_consensus"):
            normalized.append(rec)
        else:
            reason = rec.get("ineligibility_reason") or "unknown"
            inelig_reasons[reason] = inelig_reasons.get(reason, 0) + 1

    # Deduplicate by (event_key, source_id, expert_slug, market_type, selection, line)
    deduped: Dict[Tuple, Dict[str, Any]] = {}
    removed = 0

    for rec in normalized:
        ev = rec.get("event") or {}
        mk = rec.get("market") or {}
        prov = rec.get("provenance") or {}
        key = (
            ev.get("event_key"),
            prov.get("source_id"),
            prov.get("expert_slug"),
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
        print(f"[DEBUG] BettingPros experts normalized: total={total} eligible={elig} ineligible={inelig_total} removed_dupes={removed}")
        if inelig_reasons:
            print(f"[DEBUG] Ineligibility: {inelig_reasons}")

    return normalized


def normalize_file(
    raw_path: str,
    out_path: str,
    debug: bool = False,
    sport: str = "NBA",
    target_date: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Normalize a raw BettingPros experts JSON file and write output."""
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

    normalized = normalize_bettingpros_experts_records(
        raw_records, debug=debug, sport=sport, target_date=target_date
    )
    write_json(out_path, normalized)
    return normalized


__all__ = [
    "normalize_bettingpros_experts_record",
    "normalize_bettingpros_experts_records",
    "normalize_file",
]
