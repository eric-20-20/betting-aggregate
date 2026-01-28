from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from store import data_store, write_json
from utils import normalize_text

ALLOWED_MARKETS = {"spread", "moneyline", "total"}


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
    event_key = f"NBA:{dt.year:04d}:{dt.month:02d}"
    if dt.day:
        event_key = f"{event_key}:{dt.day:02d}"
    parts = event_key.split(":")
    day_key = event_key if len(parts) >= 4 else event_key
    return event_key, day_key


def _parse_odds(text: str) -> Optional[int]:
    if not text:
        return None
    m = re.search(r"([+-]\d{2,4})", text)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return None


def _normalize_half(value: str) -> str:
    return value.replace("½", ".5").replace("Â½", ".5")


def _parse_line(text: str) -> Optional[float]:
    if not text:
        return None
    txt = _normalize_half(text)
    m = re.search(r"([+-]?\d+(?:\.\d+)?)", txt)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return None
    return None


def _detect_market_type(text: str) -> str:
    txt = text.lower()
    if re.search(r"\bover\b|\bunder\b", txt) and re.search(r"\d", txt):
        return "total"
    if re.search(r"\bml\b|\bmoneyline\b", txt):
        return "moneyline"
    return "unknown"


def _resolve_team(text: str) -> Optional[str]:
    normalized = normalize_text(text)
    manual = {"cavs": "CLE", "cavaliers": "CLE"}
    if normalized in manual:
        return manual[normalized]
    codes = data_store.lookup_team_code(normalized)
    if len(codes) == 1:
        return next(iter(codes))
    if len(codes) > 1:
        return ",".join(sorted(codes))
    return None


def _extract_team_and_line(text: str) -> Tuple[Optional[str], Optional[float], Optional[int]]:
    txt = _normalize_half(text)
    tokens = txt.split()
    team = None
    for tok in tokens:
        letters = re.sub(r"[^A-Za-z]", "", tok)
        if not letters:
            continue
        team = _resolve_team(letters)
        if team:
            break

    numbers: List[float] = []
    for tok in tokens:
        if re.match(r"[+-]?\d+(?:\.\d+)?", tok):
            try:
                numbers.append(float(tok))
            except ValueError:
                continue
    spread = None
    odds = None
    for val in numbers:
        if abs(val) <= 40 and spread is None:
            spread = val
        elif abs(val) >= 50 and odds is None:
            odds = int(val)
    # If we only saw one number and it's small (e.g., +6.5) treat as spread; odds may be elsewhere.
    return team, spread, odds


def _parse_matchup_hint(raw: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    hint = raw.get("matchup_hint")
    if not hint or not isinstance(hint, str):
        return None, None, None, None
    txt = hint
    # normalize separators
    if "@" in txt:
        parts = [p.strip() for p in txt.split("@", 1)]
        away_alias, home_alias = parts if len(parts) == 2 else (None, None)
    elif re.search(r"\bvs\b|\bv\b", txt, re.IGNORECASE):
        parts = re.split(r"\bvs\.?\b|\bv\b", txt, flags=re.IGNORECASE)
        parts = [p.strip(" |") for p in parts if p.strip(" |")]
        away_alias, home_alias = parts[:2] if len(parts) >= 2 else (None, None)
    elif re.search(r"\bat\b", txt, re.IGNORECASE):
        parts = re.split(r"\bat\b", txt, flags=re.IGNORECASE)
        parts = [p.strip() for p in parts if p.strip()]
        away_alias, home_alias = parts[:2] if len(parts) >= 2 else (None, None)
    else:
        return None, None, None, None

    def resolve(alias: Optional[str]) -> Tuple[Optional[str], Optional[bool]]:
        if not alias:
            return None, None
        code = _resolve_team(alias)
        if code and "," in code:
            return None, False  # ambiguous marker
        return code, True

    away_code, away_ok = resolve(away_alias)
    home_code, home_ok = resolve(home_alias)

    if away_ok is False or home_ok is False:
        return None, None, "ambiguous_team", hint
    if not away_code or not home_code:
        return None, None, "team_not_found", hint
    matchup = f"{away_code}@{home_code}"
    return away_code, home_code, None, matchup


def _extract_total(text: str) -> Tuple[Optional[str], Optional[float]]:
    txt = text.lower()
    dir_match = re.search(r"\b(over|under)\b", txt)
    direction = dir_match.group(1).upper() if dir_match else None
    line = _parse_line(text)
    return direction, line


def _eligibility(market_type: str, selection: Optional[str], line: Optional[float], direction: Optional[str]) -> Tuple[bool, Optional[str]]:
    if market_type not in ALLOWED_MARKETS:
        return False, "unsupported_market"
    if market_type == "total":
        if not direction:
            return False, "unparsed_direction"
        if line is None:
            return False, "unparsed_line"
        return True, None
    if market_type in {"spread", "moneyline"}:
        if not selection:
            return False, "unparsed_team"
        return True, None
    return False, "unsupported_market"


def normalize_raw_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    raw_pick_text = raw.get("raw_pick_text") or ""
    market_family = raw.get("market_family") or _detect_market_type(raw_pick_text)
    market_type = _detect_market_type(raw_pick_text)

    selection = None
    side = None
    line = None
    odds = _parse_odds(raw_pick_text) or _parse_odds(raw.get("raw_block") or "")
    stat_key = None
    player_key = None

    away_team = raw.get("away_team")
    home_team = raw.get("home_team")
    matchup = None
    matchup_error = None
    parsed_away, parsed_home, match_err, matchup_str = _parse_matchup_hint(raw)
    if parsed_away and parsed_home:
        away_team, home_team = parsed_away, parsed_home
        matchup = matchup_str
    elif match_err:
        matchup_error = match_err

    if market_type == "total":
        direction, line = _extract_total(raw_pick_text)
        side = direction
        selection = direction
    else:
        team, spread_val, parsed_odds = _extract_team_and_line(raw_pick_text)
        odds = odds or parsed_odds
        selection = team
        side = team
        if spread_val is not None:
            market_type = "spread"
            line = spread_val
            if odds is None:
                odds = -110
        elif odds is not None and team:
            market_type = "moneyline"
            line = None
        else:
            market_type = "unknown"

    if market_family == "unknown":
        market_family = market_type

    event_key, day_key = _build_event_keys(raw)
    matchup_key = None
    if day_key and away_team and home_team:
        event_key = f"{day_key}:{away_team}@{home_team}"
        teams_sorted = sorted([away_team, home_team])
        matchup_key = f"{day_key}:{teams_sorted[0]}-{teams_sorted[1]}"
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
        "market_type": market_type,
        "market_family": market_family,
        "selection": selection,
        "side": side,
        "line": line,
        "odds": odds,
        "player_key": player_key,
        "stat_key": stat_key,
    }

    provenance_fields = [
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
    ]
    provenance = {k: raw.get(k) for k in provenance_fields}

    eligible, reason = _eligibility(market_type, selection, line, side if market_type == "total" else None)
    if matchup_error and market_type in {"spread", "moneyline", "total"}:
        eligible = False
        reason = matchup_error
    market["matchup"] = matchup

    return {
        "eligible_for_consensus": eligible,
        "ineligibility_reason": reason,
        "event": event,
        "market": market,
        "provenance": provenance,
    }


def normalize_file(raw_path: str, out_path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(raw_path):
        write_json(out_path, [])
        return []
    try:
        with open(raw_path, "r", encoding="utf-8") as f:
            raw_records = json.load(f)
    except json.JSONDecodeError:
        raw_records = []

    normalized: List[Dict[str, Any]] = []
    if isinstance(raw_records, list):
        for item in raw_records:
            if isinstance(item, dict):
                normalized.append(normalize_raw_record(item))

    write_json(out_path, normalized)
    return normalized


__all__ = ["normalize_raw_record", "normalize_file"]
