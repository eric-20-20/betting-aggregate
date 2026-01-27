from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from store import data_store, write_json
from utils import normalize_text

ALLOWED_STANDARD = {"spread", "moneyline", "total"}


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
    event_key = f"NBA:{dt.year:04d}:{dt.month:02d}:{dt.day:02d}"
    day_key = ":".join(event_key.split(":")[:3])
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


def _parse_prop(text: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[float], Optional[int]]:
    cleaned = re.sub(r"^\s*\d+\s*stars?\s*", "", text, flags=re.IGNORECASE)
    odds = None
    m_odds = re.search(r"([+-]\d{2,4})", cleaned)
    if m_odds:
        odds = int(m_odds.group(1))
    line = None
    m_line = re.search(r"(\d+(?:\.\d+)?)\s*(?:points|rebound|rebounds|assists|threes|pts|reb|ast)", cleaned, re.IGNORECASE)
    if m_line:
        line = float(m_line.group(1))
    direction = None
    m_dir = re.search(r"\b(over|under)\b", cleaned, re.IGNORECASE)
    if m_dir:
        direction = m_dir.group(1).upper()
    stat_key = None
    for key in ["points", "rebounds", "assists", "threes", "pts", "reb", "ast", "pra", "pts+reb+ast"]:
        if key in normalize_text(cleaned):
            stat_key = key
            break
    player_name = None
    player_match = re.match(r"([A-Za-z][A-Za-z\.\s'-]+)", cleaned)
    if player_match:
        player_name = player_match.group(1).strip()
    return player_name, stat_key, direction, line, odds


def normalize_raw_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    surface = raw.get("source_surface")
    raw_pick_text = raw.get("raw_pick_text") or ""
    market_family = raw.get("market_family") or "unknown"
    market_type = market_family

    event_key, day_key = _build_event_keys(raw)
    away, home, matchup = _parse_matchup_hint(raw.get("matchup_hint"))

    selection = None
    side = None
    line = None
    odds = None
    player_key = None
    stat_key = None
    inelig_reason = None
    eligible = True

    if surface == "nba_player_props" or market_family == "player_prop":
        market_type = "player_prop"
        player_name, stat_key, direction, line, odds = _parse_prop(raw_pick_text)
        side = direction
        if player_name:
            existing = data_store.lookup_player_key(player_name)
            player_key = existing or data_store.add_player(full_name=player_name, alias_text=player_name, is_verified=False)
        else:
            eligible, inelig_reason = False, "unparsed_player"
        rating_val = raw.get("rating_stars") if raw.get("rating_stars") is not None else raw.get("rating")
        if rating_val is not None and rating_val < 4:
            eligible, inelig_reason = False, "rating_below_threshold"
    else:
        selection, line, odds = _parse_standard_pick(raw_pick_text)
        side = selection
        if surface in {"nba_model_pro_betting", "nba_sharp_picks"}:
            pct = raw.get("pro_pct") or 0
            if pct < 10:
                eligible, inelig_reason = False, "sharp_pct_below_threshold"
        if surface == "nba_model_bets":
            rating = raw.get("rating_stars") if raw.get("rating_stars") is not None else raw.get("rating")
            if rating is None:
                eligible, inelig_reason = False, "missing_rating"
            elif rating < 4:
                eligible, inelig_reason = False, "rating_below_threshold"
        if selection is None:
            eligible, inelig_reason = False, "unparsed_team"

    event = {
        "sport": "NBA",
        "event_key": event_key,
        "day_key": day_key,
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
    ]
    provenance = {k: raw.get(k) for k in prov_fields}

    eligible = eligible and market_type in (ALLOWED_STANDARD | {"player_prop"})
    if market_type == "player_prop":
        if not (player_key and stat_key and side):
            eligible, inelig_reason = False, "missing_required_fields"
    if not eligible and not inelig_reason:
        inelig_reason = "unsupported_market"

    return {
        "eligible_for_consensus": eligible,
        "ineligibility_reason": inelig_reason,
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
