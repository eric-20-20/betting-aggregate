from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from store import data_store, write_json


def _parse_dt(value: Any) -> Optional[datetime]:
    """Parse ISO8601 string or datetime into an aware UTC datetime."""
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def build_event_key(raw: Dict[str, Any]) -> Optional[str]:
    """Return stable day-based event key expected by consensus_nba.day_key."""
    dt = _parse_dt(raw.get("event_start_time_utc")) or _parse_dt(raw.get("observed_at_utc"))
    if not dt:
        return None
    return f"NBA:{dt.year:04d}:{dt.month:02d}:{dt.day:02d}"


def _clean_player_tokens(text: str) -> str:
    # Expand compressed initials like "G.Antetokounmpo" -> "G Antetokounmpo"
    text = re.sub(r"(?<![A-Za-z])([A-Z])\.([A-Za-z]+)", r"\1 \2", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_direction(text: str) -> Optional[str]:
    if not text:
        return None
    match = re.search(r"\b(over|under)\b", text, re.IGNORECASE)
    if match:
        return match.group(1).upper()

    prefix = re.search(r"\b([ou])\s*\d", text, re.IGNORECASE)
    if prefix:
        return "OVER" if prefix.group(1).lower() == "o" else "UNDER"
    return None


def _extract_line(text: str) -> Optional[float]:
    if not text:
        return None
    match = re.search(r"\b(?:over|under|o|u)\s*([0-9]+(?:\.[0-9]+)?)", text, re.IGNORECASE)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    return None


def _extract_odds(text: str) -> Optional[int]:
    if not text:
        return None
    match = re.search(r"([+-]\d{2,4})", text)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None


def _extract_stat_key(raw: Dict[str, Any]) -> Optional[str]:
    for field in ("raw_pick_text", "raw_block"):
        text = raw.get(field)
        if text:
            key = data_store.lookup_stat_key(text)
            if key:
                return key
    return None


def _extract_player_name(raw_pick_text: str) -> Optional[str]:
    if not raw_pick_text:
        return None

    cleaned = _clean_player_tokens(raw_pick_text)
    match = re.search(r"^\s*([A-Za-z\s\.'-]+?)\s+(?:over|under|o|u)\b", cleaned, re.IGNORECASE)
    if match:
        candidate = match.group(1)
    else:
        tokens = cleaned.split()
        stop = {"over", "under", "o", "u"}
        candidate_parts: List[str] = []
        for tok in tokens:
            if tok.lower() in stop:
                break
            if re.fullmatch(r"[+-]?\d+(?:\.\d+)?", tok):
                break
            candidate_parts.append(tok)
        candidate = " ".join(candidate_parts)

    candidate = _clean_player_tokens(candidate)
    return candidate or None


def _resolve_player_key(player_name: Optional[str]) -> Optional[str]:
    if not player_name:
        return None
    existing = data_store.lookup_player_key(player_name)
    if existing:
        return existing

    full_name = " ".join(word.capitalize() for word in player_name.split()) or player_name
    return data_store.add_player(full_name=full_name, alias_text=player_name, is_verified=False)


def _build_selection(player_key: Optional[str], stat_key: Optional[str], direction: Optional[str]) -> Optional[str]:
    if not (player_key and stat_key and direction):
        return None
    return f"{player_key}::{stat_key}::{direction}"


def _eligibility(market_type: str, event_key: Optional[str], player_key: Optional[str], stat_key: Optional[str], direction: Optional[str], line: Optional[float]) -> tuple[bool, Optional[str]]:
    if market_type != "player_prop":
        return False, "unsupported_market"
    if not event_key:
        return False, "missing_event_key"
    if not player_key:
        return False, "unparsed_player"
    if not stat_key:
        return False, "unparsed_stat"
    if direction is None:
        return False, "unparsed_direction"
    if line is None:
        return False, "unparsed_line"
    return True, None


def normalize_raw_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    market_type = "player_prop" if raw.get("market_family") == "player_prop" else "unknown"

    pick_text = raw.get("raw_pick_text") or ""
    block_text = raw.get("raw_block") or ""

    direction = _extract_direction(pick_text) or _extract_direction(block_text)
    line = _extract_line(pick_text) or _extract_line(block_text)
    odds = _extract_odds(pick_text) or _extract_odds(block_text)
    stat_key = _extract_stat_key(raw)

    player_name = _extract_player_name(pick_text)
    player_key = _resolve_player_key(player_name)

    event_key = build_event_key(raw)
    selection = _build_selection(player_key, stat_key, direction)

    event = {
        "event_key": event_key,
        "away_team": raw.get("away_team"),
        "home_team": raw.get("home_team"),
    }

    market = {
        "market_type": market_type,
        "selection": selection,
        "player_key": player_key,
        "stat_key": stat_key,
        "line": line,
        "odds": odds,
        "side": direction,
    }

    provenance = {
        "source_id": raw.get("source_id"),
        "source_surface": raw.get("source_surface"),
        "canonical_url": raw.get("canonical_url"),
        "raw_fingerprint": raw.get("raw_fingerprint"),
        "expert_name": raw.get("expert_name"),
        "expert_handle": raw.get("expert_handle"),
        "expert_profile": raw.get("expert_profile"),
        "expert_slug": raw.get("expert_slug"),
    }

    eligible, reason = _eligibility(market_type, event_key, player_key, stat_key, direction, line)

    return {
        "event": event,
        "market": market,
        "provenance": provenance,
        "eligible_for_consensus": eligible,
        "ineligibility_reason": reason,
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


__all__ = ["normalize_raw_record", "normalize_file", "build_event_key"]
