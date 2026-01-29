from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from store import data_store, write_json
from action_ingest import parse_standard_market
from utils import normalize_text
from src.player_aliases_nba import normalize_player_key


def build_matchup_key(day_key: Optional[str], team_a: Optional[str], team_b: Optional[str]) -> Optional[str]:
    """Order-invariant matchup key using canonical team codes/names."""
    if not (day_key and team_a and team_b):
        return None
    t1, t2 = sorted([str(team_a).upper(), str(team_b).upper()])
    return f"{day_key}:{t1}-{t2}"


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


def _normalize_team(token: Optional[str]) -> Optional[str]:
    if not token:
        return None
    normalized = token.strip()
    # Quick aliases for common BetQL/Action naming
    alias_map = {
        "suns": "PHX",
        "suns.": "PHX",
        "sixers": "PHI",
        "76ers": "PHI",
        "blazers": "POR",
        "trail blazers": "POR",
        "trail-blazers": "POR",
        "pels": "NOP",
        "pelicans": "NOP",
        "knicks": "NYK",
        "nets": "BKN",
        "suns": "PHX",
        "warriors": "GSW",
        "spurs": "SAS",
        "timberwolves": "MIN",
        "t-wolves": "MIN",
    }
    if normalized.lower() in alias_map:
        return alias_map[normalized.lower()]
    matches = data_store.lookup_team_code(normalized)
    if len(matches) == 1:
        return next(iter(matches))
    if len(normalized) == 3 and normalized.upper() in data_store.teams:
        return normalized.upper()
    return None


def _parse_teams_from_canonical(url: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    if not url:
        return None, None
    m = re.search(r"/nba-game/([^/]+)/", url)
    if not m:
        return None, None
    slug = m.group(1)
    slug = slug.split("-score")[0]
    tokens = [t for t in slug.split("-") if t]
    if len(tokens) < 2:
        return None, None
    for i in range(1, len(tokens)):
        away_raw = " ".join(tokens[:i])
        home_raw = " ".join(tokens[i:])
        away_code = _normalize_team(away_raw)
        home_code = _normalize_team(home_raw)
        if away_code and home_code:
            return away_code, home_code
    return None, None


def _build_day_and_event(dt: Optional[datetime], away: Optional[str], home: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    if not dt:
        return None, None
    day_key = f"NBA:{dt.year:04d}:{dt.month:02d}:{dt.day:02d}"
    if away and home:
        return day_key, f"{day_key}:{away}@{home}"
    return day_key, day_key


def build_event_key(raw: Dict[str, Any]) -> Optional[str]:
    """Backwards-compatible helper: returns the event_key string."""
    dt = _parse_dt(raw.get("event_start_time_utc")) or _parse_dt(raw.get("observed_at_utc"))
    away = raw.get("away_team")
    home = raw.get("home_team")
    _, ev = _build_day_and_event(dt, away, home)
    return ev


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


def _eligibility(
    market_type: str,
    event_key: Optional[str],
    away_team: Optional[str],
    home_team: Optional[str],
    player_key: Optional[str],
    stat_key: Optional[str],
    direction: Optional[str],
    line: Optional[float],
    selection: Optional[str],
) -> tuple[bool, Optional[str]]:
    if not event_key:
        return False, "missing_event_key"

    if market_type == "player_prop":
        if not player_key:
            return False, "unparsed_player"
        if not stat_key:
            return False, "unparsed_stat"
        if direction is None:
            return False, "unparsed_direction"
        if line is None:
            return False, "unparsed_line"
        return True, None

    if market_type not in {"spread", "moneyline", "total"}:
        return False, "unsupported_market"
    if not (away_team and home_team):
        return False, "unparsed_team"
    if market_type in {"spread", "moneyline"}:
        if not selection:
            return False, "unparsed_team"
        if market_type == "spread" and line is None:
            return False, "unparsed_line"
    if market_type == "total":
        if direction is None:
            return False, "unparsed_direction"
        if line is None:
            return False, "unparsed_line"
    return True, None


def normalize_raw_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    pick_text = raw.get("raw_pick_text") or ""
    block_text = raw.get("raw_block") or ""

    # Teams
    away_raw = raw.get("away_team")
    home_raw = raw.get("home_team")
    if not (away_raw and home_raw):
        away_raw, home_raw = _parse_teams_from_canonical(raw.get("canonical_url"))
    away_team = _normalize_team(away_raw) if away_raw else None
    home_team = _normalize_team(home_raw) if home_raw else None

    dt = _parse_dt(raw.get("event_start_time_utc")) or _parse_dt(raw.get("observed_at_utc"))
    day_key, event_key = _build_day_and_event(dt, away_team, home_team)
    matchup_key = build_matchup_key(day_key, away_team, home_team)

    is_prop_pick = is_covers_player_prop(pick_text, block_text) if raw.get("source_id") == "covers" else False
    market_type = None
    direction = None
    line = None
    odds = None
    selection = None
    player_key = None
    stat_key = None

    # Covers-first strict routing (mutually exclusive)
    if raw.get("source_id") == "covers":
        sp_ok, sp_team, sp_line = is_covers_spread(pick_text)
        gt_ok, gt_line, gt_dir = is_covers_game_total(pick_text)
        if sp_ok:
            market_type = "spread"
            direction = sp_team
            selection = _normalize_team(sp_team)
            line = sp_line
        elif gt_ok:
            market_type = "total"
            direction = gt_dir
            selection = "GAME_TOTAL"
            line = gt_line
        elif is_prop_pick:
            market_type = "player_prop"

    if market_type is None and (raw.get("market_family") == "player_prop"):
        market_type = "player_prop"

    # Last-resort guard for Covers mislabels
    if market_type is None and raw.get("source_id") == "covers" and is_covers_player_prop(pick_text, block_text):
        market_type = "player_prop"

    manual_ineligible_reason = None

    if market_type == "player_prop":
        direction = _extract_direction(pick_text) or _extract_direction(block_text)
        line = _extract_line(pick_text) or _extract_line(block_text)
        odds = _extract_odds(pick_text) or _extract_odds(block_text)
        stat_key = _extract_stat_key(raw)
        player_name = _extract_player_name(pick_text)
        player_key = _resolve_player_key(player_name)
        player_key = normalize_player_key(player_key)
        selection = _build_selection(player_key, stat_key, direction)
    else:
        unrealistic_line = False
        odds = _extract_odds(pick_text) or _extract_odds(block_text)
        odds = int(odds) if isinstance(odds, str) and re.match(r"[+-]\d{2,4}", odds) else odds

        parsed = parse_standard_market(pick_text)
        market_type = market_type or parsed.get("market_type") or "unknown"
        odds = odds or parsed.get("odds")
        if market_type == "spread":
            team_code = _normalize_team(parsed.get("team_code"))
            selection = team_code
            direction = team_code
            line = parsed.get("line") if parsed.get("line") is not None else raw.get("line_hint")
            if line is not None and abs(line) > 40:
                unrealistic_line = True
        elif market_type == "moneyline":
            team_code = _normalize_team(parsed.get("team_code"))
            selection = team_code
            direction = team_code
            line = None
        elif market_type == "total":
            direction = (parsed.get("side") or "").upper() if parsed.get("side") else None
            selection = "GAME_TOTAL"
            line = parsed.get("line") if parsed.get("line") is not None else raw.get("line_hint")
            if line is not None and not (150 <= line <= 300):
                unrealistic_line = True

        # Covers standard sanity checks
        if raw.get("source_id") == "covers":
            if market_type == "spread" and line is not None and abs(line) > 30:
                market_type = "unknown"
            if market_type == "total" and line is not None and not (170 <= line <= 280):
                market_type = "unknown"

    if market_type is None or market_type == "unknown":
        manual_ineligible_reason = "unparsed_market"
        market_type = market_type or "unknown"

    event = {
        "sport": raw.get("sport"),
        "event_key": event_key,
        "day_key": day_key,
        "matchup_key": matchup_key,
        "away_team": away_team,
        "home_team": home_team,
        "event_start_time_utc": raw.get("event_start_time_utc"),
    }

    market = {
        "market_type": market_type,
        "market_family": raw.get("market_family"),
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
        "raw_pick_text": raw.get("raw_pick_text"),
        "raw_block": raw.get("raw_block"),
        "raw_fingerprint": raw.get("raw_fingerprint"),
        "expert_name": raw.get("expert_name"),
        "expert_handle": raw.get("expert_handle"),
        "expert_profile": raw.get("expert_profile"),
        "expert_slug": raw.get("expert_slug"),
        "matchup_hint": raw.get("matchup_hint"),
    }

    eligible, reason = _eligibility(
        market_type,
        event_key,
        away_team,
        home_team,
        player_key,
        stat_key,
        direction,
        line,
        selection,
    )
    if manual_ineligible_reason:
        eligible, reason = False, manual_ineligible_reason
    if not reason and 'unrealistic_line' in locals() and unrealistic_line:
        eligible, reason = False, "unrealistic_line"

    return {
        "event": event,
        "market": market,
        "provenance": provenance,
        "eligible_for_consensus": eligible,
        "ineligibility_reason": reason,
    }


def _parse_matchup_id(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    m = re.search("/nba/matchup/(\\d+)/picks", url)
    return m.group(1) if m else None


def _infer_teams_from_text(text: str) -> list[str]:
    tokens = re.findall(r"[A-Za-z][A-Za-z\-']+", text or "")
    found: list[str] = []
    n = len(tokens)
    for i in range(n):
        spans = [tokens[i]]
        if i + 1 < n:
            spans.append(f"{tokens[i]} {tokens[i+1]}")
        for span in spans:
            codes = data_store.lookup_team_code(span)
            if len(codes) == 1:
                code = next(iter(codes))
                if code not in found:
                    found.append(code)
            if len(found) >= 2:
                return found[:2]
    return found[:2]


def is_covers_game_total(raw_pick_text: str) -> tuple[bool, Optional[float], Optional[str]]:
    text_raw = raw_pick_text or ""
    text = re.sub(r"\s+", " ", text_raw).strip().lower()

    # Must begin with over/under followed by a numeric total
    m = re.match(r"^(over|under)\s+([0-9]+(?:\.5)?)\s*(?:\(?[+-]\d{2,4}\)?\s*)?$", text)
    if not m:
        return False, None, None

    direction = m.group(1).upper()
    try:
        line = float(m.group(2))
    except ValueError:
        return False, None, None

    if not (150 <= line <= 300):
        return False, None, None

    # Exclude player-ish text or stat props
    stat_keywords = [
        "points",
        "rebounds",
        "assists",
        "steals",
        "blocks",
        "pra",
        "points+rebounds+assists",
        "pts+reb+ast",
        "3-pointers",
        "3 pointers",
    ]
    if any(k in text for k in stat_keywords):
        return False, None, None

    if re.search(r"\b[A-Z][a-z]+\s+[A-Z][a-z]+\b", text_raw):
        return False, None, None

    return True, line, direction


def is_covers_spread(raw_pick_text: str) -> tuple[bool, Optional[str], Optional[float]]:
    text = re.sub(r"\s+", " ", (raw_pick_text or "")).strip()
    m = re.match(r"(?i)^([A-Za-z]{2,4})\s*([+-]\d+(?:\.5)?)\s*(?:\(?[+-]\d{2,4}\)?\s*)?$", text)
    if not m:
        return False, None, None

    team = m.group(1).upper()
    try:
        line = float(m.group(2))
    except ValueError:
        return False, None, None

    if abs(line) > 40:
        return False, None, None
    return True, team, line


def is_covers_player_prop(raw_pick_text: str, raw_block: str) -> bool:
    text_raw = f"{raw_pick_text or ''} {raw_block or ''}"
    text_norm = normalize_text(text_raw)

    # Exclude totals/spreads that match strict detectors
    if is_covers_spread(raw_pick_text)[0] or is_covers_game_total(raw_pick_text)[0]:
        return False

    stat_keywords = [
        "points scored",
        "points",
        "rebounds",
        "assists",
        "steals",
        "blocks",
        "pra",
        "pts+reb+ast",
        "points+rebounds+assists",
        "3-pointers",
        "3 pointers",
        "3-pointers made",
        "total rebounds",
        "total assists",
        "total points",
    ]
    label_keywords = ["projection", "points scored", "total rebounds", "total assists", "3-pointers made"]

    has_stat = any(k in text_norm for k in stat_keywords)
    has_label = any(k in text_norm for k in label_keywords)

    return has_stat or has_label


def normalize_file(raw_path: str, out_path: str, debug: bool = False) -> List[Dict[str, Any]]:
    if not os.path.exists(raw_path):
        write_json(out_path, [])
        return []

    try:
        with open(raw_path, "r", encoding="utf-8") as f:
            raw_records = json.load(f)
    except json.JSONDecodeError:
        raw_records = []

    # Build Covers matchup_id -> (away, home) map from player props (if teams present)
    covers_matchups: Dict[str, tuple[Optional[str], Optional[str]]] = {}
    covers_seen_order: Dict[str, list[str]] = {}
    if isinstance(raw_records, list):
        for item in raw_records:
            if not isinstance(item, dict):
                continue
            if item.get("source_id") != "covers":
                continue
            mid = _parse_matchup_id(item.get("canonical_url"))
            if not mid:
                continue
            if (item.get("market_family") or "").lower() == "player_prop" or is_covers_player_prop(item.get("raw_pick_text") or "", item.get("raw_block") or ""):
                a = _normalize_team(item.get("away_team"))
                h = _normalize_team(item.get("home_team"))
                if not (a and h):
                    inferred = _infer_teams_from_text(item.get("raw_block") or item.get("raw_pick_text") or "")
                    if len(inferred) >= 2:
                        a = a or inferred[0]
                        h = h or inferred[1]
                if mid not in covers_matchups or (a and h and (covers_matchups[mid][0] is None or covers_matchups[mid][1] is None)):
                    covers_matchups[mid] = (a, h)
            # Track team codes appearing in standard picks to use as fallback
            parsed = parse_standard_market(item.get("raw_pick_text") or "") if (item.get("market_family") or "").lower() == "standard" else None
            team_code = None
            if parsed:
                team_code = _normalize_team(parsed.get("team_code"))
            if team_code:
                covers_seen_order.setdefault(mid, [])
                if team_code not in covers_seen_order[mid]:
                    covers_seen_order[mid].append(team_code)
            else:
                inferred = _infer_teams_from_text(item.get("raw_block") or item.get("raw_pick_text") or "")
                if inferred:
                    covers_seen_order.setdefault(mid, [])
                    for code in inferred:
                        if code not in covers_seen_order[mid]:
                            covers_seen_order[mid].append(code)
    # Fill missing matchup map using team codes seen across picks
    for mid, teams in covers_seen_order.items():
        if mid in covers_matchups and covers_matchups[mid][0] and covers_matchups[mid][1]:
            continue
        if len(teams) >= 2:
            covers_matchups[mid] = (teams[0], teams[1])
    if debug and covers_matchups:
        print(f"[DEBUG covers] matchup_ids from props: {len(covers_matchups)}")

    totals_ct = spreads_ct = props_ct = enriched_std_ct = skipped_props_ct = 0
    normalized: List[Dict[str, Any]] = []
    if isinstance(raw_records, list):
        for item in raw_records:
            if isinstance(item, dict):
                enriched_now = False
                is_prop = item.get("source_id") == "covers" and is_covers_player_prop(item.get("raw_pick_text") or "", item.get("raw_block") or "")
                if item.get("source_id") == "covers" and not is_prop:
                    mid = _parse_matchup_id(item.get("canonical_url"))
                    if mid and mid in covers_matchups:
                        away_enr, home_enr = covers_matchups[mid]
                        if item.get("away_team") is None and away_enr:
                            item["away_team"] = away_enr
                            enriched_now = True
                        if item.get("home_team") is None and home_enr:
                            item["home_team"] = home_enr
                            enriched_now = True
                elif item.get("source_id") == "covers" and is_prop:
                    skipped_props_ct += 1
                    if debug:
                        mid = _parse_matchup_id(item.get("canonical_url"))
                        print(f"[DEBUG covers] skip enrich (player_prop): mid={mid} text={item.get('raw_pick_text')}")

                normalized_record = normalize_raw_record(item)
                normalized.append(normalized_record)

                if item.get("source_id") == "covers":
                    mt = normalized_record.get("market", {}).get("market_type")
                    if mt == "total":
                        totals_ct += 1
                    elif mt == "spread":
                        spreads_ct += 1
                    elif mt == "player_prop":
                        props_ct += 1
                    if mt in {"spread", "total", "moneyline"} and enriched_now:
                        enriched_std_ct += 1
                        if debug:
                            mid = _parse_matchup_id(item.get("canonical_url"))
                            line = normalized_record.get("market", {}).get("line")
                            print(f"[DEBUG covers] enrich ({mt}): mid={mid} away={item.get('away_team')} home={item.get('home_team')} line={line}")
    if debug:
        print(f"[DEBUG covers classify] totals={totals_ct} spreads={spreads_ct} props={props_ct} enriched_standard={enriched_std_ct} skipped_props={skipped_props_ct}")

    write_json(out_path, normalized)
    return normalized


__all__ = ["normalize_raw_record", "normalize_file", "build_event_key"]
