from __future__ import annotations

import json
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from store import get_data_store, write_json
from src.normalizer_bettingpros_nba import _build_event_keys, _map_team, _parse_dt

ALLOWED_MARKETS = {"player_prop"}


def normalize_bettingpros_props_record(
    raw: Dict[str, Any],
    sport: str = "NBA",
    min_proj_diff: float = 1.5,
) -> Dict[str, Any]:
    store = get_data_store(sport)

    observed_dt = _parse_dt(raw.get("observed_at_utc"))
    event_dt = _parse_dt(raw.get("event_start_time_utc")) or observed_dt

    away_team = _map_team(raw.get("away_team"), store=store, sport=sport)
    home_team = _map_team(raw.get("home_team"), store=store, sport=sport)
    event_key, day_key, matchup_key = _build_event_keys(observed_dt, event_dt, away_team, home_team, sport=sport)

    market_type = raw.get("market_type")
    selection = raw.get("selection")
    side = raw.get("side")
    line = raw.get("line")
    odds = raw.get("odds")
    player_name = raw.get("player_name")
    stat_key = raw.get("stat_key")
    proj_diff = raw.get("projection_diff")

    eligible = True
    inelig_reason = None

    if market_type not in ALLOWED_MARKETS:
        eligible = False
        inelig_reason = "unsupported_market"
    elif not player_name:
        eligible = False
        inelig_reason = "unparsed_player"
    elif not stat_key:
        eligible = False
        inelig_reason = "unparsed_stat"
    elif side not in {"OVER", "UNDER"}:
        eligible = False
        inelig_reason = "unparsed_direction"
    elif line is None:
        eligible = False
        inelig_reason = "unparsed_line"
    elif away_team is None or home_team is None or event_key is None:
        eligible = False
        inelig_reason = "unparsed_event"
    else:
        try:
            proj_diff_val = abs(float(proj_diff))
        except Exception:
            proj_diff_val = None
        if proj_diff_val is None:
            eligible = False
            inelig_reason = "missing_proj_diff"
        elif proj_diff_val < float(min_proj_diff):
            eligible = False
            inelig_reason = "below_proj_diff_threshold"
        else:
            player_slug = re.sub(r"[^a-z0-9]+", "_", player_name.lower()).strip("_")
            player_key = f"{sport}:{player_slug}"
            selection = f"{player_key}::{stat_key}::{side}"

    provenance = {
        "source_id": "bettingpros_props",
        "source_surface": raw.get("source_surface") or "bettingpros_prop_bets",
        "sport": sport,
        "observed_at_utc": raw.get("observed_at_utc"),
        "canonical_url": raw.get("canonical_url"),
        "raw_fingerprint": raw.get("raw_fingerprint"),
        "raw_pick_text": raw.get("raw_pick_text"),
        "raw_block": raw.get("raw_block"),
        "matchup_hint": raw.get("matchup_hint"),
        "bet_rating": raw.get("bet_rating"),
        "projection": raw.get("projection"),
        "projection_diff": raw.get("projection_diff"),
        "expected_value": raw.get("expected_value"),
        "projection_probability": raw.get("projection_probability"),
        "cover_probability": raw.get("cover_probability"),
        "recommended_side": raw.get("recommended_side"),
        "opposition_rank": raw.get("opposition_rank"),
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
    if market_type == "player_prop" and eligible and selection and selection.count("::") >= 2:
        pp_player_key = selection.split("::")[0]
        pp_stat_key = selection.split("::")[1]

    market = {
        "market_type": market_type,
        "market_family": "player_prop",
        "selection": selection,
        "side": side,
        "line": line,
        "odds": odds,
        "player_key": pp_player_key,
        "stat_key": pp_stat_key,
    }

    return {
        "provenance": provenance,
        "event": event,
        "market": market,
        "eligible_for_consensus": eligible,
        "ineligibility_reason": inelig_reason,
        "eligibility": {
            "eligible_for_consensus": eligible,
            "ineligibility_reason": inelig_reason,
        },
    }


def normalize_bettingpros_props_records(
    raw_records: List[Dict[str, Any]],
    debug: bool = False,
    sport: str = "NBA",
    min_proj_diff: float = 1.5,
) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    inelig_reasons: Dict[str, int] = {}

    for raw in raw_records:
        if not isinstance(raw, dict):
            continue
        rec = normalize_bettingpros_props_record(raw, sport=sport, min_proj_diff=min_proj_diff)
        if rec.get("eligible_for_consensus"):
            normalized.append(rec)
        else:
            reason = rec.get("ineligibility_reason") or "unknown"
            inelig_reasons[reason] = inelig_reasons.get(reason, 0) + 1

    deduped: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
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
        print(
            f"[DEBUG] BettingPros Props normalized: total={total} eligible={elig} "
            f"ineligible={inelig_total} removed_dupes={removed}"
        )
        if inelig_reasons:
            print(f"[DEBUG] BettingPros Props ineligibility: {inelig_reasons}")

    return normalized


def normalize_file(
    raw_path: str,
    out_path: str,
    debug: bool = False,
    sport: str = "NBA",
    min_proj_diff: float = 1.5,
) -> List[Dict[str, Any]]:
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

    normalized = normalize_bettingpros_props_records(
        raw_records,
        debug=debug,
        sport=sport,
        min_proj_diff=min_proj_diff,
    )
    write_json(out_path, normalized)
    return normalized


__all__ = [
    "normalize_bettingpros_props_record",
    "normalize_bettingpros_props_records",
    "normalize_file",
]
