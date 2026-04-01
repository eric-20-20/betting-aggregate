"""Grade signals using external provider data with caching and idempotent writes.

Supports both NBA and NCAAB sports via --sport parameter.

Env:
  RESULTS_PROVIDER=nba_api|nba_cdn|api_sports|auto
  API_SPORTS_KEY=<key>
  API_SPORTS_BASE=https://v1.basketball.api-sports.io (scheme auto-added if missing)

Flags:
  --sport NBA|NCAAB : select sport to grade (default NBA)
  --refresh-cache : ignore cached provider files and refetch
  --provider auto|nba_api|api_sports : override provider selection (default auto -> nba_api priority)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import copy
from collections import Counter, defaultdict
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Set

# ensure repo root on path for src package imports
sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.results import nba_provider  # type: ignore
from src.results import nba_provider_api_sports  # type: ignore
from src.signal_keys import (
    build_selection_key,
    build_offer_key,
    player_match_confidence as compute_player_match_confidence,
    is_roi_eligible,
)

# Sport constants
NBA_SPORT = "NBA"
NCAAB_SPORT = "NCAAB"


def get_signals_latest_path(sport: str) -> Path:
    if sport == NCAAB_SPORT:
        return Path("data/ledger/ncaab/signals_latest.jsonl")
    return Path("data/ledger/signals_latest.jsonl")


def get_grades_occ_path(sport: str) -> Path:
    if sport == NCAAB_SPORT:
        return Path("data/ledger/ncaab/grades_occurrences.jsonl")
    return Path("data/ledger/grades_occurrences.jsonl")


def get_grades_latest_path(sport: str) -> Path:
    if sport == NCAAB_SPORT:
        return Path("data/ledger/ncaab/grades_latest.jsonl")
    return Path("data/ledger/grades_latest.jsonl")


# Legacy paths for backward compatibility
SIGNALS_LATEST_PATH = Path("data/ledger/signals_latest.jsonl")
GRADES_OCC_PATH = Path("data/ledger/grades_occurrences.jsonl")
GRADES_LATEST_PATH = Path("data/ledger/grades_latest.jsonl")
TERMINAL_STATUSES = {"WIN", "LOSS", "PUSH", "INELIGIBLE"}


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not path.exists():
        return out
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
            f.write("\n")


def append_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
            f.write("\n")


def odds_to_stake_units(odds: Optional[Any], source_units: Optional[float] = None) -> float:
    """Return stake size in units, using source-provided size or odds-based default.

    Priority: source-provided unit_size > odds-based implied > flat 1.0u default.
    """
    # 1. Use source-provided unit size if valid
    if source_units is not None:
        try:
            su = float(source_units)
            if 0.1 <= su <= 5.0:
                return round(su, 2)
        except (TypeError, ValueError):
            pass

    # 2. No odds → flat 1u default
    if odds is None:
        return 1.0

    try:
        o = float(odds)
    except (TypeError, ValueError):
        return 1.0
    if o == 0:
        return 1.0

    # 3. Derive from implied probability: favorites get ~1u+, longshots get less
    if o > 0:
        prob = 100.0 / (o + 100.0)
    else:
        prob = abs(o) / (abs(o) + 100.0)

    # Scale so -110 (prob=0.524) ≈ 1.05u
    stake = round(prob * 2, 2)
    return max(0.25, min(2.0, stake))


def american_odds_to_units(odds: Optional[Any], outcome: str, stake: float = 1.0) -> Optional[float]:
    """
    Return profit/loss units given American odds, outcome, and stake size.

    WIN:  +win_units * stake  (e.g., -110 WIN at 1.05u → +0.955)
    LOSS: -stake              (e.g., +500 LOSS at 0.33u → -0.33)
    PUSH: 0.0
    """
    if outcome == "PUSH":
        return 0.0
    if odds is None:
        # No odds: WIN profit unknown, LOSS = -stake
        return round(-stake, 3) if outcome == "LOSS" else None
    try:
        o = float(odds)
    except (TypeError, ValueError):
        return round(-stake, 3) if outcome == "LOSS" else None
    if o == 0:
        return round(-stake, 3) if outcome == "LOSS" else None
    win_units = (o / 100.0) if o > 0 else (100.0 / abs(o))
    if outcome == "WIN":
        return round(win_units * stake, 3)
    if outcome == "LOSS":
        return round(-stake, 3)
    return None


COMBO_STAT_KEYS = {"pts_reb", "pts_ast", "reb_ast", "pts_reb_ast"}


def extract_stat_key(signal: Dict[str, Any]) -> Optional[str]:
    # Parse stat key from selection (NBA:player::stat::dir)
    sel = signal.get("selection")
    stat_from_sel = None
    if isinstance(sel, str) and "::" in sel:
        parts = sel.split("::")
        if len(parts) >= 3:
            stat_from_sel = parts[1]
    # For combo stats, always use the selection key — atomic_stat may
    # contain only one component (e.g. "points" instead of "pts_reb")
    if stat_from_sel and stat_from_sel in COMBO_STAT_KEYS:
        return stat_from_sel
    # For single stats, prefer atomic_stat if available
    stat = signal.get("atomic_stat")
    if isinstance(stat, str) and stat:
        return stat
    return stat_from_sel


def parse_event_teams(event_key: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not event_key or "@" not in str(event_key):
        return None, None
    try:
        teams = str(event_key).split(":")[-1]
        away, home = teams.split("@", 1)
        return away.upper(), home.upper()
    except Exception:
        return None, None


def derive_teams(signal: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    away = signal.get("away_team")
    home = signal.get("home_team")
    # away_team/home_team fields take precedence - they contain actual orientation
    # canonical_game_key is sorted alphabetically, NOT by away/home order
    if away and home:
        return str(away).upper(), str(home).upper()

    def parse_pair(val: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        if not isinstance(val, str):
            return None, None
        if "@" in val:
            # Handle NBA event_key format: "NBA:2026:03:03:OKC@CHI" — take only the last segment
            last = val.rsplit(":", 1)[-1] if ":" in val else val
            if "@" in last:
                a, h = last.split("@", 1)
                return a, h
            a, h = val.split("@", 1)
            return a, h
        if "-" in val:
            a, h = val.split("-", 1)
            return a, h
        return None, None

    ev_away, ev_home = parse_pair(signal.get("event_key"))
    away = away or ev_away
    home = home or ev_home
    mk_away, mk_home = parse_pair(signal.get("matchup_key"))
    away = away or mk_away
    home = home or mk_home
    matchup = signal.get("matchup")
    mu_away, mu_home = parse_pair(matchup)
    away = away or mu_away
    home = home or mu_home
    return away.upper() if away else None, home.upper() if home else None


def parse_direction(signal: Dict[str, Any]) -> Optional[str]:
    direction = signal.get("direction")
    if isinstance(direction, str) and direction:
        d = direction.upper()
        # Normalize prefixed directions: PLAYER_OVER -> OVER, PLAYER_UNDER -> UNDER
        if "OVER" in d:
            return "OVER"
        if "UNDER" in d:
            return "UNDER"
        return d
    sel = signal.get("selection")
    if isinstance(sel, str) and "::" in sel:
        parts = sel.split("::")
        if len(parts) >= 3:
            return parts[2].upper()
    return None


def pick_line(signal: Dict[str, Any]) -> Optional[float]:
    for key in ("line_median", "line", "line_max", "line_min"):
        val = signal.get(key)
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                continue
    return None


def pick_odds(signal: Dict[str, Any]) -> Optional[float]:
    for key in ("best_odds",):
        if signal.get(key) is not None:
            try:
                return float(signal.get(key))
            except Exception:
                pass
    odds_list = signal.get("odds_list") or signal.get("odds") or []
    if isinstance(odds_list, list) and odds_list:
        try:
            return float(odds_list[0])
        except Exception:
            return None
    return None


def day_key_to_date_str(day_key: Optional[str]) -> Optional[str]:
    if not isinstance(day_key, str):
        return None
    # Handle both NBA: and NCAAB: prefixes
    if day_key.startswith("NBA:") or day_key.startswith("NCAAB:"):
        parts = day_key.split(":")
        if len(parts) >= 4 and all(p.isdigit() for p in parts[1:4]):
            return f"{parts[1]}-{parts[2]}-{parts[3]}"
    # already yyyy-mm-dd
    if len(day_key) == 10 and day_key[4] == "-" and day_key[7] == "-":
        return day_key
    return None


def event_key_to_date_str(event_key: Optional[str]) -> Optional[str]:
    if not isinstance(event_key, str):
        return None
    # Handle both NBA: and NCAAB: prefixes
    if event_key.startswith("NBA:") or event_key.startswith("NCAAB:"):
        parts = event_key.split(":")
        # Format 1: SPORT:YYYY:MM:DD:... (e.g., "NBA:2026:02:06:MIA@BOS")
        if len(parts) >= 4 and all(p.isdigit() for p in parts[1:4]):
            return f"{parts[1]}-{parts[2]}-{parts[3]}"
        # Format 2: SPORT:YYYYMMDD:... (e.g., "NBA:20260206:MIA@BOS:2130")
        if len(parts) >= 2 and len(parts[1]) == 8 and parts[1].isdigit():
            y, m, d = parts[1][:4], parts[1][4:6], parts[1][6:8]
            return f"{y}-{m}-{d}"
    return None


def derive_date_str(signal: Dict[str, Any]) -> Optional[str]:
    dk = signal.get("day_key")
    if dk:
        ds = day_key_to_date_str(dk)
        if ds:
            return ds
    cgk = signal.get("canonical_game_key")
    if isinstance(cgk, (list, tuple)) and len(cgk) >= 2:
        ds = day_key_to_date_str(cgk[1])
        if ds:
            return ds
    # Try event_key first
    ek = signal.get("event_key")
    ds = event_key_to_date_str(ek)
    if ds:
        return ds
    # Fallback to canonical_event_key
    cek = signal.get("canonical_event_key")
    return event_key_to_date_str(cek)


def check_eligibility(signal: Dict[str, Any], date_str: Optional[str], sport: str = "NBA") -> Tuple[bool, str, List[str]]:
    """Return (eligible?, reason, missing_fields) before hitting providers."""

    # Skip conflict signals - they are intentionally ungradeable by design
    if signal.get("signal_type") == "avoid_conflict":
        return False, "conflict_signal_by_design", []

    if date_str is None:
        return False, "bad_date_format", ["date_str"]

    missing: List[str] = []
    day_key = signal.get("day_key")
    if not day_key:
        missing.append("day_key")
    market = signal.get("market_type")
    if market not in {"player_prop", "spread", "total", "moneyline"}:
        return False, "unsupported_market", ["market_type"] if market is None else []

    cgk = signal.get("canonical_game_key")
    has_cgk = isinstance(cgk, (list, tuple)) and len(cgk) >= 4 and cgk[2] and cgk[3]
    has_explicit_matchup = bool(signal.get("away_team") and signal.get("home_team"))
    away_team, home_team = derive_teams(signal)
    has_derived_matchup = bool(away_team and home_team)
    if market == "player_prop":
        if not (has_cgk or has_explicit_matchup or has_derived_matchup):
            if not has_cgk:
                missing.append("canonical_game_key")
            if not signal.get("away_team"):
                missing.append("away_team")
            if not signal.get("home_team"):
                missing.append("home_team")
            return False, "missing_matchup_fields", missing

    if not away_team:
        missing.append("away_team")
    if not home_team:
        missing.append("home_team")
    if missing:
        return False, "missing_matchup_fields", missing

    selection = signal.get("selection") or ""
    line = pick_line(signal)
    direction = parse_direction(signal)
    stat_key = extract_stat_key(signal)
    player_id = signal.get("player_id")
    # Try to extract player_id from selection if not provided (e.g., "p_pritchard::points::OVER")
    if not player_id and selection and "::" in selection:
        player_id = selection.split("::")[0]
        # Strip NBA: prefix if present
        if player_id.upper().startswith("NBA:"):
            player_id = player_id[4:]
    if market == "moneyline":
        if not selection:
            return False, "missing_selection", ["selection"]
    elif market == "spread":
        if line is None:
            return False, "missing_line", ["line"]
        if not selection:
            return False, "missing_selection", ["selection"]
    elif market == "total":
        if line is None:
            return False, "missing_line", ["line"]
        min_total = 100 if sport == "NCAAB" else 150
        if line < min_total:
            return False, "implausible_total_line", ["line"]
        if not direction:
            return False, "missing_direction", ["direction"]
    elif market == "player_prop":
        if not player_id:
            return False, "missing_player_id", ["player_id"]
        if not stat_key:
            return False, "missing_stat_key", ["atomic_stat", "selection"]
        if not direction:
            return False, "missing_direction", ["direction", "selection"]
        if line is None:
            return False, "missing_line", ["line"]

    return True, "", []


def _try_all_games_player_fallback(
    signal: Dict[str, Any],
    date_str: Optional[str],
    refresh_cache: bool,
    provider: Any,
) -> Optional[Dict[str, Any]]:
    """Search all games on a date (and +/-1 day) for a player's statline.

    Returns the statline dict (with value, game_id, matched_away, matched_home)
    or None if player not found in any game.
    """
    if not date_str:
        return None
    stat_key = extract_stat_key(signal)
    if not stat_key:
        return None
    player_id = signal.get("player_id")
    if not player_id and isinstance(signal.get("selection"), str):
        player_id = signal["selection"].split("::")[0]
    if not player_id:
        return None
    away_team, home_team = derive_teams(signal)
    if not (
        isinstance(signal.get("event_key"), str)
        and "@" in signal.get("event_key", "")
        and away_team
        and home_team
    ):
        return None

    # Try primary date first, then +/-1 day
    dates_to_try = [date_str]
    try:
        base = datetime.strptime(date_str, "%Y-%m-%d")
        dates_to_try.append((base - timedelta(days=1)).strftime("%Y-%m-%d"))
        dates_to_try.append((base + timedelta(days=1)).strftime("%Y-%m-%d"))
    except Exception:
        pass

    for ds in dates_to_try:
        try:
            result = nba_provider.search_all_games_for_player_statline(
                player_id, stat_key, ds, refresh_cache=refresh_cache,
            )
        except Exception:
            continue
        if result is not None:
            matched_away = str(result.get("matched_away") or "").upper()
            matched_home = str(result.get("matched_home") or "").upper()
            if {away_team, home_team} != {matched_away, matched_home}:
                continue
            return result
    return None


def grade_signal(
    signal: Dict[str, Any],
    graded_at: str,
    date_str: Optional[str],
    refresh_cache: bool,
    provider_name: str,
    debug: bool = False,
    debug_samples: Optional[List[Dict[str, Any]]] = None,
    counters: Optional[Counter] = None,
    sport: str = NBA_SPORT,
    results_provider: Any = None,
) -> Dict[str, Any]:
    sid = signal.get("signal_id")
    day_key = signal.get("day_key")
    away, home = derive_teams(signal)
    urls = collect_urls(signal)
    market = signal.get("market_type")
    notes: List[str] = []
    raw_event_key = signal.get("event_key")
    safe_event_key = (
        f"{sport}:{date_str.replace('-', ':')}:{away}@{home}"
        if date_str and away and home
        else None
    )

    # Use passed provider or fall back to nba_provider
    provider = results_provider or nba_provider

    def attach_event_keys(row: Dict[str, Any]) -> Dict[str, Any]:
        row.setdefault("event_key_raw", raw_event_key)
        if safe_event_key:
            row.setdefault("event_key_safe", safe_event_key)
            row.setdefault("canonical_event_key", safe_event_key)
        row.setdefault("grade_notes", row.get("notes") or "")
        return row

    if not date_str:
        return attach_event_keys(
            {
                "signal_id": sid,
                "graded_at_utc": graded_at,
                "status": "ERROR",
                "result": None,
                "units": None,
                "market_type": market,
            "selection": signal.get("selection"),
            "event_key": signal.get("event_key"),
            "day_key": day_key,
            "urls": urls,
            "notes": "bad_date_format",
            "missing_fields": ["date_str"],
            "signal_debug": {
                "event_key": signal.get("event_key"),
                "canonical_game_key": signal.get("canonical_game_key"),
                "away_team": signal.get("away_team"),
                "home_team": signal.get("home_team"),
                "matchup_key": signal.get("matchup_key"),
                "matchup": signal.get("matchup"),
            },
            }
        )

    if not day_key or not away or not home:
        missing = []
        if not day_key:
            missing.append("day_key")
        if not away:
            missing.append("away_team")
        if not home:
            missing.append("home_team")
        return attach_event_keys(
            {
                "signal_id": sid,
                "graded_at_utc": graded_at,
                "status": "ERROR",
                "result": None,
                "units": None,
                "market_type": market,
            "selection": signal.get("selection"),
            "event_key": signal.get("event_key"),
            "day_key": day_key,
            "urls": urls,
            "notes": "signal_missing_matchup" if away is None or home is None else "signal_missing_day_key",
            "missing_fields": missing,
            "signal_debug": {
                "event_key": signal.get("event_key"),
                "canonical_game_key": signal.get("canonical_game_key"),
                "away_team": signal.get("away_team"),
                "home_team": signal.get("home_team"),
                "matchup_key": signal.get("matchup_key"),
                "matchup": signal.get("matchup"),
            },
            }
        )

    event_key = signal.get("event_key")
    event_key_format = None
    if isinstance(event_key, str) and "-" in event_key and "@" not in event_key:
        event_key_format = "hyphen"

    try:
        game_result = provider.fetch_game_result(
            safe_event_key or raw_event_key,
            away,
            home,
            date_str,
            refresh_cache=refresh_cache,
        )
    except RuntimeError:
        return attach_event_keys(
            {
                "signal_id": sid,
                "graded_at_utc": graded_at,
                "status": "ERROR",
                "result": None,
                "units": None,
            "market_type": market,
            "selection": signal.get("selection"),
            "event_key": signal.get("event_key"),
                "day_key": day_key,
                "urls": urls,
                "notes": "env_missing",
            }
        )

    games_info = None
    if isinstance(game_result, dict) and game_result.get("games_info"):
        try:
            games_info = copy.deepcopy(game_result.get("games_info"))
        except Exception:
            games_info = json.loads(json.dumps(game_result.get("games_info")))

    if debug and debug_samples is not None and len(debug_samples) < 5:
        debug_samples.append(
            {
                "signal_id": sid,
                "provider": provider_name,
                "date": date_str,
                "games_info": games_info,
                "status": (game_result or {}).get("status") if isinstance(game_result, dict) else None,
                "game_id": (game_result or {}).get("game_id") if isinstance(game_result, dict) else None,
                "market_type": market,
                "selection": signal.get("selection"),
                "event_key_safe": safe_event_key,
                "event_key_raw": raw_event_key,
            }
        )

    if game_result is None:
        note = "provider_returned_none"
        return attach_event_keys(
            {
                "signal_id": sid,
                "graded_at_utc": graded_at,
                "status": "ERROR",
            "result": None,
            "units": None,
            "market_type": market,
            "selection": signal.get("selection"),
            "event_key": signal.get("event_key"),
            "day_key": day_key,
                "urls": urls,
                "notes": note,
                "games_info": games_info,
                "debug_event_key_format": event_key_format or "",
            }
        )
    if isinstance(game_result, dict) and game_result.get("status") == "NO_GAMES":
        return attach_event_keys(
            {
                "signal_id": sid,
                "graded_at_utc": graded_at,
                "status": "ERROR",
            "result": None,
            "units": None,
            "market_type": market,
            "selection": signal.get("selection"),
            "event_key": signal.get("event_key"),
            "day_key": day_key,
                "urls": urls,
                "notes": "no_games_for_date",
                "games_info": games_info,
                "debug_event_key_format": event_key_format or "",
            }
        )

    status_raw = game_result.get("status")
    if debug and games_info and date_str and games_info.get("date") and games_info.get("date") != date_str:
        notes.append(f"meta_date_mismatch:{games_info.get('date')}!= {date_str}")
    if status_raw == "ERROR":
        error_notes = game_result.get("notes") or ";".join(notes) or "game_not_found"
        # For player props with wrong matchup, try searching all games for the player
        if market == "player_prop" and "game_not_found" in str(error_notes):
            fallback_stat = _try_all_games_player_fallback(
                signal, date_str, refresh_cache, provider
            )
            if fallback_stat is not None:
                # Successfully found player in another game — grade the prop
                line = pick_line(signal)
                direction = parse_direction(signal)
                stat_key = extract_stat_key(signal)
                val = fallback_stat.get("value")
                if val is None and stat_key:
                    val = fallback_stat.get(stat_key)
                if val is not None and line is not None and direction:
                    try:
                        val_num = float(val)
                    except (TypeError, ValueError):
                        val_num = None
                    if val_num is not None:
                        if direction == "OVER":
                            result = "WIN" if val_num > line else "PUSH" if val_num == line else "LOSS"
                        else:
                            result = "WIN" if val_num < line else "PUSH" if val_num == line else "LOSS"
                        if counters is not None:
                            counters["prop_fallback_all_games_graded"] += 1
                        return attach_event_keys(
                            {
                                "signal_id": sid,
                                "graded_at_utc": graded_at,
                                "status": "GRADED",
                                "result": result,
                                "units": None,
                                "market_type": market,
                                "selection": signal.get("selection"),
                                "event_key": signal.get("event_key"),
                                "day_key": day_key,
                                "urls": urls,
                                "notes": "graded_via_all_games_search",
                                "games_info": games_info,
                                "actual_stat_value": val_num,
                                "fallback_game_id": fallback_stat.get("game_id"),
                                "fallback_matched_away": fallback_stat.get("matched_away"),
                                "fallback_matched_home": fallback_stat.get("matched_home"),
                            }
                        )
        return attach_event_keys(
            {
                "signal_id": sid,
                "graded_at_utc": graded_at,
                "status": "ERROR",
                "result": None,
                "units": None,
                "market_type": market,
                "selection": signal.get("selection"),
                "event_key": signal.get("event_key"),
                "day_key": day_key,
                "urls": urls,
                "notes": error_notes,
                "games_info": games_info,
            }
        )
    home_score = game_result.get("home_score")
    away_score = game_result.get("away_score")
    game_id = game_result.get("game_id")

    tried_heal = False
    while status_raw and status_raw not in {"FINAL", "STATUS_FINAL"}:
        reason = pending_reason(status_raw, date_str, games_info)
        if (
            not debug
            and isinstance(games_info, dict)
            and games_info.get("date_used")
            and date_str
            and str(games_info.get("date_used")) != date_str
        ):
            reason = f"{reason};date_used_mismatch"
        upgrade = should_upgrade_pending_to_error(reason, date_str, games_info)
        heal_meta: Optional[Dict[str, Any]] = None
        if upgrade and upgrade.startswith("stale_scheduled"):
            heal_key = f"{date_str}|{away}|{home}"
            if heal_key in _HEALED_GAMES:
                heal_meta = {"healed": False, "reason": "already_attempted"}
                if counters is not None:
                    counters["healed_stale_scheduled_skipped_cached"] += 1
            elif not tried_heal:
                _HEALED_GAMES.add(heal_key)
                healed_res, heal_meta = heal_stale_scheduled(
                    safe_event_key or raw_event_key, away, home, date_str or "", games_info, refresh_cache, provider=provider
                )
                tried_heal = True
                if heal_meta.get("healed"):
                    if counters is not None:
                        counters["healed_stale_scheduled_success"] += 1
                    game_result = healed_res or game_result
                    status_raw = game_result.get("status")
                    try:
                        games_info = copy.deepcopy(game_result.get("games_info"))
                    except Exception:
                        games_info = json.loads(json.dumps(game_result.get("games_info")))
                    home_score = game_result.get("home_score")
                    away_score = game_result.get("away_score")
                    game_id = game_result.get("game_id")
                    continue
                else:
                    if counters is not None:
                        counters["healed_stale_scheduled_no_change"] += 1
            if heal_meta and isinstance(games_info, dict) and (debug or upgrade):
                try:
                    gi = copy.deepcopy(games_info)
                except Exception:
                    gi = json.loads(json.dumps(games_info))
                gi["heal_meta"] = heal_meta
                games_info = gi
        if upgrade:
            return attach_event_keys(
                {
                    "signal_id": sid,
                    "graded_at_utc": graded_at,
                    "status": "ERROR",
                    "result": None,
                    "units": None,
                    "market_type": market,
                    "selection": signal.get("selection"),
                    "event_key": signal.get("event_key"),
                    "day_key": day_key,
                    "urls": urls,
                    "notes": upgrade,
                    "games_info": games_info,
                }
            )
        return attach_event_keys(
            {
                "signal_id": sid,
                "graded_at_utc": graded_at,
                "status": "PENDING",
            "result": None,
            "units": None,
            "market_type": market,
            "selection": signal.get("selection"),
            "event_key": signal.get("event_key"),
                "day_key": day_key,
                "urls": urls,
                "notes": reason,
                "games_info": games_info,
            }
        )

    if home_score is None or away_score is None:
        return attach_event_keys(
            {
                "signal_id": sid,
                "graded_at_utc": graded_at,
                "status": "ERROR",
            "result": None,
            "units": None,
            "market_type": market,
            "selection": signal.get("selection"),
            "event_key": signal.get("event_key"),
                "day_key": day_key,
                "urls": urls,
                "notes": "score_missing",
                "games_info": games_info,
            }
        )

    line = pick_line(signal)
    # For spreads: prefer market_line (Odds API, has correct sign) over consensus line
    # which may be unsigned/wrong-signed from scrapers like BetQL model spread
    if market == "spread":
        mkt_line = signal.get("market_line")
        if mkt_line is not None:
            try:
                line = float(mkt_line)
            except (TypeError, ValueError):
                pass
    odds = pick_odds(signal)
    direction = parse_direction(signal)

    if market in {"spread", "total", "moneyline"}:
        if market == "moneyline":
            selected = (signal.get("selection") or "").upper()
            # Strip common suffixes like _ML from selection (e.g., "GSW_ML" -> "GSW")
            if selected.endswith("_ML"):
                selected = selected[:-3]
            if not selected:
                notes.append("stat_missing")
                result = None
            else:
                winner = away if away_score > home_score else home if home_score > away_score else None
                if winner is None:
                    result = "PUSH"
                else:
                    result = "WIN" if selected == winner else "LOSS"
        elif market == "spread":
            if line is None or not signal.get("selection"):
                return attach_event_keys(
                    {
                        "signal_id": sid,
                        "graded_at_utc": graded_at,
                        "status": "ERROR",
                    "result": None,
                    "units": None,
                    "market_type": market,
                    "selection": signal.get("selection"),
                    "event_key": signal.get("event_key"),
                        "day_key": day_key,
                        "urls": urls,
                        "notes": "missing_line",
                    }
                )
            selected = signal.get("selection").upper()
            # Strip _SPREAD suffix if present (e.g., "MIA_SPREAD" -> "MIA")
            if selected.endswith("_SPREAD"):
                selected = selected[:-7]
            if selected == away:
                margin = away_score - home_score
            elif selected == home:
                margin = home_score - away_score
            else:
                margin = None
                notes.append("stat_missing")
            if margin is None:
                result = None
            else:
                adj = margin + line
                result = "WIN" if adj > 0 else "PUSH" if adj == 0 else "LOSS"
        else:  # total
            if line is None or not direction:
                return {
                    "signal_id": sid,
                    "graded_at_utc": graded_at,
                    "status": "ERROR",
                    "result": None,
                    "units": None,
                    "market_type": market,
                    "selection": signal.get("selection"),
                    "event_key": signal.get("event_key"),
                    "day_key": day_key,
                    "urls": urls,
                    "notes": "missing_line",
                }
            total_pts = home_score + away_score
            if direction == "OVER":
                result = "WIN" if total_pts > line else "PUSH" if total_pts == line else "LOSS"
            else:
                result = "WIN" if total_pts < line else "PUSH" if total_pts == line else "LOSS"

    elif market == "player_prop":
        stat_key = extract_stat_key(signal)
        if not stat_key or line is None or not direction:
            return attach_event_keys(
                {
                    "signal_id": sid,
                    "graded_at_utc": graded_at,
                    "status": "ERROR",
                "result": None,
                "units": None,
                "market_type": market,
                "selection": signal.get("selection"),
                "event_key": signal.get("event_key"),
                    "day_key": day_key,
                    "urls": urls,
                    "notes": "stat_missing",
                }
            )
        player_id = signal.get("player_id")
        if not player_id and isinstance(signal.get("selection"), str):
            player_id = signal["selection"].split("::")[0]
        if not player_id:
            return attach_event_keys(
                {
                    "signal_id": sid,
                    "graded_at_utc": graded_at,
                    "status": "ERROR",
                "result": None,
                "units": None,
                "market_type": market,
                "selection": signal.get("selection"),
                "event_key": signal.get("event_key"),
                    "day_key": day_key,
                    "urls": urls,
                    "notes": "player_not_found",
                }
            )
        try:
            statline = provider.fetch_player_statline(
                safe_event_key or raw_event_key,
                player_id,
                stat_key,
                date_str,
                away,
                home,
                refresh_cache=refresh_cache,
            )
        except RuntimeError:
            return attach_event_keys(
                {
                    "signal_id": sid,
                    "graded_at_utc": graded_at,
                    "status": "ERROR",
                "result": None,
                "units": None,
                "market_type": market,
                "selection": signal.get("selection"),
                "event_key": signal.get("event_key"),
                    "day_key": day_key,
                    "urls": urls,
                    "notes": "env_missing",
                }
            )
        if not statline:
            return attach_event_keys(
                {
                    "signal_id": sid,
                    "graded_at_utc": graded_at,
                    "status": "ERROR",
                "result": None,
                "units": None,
                "market_type": market,
                "selection": signal.get("selection"),
                "event_key": signal.get("event_key"),
                    "day_key": day_key,
                    "urls": urls,
                    "notes": "player_not_found",
                    "games_info": games_info,
                }
            )
        player_meta = statline.get("meta") if isinstance(statline, dict) else None
        if isinstance(statline, dict) and statline.get("player_not_found"):
            notes_val = player_meta.get("player_notes") if player_meta else "player_not_found"
            return attach_event_keys(
                {
                    "signal_id": sid,
                    "graded_at_utc": graded_at,
                    "status": "ERROR",
                "result": None,
                "units": None,
                "market_type": market,
                "selection": signal.get("selection"),
                "event_key": signal.get("event_key"),
                    "day_key": day_key,
                    "urls": urls,
                    "notes": notes_val,
                    "games_info": games_info,
                    "player_meta": player_meta,
                }
            )
        val = statline.get("value") if isinstance(statline, dict) else None
        if val is None and isinstance(statline, dict):
            val = statline.get(stat_key)
        if val is None:
            return attach_event_keys(
                {
                    "signal_id": sid,
                    "graded_at_utc": graded_at,
                    "status": "ERROR",
                "result": None,
                "units": None,
                "market_type": market,
                "selection": signal.get("selection"),
                "event_key": signal.get("event_key"),
                    "day_key": day_key,
                    "urls": urls,
                    "notes": "stat_missing",
                    "games_info": games_info,
                    "player_meta": player_meta,
                }
            )
        try:
            val_num = float(val)
        except (TypeError, ValueError):
            return attach_event_keys(
                {
                    "signal_id": sid,
                    "graded_at_utc": graded_at,
                    "status": "ERROR",
                "result": None,
                "units": None,
                "market_type": market,
                "selection": signal.get("selection"),
                "event_key": signal.get("event_key"),
                    "day_key": day_key,
                    "urls": urls,
                    "notes": "stat_missing",
                    "games_info": games_info,
                    "player_meta": player_meta,
                }
            )
        if direction == "OVER":
            result = "WIN" if val_num > line else "PUSH" if val_num == line else "LOSS"
        else:
            result = "WIN" if val_num < line else "PUSH" if val_num == line else "LOSS"
    else:
        return {
            "signal_id": sid,
            "graded_at_utc": graded_at,
            "status": "ERROR",
            "result": None,
            "units": None,
            "market_type": market,
            "selection": signal.get("selection"),
            "event_key": signal.get("event_key"),
            "day_key": day_key,
            "urls": urls,
            "notes": "unsupported_market",
        }

    source_units = signal.get("unit_size")
    stake = odds_to_stake_units(odds, source_units)
    units = american_odds_to_units(odds, result, stake=stake) if result else None
    if odds is None:
        notes.append("missing_odds")

    status_val = result if result else "ERROR"

    # Build selection_key and offer_key for audit trail
    selection_key = build_selection_key(
        day_key=day_key or "",
        market_type=market or "",
        selection=signal.get("selection"),
        player_id=signal.get("player_id"),
        atomic_stat=extract_stat_key(signal),
        direction=parse_direction(signal),
        team=signal.get("selection") if market in ("spread", "moneyline") else None,
    )
    offer_key = build_offer_key(selection_key, line, int(odds) if odds else None)

    # Compute audit fields
    stat_value: Optional[float] = None
    match_confidence: Optional[float] = None

    if market == "player_prop" and "val_num" in locals():
        stat_value = val_num
    if market == "player_prop" and "player_meta" in locals() and player_meta:
        match_method = player_meta.get("match_method")
        match_confidence = compute_player_match_confidence(match_method)

    # Determine ROI eligibility (never silently assume -110)
    signal_has_contract_odds = signal.get("odds") is not None
    roi_eligible = signal_has_contract_odds and is_roi_eligible(
        status=status_val,
        odds=int(odds) if odds else None,
        player_match_confidence_val=match_confidence,
        market_type=market,
    )

    row = {
        "signal_id": sid,
        "graded_at_utc": graded_at,
        "status": status_val,
        "result": result,
        "units": units,
        "stake": stake,
        "odds": int(odds) if odds is not None else None,
        "unit_source": "source" if source_units is not None else ("implied" if odds is not None else "default"),
        "market_type": market,
        "selection": signal.get("selection"),
        "event_key": signal.get("event_key"),
        "day_key": day_key,
        "line": line,  # Added for audit/verification purposes
        "direction": direction,  # Added for totals/props audit
        "urls": urls,
        "notes": ";".join(notes) if notes else "",
        # Contract audit fields
        "selection_key": selection_key,
        "offer_key": offer_key,
        "roi_eligible": roi_eligible,
    }

    # Add stat_value for player props
    if stat_value is not None:
        row["stat_value"] = stat_value

    # Add player_match_confidence for player props
    if match_confidence is not None:
        row["player_match_confidence"] = match_confidence

    # Provider audit fields
    if games_info:
        row["games_info"] = games_info
        row["provider_game_id"] = games_info.get("matched_game_id")
    if market == "player_prop" and "player_meta" in locals() and player_meta:
        row["player_meta"] = player_meta
        row["provider"] = player_meta.get("provider")

    return attach_event_keys(row)


def grade_signal_sources(signal: Dict[str, Any], stat_value: Optional[float]) -> List[Dict[str, Any]]:
    """
    Grade each support (source/expert row) within a signal using its specific line.

    Returns a list of dicts with keys:
      source_id, expert_slug, expert_name, expert_graded_line, expert_result

    Only player_prop supports are graded here (other markets grade at signal level).
    For non-player_prop signals the function returns an empty list.

    Called after grade_signal() produces a WIN/LOSS/PUSH result. Only meaningful
    when stat_value is not None (i.e., the grader successfully fetched the stat).
    """
    market = signal.get("market_type")
    if market != "player_prop" or stat_value is None:
        return []

    direction = parse_direction(signal)
    if direction not in {"OVER", "UNDER"}:
        return []

    signal_line = signal.get("line")

    results: List[Dict[str, Any]] = []
    for sup in signal.get("supports") or []:
        sup_line = sup.get("line")
        graded_line = sup_line if isinstance(sup_line, (int, float)) else (
            signal_line if isinstance(signal_line, (int, float)) else None
        )
        if graded_line is None:
            expert_result = None
        elif direction == "OVER":
            expert_result = "WIN" if stat_value > graded_line else "PUSH" if stat_value == graded_line else "LOSS"
        else:  # UNDER
            expert_result = "WIN" if stat_value < graded_line else "PUSH" if stat_value == graded_line else "LOSS"

        expert_name = sup.get("expert_name")
        expert_slug = None
        if expert_name:
            expert_slug = expert_name.lower().replace(" ", "_").replace(".", "")

        results.append({
            "source_id":         sup.get("source_id"),
            "expert_slug":       expert_slug,
            "expert_name":       expert_name,
            "expert_graded_line": graded_line,
            "expert_result":     expert_result,
        })

    return results


def parse_day_key_to_date(day_key: Optional[str]) -> Optional[date]:
    if not isinstance(day_key, str):
        return None
    parts = day_key.split(":")
    # Handle both NBA: and NCAAB: prefixes
    if len(parts) < 4 or parts[0] not in ("NBA", "NCAAB"):
        return None
    try:
        return date(int(parts[1]), int(parts[2]), int(parts[3]))
    except Exception:
        return None


def parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts or not isinstance(ts, str):
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def collect_urls(signal: Dict[str, Any]) -> List[str]:
    urls: List[str] = []
    seen = set()
    if isinstance(signal.get("urls"), list):
        for u in signal["urls"]:
            if isinstance(u, str) and u not in seen:
                seen.add(u)
                urls.append(u)
    for sup in signal.get("supports") or []:
        cu = sup.get("canonical_url") or sup.get("url")
        if isinstance(cu, str) and cu not in seen:
            seen.add(cu)
            urls.append(cu)
    return urls


def _pick(d: Optional[Dict[str, Any]], keys: Iterable[str]) -> Dict[str, Any]:
    if not isinstance(d, dict):
        return {}
    return {k: d[k] for k in keys if k in d}


def _stable_games_info(games_info: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    keep = ("matched_game_id", "matched_status", "date_used", "date_shifted", "date_strategy", "match_orientation")
    return _pick(games_info, keep)


def _stable_player_meta(player_meta: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    keep = ("game_id", "player_notes", "match_method", "matched_personId", "matched_name_raw")
    return _pick(player_meta, keep)


def _stable_grade_fingerprint(row: Dict[str, Any]) -> Dict[str, Any]:
    urls_raw = row.get("urls") or []
    urls = sorted({u for u in urls_raw if isinstance(u, str)})
    return {
        "signal_id": row.get("signal_id"),
        "status": row.get("status"),
        "result": row.get("result"),
        "notes": row.get("notes") or "",
        "market_type": row.get("market_type"),
        "selection": row.get("selection"),
        "day_key": row.get("day_key"),
        "event_key_safe": row.get("event_key_safe") or "",
        "event_key": row.get("event_key") or "",
        "urls": urls,
        "games_info": _stable_games_info(row.get("games_info")),
        "player_meta": _stable_player_meta(row.get("player_meta")),
        "units": row.get("units"),
        "stake": row.get("stake"),
    }


def materially_equal(prev: Optional[Dict[str, Any]], new: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(prev, dict) or not isinstance(new, dict):
        return False
    if bool(prev.get("source_grades")) != bool(new.get("source_grades")):
        return False
    return _stable_grade_fingerprint(prev) == _stable_grade_fingerprint(new)


def compact_games_info(row: Dict[str, Any], keep_full: bool) -> None:
    if not isinstance(row, dict):
        return
    gi = row.get("games_info")
    if not isinstance(gi, dict):
        return
    if keep_full:
        return
    row["games_info"] = _stable_games_info(gi)


def pending_reason(status_raw: Optional[str], date_str: Optional[str], games_info: Optional[Dict[str, Any]]) -> str:
    if not status_raw:
        return "pending_unknown_status"
    status = str(status_raw).upper().strip()
    if status in {"IN_PROGRESS", "LIVE", "HALF", "Q1", "Q2", "Q3", "Q4", "STATUS_IN_PROGRESS"}:
        return "pending_in_progress"
    if status in {"SCHEDULED", "PRE", "STATUS_SCHEDULED"}:
        requested_dt = None
        try:
            requested_dt = datetime.strptime(date_str, "%Y-%m-%d").date() if date_str else None
        except Exception:
            requested_dt = None
        date_used = None
        try:
            if isinstance(games_info, dict) and games_info.get("date_used"):
                date_used = datetime.strptime(str(games_info.get("date_used")), "%Y-%m-%d").date()
        except Exception:
            date_used = None
        if requested_dt and date_used:
            if date_used < requested_dt:
                return "pending_scheduled_date_shifted_earlier"
            if date_used > requested_dt:
                return "pending_scheduled_date_shifted_later"
        today_utc = datetime.now(timezone.utc).date()
        if requested_dt and requested_dt < today_utc:
            return "pending_scheduled_past_date"
        return "pending_scheduled"
    if status in {"POSTPONED", "PPD", "CANCELLED"}:
        return "pending_postponed"
    if status in {"FINAL", "STATUS_FINAL"}:
        return ""
    return f"pending_{status.lower()}"


def should_upgrade_pending_to_error(pending_note: str, date_str: Optional[str], games_info: Optional[Dict[str, Any]]) -> Optional[str]:
    try:
        requested_date = datetime.strptime(date_str, "%Y-%m-%d").date() if date_str else None
    except Exception:
        requested_date = None
    today_utc = datetime.now(timezone.utc).date()

    if not requested_date:
        return None
    if requested_date >= today_utc:
        return None

    if pending_note == "pending_scheduled_past_date":
        return "stale_scheduled_past_date"
    if pending_note.startswith("pending_scheduled_date_shifted_"):
        return "stale_scheduled_shifted_past_date"
    if isinstance(games_info, dict) and games_info.get("date_used") and str(games_info.get("date_used")) != date_str:
        return "stale_date_used_mismatch_past_date"
    return None


_HEALED_GAMES: Set[str] = set()


def heal_stale_scheduled(
    event_key: Optional[str],
    away: str,
    home: str,
    date_str: str,
    initial_games_info: Optional[Dict[str, Any]],
    refresh_cache: bool,
    provider: Any = None,
) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    heal_meta: Dict[str, Any] = {"healed": False, "path": None}
    if refresh_cache:
        heal_meta["reason"] = "already_refreshing"
        return None, heal_meta
    # Use passed provider or fall back to nba_provider
    results_provider = provider or nba_provider
    try:
        res = results_provider.fetch_game_result(event_key, away, home, date_str, refresh_cache=True)
    except Exception:
        heal_meta["reason"] = "fetch_exception"
        return None, heal_meta
    heal_meta["path"] = "refresh_cache"
    if isinstance(res, dict) and res.get("status") and str(res.get("status")).upper() != "SCHEDULED":
        heal_meta["healed"] = True
        return res, heal_meta
    heal_meta["reason"] = "refresh_cache_no_change"
    return None, heal_meta


def pick_latest_by_signal(grades: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    latest: Dict[str, Dict[str, Any]] = {}
    for row in grades:
        sid = row.get("signal_id")
        if not sid:
            continue
        current = latest.get(sid)
        ts_new = parse_iso(row.get("graded_at_utc")) or datetime.min.replace(tzinfo=timezone.utc)
        ts_old = parse_iso(current.get("graded_at_utc")) if current else None
        if current is None or (ts_old is not None and ts_new > ts_old):
            latest[sid] = row
    return latest


def main() -> None:
    ap = argparse.ArgumentParser(description="Grade signals (NBA or NCAAB).")
    ap.add_argument("--sport", choices=["NBA", "NCAAB"], default="NBA", help="Sport to grade (default: NBA)")
    ap.add_argument("--since", type=str, help="Only grade signals with day_key >= YYYY-MM-DD")
    ap.add_argument("--mode", choices=["pending", "grade"], default="grade", help="pending=just enqueue PENDING rows; grade=compute outcomes")
    ap.add_argument("--dry-run", action="store_true", help="Do not write any output files.")
    ap.add_argument("--debug", action="store_true", help="Print debug samples and counters.")
    ap.add_argument("--refresh-cache", action="store_true", help="Force refresh of provider caches.")
    ap.add_argument("--provider", choices=["auto", "nba_api", "nba_cdn", "api_sports"], default="auto", help="Override results provider selection.")
    ap.add_argument(
        "--signals",
        type=str,
        default=None,
        help="Signals JSONL input (default: auto based on sport). Use signals_occurrences.jsonl for history grading.",
    )
    ap.add_argument(
        "--regrade",
        action="store_true",
        help="Bypass already_graded checks and re-grade all signals (useful for fixing grades after provider updates).",
    )
    args = ap.parse_args()

    # Determine sport and load appropriate provider
    sport = args.sport
    if sport == NCAAB_SPORT:
        from src.results import ncaab_provider_espn  # type: ignore
        results_provider = ncaab_provider_espn
    else:
        results_provider = nba_provider

    # Get sport-specific paths
    signals_latest_path = get_signals_latest_path(sport)
    grades_occ_path = get_grades_occ_path(sport)
    grades_latest_path = get_grades_latest_path(sport)

    if args.provider != "auto":
        os.environ["RESULTS_PROVIDER"] = args.provider
        if args.provider == "nba_api":
            os.environ["NBA_API"] = "1"
    else:
        # allow default dispatch to choose nba_api without forcing
        os.environ.pop("RESULTS_PROVIDER", None)

    since_date: Optional[date] = None
    if args.since:
        try:
            since_date = datetime.strptime(args.since, "%Y-%m-%d").date()
        except ValueError:
            raise SystemExit(f"Invalid --since value: {args.since} (expected YYYY-MM-DD)")

    # Use explicit signals path or sport-specific default
    signals_path = Path(args.signals) if args.signals else signals_latest_path
    print(f"[grader] Reading {sport} signals from: {signals_path}")
    signals = read_jsonl(signals_path)

    # Build market_line lookup from plays files (score_signals writes market_line there;
    # grader needs it to grade spreads with correct sign)
    _market_line_by_sid: Dict[str, float] = {}
    _plays_dir = Path("data/plays")
    if _plays_dir.exists():
        for _pf in _plays_dir.glob("plays_*.json"):
            try:
                import json as _json
                _d = _json.loads(_pf.read_text())
                for _play in _d.get("plays", []):
                    _sig = _play.get("signal") or {}
                    _sid = _sig.get("signal_id")
                    _ml = _sig.get("market_line")
                    if _sid and _ml is not None:
                        _market_line_by_sid[_sid] = float(_ml)
            except Exception:
                pass
    if _market_line_by_sid:
        # Inject market_line into signals so pick_line() can use it
        for _s in signals:
            _sid = _s.get("signal_id")
            if _sid and _sid in _market_line_by_sid and _s.get("market_type") == "spread":
                _s["market_line"] = _market_line_by_sid[_sid]
    # Pick best hydrated signal per id to fill missing matchup info
    best_by_signal_id: Dict[str, Dict[str, Any]] = {}
    def score_sig(s: Dict[str, Any]) -> int:
        score = 0
        cgk = s.get("canonical_game_key")
        if isinstance(cgk, (list, tuple)) and len(cgk) >= 4 and cgk[2] and cgk[3]:
            score += 3
        if s.get("away_team") and s.get("home_team"):
            score += 2
        if s.get("event_key"):
            score += 2
        if s.get("matchup_key"):
            score += 1
        if s.get("matchup"):
            score += 1
        return score
    for s in signals:
        sid = s.get("signal_id")
        if not sid:
            continue
        sc = score_sig(s)
        prev = best_by_signal_id.get(sid)
        if prev is None or sc > score_sig(prev):
            best_by_signal_id[sid] = s
    existing_occ = read_jsonl(grades_occ_path)
    latest_by_signal = pick_latest_by_signal(existing_occ)

    # Get provider info - for NCAAB we may not have full provider_info()
    try:
        prov_info = results_provider.provider_info() if hasattr(results_provider, 'provider_info') else {"provider": sport.lower()}
    except Exception:
        prov_info = {"provider": sport.lower()}
    print(f"[provider] name={prov_info.get('provider')} base={prov_info.get('base')} api_key_present={prov_info.get('key_present')} refresh_cache={args.refresh_cache}")

    counters = Counter()
    unresolved_cdn: List[Dict[str, Any]] = []
    new_rows: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc).isoformat()
    graded_samples: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    error_samples: List[Dict[str, Any]] = []
    debug_provider_samples: List[Dict[str, Any]] = []

    for sig in signals:
        sid = sig.get("signal_id")
        if not sid:
            counters["skipped_missing_id"] += 1
            continue

        day_dt = parse_day_key_to_date(sig.get("day_key"))
        if since_date and day_dt and day_dt < since_date:
            counters["skipped_before_since"] += 1
            continue

        prev = latest_by_signal.get(sid)
        if prev:
            prev_status = prev.get("status")
            if args.mode == "pending":
                if prev_status == "PENDING":
                    counters["already_pending"] += 1
                    continue
                if prev_status in TERMINAL_STATUSES:
                    counters["already_terminal"] += 1
                    continue
            if args.mode == "grade" and prev_status in TERMINAL_STATUSES and not args.regrade:
                counters["already_graded"] += 1
                continue

        # hydrate from best-known record for this signal id when matchup fields are missing
        if sid in best_by_signal_id:
            best_sig = best_by_signal_id[sid]
            merged = dict(best_sig)
            merged.update({k: v for k, v in sig.items() if v is not None})
        else:
            merged = sig

        date_str = derive_date_str(merged)
        raw_event_key = merged.get("event_key")
        away_safe, home_safe = derive_teams(merged)
        safe_event_key = (
            f"{sport}:{date_str.replace('-', ':')}:{away_safe}@{home_safe}"
            if date_str and away_safe and home_safe
            else None
        )

        if args.mode == "pending":
            grade_row = {
                "signal_id": sid,
                "graded_at_utc": now,
                "status": "PENDING",
                "result": None,
                "units": None,
                "market_type": sig.get("market_type"),
                "selection": sig.get("selection"),
                "event_key": sig.get("event_key"),
                "day_key": sig.get("day_key"),
                "urls": collect_urls(sig),
                "notes": "pending grade",
            }
        else:
            # Check for pre-graded results (e.g., from SportsLine expert picks)
            pregraded_result = merged.get("pregraded_result")
            pregraded_by = merged.get("pregraded_by")
            if pregraded_result and pregraded_result in {"WIN", "LOSS", "PUSH"}:
                # Use pre-graded result directly instead of fetching from provider
                odds = merged.get("odds")
                pre_source_units = merged.get("unit_size")
                pre_stake = odds_to_stake_units(odds, pre_source_units)
                units = american_odds_to_units(odds, pregraded_result, stake=pre_stake) if pregraded_result else None
                grade_row = {
                    "signal_id": sid,
                    "graded_at_utc": now,
                    "status": pregraded_result,
                    "result": pregraded_result,
                    "units": units,
                    "stake": pre_stake,
                    "odds": int(odds) if odds is not None else None,
                    "unit_source": "source" if pre_source_units else ("implied" if odds is not None else "default"),
                    "market_type": merged.get("market_type"),
                    "selection": merged.get("selection"),
                    "event_key": merged.get("event_key"),
                    "event_key_raw": raw_event_key,
                    "event_key_safe": safe_event_key,
                    "day_key": merged.get("day_key"),
                    "urls": collect_urls(merged),
                    "notes": f"pregraded_by_{pregraded_by}" if pregraded_by else "pregraded",
                    "graded_by": pregraded_by,
                    # Include scores if available
                    "home_score": merged.get("home_score"),
                    "away_score": merged.get("away_score"),
                }
                counters["pregraded"] += 1
                counters[f"pregraded_{pregraded_result}"] += 1
            else:
                eligible, reason, missing_fields = check_eligibility(merged, date_str, sport=sport)
                if not eligible:
                    grade_row = {
                        "signal_id": sid,
                        "graded_at_utc": now,
                        "status": "INELIGIBLE",
                        "result": None,
                        "units": None,
                        "market_type": merged.get("market_type"),
                        "selection": merged.get("selection"),
                        "event_key": merged.get("event_key"),
                        "event_key_raw": raw_event_key,
                        "event_key_safe": safe_event_key,
                        "day_key": merged.get("day_key"),
                        "canonical_game_key": merged.get("canonical_game_key"),
                        "urls": collect_urls(merged),
                        "notes": reason,
                        "missing_fields": missing_fields,
                    }
                    compact_games_info(grade_row, keep_full=args.debug or grade_row.get("status") == "ERROR")
                    new_rows.append(grade_row)
                    latest_by_signal[sid] = grade_row
                    counters["added"] += 1
                    counters[f"ineligible_{reason}"] += 1
                    continue
                grade_row = grade_signal(
                    merged,
                    now,
                    date_str,
                    args.refresh_cache,
                    provider_name=prov_info.get("provider", ""),
                    debug=args.debug,
                    debug_samples=debug_provider_samples,
                    counters=counters,
                    sport=sport,
                    results_provider=results_provider,
                )

        if grade_row.get("status") == "PENDING":
            counters[f"pending_{grade_row.get('notes')}"] += 1

        # Per-source grading: attach source_grades before materially_equal check so
        # already-graded signals still get source_grades populated on regrade runs.
        if args.mode == "grade" and grade_row.get("status") in {"WIN", "LOSS", "PUSH", "GRADED"}:
            stat_val = grade_row.get("stat_value") or grade_row.get("actual_stat_value")
            source_grades = grade_signal_sources(merged, stat_val)
            if source_grades:
                grade_row["source_grades"] = source_grades

        grade_row.setdefault("grade_notes", grade_row.get("notes") or "")

        if prev and materially_equal(prev, grade_row):
            counters["unchanged"] += 1
            continue

        compact_games_info(grade_row, keep_full=args.debug or grade_row.get("status") == "ERROR")

        new_rows.append(grade_row)
        latest_by_signal[sid] = grade_row
        counters["added"] += 1
        if args.mode == "grade" and grade_row.get("status") in {"WIN", "LOSS", "PUSH"}:
            mkt = grade_row.get("market_type") or "unknown"
            if len(graded_samples[mkt]) < 5:
                graded_samples[mkt].append(grade_row)
        if args.mode == "grade" and grade_row.get("status") == "ERROR":
            counters[f"error_{grade_row.get('notes')}"] += 1
            if len(error_samples) < 5:
                sample = {
                    "signal_id": sid,
                    "notes": grade_row.get("notes"),
                    "day_key": sig.get("day_key"),
                    "event_key": sig.get("event_key"),
                    "derived_date": date_str,
                    "games_info": grade_row.get("games_info"),
                    "player_meta": grade_row.get("player_meta"),
                    "line_fields": {k: sig.get(k) for k in ("line", "line_median", "line_max", "line_min")},
                    "player_candidates": None,
                    "player_sample_names": None,
                }
                if grade_row.get("player_meta"):
                    sample["player_candidates"] = grade_row["player_meta"].get("player_candidates")
                    sample["player_sample_names"] = grade_row["player_meta"].get("sample_players")
                error_samples.append(sample)
            if (
                args.debug or os.environ.get("DEBUG_CDN_PROPS") == "1"
            ) and grade_row.get("market_type") == "player_prop" and grade_row.get("player_meta"):
                pm = grade_row["player_meta"]
                note = pm.get("player_notes") or grade_row.get("notes")
                if note in {"player_not_found", "player_not_in_game"}:
                    player_id = merged.get("player_id") or ""
                    stat_key = extract_stat_key(merged) or ""
                    unresolved_cdn.append(
                        {
                            "player_id": player_id,
                            "stat_key": stat_key,
                            "game_id": pm.get("game_id") or grade_row.get("game_id"),
                            "event_key": grade_row.get("event_key"),
                            "requested_norm": pm.get("requested_player_key_norm"),
                            "players_norm_sample": (pm.get("players_norm_sample") or [])[:8],
                            "notes": note,
                        }
                    )

    # Prune orphaned grades (signal_ids no longer in the current ledger)
    current_signal_ids = {s.get("signal_id") for s in signals if s.get("signal_id")}
    pruned = {sid: g for sid, g in latest_by_signal.items() if sid in current_signal_ids}
    orphan_count = len(latest_by_signal) - len(pruned)
    if orphan_count:
        print(f"[grader] Pruned {orphan_count} orphaned grades (no matching signal in ledger)")

    for row in pruned.values():
        row.setdefault("grade_notes", row.get("notes") or "")

    if not args.dry_run:
        if new_rows:
            append_jsonl(grades_occ_path, new_rows)
        write_jsonl(grades_latest_path, pruned.values())

    # Validation output
    status_counts = Counter(row.get("status") for row in pruned.values())
    result_counts = Counter(row.get("result") for row in pruned.values() if row.get("result"))
    market_counts = Counter(row.get("market_type") for row in pruned.values())
    print("Grade counts by status:", dict(status_counts))
    print("Result counts:", dict(result_counts))
    print("Grade counts by market_type:", dict(market_counts))
    if args.debug:
        try:
            from src.results import nba_provider_nba_api as nba_api_provider  # type: ignore
            print("Provider stats (nba_api):", nba_api_provider.STATS)
        except Exception:
            pass
        try:
            print("Provider stats (api_sports):", nba_provider_api_sports.STATS)
        except Exception:
            pass
        try:
            from src.results import nba_provider_nba_cdn as nba_cdn_provider  # type: ignore
            print("Provider stats (nba_cdn):", nba_cdn_provider.STATS)
        except Exception:
            pass
        if counters:
            print("Error note counts (selected):", {k: v for k, v in counters.items() if k.startswith("error_")})
        if debug_provider_samples:
            print("Provider debug samples (up to 5):")
            for row in debug_provider_samples:
                print(row)
        if unresolved_cdn:
            print("UNRESOLVED CDN PLAYERS (top 20):")
            agg = Counter()
            sample_by_key: Dict[str, Dict[str, Any]] = {}
            for item in unresolved_cdn:
                key = f"{item.get('player_id')}|{item.get('stat_key')}|{item.get('requested_norm')}"
                agg[key] += 1
                sample_by_key.setdefault(key, item)
            for key, cnt in agg.most_common(20):
                sample = sample_by_key[key]
                print(
                    {
                        "count": cnt,
                        "player_id": sample.get("player_id"),
                        "stat_key": sample.get("stat_key"),
                        "requested_norm": sample.get("requested_norm"),
                        "game_id": sample.get("game_id"),
                        "event_key": sample.get("event_key"),
                        "players_norm_sample": sample.get("players_norm_sample"),
                        "replay_cmd": f"python3 scripts/check_nba_cdn_boxscore_players.py --game-id {sample.get('game_id')} --debug --refresh-cache --find {sample.get('player_id')}",
                    }
                )

    if args.mode == "pending":
        pending_samples = [row for row in latest_by_signal.values() if row.get("status") == "PENDING"][:5]
        print("Sample PENDING rows (up to 5):")
        for row in pending_samples:
            preview = {
                "signal_id": row.get("signal_id"),
                "graded_at_utc": row.get("graded_at_utc"),
                "market_type": row.get("market_type"),
                "selection": row.get("selection"),
                "day_key": row.get("day_key"),
            }
            print(preview)
    else:
        if graded_samples:
            for mkt, rows in graded_samples.items():
                print(f"Sample graded rows for {mkt} (up to 5):")
                for row in rows:
                    preview = {
                        "signal_id": row.get("signal_id"),
                        "status": row.get("status"),
                        "result": row.get("result"),
                        "units": row.get("units"),
                        "selection": row.get("selection"),
                        "day_key": row.get("day_key"),
                        "notes": row.get("notes"),
                    }
                    print(preview)
        if error_samples:
            print("Sample ERROR rows (up to 5):")
            for row in error_samples:
                print(row)

    if args.debug:
        print("Debug counters:", dict(counters))
        if new_rows:
            print(f"First 3 new grade rows (of {len(new_rows)}):")
            for row in new_rows[:3]:
                print(row)
    else:
        print(f"Prepared {len(new_rows)} new grade rows (mode={args.mode}, dry_run={args.dry_run}).")


if __name__ == "__main__":
    main()
