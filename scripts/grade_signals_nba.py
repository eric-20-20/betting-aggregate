"""Grade NBA signals using external provider data with caching and idempotent writes.

Env:
  RESULTS_PROVIDER=nba_api|nba_cdn|api_sports|auto
  API_SPORTS_KEY=<key>
  API_SPORTS_BASE=https://v1.basketball.api-sports.io (scheme auto-added if missing)

Flags:
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
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# ensure repo root on path for src package imports
sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.results import nba_provider  # type: ignore
from src.results import nba_provider_api_sports  # type: ignore


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


def american_odds_to_units(odds: Optional[Any], outcome: str) -> Optional[float]:
    """
    Return profit units for a 1u stake given American odds and outcome WIN/LOSS/PUSH.
    """
    if outcome == "PUSH":
        return 0.0
    if odds is None:
        return None
    try:
        o = float(odds)
    except (TypeError, ValueError):
        return None
    if o == 0:
        return None
    win_units = (o / 100.0) if o > 0 else (100.0 / abs(o))
    if outcome == "WIN":
        return round(win_units, 3)
    if outcome == "LOSS":
        return -1.0
    return None


def extract_stat_key(signal: Dict[str, Any]) -> Optional[str]:
    # prefer atomic_stat, else selection parsing NBA:player::stat::dir
    stat = signal.get("atomic_stat")
    if isinstance(stat, str) and stat:
        return stat
    sel = signal.get("selection")
    if isinstance(sel, str) and "::" in sel:
        parts = sel.split("::")
        if len(parts) >= 3:
            return parts[1]
    return None


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
    # canonical key takes precedence
    cgk = signal.get("canonical_game_key")
    if isinstance(cgk, (list, tuple)) and len(cgk) >= 4 and cgk[2] and cgk[3]:
        return str(cgk[2]).upper(), str(cgk[3]).upper()
    if away and home:
        return str(away).upper(), str(home).upper()

    def parse_pair(val: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        if not isinstance(val, str):
            return None, None
        if "@" in val:
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
        return direction.upper()
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
    if day_key.startswith("NBA:"):
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
    if event_key.startswith("NBA:"):
        parts = event_key.split(":")
        if len(parts) >= 4 and all(p.isdigit() for p in parts[1:4]):
            return f"{parts[1]}-{parts[2]}-{parts[3]}"
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
    ek = signal.get("event_key")
    return event_key_to_date_str(ek)


def check_eligibility(signal: Dict[str, Any], date_str: Optional[str]) -> Tuple[bool, str, List[str]]:
    """Return (eligible?, reason, missing_fields) before hitting providers."""

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
    if market == "player_prop":
        if not (has_cgk or has_explicit_matchup):
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


def grade_signal(
    signal: Dict[str, Any],
    graded_at: str,
    date_str: Optional[str],
    refresh_cache: bool,
    provider_name: str,
    debug: bool = False,
    debug_samples: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    sid = signal.get("signal_id")
    day_key = signal.get("day_key")
    away, home = derive_teams(signal)
    urls = collect_urls(signal)
    market = signal.get("market_type")
    notes: List[str] = []
    raw_event_key = signal.get("event_key")
    safe_event_key = (
        f"NBA:{date_str.replace('-', ':')}:{away}@{home}"
        if date_str and away and home
        else None
    )

    def attach_event_keys(row: Dict[str, Any]) -> Dict[str, Any]:
        row.setdefault("event_key_raw", raw_event_key)
        if safe_event_key:
            row.setdefault("event_key_safe", safe_event_key)
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
        game_result = nba_provider.fetch_game_result(
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

    line = pick_line(signal)
    odds = pick_odds(signal)
    direction = parse_direction(signal)

    home_score = game_result.get("home_score")
    away_score = game_result.get("away_score")
    game_id = game_result.get("game_id")

    status_raw = game_result.get("status")
    if debug and games_info and date_str and games_info.get("date") and games_info.get("date") != date_str:
        notes.append(f"meta_date_mismatch:{games_info.get('date')}!= {date_str}")
    if status_raw == "ERROR":
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
                "notes": game_result.get("notes") or ";".join(notes) or "game_not_found",
                "games_info": games_info,
            }
        )
    if status_raw and status_raw != "FINAL":
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
                "notes": "game_not_final",
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

    if market in {"spread", "total", "moneyline"}:
        if market == "moneyline":
            selected = (signal.get("selection") or "").upper()
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
            statline = nba_provider.fetch_player_statline(
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

    units = american_odds_to_units(odds, result) if result else None
    if odds is None:
        notes.append("missing_odds")

    status_val = result if result else "ERROR"
    row = {
        "signal_id": sid,
        "graded_at_utc": graded_at,
        "status": status_val,
        "result": result,
        "units": units,
        "market_type": market,
        "selection": signal.get("selection"),
        "event_key": signal.get("event_key"),
        "day_key": day_key,
        "urls": urls,
        "notes": ";".join(notes) if notes else "",
    }
    if games_info:
        row["games_info"] = games_info
    if market == "player_prop" and "player_meta" in locals() and player_meta:
        row["player_meta"] = player_meta
    return attach_event_keys(row)


def parse_day_key_to_date(day_key: Optional[str]) -> Optional[date]:
    if not isinstance(day_key, str):
        return None
    parts = day_key.split(":")
    if len(parts) < 4 or parts[0] != "NBA":
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
    ap = argparse.ArgumentParser(description="Grade NBA signals.")
    ap.add_argument("--since", type=str, help="Only grade signals with day_key >= YYYY-MM-DD")
    ap.add_argument("--mode", choices=["pending", "grade"], default="grade", help="pending=just enqueue PENDING rows; grade=compute outcomes")
    ap.add_argument("--dry-run", action="store_true", help="Do not write any output files.")
    ap.add_argument("--debug", action="store_true", help="Print debug samples and counters.")
    ap.add_argument("--refresh-cache", action="store_true", help="Force refresh of provider caches.")
    ap.add_argument("--provider", choices=["auto", "nba_api", "nba_cdn", "api_sports"], default="auto", help="Override results provider selection.")
    args = ap.parse_args()

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

    signals = read_jsonl(SIGNALS_LATEST_PATH)
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
    existing_occ = read_jsonl(GRADES_OCC_PATH)
    latest_by_signal = pick_latest_by_signal(existing_occ)

    prov_info = nba_provider.provider_info()
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
            if args.mode == "grade" and prev_status in TERMINAL_STATUSES:
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
            f"NBA:{date_str.replace('-', ':')}:{away_safe}@{home_safe}"
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
            eligible, reason, missing_fields = check_eligibility(merged, date_str)
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
            )

        if prev and prev == grade_row:
            counters["unchanged"] += 1
            continue

        new_rows.append(grade_row)
        latest_by_signal[sid] = grade_row
        counters["added"] += 1
        if args.mode == "grade" and grade_row.get("status") == "GRADED":
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

    if not args.dry_run:
        if new_rows:
            append_jsonl(GRADES_OCC_PATH, new_rows)
        write_jsonl(GRADES_LATEST_PATH, latest_by_signal.values())

    # Validation output
    status_counts = Counter(row.get("status") for row in latest_by_signal.values())
    result_counts = Counter(row.get("result") for row in latest_by_signal.values() if row.get("result"))
    market_counts = Counter(row.get("market_type") for row in latest_by_signal.values())
    print("Grade counts by status:", dict(status_counts))
    print("Result counts:", dict(result_counts))
    print("Grade counts by market_type:", dict(market_counts))
    if args.debug:
        if prov_info.get("provider") == "nba_api":
            try:
                from src.results import nba_provider_nba_api as nba_api_provider  # type: ignore
                print("Provider stats:", nba_api_provider.STATS)
            except Exception:
                pass
        else:
            try:
                print("Provider stats:", nba_provider_api_sports.STATS)
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
