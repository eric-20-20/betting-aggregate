"""Grade consensus signals against game results using canonical game keys.

Inputs:
  - data/ledger/signals_latest.jsonl (default) or signals_occurrences.jsonl
  - data/results/games.jsonl

Output (append-only with dedupe by signal_id):
  - data/ledger/grades.jsonl

Usage:
    python3 scripts/grade_signals.py [--use-occurrences] [--write-nodata] [--debug]
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


OCCURRENCES_PATH = Path("data/ledger/signals_occurrences.jsonl")
LATEST_PATH = Path("data/ledger/signals_latest.jsonl")
RESULTS_PATH = Path("data/results/games.jsonl")
GRADES_PATH = Path("data/ledger/grades.jsonl")
SUPPORTED_MARKETS = {"spread", "total", "moneyline"}
DEBUG_LIMIT = 20


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


def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
            f.write("\n")


def parse_event_teams(event_key: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not event_key or "@" not in event_key:
        return None, None, None
    # event_key formats: NBA:YYYY:MM:DD:AWY@HOME[:time]
    parts = event_key.split(":")
    teams_part = None
    if len(parts) >= 4:
        teams_part = parts[4] if len(parts) >= 5 else parts[3]
    teams_part = teams_part or event_key.split(":")[-1]
    if "@" not in teams_part:
        return None, None, None
    away, home = teams_part.split("@", 1)
    day = ":".join(parts[0:4]) if len(parts) >= 4 else None
    return away.upper(), home.upper(), day


def game_key_core(event_key: Optional[str]) -> Optional[str]:
    away, home, day = parse_event_teams(event_key)
    if not (away and home and day):
        return None
    return f"{day}:{away}@{home}"


def normalize_day_key_from_signal(signal: Dict[str, Any]) -> Optional[str]:
    day_raw = signal.get("day_key")
    if isinstance(day_raw, str) and re.match(r"^NBA:\d{4}:\d{2}:\d{2}$", day_raw):
        return day_raw
    if isinstance(day_raw, str):
        m = re.match(r"^NBA:(\d{4})(\d{2})(\d{2})", day_raw)
        if m:
            return f"NBA:{m.group(1)}:{m.group(2)}:{m.group(3)}"
    evk = signal.get("event_key")
    if isinstance(evk, str):
        m = re.match(r"^(NBA:\d{4}:\d{2}:\d{2})", evk)
        if m:
            return m.group(1)
        m = re.match(r"^NBA:(\d{4})(\d{2})(\d{2})", evk)
        if m:
            return f"NBA:{m.group(1)}:{m.group(2)}:{m.group(3)}"
    mk = signal.get("matchup_key")
    if isinstance(mk, str):
        m = re.match(r"^(NBA:\d{4}:\d{2}:\d{2})", mk)
        if m:
            return m.group(1)
        m = re.match(r"^NBA:(\d{4})(\d{2})(\d{2})", mk)
        if m:
            return f"NBA:{m.group(1)}:{m.group(2)}:{m.group(3)}"
    return None


def derive_teams_from_signal(signal: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    away = signal.get("away_team")
    home = signal.get("home_team")

    if away and home:
        return away.upper(), home.upper()

    # Try parsing day_key if it embeds matchup (e.g., NBA:20260123:NOP@MEM or NBA:2026:01:23:NOP@MEM)
    dk = signal.get("day_key")
    if isinstance(dk, str) and "@" in dk:
        try:
            teams_part = dk.split(":")[-1]
            if "@" in teams_part:
                a, h = teams_part.split("@", 1)
                away = away or a.upper()
                home = home or h.upper()
        except Exception:
            pass

    evk = signal.get("event_key")
    ev_away, ev_home, _ = parse_event_teams(evk)
    away = away or ev_away
    home = home or ev_home

    mk = signal.get("matchup_key")
    if (not away or not home) and isinstance(mk, str) and "-" in mk:
        # format: NBA:YYYY:MM:DD:HOM-AWY (order home-away)
        try:
            teams_part = mk.split(":")[-1]
            if "-" in teams_part:
                home_m, away_m = teams_part.split("-", 1)
                home = home or home_m.upper()
                away = away or away_m.upper()
        except Exception:
            pass

    matchup = signal.get("matchup")
    if (not away or not home) and matchup and "@" in str(matchup):
        parts = str(matchup).split("@")
        if len(parts) == 2:
            away = away or parts[0].upper()
            home = home or parts[1].upper()

    return away.upper() if away else None, home.upper() if home else None


def derive_canonical_game_key(day_key: Optional[str], away: Optional[str], home: Optional[str]) -> Optional[Tuple[str, str, str, str]]:
    if not (day_key and away and home):
        return None
    return ("NBA", day_key, away.upper(), home.upper())


def _shift_day_key(day_key: Optional[str], delta_days: int) -> Optional[str]:
    if not day_key or not isinstance(day_key, str):
        return None
    m = re.match(r"^NBA:(\d{4}):(\d{2}):(\d{2})$", day_key)
    if not m:
        return None
    try:
        dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        dt_shift = dt + timedelta(days=delta_days)
        return f"NBA:{dt_shift:%Y:%m:%d}"
    except Exception:
        return None


def extract_signal_game(signal: Dict[str, Any]) -> Optional[Tuple[str, str, str, str]]:
    if signal.get("market_type") == "player_prop":
        return None
    if signal.get("signal_type") == "avoid_conflict" or signal.get("selection") in {
        "spread_conflict",
        "total_conflict",
        "moneyline_conflict",
    }:
        return None
    day_key = normalize_day_key_from_signal(signal)
    away, home = derive_teams_from_signal(signal)
    return derive_canonical_game_key(day_key, away, home)


def to_float(val: Any) -> Optional[float]:
    try:
        if val is None:
            return None
        return float(val)
    except (TypeError, ValueError):
        return None


def grade_spread(row: Dict[str, Any], result: Dict[str, Any]) -> Tuple[str, int]:
    selected = (row.get("selection") or "").upper()
    line = to_float(row.get("line_median")) or to_float(row.get("line"))
    if not selected or line is None:
        return "NO_DATA", 0
    away = (result.get("away_team") or "").upper()
    home = (result.get("home_team") or "").upper()
    away_score = result.get("away_score")
    home_score = result.get("home_score")
    if away_score is None or home_score is None:
        return "NO_DATA", 0
    if selected == away:
        margin = away_score - home_score
    elif selected == home:
        margin = home_score - away_score
    else:
        return "NO_DATA", 0
    adj = margin + line
    if adj > 0:
        return "WIN", 1
    if adj == 0:
        return "PUSH", 0
    return "LOSS", -1


def grade_total(row: Dict[str, Any], result: Dict[str, Any]) -> Tuple[str, int]:
    direction = (row.get("selection") or row.get("direction") or "").upper()
    line = to_float(row.get("line_median")) or to_float(row.get("line"))
    if direction not in {"OVER", "UNDER"} or line is None:
        return "NO_DATA", 0
    home_score = result.get("home_score")
    away_score = result.get("away_score")
    if home_score is None or away_score is None:
        return "NO_DATA", 0
    total = home_score + away_score
    if direction == "OVER":
        if total > line:
            return "WIN", 1
        if total == line:
            return "PUSH", 0
        return "LOSS", -1
    else:  # UNDER
        if total < line:
            return "WIN", 1
        if total == line:
            return "PUSH", 0
        return "LOSS", -1


def grade_moneyline(row: Dict[str, Any], result: Dict[str, Any]) -> Tuple[str, int]:
    selected = (row.get("selection") or "").upper()
    away = (result.get("away_team") or "").upper()
    home = (result.get("home_team") or "").upper()
    if not selected or not away or not home:
        return "NO_DATA", 0
    away_score = result.get("away_score")
    home_score = result.get("home_score")
    if away_score is None or home_score is None:
        return "NO_DATA", 0
    winner = away if away_score > home_score else home if home_score > away_score else None
    if winner is None:
        return "PUSH", 0
    return ("WIN", 1) if selected == winner else ("LOSS", -1)


def build_signal_index(signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: Dict[str, Dict[str, Any]] = {}
    fallback_dedup: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for sig in signals:
        sid = sig.get("signal_id")
        if sid:
            deduped[sid] = sig
            continue
        key = (sig.get("signal_key"), sig.get("sources_combo"), sig.get("market_type"))
        fallback_dedup[key] = sig
    combined = list(deduped.values()) + [v for k, v in fallback_dedup.items() if v.get("signal_id") not in deduped]
    return combined


def main() -> None:
    ap = argparse.ArgumentParser(description="Grade signals against results")
    ap.add_argument("--use-occurrences", action="store_true", help="Use signals_occurrences instead of signals_latest")
    ap.add_argument("--write-nodata", action="store_true", help="Persist NO_DATA rows (default: skip)")
    ap.add_argument("--debug", action="store_true", help="Print debug join attempts")
    args = ap.parse_args()

    signals_path = OCCURRENCES_PATH if args.use_occurrences else LATEST_PATH
    signals_raw = read_jsonl(signals_path)
    signals = build_signal_index(signals_raw)

    results = {}
    for row in read_jsonl(RESULTS_PATH):
        key = tuple(row.get("canonical_game_key")) if isinstance(row.get("canonical_game_key"), list) else None
        if key:
            results[key] = row
    result_keys = set(results.keys())
    existing_grades = read_jsonl(GRADES_PATH)
    already_ids = {g.get("signal_id") for g in existing_grades if g.get("signal_id")}
    already_keys = {
        (g.get("signal_key"), g.get("sources_combo"), g.get("market_type"))
        for g in existing_grades
        if g.get("signal_key")
    }

    counters = Counter()
    missing_result_keys = []
    debug_samples = []
    new_grades: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc).isoformat()

    for row in signals:
        counters["attempted"] += 1
        sid = row.get("signal_id")
        key_tuple = (row.get("signal_key"), row.get("sources_combo"), row.get("market_type"))
        if (sid and sid in already_ids) or (not sid and key_tuple in already_keys):
            counters["already_graded"] += 1
            continue
        mkt = row.get("market_type")
        if mkt not in SUPPORTED_MARKETS:
            counters["skipped_non_standard_market"] += 1
            continue
        if row.get("selection") in {"spread_conflict", "total_conflict", "moneyline_conflict"} or row.get("signal_type") == "avoid_conflict":
            counters["skipped_conflict_signal"] += 1
            continue
        canonical_key = extract_signal_game(row)
        if not canonical_key:
            counters["skipped_missing_canonical_key"] += 1
            continue
        has_result = canonical_key in result_keys
        swap_key = (canonical_key[0], canonical_key[1], canonical_key[3], canonical_key[2]) if len(canonical_key) == 4 else None
        has_swap = swap_key in result_keys if swap_key else False
        if not has_result and not has_swap:
            counters["skipped_no_result_for_key"] += 1
            if len(missing_result_keys) < 10:
                missing_result_keys.append(canonical_key)
            continue
        result = results.get(canonical_key) if canonical_key else None
        joined_via_swap = False
        if not result and canonical_key and len(canonical_key) == 4:
            swap_key = (canonical_key[0], canonical_key[1], canonical_key[3], canonical_key[2])
            if swap_key in results:
                result = results[swap_key]
                joined_via_swap = True
        # optional +/-1 day fallback (same teams)
        if not result and canonical_key and len(canonical_key) == 4:
            day = canonical_key[1]
            alt_keys = []
            for delta in (-1, 1):
                alt_day = _shift_day_key(day, delta)
                if alt_day:
                    alt_keys.append((canonical_key[0], alt_day, canonical_key[2], canonical_key[3]))
                    alt_keys.append((canonical_key[0], alt_day, canonical_key[3], canonical_key[2]))
            found = [k for k in alt_keys if k in results]
            if len(found) == 1:
                result = results[found[0]]
                joined_via_swap = found[0][2] == canonical_key[3]
        if not result:
            counters["skipped_no_game"] += 1
            if args.debug and len(debug_samples) < DEBUG_LIMIT:
                debug_samples.append((row, canonical_key, None, "no_matching_game"))
            continue

        if mkt == "spread":
            outcome, units = grade_spread(row, result)
        elif mkt == "total":
            outcome, units = grade_total(row, result)
        else:  # moneyline
            outcome, units = grade_moneyline(row, result)

        if outcome == "NO_DATA" and not args.write_nodata:
            counters["skipped_missing_fields"] += 1
            if args.debug and len(debug_samples) < DEBUG_LIMIT:
                debug_samples.append((row, canonical_key, result, "missing_fields"))
            continue

        grade_row = {
            "graded_at_utc": now,
            "signal_id": sid,
            "signal_key": row.get("signal_key"),
            "signal_type": row.get("signal_type"),
            "market_type": mkt,
            "sources_combo": row.get("sources_combo"),
            "sources": row.get("sources"),
            "experts": row.get("experts"),
            "day_key": row.get("day_key"),
            "event_key": row.get("event_key"),
            "matchup_key": row.get("matchup_key"),
            "away_team": row.get("away_team"),
            "home_team": row.get("home_team"),
            "selection": row.get("selection"),
            "direction": row.get("direction") or row.get("side"),
            "side": row.get("side"),
            "line": row.get("line_median") if row.get("line_median") is not None else row.get("line"),
            "odds": row.get("odds") or row.get("best_odds"),
            "canonical_game_key": canonical_key,
            "joined_via_swap": joined_via_swap,
            "final_home_score": result.get("home_score"),
            "final_away_score": result.get("away_score"),
            "computed_total_points": (result.get("home_score") or 0) + (result.get("away_score") or 0)
            if result.get("home_score") is not None and result.get("away_score") is not None
            else None,
            "computed_margin": (result.get("home_score") - result.get("away_score"))
            if result.get("home_score") is not None and result.get("away_score") is not None
            else None,
            "result": outcome,
            "units": units,
        }
        new_grades.append(grade_row)
        counters["graded"] += 1
        already_ids.add(sid)
        already_keys.add(key_tuple)

        if args.debug and len(debug_samples) < DEBUG_LIMIT:
            debug_samples.append((row, canonical_key, result, f"graded:{outcome}"))

    all_grades = existing_grades + new_grades
    write_jsonl(GRADES_PATH, all_grades)

    if args.debug:
        print("=== Debug join samples (up to first 20) ===")
        for sig, ck, res, note in debug_samples:
            derived_day = normalize_day_key_from_signal(sig)
            d_away, d_home = derive_teams_from_signal(sig)
            print(
                f"{note} | key={ck} | derived day={derived_day} teams={d_away}@{d_home} mkt={sig.get('market_type')} sel={sig.get('selection')} line={sig.get('line_median') or sig.get('line')}"
            )
            if res:
                print(f"  result teams={res.get('away_team')}@{res.get('home_team')} score {res.get('away_score')}-{res.get('home_score')}")
        # overlap audit
        signal_keys = {extract_signal_game(sig) for sig in signals if extract_signal_game(sig) is not None}
        signal_keys = {k for k in signal_keys if k}
        result_keys = set(results.keys())
        overlap = signal_keys & result_keys
        print(f"Signal canonical keys: {len(signal_keys)} | Result keys: {len(result_keys)} | Overlap: {len(overlap)}")
        missing = list(signal_keys - result_keys)
        if missing:
            print("First 10 missing keys:", missing[:10])
        print("Skipped summary:", {k: counters[k] for k in ["skipped_non_standard_market", "skipped_conflict_signal", "skipped_missing_canonical_key", "skipped_no_result_for_key"]})
        if missing_result_keys:
            print("First missing_no_result keys:", missing_result_keys)
    print(
        "Grade summary:",
        dict(
            attempted=counters["attempted"],
            graded=counters["graded"],
            skipped_no_game=counters["skipped_no_game"],
            skipped_missing_fields=counters["skipped_missing_fields"],
            already_graded=counters["already_graded"],
        ),
    )
    print(f"Added {len(new_grades)} new grades. Total stored: {len(all_grades)}")


if __name__ == "__main__":
    main()
