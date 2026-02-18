"""Build occurrence-level research dataset for NBA picks.

Inputs (JSONL):
  - data/ledger/signals_occurrences.jsonl
  - data/ledger/grades_latest.jsonl (or grades_occurrences.jsonl)

Output:
  - data/analysis/graded_occurrences_latest.jsonl   (one row per pick occurrence)

Usage:
    python3 scripts/build_research_dataset_nba_occurrences.py
    python3 scripts/build_research_dataset_nba_occurrences.py --signals data/ledger/signals_occurrences.jsonl --grades data/ledger/grades_occurrences.jsonl --out data/analysis/graded_occurrences_latest.jsonl
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


DEFAULT_SIGNALS_PATH = Path("data/ledger/signals_occurrences.jsonl")
DEFAULT_GRADES_PATH = Path("data/ledger/grades_latest.jsonl")
DEFAULT_OUTPUT_PATH = Path("data/analysis/graded_occurrences_latest.jsonl")


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


def parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts or not isinstance(ts, str):
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def american_odds_to_units(odds: Optional[Any], outcome: str) -> Optional[float]:
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


def pick_best_odds(row: Dict[str, Any]) -> Optional[float]:
    if row.get("best_odds") is not None:
        try:
            return float(row.get("best_odds"))
        except Exception:
            pass
    odds_list = row.get("odds_list") or row.get("odds") or []
    if isinstance(odds_list, list) and odds_list:
        try:
            return float(odds_list[0])
        except Exception:
            return None
    return None


def best_grade(grades: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Pick best grade per signal_id.

    Priority:
        1) WIN/LOSS/PUSH
        2) INELIGIBLE
        3) ERROR
        4) PENDING/other
    Ties: latest graded_at_utc wins.
    """
    if not grades:
        return None

    TERMINAL = {"WIN", "LOSS", "PUSH"}

    def priority(g: Dict[str, Any]) -> Tuple[int, float]:
        status = (g.get("status") or "").upper()
        ts = parse_iso(g.get("graded_at_utc")) or datetime.min
        if status in TERMINAL:
            return (0, ts.timestamp())
        if status == "INELIGIBLE":
            return (1, ts.timestamp())
        if status == "ERROR":
            return (2, ts.timestamp())
        return (3, ts.timestamp())

    # Lower priority value is better; for ties pick latest timestamp
    sorted_grades = sorted(grades, key=lambda g: (priority(g)[0], -priority(g)[1]))
    return sorted_grades[0]


def grade_status_and_result(grade: Optional[Dict[str, Any]]) -> Tuple[str, Optional[str]]:
    if not grade:
        return "UNGRADED", None
    status = (grade.get("status") or "").upper()
    result = (grade.get("result") or status or "").upper() or None
    if result in {"WIN", "LOSS", "PUSH"}:
        return "GRADED", result
    if status == "INELIGIBLE":
        return "INELIGIBLE", None
    if status == "ERROR":
        return "ERROR", None
    return "UNGRADED", None


def odds_for_units(occurrence: Dict[str, Any], grade: Optional[Dict[str, Any]]) -> Optional[float]:
    for row in (occurrence, grade or {}):
        if row is None:
            continue
        if row.get("best_odds") is not None:
            try:
                return float(row.get("best_odds"))
            except Exception:
                pass
        odds_val = row.get("odds_list") or row.get("odds")
        if odds_val is None:
            continue
        if isinstance(odds_val, list) and odds_val:
            try:
                return float(odds_val[0])
            except Exception:
                continue
        # Handle scalar odds (int or string like "-110")
        if isinstance(odds_val, (int, float)):
            return float(odds_val)
        if isinstance(odds_val, str):
            try:
                return float(odds_val)
            except (TypeError, ValueError):
                continue
    return None


def normalize_occ_source_id(raw: Any) -> str:
    if not isinstance(raw, str):
        return "unknown"
    val = raw.strip().lower()
    if not val:
        return "unknown"
    if val == "actionnetwork":
        return "action"
    if val in {"action", "covers", "betql", "sportscapping"}:
        return val
    return val


def _safe_float(x: Any) -> Optional[float]:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _collect_support_lines(row: Dict[str, Any]) -> List[float]:
    lines: List[float] = []
    for sup in row.get("supports") or []:
        if not isinstance(sup, dict):
            continue
        val = _safe_float(sup.get("line"))
        if val is not None:
            lines.append(val)
    return lines


def _median(vals: List[float]) -> Optional[float]:
    if not vals:
        return None
    sorted_vals = sorted(vals)
    n = len(sorted_vals)
    mid = n // 2
    if n % 2 == 1:
        return sorted_vals[mid]
    return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2.0


def derive_spread_fields(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Standardize spread picks into explicit fields and capture data-quality reasons.
    """
    market = (row.get("market_type") or "").strip().lower()
    if market != "spread":
        return {
            "spread_team": None,
            "spread_side": None,
            "spread_line": None,
            "spread_pick": None,
            "spread_is_valid": False,
            "spread_invalid_reason": None,
        }

    raw_selection = row.get("selection")
    selection = raw_selection.strip().upper() if isinstance(raw_selection, str) else None
    away_team = row.get("away_team")
    away = away_team.strip().upper() if isinstance(away_team, str) else None
    home_team = row.get("home_team")
    home = home_team.strip().upper() if isinstance(home_team, str) else None

    # Parse line
    spread_line: Optional[float] = None
    line_reason: Optional[str] = None
    if row.get("line") is None:
        line_reason = "missing_line"
    else:
        spread_line = _safe_float(row.get("line"))
        if spread_line is None:
            line_reason = "missing_line"

    # Determine team/side
    # Normalize selection by stripping common suffixes like _spread
    normalized_selection = selection
    if selection and selection.endswith("_spread"):
        normalized_selection = selection[:-7]  # Remove "_spread" suffix

    spread_team: Optional[str] = None
    spread_side: Optional[str] = None
    team_reason: Optional[str] = None
    if normalized_selection and away and normalized_selection == away:
        spread_team = away
        spread_side = "AWAY"
    elif normalized_selection and home and normalized_selection == home:
        spread_team = home
        spread_side = "HOME"
    else:
        team_reason = "team_only_selection_or_missing_teams"

    invalid_reason = line_reason or team_reason
    spread_is_valid = invalid_reason is None

    spread_pick: Optional[str] = None
    if spread_is_valid and spread_team and spread_line is not None:
        # format with explicit sign on positive numbers
        spread_pick = f"{spread_team} {spread_line:+g}"

    return {
        "spread_team": spread_team,
        "spread_side": spread_side,
        "spread_line": spread_line,
        "spread_pick": spread_pick,
        "spread_is_valid": spread_is_valid,
        "spread_invalid_reason": invalid_reason,
    }


def build_rows(signals_path: Path, grades_path: Path) -> List[Dict[str, Any]]:
    signals = read_jsonl(signals_path)
    grades = read_jsonl(grades_path)

    grades_by_id: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for g in grades:
        sid = g.get("signal_id")
        if sid:
            grades_by_id[sid].append(g)

    best_grade_by_id: Dict[str, Optional[Dict[str, Any]]] = {}
    for sid, glist in grades_by_id.items():
        best_grade_by_id[sid] = best_grade(glist)

    # Deduplicate by signal_id, keeping the LATEST occurrence
    # This prevents the same signal from appearing multiple times when
    # it was observed in multiple consensus runs
    latest_by_signal: Dict[str, Dict[str, Any]] = {}
    occurrence_counts: Dict[str, int] = Counter()
    for occ in signals:
        sid = occ.get("signal_id")
        if not sid:
            continue
        occurrence_counts[sid] += 1
        ts = parse_iso(occ.get("observed_at_utc"))
        existing = latest_by_signal.get(sid)
        if existing is None:
            latest_by_signal[sid] = occ
        else:
            existing_ts = parse_iso(existing.get("observed_at_utc"))
            if ts and existing_ts and ts > existing_ts:
                latest_by_signal[sid] = occ

    print(f"[occ] Deduplicated: {len(signals)} occurrences -> {len(latest_by_signal)} unique signals")

    out_rows: List[Dict[str, Any]] = []

    for occ in latest_by_signal.values():
        sid = occ.get("signal_id")
        if not sid:
            continue
        grade = best_grade_by_id.get(sid) or {}
        grade_status, result = grade_status_and_result(grade)

        odds_val = odds_for_units(occ, grade)
        units = grade.get("units")
        if grade_status == "GRADED" and units is None and result:
            units = american_odds_to_units(odds_val, result)

        occ_source_id = normalize_occ_source_id(occ.get("source_id") or occ.get("source_surface"))
        occ_sources_present = [occ_source_id] if occ_source_id and occ_source_id != "unknown" else []
        occ_sources_combo = occ_source_id if occ_source_id else "unknown"

        signal_sources_present = []
        if isinstance(occ.get("sources_present"), list) and occ.get("sources_present"):
            signal_sources_present = [s for s in occ.get("sources_present") if s]
        signal_sources_combo = "|".join(sorted(set(signal_sources_present))) if signal_sources_present else "unknown"

        row: Dict[str, Any] = {
            "occurrence_id": occ.get("occurrence_id"),
            "signal_id": sid,
            "source_id": occ.get("source_id"),
            "source_surface": occ.get("source_surface"),
            "experts": occ.get("experts") or [],
            "expert": occ.get("expert") or occ.get("expert_name"),
            "expert_name": occ.get("expert_name"),
            "expert_id": occ.get("expert_id"),
            "occ_source_id": occ_source_id,
            "occ_sources_present": occ_sources_present,
            "occ_sources_combo": occ_sources_combo,
            "sources_present": signal_sources_present,
            "sources_combo": signal_sources_combo,
            "signal_sources_present": signal_sources_present,
            "signal_sources_combo": signal_sources_combo,
            "market_type": occ.get("market_type"),
            "selection": occ.get("selection") or grade.get("selection"),
            "selection_key": occ.get("selection_key"),
            "line": occ.get("line"),
            "line_median": occ.get("line_median"),
            "direction": occ.get("direction"),
            "away_team": occ.get("away_team") or grade.get("away_team"),
            "home_team": occ.get("home_team") or grade.get("home_team"),
            "player_key": occ.get("player_key"),
            "player_id": occ.get("player_id"),
            "atomic_stat": occ.get("atomic_stat"),
            "day_key": occ.get("day_key") or grade.get("day_key"),
            "event_key": occ.get("event_key") or grade.get("event_key"),
            "observed_at_utc": occ.get("observed_at_utc"),
            "urls": occ.get("urls") or [],
            "canonical_url": occ.get("canonical_url"),
            "score": occ.get("score"),
            "signal_type": occ.get("signal_type"),
            "supports": occ.get("supports") or [],
            "has_all_sources": occ.get("has_all_sources"),
            "count_total": occ.get("count_total"),
            "grade_status": grade_status,
            "status": grade.get("status"),
            "result": result,
            "units": units,
            "notes": grade.get("notes"),
            "provider": grade.get("provider"),
            "games_info": grade.get("games_info"),
            "player_meta": grade.get("player_meta"),
            "best_odds": occ.get("best_odds"),
            "final_home_score": grade.get("final_home_score"),
            "final_away_score": grade.get("final_away_score"),
            "grade_best_odds": grade.get("best_odds"),
            "odds_for_units": odds_val,
            "roi_eligible": grade_status == "GRADED" and odds_val is not None,
            "occurrence_count": occurrence_counts.get(sid, 1),  # How many times this signal was observed
        }

        # Carry over odds_list for reference if present
        if occ.get("odds_list") is not None:
            row["odds_list"] = occ.get("odds_list")
        elif grade.get("odds_list") is not None:
            row["odds_list"] = grade.get("odds_list")

        market_lower = (row.get("market_type") or "").strip().lower()
        if market_lower == "spread":
            if row.get("line") is None:
                support_lines = _collect_support_lines(row)
                if support_lines:
                    line_min = min(support_lines)
                    line_max = max(support_lines)
                    line_median = _median(support_lines)
                    promoted_line = line_median
                    row["line"] = row.get("line") if row.get("line") is not None else promoted_line
                    if row.get("line_min") is None:
                        row["line_min"] = line_min
                    if row.get("line_max") is None:
                        row["line_max"] = line_max
                    if row.get("line_median") is None:
                        row["line_median"] = line_median
                    row["spread_line_recovered"] = True
                    existing_notes = row.get("notes") or ""
                    if existing_notes:
                        if "line_promoted_from_supports" not in existing_notes:
                            row["notes"] = f"{existing_notes};line_promoted_from_supports"
                    else:
                        row["notes"] = "line_promoted_from_supports"
                else:
                    row["spread_line_recovered"] = False
            else:
                if row.get("line_median") is None:
                    line_val = _safe_float(row.get("line"))
                    if line_val is not None:
                        row["line_median"] = line_val
                row["spread_line_recovered"] = False

        spread_fields = derive_spread_fields(row)
        row.update(spread_fields)

        if (row.get("market_type") or "").strip().lower() == "spread" and not row.get("spread_is_valid"):
            row["grade_status"] = "INELIGIBLE"
            row["result"] = None
            row["status"] = "INELIGIBLE"
            row["notes"] = row.get("spread_invalid_reason") or row.get("notes")
            row["roi_eligible"] = False

        # Normalize final scores and compute audit fields
        home_score_raw = row.get("final_home_score")
        away_score_raw = row.get("final_away_score")
        if home_score_raw is None:
            for key in ("home_score", "home_team_score"):
                if grade.get(key) is not None:
                    home_score_raw = grade.get(key)
                    break
        if away_score_raw is None:
            for key in ("away_score", "away_team_score"):
                if grade.get(key) is not None:
                    away_score_raw = grade.get(key)
                    break

        home_score = _safe_float(home_score_raw)
        away_score = _safe_float(away_score_raw)
        if home_score is not None:
            row["final_home_score"] = int(home_score) if float(home_score).is_integer() else home_score
        if away_score is not None:
            row["final_away_score"] = int(away_score) if float(away_score).is_integer() else away_score

        if home_score is not None and away_score is not None:
            row["final_total_points"] = home_score + away_score
            row["final_margin_home_minus_away"] = home_score - away_score

        spread_result_mismatch = False
        if (
            market_lower == "spread"
            and row.get("spread_is_valid")
            and row.get("grade_status") == "GRADED"
            and row.get("result") in {"WIN", "LOSS", "PUSH"}
        ):
            spread_line = _safe_float(row.get("spread_line"))
            if spread_line is None:
                spread_line = _safe_float(row.get("line"))
            if spread_line is not None and home_score is not None and away_score is not None:
                total_points = home_score + away_score
                row["spread_total_points"] = total_points
                if row.get("spread_side") == "AWAY":
                    margin = (away_score + spread_line) - home_score
                    audit_prefix = "away"
                    team_label = row.get("away_team")
                elif row.get("spread_side") == "HOME":
                    margin = (home_score + spread_line) - away_score
                    audit_prefix = "home"
                    team_label = row.get("home_team")
                else:
                    margin = None
                    audit_prefix = None
                    team_label = None

                if margin is not None:
                    row["spread_cover_margin"] = margin
                    computed_result = "WIN" if margin > 0 else "LOSS" if margin < 0 else "PUSH"
                    spread_pick = row.get("spread_pick")
                    if not spread_pick and team_label and spread_line is not None:
                        spread_pick = f"{team_label} {spread_line:+g}"
                    row["spread_audit"] = f"{spread_pick or 'spread'}: ({audit_prefix} {away_score if audit_prefix == 'away' else home_score} + {spread_line:+g}) - {'home' if audit_prefix == 'away' else 'away'} {home_score if audit_prefix == 'away' else away_score} = {margin:+g} => {computed_result}"
                    if computed_result != row.get("result"):
                        spread_result_mismatch = True
                        existing_notes = row.get("notes") or ""
                        if existing_notes:
                            if "spread_result_mismatch" not in existing_notes:
                                row["notes"] = f"{existing_notes};spread_result_mismatch"
                        else:
                            row["notes"] = "spread_result_mismatch"
                row["spread_result_mismatch"] = spread_result_mismatch
            else:
                row["spread_result_mismatch"] = False
        else:
            row["spread_result_mismatch"] = False

        out_rows.append(row)

    return out_rows


def main() -> None:
    ap = argparse.ArgumentParser(description="Build occurrence-level research dataset from signals_occurrences + grades.")
    ap.add_argument(
        "--signals",
        type=str,
        default=str(DEFAULT_SIGNALS_PATH),
        help="Signals occurrences JSONL input (default: signals_occurrences.jsonl).",
    )
    ap.add_argument(
        "--grades",
        type=str,
        default=str(DEFAULT_GRADES_PATH),
        help="Grades JSONL input (default: grades_latest.jsonl or grades_occurrences.jsonl).",
    )
    ap.add_argument(
        "--out",
        type=str,
        default=str(DEFAULT_OUTPUT_PATH),
        help="Output JSONL path (default: data/analysis/graded_occurrences_latest.jsonl).",
    )
    args = ap.parse_args()

    signals_path = Path(args.signals)
    grades_path = Path(args.grades)
    output_path = Path(args.out)

    print(f"[occ] Signals: {signals_path}")
    print(f"[occ] Grades: {grades_path}")
    print(f"[occ] Output: {output_path}")

    rows = build_rows(signals_path, grades_path)
    write_jsonl(output_path, rows)
    total = len(rows)
    graded = sum(1 for r in rows if r.get("grade_status") == "GRADED")
    graded_missing_sources_present = sum(
        1
        for r in rows
        if r.get("grade_status") == "GRADED" and not (isinstance(r.get("sources_present"), list) and len(r.get("sources_present")) > 0)
    )
    graded_occ_unknown = sum(1 for r in rows if r.get("grade_status") == "GRADED" and (r.get("occ_sources_combo") or "unknown") == "unknown")

    print(f"[occ] Wrote {total} rows ({graded} graded) to {output_path}")
    print(f"[occ] Graded rows missing/empty sources_present: {graded_missing_sources_present}")
    print(f"[occ] Graded rows with occ_sources_combo == 'unknown': {graded_occ_unknown}")

    status_counts: Dict[str, int] = defaultdict(int)
    result_counts: Dict[str, int] = defaultdict(int)
    occ_combo_counts: Dict[str, int] = defaultdict(int)
    signal_combo_counts: Dict[str, int] = defaultdict(int)
    spread_invalid_by_reason: Counter[str] = Counter()
    spread_total = 0
    spread_valid = 0
    spread_invalid = 0
    spread_line_recovered = 0
    spread_line_recovered_valid = 0
    spread_line_recovered_invalid = 0
    spread_graded_with_scores = 0
    spread_result_mismatch_count = 0
    for r in rows:
        status_counts[r.get("grade_status")] += 1
        if r.get("result"):
            result_counts[r.get("result")] += 1
        occ_combo_counts[r.get("occ_sources_combo") or "unknown"] += 1
        signal_combo_counts[r.get("signal_sources_combo") or "unknown"] += 1
        if (r.get("market_type") or "").strip().lower() == "spread":
            spread_total += 1
            if r.get("spread_is_valid"):
                spread_valid += 1
            else:
                spread_invalid += 1
                spread_invalid_by_reason[(r.get("spread_invalid_reason") or "unknown")] += 1
            if r.get("spread_line_recovered"):
                spread_line_recovered += 1
                if r.get("spread_is_valid"):
                    spread_line_recovered_valid += 1
                else:
                    spread_line_recovered_invalid += 1
            if (
                r.get("spread_is_valid")
                and r.get("grade_status") == "GRADED"
                and r.get("result") in {"WIN", "LOSS", "PUSH"}
                and r.get("final_home_score") is not None
                and r.get("final_away_score") is not None
            ):
                spread_graded_with_scores += 1
            if r.get("spread_result_mismatch"):
                spread_result_mismatch_count += 1
    print(f"[occ] By grade_status: {dict(status_counts)}")
    print(f"[occ] By result (WIN/LOSS/PUSH): {dict(result_counts)}")
    occ_top5 = sorted(occ_combo_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:5]
    sig_top5 = sorted(signal_combo_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:5]
    print(f"[occ] Top occ_sources_combo: {occ_top5}")
    print(f"[occ] Top signal_sources_combo: {sig_top5}")
    print(f"[occ] Spread rows: total={spread_total}, valid={spread_valid}, invalid={spread_invalid}")
    if spread_invalid_by_reason:
        print(f"[occ] Spread invalid reasons (top 10): {spread_invalid_by_reason.most_common(10)}")
    print(f"[occ] Spread line recovered: total={spread_line_recovered}, valid={spread_line_recovered_valid}, invalid={spread_line_recovered_invalid}")
    print(f"[occ] Spread graded with scores: {spread_graded_with_scores}")
    print(f"[occ] Spread result mismatches: {spread_result_mismatch_count}")


if __name__ == "__main__":
    main()
