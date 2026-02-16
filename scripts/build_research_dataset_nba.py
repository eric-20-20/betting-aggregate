"""Build a single, analysis-ready research dataset for NBA signals.

Inputs (all JSONL):
  - data/ledger/signals_latest.jsonl (or signals_occurrences.jsonl)
  - data/ledger/grades_latest.jsonl (or grades_occurrences.jsonl)

Output:
  - data/analysis/graded_signals_latest.jsonl   (one row per signal_id)

Usage:
    # Latest (deduplicated) signals
    python3 scripts/build_research_dataset_nba.py

    # All occurrences (for full history analysis)
    python3 scripts/build_research_dataset_nba.py \
      --signals data/ledger/signals_occurrences.jsonl \
      --grades data/ledger/grades_occurrences.jsonl \
      --out data/analysis/graded_signals_occurrences.jsonl
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


DEFAULT_SIGNALS_PATH = Path("data/ledger/signals_latest.jsonl")
DEFAULT_SIGNALS_OCC_PATH = Path("data/ledger/signals_occurrences.jsonl")
DEFAULT_GRADES_PATH = Path("data/ledger/grades_latest.jsonl")
DEFAULT_OUTPUT_PATH = Path("data/analysis/graded_signals_latest.jsonl")


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


def extract_stat_key(signal: Dict[str, Any]) -> Optional[str]:
    stat = signal.get("atomic_stat")
    if isinstance(stat, str) and stat:
        return stat
    sel = signal.get("selection")
    if isinstance(sel, str) and "::" in sel:
        parts = sel.split("::")
        if len(parts) >= 3:
            return parts[1]
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


def pick_best_odds(signal: Dict[str, Any]) -> Optional[float]:
    if signal.get("best_odds") is not None:
        try:
            return float(signal.get("best_odds"))
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
    if len(day_key) == 10 and day_key[4] == "-" and day_key[7] == "-":
        return day_key
    if day_key.startswith("NBA:"):
        parts = day_key.split(":")
        if len(parts) >= 4 and all(p.isdigit() for p in parts[1:4]):
            return f"{parts[1]}-{parts[2]}-{parts[3]}"
    return None


def event_key_to_date_str(event_key: Optional[str]) -> Optional[str]:
    if not isinstance(event_key, str):
        return None
    if event_key.startswith("NBA:"):
        parts = event_key.split(":")
        if len(parts) >= 4 and all(p.isdigit() for p in parts[1:4]):
            return f"{parts[1]}-{parts[2]}-{parts[3]}"
    return None


def derive_day_key(signal: Dict[str, Any], grade: Dict[str, Any]) -> Optional[str]:
    if signal.get("day_key"):
        return signal["day_key"]
    cgk = signal.get("canonical_game_key")
    if isinstance(cgk, (list, tuple)) and len(cgk) >= 2 and cgk[1]:
        return str(cgk[1])
    if grade.get("day_key"):
        return grade["day_key"]
    return None


def is_valid_team_code(value: Optional[str]) -> bool:
    """
    Check if value is a valid NBA team tricode.

    Valid tricodes are 2-4 uppercase letters (e.g., LAL, DEN, NOP).
    Rejects event_key-like values (containing : or @).
    """
    if not value or not isinstance(value, str):
        return False
    val = value.strip().upper()
    # Reject if contains event_key-like characters
    if ":" in val or "@" in val:
        return False
    # Must be 2-4 letters
    import re
    if not re.match(r"^[A-Z]{2,4}$", val):
        return False
    return True


def derive_teams(signal: Dict[str, Any], grade: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    def validate_pair(away: Optional[str], home: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        """Return team pair only if both are valid tricodes."""
        if away and home:
            away_clean = str(away).upper()
            home_clean = str(home).upper()
            if is_valid_team_code(away_clean) and is_valid_team_code(home_clean):
                return away_clean, home_clean
        return None, None

    for src in (signal, grade):
        cgk = src.get("canonical_game_key")
        if isinstance(cgk, (list, tuple)) and len(cgk) >= 4 and cgk[2] and cgk[3]:
            away, home = validate_pair(str(cgk[2]), str(cgk[3]))
            if away and home:
                return away, home
        away = src.get("away_team")
        home = src.get("home_team")
        away_clean, home_clean = validate_pair(away, home)
        if away_clean and home_clean:
            return away_clean, home_clean

    def parse_pair(val: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        if not isinstance(val, str):
            return None, None
        # Extract team portion from event_key-like strings
        if ":" in val:
            # Get last segment after colons
            val = val.split(":")[-1]
        if "@" in val:
            a, h = val.split("@", 1)
            return validate_pair(a, h)
        if "-" in val:
            a, h = val.split("-", 1)
            return validate_pair(a, h)
        return None, None

    for src in (grade, signal):
        ek_away, ek_home = parse_pair(src.get("event_key"))
        if ek_away and ek_home:
            return ek_away, ek_home
        mk_away, mk_home = parse_pair(src.get("matchup_key"))
        if mk_away and mk_home:
            return mk_away, mk_home
        mu_away, mu_home = parse_pair(src.get("matchup"))
        if mu_away and mu_home:
            return mu_away, mu_home
    return None, None


def collect_urls_sample(primary: Dict[str, Any], secondary: Optional[Dict[str, Any]] = None, limit: int = 5) -> List[str]:
    urls: List[str] = []
    seen = set()
    for src in (primary, secondary or {}):
        for u in src.get("urls") or []:
            if isinstance(u, str) and u not in seen:
                seen.add(u)
                urls.append(u)
            if len(urls) >= limit:
                return urls
    for src in (primary, secondary or {}):
        for sup in src.get("supports") or []:
            cu = sup.get("canonical_url") or sup.get("url")
            if isinstance(cu, str) and cu not in seen:
                seen.add(cu)
                urls.append(cu)
            if len(urls) >= limit:
                break
        if len(urls) >= limit:
            break
    return urls


def coerce_float(val: Any) -> Optional[float]:
    try:
        if val is None:
            return None
        return float(val)
    except Exception:
        return None


def compute_line_dispersion(line_min: Any, line_max: Any) -> Optional[float]:
    lmin = coerce_float(line_min)
    lmax = coerce_float(line_max)
    if lmin is None or lmax is None:
        return None
    return round(lmax - lmin, 3)


def score_bucket_and_order(score: Any) -> Tuple[str, int]:
    try:
        val = float(score)
    except (TypeError, ValueError):
        return "NO_SCORE", 0
    if val < 30:
        return "<30", 10
    if val < 45:
        return "30-44", 20
    if val < 60:
        return "45-59", 30
    if val < 75:
        return "60-74", 40
    return "75+", 50


def occurrence_extremes(occurrences: List[Dict[str, Any]]) -> Dict[str, Optional[str]]:
    if not occurrences:
        return {
            "observed_at_first_utc": None,
            "observed_at_latest_utc": None,
            "run_id_first_seen": None,
            "run_id_latest_seen": None,
        }

    def key_fn(row: Dict[str, Any]) -> Tuple[int, Any]:
        ts = parse_iso(row.get("observed_at_utc"))
        if ts:
            return (0, ts)
        return (1, row.get("observed_at_utc") or "")

    first = min(occurrences, key=key_fn)
    last = max(occurrences, key=key_fn)
    return {
        "observed_at_first_utc": first.get("observed_at_utc"),
        "observed_at_latest_utc": last.get("observed_at_utc"),
        "run_id_first_seen": first.get("run_id"),
        "run_id_latest_seen": last.get("run_id"),
    }


def resolve_grade_status(grade: Optional[Dict[str, Any]], signal: Dict[str, Any]) -> Tuple[str, Optional[str], Optional[float]]:
    if not grade:
        return "UNGRADED", None, None
    status = grade.get("status")
    if status == "INELIGIBLE":
        return "INELIGIBLE", None, None
    if status in {"WIN", "LOSS", "PUSH"}:
        odds = grade.get("best_odds")
        if odds is None:
            odds = pick_best_odds(signal)
        units = grade.get("units")
        if units is None:
            units = american_odds_to_units(odds, status) if status else None
        return "GRADED", status, units
    if status == "PENDING":
        return "PENDING", None, None
    if status == "ERROR":
        return "ERROR", None, None
    return "UNGRADED", None, None


def pick_best_grade(grades_list: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Pick the best grade row from a list of grades for the same signal_id.

    Priority:
    1. Terminal outcomes (WIN, LOSS, PUSH) - prefer these
    2. INELIGIBLE - acceptable if no terminal
    3. Among eligible rows, pick the latest by graded_at_utc
    """
    if not grades_list:
        return None

    TERMINAL = {"WIN", "LOSS", "PUSH"}

    def grade_priority(g: Dict[str, Any]) -> Tuple[int, datetime]:
        status = g.get("status") or ""
        ts = parse_iso(g.get("graded_at_utc")) or datetime.min
        if status in TERMINAL:
            return (0, ts)  # highest priority
        if status == "INELIGIBLE":
            return (1, ts)
        return (2, ts)  # lowest priority (PENDING, ERROR, etc.)

    # Sort by priority (lower is better), then by timestamp descending (latest first)
    sorted_grades = sorted(
        grades_list,
        key=lambda g: (grade_priority(g)[0], -grade_priority(g)[1].timestamp() if grade_priority(g)[1] != datetime.min else 0)
    )

    # Return the best one (first after sorting)
    best = sorted_grades[0] if sorted_grades else None

    # Only return if it has a usable status
    if best:
        status = best.get("status")
        if status in TERMINAL or status == "INELIGIBLE":
            return best

    return None


def build_rows(
    signals_path: Path,
    grades_path: Path,
    occurrences_path: Optional[Path] = None,
) -> List[Dict[str, Any]]:
    signals = read_jsonl(signals_path)
    grades = read_jsonl(grades_path)
    occs = read_jsonl(occurrences_path) if occurrences_path else []

    # Group all grades by signal_id (there may be multiple per signal_id)
    grades_by_id: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in grades:
        sid = row.get("signal_id")
        if sid:
            grades_by_id[sid].append(row)

    # Pre-compute the best grade for each signal_id
    best_grade_by_id: Dict[str, Optional[Dict[str, Any]]] = {}
    for sid, grade_list in grades_by_id.items():
        best_grade_by_id[sid] = pick_best_grade(grade_list)

    # Group occurrences by signal_id for metadata
    occ_by_id: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in occs:
        sid = row.get("signal_id")
        if sid:
            occ_by_id[sid].append(row)

    rows: List[Dict[str, Any]] = []

    # Iterate over each signal row (not unique ids)
    # This produces one output row per signal occurrence
    for sig in signals:
        sid = sig.get("signal_id")
        if not sid:
            continue

        # Get the best grade for this signal_id (same grade used for all occurrences of this signal_id)
        grade = best_grade_by_id.get(sid) or {}

        day_key = derive_day_key(sig, grade)
        date_str = day_key_to_date_str(day_key) or event_key_to_date_str(grade.get("event_key") or sig.get("event_key"))
        away_team, home_team = derive_teams(sig, grade)
        event_key_raw = grade.get("event_key_raw") or grade.get("event_key") or sig.get("event_key")
        event_key_safe = (
            grade.get("event_key_safe")
            or (
                f"NBA:{date_str.replace('-', ':')}:{away_team}@{home_team}"
                if date_str and away_team and home_team
                else None
            )
        )
        canonical_event_key = (
            grade.get("canonical_event_key")
            or sig.get("canonical_event_key")
            or event_key_safe
        )

        grade_status, result, units = resolve_grade_status(grade or None, sig)

        odds_val = pick_best_odds(sig)
        if result and units is None:
            units = american_odds_to_units(odds_val, result)

        meta: Dict[str, Any] = {}
        if grade.get("games_info"):
            meta["games_info"] = grade["games_info"]
        if grade.get("player_meta"):
            meta["player_meta"] = grade["player_meta"]

        occ_fields = occurrence_extremes(occ_by_id.get(sid, []))
        stat_source: Dict[str, Any] = {}
        stat_source.update(grade)
        stat_source.update(sig)

        score_bucket, score_bucket_order = score_bucket_and_order(sig.get("score"))
        line_dispersion = compute_line_dispersion(sig.get("line_min"), sig.get("line_max"))

        odds_list_val = sig.get("odds_list") or sig.get("odds")
        odds_list_nonempty = isinstance(odds_list_val, list) and len(odds_list_val) > 0
        has_odds = (odds_val is not None) or odds_list_nonempty
        if not has_odds:
            odds_missing_reason = "missing_best_odds_and_odds_list"
        elif odds_val is None and odds_list_nonempty:
            odds_missing_reason = "missing_best_odds"
        else:
            odds_missing_reason = ""

        row = {
            "atomic_stat": sig.get("atomic_stat"),
            "away_team": away_team,
            "best_odds": odds_val,
            "canonical_game_key": sig.get("canonical_game_key"),
            "count_total": sig.get("count_total"),
            "day_key": day_key,
            "derived_date": date_str,
            "direction": sig.get("direction"),
            "canonical_event_key": canonical_event_key,
            "event_key_raw": event_key_raw,
            "event_key_safe": event_key_safe,
            "expert_strength": sig.get("expert_strength"),
            "experts": sig.get("experts") or [],
            "grade_status": grade_status,
            "has_all_sources": sig.get("has_all_sources"),
            "has_odds": has_odds,
            "home_team": home_team,
            "line": pick_line(sig),
            "line_max": sig.get("line_max"),
            "line_median": sig.get("line_median"),
            "line_min": sig.get("line_min"),
            "line_dispersion": line_dispersion,
            "market_type": sig.get("market_type") or grade.get("market_type"),
            "missing_fields": grade.get("missing_fields") or [],
            "notes": grade.get("notes") or "",
            "observed_at_first_utc": occ_fields["observed_at_first_utc"],
            "observed_at_latest_utc": occ_fields["observed_at_latest_utc"],
            "odds_list": odds_list_val,
            "odds_missing_reason": odds_missing_reason,
            "player_id": sig.get("player_id"),
            "result": result,
            "run_id_first_seen": occ_fields["run_id_first_seen"],
            "run_id_latest_seen": occ_fields["run_id_latest_seen"],
            "score": sig.get("score"),
            "score_bucket": score_bucket,
            "score_bucket_order": score_bucket_order,
            "selection": sig.get("selection") or grade.get("selection"),
            "signal_id": sid,
            "signal_type": sig.get("signal_type"),
            "source_strength": sig.get("source_strength"),
            "sources": sig.get("sources") or [],
            "sources_combo": sig.get("sources_combo"),
            "sources_present": sig.get("sources_present") or [],
            "stat_key": extract_stat_key(stat_source),
            "units": units,
            "urls_sample": collect_urls_sample(sig, grade),
            "supports_count": len(sig.get("supports") or []),
            "weighted_expert_strength": sig.get("weighted_expert_strength"),
        }

        if meta:
            row["provider_meta"] = meta

        rows.append(row)

    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description="Build research dataset from signals + grades.")
    ap.add_argument(
        "--signals",
        type=str,
        default=str(DEFAULT_SIGNALS_PATH),
        help="Signals JSONL input (default: signals_latest.jsonl). "
             "Use signals_occurrences.jsonl for full history analysis.",
    )
    ap.add_argument(
        "--grades",
        type=str,
        default=str(DEFAULT_GRADES_PATH),
        help="Grades JSONL input (default: grades_latest.jsonl). "
             "Use grades_occurrences.jsonl for full history analysis.",
    )
    ap.add_argument(
        "--out",
        type=str,
        default=str(DEFAULT_OUTPUT_PATH),
        help="Output JSONL path (default: data/analysis/graded_signals_latest.jsonl).",
    )
    ap.add_argument(
        "--occurrences",
        type=str,
        default=None,
        help="Optional: signals_occurrences.jsonl for occurrence metadata (first/latest timestamps).",
    )
    args = ap.parse_args()

    signals_path = Path(args.signals)
    grades_path = Path(args.grades)
    output_path = Path(args.out)
    occurrences_path = Path(args.occurrences) if args.occurrences else None

    # Auto-detect occurrences file if using latest signals
    if occurrences_path is None and signals_path == DEFAULT_SIGNALS_PATH:
        if DEFAULT_SIGNALS_OCC_PATH.exists():
            occurrences_path = DEFAULT_SIGNALS_OCC_PATH

    print(f"[dataset] Signals: {signals_path}")
    print(f"[dataset] Grades: {grades_path}")
    if occurrences_path:
        print(f"[dataset] Occurrences: {occurrences_path}")
    print(f"[dataset] Output: {output_path}")

    rows = build_rows(signals_path, grades_path, occurrences_path)
    write_jsonl(output_path, rows)
    print(f"[dataset] Wrote {len(rows)} rows to {output_path}")

    # Sanity check: print grade status distribution
    grade_status_counts = Counter(r.get("grade_status") for r in rows)
    result_counts = Counter(r.get("result") for r in rows if r.get("result"))

    total = len(rows)
    graded = sum(1 for r in rows if r.get("grade_status") == "GRADED")
    ungraded = sum(1 for r in rows if r.get("grade_status") == "UNGRADED")

    print(f"[dataset] Summary: total={total}, graded={graded}, ungraded={ungraded}")
    print(f"[dataset] By grade_status: {dict(grade_status_counts)}")
    print(f"[dataset] By result (WIN/LOSS/PUSH): {dict(result_counts)}")


if __name__ == "__main__":
    main()
