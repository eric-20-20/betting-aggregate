"""Build a single, analysis-ready research dataset for NBA signals.

Inputs (all JSONL):
  - data/ledger/signals_latest.jsonl
  - data/ledger/signals_occurrences.jsonl
  - data/ledger/grades_latest.jsonl
  - data/ledger/grades_occurrences.jsonl  (not directly merged, only available if needed later)

Output:
  - data/analysis/graded_signals_latest.jsonl   (one row per signal_id)

Usage:
    python3 scripts/build_research_dataset_nba.py
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


SIGNALS_LATEST_PATH = Path("data/ledger/signals_latest.jsonl")
SIGNALS_OCC_PATH = Path("data/ledger/signals_occurrences.jsonl")
GRADES_LATEST_PATH = Path("data/ledger/grades_latest.jsonl")
OUTPUT_PATH = Path("data/analysis/graded_signals_latest.jsonl")


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


def derive_teams(signal: Dict[str, Any], grade: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    for src in (signal, grade):
        cgk = src.get("canonical_game_key")
        if isinstance(cgk, (list, tuple)) and len(cgk) >= 4 and cgk[2] and cgk[3]:
            return str(cgk[2]).upper(), str(cgk[3]).upper()
        away = src.get("away_team")
        home = src.get("home_team")
        if away and home:
            return str(away).upper(), str(home).upper()

    def parse_pair(val: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        if not isinstance(val, str):
            return None, None
        if "@" in val:
            a, h = val.split("@", 1)
            return a.upper(), h.upper()
        if "-" in val:
            a, h = val.split("-", 1)
            return a.upper(), h.upper()
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


def build_rows() -> List[Dict[str, Any]]:
    signals = read_jsonl(SIGNALS_LATEST_PATH)
    grades = read_jsonl(GRADES_LATEST_PATH)
    occs = read_jsonl(SIGNALS_OCC_PATH)

    signals_by_id: Dict[str, Dict[str, Any]] = {}
    for row in signals:
        sid = row.get("signal_id")
        if sid:
            signals_by_id[sid] = row

    grades_by_id: Dict[str, Dict[str, Any]] = {}
    for row in grades:
        sid = row.get("signal_id")
        if sid:
            grades_by_id[sid] = row

    occ_by_id: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in occs:
        sid = row.get("signal_id")
        if sid:
            occ_by_id[sid].append(row)

    all_ids = set(signals_by_id) | set(grades_by_id)
    rows: List[Dict[str, Any]] = []

    for sid in sorted(all_ids):
        sig = signals_by_id.get(sid, {})
        grade = grades_by_id.get(sid, {})

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
    rows = build_rows()
    write_jsonl(OUTPUT_PATH, rows)
    print(f"Wrote {len(rows)} rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
