"""Generate sliceable reports from the research dataset (NBA).

Inputs:
  - data/analysis/graded_signals_latest.jsonl

Outputs:
  - data/reports/coverage_summary.json
  - data/reports/by_sources_combo_market.json
  - data/reports/by_sources_combo_score_bucket.json
  - data/reports/by_expert.json

Run:
    python3 scripts/report_records.py
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


DATASET_PATH = Path("data/analysis/graded_signals_latest.jsonl")
REPORT_DIR = Path("data/reports")
COVERAGE_PATH = REPORT_DIR / "coverage_summary.json"
BY_SOURCES_MARKET_PATH = REPORT_DIR / "by_sources_combo_market.json"
BY_SCORE_BUCKET_PATH = REPORT_DIR / "by_sources_combo_score_bucket.json"
BY_EXPERT_PATH = REPORT_DIR / "by_expert.json"
BY_SCORE_BUCKET_SIMPLE_PATH = REPORT_DIR / "by_score_bucket.json"
BY_SCORE_BUCKET_SOURCES_PATH = REPORT_DIR / "by_score_bucket_sources_combo.json"
MIN_SAMPLE_SIZE_EXPERT = 10


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


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, sort_keys=True, indent=2)


def pick_odds(row: Dict[str, Any]) -> Optional[float]:
    if row.get("best_odds") is not None:
        try:
            return float(row.get("best_odds"))
        except Exception:
            pass
    odds_list = row.get("odds_list")
    if isinstance(odds_list, list) and odds_list:
        try:
            return float(odds_list[0])
        except Exception:
            return None
    return None


def bucket_score(score: Any) -> str:
    try:
        val = float(score)
    except (TypeError, ValueError):
        return "unknown"
    if val < 30:
        return "<30"
    if val < 45:
        return "30-44"
    if val < 60:
        return "45-59"
    if val < 75:
        return "60-74"
    return "75+"


def bucket_order(score_bucket: str) -> int:
    orders = {
        "NO_SCORE": 0,
        "<30": 10,
        "30-44": 20,
        "45-59": 30,
        "60-74": 40,
        "75+": 50,
        "unknown": 5,
    }
    return orders.get(score_bucket, 0)


def init_metrics() -> Dict[str, Any]:
    return {
        "bets_with_units": 0,
        "losses": 0,
        "net_units": 0.0,
        "n": 0,
        "odds_sum": 0.0,
        "odds_count": 0,
        "pushes": 0,
        "units_missing_count": 0,
        "wins": 0,
    }


def finalize_metrics(m: Dict[str, Any]) -> Dict[str, Any]:
    denom = m["wins"] + m["losses"]
    m["win_pct"] = (m["wins"] / denom) if denom > 0 else None
    m["roi"] = (m["net_units"] / m["bets_with_units"]) if m["bets_with_units"] > 0 else None
    m["avg_odds"] = (m["odds_sum"] / m["odds_count"]) if m["odds_count"] > 0 else None
    return m


def accumulate_group(rows: Iterable[Dict[str, Any]], key_fn) -> List[Dict[str, Any]]:
    agg: Dict[Any, Dict[str, Any]] = {}
    for row in rows:
        key = key_fn(row)
        if key not in agg:
            agg[key] = init_metrics()
        m = agg[key]
        m["n"] += 1

        result = row.get("result")
        if result == "WIN":
            m["wins"] += 1
        elif result == "LOSS":
            m["losses"] += 1
        elif result == "PUSH":
            m["pushes"] += 1

        units = row.get("units")
        if units is not None:
            try:
                m["net_units"] += float(units)
                m["bets_with_units"] += 1
            except Exception:
                m["units_missing_count"] += 1
        else:
            m["units_missing_count"] += 1

        odds = pick_odds(row)
        if odds is not None:
            m["odds_sum"] += odds
            m["odds_count"] += 1

    output: List[Dict[str, Any]] = []
    for key, metrics in agg.items():
        finalized = finalize_metrics(metrics)
        if isinstance(key, tuple):
            # unpack tuple keys into named fields
            if len(key) == 2:
                finalized["sources_combo"], finalized["market_type"] = key
            elif len(key) == 3:
                finalized["sources_combo"], finalized["score_bucket"], finalized["market_type"] = key
            else:
                finalized["key"] = key
        else:
            finalized["key"] = key
        output.append(finalized)
    return sorted(output, key=lambda r: (-r.get("n", 0), r.get("sources_combo", ""), r.get("market_type", "")))


def coverage_report(rows: List[Dict[str, Any]], meta: Dict[str, Any]) -> Dict[str, Any]:
    counts_status = Counter(r.get("grade_status") for r in rows)
    counts_market = Counter(r.get("market_type") for r in rows)

    ineligible_missing = Counter()
    for r in rows:
        if r.get("grade_status") == "INELIGIBLE":
            for mf in r.get("missing_fields") or []:
                ineligible_missing[mf] += 1

    error_notes = Counter()
    for r in rows:
        if r.get("grade_status") == "ERROR":
            note = (r.get("notes") or "").strip()
            if ";" in note:
                for part in note.split(";"):
                    if part:
                        error_notes[part] += 1
            elif note:
                error_notes[note] += 1

    return {
        "meta": meta,
        "grade_status_counts": dict(counts_status),
        "market_type_counts": dict(counts_market),
        "ineligible_missing_fields_top": ineligible_missing.most_common(10),
        "error_notes_top": error_notes.most_common(10),
    }


def by_sources_combo_market(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    graded = [r for r in rows if r.get("grade_status") == "GRADED"]
    return accumulate_group(
        graded,
        key_fn=lambda r: (r.get("sources_combo") or "unknown", r.get("market_type") or "unknown"),
    )


def by_sources_combo_score_bucket(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    graded = [r for r in rows if r.get("grade_status") == "GRADED"]
    return accumulate_group(
        graded,
        key_fn=lambda r: (
            r.get("sources_combo") or "unknown",
            bucket_score(r.get("score")),
            r.get("market_type") or "unknown",
        ),
    )


def by_expert(rows: List[Dict[str, Any]], min_sample: int) -> Dict[str, Any]:
    graded = [r for r in rows if r.get("grade_status") == "GRADED"]
    exploded: List[Dict[str, Any]] = []
    for r in graded:
        for expert in r.get("experts") or []:
            exploded.append({**r, "expert": expert})
    grouped = accumulate_group(exploded, key_fn=lambda r: r.get("expert") or "unknown")
    for g in grouped:
        if "key" in g and "expert" not in g:
            g["expert"] = g.pop("key")
    filtered = [g for g in grouped if g.get("n", 0) >= min_sample]
    return {"meta": {"min_sample_size": min_sample}, "rows": grouped, "rows_filtered": filtered}


def score_bucket_key(row: Dict[str, Any]) -> Tuple[str, int]:
    sb = row.get("score_bucket")
    if not sb:
        sb = bucket_score(row.get("score"))
    order = row.get("score_bucket_order")
    if order is None:
        order = bucket_order(sb)
    return sb, order


def aggregate_score_groups(rows: List[Dict[str, Any]], key_fn) -> List[Dict[str, Any]]:
    agg: Dict[Any, Dict[str, Any]] = {}
    for r in rows:
        key = key_fn(r)
        if key not in agg:
            agg[key] = {
                "n_total": 0,
                "n_graded": 0,
                "wins": 0,
                "losses": 0,
                "pushes": 0,
                "win_pct": None,
                "net_units": 0.0,
                "bets_with_units": 0,
                "roi": None,
                "units_null_graded": 0,
                "score_bucket_order": None,
            }
        m = agg[key]
        m["n_total"] += 1

        grade_status = r.get("grade_status")
        result = r.get("result")
        if grade_status == "GRADED" and result in {"WIN", "LOSS", "PUSH"}:
            m["n_graded"] += 1
            if result == "WIN":
                m["wins"] += 1
            elif result == "LOSS":
                m["losses"] += 1
            elif result == "PUSH":
                m["pushes"] += 1

            units = r.get("units")
            if units is None:
                m["units_null_graded"] += 1
            else:
                try:
                    m["net_units"] += float(units)
                    m["bets_with_units"] += 1
                except Exception:
                    m["units_null_graded"] += 1

        sb, order = score_bucket_key(r)
        if m["score_bucket_order"] is None:
            m["score_bucket_order"] = order

    for m in agg.values():
        denom = m["wins"] + m["losses"]
        m["win_pct"] = (m["wins"] / denom) if denom > 0 else None
        m["roi"] = (m["net_units"] / m["bets_with_units"]) if m["bets_with_units"] > 0 else None
    out: List[Dict[str, Any]] = []
    for key, metrics in agg.items():
        if isinstance(key, tuple):
            # handle tuple keys unpacked by caller
            pass
        metrics["key"] = key
        out.append(metrics)
    return out


def by_score_bucket(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def key_fn(r: Dict[str, Any]) -> Tuple[str, int, str]:
        sb, order = score_bucket_key(r)
        mt = r.get("market_type") or "unknown"
        return (sb or "NO_SCORE", order, mt)

    grouped = aggregate_score_groups(rows, key_fn)
    for g in grouped:
        sb, order, mt = g["key"]
        g.pop("key", None)
        g["score_bucket"] = sb
        g["score_bucket_order"] = order
        g["market_type"] = mt
    grouped.sort(key=lambda x: (x.get("score_bucket_order", 0), x.get("market_type", "")))
    return grouped


def by_score_bucket_sources_combo(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def key_fn(r: Dict[str, Any]) -> Tuple[str, int, str, str]:
        sb, order = score_bucket_key(r)
        mt = r.get("market_type") or "unknown"
        sc = r.get("sources_combo") or "unknown"
        return (sb or "NO_SCORE", order, mt, sc)

    grouped = aggregate_score_groups(rows, key_fn)
    for g in grouped:
        sb, order, mt, sc = g["key"]
        g.pop("key", None)
        g["score_bucket"] = sb
        g["score_bucket_order"] = order
        g["market_type"] = mt
        g["sources_combo"] = sc
        denom = g["wins"] + g["losses"]
        g["min_sample_warning"] = denom < 5
    grouped.sort(key=lambda x: (x.get("score_bucket_order", 0), x.get("market_type", ""), x.get("sources_combo", "")))
    return grouped


def build_meta(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    graded_rows = sum(1 for r in rows if r.get("grade_status") == "GRADED")
    return {
        "dataset_path": str(DATASET_PATH),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "total_rows": len(rows),
        "graded_rows": graded_rows,
        "excluded_ineligible_rows": sum(1 for r in rows if r.get("grade_status") == "INELIGIBLE"),
        "excluded_error_rows": sum(1 for r in rows if r.get("grade_status") == "ERROR"),
    }


def main() -> None:
    rows = read_jsonl(DATASET_PATH)
    meta = build_meta(rows)

    write_json(COVERAGE_PATH, coverage_report(rows, meta))
    write_json(BY_SOURCES_MARKET_PATH, {"meta": meta, "rows": by_sources_combo_market(rows)})
    write_json(BY_SCORE_BUCKET_PATH, {"meta": meta, "rows": by_sources_combo_score_bucket(rows)})
    expert_data = by_expert(rows, MIN_SAMPLE_SIZE_EXPERT)
    expert_meta = {**meta, **(expert_data.get("meta") or {})}
    write_json(
        BY_EXPERT_PATH,
        {
            "meta": expert_meta,
            "rows": expert_data.get("rows", []),
            "rows_filtered": expert_data.get("rows_filtered", []),
        },
    )
    write_json(BY_SCORE_BUCKET_SIMPLE_PATH, {"meta": meta, "rows": by_score_bucket(rows)})
    write_json(BY_SCORE_BUCKET_SOURCES_PATH, {"meta": meta, "rows": by_score_bucket_sources_combo(rows)})

    print("Reports written to data/reports/")


if __name__ == "__main__":
    main()
