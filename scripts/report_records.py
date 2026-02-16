"""Generate sliceable reports from the research dataset.

Supports both NBA and NCAAB via --sport parameter.

Inputs:
  - data/analysis/graded_signals_latest.jsonl (or ncaab/ subdirectory for NCAAB)
  - data/analysis/graded_occurrences_latest.jsonl

Outputs:
  - data/reports/coverage_summary.json
  - data/reports/by_sources_combo_market.json
  - data/reports/by_sources_combo_score_bucket.json
  - data/reports/by_expert.json
  - data/reports/by_source_participation_counts.json
  - data/reports/by_source_participation_counts_market.json
  - data/reports/by_source_record.json
  - data/reports/by_source_record_market.json
  - data/reports/by_expert_record.json
  - data/reports/by_source_participation_record.json
  - data/reports/by_sources_combo_record.json

Run:
    python3 scripts/report_records.py
    python3 scripts/report_records.py --sport NCAAB
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Sport constants
NBA_SPORT = "NBA"
NCAAB_SPORT = "NCAAB"


def get_dataset_path(sport: str) -> Path:
    if sport == NCAAB_SPORT:
        return Path("data/analysis/ncaab/graded_signals_latest.jsonl")
    return Path("data/analysis/graded_signals_latest.jsonl")


def get_dataset_occ_path(sport: str) -> Path:
    if sport == NCAAB_SPORT:
        return Path("data/analysis/ncaab/graded_occurrences_latest.jsonl")
    return Path("data/analysis/graded_occurrences_latest.jsonl")


def get_report_dir(sport: str) -> Path:
    if sport == NCAAB_SPORT:
        return Path("data/reports/ncaab")
    return Path("data/reports")


# Default paths for backward compatibility
DATASET_PATH = Path("data/analysis/graded_signals_latest.jsonl")
DATASET_OCC_PATH = Path("data/analysis/graded_occurrences_latest.jsonl")
REPORT_DIR = Path("data/reports")
COVERAGE_PATH = REPORT_DIR / "coverage_summary.json"
BY_SOURCES_MARKET_PATH = REPORT_DIR / "by_sources_combo_market.json"
BY_SCORE_BUCKET_PATH = REPORT_DIR / "by_sources_combo_score_bucket.json"
BY_EXPERT_PATH = REPORT_DIR / "by_expert.json"
BY_SCORE_BUCKET_SIMPLE_PATH = REPORT_DIR / "by_score_bucket.json"
BY_SCORE_BUCKET_SOURCES_PATH = REPORT_DIR / "by_score_bucket_sources_combo.json"
BY_SOURCE_PARTICIPATION_PATH = REPORT_DIR / "by_source_participation_counts.json"
BY_SOURCE_PARTICIPATION_MARKET_PATH = REPORT_DIR / "by_source_participation_counts_market.json"
BY_SOURCE_RECORD_PATH = REPORT_DIR / "by_source_record.json"
BY_SOURCE_RECORD_MARKET_PATH = REPORT_DIR / "by_source_record_market.json"
BY_EXPERT_RECORD_PATH = REPORT_DIR / "by_expert_record.json"
BY_SOURCE_PARTICIPATION_RECORD_PATH = REPORT_DIR / "by_source_participation_record.json"
BY_SOURCES_COMBO_RECORD_PATH = REPORT_DIR / "by_sources_combo_record.json"
SPREAD_MISMATCH_SAMPLES_PATH = REPORT_DIR / "spread_mismatch_samples.json"
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


def init_participation_metrics() -> Dict[str, Any]:
    return {
        "n_graded": 0,
        "wins": 0,
        "losses": 0,
        "pushes": 0,
    }


def participating_sources(row: Dict[str, Any]) -> List[str]:
    sources_present = row.get("sources_present") or []
    if sources_present:
        return sorted({s for s in sources_present if s})
    return sorted({s for s in (row.get("sources") or []) if s})


def derive_sources_present(row: Dict[str, Any]) -> List[str]:
    sources: set[str] = set()
    for s in row.get("sources_present") or []:
        if s:
            sources.add(s)
    for s in row.get("sources") or []:
        if s:
            sources.add(s)
    for sup in row.get("supports") or []:
        if isinstance(sup, dict):
            for key in ("source_id", "source"):
                val = sup.get(key)
                if isinstance(val, str) and val:
                    sources.add(val)
    return sorted(sources)


def accumulate_participation(rows: Iterable[Dict[str, Any]], key_fn) -> List[Dict[str, Any]]:
    agg: Dict[Any, Dict[str, Any]] = {}
    for row in rows:
        key = key_fn(row)
        if key not in agg:
            agg[key] = init_participation_metrics()
        m = agg[key]
        m["n_graded"] += 1

        result = row.get("result")
        if result == "WIN":
            m["wins"] += 1
        elif result == "LOSS":
            m["losses"] += 1
        elif result == "PUSH":
            m["pushes"] += 1

    output: List[Dict[str, Any]] = []
    for key, metrics in agg.items():
        if isinstance(key, tuple):
            if len(key) == 2:
                metrics["source_id"], metrics["market_type"] = key
            else:
                metrics["key"] = key
        else:
            metrics["source_id"] = key
        output.append(metrics)

    return sorted(
        output,
        key=lambda r: (-r.get("n_graded", 0), r.get("source_id", ""), r.get("market_type", "")),
    )


def spread_coverage(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    spreads = [r for r in rows if (r.get("market_type") or "").lower() == "spread"]
    total = len(spreads)
    valid = sum(1 for r in spreads if r.get("spread_is_valid"))
    invalid = total - valid
    invalid_reasons = Counter()
    line_recovered_total = 0
    line_recovered_valid = 0
    line_recovered_invalid = 0
    graded_with_scores = 0
    mismatch_count = 0
    for r in spreads:
        if not r.get("spread_is_valid"):
            invalid_reasons[r.get("spread_invalid_reason") or "unknown"] += 1
        if r.get("spread_line_recovered"):
            line_recovered_total += 1
            if r.get("spread_is_valid"):
                line_recovered_valid += 1
            else:
                line_recovered_invalid += 1
        if (
            r.get("spread_is_valid")
            and r.get("grade_status") == "GRADED"
            and r.get("result") in {"WIN", "LOSS", "PUSH"}
            and r.get("final_home_score") is not None
            and r.get("final_away_score") is not None
        ):
            graded_with_scores += 1
        if r.get("spread_result_mismatch"):
            mismatch_count += 1
    return {
        "total_spreads": total,
        "valid_spreads": valid,
        "invalid_spreads": invalid,
        "invalid_reasons_top": invalid_reasons.most_common(10),
        "line_recovered_total": line_recovered_total,
        "line_recovered_valid": line_recovered_valid,
        "line_recovered_invalid": line_recovered_invalid,
        "graded_spreads_with_scores": graded_with_scores,
        "graded_spreads_mismatch": mismatch_count,
    }


def coverage_report(rows: List[Dict[str, Any]], meta: Dict[str, Any], occurrence_rows: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
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

    report = {
        "meta": meta,
        "grade_status_counts": dict(counts_status),
        "market_type_counts": dict(counts_market),
        "ineligible_missing_fields_top": ineligible_missing.most_common(10),
        "error_notes_top": error_notes.most_common(10),
    }
    if occurrence_rows is not None:
        report["spread_data_quality"] = spread_coverage(occurrence_rows)
    return report


def spread_mismatch_samples(rows: List[Dict[str, Any]], limit: int = 25) -> List[Dict[str, Any]]:
    samples: List[Dict[str, Any]] = []
    for r in rows:
        if not r.get("spread_result_mismatch"):
            continue
        samples.append(
            {
                "day_key": r.get("day_key"),
                "event_key": r.get("event_key"),
                "away_team": r.get("away_team"),
                "home_team": r.get("home_team"),
                "selection": r.get("selection"),
                "spread_pick": r.get("spread_pick"),
                "spread_line": r.get("spread_line"),
                "final_home_score": r.get("final_home_score"),
                "final_away_score": r.get("final_away_score"),
                "spread_cover_margin": r.get("spread_cover_margin"),
                "result": r.get("result"),
                "notes": r.get("notes"),
                "signal_sources_combo": r.get("signal_sources_combo"),
                "occ_source_id": r.get("occ_source_id"),
            }
        )
        if len(samples) >= limit:
            break
    return samples


def is_reportable(row: Dict[str, Any]) -> bool:
    """Filter out avoid_conflict signals and signals with no supports.

    avoid_conflict signals are internal markers to prevent conflicting bets,
    not actual consensus signals. They should not be included in win rate reports.
    """
    signal_type = row.get("signal_type")
    supports_count = row.get("supports_count", 0)

    # Exclude avoid_conflict signals
    if signal_type == "avoid_conflict":
        return False

    # Exclude signals with no supporting evidence
    if supports_count == 0:
        return False

    return True


def by_sources_combo_market(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    graded = [r for r in rows if r.get("grade_status") == "GRADED" and is_reportable(r)]
    return accumulate_group(
        graded,
        key_fn=lambda r: (r.get("sources_combo") or "unknown", r.get("market_type") or "unknown"),
    )


def by_sources_combo_score_bucket(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    graded = [r for r in rows if r.get("grade_status") == "GRADED" and is_reportable(r)]
    return accumulate_group(
        graded,
        key_fn=lambda r: (
            r.get("sources_combo") or "unknown",
            bucket_score(r.get("score")),
            r.get("market_type") or "unknown",
        ),
    )


def by_expert(rows: List[Dict[str, Any]], min_sample: int) -> Dict[str, Any]:
    graded = [r for r in rows if r.get("grade_status") == "GRADED" and is_reportable(r)]
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
    reportable = [r for r in rows if is_reportable(r)]

    def key_fn(r: Dict[str, Any]) -> Tuple[str, int, str]:
        sb, order = score_bucket_key(r)
        mt = r.get("market_type") or "unknown"
        return (sb or "NO_SCORE", order, mt)

    grouped = aggregate_score_groups(reportable, key_fn)
    for g in grouped:
        sb, order, mt = g["key"]
        g.pop("key", None)
        g["score_bucket"] = sb
        g["score_bucket_order"] = order
        g["market_type"] = mt
    grouped.sort(key=lambda x: (x.get("score_bucket_order", 0), x.get("market_type", "")))
    return grouped


def by_score_bucket_sources_combo(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    reportable = [r for r in rows if is_reportable(r)]

    def key_fn(r: Dict[str, Any]) -> Tuple[str, int, str, str]:
        sb, order = score_bucket_key(r)
        mt = r.get("market_type") or "unknown"
        sc = r.get("sources_combo") or "unknown"
        return (sb or "NO_SCORE", order, mt, sc)

    grouped = aggregate_score_groups(reportable, key_fn)
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


def by_source_participation_counts(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    reportable = [r for r in rows if r.get("grade_status") == "GRADED" and r.get("result") in {"WIN", "LOSS", "PUSH"}]
    exploded: List[Dict[str, Any]] = []
    debug_samples: List[Dict[str, Any]] = []
    for r in reportable:
        for src in participating_sources(r):
            exploded_row = {**r, "source_id": src}
            exploded.append(exploded_row)
            if src == "action" and len(debug_samples) < 5:
                debug_samples.append(
                    {
                        "sources_combo": r.get("sources_combo"),
                        "market_type": r.get("market_type"),
                        "selection": r.get("selection"),
                        "result": r.get("result"),
                        "day_key": r.get("day_key"),
                    }
                )
    rows_out = accumulate_participation(exploded, key_fn=lambda r: r.get("source_id") or "unknown")
    return {"rows": rows_out, "debug_samples": debug_samples}


def by_source_participation_counts_market(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    reportable = [r for r in rows if r.get("grade_status") == "GRADED" and r.get("result") in {"WIN", "LOSS", "PUSH"}]
    exploded: List[Dict[str, Any]] = []
    for r in reportable:
        for src in participating_sources(r):
            exploded.append({**r, "source_id": src})
    return accumulate_participation(
        exploded,
        key_fn=lambda r: (
            r.get("source_id") or "unknown",
            r.get("market_type") or "unknown",
        ),
    )


def build_occurrence_meta(rows: List[Dict[str, Any]], dataset_occ_path: Path = DATASET_OCC_PATH) -> Dict[str, Any]:
    graded_rows = [r for r in rows if r.get("grade_status") == "GRADED" and r.get("result") in {"WIN", "LOSS", "PUSH"}]
    # Count unique signals (deduped) vs total occurrences
    unique_signal_ids = set()
    for r in graded_rows:
        sig_id = r.get("signal_id")
        if sig_id:
            unique_signal_ids.add(sig_id)
    return {
        "dataset_path": str(dataset_occ_path),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "total_rows": len(rows),
        "graded_rows": len(graded_rows),
        "graded_unique_signals": len(unique_signal_ids),
        "excluded_ineligible_rows": sum(1 for r in rows if r.get("grade_status") == "INELIGIBLE"),
        "excluded_error_rows": sum(1 for r in rows if r.get("grade_status") == "ERROR"),
    }


def graded_occurrence_rows(rows: List[Dict[str, Any]], dedupe: bool = True) -> List[Dict[str, Any]]:
    """
    Filter to graded rows with valid results.

    If dedupe=True (default), deduplicate by signal_id to count each unique signal once,
    not each observation. This prevents inflated counts when the same signal is observed
    across multiple runs before the game.
    """
    graded: List[Dict[str, Any]] = []
    seen_signal_ids: set = set()
    for r in rows:
        if r.get("grade_status") != "GRADED" or r.get("result") not in {"WIN", "LOSS", "PUSH"}:
            continue
        if (r.get("market_type") or "").lower() == "spread" and r.get("spread_is_valid") is False:
            continue
        if dedupe:
            signal_id = r.get("signal_id")
            if signal_id and signal_id in seen_signal_ids:
                continue
            if signal_id:
                seen_signal_ids.add(signal_id)
        graded.append(r)
    return graded


def accumulate_records_with_samples(rows: Iterable[Dict[str, Any]], key_fn):
    agg: Dict[Any, Dict[str, Any]] = {}
    samples: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
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

        if len(samples[key]) < 20:
            samples[key].append(row)

    output: List[Dict[str, Any]] = []
    for key, metrics in agg.items():
        finalized = finalize_metrics(metrics)
        finalized["key"] = key
        output.append(finalized)
    output.sort(key=lambda r: (-r.get("n", 0), str(r.get("key"))))
    return output, samples


def extract_expert_key(row: Dict[str, Any]) -> str:
    experts = row.get("experts")
    if isinstance(experts, list) and experts:
        if len(experts) == 1:
            return experts[0]
        return "MULTI"
    expert = row.get("expert")
    if isinstance(expert, str) and expert:
        return expert
    return "unknown"


def build_sample_snippet(row: Dict[str, Any]) -> Dict[str, Any]:
    snippet: Dict[str, Any] = {
        "selection": row.get("selection"),
        "line": row.get("line") if row.get("line") is not None else row.get("line_median"),
        "result": row.get("result"),
        "occ_source_id": row.get("occ_source_id"),
        "signal_sources_combo": row.get("signal_sources_combo"),
    }
    if row.get("away_team") or row.get("home_team"):
        snippet["teams"] = {"away": row.get("away_team"), "home": row.get("home_team")}
    games_info = row.get("games_info") or {}
    if isinstance(games_info, dict):
        score_fields = {}
        for k in ("final_score", "final", "home_score", "away_score", "home_team_score", "away_team_score"):
            if games_info.get(k) is not None:
                score_fields[k] = games_info.get(k)
        if score_fields:
            snippet["games_info_score"] = score_fields
    if row.get("event_key"):
        snippet["event_key"] = row.get("event_key")
    if row.get("day_key"):
        snippet["day_key"] = row.get("day_key")
    return {k: v for k, v in snippet.items() if v is not None}


def annotate_anomalies(groups: List[Dict[str, Any]], samples_map: Dict[Any, List[Dict[str, Any]]]) -> None:
    for g in groups:
        key = g.get("key")
        n = g.get("n", 0)
        wins = g.get("wins", 0)
        losses = g.get("losses", 0)
        win_pct = g.get("win_pct")
        anomaly = False
        if n >= 30 and win_pct is not None and (win_pct <= 0.05 or win_pct >= 0.95):
            anomaly = True
        if n >= 20 and wins == n and n > 0:
            anomaly = True
        if anomaly:
            g["anomaly"] = True
            sample_rows = samples_map.get(key, []) if samples_map else []
            snippets = []
            for r in sample_rows:
                snippets.append(build_sample_snippet(r))
                if len(snippets) >= 3:
                    break
            g["anomaly_samples"] = snippets


def by_source_record(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    graded = graded_occurrence_rows(rows)
    grouped, samples = accumulate_records_with_samples(
        graded,
        key_fn=lambda r: r.get("occ_source_id") or r.get("occ_sources_combo") or "unknown",
    )
    annotate_anomalies(grouped, samples)
    for g in grouped:
        if "key" in g:
            g["source_id"] = g.pop("key")
    grouped.sort(key=lambda r: (-r.get("n", 0), r.get("source_id", "")))
    return grouped


def by_source_record_market(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    graded = graded_occurrence_rows(rows)
    grouped, samples = accumulate_records_with_samples(
        graded,
        key_fn=lambda r: (r.get("source_id") or "unknown", r.get("market_type") or "unknown"),
    )
    annotate_anomalies(grouped, samples)
    for g in grouped:
        key = g.pop("key", None)
        if isinstance(key, tuple) and len(key) == 2:
            g["source_id"], g["market_type"] = key
        else:
            g["key"] = key
    grouped.sort(key=lambda r: (-r.get("n", 0), r.get("source_id", ""), r.get("market_type", "")))
    return grouped


def by_expert_record(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    graded = graded_occurrence_rows(rows)
    grouped, samples = accumulate_records_with_samples(graded, key_fn=extract_expert_key)
    annotate_anomalies(grouped, samples)
    for g in grouped:
        if "key" in g and "expert" not in g:
            g["expert"] = g.pop("key")
    grouped.sort(key=lambda r: (-r.get("n", 0), r.get("expert", "")))
    filtered = [g for g in grouped if g.get("n", 0) >= MIN_SAMPLE_SIZE_EXPERT]
    return {"meta": {"min_sample_size": MIN_SAMPLE_SIZE_EXPERT}, "rows": grouped, "rows_filtered": filtered}


def by_source_participation_record(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    graded = graded_occurrence_rows(rows)
    exploded: List[Dict[str, Any]] = []
    missing_samples: List[Dict[str, Any]] = []
    for r in graded:
        derived = derive_sources_present(r)
        if not derived:
            derived = ["unknown"]
            if len(missing_samples) < 5:
                missing_samples.append(build_sample_snippet(r))
        for src in derived:
            exploded.append({**r, "source_participation_id": src})

    grouped, samples = accumulate_records_with_samples(exploded, key_fn=lambda r: r.get("source_participation_id") or "unknown")
    annotate_anomalies(grouped, samples)
    for g in grouped:
        key = g.pop("key", None)
        g["source_id"] = key
        if key == "unknown":
            g["anomaly"] = True
            g["anomaly_reason"] = "missing_sources_present"
            g["anomaly_samples"] = (g.get("anomaly_samples") or []) + missing_samples[:3]
    grouped.sort(key=lambda r: (-r.get("n", 0), r.get("source_id", "")))
    return {"rows": grouped}


def by_sources_combo_record(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    graded = graded_occurrence_rows(rows)
    exploded: List[Dict[str, Any]] = []
    missing_samples: List[Dict[str, Any]] = []
    for r in graded:
        combo = r.get("signal_sources_combo") or "unknown"
        if combo == "unknown" and len(missing_samples) < 5:
            missing_samples.append(build_sample_snippet(r))
        exploded.append({**r, "sources_combo_present": combo})

    grouped, samples = accumulate_records_with_samples(exploded, key_fn=lambda r: r.get("sources_combo_present") or "unknown")
    annotate_anomalies(grouped, samples)
    for g in grouped:
        key = g.pop("key", None)
        g["sources_combo"] = key
        if key == "unknown":
            g["anomaly"] = True
            g["anomaly_reason"] = "missing_sources_present"
            g["anomaly_samples"] = (g.get("anomaly_samples") or []) + missing_samples[:3]
    grouped.sort(key=lambda r: (-r.get("n", 0), r.get("sources_combo", "")))
    return {"rows": grouped}


def build_meta(rows: List[Dict[str, Any]], dataset_path: Path = DATASET_PATH) -> Dict[str, Any]:
    graded_rows = sum(1 for r in rows if r.get("grade_status") == "GRADED")
    reportable_rows = sum(1 for r in rows if r.get("grade_status") == "GRADED" and is_reportable(r))
    avoid_conflict_rows = sum(1 for r in rows if r.get("signal_type") == "avoid_conflict")
    no_supports_rows = sum(1 for r in rows if r.get("supports_count", 0) == 0)
    return {
        "dataset_path": str(dataset_path),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "total_rows": len(rows),
        "graded_rows": graded_rows,
        "reportable_graded_rows": reportable_rows,
        "excluded_ineligible_rows": sum(1 for r in rows if r.get("grade_status") == "INELIGIBLE"),
        "excluded_error_rows": sum(1 for r in rows if r.get("grade_status") == "ERROR"),
        "excluded_avoid_conflict": avoid_conflict_rows,
        "excluded_no_supports": no_supports_rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate sliceable reports from research dataset")
    parser.add_argument(
        "--sport",
        choices=[NBA_SPORT, NCAAB_SPORT],
        default=NBA_SPORT,
        help=f"Sport to process (default: {NBA_SPORT})",
    )
    args = parser.parse_args()
    sport = args.sport

    # Use sport-specific paths
    dataset_path = get_dataset_path(sport)
    dataset_occ_path = get_dataset_occ_path(sport)
    report_dir = get_report_dir(sport)

    # Sport-specific report paths
    coverage_path = report_dir / "coverage_summary.json"
    by_sources_market_path = report_dir / "by_sources_combo_market.json"
    by_score_bucket_path = report_dir / "by_sources_combo_score_bucket.json"
    by_expert_path = report_dir / "by_expert.json"
    by_score_bucket_simple_path = report_dir / "by_score_bucket.json"
    by_score_bucket_sources_path = report_dir / "by_score_bucket_sources_combo.json"
    by_source_participation_path = report_dir / "by_source_participation_counts.json"
    by_source_participation_market_path = report_dir / "by_source_participation_counts_market.json"
    by_source_record_path = report_dir / "by_source_record.json"
    by_source_record_market_path = report_dir / "by_source_record_market.json"
    by_expert_record_path = report_dir / "by_expert_record.json"
    by_source_participation_record_path = report_dir / "by_source_participation_record.json"
    by_sources_combo_record_path = report_dir / "by_sources_combo_record.json"
    spread_mismatch_samples_path = report_dir / "spread_mismatch_samples.json"

    print(f"Processing {sport} reports...")
    rows = read_jsonl(dataset_path)
    rows_occ = read_jsonl(dataset_occ_path)
    meta = build_meta(rows, dataset_path)
    meta_occ = build_occurrence_meta(rows_occ, dataset_occ_path)

    print(f"[signals] Loaded {len(rows)} rows ({meta.get('graded_rows')} graded; {meta.get('reportable_graded_rows')} reportable).")
    print(f"[occurrences] Loaded {len(rows_occ)} rows ({meta_occ.get('graded_rows')} graded).")

    write_json(coverage_path, coverage_report(rows, meta, rows_occ))
    write_json(by_sources_market_path, {"meta": meta, "rows": by_sources_combo_market(rows)})
    write_json(by_score_bucket_path, {"meta": meta, "rows": by_sources_combo_score_bucket(rows)})
    expert_data = by_expert(rows, MIN_SAMPLE_SIZE_EXPERT)
    expert_meta = {**meta, **(expert_data.get("meta") or {})}
    write_json(
        by_expert_path,
        {
            "meta": expert_meta,
            "rows": expert_data.get("rows", []),
            "rows_filtered": expert_data.get("rows_filtered", []),
        },
    )
    write_json(by_score_bucket_simple_path, {"meta": meta, "rows": by_score_bucket(rows)})
    write_json(by_score_bucket_sources_path, {"meta": meta, "rows": by_score_bucket_sources_combo(rows)})
    source_participation = by_source_participation_counts(rows)
    write_json(
        by_source_participation_path,
        {"meta": meta, "rows": source_participation.get("rows", []), "debug_samples": source_participation.get("debug_samples", [])},
    )
    write_json(
        by_source_participation_market_path,
        {"meta": meta, "rows": by_source_participation_counts_market(rows)},
    )
    write_json(by_source_record_path, {"meta": meta_occ, "rows": by_source_record(rows_occ)})
    write_json(by_source_record_market_path, {"meta": meta_occ, "rows": by_source_record_market(rows_occ)})
    expert_record_data = by_expert_record(rows_occ)
    write_json(
        by_expert_record_path,
        {
            "meta": {**meta_occ, **(expert_record_data.get("meta") or {})},
            "rows": expert_record_data.get("rows", []),
            "rows_filtered": expert_record_data.get("rows_filtered", []),
        },
    )
    source_participation_record = by_source_participation_record(rows_occ)
    write_json(
        by_source_participation_record_path,
        {"meta": meta_occ, "rows": source_participation_record.get("rows", [])},
    )
    write_json(
        by_sources_combo_record_path,
        {"meta": meta_occ, "rows": by_sources_combo_record(rows_occ).get("rows", [])},
    )
    write_json(
        spread_mismatch_samples_path,
        {"meta": meta_occ, "rows": spread_mismatch_samples(rows_occ)},
    )

    print(f"Reports written to {report_dir}:")
    for path in [
        coverage_path,
        by_sources_market_path,
        by_score_bucket_path,
        by_expert_path,
        by_score_bucket_simple_path,
        by_score_bucket_sources_path,
        by_source_participation_path,
        by_source_participation_market_path,
        by_source_record_path,
        by_source_record_market_path,
        by_expert_record_path,
        by_source_participation_record_path,
        by_sources_combo_record_path,
        spread_mismatch_samples_path,
    ]:
        print(f"  - {path}")


if __name__ == "__main__":
    main()
