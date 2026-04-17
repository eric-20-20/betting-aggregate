from __future__ import annotations

import argparse
import csv
import json
import math
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTCOMES_PATH = REPO_ROOT / "data/ledger/expert_pick_outcomes_latest.jsonl"
DEFAULT_REPORT_DIR = REPO_ROOT / "data/reports"
MIN_SAMPLE_SIZE_EXPERT = 10
NBA_SPORT = "NBA"
NCAAB_SPORT = "NCAAB"
COMBINED_GRADE_MODE = "combined_deduped"


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, sort_keys=True, indent=2)


def _stringify_csv_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def write_rows_csv(path: Path, payload: Dict[str, Any]) -> None:
    rows = payload.get("rows", [])
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = set()
    for row in rows:
        fieldnames.update(row.keys())
    ordered = sorted(fieldnames)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=ordered)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _stringify_csv_value(row.get(key)) for key in ordered})


def get_report_dir(sport: str) -> Path:
    return DEFAULT_REPORT_DIR / "ncaab" if sport == NCAAB_SPORT else DEFAULT_REPORT_DIR


def get_legacy_report_path(sport: str) -> Path:
    return get_report_dir(sport) / "by_expert_supports.json"


def init_metrics() -> Dict[str, Any]:
    return {
        "bets_with_units": 0,
        "losses": 0,
        "net_units": 0.0,
        "total_stake": 0.0,
        "n": 0,
        "odds_sum": 0.0,
        "odds_count": 0,
        "pushes": 0,
        "units_missing_count": 0,
        "wins": 0,
    }


def wilson_lower(wins: int, n: int, z: float = 1.96) -> float:
    if n == 0:
        return 0.0
    p = wins / n
    denom = 1 + z * z / n
    center = p + z * z / (2 * n)
    spread = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return (center - spread) / denom


def finalize_metrics(metrics: Dict[str, Any]) -> Dict[str, Any]:
    denom = metrics["wins"] + metrics["losses"]
    metrics["win_pct"] = (metrics["wins"] / denom) if denom > 0 else None
    metrics["wilson_lower"] = round(wilson_lower(metrics["wins"], denom), 4) if denom > 0 else None
    metrics["roi"] = (metrics["net_units"] / metrics["total_stake"]) if metrics["total_stake"] > 0 else None
    metrics["avg_odds"] = (metrics["odds_sum"] / metrics["odds_count"]) if metrics["odds_count"] > 0 else None
    return metrics


def accumulate_metric(metrics: Dict[str, Any], row: Dict[str, Any], result_field: str = "support_result") -> None:
    result = row.get(result_field)
    metrics["n"] += 1
    if result == "WIN":
        metrics["wins"] += 1
    elif result == "LOSS":
        metrics["losses"] += 1
    elif result == "PUSH":
        metrics["pushes"] += 1

    units = row.get("units")
    stake = row.get("stake", 1.0)
    if units is not None:
        try:
            metrics["net_units"] += float(units)
            metrics["bets_with_units"] += 1
            metrics["total_stake"] += float(stake) if stake is not None else 1.0
        except Exception:
            metrics["units_missing_count"] += 1
    else:
        metrics["units_missing_count"] += 1

    odds = row.get("odds")
    if odds is not None:
        try:
            metrics["odds_sum"] += float(odds)
            metrics["odds_count"] += 1
        except Exception:
            pass


def effective_result(row: Dict[str, Any]) -> Optional[str]:
    support_result = row.get("support_result")
    if support_result in {"WIN", "LOSS", "PUSH"}:
        return str(support_result)
    if row.get("grade_mode") == "pregraded":
        grade_status = row.get("grade_status")
        if grade_status in {"WIN", "LOSS", "PUSH"}:
            return str(grade_status)
    return None


def expert_pick_key_for_dedupe(row: Dict[str, Any]) -> str:
    key = row.get("expert_pick_key")
    if key:
        return str(key)
    payload = [
        row.get("expert_name") or "unknown",
        row.get("source_id") or "unknown",
        row.get("canonical_event_key") or row.get("event_key") or row.get("day_key") or "unknown",
        row.get("market_type") or "unknown",
        row.get("support_selection") or row.get("selection") or "unknown",
        row.get("support_direction") or row.get("direction") or "unknown",
        row.get("support_line"),
        row.get("atomic_stat"),
        row.get("player_id") or row.get("player_key"),
    ]
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def combined_dedupe_rank(row: Dict[str, Any]) -> Tuple[int, int, int, str, str]:
    eff = effective_result(row)
    is_live_terminal = 1 if row.get("grade_mode") == "live" and eff in {"WIN", "LOSS", "PUSH"} else 0
    is_pregraded_terminal = 1 if row.get("grade_mode") == "pregraded" and eff in {"WIN", "LOSS", "PUSH"} else 0
    has_support_result = 1 if row.get("support_result") in {"WIN", "LOSS", "PUSH"} else 0
    return (
        is_live_terminal,
        is_pregraded_terminal,
        has_support_result,
        str(row.get("graded_at_utc") or ""),
        str(row.get("signal_id") or ""),
    )


def build_combined_support_rows(rows: Iterable[Dict[str, Any]], sport: str) -> List[Dict[str, Any]]:
    sport_rows = [
        row for row in rows
        if row.get("sport", NBA_SPORT) == sport
        and row.get("is_primary_record")
    ]
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in sport_rows:
        if effective_result(row) not in {"WIN", "LOSS", "PUSH"}:
            continue
        grouped[expert_pick_key_for_dedupe(row)].append(dict(row))

    deduped_rows: List[Dict[str, Any]] = []
    for _, bucket in grouped.items():
        representative = sorted(bucket, key=combined_dedupe_rank, reverse=True)[0]
        representative["effective_result"] = effective_result(representative)
        representative["effective_grade_mode"] = representative.get("grade_mode")
        deduped_rows.append(representative)
    return deduped_rows


def build_support_rows(rows: Iterable[Dict[str, Any]], sport: str, grade_mode: str) -> List[Dict[str, Any]]:
    if grade_mode == COMBINED_GRADE_MODE:
        return build_combined_support_rows(rows, sport)
    result: List[Dict[str, Any]] = []
    for row in rows:
        if row.get("sport", NBA_SPORT) != sport:
            continue
        if not row.get("is_primary_record"):
            continue
        if row.get("grade_mode") != grade_mode:
            continue
        eff = effective_result(row)
        if eff not in {"WIN", "LOSS", "PUSH"}:
            continue
        row_copy = dict(row)
        row_copy["effective_result"] = eff
        row_copy["effective_grade_mode"] = grade_mode
        result.append(row_copy)
    return result


def build_report(rows: Iterable[Dict[str, Any]], grade_mode: str, sport: str, min_sample_size: int) -> Dict[str, Any]:
    expert_agg: Dict[Tuple[str, str], Dict[str, Any]] = {}
    market_agg: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    support_rows = build_support_rows(rows, sport, grade_mode)

    for row in support_rows:
        expert_name = str(row.get("expert_name") or "unknown")
        source_id = str(row.get("source_id") or "unknown")
        market_type = str(row.get("market_type") or "unknown")

        expert_key = (expert_name, source_id)
        market_key = (expert_name, source_id, market_type)
        expert_agg.setdefault(expert_key, init_metrics())
        market_agg.setdefault(market_key, init_metrics())
        accumulate_metric(expert_agg[expert_key], row, result_field="effective_result")
        accumulate_metric(market_agg[market_key], row, result_field="effective_result")

    rows_out: List[Dict[str, Any]] = []
    for (expert_name, source_id), metrics in expert_agg.items():
        finalized = finalize_metrics(metrics)
        finalized["expert_name"] = expert_name
        finalized["source_id"] = source_id
        by_market: Dict[str, Dict[str, Any]] = {}
        for market_type in ("spread", "total", "moneyline", "player_prop"):
            market_key = (expert_name, source_id, market_type)
            if market_key not in market_agg:
                continue
            market_final = finalize_metrics(dict(market_agg[market_key]))
            by_market[market_type] = {
                "n": market_final["n"],
                "wins": market_final["wins"],
                "losses": market_final["losses"],
                "win_pct": market_final["win_pct"],
                "wilson_lower": market_final["wilson_lower"],
                "net_units": round(market_final["net_units"], 3),
            }
        finalized["by_market"] = by_market
        rows_out.append(finalized)

    rows_out.sort(key=lambda row: (-(row.get("wins", 0) + row.get("losses", 0)), row.get("expert_name", ""), row.get("source_id", "")))
    rows_filtered = [row for row in rows_out if row.get("wins", 0) + row.get("losses", 0) >= min_sample_size]
    return {
        "meta": {
            "source": "expert_pick_outcomes_latest.jsonl",
            "sport": sport,
            "grade_mode": grade_mode,
            "min_sample_size": min_sample_size,
            "included_primary_rows": len(support_rows),
            "total_experts": len(rows_out),
        },
        "rows": rows_out,
        "rows_filtered": rows_filtered,
    }


def build_agreement_base_key(row: Dict[str, Any]) -> str:
    market = row.get("market_type") or "unknown"
    event_key = row.get("canonical_event_key") or row.get("event_key") or row.get("day_key") or "unknown"
    if market == "player_prop":
        payload = [
            event_key,
            market,
            row.get("player_id") or row.get("player_key") or row.get("selection"),
            row.get("atomic_stat") or "unknown",
            row.get("support_direction") or row.get("direction") or "unknown",
        ]
    elif market in {"spread", "moneyline"}:
        payload = [
            event_key,
            market,
            row.get("support_selection") or row.get("selection") or "unknown",
        ]
    elif market == "total":
        payload = [
            event_key,
            market,
            row.get("support_direction") or row.get("direction") or "unknown",
        ]
    else:
        payload = [
            event_key,
            market,
            row.get("support_selection") or row.get("selection") or "unknown",
            row.get("support_direction") or row.get("direction") or "unknown",
        ]
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def choose_pair_representative(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    def sort_key(row: Dict[str, Any]) -> Tuple[int, int, str, str]:
        has_line = 1 if row.get("support_line") is not None else 0
        has_odds = 1 if row.get("odds") is not None else 0
        return (
            has_line,
            has_odds,
            str(row.get("graded_at_utc") or ""),
            str(row.get("signal_id") or ""),
        )

    return sorted(rows, key=sort_key, reverse=True)[0]


def build_agreement_report(rows: Iterable[Dict[str, Any]], grade_mode: str, sport: str, min_sample_size: int) -> Dict[str, Any]:
    support_rows = build_support_rows(rows, sport, grade_mode)
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in support_rows:
        grouped[build_agreement_base_key(row)].append(row)

    pair_agg: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    market_agg: Dict[Tuple[str, str, str, str, str], Dict[str, Any]] = {}
    split_examples: List[Dict[str, Any]] = []
    total_groups = 0
    groups_with_pairs = 0
    split_pairs = 0

    for group_key, bucket in grouped.items():
        total_groups += 1
        by_expert: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
        for row in bucket:
            by_expert[(str(row.get("expert_name") or "unknown"), str(row.get("source_id") or "unknown"))].append(row)
        if len(by_expert) < 2:
            continue
        groups_with_pairs += 1
        representatives = {
            expert_key: choose_pair_representative(expert_rows)
            for expert_key, expert_rows in by_expert.items()
        }
        for (expert_a, row_a), (expert_b, row_b) in combinations(representatives.items(), 2):
            name_a = f"{expert_a[0]} [{expert_a[1]}]"
            name_b = f"{expert_b[0]} [{expert_b[1]}]"
            pair_key = tuple(sorted((name_a, name_b)))
            result_a = row_a.get("effective_result") or effective_result(row_a)
            result_b = row_b.get("effective_result") or effective_result(row_b)
            pair_result: Optional[str]
            if result_a == result_b and result_a in {"WIN", "LOSS", "PUSH"}:
                pair_result = result_a
            else:
                pair_result = None
                split_pairs += 1
                if len(split_examples) < 20:
                    split_examples.append(
                        {
                            "agreement_key": group_key,
                            "expert_a": name_a,
                            "expert_b": name_b,
                            "result_a": result_a,
                            "result_b": result_b,
                            "line_a": row_a.get("support_line"),
                            "line_b": row_b.get("support_line"),
                            "event_key": row_a.get("canonical_event_key") or row_a.get("event_key"),
                            "market_type": row_a.get("market_type"),
                            "selection": row_a.get("selection"),
                        }
                    )

            pair_metrics = pair_agg.setdefault((pair_key[0], pair_key[1], expert_a[0], expert_b[0]), init_metrics())
            market_metrics = market_agg.setdefault((pair_key[0], pair_key[1], expert_a[0], expert_b[0], str(row_a.get("market_type") or "unknown")), init_metrics())
            pair_metrics["split_count"] = pair_metrics.get("split_count", 0)
            market_metrics["split_count"] = market_metrics.get("split_count", 0)
            if pair_result is None:
                pair_metrics["split_count"] += 1
                market_metrics["split_count"] += 1
                continue

            synthetic_row = {
                "support_result": pair_result,
                "units": None,
                "stake": None,
                "odds": None,
            }
            accumulate_metric(pair_metrics, synthetic_row)
            accumulate_metric(market_metrics, synthetic_row)

    rows_out: List[Dict[str, Any]] = []
    for (pair_name_a, pair_name_b, expert_a_name, expert_b_name), metrics in pair_agg.items():
        finalized = finalize_metrics(metrics)
        finalized["expert_a"] = pair_name_a
        finalized["expert_b"] = pair_name_b
        finalized["display_pair"] = f"{pair_name_a} + {pair_name_b}"
        finalized["split_count"] = metrics.get("split_count", 0)
        by_market: Dict[str, Dict[str, Any]] = {}
        for market_type in ("spread", "total", "moneyline", "player_prop"):
            market_key = (pair_name_a, pair_name_b, expert_a_name, expert_b_name, market_type)
            if market_key not in market_agg:
                continue
            market_final = finalize_metrics(dict(market_agg[market_key]))
            by_market[market_type] = {
                "n": market_final["n"],
                "wins": market_final["wins"],
                "losses": market_final["losses"],
                "pushes": market_final["pushes"],
                "split_count": market_agg[market_key].get("split_count", 0),
                "win_pct": market_final["win_pct"],
                "wilson_lower": market_final["wilson_lower"],
            }
        finalized["by_market"] = by_market
        rows_out.append(finalized)

    rows_out.sort(key=lambda row: (-(row.get("wins", 0) + row.get("losses", 0)), row.get("display_pair", "")))
    rows_filtered = [row for row in rows_out if row.get("wins", 0) + row.get("losses", 0) >= min_sample_size]
    return {
        "meta": {
            "source": "expert_pick_outcomes_latest.jsonl",
            "sport": sport,
            "grade_mode": grade_mode,
            "min_sample_size": min_sample_size,
            "agreement_groups_total": total_groups,
            "agreement_groups_with_pairs": groups_with_pairs,
            "split_pairs": split_pairs,
            "total_pairs": len(rows_out),
        },
        "rows": rows_out,
        "rows_filtered": rows_filtered,
        "split_examples": split_examples,
    }


def support_direction(row: Dict[str, Any]) -> Optional[str]:
    direction = row.get("support_direction") or row.get("direction")
    if direction is None:
        return None
    return str(direction).upper()


def support_stat(row: Dict[str, Any]) -> Optional[str]:
    stat = row.get("atomic_stat")
    if stat is None:
        return None
    return str(stat)


def build_expert_detail_report(
    rows: Iterable[Dict[str, Any]],
    grade_mode: str,
    sport: str,
    min_sample_size: int,
    include_direction: bool,
) -> Dict[str, Any]:
    support_rows = build_support_rows(rows, sport, grade_mode)
    agg: Dict[Tuple[Any, ...], Dict[str, Any]] = {}

    for row in support_rows:
        expert_name = str(row.get("expert_name") or "unknown")
        source_id = str(row.get("source_id") or "unknown")
        market_type = str(row.get("market_type") or "unknown")
        atomic_stat = support_stat(row)
        direction = support_direction(row)
        key_parts: List[Any] = [expert_name, source_id, market_type, atomic_stat]
        if include_direction:
            key_parts.append(direction)
        key = tuple(key_parts)
        agg.setdefault(key, init_metrics())
        accumulate_metric(agg[key], row, result_field="effective_result")

    rows_out: List[Dict[str, Any]] = []
    for key, metrics in agg.items():
        finalized = finalize_metrics(metrics)
        finalized["expert_name"] = key[0]
        finalized["source_id"] = key[1]
        finalized["market_type"] = key[2]
        finalized["atomic_stat"] = key[3]
        if include_direction:
            finalized["direction"] = key[4]
        rows_out.append(finalized)

    rows_out.sort(
        key=lambda row: (
            -(row.get("wins", 0) + row.get("losses", 0)),
            row.get("expert_name", ""),
            row.get("source_id", ""),
            row.get("market_type", ""),
            str(row.get("atomic_stat") or ""),
            str(row.get("direction") or ""),
        )
    )
    rows_filtered = [row for row in rows_out if row.get("wins", 0) + row.get("losses", 0) >= min_sample_size]
    return {
        "meta": {
            "source": "expert_pick_outcomes_latest.jsonl",
            "sport": sport,
            "grade_mode": grade_mode,
            "min_sample_size": min_sample_size,
            "included_primary_rows": len(support_rows),
            "include_direction": include_direction,
            "total_rows": len(rows_out),
        },
        "rows": rows_out,
        "rows_filtered": rows_filtered,
    }


def build_source_rating_report(
    rows: Iterable[Dict[str, Any]],
    grade_mode: str,
    sport: str,
    min_sample_size: int,
) -> Dict[str, Any]:
    support_rows = build_support_rows(rows, sport, grade_mode)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for row in support_rows:
        source_id = str(row.get("source_id") or "unknown")
        rating_bucket = str(row.get("rating_bucket") or "unrated")
        key = (source_id, rating_bucket)
        agg.setdefault(key, init_metrics())
        accumulate_metric(agg[key], row, result_field="effective_result")

    rows_out: List[Dict[str, Any]] = []
    for (source_id, rating_bucket), metrics in agg.items():
        finalized = finalize_metrics(metrics)
        finalized["source_id"] = source_id
        finalized["rating_bucket"] = rating_bucket
        rows_out.append(finalized)

    rows_out.sort(
        key=lambda row: (
            -(row.get("wins", 0) + row.get("losses", 0)),
            row.get("source_id", ""),
            row.get("rating_bucket", ""),
        )
    )
    rows_filtered = [row for row in rows_out if row.get("wins", 0) + row.get("losses", 0) >= min_sample_size]
    return {
        "meta": {
            "source": "expert_pick_outcomes_latest.jsonl",
            "sport": sport,
            "grade_mode": grade_mode,
            "min_sample_size": min_sample_size,
            "included_primary_rows": len(support_rows),
            "total_rows": len(rows_out),
        },
        "rows": rows_out,
        "rows_filtered": rows_filtered,
    }


def build_source_rating_detail_report(
    rows: Iterable[Dict[str, Any]],
    grade_mode: str,
    sport: str,
    min_sample_size: int,
    include_direction: bool,
) -> Dict[str, Any]:
    support_rows = build_support_rows(rows, sport, grade_mode)
    agg: Dict[Tuple[Any, ...], Dict[str, Any]] = {}

    for row in support_rows:
        source_id = str(row.get("source_id") or "unknown")
        rating_bucket = str(row.get("rating_bucket") or "unrated")
        market_type = str(row.get("market_type") or "unknown")
        atomic_stat = support_stat(row)
        direction = support_direction(row)
        key_parts: List[Any] = [source_id, rating_bucket, market_type, atomic_stat]
        if include_direction:
            key_parts.append(direction)
        key = tuple(key_parts)
        agg.setdefault(key, init_metrics())
        accumulate_metric(agg[key], row, result_field="effective_result")

    rows_out: List[Dict[str, Any]] = []
    for key, metrics in agg.items():
        finalized = finalize_metrics(metrics)
        finalized["source_id"] = key[0]
        finalized["rating_bucket"] = key[1]
        finalized["market_type"] = key[2]
        finalized["atomic_stat"] = key[3]
        if include_direction:
            finalized["direction"] = key[4]
        rows_out.append(finalized)

    rows_out.sort(
        key=lambda row: (
            -(row.get("wins", 0) + row.get("losses", 0)),
            row.get("source_id", ""),
            row.get("rating_bucket", ""),
            row.get("market_type", ""),
            str(row.get("atomic_stat") or ""),
            str(row.get("direction") or ""),
        )
    )
    rows_filtered = [row for row in rows_out if row.get("wins", 0) + row.get("losses", 0) >= min_sample_size]
    return {
        "meta": {
            "source": "expert_pick_outcomes_latest.jsonl",
            "sport": sport,
            "grade_mode": grade_mode,
            "min_sample_size": min_sample_size,
            "included_primary_rows": len(support_rows),
            "include_direction": include_direction,
            "total_rows": len(rows_out),
        },
        "rows": rows_out,
        "rows_filtered": rows_filtered,
    }


def build_agreement_detail_report(
    rows: Iterable[Dict[str, Any]],
    grade_mode: str,
    sport: str,
    min_sample_size: int,
    include_direction: bool,
    max_group_size: int = 4,
) -> Dict[str, Any]:
    support_rows = build_support_rows(rows, sport, grade_mode)
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in support_rows:
        grouped[build_agreement_base_key(row)].append(row)

    agg: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    split_examples: List[Dict[str, Any]] = []
    total_groups = 0

    for group_key, bucket in grouped.items():
        total_groups += 1
        by_expert: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
        for row in bucket:
            by_expert[(str(row.get("expert_name") or "unknown"), str(row.get("source_id") or "unknown"))].append(row)
        if len(by_expert) < 2:
            continue

        representatives = {
            expert_key: choose_pair_representative(expert_rows)
            for expert_key, expert_rows in by_expert.items()
        }
        expert_items = sorted(representatives.items())
        for group_size in range(2, min(max_group_size, len(expert_items)) + 1):
            for combo in combinations(expert_items, group_size):
                combo_rows = [row for _, row in combo]
                combo_results = {(row.get("effective_result") or effective_result(row)) for row in combo_rows}
                meta_row = combo_rows[0]
                experts = tuple(f"{expert}[{source}]" for (expert, source), _ in combo)
                market_type = str(meta_row.get("market_type") or "unknown")
                atomic_stat = support_stat(meta_row)
                direction = support_direction(meta_row)
                key_parts: List[Any] = [experts, group_size, market_type, atomic_stat]
                if include_direction:
                    key_parts.append(direction)
                agg_key = tuple(key_parts)
                metrics = agg.setdefault(agg_key, init_metrics())
                metrics["split_count"] = metrics.get("split_count", 0)

                if len(combo_results) != 1:
                    metrics["split_count"] += 1
                    if len(split_examples) < 30:
                        split_examples.append(
                            {
                                "agreement_key": group_key,
                                "experts": list(experts),
                                "market_type": market_type,
                                "atomic_stat": atomic_stat,
                                "direction": direction,
                                "results": [(row.get("effective_result") or effective_result(row)) for row in combo_rows],
                                "lines": [row.get("support_line") for row in combo_rows],
                            }
                        )
                    continue

                pair_result = next(iter(combo_results))
                synthetic_row = {
                    "support_result": pair_result,
                    "units": None,
                    "stake": None,
                    "odds": None,
                }
                accumulate_metric(metrics, synthetic_row)

    rows_out: List[Dict[str, Any]] = []
    for key, metrics in agg.items():
        finalized = finalize_metrics(metrics)
        finalized["experts"] = list(key[0])
        finalized["display_group"] = " + ".join(key[0])
        finalized["group_size"] = key[1]
        finalized["market_type"] = key[2]
        finalized["atomic_stat"] = key[3]
        finalized["split_count"] = metrics.get("split_count", 0)
        if include_direction:
            finalized["direction"] = key[4]
        rows_out.append(finalized)

    rows_out.sort(
        key=lambda row: (
            -(row.get("wins", 0) + row.get("losses", 0)),
            -row.get("group_size", 0),
            row.get("display_group", ""),
            row.get("market_type", ""),
            str(row.get("atomic_stat") or ""),
            str(row.get("direction") or ""),
        )
    )
    rows_filtered = [row for row in rows_out if row.get("wins", 0) + row.get("losses", 0) >= min_sample_size]
    return {
        "meta": {
            "source": "expert_pick_outcomes_latest.jsonl",
            "sport": sport,
            "grade_mode": grade_mode,
            "min_sample_size": min_sample_size,
            "include_direction": include_direction,
            "agreement_groups_total": total_groups,
            "included_primary_rows": len(support_rows),
            "total_rows": len(rows_out),
            "max_group_size": max_group_size,
        },
        "rows": rows_out,
        "rows_filtered": rows_filtered,
        "split_examples": split_examples,
    }


def build_audit_report(rows: Iterable[Dict[str, Any]], sport: str) -> Dict[str, Any]:
    sport_rows = [row for row in rows if row.get("sport", NBA_SPORT) == sport]
    primary_rows = [row for row in sport_rows if row.get("is_primary_record")]
    duplicate_rows = len(sport_rows) - len(primary_rows)
    grade_mode_counts = Counter(row.get("grade_mode") for row in primary_rows)
    support_result_counts = Counter(row.get("support_result") for row in primary_rows if row.get("support_result"))
    mismatch_rows = [
        row for row in primary_rows
        if row.get("support_result") in {"WIN", "LOSS", "PUSH"}
        and row.get("signal_result") in {"WIN", "LOSS", "PUSH"}
        and row.get("support_result") != row.get("signal_result")
    ]
    duplicates_by_expert = Counter(
        (str(row.get("expert_name") or "unknown"), str(row.get("source_id") or "unknown"))
        for row in sport_rows if not row.get("is_primary_record")
    )
    mismatches_by_expert = Counter(
        (str(row.get("expert_name") or "unknown"), str(row.get("source_id") or "unknown"))
        for row in mismatch_rows
    )
    ungraded_primary_rows = [row for row in primary_rows if row.get("support_result") not in {"WIN", "LOSS", "PUSH"}]
    return {
        "meta": {
            "sport": sport,
            "source": "expert_pick_outcomes_latest.jsonl",
            "total_rows": len(sport_rows),
            "primary_rows": len(primary_rows),
            "duplicate_audit_rows": duplicate_rows,
            "ungraded_primary_rows": len(ungraded_primary_rows),
        },
        "grade_mode_counts": dict(grade_mode_counts),
        "support_result_counts": dict(support_result_counts),
        "support_signal_mismatch_count": len(mismatch_rows),
        "top_duplicate_experts": [
            {"expert_name": expert, "source_id": source, "duplicate_rows": count}
            for (expert, source), count in duplicates_by_expert.most_common(20)
        ],
        "top_mismatch_experts": [
            {"expert_name": expert, "source_id": source, "mismatch_rows": count}
            for (expert, source), count in mismatches_by_expert.most_common(20)
        ],
        "mismatch_examples": mismatch_rows[:20],
    }


def load_legacy_rows(path: Path) -> Dict[Tuple[str, str], Dict[str, Any]]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text())
    result: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in payload.get("rows", []):
        result[(str(row.get("expert_name") or "unknown"), str(row.get("source_id") or "unknown"))] = row
    return result


def summarize_top_10(live_report: Dict[str, Any], legacy_rows: Dict[Tuple[str, str], Dict[str, Any]]) -> Dict[str, Any]:
    top_rows = sorted(
        live_report.get("rows", []),
        key=lambda row: (-(row.get("wins", 0) + row.get("losses", 0)), row.get("expert_name", ""), row.get("source_id", "")),
    )[:10]
    summary_rows: List[Dict[str, Any]] = []
    for row in top_rows:
        key = (str(row.get("expert_name") or "unknown"), str(row.get("source_id") or "unknown"))
        legacy = legacy_rows.get(key, {})
        summary_rows.append(
            {
                "expert_name": key[0],
                "source_id": key[1],
                "new_wins": row.get("wins", 0),
                "new_losses": row.get("losses", 0),
                "new_win_rate": row.get("win_pct"),
                "old_wins": legacy.get("wins"),
                "old_losses": legacy.get("losses"),
                "old_win_rate": legacy.get("win_pct"),
            }
        )
    return {"top_10_live_before_after": summary_rows}


def build_reports_from_outcomes(
    outcomes: Iterable[Dict[str, Any]],
    sport: str = NBA_SPORT,
    min_sample_size: int = MIN_SAMPLE_SIZE_EXPERT,
) -> Dict[str, Dict[str, Any]]:
    outcomes_list = list(outcomes)
    return {
        "supports_live": build_report(outcomes_list, "live", sport, min_sample_size),
        "supports_pregraded": build_report(outcomes_list, "pregraded", sport, min_sample_size),
        "supports_combined": build_report(outcomes_list, COMBINED_GRADE_MODE, sport, min_sample_size),
        "source_rating_live": build_source_rating_report(outcomes_list, "live", sport, min_sample_size),
        "source_rating_pregraded": build_source_rating_report(outcomes_list, "pregraded", sport, min_sample_size),
        "source_rating_combined": build_source_rating_report(outcomes_list, COMBINED_GRADE_MODE, sport, min_sample_size),
        "source_rating_market_stat_live": build_source_rating_detail_report(outcomes_list, "live", sport, min_sample_size, include_direction=False),
        "source_rating_market_stat_pregraded": build_source_rating_detail_report(outcomes_list, "pregraded", sport, min_sample_size, include_direction=False),
        "source_rating_market_stat_combined": build_source_rating_detail_report(outcomes_list, COMBINED_GRADE_MODE, sport, min_sample_size, include_direction=False),
        "source_rating_market_stat_direction_live": build_source_rating_detail_report(outcomes_list, "live", sport, min_sample_size, include_direction=True),
        "source_rating_market_stat_direction_pregraded": build_source_rating_detail_report(outcomes_list, "pregraded", sport, min_sample_size, include_direction=True),
        "source_rating_market_stat_direction_combined": build_source_rating_detail_report(outcomes_list, COMBINED_GRADE_MODE, sport, min_sample_size, include_direction=True),
        "supports_market_stat_live": build_expert_detail_report(outcomes_list, "live", sport, min_sample_size, include_direction=False),
        "supports_market_stat_pregraded": build_expert_detail_report(outcomes_list, "pregraded", sport, min_sample_size, include_direction=False),
        "supports_market_stat_combined": build_expert_detail_report(outcomes_list, COMBINED_GRADE_MODE, sport, min_sample_size, include_direction=False),
        "supports_market_stat_direction_live": build_expert_detail_report(outcomes_list, "live", sport, min_sample_size, include_direction=True),
        "supports_market_stat_direction_pregraded": build_expert_detail_report(outcomes_list, "pregraded", sport, min_sample_size, include_direction=True),
        "supports_market_stat_direction_combined": build_expert_detail_report(outcomes_list, COMBINED_GRADE_MODE, sport, min_sample_size, include_direction=True),
        "agreement_live": build_agreement_report(outcomes_list, "live", sport, min_sample_size),
        "agreement_pregraded": build_agreement_report(outcomes_list, "pregraded", sport, min_sample_size),
        "agreement_combined": build_agreement_report(outcomes_list, COMBINED_GRADE_MODE, sport, min_sample_size),
        "agreement_market_stat_live": build_agreement_detail_report(outcomes_list, "live", sport, min_sample_size, include_direction=False),
        "agreement_market_stat_pregraded": build_agreement_detail_report(outcomes_list, "pregraded", sport, min_sample_size, include_direction=False),
        "agreement_market_stat_combined": build_agreement_detail_report(outcomes_list, COMBINED_GRADE_MODE, sport, min_sample_size, include_direction=False),
        "agreement_market_stat_direction_live": build_agreement_detail_report(outcomes_list, "live", sport, min_sample_size, include_direction=True),
        "agreement_market_stat_direction_pregraded": build_agreement_detail_report(outcomes_list, "pregraded", sport, min_sample_size, include_direction=True),
        "agreement_market_stat_direction_combined": build_agreement_detail_report(outcomes_list, COMBINED_GRADE_MODE, sport, min_sample_size, include_direction=True),
        "audit": build_audit_report(outcomes_list, sport),
    }


def write_reports(
    reports: Dict[str, Dict[str, Any]],
    sport: str,
    report_dir: Optional[Path] = None,
) -> Dict[str, Path]:
    base_dir = report_dir or get_report_dir(sport)
    paths = {
        "supports_live": base_dir / "by_expert_supports_live.json",
        "supports_pregraded": base_dir / "by_expert_supports_pregraded.json",
        "supports_combined": base_dir / "by_expert_supports_combined.json",
        "supports_default": base_dir / "by_expert_supports.json",
        "source_rating_live": base_dir / "by_source_rating_supports_live.json",
        "source_rating_pregraded": base_dir / "by_source_rating_supports_pregraded.json",
        "source_rating_combined": base_dir / "by_source_rating_supports_combined.json",
        "source_rating_market_stat_live": base_dir / "by_source_rating_supports_market_stat_live.json",
        "source_rating_market_stat_pregraded": base_dir / "by_source_rating_supports_market_stat_pregraded.json",
        "source_rating_market_stat_combined": base_dir / "by_source_rating_supports_market_stat_combined.json",
        "source_rating_market_stat_direction_live": base_dir / "by_source_rating_supports_market_stat_direction_live.json",
        "source_rating_market_stat_direction_pregraded": base_dir / "by_source_rating_supports_market_stat_direction_pregraded.json",
        "source_rating_market_stat_direction_combined": base_dir / "by_source_rating_supports_market_stat_direction_combined.json",
        "supports_market_stat_live": base_dir / "by_expert_supports_market_stat_live.json",
        "supports_market_stat_pregraded": base_dir / "by_expert_supports_market_stat_pregraded.json",
        "supports_market_stat_combined": base_dir / "by_expert_supports_market_stat_combined.json",
        "supports_market_stat_direction_live": base_dir / "by_expert_supports_market_stat_direction_live.json",
        "supports_market_stat_direction_pregraded": base_dir / "by_expert_supports_market_stat_direction_pregraded.json",
        "supports_market_stat_direction_combined": base_dir / "by_expert_supports_market_stat_direction_combined.json",
        "agreement_live": base_dir / "by_expert_agreement_live.json",
        "agreement_pregraded": base_dir / "by_expert_agreement_pregraded.json",
        "agreement_combined": base_dir / "by_expert_agreement_combined.json",
        "agreement_default": base_dir / "by_expert_agreement.json",
        "agreement_market_stat_live": base_dir / "by_expert_agreement_market_stat_live.json",
        "agreement_market_stat_pregraded": base_dir / "by_expert_agreement_market_stat_pregraded.json",
        "agreement_market_stat_combined": base_dir / "by_expert_agreement_market_stat_combined.json",
        "agreement_market_stat_direction_live": base_dir / "by_expert_agreement_market_stat_direction_live.json",
        "agreement_market_stat_direction_pregraded": base_dir / "by_expert_agreement_market_stat_direction_pregraded.json",
        "agreement_market_stat_direction_combined": base_dir / "by_expert_agreement_market_stat_direction_combined.json",
        "audit": base_dir / "expert_record_trust_audit.json",
    }
    write_json(paths["supports_live"], reports["supports_live"])
    write_json(paths["supports_pregraded"], reports["supports_pregraded"])
    write_json(paths["supports_combined"], reports["supports_combined"])
    write_json(paths["supports_default"], reports["supports_combined"])
    write_json(paths["source_rating_live"], reports["source_rating_live"])
    write_json(paths["source_rating_pregraded"], reports["source_rating_pregraded"])
    write_json(paths["source_rating_combined"], reports["source_rating_combined"])
    write_rows_csv(paths["supports_live"].with_suffix(".csv"), reports["supports_live"])
    write_rows_csv(paths["supports_pregraded"].with_suffix(".csv"), reports["supports_pregraded"])
    write_rows_csv(paths["supports_combined"].with_suffix(".csv"), reports["supports_combined"])
    write_rows_csv(paths["supports_default"].with_suffix(".csv"), reports["supports_combined"])
    write_rows_csv(paths["source_rating_live"].with_suffix(".csv"), reports["source_rating_live"])
    write_rows_csv(paths["source_rating_pregraded"].with_suffix(".csv"), reports["source_rating_pregraded"])
    write_rows_csv(paths["source_rating_combined"].with_suffix(".csv"), reports["source_rating_combined"])
    write_json(paths["source_rating_market_stat_live"], reports["source_rating_market_stat_live"])
    write_json(paths["source_rating_market_stat_pregraded"], reports["source_rating_market_stat_pregraded"])
    write_json(paths["source_rating_market_stat_combined"], reports["source_rating_market_stat_combined"])
    write_json(paths["source_rating_market_stat_direction_live"], reports["source_rating_market_stat_direction_live"])
    write_json(paths["source_rating_market_stat_direction_pregraded"], reports["source_rating_market_stat_direction_pregraded"])
    write_json(paths["source_rating_market_stat_direction_combined"], reports["source_rating_market_stat_direction_combined"])
    write_rows_csv(paths["source_rating_market_stat_live"].with_suffix(".csv"), reports["source_rating_market_stat_live"])
    write_rows_csv(paths["source_rating_market_stat_pregraded"].with_suffix(".csv"), reports["source_rating_market_stat_pregraded"])
    write_rows_csv(paths["source_rating_market_stat_combined"].with_suffix(".csv"), reports["source_rating_market_stat_combined"])
    write_rows_csv(paths["source_rating_market_stat_direction_live"].with_suffix(".csv"), reports["source_rating_market_stat_direction_live"])
    write_rows_csv(paths["source_rating_market_stat_direction_pregraded"].with_suffix(".csv"), reports["source_rating_market_stat_direction_pregraded"])
    write_rows_csv(paths["source_rating_market_stat_direction_combined"].with_suffix(".csv"), reports["source_rating_market_stat_direction_combined"])
    write_json(paths["supports_market_stat_live"], reports["supports_market_stat_live"])
    write_json(paths["supports_market_stat_pregraded"], reports["supports_market_stat_pregraded"])
    write_json(paths["supports_market_stat_combined"], reports["supports_market_stat_combined"])
    write_json(paths["supports_market_stat_direction_live"], reports["supports_market_stat_direction_live"])
    write_json(paths["supports_market_stat_direction_pregraded"], reports["supports_market_stat_direction_pregraded"])
    write_json(paths["supports_market_stat_direction_combined"], reports["supports_market_stat_direction_combined"])
    write_rows_csv(paths["supports_market_stat_live"].with_suffix(".csv"), reports["supports_market_stat_live"])
    write_rows_csv(paths["supports_market_stat_pregraded"].with_suffix(".csv"), reports["supports_market_stat_pregraded"])
    write_rows_csv(paths["supports_market_stat_combined"].with_suffix(".csv"), reports["supports_market_stat_combined"])
    write_rows_csv(paths["supports_market_stat_direction_live"].with_suffix(".csv"), reports["supports_market_stat_direction_live"])
    write_rows_csv(paths["supports_market_stat_direction_pregraded"].with_suffix(".csv"), reports["supports_market_stat_direction_pregraded"])
    write_rows_csv(paths["supports_market_stat_direction_combined"].with_suffix(".csv"), reports["supports_market_stat_direction_combined"])
    write_json(paths["agreement_live"], reports["agreement_live"])
    write_json(paths["agreement_pregraded"], reports["agreement_pregraded"])
    write_json(paths["agreement_combined"], reports["agreement_combined"])
    write_json(paths["agreement_default"], reports["agreement_combined"])
    write_rows_csv(paths["agreement_live"].with_suffix(".csv"), reports["agreement_live"])
    write_rows_csv(paths["agreement_pregraded"].with_suffix(".csv"), reports["agreement_pregraded"])
    write_rows_csv(paths["agreement_combined"].with_suffix(".csv"), reports["agreement_combined"])
    write_rows_csv(paths["agreement_default"].with_suffix(".csv"), reports["agreement_combined"])
    write_json(paths["agreement_market_stat_live"], reports["agreement_market_stat_live"])
    write_json(paths["agreement_market_stat_pregraded"], reports["agreement_market_stat_pregraded"])
    write_json(paths["agreement_market_stat_combined"], reports["agreement_market_stat_combined"])
    write_json(paths["agreement_market_stat_direction_live"], reports["agreement_market_stat_direction_live"])
    write_json(paths["agreement_market_stat_direction_pregraded"], reports["agreement_market_stat_direction_pregraded"])
    write_json(paths["agreement_market_stat_direction_combined"], reports["agreement_market_stat_direction_combined"])
    write_rows_csv(paths["agreement_market_stat_live"].with_suffix(".csv"), reports["agreement_market_stat_live"])
    write_rows_csv(paths["agreement_market_stat_pregraded"].with_suffix(".csv"), reports["agreement_market_stat_pregraded"])
    write_rows_csv(paths["agreement_market_stat_combined"].with_suffix(".csv"), reports["agreement_market_stat_combined"])
    write_rows_csv(paths["agreement_market_stat_direction_live"].with_suffix(".csv"), reports["agreement_market_stat_direction_live"])
    write_rows_csv(paths["agreement_market_stat_direction_pregraded"].with_suffix(".csv"), reports["agreement_market_stat_direction_pregraded"])
    write_rows_csv(paths["agreement_market_stat_direction_combined"].with_suffix(".csv"), reports["agreement_market_stat_direction_combined"])
    write_json(paths["audit"], reports["audit"])
    return paths


def main() -> None:
    parser = argparse.ArgumentParser(description="Build trusted expert support/agreement reports from expert pick outcomes")
    parser.add_argument("--sport", choices=[NBA_SPORT, NCAAB_SPORT], default=NBA_SPORT)
    parser.add_argument("--outcomes", type=Path, default=DEFAULT_OUTCOMES_PATH)
    args = parser.parse_args()

    outcomes = read_jsonl(args.outcomes)
    reports = build_reports_from_outcomes(outcomes, sport=args.sport, min_sample_size=MIN_SAMPLE_SIZE_EXPERT)
    paths = write_reports(reports, sport=args.sport)

    legacy_rows = load_legacy_rows(get_legacy_report_path(args.sport))
    summary = {
        "paths": {key: str(path) for key, path in paths.items()},
        "supports_live_meta": reports["supports_live"]["meta"],
        "supports_pregraded_meta": reports["supports_pregraded"]["meta"],
        "agreement_live_meta": reports["agreement_live"]["meta"],
        "agreement_pregraded_meta": reports["agreement_pregraded"]["meta"],
        "audit_meta": reports["audit"]["meta"],
    }
    summary.update(summarize_top_10(reports["supports_live"], legacy_rows))
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
