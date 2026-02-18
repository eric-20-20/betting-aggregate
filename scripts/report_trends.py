"""Trend analysis report — find actionable patterns in graded betting data.

Outputs to data/reports/trends/:
  - by_consensus_strength.json    : 1-source vs 2 vs 3 vs 4+ agreement
  - by_market_type.json           : spread / total / player_prop / moneyline per combo
  - by_month.json                 : month-over-month performance
  - by_day_of_week.json           : day-of-week patterns
  - by_expert.json                : individual expert win rates (min 20 picks)
  - by_confidence_tier.json       : BetQL star ratings, SportsLine "Best Bet", etc.
  - top_trends_summary.json       : ranked list of strongest findings with sample-size flags

Run:
    python3 scripts/report_trends.py
    python3 scripts/report_trends.py --min-n 30   # custom min sample size
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ── Helpers ──────────────────────────────────────────────────────────────────

DATASET_PATH = Path("data/analysis/graded_occurrences_latest.jsonl")
REPORT_DIR = Path("data/reports/trends")


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
        json.dump(obj, f, ensure_ascii=False, indent=2)


def american_odds_to_units(odds: float, result: str) -> Optional[float]:
    """Convert American odds + WIN/LOSS/PUSH to profit in units (1u stake)."""
    if result == "PUSH":
        return 0.0
    if result == "WIN":
        if odds > 0:
            return odds / 100.0
        elif odds < 0:
            return 100.0 / abs(odds)
    if result == "LOSS":
        return -1.0
    return None


def wilson_lower(wins: int, n: int, z: float = 1.96) -> float:
    """Wilson score lower bound — conservative win% estimate."""
    if n == 0:
        return 0.0
    p = wins / n
    denom = 1 + z * z / n
    center = p + z * z / (2 * n)
    spread = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)
    return (center - spread) / denom


def build_record(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute W/L/P/ROI record from a list of graded rows."""
    wins = sum(1 for r in rows if r.get("result") == "WIN" or r.get("status") == "WIN")
    losses = sum(1 for r in rows if r.get("result") == "LOSS" or r.get("status") == "LOSS")
    pushes = sum(1 for r in rows if r.get("result") == "PUSH" or r.get("status") == "PUSH")
    n = wins + losses + pushes
    decided = wins + losses
    win_pct = wins / decided if decided > 0 else None

    # ROI from units
    total_units = 0.0
    bets_with_units = 0
    for r in rows:
        u = r.get("units")
        if u is not None:
            total_units += u
            bets_with_units += 1

    roi = total_units / bets_with_units if bets_with_units > 0 else None

    return {
        "n": n,
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "win_pct": round(win_pct, 4) if win_pct is not None else None,
        "wilson_lower": round(wilson_lower(wins, decided), 4) if decided > 0 else None,
        "bets_with_units": bets_with_units,
        "net_units": round(total_units, 3),
        "roi": round(roi, 4) if roi is not None else None,
    }


def flag_sample_size(record: Dict[str, Any], min_n: int) -> Dict[str, Any]:
    """Add sample size quality flag."""
    n = record.get("n", 0)
    if n < min_n:
        record["sample_flag"] = "INSUFFICIENT"
    elif n < min_n * 3:
        record["sample_flag"] = "LOW"
    else:
        record["sample_flag"] = "OK"
    return record


def parse_day_key(day_key: str) -> Optional[date]:
    """Parse 'NBA:2026:01:23' → date(2026, 1, 23)."""
    if not day_key:
        return None
    parts = day_key.split(":")
    if len(parts) >= 4:
        try:
            return date(int(parts[1]), int(parts[2]), int(parts[3]))
        except (ValueError, IndexError):
            return None
    return None


def get_source_count(row: Dict[str, Any]) -> int:
    """Count how many distinct sources back this signal."""
    sp = row.get("sources_present") or row.get("signal_sources_present") or []
    return len(sp) if isinstance(sp, list) else 1


# ── Filters ──────────────────────────────────────────────────────────────────

def is_graded(row: Dict[str, Any]) -> bool:
    """Only include rows with a final result (WIN/LOSS/PUSH)."""
    result = row.get("result") or row.get("status")
    return result in ("WIN", "LOSS", "PUSH")


# ── Report builders ──────────────────────────────────────────────────────────

def report_consensus_strength(rows: List[Dict[str, Any]], min_n: int) -> Dict[str, Any]:
    """Break down performance by number of agreeing sources."""
    groups: Dict[str, List] = defaultdict(list)
    for r in rows:
        sc = get_source_count(r)
        label = f"{sc}_source" if sc < 4 else "4+_sources"
        groups[label].append(r)

    results = []
    for label in sorted(groups.keys()):
        rec = build_record(groups[label])
        rec["consensus_strength"] = label
        flag_sample_size(rec, min_n)
        results.append(rec)
    return {"report": "consensus_strength", "rows": results}


def report_by_market_type(rows: List[Dict[str, Any]], min_n: int) -> Dict[str, Any]:
    """Performance by market type, crossed with consensus strength."""
    # Simple market type breakdown
    by_market: Dict[str, List] = defaultdict(list)
    # Market x consensus strength
    by_market_consensus: Dict[str, List] = defaultdict(list)

    for r in rows:
        mt = r.get("market_type") or "unknown"
        sc = get_source_count(r)
        sc_label = f"{sc}_source" if sc < 4 else "4+_sources"
        by_market[mt].append(r)
        by_market_consensus[f"{mt}|{sc_label}"].append(r)

    market_rows = []
    for mt in sorted(by_market.keys()):
        rec = build_record(by_market[mt])
        rec["market_type"] = mt
        flag_sample_size(rec, min_n)
        market_rows.append(rec)

    cross_rows = []
    for key in sorted(by_market_consensus.keys()):
        mt, sc_label = key.split("|")
        rec = build_record(by_market_consensus[key])
        rec["market_type"] = mt
        rec["consensus_strength"] = sc_label
        flag_sample_size(rec, min_n)
        cross_rows.append(rec)

    return {
        "report": "market_type",
        "by_market": market_rows,
        "by_market_x_consensus": cross_rows,
    }


def report_by_month(rows: List[Dict[str, Any]], min_n: int) -> Dict[str, Any]:
    """Month-over-month performance."""
    groups: Dict[str, List] = defaultdict(list)
    for r in rows:
        d = parse_day_key(r.get("day_key", ""))
        if d:
            groups[d.strftime("%Y-%m")].append(r)

    results = []
    for month in sorted(groups.keys()):
        rec = build_record(groups[month])
        rec["month"] = month
        flag_sample_size(rec, min_n)
        results.append(rec)
    return {"report": "by_month", "rows": results}


def report_by_day_of_week(rows: List[Dict[str, Any]], min_n: int) -> Dict[str, Any]:
    """Day-of-week patterns."""
    groups: Dict[str, List] = defaultdict(list)
    for r in rows:
        d = parse_day_key(r.get("day_key", ""))
        if d:
            groups[d.strftime("%A")].append(r)

    # Sort by day order
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    results = []
    for day in day_order:
        if day in groups:
            rec = build_record(groups[day])
            rec["day_of_week"] = day
            flag_sample_size(rec, min_n)
            results.append(rec)
    return {"report": "by_day_of_week", "rows": results}


def _extract_experts(row: Dict[str, Any]) -> List[str]:
    """Extract expert identifiers from a row, falling back to scalar fields."""
    experts = row.get("experts") or []
    if experts:
        return experts
    # Fallback: use expert_name or expert scalar field
    for field in ("expert_name", "expert"):
        val = row.get(field)
        if val and isinstance(val, str) and val.strip():
            return [val.strip()]
    return []


def report_by_expert(rows: List[Dict[str, Any]], min_n: int) -> Dict[str, Any]:
    """Individual expert performance. Only experts with >= min_n picks."""
    groups: Dict[str, List] = defaultdict(list)
    for r in rows:
        for exp in _extract_experts(r):
            groups[exp].append(r)

    results = []
    for expert in sorted(groups.keys()):
        grp = groups[expert]
        if len(grp) < min_n:
            continue
        rec = build_record(grp)
        rec["expert"] = expert

        # How many of this expert's picks are consensus (2+ sources)?
        consensus_count = sum(1 for r in grp if get_source_count(r) >= 2)
        rec["consensus_pct"] = round(consensus_count / len(grp), 4) if grp else 0
        rec["avg_sources"] = round(
            sum(get_source_count(r) for r in grp) / len(grp), 2
        ) if grp else 0

        flag_sample_size(rec, min_n)
        results.append(rec)

    # Sort by wilson_lower descending (conservative estimate)
    results.sort(key=lambda x: (x.get("wilson_lower") or 0), reverse=True)
    return {"report": "by_expert", "rows": results}


def report_by_source_surface(rows: List[Dict[str, Any]], min_n: int) -> Dict[str, Any]:
    """Performance by source_surface (proxy for pick type / confidence tier)."""
    groups: Dict[str, List] = defaultdict(list)
    for r in rows:
        surf = r.get("source_surface") or "unknown"
        groups[surf].append(r)

    results = []
    for surf in sorted(groups.keys()):
        if len(groups[surf]) < min_n:
            continue
        rec = build_record(groups[surf])
        rec["source_surface"] = surf
        flag_sample_size(rec, min_n)
        results.append(rec)

    results.sort(key=lambda x: x.get("n", 0), reverse=True)
    return {"report": "by_source_surface", "rows": results}


def report_consensus_x_market_detail(rows: List[Dict[str, Any]], min_n: int) -> Dict[str, Any]:
    """Detailed cross-tab: sources_combo × market_type for combos with 2+ sources."""
    groups: Dict[str, List] = defaultdict(list)
    for r in rows:
        sc = get_source_count(r)
        if sc < 2:
            continue  # skip solo picks
        combo = r.get("sources_combo") or "unknown"
        mt = r.get("market_type") or "unknown"
        groups[f"{combo}@@{mt}"].append(r)

    results = []
    for key in sorted(groups.keys()):
        grp = groups[key]
        if len(grp) < min_n:
            continue
        combo, mt = key.split("@@", 1)
        rec = build_record(grp)
        rec["sources_combo"] = combo
        rec["market_type"] = mt
        flag_sample_size(rec, min_n)
        results.append(rec)

    results.sort(key=lambda x: (x.get("win_pct") or 0), reverse=True)
    return {"report": "consensus_x_market", "rows": results}


def report_slate_size(rows: List[Dict[str, Any]], min_n: int) -> Dict[str, Any]:
    """Performance by number of games on the slate that day."""
    groups: Dict[str, List] = defaultdict(list)
    for r in rows:
        gi = r.get("games_info") or {}
        gc = gi.get("games_count") or gi.get("games_count_actual")
        if gc is None:
            continue
        if gc <= 3:
            bucket = "1-3 games"
        elif gc <= 6:
            bucket = "4-6 games"
        elif gc <= 10:
            bucket = "7-10 games"
        else:
            bucket = "11+ games"
        groups[bucket].append(r)

    bucket_order = ["1-3 games", "4-6 games", "7-10 games", "11+ games"]
    results = []
    for bucket in bucket_order:
        if bucket in groups:
            rec = build_record(groups[bucket])
            rec["slate_size"] = bucket
            flag_sample_size(rec, min_n)
            results.append(rec)
    return {"report": "by_slate_size", "rows": results}


def build_top_trends(all_reports: Dict[str, Any], min_n: int) -> Dict[str, Any]:
    """Rank the strongest trends across all reports."""
    trends = []

    # Extract notable rows from each report
    for report_name, report_data in all_reports.items():
        rows_list = report_data.get("rows") or report_data.get("by_market_x_consensus") or []
        for row in rows_list:
            n = row.get("n", 0)
            win_pct = row.get("win_pct")
            roi = row.get("roi")
            if win_pct is None or n < min_n:
                continue

            # Score: combination of win_pct edge over 50% and sample size
            edge = win_pct - 0.50
            if edge <= 0:
                continue  # only surface winning trends

            # Build description
            desc_parts = []
            for key in ["consensus_strength", "market_type", "sources_combo",
                         "month", "day_of_week", "expert", "source_surface", "slate_size"]:
                if key in row and row[key]:
                    desc_parts.append(f"{key}={row[key]}")
            desc = ", ".join(desc_parts) if desc_parts else report_name

            trends.append({
                "description": desc,
                "report": report_name,
                "n": n,
                "win_pct": win_pct,
                "wilson_lower": row.get("wilson_lower"),
                "roi": roi,
                "net_units": row.get("net_units"),
                "edge_over_50": round(edge, 4),
                "sample_flag": row.get("sample_flag", "UNKNOWN"),
            })

    # Sort by wilson_lower descending (conservative estimate of true win%)
    trends.sort(key=lambda x: (x.get("wilson_lower") or 0), reverse=True)

    return {
        "report": "top_trends_summary",
        "description": "Trends ranked by Wilson lower bound (conservative win% estimate). "
                       "Only includes trends with win% > 50% and n >= min_n.",
        "min_n": min_n,
        "count": len(trends),
        "trends": trends[:50],  # top 50
    }


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Trend analysis report")
    parser.add_argument("--min-n", type=int, default=20, help="Minimum sample size (default 20)")
    parser.add_argument("--dataset", default=str(DATASET_PATH), help="Path to graded occurrences JSONL")
    args = parser.parse_args()

    min_n = args.min_n
    dataset = Path(args.dataset)

    print(f"Loading {dataset}...")
    raw = read_jsonl(dataset)
    rows = [r for r in raw if is_graded(r)]
    print(f"Loaded {len(raw)} rows, {len(rows)} graded (W/L/P)")

    reports = {}

    print("  Building consensus strength report...")
    reports["consensus_strength"] = report_consensus_strength(rows, min_n)

    print("  Building market type report...")
    reports["market_type"] = report_by_market_type(rows, min_n)

    print("  Building monthly report...")
    reports["by_month"] = report_by_month(rows, min_n)

    print("  Building day-of-week report...")
    reports["by_day_of_week"] = report_by_day_of_week(rows, min_n)

    print("  Building expert report...")
    reports["by_expert"] = report_by_expert(rows, min_n)

    print("  Building source surface report...")
    reports["by_source_surface"] = report_by_source_surface(rows, min_n)

    print("  Building consensus × market detail...")
    reports["consensus_x_market"] = report_consensus_x_market_detail(rows, min_n)

    print("  Building slate size report...")
    reports["by_slate_size"] = report_slate_size(rows, min_n)

    print("  Ranking top trends...")
    top = build_top_trends(reports, min_n)

    # Write all reports
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    for name, data in reports.items():
        path = REPORT_DIR / f"{name}.json"
        write_json(path, data)
        row_count = len(data.get("rows") or data.get("by_market") or [])
        print(f"  Wrote {path} ({row_count} rows)")

    write_json(REPORT_DIR / "top_trends_summary.json", top)
    print(f"  Wrote {REPORT_DIR / 'top_trends_summary.json'} ({top['count']} trends)")

    # Print top 10 to console
    print(f"\n{'='*70}")
    print(f"  TOP TRENDS (by Wilson lower bound, min_n={min_n})")
    print(f"{'='*70}")
    for i, t in enumerate(top["trends"][:15], 1):
        flag = f" [{t['sample_flag']}]" if t["sample_flag"] != "OK" else ""
        roi_str = f"ROI {t['roi']:+.1%}" if t["roi"] is not None else "ROI n/a"
        print(f"  {i:2d}. {t['win_pct']:.1%} win ({t['n']:,} picks) | "
              f"wilson≥{t['wilson_lower']:.1%} | {roi_str} | "
              f"{t['description']}{flag}")

    # Print consensus strength summary
    cs = reports["consensus_strength"]
    print(f"\n{'='*70}")
    print(f"  CONSENSUS STRENGTH BREAKDOWN")
    print(f"{'='*70}")
    for row in cs["rows"]:
        roi_str = f"ROI {row['roi']:+.1%}" if row["roi"] is not None else "ROI n/a"
        print(f"  {row['consensus_strength']:>12s}: {row['win_pct']:.1%} win "
              f"({row['n']:,} picks) | {roi_str} | "
              f"units={row['net_units']:+.1f}{' ['+row['sample_flag']+']' if row['sample_flag'] != 'OK' else ''}")

    print(f"\nAll reports written to {REPORT_DIR}/")


if __name__ == "__main__":
    main()
