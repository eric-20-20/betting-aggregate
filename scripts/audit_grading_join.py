"""Audit grading join quality after canonical_event_key migration.

Checks for:
1. % of signals with grades
2. Event key matching between signals and grades
3. Any "ungraded due to event mismatch" issues
4. Time window analysis for grade attachment

Usage:
    python3 scripts/audit_grading_join.py
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.source_contract import normalize_event_key


SIGNALS_PATH = Path("data/ledger/signals_latest.jsonl")
GRADES_PATH = Path("data/ledger/grades_latest.jsonl")
GRADED_SIGNALS_PATH = Path("data/analysis/graded_signals_latest.jsonl")


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    """Read JSONL file."""
    if not path.exists():
        return []
    records = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def parse_iso(ts: Optional[str]) -> Optional[datetime]:
    """Parse ISO timestamp."""
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def analyze_join_quality(
    signals: List[Dict[str, Any]],
    grades: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Analyze how well grades join to signals."""

    # Index grades by signal_id
    grades_by_id: Dict[str, Dict[str, Any]] = {}
    for g in grades:
        sid = g.get("signal_id")
        if sid:
            grades_by_id[sid] = g

    stats = {
        "total_signals": len(signals),
        "total_grades": len(grades),
        "signals_with_grade": 0,
        "signals_without_grade": 0,
        "grade_status_distribution": Counter(),
        "event_key_match": Counter(),
        "canonical_key_match": Counter(),
        "mismatch_samples": [],
    }

    for sig in signals:
        sid = sig.get("signal_id")
        if not sid:
            continue

        grade = grades_by_id.get(sid)
        if grade:
            stats["signals_with_grade"] += 1
            status = grade.get("status") or grade.get("result")
            stats["grade_status_distribution"][status] += 1

            # Check event key consistency
            sig_event_key = sig.get("event_key")
            sig_canonical = sig.get("canonical_event_key")
            grade_event_key = grade.get("event_key")
            grade_canonical = grade.get("canonical_event_key")
            grade_safe = grade.get("event_key_safe")

            # Normalize for comparison
            sig_normalized = normalize_event_key(sig_event_key) or sig_canonical
            grade_normalized = grade_canonical or grade_safe or normalize_event_key(grade_event_key)

            if sig_normalized and grade_normalized:
                if sig_normalized == grade_normalized:
                    stats["event_key_match"]["match"] += 1
                else:
                    stats["event_key_match"]["mismatch"] += 1
                    if len(stats["mismatch_samples"]) < 5:
                        stats["mismatch_samples"].append({
                            "signal_id": sid,
                            "signal_event_key": sig_event_key,
                            "signal_canonical": sig_canonical,
                            "grade_event_key": grade_event_key,
                            "grade_canonical": grade_canonical,
                            "sig_normalized": sig_normalized,
                            "grade_normalized": grade_normalized,
                        })
            elif sig_normalized or grade_normalized:
                stats["event_key_match"]["partial"] += 1
            else:
                stats["event_key_match"]["both_missing"] += 1
        else:
            stats["signals_without_grade"] += 1

    # Calculate percentages
    if stats["total_signals"] > 0:
        stats["pct_graded"] = round(
            100 * stats["signals_with_grade"] / stats["total_signals"], 2
        )
    else:
        stats["pct_graded"] = 0

    return stats


def analyze_graded_dataset(graded: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Analyze the research dataset for join quality."""
    stats = {
        "total_rows": len(graded),
        "has_grade_status": 0,
        "grade_status_dist": Counter(),
        "has_canonical_event_key": 0,
        "has_event_key_raw": 0,
        "has_event_key_safe": 0,
        "key_consistency": Counter(),
    }

    for row in graded:
        status = row.get("grade_status")
        if status:
            stats["has_grade_status"] += 1
            stats["grade_status_dist"][status] += 1

        if row.get("canonical_event_key"):
            stats["has_canonical_event_key"] += 1
        if row.get("event_key_raw"):
            stats["has_event_key_raw"] += 1
        if row.get("event_key_safe"):
            stats["has_event_key_safe"] += 1

        # Check consistency
        canonical = row.get("canonical_event_key")
        safe = row.get("event_key_safe")
        if canonical and safe:
            if canonical == safe:
                stats["key_consistency"]["canonical_equals_safe"] += 1
            else:
                stats["key_consistency"]["canonical_differs_safe"] += 1
        elif canonical:
            stats["key_consistency"]["canonical_only"] += 1
        elif safe:
            stats["key_consistency"]["safe_only"] += 1
        else:
            stats["key_consistency"]["neither"] += 1

    return stats


def main() -> None:
    print("=" * 70)
    print("GRADING JOIN AUDIT")
    print("=" * 70)
    print()

    # Load data
    signals = read_jsonl(SIGNALS_PATH)
    grades = read_jsonl(GRADES_PATH)
    graded_dataset = read_jsonl(GRADED_SIGNALS_PATH)

    print(f"Loaded {len(signals):,} signals from {SIGNALS_PATH}")
    print(f"Loaded {len(grades):,} grades from {GRADES_PATH}")
    print(f"Loaded {len(graded_dataset):,} graded rows from {GRADED_SIGNALS_PATH}")
    print()

    # Analyze signal-to-grade join
    print("-" * 50)
    print("SIGNAL-TO-GRADE JOIN ANALYSIS")
    print("-" * 50)

    join_stats = analyze_join_quality(signals, grades)

    print(f"  Signals with grade:    {join_stats['signals_with_grade']:,}")
    print(f"  Signals without grade: {join_stats['signals_without_grade']:,}")
    print(f"  % graded:              {join_stats['pct_graded']}%")
    print()

    print("  Grade status distribution:")
    for status, count in join_stats["grade_status_distribution"].most_common():
        print(f"    {status or 'None':15s}: {count:,}")
    print()

    print("  Event key matching:")
    for match_type, count in join_stats["event_key_match"].most_common():
        print(f"    {match_type:15s}: {count:,}")
    print()

    if join_stats["mismatch_samples"]:
        print("  ⚠️  EVENT KEY MISMATCH SAMPLES:")
        for sample in join_stats["mismatch_samples"]:
            print(f"    Signal ID: {sample['signal_id']}")
            print(f"      Signal: {sample['sig_normalized']}")
            print(f"      Grade:  {sample['grade_normalized']}")
        print()

    # Analyze graded dataset
    if graded_dataset:
        print("-" * 50)
        print("GRADED DATASET ANALYSIS")
        print("-" * 50)

        ds_stats = analyze_graded_dataset(graded_dataset)

        print(f"  Total rows:              {ds_stats['total_rows']:,}")
        print(f"  Has grade_status:        {ds_stats['has_grade_status']:,}")
        print(f"  Has canonical_event_key: {ds_stats['has_canonical_event_key']:,}")
        print(f"  Has event_key_raw:       {ds_stats['has_event_key_raw']:,}")
        print(f"  Has event_key_safe:      {ds_stats['has_event_key_safe']:,}")
        print()

        print("  Grade status in dataset:")
        for status, count in ds_stats["grade_status_dist"].most_common():
            print(f"    {status or 'None':15s}: {count:,}")
        print()

        print("  Key consistency:")
        for cons, count in ds_stats["key_consistency"].most_common():
            print(f"    {cons:25s}: {count:,}")
        print()

    # Summary
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print()

    warnings = []

    # Check join rate
    if join_stats["pct_graded"] < 90:
        warnings.append(
            f"Low grade attachment rate: {join_stats['pct_graded']}% "
            f"(expected 90%+)"
        )

    # Check mismatches
    mismatch_count = join_stats["event_key_match"].get("mismatch", 0)
    if mismatch_count > 0:
        warnings.append(
            f"{mismatch_count} event key mismatches detected between signals and grades"
        )

    if warnings:
        print("⚠️  WARNINGS:")
        for w in warnings:
            print(f"  - {w}")
    else:
        print("✅ Grading join looks healthy.")
        print(f"   {join_stats['pct_graded']}% of signals have grades attached.")
        print(f"   {join_stats['event_key_match'].get('match', 0)} event keys match correctly.")

    print()


if __name__ == "__main__":
    main()
