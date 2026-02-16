"""Audit ledger continuity after canonical_event_key migration.

Checks for:
1. Signal key fragmentation (no accidental doubling)
2. Selection key continuity
3. Offer key continuity
4. Legacy vs canonical usage percentages

Usage:
    python3 scripts/audit_ledger_continuity.py
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.source_contract import is_canonical_event_key, is_legacy_event_key


LEDGER_FILES = {
    "signals_latest": Path("data/ledger/signals_latest.jsonl"),
    "signals_occurrences": Path("data/ledger/signals_occurrences.jsonl"),
    "grades_latest": Path("data/ledger/grades_latest.jsonl"),
    "grades_occurrences": Path("data/ledger/grades_occurrences.jsonl"),
}


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


def analyze_keys(records: List[Dict[str, Any]], file_name: str) -> Dict[str, Any]:
    """Analyze key distribution in records."""
    stats = {
        "total_records": len(records),
        "unique_signal_id": 0,
        "unique_signal_key": 0,
        "unique_selection_key": 0,
        "unique_offer_key": 0,
        "has_canonical_event_key": 0,
        "has_legacy_event_key_only": 0,
        "has_both_keys": 0,
        "no_event_key": 0,
        "event_key_format": Counter(),
    }

    signal_ids = set()
    signal_keys = set()
    selection_keys = set()
    offer_keys = set()

    # Track duplicates
    signal_key_counts = Counter()
    selection_key_counts = Counter()

    for rec in records:
        # Collect IDs
        sid = rec.get("signal_id")
        if sid:
            signal_ids.add(sid)

        skey = rec.get("signal_key")
        if skey:
            signal_keys.add(skey)
            signal_key_counts[skey] += 1

        selkey = rec.get("selection_key")
        if selkey:
            selection_keys.add(selkey)
            selection_key_counts[selkey] += 1

        okey = rec.get("offer_key")
        if okey:
            offer_keys.add(okey)

        # Analyze event key formats
        event_key = rec.get("event_key")
        canonical_key = rec.get("canonical_event_key")

        has_canonical = bool(canonical_key)
        has_legacy = is_legacy_event_key(event_key)
        has_canonical_in_event_key = is_canonical_event_key(event_key)

        if has_canonical and has_legacy:
            stats["has_both_keys"] += 1
            stats["event_key_format"]["both"] += 1
        elif has_canonical or has_canonical_in_event_key:
            stats["has_canonical_event_key"] += 1
            stats["event_key_format"]["canonical_only"] += 1
        elif has_legacy:
            stats["has_legacy_event_key_only"] += 1
            stats["event_key_format"]["legacy_only"] += 1
        elif event_key:
            # Has event_key but doesn't match either pattern
            stats["event_key_format"]["unknown"] += 1
        else:
            stats["no_event_key"] += 1
            stats["event_key_format"]["none"] += 1

    stats["unique_signal_id"] = len(signal_ids)
    stats["unique_signal_key"] = len(signal_keys)
    stats["unique_selection_key"] = len(selection_keys)
    stats["unique_offer_key"] = len(offer_keys)

    # Top duplicates
    stats["top_signal_key_dupes"] = signal_key_counts.most_common(10)
    stats["top_selection_key_dupes"] = selection_key_counts.most_common(10)

    # Fragmentation check
    if stats["unique_signal_id"] > 0:
        stats["signal_key_to_id_ratio"] = round(
            stats["unique_signal_key"] / stats["unique_signal_id"], 3
        )
    else:
        stats["signal_key_to_id_ratio"] = None

    # Legacy percentage
    total_with_keys = stats["total_records"] - stats["no_event_key"]
    if total_with_keys > 0:
        stats["pct_legacy_only"] = round(
            100 * stats["has_legacy_event_key_only"] / total_with_keys, 2
        )
        stats["pct_has_canonical"] = round(
            100 * (stats["has_canonical_event_key"] + stats["has_both_keys"]) / total_with_keys, 2
        )
    else:
        stats["pct_legacy_only"] = 0
        stats["pct_has_canonical"] = 0

    return stats


def check_fragmentation(stats: Dict[str, Any]) -> List[str]:
    """Check for signs of fragmentation."""
    warnings = []

    # Signal key to ID ratio should be ~1.0
    # If significantly > 1.0, we have fragmentation
    ratio = stats.get("signal_key_to_id_ratio")
    if ratio and ratio > 1.1:
        warnings.append(
            f"WARN: signal_key/signal_id ratio is {ratio} (expected ~1.0). "
            f"Possible key fragmentation."
        )

    # Legacy-only records should be going down over time
    pct_legacy = stats.get("pct_legacy_only", 0)
    if pct_legacy > 10:
        warnings.append(
            f"WARN: {pct_legacy}% of records still use legacy event_key only. "
            f"Consider re-running backfill."
        )

    return warnings


def main() -> None:
    print("=" * 70)
    print("LEDGER CONTINUITY AUDIT")
    print("=" * 70)
    print()

    all_warnings = []

    for name, path in LEDGER_FILES.items():
        print(f"Analyzing: {name}")
        print("-" * 50)

        records = read_jsonl(path)
        if not records:
            print(f"  [skip] File not found or empty: {path}")
            print()
            continue

        stats = analyze_keys(records, name)

        print(f"  Total records:          {stats['total_records']:,}")
        print(f"  Unique signal_id:       {stats['unique_signal_id']:,}")
        print(f"  Unique signal_key:      {stats['unique_signal_key']:,}")
        print(f"  Unique selection_key:   {stats['unique_selection_key']:,}")
        print(f"  Unique offer_key:       {stats['unique_offer_key']:,}")
        print()
        print(f"  Event key format breakdown:")
        for fmt, count in stats["event_key_format"].most_common():
            pct = 100 * count / stats["total_records"] if stats["total_records"] else 0
            print(f"    {fmt:20s}: {count:,} ({pct:.1f}%)")
        print()
        print(f"  Has canonical_event_key:  {stats['has_canonical_event_key']:,}")
        print(f"  Has both (legacy+canon):  {stats['has_both_keys']:,}")
        print(f"  Legacy event_key only:    {stats['has_legacy_event_key_only']:,}")
        print(f"  No event_key:             {stats['no_event_key']:,}")
        print()
        print(f"  % with canonical:         {stats['pct_has_canonical']}%")
        print(f"  % legacy only:            {stats['pct_legacy_only']}%")
        print()

        if stats.get("signal_key_to_id_ratio"):
            print(f"  Signal key/ID ratio:      {stats['signal_key_to_id_ratio']}")

        # Check for fragmentation
        warnings = check_fragmentation(stats)
        if warnings:
            print()
            for w in warnings:
                print(f"  ⚠️  {w}")
            all_warnings.extend(warnings)

        # Show top duplicates
        if stats["top_signal_key_dupes"]:
            print()
            print("  Top 5 signal_key duplicates:")
            for key, count in stats["top_signal_key_dupes"][:5]:
                if count > 1:
                    short_key = key[:50] + "..." if len(key) > 50 else key
                    print(f"    {count:4d}x  {short_key}")

        print()

    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)

    if all_warnings:
        print()
        print("⚠️  WARNINGS FOUND:")
        for w in all_warnings:
            print(f"  - {w}")
    else:
        print()
        print("✅ No fragmentation issues detected.")
        print("✅ Ledger continuity looks healthy.")

    print()


if __name__ == "__main__":
    main()
