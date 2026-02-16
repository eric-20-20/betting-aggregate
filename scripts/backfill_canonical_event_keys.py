"""Backfill canonical_event_key to existing ledger and output files.

This script adds canonical_event_key to records that have event_key but lack
canonical_event_key. It uses the normalize_event_key function to convert
legacy formats to canonical format.

Usage:
    # Dry run (preview changes)
    python3 scripts/backfill_canonical_event_keys.py --dry-run

    # Apply changes to all files
    python3 scripts/backfill_canonical_event_keys.py

    # Backfill specific file
    python3 scripts/backfill_canonical_event_keys.py --file data/ledger/signals_latest.jsonl
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

# Import the normalize function from source_contract
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.source_contract import normalize_event_key, is_canonical_event_key


# Files to backfill
LEDGER_FILES = [
    Path("data/ledger/signals_latest.jsonl"),
    Path("data/ledger/signals_occurrences.jsonl"),
    Path("data/ledger/grades_latest.jsonl"),
    Path("data/ledger/grades_occurrences.jsonl"),
]

ANALYSIS_FILES = [
    Path("data/analysis/graded_signals_latest.jsonl"),
    Path("data/analysis/graded_signals_occurrences.jsonl"),
    Path("data/analysis/graded_occurrences_latest.jsonl"),
]

OUTPUT_JSON_FILES = [
    Path("out/normalized_action_nba.json"),
    Path("out/normalized_covers_nba.json"),
    Path("out/normalized_betql_prop_nba.json"),
    Path("out/normalized_betql_sharp_nba.json"),
    Path("out/normalized_betql_spread_nba.json"),
    Path("out/normalized_betql_total_nba.json"),
]


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


def write_jsonl(path: Path, records: List[Dict[str, Any]]) -> None:
    """Write JSONL file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")))
            f.write("\n")


def read_json(path: Path) -> List[Dict[str, Any]]:
    """Read JSON file (array of records)."""
    if not path.exists():
        return []
    with path.open() as f:
        data = json.load(f)
    return data if isinstance(data, list) else []


def write_json(path: Path, records: List[Dict[str, Any]]) -> None:
    """Write JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)


def backfill_record(record: Dict[str, Any]) -> tuple[Dict[str, Any], bool]:
    """Add canonical_event_key to a record if missing.

    Returns: (updated_record, was_modified)
    """
    modified = False

    # Check top-level event_key
    if "event_key" in record and "canonical_event_key" not in record:
        canonical = normalize_event_key(record["event_key"])
        if canonical:
            record["canonical_event_key"] = canonical
            modified = True

    # Check event_key_safe (already canonical format)
    if "event_key_safe" in record and "canonical_event_key" not in record:
        event_key_safe = record["event_key_safe"]
        if event_key_safe and is_canonical_event_key(event_key_safe):
            record["canonical_event_key"] = event_key_safe
            modified = True

    # Check nested event dict (for normalized picks)
    if "event" in record and isinstance(record["event"], dict):
        event = record["event"]
        if "event_key" in event and "canonical_event_key" not in event:
            canonical = normalize_event_key(event["event_key"])
            if canonical:
                event["canonical_event_key"] = canonical
                modified = True

    return record, modified


def backfill_file(path: Path, dry_run: bool = False) -> Dict[str, int]:
    """Backfill canonical_event_key to a file.

    Returns: stats dict with counts
    """
    stats = {
        "total": 0,
        "modified": 0,
        "already_has_canonical": 0,
        "no_event_key": 0,
        "conversion_failed": 0,
    }

    if not path.exists():
        print(f"  [skip] {path} does not exist")
        return stats

    # Detect format
    is_jsonl = path.suffix == ".jsonl"

    if is_jsonl:
        records = read_jsonl(path)
    else:
        records = read_json(path)

    stats["total"] = len(records)
    updated_records = []

    for rec in records:
        # Check if already has canonical_event_key
        has_canonical = (
            "canonical_event_key" in rec or
            (isinstance(rec.get("event"), dict) and "canonical_event_key" in rec["event"])
        )

        if has_canonical:
            stats["already_has_canonical"] += 1
            updated_records.append(rec)
            continue

        # Check if has event_key to convert
        has_event_key = (
            "event_key" in rec or
            "event_key_safe" in rec or
            (isinstance(rec.get("event"), dict) and "event_key" in rec["event"])
        )

        if not has_event_key:
            stats["no_event_key"] += 1
            updated_records.append(rec)
            continue

        # Try to backfill
        updated_rec, was_modified = backfill_record(rec)

        if was_modified:
            stats["modified"] += 1
        else:
            stats["conversion_failed"] += 1

        updated_records.append(updated_rec)

    # Write back if not dry run and there were modifications
    if not dry_run and stats["modified"] > 0:
        if is_jsonl:
            write_jsonl(path, updated_records)
        else:
            write_json(path, updated_records)
        print(f"  [write] {path}: {stats['modified']} records updated")
    else:
        action = "dry-run" if dry_run else "no changes"
        print(f"  [{action}] {path}: {stats['modified']} would be modified")

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill canonical_event_key to ledger files")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing")
    parser.add_argument("--file", type=str, help="Backfill specific file only")
    parser.add_argument("--include-output", action="store_true", help="Also backfill out/ JSON files")
    args = parser.parse_args()

    if args.file:
        files = [Path(args.file)]
    else:
        files = LEDGER_FILES + ANALYSIS_FILES
        if args.include_output:
            files += OUTPUT_JSON_FILES

    print(f"Backfilling canonical_event_key {'(dry run)' if args.dry_run else ''}")
    print(f"Files to process: {len(files)}")
    print()

    total_stats = {
        "total": 0,
        "modified": 0,
        "already_has_canonical": 0,
        "no_event_key": 0,
        "conversion_failed": 0,
    }

    for path in files:
        print(f"Processing: {path}")
        stats = backfill_file(path, dry_run=args.dry_run)
        for key in total_stats:
            total_stats[key] += stats[key]

    print()
    print("=" * 50)
    print("Summary:")
    print(f"  Total records:          {total_stats['total']}")
    print(f"  Already had canonical:  {total_stats['already_has_canonical']}")
    print(f"  Modified:               {total_stats['modified']}")
    print(f"  No event_key:           {total_stats['no_event_key']}")
    print(f"  Conversion failed:      {total_stats['conversion_failed']}")

    if args.dry_run:
        print()
        print("Run without --dry-run to apply changes.")


if __name__ == "__main__":
    main()
