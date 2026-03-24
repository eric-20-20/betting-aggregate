#!/usr/bin/env python3
"""
One-time backfill: push all rows from graded_occurrences_latest.jsonl
to the Supabase graded_occurrences table.

Run this once after creating the table with the new schema.
Subsequent daily pushes are handled by push_to_supabase.py automatically.

Usage:
  python3 scripts/backfill_occurrences_supabase.py
  python3 scripts/backfill_occurrences_supabase.py --dry-run   # count rows only
  python3 scripts/backfill_occurrences_supabase.py --debug
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

OCCURRENCES_FILE = REPO_ROOT / "data" / "analysis" / "graded_occurrences_latest.jsonl"
BATCH_SIZE = 500


def main():
    parser = argparse.ArgumentParser(description="Backfill graded_occurrences to Supabase")
    parser.add_argument("--dry-run", action="store_true",
                        help="Count rows and validate without pushing to Supabase")
    parser.add_argument("--start-row", type=int, default=0,
                        help="Skip the first N rows (resume after a partial run)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger("supabase.backfill_occurrences")

    if not OCCURRENCES_FILE.exists():
        logger.error("File not found: %s", OCCURRENCES_FILE)
        sys.exit(1)

    logger.info("Loading %s ...", OCCURRENCES_FILE)
    with open(OCCURRENCES_FILE) as f:
        records = [json.loads(l) for l in f if l.strip()]

    total = len(records)
    graded = sum(1 for r in records if r.get("result") in ("WIN", "LOSS", "PUSH"))
    logger.info("Loaded %d rows (%d graded WIN/LOSS/PUSH)", total, graded)

    if args.dry_run:
        # Quick validation: check for missing occurrence_id
        missing_id = [i for i, r in enumerate(records) if not r.get("occurrence_id")]
        if missing_id:
            logger.warning("%d rows have no occurrence_id (rows: %s...)",
                           len(missing_id), missing_id[:5])
        else:
            logger.info("All rows have occurrence_id — OK")
        logger.info("Dry run complete. Would push %d rows to Supabase.", total)
        return

    try:
        from src.supabase_writer import upsert_occurrences
    except RuntimeError as e:
        logger.error("Supabase not configured: %s", e)
        sys.exit(1)

    if args.start_row:
        records = records[args.start_row:]
        logger.info("Resuming from row %d (%d rows remaining)", args.start_row, len(records))

    pushed = 0
    batch_num = 0
    batch_count = (len(records) + BATCH_SIZE - 1) // BATCH_SIZE
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        batch_num += 1
        n = upsert_occurrences(batch)
        pushed += len(batch)
        logger.info("Batch %d/%d — pushed %d rows (total so far: %d/%d)",
                    batch_num, batch_count, len(batch), pushed, total)

    logger.info("Backfill complete. Pushed %d / %d rows.", pushed, total)
    logger.info("graded_occurrences table is now ready for pattern_analysis_supabase.py")


if __name__ == "__main__":
    main()
