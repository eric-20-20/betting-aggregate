#!/usr/bin/env python3
"""
Post-pipeline Supabase push.

Called at the end of run_daily.sh to sync today's pipeline output to Supabase.
Non-blocking: exits 0 even on Supabase errors so the pipeline itself isn't disrupted.

Usage:
  python3 scripts/push_to_supabase.py                # push today's files
  python3 scripts/push_to_supabase.py --date 2026-03-17
  python3 scripts/push_to_supabase.py --date 2026-03-17 --debug
"""
from __future__ import annotations

import argparse
import datetime
import json
import logging
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))


def main() -> None:
    parser = argparse.ArgumentParser(description="Push today's pipeline output to Supabase")
    parser.add_argument("--date", default=None, help="Date in YYYY-MM-DD format (default: today)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger("supabase.push")

    date_str = args.date or datetime.date.today().isoformat()

    signals_file     = str(REPO_ROOT / "data" / "ledger" / "signals_latest.jsonl")
    grades_file      = str(REPO_ROOT / "data" / "ledger" / "grades_latest.jsonl")
    plays_file       = str(REPO_ROOT / "data" / "plays" / f"plays_{date_str}.json")
    occurrences_file = str(REPO_ROOT / "data" / "analysis" / "graded_occurrences_latest.jsonl")

    logger.info("Pushing pipeline output for %s to Supabase", date_str)
    logger.info("  signals:     %s", signals_file)
    logger.info("  grades:      %s", grades_file)
    logger.info("  plays:       %s", plays_file)
    logger.info("  occurrences: %s", occurrences_file)

    try:
        from src.supabase_writer import push_daily_run, upsert_occurrences
        push_daily_run(signals_file, grades_file, plays_file)

        # Push graded occurrences (primary pattern analysis table)
        if os.path.exists(occurrences_file):
            with open(occurrences_file) as f:
                occ_records = [json.loads(l) for l in f if l.strip()]
            n = upsert_occurrences(occ_records)
            logger.info("Pushed %d occurrence rows from %s", n, occurrences_file)
        else:
            logger.debug("Occurrences file not found: %s", occurrences_file)

        logger.info("Supabase push complete for %s", date_str)
    except RuntimeError as e:
        # Missing credentials — warn but don't fail the pipeline
        logger.warning("Supabase push skipped: %s", e)
        sys.exit(0)
    except Exception as e:
        logger.error("Supabase push failed: %s", e, exc_info=args.debug)
        # Exit 0 so run_daily.sh continues even if Supabase is down
        sys.exit(0)


if __name__ == "__main__":
    main()
