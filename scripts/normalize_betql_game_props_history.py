#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.normalizer_betql_nba import normalize_betql_prop_record
from store import ensure_dir

logger = logging.getLogger("betql.normalize.game_props")


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def date_range(start: date, end: date) -> List[date]:
    if end < start:
        raise ValueError("end date must be >= start date")
    days: List[date] = []
    cur = start
    while cur <= end:
        days.append(cur)
        cur += timedelta(days=1)
    return days


def read_jsonl(path: Path) -> List[Dict]:
    rows: List[Dict] = []
    if not path.exists():
        return rows
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def write_jsonl_atomic(path: Path, rows: Iterable[Dict]) -> None:
    ensure_dir(str(path.parent))
    fd, tmp_path = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
                f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        raise


def enrich_date_fields(raw: Dict, day: date) -> Dict:
    enriched = dict(raw)
    enriched.setdefault(
        "event_start_time_utc",
        datetime(day.year, day.month, day.day, tzinfo=timezone.utc).isoformat(),
    )
    enriched.setdefault("day_key", f"NBA:{day.year:04d}:{day.month:02d}:{day.day:02d}")
    enriched.setdefault("date_str", day.isoformat())
    return enriched


def process_file(in_path: Path, out_path: Path, day: date, overwrite: bool, debug: bool) -> int:
    if out_path.exists() and not overwrite:
        if debug:
            logger.debug("skip existing %s", out_path)
        return 0
    raw_rows = read_jsonl(in_path)
    normalized: List[Dict] = []
    for raw in raw_rows:
        enriched = enrich_date_fields(raw, day)
        rec = normalize_betql_prop_record(enriched)
        normalized.append(rec)
    write_jsonl_atomic(out_path, normalized)
    return len(normalized)


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize BetQL game-page props history JSONL files.")
    parser.add_argument("--in-root", default="data/history/betql_game_props_raw")
    parser.add_argument("--out-root", default="data/history/betql_normalized/props")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    start = parse_date(args.start)
    end = parse_date(args.end)
    days = date_range(start, end)

    total_written = 0
    for day in days:
        in_path = Path(args.in_root) / f"{day.isoformat()}.jsonl"
        out_path = Path(args.out_root) / f"{day.isoformat()}.jsonl"
        if not in_path.exists():
            if args.debug:
                logger.debug("input missing %s", in_path)
            continue
        if args.dry_run:
            logger.info("[dry-run] would normalize %s -> %s", in_path, out_path)
            continue
        count = process_file(in_path, out_path, day, overwrite=args.overwrite, debug=args.debug)
        total_written += count
        logger.info("normalized %s rows for %s -> %s", count, day.isoformat(), out_path)

    logger.info("done. total normalized rows=%s", total_written)


if __name__ == "__main__":
    main()
