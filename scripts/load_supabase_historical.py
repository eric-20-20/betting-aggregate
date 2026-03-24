#!/usr/bin/env python3
"""
One-time historical data load into Supabase.

Loads:
  - data/ledger/signals_latest.jsonl  → signals + signal_sources + experts
  - data/ledger/grades_latest.jsonl   → grades
  - data/plays/plays_*.json           → plays

Usage:
  python3 scripts/load_supabase_historical.py
  python3 scripts/load_supabase_historical.py --dry-run
  python3 scripts/load_supabase_historical.py --table signals   # load only one table
"""
from __future__ import annotations

import argparse
import glob
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.supabase_writer import _dedup_ss_rows  # noqa: E402

logger = logging.getLogger("supabase.load")

SIGNALS_FILE = REPO_ROOT / "data" / "ledger" / "signals_latest.jsonl"
GRADES_FILE  = REPO_ROOT / "data" / "ledger" / "grades_latest.jsonl"
PLAYS_DIR    = REPO_ROOT / "data" / "plays"

BATCH_SIZE = 500
IN_BATCH_SIZE = 50   # for .in_() filters — SHA256 IDs are 64 chars, keep URL short


# ──────────────────────────────────────────────
# Client
# ──────────────────────────────────────────────

def get_client():
    """Return a Supabase client. Reads SUPABASE_URL and SUPABASE_KEY from env."""
    try:
        from supabase import create_client
    except ImportError:
        raise RuntimeError("supabase package not installed. Run: pip install supabase")

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        # Try loading from .env in repo root
        env_file = REPO_ROOT / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if "=" in line and not line.startswith("#"):
                    k, _, v = line.partition("=")
                    os.environ.setdefault(k.strip(), v.strip())
            url = os.environ.get("SUPABASE_URL")
            key = os.environ.get("SUPABASE_KEY")

    if not url or not key:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_KEY must be set in environment or .env file.\n"
            "Example .env:\n  SUPABASE_URL=https://xxxx.supabase.co\n  SUPABASE_KEY=eyJ..."
        )
    return create_client(url, key)


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def batched(lst: List, size: int):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def upsert_batch(client, table: str, rows: List[Dict], dry_run: bool = False) -> int:
    if not rows:
        return 0
    if dry_run:
        logger.debug("[DRY RUN] Would upsert %d rows into %s", len(rows), table)
        return len(rows)
    resp = client.table(table).upsert(rows, on_conflict=None).execute()
    return len(rows)


def upsert_batch_keyed(client, table: str, rows: List[Dict], conflict_col: str,
                       dry_run: bool = False) -> int:
    if not rows:
        return 0
    if dry_run:
        logger.debug("[DRY RUN] Would upsert %d rows into %s (conflict: %s)", len(rows), table, conflict_col)
        return len(rows)
    client.table(table).upsert(rows, on_conflict=conflict_col).execute()
    return len(rows)


# ──────────────────────────────────────────────
# Signals load
# ──────────────────────────────────────────────

def load_signals(client, dry_run: bool = False) -> None:
    logger.info("Loading signals from %s", SIGNALS_FILE)
    with open(SIGNALS_FILE) as f:
        raw_lines = [l.strip() for l in f if l.strip()]

    records = [json.loads(l) for l in raw_lines if l]

    # ── Pass 1: collect all unique experts ──────────────────────────
    # key → {source_id, slug, display_name}
    expert_map: Dict[str, Dict] = {}
    for r in records:
        for sup in r.get("supports") or []:
            source_id = sup.get("source_id")
            expert_name = sup.get("expert_name")
            if source_id and expert_name:
                slug = expert_name.lower().replace(" ", "_").replace(".", "")
                key = f"{source_id}|{slug}"
                if key not in expert_map:
                    expert_map[key] = {"source_id": source_id, "slug": slug,
                                       "display_name": expert_name, "profile_url": None}

    # Batch upsert all experts at once, get back their IDs
    expert_id_map: Dict[str, int] = {}  # "source_id|slug" → id
    if expert_map and not dry_run:
        logger.info("  upserting %d unique experts ...", len(expert_map))
        expert_rows = list(expert_map.values())
        for batch in [expert_rows[i:i+BATCH_SIZE] for i in range(0, len(expert_rows), BATCH_SIZE)]:
            resp = client.table("experts").upsert(
                batch, on_conflict="source_id,slug"
            ).execute()
            for row in (resp.data or []):
                k = f"{row['source_id']}|{row['slug']}"
                expert_id_map[k] = row["id"]
        # If upsert didn't return rows (some Supabase configs), fetch all
        if not expert_id_map:
            resp2 = client.table("experts").select("id, source_id, slug").execute()
            for row in (resp2.data or []):
                k = f"{row['source_id']}|{row['slug']}"
                expert_id_map[k] = row["id"]
        logger.info("  %d expert IDs resolved", len(expert_id_map))

    # ── Pass 2: build signal rows + signal_sources rows ─────────────
    signal_rows: List[Dict] = []
    ss_rows: List[Dict] = []

    for i, r in enumerate(records):
        sid = r.get("signal_id")
        if not sid:
            continue

        sources_present = r.get("sources_present") or r.get("sources", [])

        signal_rows.append({
            "signal_id":       sid,
            "day_key":         r.get("day_key"),
            "event_key":       r.get("event_key") or r.get("canonical_event_key"),
            "sport":           r.get("sport", "NBA"),
            "away_team":       r.get("away_team"),
            "home_team":       r.get("home_team"),
            "market_type":     r.get("market_type"),
            "selection":       r.get("selection"),
            "direction":       r.get("direction"),
            "line":            r.get("line"),
            "line_min":        r.get("line_min"),
            "line_max":        r.get("line_max"),
            "atomic_stat":     r.get("atomic_stat"),
            "player_key":      r.get("player_key") or r.get("player_id"),
            "sources_combo":   r.get("sources_combo"),
            "sources_count":   len(sources_present),
            "signal_type":     r.get("signal_type"),
            "score":           r.get("score"),
            "run_id":          r.get("run_id"),
            "observed_at_utc": r.get("observed_at_utc"),
        })

        for sup in r.get("supports") or []:
            source_id = sup.get("source_id")
            expert_name = sup.get("expert_name")
            expert_slug = None
            expert_id = None
            if expert_name:
                expert_slug = expert_name.lower().replace(" ", "_").replace(".", "")
            if source_id and expert_slug:
                expert_id = expert_id_map.get(f"{source_id}|{expert_slug}")

            ss_rows.append({
                "signal_id":    sid,
                "source_id":    source_id,
                "expert_id":    expert_id,
                "expert_slug":  expert_slug,
                "expert_name":  expert_name,
                "line":         sup.get("line"),
                "odds":         None,
                "raw_pick_text": sup.get("raw_pick_text"),
            })

        if (i + 1) % 5000 == 0:
            logger.info("  signals: built %d / %d ...", i + 1, len(records))

    # ── Pass 3: write signals then signal_sources ────────────────────
    logger.info("  writing %d signal rows ...", len(signal_rows))
    for batch_start in range(0, len(signal_rows), BATCH_SIZE):
        upsert_batch_keyed(client, "signals",
                           signal_rows[batch_start:batch_start + BATCH_SIZE],
                           "signal_id", dry_run)

    ss_rows = _dedup_ss_rows(ss_rows)
    logger.info("  writing %d signal_sources rows ...", len(ss_rows))
    if ss_rows and not dry_run:
        # Delete existing signal_sources for these signal_ids then re-insert,
        # mirroring the pattern in src/supabase_writer.py lines 169-175.
        signal_ids = list({r["signal_id"] for r in ss_rows})
        for del_start in range(0, len(signal_ids), IN_BATCH_SIZE):
            client.table("signal_sources").delete().in_(
                "signal_id", signal_ids[del_start:del_start + IN_BATCH_SIZE]
            ).execute()
        for batch_start in range(0, len(ss_rows), BATCH_SIZE):
            client.table("signal_sources").insert(
                ss_rows[batch_start:batch_start + BATCH_SIZE]
            ).execute()

    total = len(signal_rows) + len(ss_rows)
    logger.info("Signals load complete: %d total rows written", total)


# ──────────────────────────────────────────────
# Grades load
# ──────────────────────────────────────────────

def load_grades(client, dry_run: bool = False) -> None:
    logger.info("Loading grades from %s", GRADES_FILE)
    with open(GRADES_FILE) as f:
        raw_lines = [l.strip() for l in f if l.strip()]

    # Pre-fetch all signal_ids present in Supabase to avoid FK violations
    # (grades_latest.jsonl may reference signals evicted from signals_latest.jsonl)
    if not dry_run:
        logger.info("  fetching known signal_ids from Supabase ...")
        known_signal_ids: set = set()
        page_size = 1000
        offset = 0
        while True:
            resp = client.table("signals").select("signal_id").range(offset, offset + page_size - 1).execute()
            batch = resp.data or []
            for r in batch:
                known_signal_ids.add(r["signal_id"])
            if len(batch) < page_size:
                break
            offset += page_size
        logger.info("  %d known signal_ids loaded", len(known_signal_ids))
    else:
        known_signal_ids = None

    rows: List[Dict] = []
    total_loaded = 0
    skipped = 0

    for i, line in enumerate(raw_lines):
        r = json.loads(line)
        sid = r.get("signal_id")
        if not sid:
            continue

        if known_signal_ids is not None and sid not in known_signal_ids:
            skipped += 1
            continue

        rows.append({
            "signal_id":     sid,
            "day_key":       r.get("day_key"),
            "result":        r.get("result"),
            "status":        r.get("status"),
            "line":          r.get("line"),
            "odds":          r.get("odds"),
            "stat_value":    r.get("stat_value") or r.get("actual_stat_value"),
            "market_type":   r.get("market_type"),
            "selection":     r.get("selection"),
            "direction":     r.get("direction"),
            "player_key":    r.get("player_key"),
            "provider":      r.get("provider"),
            "graded_at_utc": r.get("graded_at_utc"),
            "notes":         r.get("notes"),
            "units":         r.get("units"),
        })

        if len(rows) >= BATCH_SIZE:
            upsert_batch_keyed(client, "grades", rows, "signal_id", dry_run)
            total_loaded += len(rows)
            rows = []

        if (i + 1) % 5000 == 0:
            logger.info("  grades: processed %d / %d ...", i + 1, len(raw_lines))

    if rows:
        upsert_batch_keyed(client, "grades", rows, "signal_id", dry_run)
        total_loaded += len(rows)

    logger.info("Grades load complete: %d rows written, %d skipped (orphaned signal_id)", total_loaded, skipped)


# ──────────────────────────────────────────────
# Plays load
# ──────────────────────────────────────────────

def load_plays(client, dry_run: bool = False) -> None:
    plays_files = sorted(glob.glob(str(PLAYS_DIR / "plays_*.json")))
    logger.info("Loading plays from %d files in %s", len(plays_files), PLAYS_DIR)

    # Pre-fetch known signal_ids to avoid FK violations
    if not dry_run:
        logger.info("  fetching known signal_ids from Supabase ...")
        known_signal_ids: set = set()
        page_size = 1000
        offset = 0
        while True:
            resp = client.table("signals").select("signal_id").range(offset, offset + page_size - 1).execute()
            batch = resp.data or []
            for r in batch:
                known_signal_ids.add(r["signal_id"])
            if len(batch) < page_size:
                break
            offset += page_size
        logger.info("  %d known signal_ids loaded", len(known_signal_ids))
    else:
        known_signal_ids = None

    rows: List[Dict] = []
    total_loaded = 0
    skipped = 0

    for path in plays_files:
        try:
            with open(path) as f:
                data = json.load(f)
        except Exception as e:
            logger.warning("Skipping %s: %s", path, e)
            continue

        date_str = data.get("meta", {}).get("date")
        if not date_str:
            # Parse from filename: plays_2026-03-17.json
            fname = Path(path).stem  # "plays_2026-03-17"
            date_str = fname.replace("plays_", "")

        for play in data.get("plays", []):
            sig = play.get("signal", {})
            signal_id = sig.get("signal_id")
            if not signal_id:
                continue

            if known_signal_ids is not None and signal_id not in known_signal_ids:
                skipped += 1
                continue

            matched = play.get("matched_pattern")
            pattern_id = matched.get("id") if isinstance(matched, dict) else matched

            rows.append({
                "date":            date_str,
                "signal_id":       signal_id,
                "tier":            play.get("tier"),
                "rank":            play.get("rank"),
                "wilson_score":    play.get("wilson_score"),
                "composite_score": play.get("composite_score"),
                "matched_pattern": pattern_id,
                "summary":         play.get("summary"),
                "factors":         json.dumps(play.get("factors")) if play.get("factors") else None,
                "expert_detail":   json.dumps(play.get("expert_detail")) if play.get("expert_detail") else None,
            })

        if len(rows) >= BATCH_SIZE:
            upsert_batch_keyed(client, "plays", rows, "date,signal_id", dry_run)
            total_loaded += len(rows)
            rows = []

    if rows:
        upsert_batch_keyed(client, "plays", rows, "date,signal_id", dry_run)
        total_loaded += len(rows)

    logger.info("Plays load complete: %d rows written, %d skipped (orphaned signal_id), from %d files",
                total_loaded, skipped, len(plays_files))


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Load historical data into Supabase")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be loaded without writing")
    parser.add_argument("--table", choices=["signals", "grades", "plays"],
                        help="Load only a specific table")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if args.dry_run:
        logger.info("DRY RUN — no data will be written to Supabase")

    client = get_client()
    logger.info("Connected to Supabase")

    if not args.table or args.table == "signals":
        load_signals(client, dry_run=args.dry_run)

    if not args.table or args.table == "grades":
        load_grades(client, dry_run=args.dry_run)

    if not args.table or args.table == "plays":
        load_plays(client, dry_run=args.dry_run)

    logger.info("Done.")


if __name__ == "__main__":
    main()
