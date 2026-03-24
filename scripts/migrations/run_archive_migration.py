"""
Migration: Move stale JuiceReel history signals from signals/grades into
juicereel_history_archive.

Usage:
  python3 scripts/migrations/run_archive_migration.py --dry-run     # counts only, no writes
  python3 scripts/migrations/run_archive_migration.py --skip-delete # archive, no delete
  python3 scripts/migrations/run_archive_migration.py               # full migration

Run order:
  1. Run 001_juicereel_history_archive.sql in Supabase SQL Editor first.
  2. --dry-run to verify expected counts (~6,256 stale).
  3. --skip-delete to populate archive.
  4. Confirm counts in Supabase, then run without flags to delete from main tables.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

BATCH_SIZE = 500       # for upsert/delete operations
IN_BATCH_SIZE = 50    # .in_() filter — SHA256 IDs are 64 chars; 500×64 = 32KB URL → Bad Request
STALE_SIGNAL_TYPES = ("juicereel_history_history", "solo_pick")

# solo_pick stale ones are only the JuiceReel solo_picks not in local ledger.
# We identify stale by: in Supabase + NOT in local ledger.


def _batched(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def load_local_signal_ids() -> set:
    path = REPO_ROOT / "data" / "ledger" / "signals_latest.jsonl"
    ids = set()
    with open(path) as f:
        for line in f:
            ids.add(json.loads(line)["signal_id"])
    print(f"[local] Loaded {len(ids):,} signal_ids from signals_latest.jsonl")
    return ids


def fetch_supabase_juicereel_signal_ids(client) -> list[str]:
    """Paginate through all juicereel_history_history + solo_pick signals in Supabase."""
    all_ids = []
    for signal_type in STALE_SIGNAL_TYPES:
        offset = 0
        while True:
            resp = (
                client.table("signals")
                .select("signal_id")
                .eq("signal_type", signal_type)
                .range(offset, offset + BATCH_SIZE - 1)
                .execute()
            )
            rows = resp.data or []
            all_ids.extend(r["signal_id"] for r in rows)
            if len(rows) < BATCH_SIZE:
                break
            offset += BATCH_SIZE
        print(f"  signal_type={signal_type}: fetched so far {len(all_ids):,}")
    return all_ids


def fetch_rows_by_ids(client, table: str, id_col: str, ids: list[str]) -> list[dict]:
    # Use IN_BATCH_SIZE (50) not BATCH_SIZE — SHA256 IDs are 64 chars each;
    # batching 500 at a time produces ~32KB URLs that PostgREST rejects as Bad Request.
    rows = []
    for i, batch in enumerate(_batched(ids, IN_BATCH_SIZE)):
        resp = client.table(table).select("*").in_(id_col, batch).execute()
        rows.extend(resp.data or [])
        if (i + 1) % 20 == 0:
            print(f"  fetched {len(rows):,}/{len(ids):,}...", end="\r")
    print(f"  fetched {len(rows):,}/{len(ids):,}")
    return rows


def build_archive_rows(signal_rows: list[dict], grade_by_id: dict[str, dict]) -> list[dict]:
    archive = []
    for s in signal_rows:
        sid = s["signal_id"]
        g = grade_by_id.get(sid, {})

        direction = s.get("direction")
        direction_known = bool(direction and direction.strip())

        row = {
            # signal columns
            "signal_id":       sid,
            "day_key":         s.get("day_key"),
            "event_key":       s.get("event_key"),
            "sport":           s.get("sport", "NBA"),
            "away_team":       s.get("away_team"),
            "home_team":       s.get("home_team"),
            "market_type":     s.get("market_type"),
            "selection":       s.get("selection"),
            "direction":       direction,
            "line":            s.get("line"),
            "line_min":        s.get("line_min"),
            "line_max":        s.get("line_max"),
            "atomic_stat":     s.get("atomic_stat"),
            "player_key":      s.get("player_key"),
            "stat_key":        s.get("stat_key"),
            "sources_combo":   s.get("sources_combo"),
            "sources_count":   s.get("sources_count"),
            "signal_type":     s.get("signal_type"),
            "score":           s.get("score"),
            "run_id":          s.get("run_id"),
            "observed_at_utc": s.get("observed_at_utc"),
            # grade columns (None if no grade)
            "result":          g.get("result"),
            "grade_status":    g.get("status"),
            "grade_line":      g.get("line"),
            "odds":            g.get("odds"),
            "stat_value":      g.get("stat_value"),
            "provider":        g.get("provider"),
            "graded_at_utc":   g.get("graded_at_utc"),
            "grade_notes":     g.get("notes"),
            "units":           g.get("units"),
            # archive-specific
            "direction_known": direction_known,
        }
        archive.append(row)
    return archive


def upsert_archive(client, rows: list[dict]) -> int:
    total = 0
    for batch in _batched(rows, BATCH_SIZE):
        client.table("juicereel_history_archive").upsert(
            batch, on_conflict="signal_id", ignore_duplicates=True
        ).execute()
        total += len(batch)
        print(f"  archived {total:,}/{len(rows):,}...", end="\r")
    print()
    return total


def verify_archive_counts(client, expected: int, local_ids: set):
    resp = client.table("juicereel_history_archive").select("signal_id", count="exact").execute()
    total = resp.count
    print(f"\n[verify] juicereel_history_archive total rows: {total:,} (expected {expected:,})")

    # Fetch all archive signal_ids to check for contamination from local ledger
    all_archive = []
    offset = 0
    while True:
        r = client.table("juicereel_history_archive").select("signal_id").range(offset, offset + 999).execute()
        rows = r.data or []
        all_archive.extend(row["signal_id"] for row in rows)
        if len(rows) < 1000:
            break
        offset += 1000

    contaminated = [sid for sid in all_archive if sid in local_ids]
    if contaminated:
        print(f"  WARNING: {len(contaminated)} archive rows are in current local ledger — removing...")
        for i in range(0, len(contaminated), IN_BATCH_SIZE):
            batch = contaminated[i:i + IN_BATCH_SIZE]
            client.table("juicereel_history_archive").delete().in_("signal_id", batch).execute()
        resp = client.table("juicereel_history_archive").select("signal_id", count="exact").execute()
        total = resp.count
        print(f"  Cleaned. New archive count: {total:,}")

    if total != expected:
        print(f"  ERROR: count mismatch after cleanup! Expected {expected:,}, got {total:,}")
        return False

    # direction_known breakdown
    resp_t = (
        client.table("juicereel_history_archive")
        .select("signal_id", count="exact")
        .eq("direction_known", True)
        .execute()
    )
    resp_f = (
        client.table("juicereel_history_archive")
        .select("signal_id", count="exact")
        .eq("direction_known", False)
        .execute()
    )
    print(f"  direction_known=TRUE : {resp_t.count:,}")
    print(f"  direction_known=FALSE: {resp_f.count:,}")
    return True


def delete_stale_from_main_tables(client, stale_ids: list[str]):
    """Delete FK children first (plays, grades), then signals (CASCADE deletes signal_sources)."""
    # plays → grades → signals (FK order; signal_sources cascades with signals)
    print(f"\n[delete] Deleting stale plays rows (FK child of signals)...")
    plays_deleted = 0
    for batch in _batched(stale_ids, IN_BATCH_SIZE):
        client.table("plays").delete().in_("signal_id", batch).execute()
        plays_deleted += len(batch)
        print(f"  plays deleted: {plays_deleted:,}/{len(stale_ids):,}...", end="\r")
    print()

    print(f"[delete] Deleting {len(stale_ids):,} stale grades from grades table...")
    grade_deleted = 0
    for batch in _batched(stale_ids, IN_BATCH_SIZE):
        client.table("grades").delete().in_("signal_id", batch).execute()
        grade_deleted += len(batch)
        print(f"  grades deleted: {grade_deleted:,}/{len(stale_ids):,}...", end="\r")
    print()

    print(f"[delete] Deleting {len(stale_ids):,} stale signals from signals table...")
    sig_deleted = 0
    for batch in _batched(stale_ids, IN_BATCH_SIZE):
        client.table("signals").delete().in_("signal_id", batch).execute()
        sig_deleted += len(batch)
        print(f"  signals deleted: {sig_deleted:,}/{len(stale_ids):,}...", end="\r")
    print()
    print(f"  (signal_sources rows cascade-deleted with signals)")


def print_final_counts(client):
    resp_s = client.table("signals").select("signal_id", count="exact").execute()
    resp_g = client.table("grades").select("signal_id", count="exact").execute()
    resp_a = client.table("juicereel_history_archive").select("signal_id", count="exact").execute()
    print(f"\n[final counts]")
    print(f"  signals (main):              {resp_s.count:,}")
    print(f"  grades (main):               {resp_g.count:,}")
    print(f"  juicereel_history_archive:   {resp_a.count:,}")


def main():
    parser = argparse.ArgumentParser(description="Migrate stale JuiceReel signals to archive table.")
    parser.add_argument("--dry-run", action="store_true", help="Print counts only, no writes.")
    parser.add_argument("--skip-delete", action="store_true", help="Archive but do not delete from main tables.")
    args = parser.parse_args()

    from src.supabase_writer import get_client
    client = get_client()

    # --- Phase 1: Load local signal_ids ---
    local_ids = load_local_signal_ids()

    # --- Phase 2: Fetch JuiceReel signal_ids from Supabase ---
    print("\n[supabase] Fetching juicereel signal_ids...")
    supabase_jr_ids = fetch_supabase_juicereel_signal_ids(client)
    print(f"[supabase] Total juicereel signals in Supabase: {len(supabase_jr_ids):,}")

    # --- Phase 3: Identify stale ---
    stale_ids = [sid for sid in supabase_jr_ids if sid not in local_ids]
    current_jr_ids = [sid for sid in supabase_jr_ids if sid in local_ids]
    print(f"\n[diff]")
    print(f"  stale (in Supabase, NOT local): {len(stale_ids):,}")
    print(f"  current (in both):              {len(current_jr_ids):,}")

    # --- Phase 4: Sanity check ---
    overlap = set(stale_ids) & local_ids
    if overlap:
        print(f"  ERROR: {len(overlap)} stale_ids found in local_ids — logic error, aborting.")
        sys.exit(1)
    print(f"  Sanity check passed: zero overlap between stale and local.")

    if args.dry_run:
        print("\n[dry-run] No writes performed. Re-run without --dry-run to proceed.")
        return

    # --- Phase 5: Fetch full signal + grade rows for stale_ids ---
    print(f"\n[fetch] Fetching {len(stale_ids):,} stale signal rows from Supabase...")
    signal_rows = fetch_rows_by_ids(client, "signals", "signal_id", stale_ids)
    print(f"  Fetched {len(signal_rows):,} signal rows")

    print(f"[fetch] Fetching grade rows for stale_ids...")
    grade_rows = fetch_rows_by_ids(client, "grades", "signal_id", stale_ids)
    grade_by_id = {g["signal_id"]: g for g in grade_rows}
    print(f"  Fetched {len(grade_rows):,} grade rows ({len(stale_ids) - len(grade_rows):,} signals have no grade)")

    # --- Phase 6: Build archive rows ---
    archive_rows = build_archive_rows(signal_rows, grade_by_id)
    dk = sum(1 for r in archive_rows if r["direction_known"])
    print(f"\n[build] {len(archive_rows):,} archive rows built")
    print(f"  direction_known=TRUE : {dk:,}")
    print(f"  direction_known=FALSE: {len(archive_rows) - dk:,}")

    # --- Phase 7: Upsert into juicereel_history_archive ---
    print(f"\n[archive] Upserting {len(archive_rows):,} rows into juicereel_history_archive...")
    upsert_archive(client, archive_rows)

    # --- Phase 8/9: Verify archive ---
    ok = verify_archive_counts(client, len(stale_ids), local_ids)
    if not ok:
        print("\nERROR: Archive count mismatch. Do NOT proceed with deletion. Investigate first.")
        sys.exit(1)

    if args.skip_delete:
        print("\n[skip-delete] Archive complete. Main tables untouched.")
        print("Re-run without --skip-delete to delete stale records from signals/grades.")
        return

    # --- Phase 11/12: Delete from main tables ---
    delete_stale_from_main_tables(client, stale_ids)

    # --- Phase 13: Final counts ---
    print_final_counts(client)
    print("\nMigration complete.")


if __name__ == "__main__":
    main()
