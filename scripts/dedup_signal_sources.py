"""
Delete duplicate signal_sources rows from Supabase.

A duplicate is any row that shares the same:
  signal_id + source_id + COALESCE(expert_slug, '') + COALESCE(line, -1000000)

Keeps the row with the lowest id (MIN(id)) and deletes the rest.

Usage:
    python3 scripts/dedup_signal_sources.py          # dry-run: count only
    python3 scripts/dedup_signal_sources.py --delete  # actually delete
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

# Load .env
env_file = REPO_ROOT / ".env"
if env_file.exists():
    for _line in env_file.read_text().splitlines():
        if "=" in _line and not _line.startswith("#"):
            k, _, v = _line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

from src.supabase_writer import get_client  # noqa: E402

PAGE_SIZE   = 1000   # rows fetched per page when scanning
DELETE_BATCH = 500   # ids deleted per DELETE call


def fetch_all_rows(client) -> List[Dict[str, Any]]:
    """Fetch all signal_sources rows (id, signal_id, source_id, expert_slug, line)."""
    rows: List[Dict[str, Any]] = []
    offset = 0
    print("Fetching all signal_sources rows...", flush=True)
    while True:
        resp = (
            client.table("signal_sources")
            .select("id,signal_id,source_id,expert_slug,line")
            .order("id")
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )
        batch = resp.data or []
        rows.extend(batch)
        print(f"  fetched {len(rows):,} so far...", end="\r", flush=True)
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    print(f"  fetched {len(rows):,} total rows.          ")
    return rows


def find_duplicate_ids(rows: List[Dict[str, Any]]) -> List[int]:
    """
    Group rows by (signal_id, source_id, expert_slug, line).
    Within each group keep MIN(id), collect the rest as duplicates.
    Returns sorted list of ids to delete.
    """
    groups: Dict[tuple, List[int]] = {}
    for row in rows:
        key = (
            row["signal_id"],
            row.get("source_id") or "",
            row.get("expert_slug") or "",
            row.get("line") if row.get("line") is not None else -1000000,
        )
        groups.setdefault(key, []).append(row["id"])

    to_delete: List[int] = []
    for ids in groups.values():
        if len(ids) > 1:
            ids_sorted = sorted(ids)
            to_delete.extend(ids_sorted[1:])  # keep first (min), delete rest

    return sorted(to_delete)


def delete_in_batches(client, ids: List[int], dry_run: bool) -> int:
    """Delete rows by id in batches. Returns count deleted."""
    if not ids:
        return 0
    deleted = 0
    for i in range(0, len(ids), DELETE_BATCH):
        batch = ids[i : i + DELETE_BATCH]
        if not dry_run:
            client.table("signal_sources").delete().in_("id", batch).execute()
        deleted += len(batch)
        print(f"  {'[dry-run] would delete' if dry_run else 'deleted'} {deleted:,} / {len(ids):,}", end="\r", flush=True)
    print()
    return deleted


def final_count(client) -> int:
    resp = client.table("signal_sources").select("id", count="exact").execute()
    return resp.count or 0


def main() -> None:
    ap = argparse.ArgumentParser(description="Deduplicate signal_sources rows in Supabase")
    ap.add_argument("--delete", action="store_true", help="Actually delete (default: dry-run count only)")
    args = ap.parse_args()

    client = get_client()

    rows = fetch_all_rows(client)
    print(f"Total signal_sources rows in Supabase: {len(rows):,}")

    to_delete = find_duplicate_ids(rows)
    dup_groups = sum(1 for ids in {} .items())  # placeholder

    # Recount groups for reporting
    groups: Dict[tuple, List[int]] = {}
    for row in rows:
        key = (
            row["signal_id"],
            row.get("source_id") or "",
            row.get("expert_slug") or "",
            row.get("line") if row.get("line") is not None else -1000000,
        )
        groups.setdefault(key, []).append(row["id"])
    dup_group_count = sum(1 for ids in groups.values() if len(ids) > 1)

    print(f"\nDuplicate groups (same signal+source+expert+line): {dup_group_count:,}")
    print(f"Rows to delete (keeping 1 per group):              {len(to_delete):,}")

    if len(to_delete) == 0:
        print("\nNo duplicates found. Nothing to do.")
        return

    if not args.delete:
        print("\nDry-run mode. Pass --delete to actually remove these rows.")
        return

    print(f"\nDeleting {len(to_delete):,} duplicate rows in batches of {DELETE_BATCH}...")
    delete_in_batches(client, to_delete, dry_run=False)

    # Verify
    remaining = final_count(client)
    expected = len(rows) - len(to_delete)
    print(f"\nFinal row count: {remaining:,}  (expected {expected:,})")
    if remaining == expected:
        print("OK — zero duplicates remain.")
    else:
        print(f"WARNING — count mismatch ({remaining - expected:+,}). Re-run to check.")


if __name__ == "__main__":
    main()
