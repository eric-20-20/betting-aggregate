"""
Backfill odds into signal_sources for three sources.

Fetches null-odds rows directly from Supabase, resolves the odds value, and
issues bulk UPDATE calls. Does NOT go through the local ledger.

Strategy per source:
  sportsline        — parse odds regex from raw_pick_text on the Supabase row
                      (text format: "Spread\nTeam -4 -110\nWIN\nUnit\n1.0")
  bettingpros_experts — match Supabase row (raw_pick_text, expert_name) against
                      raw_bettingpros_experts_history_nba.json lookup
  juicereel         — match Supabase row (signal_id, source_id, line) against
                      raw_juicereel_backfill_nba.json lookup keyed by
                      (source_id, line, selection) from the local ledger

Run:
    python3 scripts/backfill_odds_signal_sources.py [--dry-run] [--source sportsline]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

LEDGER_PATH = REPO_ROOT / "data/ledger/signals_latest.jsonl"
BP_EXPERTS_HISTORY = REPO_ROOT / "out/raw_bettingpros_experts_history_nba.json"
JR_BACKFILL = REPO_ROOT / "out/raw_juicereel_backfill_nba.json"

FETCH_PAGE = 1000   # rows per Supabase select page
UPDATE_BATCH = 200  # rows per update loop


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_odds_from_text(text: str) -> Optional[int]:
    """Extract last American odds token from text (e.g. '-110', '+145')."""
    if not text:
        return None
    m = re.findall(r"([+-]\d{3,})", text)
    return int(m[-1]) if m else None


def _fetch_all_null_odds(client, source_id: str) -> list[dict]:
    """Page through signal_sources fetching all rows with odds IS NULL for source."""
    rows = []
    offset = 0
    while True:
        r = (
            client.table("signal_sources")
            .select("id,signal_id,source_id,expert_name,expert_slug,raw_pick_text,line")
            .eq("source_id", source_id)
            .is_("odds", "null")
            .range(offset, offset + FETCH_PAGE - 1)
            .execute()
        )
        batch = r.data or []
        rows.extend(batch)
        if len(batch) < FETCH_PAGE:
            break
        offset += FETCH_PAGE
    return rows


# ---------------------------------------------------------------------------
# Per-source resolvers
# ---------------------------------------------------------------------------

def resolve_sportsline(rows: list[dict]) -> list[dict]:
    """Parse odds from raw_pick_text on each Supabase row."""
    updates = []
    skipped = 0
    for row in rows:
        odds = _parse_odds_from_text(row.get("raw_pick_text"))
        if odds is None:
            skipped += 1
            continue
        updates.append({"id": row["id"], "odds": odds})
    print(f"  Resolved: {len(updates)}  |  No odds in text: {skipped}")
    return updates


def resolve_bettingpros_experts(rows: list[dict]) -> list[dict]:
    """Match against raw history file by (raw_pick_text, expert_name)."""
    print(f"  Loading {BP_EXPERTS_HISTORY.name}...", flush=True)
    with open(BP_EXPERTS_HISTORY) as f:
        raw = json.load(f)
    history_recs = raw if isinstance(raw, list) else raw.get("records", [])

    # Build lookup: (raw_pick_text, expert_name) -> odds
    # Also (raw_pick_text, None) as fallback
    lookup: dict[tuple, int] = {}
    no_odds = 0
    for r in history_recs:
        odds = r.get("odds")
        if odds is None:
            no_odds += 1
            continue
        rpt = r.get("raw_pick_text") or r.get("selection")
        ename = r.get("expert_name")
        if rpt:
            lookup[(rpt, ename)] = odds
            lookup.setdefault((rpt, None), odds)
    print(f"  History: {len(history_recs)} records, {len(lookup)} lookup entries, {no_odds} had no odds")

    updates = []
    unmatched = 0
    for row in rows:
        rpt = row.get("raw_pick_text")
        ename = row.get("expert_name")
        odds = lookup.get((rpt, ename)) or lookup.get((rpt, None))
        if odds is None:
            unmatched += 1
            continue
        updates.append({"id": row["id"], "odds": odds})
    print(f"  Resolved: {len(updates)}  |  Unmatched: {unmatched}")
    return updates


def resolve_juicereel(rows: list[dict], source_ids: list[str]) -> list[dict]:
    """
    Match against raw backfill by (source_id, raw_pick_text, line).
    For rows with no raw_pick_text, fall back to (source_id, line) from signals
    in the local ledger keyed by signal_id.
    """
    print(f"  Loading {JR_BACKFILL.name}...", flush=True)
    with open(JR_BACKFILL) as f:
        raw = json.load(f)
    backfill_recs = raw if isinstance(raw, list) else raw.get("records", [])

    # Lookup 1: (source_id, raw_pick_text, line) -> odds
    lookup_rpt: dict[tuple, int] = {}
    # Lookup 2: (source_id, line) -> odds  (for null-rpt rows, best-effort)
    lookup_line: dict[tuple, list[int]] = defaultdict(list)
    for r in backfill_recs:
        odds = r.get("odds")
        if odds is None:
            continue
        src = r.get("source_id")
        rpt = r.get("raw_pick_text")
        lv = r.get("line")
        if src and rpt:
            lookup_rpt[(src, rpt, lv)] = odds
        if src and lv is not None:
            lookup_line[(src, lv)].append(odds)
    print(f"  Backfill: {len(backfill_recs)} records, {len(lookup_rpt)} rpt-lookup entries")

    updates = []
    unmatched = 0
    ambiguous = 0
    for row in rows:
        src = row["source_id"]
        rpt = row.get("raw_pick_text")
        lv = row.get("line")

        odds = None
        if rpt is not None:
            odds = lookup_rpt.get((src, rpt, lv)) or lookup_rpt.get((src, rpt, None))
        if odds is None and lv is not None:
            candidates = lookup_line.get((src, lv), [])
            unique = list(set(candidates))
            if len(unique) == 1:
                odds = unique[0]
            elif len(unique) > 1:
                ambiguous += 1
                continue

        if odds is None:
            unmatched += 1
            continue
        updates.append({"id": row["id"], "odds": odds})

    print(f"  Resolved: {len(updates)}  |  Unmatched: {unmatched}  |  Ambiguous (skipped): {ambiguous}")
    return updates


# ---------------------------------------------------------------------------
# Push updates
# ---------------------------------------------------------------------------

def push_updates(client, updates: list[dict], dry_run: bool) -> int:
    """UPDATE signal_sources SET odds = value WHERE id = row_id."""
    if dry_run or not updates:
        return len(updates)
    sent = 0
    for row in updates:
        client.table("signal_sources").update({"odds": row["odds"]}).eq("id", row["id"]).execute()
        sent += 1
    return sent


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--source", choices=["sportsline", "bettingpros_experts", "juicereel", "all"],
                        default="all")
    args = parser.parse_args()

    from src.supabase_writer import get_client
    client = get_client()

    sources_to_run = (
        ["sportsline", "bettingpros_experts", "juicereel"]
        if args.source == "all"
        else [args.source]
    )

    grand_total = 0
    for source in sources_to_run:
        print(f"\n[{source}]", flush=True)

        # Fetch null-odds rows from Supabase
        if source == "juicereel":
            jr_sources = ["juicereel_sxebets", "juicereel_nukethebooks", "juicereel_unitvacuum", "juicereel"]
            rows = []
            for src in jr_sources:
                batch = _fetch_all_null_odds(client, src)
                print(f"  Fetched {len(batch)} null-odds rows for {src}")
                rows.extend(batch)
        else:
            rows = _fetch_all_null_odds(client, source)
            print(f"  Fetched {len(rows)} null-odds rows from Supabase")

        if not rows:
            print("  Nothing to update.")
            continue

        # Resolve odds
        if source == "sportsline":
            updates = resolve_sportsline(rows)
        elif source == "bettingpros_experts":
            updates = resolve_bettingpros_experts(rows)
        elif source == "juicereel":
            updates = resolve_juicereel(rows, jr_sources)

        if not updates:
            print("  No rows resolved.")
            continue

        # Push
        if args.dry_run:
            print(f"  [DRY RUN] Would update {len(updates)} rows")
            if updates:
                print(f"  Sample: id={updates[0]['id']} odds={updates[0]['odds']}")
        else:
            print(f"  Pushing {len(updates)} updates...", flush=True)
            n = push_updates(client, updates, dry_run=False)
            print(f"  Done — {n} rows updated")

        grand_total += len(updates)

    print(f"\nTotal: {grand_total} rows across all sources")
    if args.dry_run:
        print("(dry run — nothing written)")


if __name__ == "__main__":
    main()
