#!/usr/bin/env python3
"""
Inspect stale JuiceReel signals in Supabase.

Stale = signal_type='juicereel_history_history' in Supabase but NOT in
        data/ledger/signals_latest.jsonl

Usage:
  python3 scripts/inspect_stale_juicereel.py
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from collections import Counter

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

# ── Load credentials via the existing writer helper ───────────────────────────
from src.supabase_writer import get_client  # noqa: E402

PAGE_SIZE = 1000
TARGET_SIGNAL_TYPE = "juicereel_history_history"


def load_local_signal_ids() -> set[str]:
    ledger = REPO_ROOT / "data" / "ledger" / "signals_latest.jsonl"
    print(f"Loading local signal IDs from {ledger} …")
    ids: set[str] = set()
    with open(ledger) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            sid = rec.get("signal_id")
            if sid:
                ids.add(sid)
    print(f"  → {len(ids):,} local signal IDs loaded")
    return ids


def fetch_all_juicereel_history(client) -> list[dict]:
    """Paginate through all juicereel_history_history signals in Supabase."""
    print(f"\nFetching all '{TARGET_SIGNAL_TYPE}' signals from Supabase …")
    all_rows: list[dict] = []
    offset = 0
    while True:
        resp = (
            client.table("signals")
            .select(
                "signal_id,day_key,event_key,sport,away_team,home_team,"
                "market_type,selection,direction,line,line_min,line_max,"
                "atomic_stat,player_key,sources_combo,sources_count,"
                "signal_type,score,run_id,observed_at_utc"
            )
            .eq("signal_type", TARGET_SIGNAL_TYPE)
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )
        batch = resp.data or []
        all_rows.extend(batch)
        print(f"  fetched {offset + len(batch):,} rows (batch size {len(batch)})")
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    print(f"  → total fetched: {len(all_rows):,}")
    return all_rows


def fetch_raw_pick_texts(client, signal_ids: list[str]) -> dict[str, list[str]]:
    """Fetch raw_pick_text from signal_sources for a sample of signal_ids."""
    print(f"\nFetching raw_pick_text for {len(signal_ids)} sample signal_ids …")
    result: dict[str, list[str]] = {}
    for batch_start in range(0, len(signal_ids), 100):
        batch = signal_ids[batch_start : batch_start + 100]
        resp = (
            client.table("signal_sources")
            .select("signal_id,source_id,expert_slug,raw_pick_text,line")
            .in_("signal_id", batch)
            .execute()
        )
        for row in (resp.data or []):
            sid = row["signal_id"]
            result.setdefault(sid, [])
            text = row.get("raw_pick_text") or ""
            if text:
                result[sid].append(
                    f"  [{row.get('source_id','?')} / {row.get('expert_slug','?')}] "
                    f"line={row.get('line')} | {text[:120]}"
                )
    return result


def print_section(title: str) -> None:
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)


def main() -> None:
    client = get_client()
    local_ids = load_local_signal_ids()

    all_rows = fetch_all_juicereel_history(client)
    total_in_sb = len(all_rows)

    stale = [r for r in all_rows if r["signal_id"] not in local_ids]
    current = [r for r in all_rows if r["signal_id"] in local_ids]

    print_section("OVERVIEW")
    print(f"  Total '{TARGET_SIGNAL_TYPE}' in Supabase : {total_in_sb:,}")
    print(f"  Current (also in local ledger)           : {len(current):,}")
    print(f"  Stale   (only in Supabase)               : {len(stale):,}")

    if not stale:
        print("\nNo stale records found — nothing to analyse.")
        return

    # ── Sport breakdown ───────────────────────────────────────────────────────
    print_section("SPORT BREAKDOWN (stale)")
    sport_ctr = Counter(r.get("sport") or "NULL" for r in stale)
    for sport, cnt in sport_ctr.most_common():
        print(f"  {sport:<20} {cnt:>6,}")

    # ── Market type breakdown ─────────────────────────────────────────────────
    print_section("MARKET_TYPE BREAKDOWN (stale)")
    mt_ctr = Counter(r.get("market_type") or "NULL" for r in stale)
    for mt, cnt in mt_ctr.most_common():
        print(f"  {mt:<30} {cnt:>6,}")

    # ── Direction breakdown ───────────────────────────────────────────────────
    print_section("DIRECTION VALUES (stale)")
    dir_ctr = Counter(r.get("direction") or "NULL" for r in stale)
    for d, cnt in dir_ctr.most_common():
        print(f"  {str(d):<30} {cnt:>6,}")

    # ── Atomic stat breakdown ─────────────────────────────────────────────────
    print_section("ATOMIC_STAT VALUES (stale)")
    stat_ctr = Counter(r.get("atomic_stat") or "NULL" for r in stale)
    for stat, cnt in stat_ctr.most_common():
        print(f"  {str(stat):<30} {cnt:>6,}")

    # ── Player key populated? ─────────────────────────────────────────────────
    print_section("PLAYER_KEY PRESENCE (stale)")
    n_with_player = sum(1 for r in stale if r.get("player_key"))
    n_without = len(stale) - n_with_player
    print(f"  player_key populated : {n_with_player:,}")
    print(f"  player_key is NULL   : {n_without:,}")

    # ── atomic_stat populated? ────────────────────────────────────────────────
    print_section("ATOMIC_STAT PRESENCE (stale)")
    n_with_stat = sum(1 for r in stale if r.get("atomic_stat"))
    print(f"  atomic_stat populated : {n_with_stat:,}")
    print(f"  atomic_stat is NULL   : {len(stale) - n_with_stat:,}")

    # ── Date range ────────────────────────────────────────────────────────────
    print_section("DATE RANGE (stale — by day_key)")
    day_keys = sorted(
        (r.get("day_key") or "NULL") for r in stale if r.get("day_key")
    )
    if day_keys:
        print(f"  Earliest : {day_keys[0]}")
        print(f"  Latest   : {day_keys[-1]}")
        day_ctr = Counter(day_keys)
        print(f"\n  Counts per day_key (all {len(day_ctr)} days):")
        for dk, cnt in sorted(day_ctr.items()):
            print(f"    {dk:<30} {cnt:>5,}")
    else:
        print("  (no day_key values found)")

    # ── 20 diverse selection examples ─────────────────────────────────────────
    print_section("SELECTION FIELD — 20 DIVERSE EXAMPLES (stale)")
    # sample from different market types to get diversity
    by_mt: dict[str, list[str]] = {}
    for r in stale:
        mt = r.get("market_type") or "NULL"
        sel = r.get("selection") or "NULL"
        by_mt.setdefault(mt, [])
        by_mt[mt].append(sel)

    seen_sel: set[str] = set()
    examples: list[str] = []
    # round-robin across market types for diversity
    from itertools import cycle
    iters = {mt: iter(sels) for mt, sels in by_mt.items()}
    mt_cycle = cycle(list(iters.keys()))
    attempts = 0
    while len(examples) < 20 and attempts < len(stale) * 2:
        mt = next(mt_cycle)
        it = iters.get(mt)
        if it is None:
            attempts += 1
            continue
        try:
            sel = next(it)
        except StopIteration:
            iters.pop(mt, None)
            if not iters:
                break
            mt_cycle = cycle(list(iters.keys()))
            attempts += 1
            continue
        if sel not in seen_sel:
            seen_sel.add(sel)
            examples.append(f"  [{mt}]  {sel}")
        attempts += 1

    for ex in examples:
        print(ex)

    # ── raw_pick_text for 10 stale signal_ids ────────────────────────────────
    print_section("RAW_PICK_TEXT — 10 STALE SIGNAL_IDS (from signal_sources)")
    # pick 10 spread across different market types / days
    sample_ids: list[str] = []
    seen_mt: set[str] = set()
    # first pick one per market type
    for r in stale:
        mt = r.get("market_type") or "NULL"
        if mt not in seen_mt:
            sample_ids.append(r["signal_id"])
            seen_mt.add(mt)
        if len(sample_ids) >= 5:
            break
    # then fill to 10 from remaining stale records
    for r in stale:
        if r["signal_id"] not in sample_ids:
            sample_ids.append(r["signal_id"])
        if len(sample_ids) >= 10:
            break

    pick_texts = fetch_raw_pick_texts(client, sample_ids)

    for i, sid in enumerate(sample_ids, 1):
        matching_rows = [r for r in stale if r["signal_id"] == sid]
        row = matching_rows[0] if matching_rows else {}
        print(f"\n  [{i}] signal_id : {sid}")
        print(f"       day_key   : {row.get('day_key')}")
        print(f"       sport     : {row.get('sport')}")
        print(f"       market    : {row.get('market_type')}")
        print(f"       selection : {row.get('selection')}")
        print(f"       direction : {row.get('direction')}")
        print(f"       line      : {row.get('line')}")
        print(f"       player_key: {row.get('player_key')}")
        print(f"       atomic_stat:{row.get('atomic_stat')}")
        texts = pick_texts.get(sid, [])
        if texts:
            for t in texts:
                print(f"       pick_text :{t}")
        else:
            print("       pick_text : (no rows in signal_sources)")

    print()
    print("Done.")


if __name__ == "__main__":
    main()
