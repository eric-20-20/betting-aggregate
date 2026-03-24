#!/usr/bin/env python3
"""
One-time migration: fix JuiceReel signals with wrong event_key team order.

JuiceReel shows matchup text like "Hawks @ Grizzlies" which parses to ATL@MEM,
but the canonical NBA schedule order is MEM@ATL. This script finds signals where
juicereel_nukethebooks or juicereel_sxebets is the only source and the event_key
team order is backwards, then either merges them into an existing correct-keyed
signal or corrects the event_key in place.

Usage:
    python3 scripts/fix_juicereel_event_keys.py [--dry-run]
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.supabase_writer import get_client, IN_BATCH_SIZE
from src.results.nba_provider_nba_api import get_games_for_date

JUICEREEL_SOURCES = {"juicereel_nukethebooks", "juicereel_sxebets"}

# ─────────────────────────────────────────────────────────────
# Schedule helpers
# ─────────────────────────────────────────────────────────────

def _norm(team: str) -> str:
    return team.strip().upper()


def canonical_order(away: str, home: str, day_key: str) -> Optional[tuple[str, str]]:
    """
    Return (canonical_away, canonical_home) for this game on day_key, or None
    if the game cannot be found in the NBA schedule.

    day_key format: "NBA:2026:03:23"
    """
    # Extract YYYY-MM-DD from "NBA:2026:03:23"
    parts = day_key.split(":")
    if len(parts) >= 4:
        date_str = f"{parts[1]}-{parts[2]}-{parts[3]}"
    else:
        return None

    try:
        games, _ = get_games_for_date(date_str)
    except Exception as exc:
        print(f"  ⚠️  Schedule lookup failed for {date_str}: {exc}")
        return None

    team_set = {_norm(away), _norm(home)}
    for g in games:
        sched_away = _norm(g.get("away_team_abbrev") or "")
        sched_home = _norm(g.get("home_team_abbrev") or "")
        if {sched_away, sched_home} == team_set:
            return sched_away, sched_home

    return None  # not found in schedule


def swapped_event_key(event_key: str) -> Optional[str]:
    """Return event_key with teams swapped, or None if not parseable."""
    # Format: "NBA:2026:03:23:ATL@MEM"
    m = re.match(r"^(.*):([A-Z]{2,4})@([A-Z]{2,4})$", event_key)
    if not m:
        return None
    prefix, away, home = m.group(1), m.group(2), m.group(3)
    return f"{prefix}:{home}@{away}"


# ─────────────────────────────────────────────────────────────
# Supabase fetching
# ─────────────────────────────────────────────────────────────

def fetch_juicereel_solo_signals(client) -> list[dict]:
    """
    Fetch all signals whose only source(s) are juicereel_nukethebooks or
    juicereel_sxebets (sources_count = 1, sources_combo in the JR source set).

    Returns list of signal dicts with signal_id, event_key, away_team, home_team,
    day_key, sources_combo, and their signal_sources rows.
    """
    # Fetch signals with sources_count=1 and sources_combo in JR sources
    jr_combos = list(JUICEREEL_SOURCES)
    results = []
    page = 0
    page_size = 1000
    while True:
        resp = (
            client.table("signals")
            .select("signal_id, event_key, away_team, home_team, day_key, sources_combo, sources_count, market_type, selection, direction, line, sport")
            .eq("sources_count", 1)
            .in_("sources_combo", jr_combos)
            .not_.is_("event_key", "null")
            .range(page * page_size, (page + 1) * page_size - 1)
            .execute()
        )
        if not resp.data:
            break
        results.extend(resp.data)
        if len(resp.data) < page_size:
            break
        page += 1

    return results


def fetch_signal_by_event_key(client, event_key: str, exclude_signal_id: str) -> Optional[dict]:
    """Fetch any signal with this exact event_key (excluding the JR signal itself)."""
    resp = (
        client.table("signals")
        .select("signal_id, event_key, away_team, home_team, sources_combo, sources_count")
        .eq("event_key", event_key)
        .neq("signal_id", exclude_signal_id)
        .limit(1)
        .execute()
    )
    return resp.data[0] if resp.data else None


def fetch_signal_sources(client, signal_id: str) -> list[dict]:
    """Fetch all signal_sources rows for a signal."""
    resp = (
        client.table("signal_sources")
        .select("id, signal_id, source_id, expert_id, expert_slug, expert_name, line, odds, raw_pick_text, expert_result, expert_graded_line")
        .eq("signal_id", signal_id)
        .execute()
    )
    return resp.data or []


def has_grade(client, signal_id: str) -> bool:
    resp = (
        client.table("grades")
        .select("signal_id")
        .eq("signal_id", signal_id)
        .limit(1)
        .execute()
    )
    return bool(resp.data)


def has_plays(client, signal_id: str) -> bool:
    resp = (
        client.table("plays")
        .select("id")
        .eq("signal_id", signal_id)
        .limit(1)
        .execute()
    )
    return bool(resp.data)


# ─────────────────────────────────────────────────────────────
# Mutation helpers
# ─────────────────────────────────────────────────────────────

def do_merge(client, jr_signal: dict, target_signal: dict, jr_sources: list[dict], dry_run: bool):
    """
    Case A: move JR signal_sources onto target_signal, then delete JR signal.
    """
    jr_id = jr_signal["signal_id"]
    tgt_id = target_signal["signal_id"]
    jr_ek = jr_signal["event_key"]
    tgt_ek = target_signal["event_key"]

    print(f"  [MERGE] {jr_ek} → {tgt_ek}")
    print(f"          jr_signal_id={jr_id[:12]}… → target_signal_id={tgt_id[:12]}…")
    for ss in jr_sources:
        print(f"          source_source={ss.get('source_id')} expert={ss.get('expert_name') or ss.get('expert_slug')}")

    if dry_run:
        return

    # Re-point signal_sources rows to the target signal
    ss_ids = [ss["id"] for ss in jr_sources]
    for batch_start in range(0, len(ss_ids), IN_BATCH_SIZE):
        batch = ss_ids[batch_start: batch_start + IN_BATCH_SIZE]
        client.table("signal_sources").update({"signal_id": tgt_id}).in_("id", batch).execute()

    # Delete grades and plays on the JR signal (orphaned after moving sources)
    client.table("grades").delete().eq("signal_id", jr_id).execute()
    client.table("plays").delete().eq("signal_id", jr_id).execute()

    # Delete the JR signal itself (signal_sources already re-pointed, so no FK violation)
    client.table("signals").delete().eq("signal_id", jr_id).execute()


def do_correct_in_place(client, jr_signal: dict, canonical_away: str, canonical_home: str, dry_run: bool):
    """
    Case B: JuiceReel is alone — update event_key, away_team, home_team in place.
    """
    jr_id = jr_signal["signal_id"]
    old_ek = jr_signal["event_key"]
    day_prefix = old_ek.rsplit(":", 1)[0]  # everything before the matchup portion
    new_ek = f"{day_prefix}:{canonical_away}@{canonical_home}"

    print(f"  [CORRECT] {old_ek} → {new_ek}")
    print(f"            signal_id={jr_id[:12]}…")

    if dry_run:
        return

    client.table("signals").update({
        "event_key": new_ek,
        "away_team": canonical_away,
        "home_team": canonical_home,
    }).eq("signal_id", jr_id).execute()


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Fix JuiceReel event_key team order in Supabase")
    parser.add_argument("--dry-run", action="store_true", help="Show what would change without touching Supabase")
    args = parser.parse_args()

    dry_run = args.dry_run
    if dry_run:
        print("DRY RUN — no changes will be made\n")

    client = get_client()

    print("Fetching JuiceReel solo signals from Supabase…")
    signals = fetch_juicereel_solo_signals(client)
    print(f"Found {len(signals)} JuiceReel-only signals with non-null event_key\n")

    n_merged = 0
    n_corrected = 0
    n_skipped = 0
    n_already_correct = 0

    for sig in signals:
        ek = sig.get("event_key") or ""
        away = sig.get("away_team") or ""
        home = sig.get("home_team") or ""
        day_key = sig.get("day_key") or ""

        if not ek or not away or not home or not day_key:
            print(f"  [SKIP] Incomplete signal {sig.get('signal_id','')[:12]}… (missing event_key/teams/day_key)")
            n_skipped += 1
            continue

        # Check canonical order
        canon = canonical_order(away, home, day_key)
        if canon is None:
            print(f"  [SKIP] {ek} — game not found in NBA schedule (manual review needed)")
            n_skipped += 1
            continue

        canon_away, canon_home = canon

        # Already correct?
        if _norm(away) == canon_away and _norm(home) == canon_home:
            n_already_correct += 1
            continue

        # Teams are swapped — proceed
        flipped_ek = swapped_event_key(ek)
        if not flipped_ek:
            print(f"  [SKIP] {ek} — cannot parse event_key for swap (manual review needed)")
            n_skipped += 1
            continue

        jr_sources = fetch_signal_sources(client, sig["signal_id"])

        # Is there a correctly-keyed signal from other sources?
        target = fetch_signal_by_event_key(client, flipped_ek, exclude_signal_id=sig["signal_id"])

        if target:
            # Case A: merge
            do_merge(client, sig, target, jr_sources, dry_run)
            n_merged += 1
        else:
            # Case B: correct in place
            do_correct_in_place(client, sig, canon_away, canon_home, dry_run)
            n_corrected += 1

    print()
    print("=" * 50)
    print("SUMMARY")
    print("=" * 50)
    print(f"  Already correct (no action needed): {n_already_correct}")
    print(f"  Merged into existing correct signal: {n_merged}")
    print(f"  Corrected in place (JR alone):       {n_corrected}")
    print(f"  Skipped for manual review:           {n_skipped}")
    total_changed = n_merged + n_corrected
    print(f"  Total changed:                       {total_changed}")
    if dry_run:
        print("\n(DRY RUN — nothing was written)")


if __name__ == "__main__":
    main()
