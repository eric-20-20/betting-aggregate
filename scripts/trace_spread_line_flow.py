#!/usr/bin/env python3
"""
Tracer script to debug spread line evidence flow through the pipeline.

Usage:
    python3 scripts/trace_spread_line_flow.py --event-key NBA:2026:01:28:ORL@MIA --selection ORL
    python3 scripts/trace_spread_line_flow.py --occurrence-id <id>
    python3 scripts/trace_spread_line_flow.py --day-key NBA:2026:01:28 --team ORL
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
LEDGER_OCCURRENCES = PROJECT_ROOT / "data" / "ledger" / "signals_occurrences.jsonl"
HISTORY_DIR = PROJECT_ROOT / "data" / "history"
BETQL_NORMALIZED_DIR = HISTORY_DIR / "betql_normalized"


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    """Load JSONL file."""
    if not path.exists():
        return []
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return rows


def find_ledger_signal(
    event_key: Optional[str] = None,
    selection: Optional[str] = None,
    occurrence_id: Optional[str] = None,
    day_key: Optional[str] = None,
    team: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Find spread signals in ledger matching criteria."""
    signals = load_jsonl(LEDGER_OCCURRENCES)
    matches = []
    for sig in signals:
        if sig.get("market_type") != "spread":
            continue
        if occurrence_id and sig.get("occurrence_id") == occurrence_id:
            matches.append(sig)
            continue
        if event_key and sig.get("event_key") == event_key:
            if selection and sig.get("selection") != selection:
                continue
            matches.append(sig)
            continue
        if day_key and sig.get("day_key") == day_key:
            if team and sig.get("selection") != team:
                continue
            matches.append(sig)
    return matches


def find_upstream_normalized(
    event_key: str, selection: str, source_id: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Search for upstream normalized records that may have fed this signal."""
    matches = []

    # Parse day from event_key (e.g., NBA:2026:01:28:ORL@MIA -> 2026-01-28)
    parts = event_key.split(":")
    if len(parts) >= 4:
        day_str = f"{parts[1]}-{parts[2]}-{parts[3]}"
    else:
        return matches

    # Search BetQL normalized spreads
    spread_file = BETQL_NORMALIZED_DIR / "spreads" / f"{day_str}.jsonl"
    if spread_file.exists():
        records = load_jsonl(spread_file)
        for rec in records:
            ev = rec.get("event") or {}
            mkt = rec.get("market") or {}
            prov = rec.get("provenance") or {}

            # Match by event_key
            if ev.get("event_key") == event_key:
                # Match by selection (team code)
                if mkt.get("selection") == selection or mkt.get("side") == selection:
                    if source_id is None or prov.get("source_id") == source_id:
                        matches.append(rec)

    return matches


def print_signal(sig: Dict[str, Any]) -> None:
    """Print signal summary."""
    print("\n" + "=" * 80)
    print("LEDGER SIGNAL")
    print("=" * 80)
    print(f"  occurrence_id: {sig.get('occurrence_id')}")
    print(f"  signal_id:     {sig.get('signal_id')}")
    print(f"  event_key:     {sig.get('event_key')}")
    print(f"  day_key:       {sig.get('day_key')}")
    print(f"  market_type:   {sig.get('market_type')}")
    print(f"  selection:     {sig.get('selection')}")
    print(f"  direction:     {sig.get('direction')}")
    print(f"  line:          {sig.get('line')}")
    print(f"  line_median:   {sig.get('line_median')}")
    print(f"  line_min:      {sig.get('line_min')}")
    print(f"  line_max:      {sig.get('line_max')}")
    print(f"  lines:         {sig.get('lines')}")
    print(f"  sources_combo: {sig.get('sources_combo')}")
    print(f"  source_id:     {sig.get('source_id')}")
    print(f"  source_surface:{sig.get('source_surface')}")
    print(f"  score:         {sig.get('score')}")
    print(f"  supports count:{len(sig.get('supports') or [])}")


def print_supports(supports: List[Dict[str, Any]]) -> None:
    """Print supports array details."""
    if not supports:
        print("\n  SUPPORTS: EMPTY []")
        print("  >> This is the problem! No supports = no line evidence to promote")
        return

    print(f"\n  SUPPORTS ({len(supports)} entries):")
    print("  " + "-" * 76)

    has_line_evidence = False
    for i, sup in enumerate(supports):
        print(f"    [{i}]")
        print(f"        source_id:      {sup.get('source_id')}")
        print(f"        source_surface: {sup.get('source_surface')}")
        print(f"        selection:      {sup.get('selection')}")
        print(f"        direction:      {sup.get('direction')}")
        print(f"        line:           {sup.get('line')}")
        print(f"        line_hint:      {sup.get('line_hint')}")
        print(f"        raw_pick_text:  {sup.get('raw_pick_text')}")
        print(f"        raw_block:      {(sup.get('raw_block') or '')[:80]}...")
        print(f"        canonical_url:  {sup.get('canonical_url')}")

        # Check if this support has line evidence
        if sup.get("line") is not None or sup.get("raw_pick_text"):
            has_line_evidence = True

    print()
    if has_line_evidence:
        print("  >> LINE EVIDENCE PRESENT in supports")
    else:
        print("  >> NO LINE EVIDENCE in supports (line=None, raw_pick_text=None)")


def print_upstream(records: List[Dict[str, Any]]) -> None:
    """Print upstream normalized records."""
    if not records:
        print("\n  UPSTREAM NORMALIZED: NOT FOUND")
        return

    print(f"\n  UPSTREAM NORMALIZED ({len(records)} records):")
    print("  " + "-" * 76)

    for i, rec in enumerate(records):
        ev = rec.get("event") or {}
        mkt = rec.get("market") or {}
        prov = rec.get("provenance") or {}

        print(f"    [{i}]")
        print(f"        event_key:      {ev.get('event_key')}")
        print(f"        market.line:    {mkt.get('line')}")
        print(f"        market.selection:{mkt.get('selection')}")
        print(f"        prov.source_id:  {prov.get('source_id')}")
        print(f"        prov.source_surface: {prov.get('source_surface')}")
        print(f"        prov.raw_pick_text:  {prov.get('raw_pick_text')}")
        print(f"        prov.raw_block:      {(prov.get('raw_block') or '')[:60]}...")
        print(f"        prov.canonical_url:  {prov.get('canonical_url')}")
        print()

        if mkt.get("line") is not None:
            print(f"        >> UPSTREAM HAS LINE: {mkt.get('line')}")
        if prov.get("raw_pick_text"):
            print(f"        >> UPSTREAM HAS RAW_PICK_TEXT: {prov.get('raw_pick_text')}")


def main():
    parser = argparse.ArgumentParser(description="Trace spread line evidence flow")
    parser.add_argument("--event-key", help="Event key (e.g., NBA:2026:01:28:ORL@MIA)")
    parser.add_argument("--selection", help="Selection/team (e.g., ORL)")
    parser.add_argument("--occurrence-id", help="Occurrence ID from ledger")
    parser.add_argument("--day-key", help="Day key (e.g., NBA:2026:01:28)")
    parser.add_argument("--team", help="Team code (e.g., ORL)")
    args = parser.parse_args()

    if not any([args.event_key, args.occurrence_id, args.day_key]):
        print("Error: Must specify --event-key, --occurrence-id, or --day-key")
        sys.exit(1)

    # Find matching signals
    signals = find_ledger_signal(
        event_key=args.event_key,
        selection=args.selection,
        occurrence_id=args.occurrence_id,
        day_key=args.day_key,
        team=args.team,
    )

    if not signals:
        print("No matching spread signals found in ledger.")
        sys.exit(0)

    print(f"\nFound {len(signals)} matching spread signal(s)")

    for sig in signals:
        print_signal(sig)
        print_supports(sig.get("supports") or [])

        # Try to find upstream normalized records
        event_key = sig.get("event_key")
        selection = sig.get("selection")
        if event_key and selection:
            # Check each source in supports
            support_sources = set()
            for sup in sig.get("supports") or []:
                if sup.get("source_id"):
                    support_sources.add(sup.get("source_id"))

            # Also check sources_combo
            if sig.get("sources_combo"):
                for src in sig["sources_combo"].split("|"):
                    support_sources.add(src.strip())

            # Search for upstream
            upstream = find_upstream_normalized(event_key, selection)
            print_upstream(upstream)

        print("\n" + "=" * 80)


if __name__ == "__main__":
    main()
