"""Audit consensus grouping after canonical_event_key migration.

Checks for:
1. Hard group counts (multi-source agreement)
2. Soft group counts (single-source signals)
3. Unified signal counts
4. Any "same game, different key" issues

Usage:
    python3 scripts/audit_consensus_overlap.py
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.source_contract import normalize_event_key, is_canonical_event_key


CONSENSUS_FILES = {
    "hard": Path("data/latest/consensus_nba_hard.json"),
    "soft": Path("data/latest/consensus_nba_soft.json"),
    "v1": Path("data/latest/consensus_nba_v1.json"),
    "within": Path("data/latest/consensus_nba_within.json"),
}

# Alternative locations
ALT_CONSENSUS_FILES = {
    "hard": Path("out/consensus_nba_hard.json"),
    "soft": Path("out/consensus_nba_soft.json"),
    "v1": Path("out/consensus_nba_v1.json"),
    "within": Path("out/consensus_nba_within.json"),
}

NORMALIZED_FILES = [
    Path("out/normalized_action_nba.json"),
    Path("out/normalized_covers_nba.json"),
    Path("out/normalized_betql_prop_nba.json"),
    Path("out/normalized_betql_sharp_nba.json"),
    Path("out/normalized_betql_spread_nba.json"),
    Path("out/normalized_betql_total_nba.json"),
]


def read_json(path: Path) -> Any:
    """Read JSON file."""
    if not path.exists():
        return None
    with path.open() as f:
        return json.load(f)


def analyze_consensus_file(data: List[Dict[str, Any]], name: str) -> Dict[str, Any]:
    """Analyze a consensus file for grouping stats."""
    if not data:
        return {"total": 0, "error": "empty or missing"}

    stats = {
        "total_signals": len(data),
        "unique_day_keys": set(),
        "unique_event_keys": set(),
        "unique_canonical_event_keys": set(),
        "sources_distribution": Counter(),
        "market_types": Counter(),
        "has_canonical": 0,
        "missing_canonical": 0,
    }

    for sig in data:
        day_key = sig.get("day_key")
        event_key = sig.get("event_key")
        canonical_key = sig.get("canonical_event_key")

        if day_key:
            stats["unique_day_keys"].add(day_key)
        if event_key:
            stats["unique_event_keys"].add(event_key)
        if canonical_key:
            stats["unique_canonical_event_keys"].add(canonical_key)
            stats["has_canonical"] += 1
        else:
            stats["missing_canonical"] += 1

        # Count sources
        sources = sig.get("sources") or []
        if isinstance(sources, list):
            stats["sources_distribution"][len(sources)] += 1

        # Market types
        market = sig.get("market_type")
        if market:
            stats["market_types"][market] += 1

    # Convert sets to counts
    stats["unique_day_keys"] = len(stats["unique_day_keys"])
    stats["unique_event_keys"] = len(stats["unique_event_keys"])
    stats["unique_canonical_event_keys"] = len(stats["unique_canonical_event_keys"])

    return stats


def check_event_key_consistency(normalized_files: List[Path]) -> Dict[str, Any]:
    """Check for same-game-different-key issues across sources."""
    # Group picks by (day_key, away_team, home_team)
    game_keys_by_matchup: Dict[Tuple[str, str, str], Set[str]] = defaultdict(set)
    canonical_keys_by_matchup: Dict[Tuple[str, str, str], Set[str]] = defaultdict(set)

    for path in normalized_files:
        data = read_json(path)
        if not data:
            continue

        for pick in data:
            event = pick.get("event", {})
            day_key = event.get("day_key")
            away = event.get("away_team")
            home = event.get("home_team")
            event_key = event.get("event_key")
            canonical_key = event.get("canonical_event_key")

            if day_key and away and home:
                matchup = (day_key, away, home)
                if event_key:
                    game_keys_by_matchup[matchup].add(event_key)
                if canonical_key:
                    canonical_keys_by_matchup[matchup].add(canonical_key)

    # Find matchups with multiple different event keys
    inconsistent_matchups = []
    for matchup, keys in game_keys_by_matchup.items():
        if len(keys) > 1:
            # Check if they normalize to the same canonical
            normalized = set(normalize_event_key(k) for k in keys if k)
            normalized.discard(None)
            if len(normalized) > 1:
                inconsistent_matchups.append({
                    "matchup": matchup,
                    "event_keys": list(keys),
                    "normalized_keys": list(normalized),
                })

    return {
        "total_matchups": len(game_keys_by_matchup),
        "inconsistent_matchups": inconsistent_matchups,
        "inconsistent_count": len(inconsistent_matchups),
    }


def main() -> None:
    print("=" * 70)
    print("CONSENSUS OVERLAP AUDIT")
    print("=" * 70)
    print()

    # Try primary locations, fall back to alt
    consensus_files = {}
    for name, path in CONSENSUS_FILES.items():
        if path.exists():
            consensus_files[name] = path
        elif ALT_CONSENSUS_FILES[name].exists():
            consensus_files[name] = ALT_CONSENSUS_FILES[name]

    if not consensus_files:
        print("⚠️  No consensus files found. Run consensus_nba.py first.")
        return

    # Analyze each consensus file
    for name, path in consensus_files.items():
        print(f"Analyzing: {name} ({path})")
        print("-" * 50)

        data = read_json(path)
        if not data:
            print("  [skip] File empty or missing")
            print()
            continue

        stats = analyze_consensus_file(data, name)

        print(f"  Total signals:              {stats['total_signals']:,}")
        print(f"  Unique day_keys:            {stats['unique_day_keys']:,}")
        print(f"  Unique event_keys:          {stats['unique_event_keys']:,}")
        print(f"  Unique canonical_event_keys: {stats['unique_canonical_event_keys']:,}")
        print()
        print(f"  Has canonical_event_key:    {stats['has_canonical']:,}")
        print(f"  Missing canonical:          {stats['missing_canonical']:,}")
        print()

        print("  Sources per signal:")
        for count, num in sorted(stats["sources_distribution"].items()):
            print(f"    {count} sources: {num:,} signals")
        print()

        print("  Market types:")
        for market, count in stats["market_types"].most_common(5):
            print(f"    {market}: {count:,}")
        print()

    # Check for cross-source consistency
    print("=" * 70)
    print("CROSS-SOURCE EVENT KEY CONSISTENCY")
    print("=" * 70)
    print()

    consistency = check_event_key_consistency(NORMALIZED_FILES)

    print(f"Total unique matchups (day_key + teams): {consistency['total_matchups']:,}")
    print(f"Inconsistent matchups (different keys):  {consistency['inconsistent_count']:,}")

    if consistency["inconsistent_matchups"]:
        print()
        print("⚠️  INCONSISTENT MATCHUPS FOUND:")
        for m in consistency["inconsistent_matchups"][:5]:
            print(f"  Matchup: {m['matchup']}")
            print(f"    Event keys: {m['event_keys']}")
            print(f"    Normalized: {m['normalized_keys']}")
    else:
        print()
        print("✅ All matchups have consistent event keys across sources.")

    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)

    total_hard = 0
    total_soft = 0
    total_unified = 0

    for name, path in consensus_files.items():
        data = read_json(path)
        if data:
            count = len(data)
            if name == "hard":
                total_hard = count
            elif name == "soft":
                total_soft = count
            elif name == "v1":
                total_unified = count

    print()
    print(f"  Hard consensus signals:     {total_hard:,}")
    print(f"  Soft consensus signals:     {total_soft:,}")
    print(f"  Unified (v1) signals:       {total_unified:,}")
    print()

    if consistency["inconsistent_count"] == 0:
        print("✅ No cross-source key inconsistencies detected.")
    else:
        print(f"⚠️  {consistency['inconsistent_count']} matchups have inconsistent keys.")
        print("   This may cause grouping issues in consensus.")

    print()


if __name__ == "__main__":
    main()
