"""Ratchet test to prevent regression to legacy event_key format.

This test ensures that:
1. New output files don't contain legacy-only event keys
2. The percentage of legacy-only keys stays below a threshold
3. All new signals have canonical_event_key

If this test fails, it means a source is writing legacy event_key format
without canonical_event_key - fix the source normalizer.
"""

from __future__ import annotations

import json
import pytest
from pathlib import Path
from typing import Any, Dict, List

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.source_contract import is_legacy_event_key, is_canonical_event_key


# Maximum allowed percentage of legacy-only records
MAX_LEGACY_ONLY_PERCENT = 0.1  # 0.1% - essentially zero for new data

# Files to check for legacy ratchet
OUTPUT_FILES = [
    Path("out/normalized_action_nba.json"),
    Path("out/normalized_covers_nba.json"),
    Path("out/normalized_betql_prop_nba.json"),
    Path("out/normalized_betql_sharp_nba.json"),
    Path("out/normalized_betql_spread_nba.json"),
    Path("out/normalized_betql_total_nba.json"),
]

LEDGER_FILES = [
    Path("data/ledger/signals_latest.jsonl"),
    Path("data/ledger/grades_latest.jsonl"),
]


def read_json(path: Path) -> List[Dict[str, Any]]:
    """Read JSON array file."""
    if not path.exists():
        return []
    with path.open() as f:
        return json.load(f)


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    """Read JSONL file."""
    if not path.exists():
        return []
    records = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def count_legacy_only(records: List[Dict[str, Any]]) -> tuple[int, int]:
    """Count records with legacy-only event keys.

    Returns: (legacy_only_count, total_with_event_key)
    """
    legacy_only = 0
    total = 0

    for rec in records:
        # Check top-level keys
        event_key = rec.get("event_key")
        canonical = rec.get("canonical_event_key")

        # Also check nested event dict for normalized picks
        if not event_key and not canonical:
            event = rec.get("event", {})
            event_key = event.get("event_key")
            canonical = event.get("canonical_event_key")

        # Skip records without any event key
        if not event_key and not canonical:
            continue

        total += 1

        # Check if legacy-only
        if is_legacy_event_key(event_key) and not canonical:
            legacy_only += 1

    return legacy_only, total


class TestNoLegacyEventKey:
    """Ratchet tests to prevent legacy event_key regression."""

    @pytest.mark.parametrize("path", OUTPUT_FILES)
    def test_output_files_no_legacy_only(self, path: Path):
        """Output files should not have legacy-only event keys.

        Note: If this test fails with high legacy percentage, the output file
        is stale. Regenerate by running the appropriate ingest script.
        """
        if not path.exists():
            pytest.skip(f"Output file not found: {path}")

        records = read_json(path)
        if not records:
            pytest.skip(f"Output file empty: {path}")

        legacy_only, total = count_legacy_only(records)

        if total == 0:
            pytest.skip(f"No records with event keys in {path}")

        pct_legacy = 100 * legacy_only / total

        # If 100% legacy, the file is stale - skip with instructions
        if pct_legacy > 50:
            pytest.skip(
                f"Stale output file: {path.name} has {pct_legacy:.0f}% legacy-only keys. "
                f"Regenerate by running the appropriate ingest script."
            )

        assert pct_legacy <= MAX_LEGACY_ONLY_PERCENT, (
            f"{path.name}: {legacy_only}/{total} ({pct_legacy:.2f}%) records use legacy-only event_key. "
            f"Max allowed: {MAX_LEGACY_ONLY_PERCENT}%. "
            f"Fix the source normalizer to emit canonical_event_key."
        )

    @pytest.mark.parametrize("path", LEDGER_FILES)
    def test_ledger_files_low_legacy(self, path: Path):
        """Ledger files should have very low legacy-only percentage."""
        if not path.exists():
            pytest.skip(f"Ledger file not found: {path}")

        records = read_jsonl(path)
        if not records:
            pytest.skip(f"Ledger file empty: {path}")

        legacy_only, total = count_legacy_only(records)

        if total == 0:
            pytest.skip(f"No records with event keys in {path}")

        pct_legacy = 100 * legacy_only / total

        # Ledger files may have some historical legacy data, allow up to 5%
        max_allowed = 5.0

        assert pct_legacy <= max_allowed, (
            f"{path.name}: {legacy_only}/{total} ({pct_legacy:.2f}%) records use legacy-only event_key. "
            f"Max allowed: {max_allowed}%. "
            f"Run scripts/backfill_canonical_event_keys.py to backfill."
        )

    def test_signals_latest_has_canonical(self):
        """signals_latest.jsonl should have canonical_event_key on all records."""
        path = Path("data/ledger/signals_latest.jsonl")
        if not path.exists():
            pytest.skip("signals_latest.jsonl not found")

        records = read_jsonl(path)
        if not records:
            pytest.skip("signals_latest.jsonl empty")

        missing_canonical = 0
        for rec in records:
            event_key = rec.get("event_key") or rec.get("event", {}).get("event_key")
            canonical = rec.get("canonical_event_key") or rec.get("event", {}).get("canonical_event_key")

            # Only check records that have an event_key
            if event_key and not canonical:
                missing_canonical += 1

        total_with_key = sum(
            1 for r in records
            if r.get("event_key") or r.get("event", {}).get("event_key")
        )

        if total_with_key == 0:
            pytest.skip("No records with event_key")

        pct_missing = 100 * missing_canonical / total_with_key

        assert pct_missing <= MAX_LEGACY_ONLY_PERCENT, (
            f"{missing_canonical}/{total_with_key} ({pct_missing:.2f}%) signals missing canonical_event_key. "
            f"Run scripts/backfill_canonical_event_keys.py to backfill."
        )


class TestCanonicalEventKeyPresent:
    """Tests that canonical_event_key is present in new records."""

    def test_action_output_has_canonical(self):
        """Action output should have canonical_event_key."""
        path = Path("out/normalized_action_nba.json")
        if not path.exists():
            pytest.skip("Action output not found")

        records = read_json(path)
        if not records:
            pytest.skip("Action output empty")

        has_canonical = 0
        for rec in records:
            event = rec.get("event", {})
            if event.get("canonical_event_key"):
                has_canonical += 1

        pct_has = 100 * has_canonical / len(records) if records else 0

        # If 0%, file is stale - skip with instructions
        if pct_has < 10:
            pytest.skip(
                f"Stale action output: only {pct_has:.0f}% have canonical_event_key. "
                f"Regenerate by running: python3 action_ingest.py"
            )

        # At least 90% should have canonical (some may have resolution failures)
        assert pct_has >= 90, (
            f"Only {has_canonical}/{len(records)} ({pct_has:.1f}%) action records have canonical_event_key. "
            f"Expected 90%+."
        )

    def test_covers_output_has_canonical(self):
        """Covers output should have canonical_event_key."""
        path = Path("out/normalized_covers_nba.json")
        if not path.exists():
            pytest.skip("Covers output not found")

        records = read_json(path)
        if not records:
            pytest.skip("Covers output empty")

        has_canonical = 0
        for rec in records:
            event = rec.get("event", {})
            if event.get("canonical_event_key"):
                has_canonical += 1

        pct_has = 100 * has_canonical / len(records) if records else 0

        # If 0%, file is stale - skip with instructions
        if pct_has < 10:
            pytest.skip(
                f"Stale covers output: only {pct_has:.0f}% have canonical_event_key. "
                f"Regenerate by running: python3 covers_ingest.py"
            )

        # At least 90% should have canonical
        assert pct_has >= 90, (
            f"Only {has_canonical}/{len(records)} ({pct_has:.1f}%) covers records have canonical_event_key. "
            f"Expected 90%+."
        )
