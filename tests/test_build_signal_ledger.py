"""Focused smoke tests for scripts/build_signal_ledger.py.

Covers two fail-closed invariants introduced alongside the incremental
checkpoint mode:

1. Supersession eviction works on the merged existing+new occurrences
   list. A checkpoint-cached occurrence from a superseded run must still
   be dropped when a newer run exists for the same day, even if the
   newer run produced no replacement for that signal.

2. History reload triggers when a history-input file's mtime changes
   on disk, regardless of `history_loaded=True` in the checkpoint.

These invariants are enforced by `evict_superseded_and_dedup_by_day()`
and `_history_input_changed()` respectively. Both are pure functions
so tests stay filesystem-isolated and quick.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from scripts.build_signal_ledger import (
    _history_input_changed,
    evict_superseded_and_dedup_by_day,
)


# ---------------------------------------------------------------------------
# Supersession eviction on merged existing+new occurrences
# ---------------------------------------------------------------------------

def _occ(signal_key: str, run_id: str, observed_at_utc: str | None = None) -> dict:
    """Minimal occurrence shape used by evict_superseded_and_dedup_by_day."""
    obs = observed_at_utc or (run_id[:10] + "T12:00:00Z")
    return {
        "signal_key": signal_key,
        "run_id": run_id,
        "observed_at_utc": obs,
    }


def test_superseded_run_evicted_even_without_replacement():
    """A cached old-run occurrence must be dropped when a newer run exists
    for the same day, even if the newer run produced no replacement.

    Regression: this was the concern behind the C1 review — if incremental
    mode re-used existing_occurrences and the eviction stage didn't re-run
    over the merged list, stale occurrences from superseded runs would
    leak forward indefinitely.
    """
    day = "2026-04-15"
    old_run = f"{day}T10-00-00Z-aaaaaa"  # early morning run
    new_run = f"{day}T20-00-00Z-bbbbbb"  # evening run (newer)

    # existing_occurrences simulates the checkpoint-cached signals from
    # the early run; the merged occurrence list now includes both.
    existing = [_occ("sigA", old_run)]
    new = []  # newer run produced NO occurrence for sigA (e.g. bug fix removed it)
    merged = existing + new

    latest_run_per_day = {day: new_run}
    kept, evicted = evict_superseded_and_dedup_by_day(merged, latest_run_per_day)

    assert evicted == 1, "superseded run_id must be evicted"
    assert kept == [], "no occurrence from the superseded run should survive"


def test_current_run_kept_when_no_newer_run():
    """If the run_id is the current latest, it is kept."""
    day = "2026-04-15"
    run = f"{day}T20-00-00Z-current"
    occs = [_occ("sigA", run)]
    kept, evicted = evict_superseded_and_dedup_by_day(occs, {day: run})
    assert evicted == 0
    assert kept == occs


def test_history_backfill_rows_never_evicted():
    """run_ids without 'Z-' (history backfill tags) must be kept
    unconditionally, even if newer runs exist for the day."""
    day = "2026-04-15"
    new_run = f"{day}T20-00-00Z-newest"
    occs = [
        _occ("sigA", "juicereel_history"),
        _occ("sigB", "action_history"),
        _occ("sigC", "dimers_history"),
    ]
    kept, evicted = evict_superseded_and_dedup_by_day(occs, {day: new_run})
    assert evicted == 0
    assert len(kept) == 3


def test_latest_run_chosen_when_duplicates_exist():
    """When two occurrences share (signal_key, day), the one with the
    lexicographically-later run_id wins. This is the normal dedup path."""
    day = "2026-04-15"
    old_run = f"{day}T10-00-00Z-aaa"
    new_run = f"{day}T20-00-00Z-bbb"
    occs = [
        _occ("sigA", old_run),
        _occ("sigA", new_run),  # later run_id for the same signal
    ]
    kept, evicted = evict_superseded_and_dedup_by_day(
        occs, latest_run_per_day={day: new_run}
    )
    # Per eviction rule: old_run < new_run → evicted.
    assert evicted == 1
    assert len(kept) == 1
    assert kept[0]["run_id"] == new_run


def test_different_signals_on_same_day_both_kept():
    """Different signal_keys on the same day should both survive dedup
    when both come from the same (latest) run."""
    day = "2026-04-15"
    run = f"{day}T20-00-00Z-one"
    occs = [_occ("sigA", run), _occ("sigB", run)]
    kept, evicted = evict_superseded_and_dedup_by_day(occs, {day: run})
    assert evicted == 0
    assert {o["signal_key"] for o in kept} == {"sigA", "sigB"}


# ---------------------------------------------------------------------------
# History input mtime detection (C2: fail-closed on disk change)
# ---------------------------------------------------------------------------

def test_missing_file_is_not_a_change(tmp_path: Path):
    """A history file that does not exist is not "changed" — nothing to
    reload. Prevents phantom reloads when an optional history file is
    absent."""
    missing = tmp_path / "does_not_exist.jsonl"
    assert _history_input_changed(missing, recorded_mtimes={}) is False


def test_first_appearance_triggers_reload(tmp_path: Path):
    """A history file that exists but has no recorded mtime in the
    checkpoint must trigger a reload. First-encounter is fail-closed:
    we have no proof we already loaded it."""
    f = tmp_path / "backfill.jsonl"
    f.write_text("{}\n")
    assert _history_input_changed(f, recorded_mtimes={}) is True


def test_unchanged_file_does_not_trigger_reload(tmp_path: Path):
    """When recorded mtime matches current mtime, no reload."""
    f = tmp_path / "backfill.jsonl"
    f.write_text("{}\n")
    current_mtime = f.stat().st_mtime
    assert _history_input_changed(f, {str(f): current_mtime}) is False


def test_newer_mtime_triggers_reload(tmp_path: Path):
    """When the file on disk has been updated since the recorded mtime,
    reload must be triggered. This is the central C2 fail-closed rule —
    a backfill file refreshed on disk MUST be re-read, regardless of
    the checkpoint's `history_loaded=True` flag.
    """
    f = tmp_path / "backfill.jsonl"
    f.write_text("old\n")
    old_mtime = f.stat().st_mtime
    # Simulate a newer write. Sleep a hair past filesystem mtime resolution.
    time.sleep(1.1)
    f.write_text("new\n")
    new_mtime = f.stat().st_mtime
    assert new_mtime > old_mtime
    assert _history_input_changed(f, {str(f): old_mtime}) is True


def test_malformed_recorded_mtime_triggers_reload(tmp_path: Path):
    """If the checkpoint contains a non-numeric mtime (e.g. from a
    corrupted write), treat the file as changed. Fail-closed."""
    f = tmp_path / "backfill.jsonl"
    f.write_text("x\n")
    assert _history_input_changed(f, {str(f): "not-a-number"}) is True


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
