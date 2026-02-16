"""Test that build_research_dataset_nba.py correctly joins grades to signals.

This test validates:
1. When multiple grades exist per signal_id, the best (latest terminal) grade is chosen
2. Output grade_status is GRADED for signals with WIN/LOSS/PUSH grades
3. The same grade is used for all occurrences of a signal_id
"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List

import pytest

# Import the functions under test
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.build_research_dataset_nba import (
    build_rows,
    pick_best_grade,
)


def make_signal(signal_id: str, day_key: str = "NBA:2026:01:23", selection: str = "NBA:player::points::OVER") -> Dict[str, Any]:
    """Create a minimal signal record."""
    return {
        "signal_id": signal_id,
        "day_key": day_key,
        "event_key": f"{day_key}:DEN@LAL",
        "market_type": "player_prop",
        "selection": selection,
        "direction": "OVER",
        "atomic_stat": "points",
        "sources": ["action", "covers"],
        "sources_combo": "action|covers",
        "score": 75,
    }


def make_grade(
    signal_id: str,
    status: str,
    graded_at_utc: str,
    result: str = None,
    units: float = None,
) -> Dict[str, Any]:
    """Create a minimal grade record."""
    return {
        "signal_id": signal_id,
        "status": status,
        "result": result,
        "units": units,
        "graded_at_utc": graded_at_utc,
        "day_key": "NBA:2026:01:23",
    }


class TestPickBestGrade:
    """Test the pick_best_grade helper function."""

    def test_picks_terminal_over_ineligible(self):
        """Terminal status (WIN/LOSS/PUSH) should be preferred over INELIGIBLE."""
        grades = [
            make_grade("A", "INELIGIBLE", "2026-01-24T12:00:00+00:00"),
            make_grade("A", "WIN", "2026-01-24T10:00:00+00:00", result="WIN", units=0.91),
        ]
        best = pick_best_grade(grades)
        assert best is not None
        assert best["status"] == "WIN"

    def test_picks_latest_among_terminal(self):
        """Among multiple terminal grades, pick the latest by graded_at_utc."""
        grades = [
            make_grade("A", "WIN", "2026-01-24T10:00:00+00:00", result="WIN"),
            make_grade("A", "LOSS", "2026-01-24T14:00:00+00:00", result="LOSS"),
            make_grade("A", "WIN", "2026-01-24T12:00:00+00:00", result="WIN"),
        ]
        best = pick_best_grade(grades)
        assert best is not None
        assert best["status"] == "LOSS"
        assert best["graded_at_utc"] == "2026-01-24T14:00:00+00:00"

    def test_picks_ineligible_when_no_terminal(self):
        """If no terminal grades exist, INELIGIBLE should be picked."""
        grades = [
            make_grade("A", "PENDING", "2026-01-24T10:00:00+00:00"),
            make_grade("A", "INELIGIBLE", "2026-01-24T12:00:00+00:00"),
            make_grade("A", "ERROR", "2026-01-24T14:00:00+00:00"),
        ]
        best = pick_best_grade(grades)
        assert best is not None
        assert best["status"] == "INELIGIBLE"

    def test_returns_none_for_empty_list(self):
        """Empty list should return None."""
        best = pick_best_grade([])
        assert best is None

    def test_returns_none_for_only_pending_error(self):
        """If only PENDING/ERROR grades, return None."""
        grades = [
            make_grade("A", "PENDING", "2026-01-24T10:00:00+00:00"),
            make_grade("A", "ERROR", "2026-01-24T12:00:00+00:00"),
        ]
        best = pick_best_grade(grades)
        assert best is None


class TestBuildRowsGradeJoin:
    """Test that build_rows correctly joins grades to signals."""

    def test_joins_grade_to_signal_by_id(self):
        """Grade should be joined to signal by signal_id."""
        signals = [
            make_signal("sig_A"),
            make_signal("sig_B"),
        ]
        grades = [
            make_grade("sig_A", "WIN", "2026-01-24T12:00:00+00:00", result="WIN", units=0.91),
            make_grade("sig_B", "LOSS", "2026-01-24T12:00:00+00:00", result="LOSS", units=-1.0),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            signals_path = Path(tmpdir) / "signals.jsonl"
            grades_path = Path(tmpdir) / "grades.jsonl"

            with signals_path.open("w") as f:
                for s in signals:
                    f.write(json.dumps(s) + "\n")

            with grades_path.open("w") as f:
                for g in grades:
                    f.write(json.dumps(g) + "\n")

            rows = build_rows(signals_path, grades_path)

        assert len(rows) == 2

        row_a = next(r for r in rows if r["signal_id"] == "sig_A")
        row_b = next(r for r in rows if r["signal_id"] == "sig_B")

        assert row_a["grade_status"] == "GRADED"
        assert row_a["result"] == "WIN"

        assert row_b["grade_status"] == "GRADED"
        assert row_b["result"] == "LOSS"

    def test_picks_latest_terminal_grade_among_multiple(self):
        """When multiple grades exist per signal_id, pick latest terminal."""
        signals = [make_signal("sig_A")]
        grades = [
            make_grade("sig_A", "PENDING", "2026-01-24T08:00:00+00:00"),
            make_grade("sig_A", "WIN", "2026-01-24T10:00:00+00:00", result="WIN"),
            make_grade("sig_A", "LOSS", "2026-01-24T14:00:00+00:00", result="LOSS"),  # latest terminal
            make_grade("sig_A", "WIN", "2026-01-24T12:00:00+00:00", result="WIN"),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            signals_path = Path(tmpdir) / "signals.jsonl"
            grades_path = Path(tmpdir) / "grades.jsonl"

            with signals_path.open("w") as f:
                for s in signals:
                    f.write(json.dumps(s) + "\n")

            with grades_path.open("w") as f:
                for g in grades:
                    f.write(json.dumps(g) + "\n")

            rows = build_rows(signals_path, grades_path)

        assert len(rows) == 1
        row = rows[0]
        assert row["grade_status"] == "GRADED"
        assert row["result"] == "LOSS"  # The latest terminal grade

    def test_same_grade_used_for_multiple_occurrences(self):
        """All occurrences of a signal_id should get the same grade."""
        # Two signal occurrences with same signal_id
        signals = [
            make_signal("sig_A"),
            make_signal("sig_A"),  # duplicate occurrence
        ]
        grades = [
            make_grade("sig_A", "WIN", "2026-01-24T12:00:00+00:00", result="WIN", units=0.91),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            signals_path = Path(tmpdir) / "signals.jsonl"
            grades_path = Path(tmpdir) / "grades.jsonl"

            with signals_path.open("w") as f:
                for s in signals:
                    f.write(json.dumps(s) + "\n")

            with grades_path.open("w") as f:
                for g in grades:
                    f.write(json.dumps(g) + "\n")

            rows = build_rows(signals_path, grades_path)

        # Both occurrences should have the same grade
        assert len(rows) == 2
        for row in rows:
            assert row["grade_status"] == "GRADED"
            assert row["result"] == "WIN"

    def test_ungraded_when_no_grade_exists(self):
        """Signal with no matching grade should be UNGRADED."""
        signals = [make_signal("sig_A")]
        grades = []  # No grades

        with tempfile.TemporaryDirectory() as tmpdir:
            signals_path = Path(tmpdir) / "signals.jsonl"
            grades_path = Path(tmpdir) / "grades.jsonl"

            with signals_path.open("w") as f:
                for s in signals:
                    f.write(json.dumps(s) + "\n")

            with grades_path.open("w") as f:
                pass  # Empty file

            rows = build_rows(signals_path, grades_path)

        assert len(rows) == 1
        assert rows[0]["grade_status"] == "UNGRADED"
        assert rows[0]["result"] is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
