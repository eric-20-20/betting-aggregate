"""Tests for scripts/generate_review_slate.py."""
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from scripts.generate_review_slate import (
    build_bet_label,
    build_game_label,
    build_review_row,
    build_tier_reason,
    generate_review_slate,
    is_near_miss,
    write_slate,
)
# Import the module as the scripts see it (via sys.path inserting src/)
import review_paths as rp


# ── Fixtures ──────────────────────────────────────────────────────────

def _make_signal(**overrides):
    base = {
        "signal_id": "abc123",
        "selection": "NBA:lebron_james::points::OVER",
        "market_type": "player_prop",
        "atomic_stat": "points",
        "direction": "OVER",
        "away_team": "LAL",
        "home_team": "BOS",
        "line": 25.5,
        "sources_count": 3,
        "sources_present": ["action", "covers", "dimers"],
        "sources_combo": "action|covers|dimers",
        "event_key": "NBA:2026:04:20:LAL@BOS",
        "day_key": "NBA:2026:04:20",
    }
    base.update(overrides)
    return base


def _make_play(tier="B", wilson=0.52, pattern=None, **overrides):
    play = {
        "tier": tier,
        "wilson_score": wilson,
        "confidence": "medium",
        "confidence_score": 50,
        "signal": _make_signal(),
        "primary_record": {"wins": 60, "losses": 50, "n": 110, "win_pct": 0.545,
                           "wilson_lower": 0.45, "lookup_level": "combo_market",
                           "label": "3_source / player_prop"},
        "recency_adjustment": 0,
        "expert_adjustment": 0,
        "stat_adjustment": 0,
        "matched_pattern": pattern,
        "factors": [],
        "supporting_factors": [],
        "summary": "Test play",
    }
    play.update(overrides)
    return play


def _make_filtered(wilson=0.43, gate_reason="low_wilson:0.430"):
    return {
        "signal": _make_signal(signal_id="filtered_abc"),
        "wilson_score": wilson,
        "composite_score": wilson,
        "tier": "D",
        "gate_reason": gate_reason,
        "summary": "Filtered play",
    }


# ── Tests ─────────────────────────────────────────────────────────────

class TestBetLabel:
    def test_player_prop(self):
        sig = _make_signal()
        label = build_bet_label(sig, "player_prop")
        assert "Lebron James" in label
        assert "OVER" in label
        assert "25.5" in label
        assert "points" in label

    def test_spread(self):
        sig = _make_signal(selection="LAL", direction="LAL", line=-3.5,
                           market_type="spread")
        label = build_bet_label(sig, "spread")
        assert "LAL" in label
        assert "-3.5" in label

    def test_total(self):
        sig = _make_signal(selection="OVER", direction="OVER", line=215.5,
                           market_type="total")
        label = build_bet_label(sig, "total")
        assert "OVER" in label
        assert "215.5" in label


class TestGameLabel:
    def test_normal(self):
        sig = _make_signal()
        assert build_game_label(sig) == "LAL @ BOS"

    def test_missing_teams(self):
        sig = _make_signal(away_team=None, home_team=None)
        assert build_game_label(sig) == "? @ ?"


class TestBuildReviewRow:
    def test_passed_play(self):
        play = _make_play()
        row = build_review_row(play, "2026-04-20", "NBA", source="passed")
        assert row["pick_id"] == "abc123"
        assert row["auto_tier"] == "B"
        assert row["auto_included"] is True
        assert row["published_tier"] is None
        assert row["published_included"] is None
        assert row["website_eligible"] is True
        assert row["market"] == "player_prop"
        assert row["support_count"] == 3
        assert row["tier_reason"]
        assert "_play_data" not in row

    def test_filtered_entry(self):
        entry = _make_filtered(wilson=0.43)
        row = build_review_row(entry, "2026-04-20", "NBA", source="filtered")
        assert row["pick_id"] == "filtered_abc"
        assert row["auto_tier"] == "D"
        assert row["auto_included"] is False
        assert row["website_eligible"] is False
        assert row["gate_reason"] == "low_wilson:0.430"
        assert "_play_data" in row

    def test_pick_id_matches_signal_id(self):
        play = _make_play()
        row = build_review_row(play, "2026-04-20", "NBA")
        assert row["pick_id"] == play["signal"]["signal_id"]

    def test_tier_reason_populated(self):
        play = _make_play()
        row = build_review_row(play, "2026-04-20", "NBA")
        assert len(row["tier_reason"]) > 0


class TestNearMiss:
    def test_above_floor(self):
        assert is_near_miss({"wilson_score": 0.43}) is True

    def test_at_floor(self):
        assert is_near_miss({"wilson_score": 0.42}) is True

    def test_below_floor(self):
        assert is_near_miss({"wilson_score": 0.41}) is False


class TestTierReason:
    def test_with_pattern(self):
        pattern = {"label": "test pattern", "hist": {"record": "60-40", "win_pct": 0.6}}
        play = _make_play(tier="A", pattern=pattern)
        reason = build_tier_reason(play, "passed")
        assert "pattern 'test pattern'" in reason
        assert "60-40" in reason

    def test_without_pattern(self):
        play = _make_play(tier="B")
        reason = build_tier_reason(play, "passed")
        assert "no pattern" in reason

    def test_filtered(self):
        entry = _make_filtered()
        reason = build_tier_reason(entry, "filtered")
        assert "D(near-miss)" in reason
        assert "gated" in reason

    def test_adjustments_shown(self):
        play = _make_play(recency_adjustment=0.03, expert_adjustment=-0.015)
        reason = build_tier_reason(play, "passed")
        assert "recency" in reason
        assert "expert" in reason


class TestGenerateSlate:
    def test_with_real_data(self, tmp_path):
        """Test using a minimal synthetic plays file."""
        plays_dir = tmp_path / "data" / "plays"
        plays_dir.mkdir(parents=True)

        plays_data = {
            "meta": {"date": "2026-04-20", "day_of_week": "Monday",
                     "total_signals": 10, "scorable_signals": 5,
                     "excluded_count": 0, "filtered_count": 1,
                     "exported_count": 2, "tier_counts": {"A": 1, "B": 1, "D": 1}},
            "plays": [
                _make_play(tier="A", wilson=0.58),
                _make_play(tier="B", wilson=0.48),
            ],
            "filtered": [
                _make_filtered(wilson=0.43),
                _make_filtered(wilson=0.38),  # below near-miss threshold
            ],
        }
        (plays_dir / "plays_2026-04-20.json").write_text(json.dumps(plays_data))

        with patch.object(rp, "REVIEW_ROOT", tmp_path / "data" / "review"):
            with patch("scripts.generate_review_slate.plays_path",
                       return_value=plays_dir / "plays_2026-04-20.json"):
                slate = generate_review_slate("2026-04-20", "NBA")

        assert slate["meta"]["total_candidates"] == 3
        assert slate["meta"]["auto_included_count"] == 2
        assert slate["meta"]["near_miss_count"] == 1

        tiers = [c["auto_tier"] for c in slate["candidates"]]
        assert tiers.count("A") == 1
        assert tiers.count("B") == 1
        assert tiers.count("D") == 1

    def test_csv_written(self, tmp_path):
        """Test that CSV output matches JSON row count."""
        plays_dir = tmp_path / "data" / "plays"
        plays_dir.mkdir(parents=True)
        review_root = tmp_path / "data" / "review"

        plays_data = {
            "meta": {"date": "2026-04-20", "day_of_week": "Monday",
                     "total_signals": 5, "scorable_signals": 3,
                     "excluded_count": 0, "filtered_count": 0,
                     "exported_count": 2, "tier_counts": {"A": 1, "B": 1}},
            "plays": [_make_play(tier="A"), _make_play(tier="B")],
            "filtered": [],
        }
        (plays_dir / "plays_2026-04-20.json").write_text(json.dumps(plays_data))

        with patch.object(rp, "REVIEW_ROOT", review_root):
            with patch("scripts.generate_review_slate.plays_path",
                       return_value=plays_dir / "plays_2026-04-20.json"):
                slate = generate_review_slate("2026-04-20", "NBA")
                json_path, csv_path = write_slate(slate, "NBA")

        assert json_path.exists()
        assert csv_path.exists()

        import csv
        with open(csv_path) as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == len(slate["candidates"])


class TestDateValidation:
    def test_valid_date(self):
        from src.review_paths import validate_date
        assert validate_date("2026-04-20") == "2026-04-20"

    def test_invalid_date_exits(self):
        from src.review_paths import validate_date
        with pytest.raises(SystemExit):
            validate_date("bad-date")

    def test_invalid_format_exits(self):
        from src.review_paths import validate_date
        with pytest.raises(SystemExit):
            validate_date("20260420")


class TestReviewPaths:
    def test_dated_dir_layout(self):
        assert rp.review_dir("NBA", "2026-04-20") == Path("data/review/nba/2026-04-20")
        assert rp.review_dir("NCAAB", "2026-04-20") == Path("data/review/ncaab/2026-04-20")

    def test_slate_json_path(self):
        p = rp.slate_json_path("NBA", "2026-04-20")
        assert p == Path("data/review/nba/2026-04-20/review_slate.json")

    def test_approved_path(self):
        p = rp.approved_path("NBA", "2026-04-20")
        assert p == Path("data/review/nba/2026-04-20/approved_slate.json")

    def test_overrides_path(self):
        p = rp.overrides_path("NBA", "2026-04-20")
        assert p == Path("data/review/nba/2026-04-20/overrides.json")

    def test_plays_path_nba(self):
        p = rp.plays_path("NBA", "2026-04-20")
        assert p == Path("data/plays/plays_2026-04-20.json")

    def test_plays_path_ncaab(self):
        p = rp.plays_path("NCAAB", "2026-04-20")
        assert p == Path("data/plays/ncaab/plays_2026-04-20.json")

    def test_parse_date_range(self):
        dates = rp.parse_date_range("2026-04-10", "2026-04-12")
        assert dates == ["2026-04-10", "2026-04-11", "2026-04-12"]

    def test_parse_date_range_single(self):
        dates = rp.parse_date_range("2026-04-10", "2026-04-10")
        assert dates == ["2026-04-10"]
