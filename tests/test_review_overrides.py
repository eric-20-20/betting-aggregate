"""Tests for scripts/apply_overrides.py."""
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from scripts.apply_overrides import (
    build_approved_slate,
    resolve_published_values,
    validate_overrides,
    write_approved_slate,
)
# Import the module as the scripts see it (via sys.path inserting src/)
import review_paths as rp


# ── Fixtures ──────────────────────────────────────────────────────────

def _make_candidate(pick_id="pick_001", auto_tier="B", auto_included=True, **kw):
    c = {
        "pick_id": pick_id,
        "date": "2026-04-20",
        "sport": "NBA",
        "game_label": "LAL @ BOS",
        "bet_label": "Lebron James OVER 25.5 points",
        "market": "player_prop",
        "selection": "NBA:lebron_james::points::OVER",
        "line": 25.5,
        "odds": None,
        "auto_tier": auto_tier,
        "tier_reason": f"{auto_tier}: test reason",
        "support_count": 3,
        "pattern_name": None,
        "source_list": ["action", "covers"],
        "auto_included": auto_included,
        "website_eligible": True,
        "published_tier": None,
        "published_included": None,
        "override_note": None,
        "wilson_score": 0.52,
        "confidence_score": 50,
        "gate_reason": None,
        "primary_record": {"wins": 60, "losses": 50, "n": 110},
        "matched_pattern": None,
    }
    c.update(kw)
    return c


def _make_slate(candidates):
    return {
        "meta": {
            "date": "2026-04-20",
            "sport": "NBA",
            "generated_at_utc": "2026-04-20T15:00:00Z",
            "source_plays_file": "data/plays/plays_2026-04-20.json",
            "total_candidates": len(candidates),
            "auto_included_count": sum(1 for c in candidates if c["auto_included"]),
            "near_miss_count": 0,
        },
        "candidates": candidates,
    }


# ── Validation tests ──────────────────────────────────────────────────

class TestValidation:
    def test_valid_override(self):
        errors = validate_overrides(
            [{"pick_id": "pick_001", "published_tier": "A", "published_included": True}],
            {"pick_001"},
        )
        assert errors == []

    def test_unknown_pick_id(self):
        errors = validate_overrides(
            [{"pick_id": "nonexistent"}],
            {"pick_001"},
        )
        assert any("not found in slate" in e for e in errors)

    def test_invalid_tier(self):
        errors = validate_overrides(
            [{"pick_id": "pick_001", "published_tier": "X"}],
            {"pick_001"},
        )
        assert any("invalid published_tier" in e for e in errors)

    def test_published_included_not_bool(self):
        errors = validate_overrides(
            [{"pick_id": "pick_001", "published_included": "yes"}],
            {"pick_001"},
        )
        assert any("must be boolean" in e for e in errors)

    def test_duplicate_pick_id(self):
        errors = validate_overrides(
            [{"pick_id": "pick_001"}, {"pick_id": "pick_001"}],
            {"pick_001"},
        )
        assert any("duplicate" in e for e in errors)

    def test_unknown_fields(self):
        errors = validate_overrides(
            [{"pick_id": "pick_001", "bad_field": "foo"}],
            {"pick_001"},
        )
        assert any("unknown fields" in e for e in errors)

    def test_missing_pick_id(self):
        errors = validate_overrides(
            [{"published_tier": "A"}],
            {"pick_001"},
        )
        assert any("missing pick_id" in e for e in errors)

    def test_override_note_must_be_string(self):
        errors = validate_overrides(
            [{"pick_id": "pick_001", "override_note": 123}],
            {"pick_001"},
        )
        assert any("must be a string" in e for e in errors)


# ── Published value resolution tests ──────────────────────────────────

class TestResolvePublished:
    def test_no_override_fallback(self):
        c = _make_candidate(auto_tier="B", auto_included=True)
        result = resolve_published_values(c, None)
        assert result["published_tier"] == "B"
        assert result["published_included"] is True

    def test_override_promotes_tier(self):
        c = _make_candidate(auto_tier="D", auto_included=False)
        override = {"pick_id": "pick_001", "published_tier": "B", "published_included": True}
        result = resolve_published_values(c, override)
        assert result["published_tier"] == "B"
        assert result["published_included"] is True

    def test_override_excludes_pick(self):
        c = _make_candidate(auto_tier="A", auto_included=True)
        override = {"pick_id": "pick_001", "published_included": False}
        result = resolve_published_values(c, override)
        assert result["published_tier"] == "A"
        assert result["published_included"] is False

    def test_override_tier_only_derives_included(self):
        c = _make_candidate(auto_tier="D", auto_included=False)
        override = {"pick_id": "pick_001", "published_tier": "B"}
        result = resolve_published_values(c, override)
        assert result["published_tier"] == "B"
        assert result["published_included"] is True

    def test_override_to_d_tier_derives_excluded(self):
        c = _make_candidate(auto_tier="A", auto_included=True)
        override = {"pick_id": "pick_001", "published_tier": "D"}
        result = resolve_published_values(c, override)
        assert result["published_tier"] == "D"
        assert result["published_included"] is False

    def test_auto_values_never_mutated(self):
        c = _make_candidate(auto_tier="B", auto_included=True)
        override = {"pick_id": "pick_001", "published_tier": "A", "published_included": True}
        result = resolve_published_values(c, override)
        assert result["auto_tier"] == "B"
        assert result["auto_included"] is True
        assert result["published_tier"] == "A"

    def test_override_note_applied(self):
        c = _make_candidate()
        override = {"pick_id": "pick_001", "override_note": "Key player out"}
        result = resolve_published_values(c, override)
        assert result["override_note"] == "Key player out"


# ── End-to-end approved slate tests ───────────────────────────────────

class TestBuildApprovedSlate:
    def test_no_overrides(self, tmp_path):
        candidates = [
            _make_candidate("p1", "A", True),
            _make_candidate("p2", "B", True),
            _make_candidate("p3", "D", False),
        ]
        slate = _make_slate(candidates)

        review_root = tmp_path / "review"
        dated_dir = review_root / "nba" / "2026-04-20"
        dated_dir.mkdir(parents=True)
        (dated_dir / "review_slate.json").write_text(json.dumps(slate))

        with patch.object(rp, "REVIEW_ROOT", review_root):
            approved = build_approved_slate("2026-04-20", "NBA")

        assert approved["meta"]["overrides_applied"] == 0
        assert approved["meta"]["published_included_count"] == 2
        for c in approved["candidates"]:
            assert c["published_tier"] is not None
            assert c["published_included"] is not None

    def test_with_overrides(self, tmp_path):
        candidates = [
            _make_candidate("p1", "A", True),
            _make_candidate("p2", "B", True),
            _make_candidate("p3", "D", False),
        ]
        slate = _make_slate(candidates)
        overrides = {
            "overrides": [
                {"pick_id": "p1", "published_included": False, "override_note": "skip"},
                {"pick_id": "p3", "published_tier": "B", "published_included": True},
            ]
        }

        review_root = tmp_path / "review"
        dated_dir = review_root / "nba" / "2026-04-20"
        dated_dir.mkdir(parents=True)
        (dated_dir / "review_slate.json").write_text(json.dumps(slate))
        (dated_dir / "overrides.json").write_text(json.dumps(overrides))

        with patch.object(rp, "REVIEW_ROOT", review_root):
            approved = build_approved_slate("2026-04-20", "NBA")

        assert approved["meta"]["overrides_applied"] == 2
        assert approved["meta"]["published_included_count"] == 2

        by_id = {c["pick_id"]: c for c in approved["candidates"]}
        assert by_id["p1"]["published_included"] is False
        assert by_id["p3"]["published_tier"] == "B"
        assert by_id["p3"]["published_included"] is True
        assert by_id["p2"]["published_tier"] == "B"
        assert by_id["p2"]["published_included"] is True

    def test_invalid_override_exits(self, tmp_path):
        candidates = [_make_candidate("p1")]
        slate = _make_slate(candidates)
        overrides = {"overrides": [{"pick_id": "nonexistent"}]}

        review_root = tmp_path / "review"
        dated_dir = review_root / "nba" / "2026-04-20"
        dated_dir.mkdir(parents=True)
        (dated_dir / "review_slate.json").write_text(json.dumps(slate))
        (dated_dir / "overrides.json").write_text(json.dumps(overrides))

        with patch.object(rp, "REVIEW_ROOT", review_root):
            with pytest.raises(SystemExit) as exc_info:
                build_approved_slate("2026-04-20", "NBA")
            assert exc_info.value.code == 1

    def test_historical_date_works(self, tmp_path):
        """Override application works for arbitrary historical dates."""
        candidates = [_make_candidate("p1", "B", True)]
        slate = _make_slate(candidates)
        slate["meta"]["date"] = "2026-01-15"

        review_root = tmp_path / "review"
        dated_dir = review_root / "nba" / "2026-01-15"
        dated_dir.mkdir(parents=True)
        (dated_dir / "review_slate.json").write_text(json.dumps(slate))

        with patch.object(rp, "REVIEW_ROOT", review_root):
            approved = build_approved_slate("2026-01-15", "NBA")

        assert approved["meta"]["date"] == "2026-01-15"
        assert approved["meta"]["published_included_count"] == 1
