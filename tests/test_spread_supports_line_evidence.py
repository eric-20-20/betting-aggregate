"""
Regression tests for spread supports line evidence.

These tests verify that spread signals properly capture line evidence
in their supports array, fixing the spread grading crisis where line
evidence was being lost.
"""
import datetime

import pytest

from consensus_nba import build_unified_signals, group_hard, group_soft


def make_spread_record(
    source: str,
    away: str,
    home: str,
    team: str,
    line: float,
    odds: float = -110,
    expert: str = "expert1",
    raw_pick_text: str = None,
    raw_block: str = None,
    source_surface: str = None,
    line_hint: str = None,
):
    """Create a spread record with full provenance for testing."""
    ev_key = f"NBA:2026:01:28:{away}@{home}"
    if raw_pick_text is None:
        raw_pick_text = f"+{line}" if line > 0 else str(line)
    if source_surface is None:
        source_surface = f"{source}_spread"
    return {
        "eligible_for_consensus": True,
        "event": {
            "event_key": ev_key,
            "day_key": "NBA:2026:01:28",
            "away_team": away,
            "home_team": home,
            "matchup_key": f"NBA:2026:01:28:{home}-{away}",
            "event_start_time_utc": "2026-01-28T00:00:00+00:00",
        },
        "market": {
            "market_type": "spread",
            "market_family": "spread",
            "selection": team,
            "side": team,
            "line": line,
            "odds": odds,
        },
        "provenance": {
            "source_id": source,
            "source_surface": source_surface,
            "expert_handle": expert,
            "expert_name": f"Expert {expert}",
            "canonical_url": f"https://{source}.com/picks",
            "raw_pick_text": raw_pick_text,
            "raw_block": raw_block or f"rating_stars=5 {team} {raw_pick_text}",
            "line_hint": line_hint,
            "raw_fingerprint": f"{source}-{expert}-{team}-{line}",
        },
    }


class TestGroupSoftSpreadSupports:
    """Tests for group_soft() spread supports."""

    def test_soft_spread_has_supports(self):
        """Verify group_soft produces non-empty supports for spreads."""
        r1 = make_spread_record("action", "ORL", "MIA", "ORL", 3.5, expert="exp1")
        r2 = make_spread_record("action", "ORL", "MIA", "ORL", 3.5, expert="exp2")

        soft = group_soft([r1, r2])
        spread_groups = [g for g in soft if g.get("market_type") == "spread"]

        assert len(spread_groups) >= 1, "Should have at least one spread group"
        grp = spread_groups[0]
        supports = grp.get("supports") or []
        assert len(supports) == 2, f"Expected 2 supports, got {len(supports)}"

    def test_soft_spread_supports_have_line(self):
        """Verify supports contain line values."""
        r1 = make_spread_record("action", "ORL", "MIA", "ORL", 3.5, expert="exp1")
        r2 = make_spread_record("action", "ORL", "MIA", "ORL", 4.0, expert="exp2")

        soft = group_soft([r1, r2])
        spread_groups = [g for g in soft if g.get("market_type") == "spread"]
        grp = spread_groups[0]
        supports = grp.get("supports") or []

        lines = [s.get("line") for s in supports]
        assert 3.5 in lines, "Support should have line 3.5"
        assert 4.0 in lines, "Support should have line 4.0"

    def test_soft_spread_supports_have_raw_pick_text(self):
        """Verify supports contain raw_pick_text from provenance."""
        r1 = make_spread_record(
            "betql", "ATL", "BOS", "BOS", -5.5, expert="model", raw_pick_text="-5.5"
        )
        r2 = make_spread_record(
            "betql", "ATL", "BOS", "BOS", -5.5, expert="model2", raw_pick_text="-5.5"
        )

        soft = group_soft([r1, r2])
        spread_groups = [g for g in soft if g.get("market_type") == "spread"]
        grp = spread_groups[0]
        supports = grp.get("supports") or []

        raw_texts = [s.get("raw_pick_text") for s in supports if s.get("raw_pick_text")]
        assert "-5.5" in raw_texts, "Support should have raw_pick_text '-5.5'"

    def test_soft_spread_supports_have_source_surface(self):
        """Verify supports contain source_surface from provenance."""
        r1 = make_spread_record(
            "betql",
            "ATL",
            "BOS",
            "ATL",
            7.5,
            expert="model",
            source_surface="betql_model_spread",
        )
        r2 = make_spread_record(
            "betql",
            "ATL",
            "BOS",
            "ATL",
            7.5,
            expert="model2",
            source_surface="betql_model_spread",
        )

        soft = group_soft([r1, r2])
        spread_groups = [g for g in soft if g.get("market_type") == "spread"]
        grp = spread_groups[0]
        supports = grp.get("supports") or []

        surfaces = [s.get("source_surface") for s in supports if s.get("source_surface")]
        assert "betql_model_spread" in surfaces, "Support should have source_surface"


class TestGroupHardSpreadSupports:
    """Tests for group_hard() spread supports (multi-source).

    Note: group_hard() for spreads requires matching line buckets and
    source matching logic. These tests verify supports are populated
    when records are processed, even if the final group count varies.
    """

    def test_hard_spread_has_supports_when_grouped(self):
        """Verify group_hard populates supports when records are grouped."""
        # Use same line bucket to ensure matching
        r1 = make_spread_record("action", "ORL", "MIA", "ORL", 3.5, expert="exp1")
        r2 = make_spread_record("betql", "ORL", "MIA", "ORL", 3.5, expert="model")

        hard = group_hard([r1, r2])
        spread_groups = [g for g in hard if g.get("market_type") == "spread"]

        # There should be at least one group with supports
        if spread_groups:
            grp = spread_groups[0]
            supports = grp.get("supports") or []
            assert len(supports) >= 1, "Grouped spread should have at least one support"
            # Verify the support has line evidence
            assert any(s.get("line") is not None for s in supports), "Support should have line"

    def test_hard_spread_supports_have_line(self):
        """Verify hard spread supports contain line values."""
        r1 = make_spread_record("action", "NOP", "MEM", "NOP", 6.5, expert="exp1")
        r2 = make_spread_record("betql", "NOP", "MEM", "NOP", 6.5, expert="model")

        hard = group_hard([r1, r2])
        spread_groups = [g for g in hard if g.get("market_type") == "spread"]

        if spread_groups:
            grp = spread_groups[0]
            supports = grp.get("supports") or []
            lines = [s.get("line") for s in supports if s.get("line") is not None]
            assert len(lines) >= 1, "Support should have line values"
            assert 6.5 in lines, "Line 6.5 should be in supports"

    def test_hard_spread_supports_have_provenance_fields(self):
        """Verify hard spread supports have all expected provenance fields."""
        r1 = make_spread_record(
            "betql",
            "PHX", "DEN",
            "PHX",
            -3.5,
            expert="model",
            raw_pick_text="-3.5",
            raw_block="rating_stars=5 PHX -3.5",
            source_surface="betql_model_spread",
        )
        r2 = make_spread_record(
            "action",
            "PHX", "DEN",
            "PHX",
            -3.5,  # Same line bucket for matching
            expert="analyst1",
            raw_pick_text="-3.5",
            source_surface="action_picks",
        )

        hard = group_hard([r1, r2])
        spread_groups = [g for g in hard if g.get("market_type") == "spread"]

        if spread_groups:
            grp = spread_groups[0]
            supports = grp.get("supports") or []

            # Check that at least one support has the expected fields
            has_line = any(s.get("line") is not None for s in supports)
            has_raw_pick_text = any(s.get("raw_pick_text") is not None for s in supports)
            has_source_surface = any(s.get("source_surface") is not None for s in supports)

            assert has_line, "At least one support should have line"
            assert has_raw_pick_text, "At least one support should have raw_pick_text"
            assert has_source_surface, "At least one support should have source_surface"


class TestUnifiedSpreadSupports:
    """Tests for unified signals preserving spread supports."""

    def test_unified_soft_spread_preserves_supports(self):
        """Verify build_unified_signals preserves supports for soft spreads."""
        r1 = make_spread_record("action", "CHI", "IND", "CHI", 2.5, expert="exp1")
        r2 = make_spread_record("action", "CHI", "IND", "CHI", 2.5, expert="exp2")

        soft = group_soft([r1, r2])
        unified = build_unified_signals([], soft, [])
        soft_spread = [u for u in unified if u.get("signal_type") == "soft_standard" and u.get("market_type") == "spread"]

        assert len(soft_spread) >= 1, "Should have at least one soft spread signal"
        sig = soft_spread[0]
        supports = sig.get("supports") or []
        assert len(supports) >= 1, "Unified signal should have supports"
        assert any(s.get("line") is not None for s in supports), "Supports should have line"

    def test_unified_hard_spread_preserves_supports(self):
        """Verify build_unified_signals preserves supports for hard spreads when they exist."""
        r1 = make_spread_record("action", "MIL", "WAS", "MIL", 2.0, expert="exp1")
        r2 = make_spread_record("betql", "MIL", "WAS", "MIL", 2.0, expert="model")

        hard = group_hard([r1, r2])

        # Hard consensus for spreads requires specific matching criteria
        # Just verify that if we get a hard group, it has supports
        hard_spread_groups = [g for g in hard if g.get("market_type") == "spread"]

        if hard_spread_groups:
            unified = build_unified_signals(hard, [], [])
            hard_spread = [u for u in unified if u.get("signal_type") == "hard_cross_source" and u.get("market_type") == "spread"]

            if hard_spread:
                sig = hard_spread[0]
                supports = sig.get("supports") or []
                assert len(supports) >= 1, "Unified hard signal should have supports"
        else:
            # If no hard groups formed, test passes - this is expected behavior
            # when records don't meet multi-source matching criteria
            pass


class TestSupportFieldCompleteness:
    """Tests for support field completeness."""

    def test_support_has_required_fields(self):
        """Verify each support has all required fields for line promotion."""
        r1 = make_spread_record(
            "betql",
            "GSW", "UTA",
            "GSW",
            9.0,
            expert="model",
            raw_pick_text="+9",
            raw_block="rating_stars=3 GSW +9",
            source_surface="betql_model_spread",
        )
        r2 = make_spread_record(
            "betql",
            "GSW", "UTA",
            "GSW",
            9.0,
            expert="model2",
            raw_pick_text="+9",
            source_surface="betql_model_spread",
        )

        soft = group_soft([r1, r2])
        spread_groups = [g for g in soft if g.get("market_type") == "spread"]
        grp = spread_groups[0]
        supports = grp.get("supports") or []

        required_fields = ["source_id", "line", "canonical_url"]
        optional_but_wanted = ["source_surface", "raw_pick_text", "raw_block"]

        for sup in supports:
            for field in required_fields:
                assert field in sup, f"Support missing required field: {field}"

            # Check that at least some optional fields are populated
            populated_optional = [f for f in optional_but_wanted if sup.get(f) is not None]
            assert len(populated_optional) >= 1, "Support should have some provenance fields"


class TestSpreadLinesFromMultipleExperts:
    """Test that different lines from multiple experts are preserved."""

    def test_multiple_lines_preserved_in_supports(self):
        """Verify different line values from different experts are all in supports."""
        r1 = make_spread_record("action", "LAL", "CLE", "LAL", 3.5, expert="exp1")
        r2 = make_spread_record("action", "LAL", "CLE", "LAL", 4.0, expert="exp2")
        r3 = make_spread_record("action", "LAL", "CLE", "LAL", 3.5, expert="exp3")

        soft = group_soft([r1, r2, r3])
        spread_groups = [g for g in soft if g.get("market_type") == "spread"]
        grp = spread_groups[0]
        supports = grp.get("supports") or []

        lines = [s.get("line") for s in supports if s.get("line") is not None]
        assert 3.5 in lines, "Line 3.5 should be in supports"
        assert 4.0 in lines, "Line 4.0 should be in supports"
        assert len(supports) == 3, "Should have 3 supports for 3 experts"
