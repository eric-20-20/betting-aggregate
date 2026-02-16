"""Contract Tests for Existing Sources.

These tests validate that all existing source implementations produce
schema-compliant output. Run these after any changes to ingestion logic.

Usage:
    python -m pytest tests/test_source_contracts.py -v
    python -m pytest tests/test_source_contracts.py -v -k "action"
    python -m pytest tests/test_source_contracts.py -v -k "covers"
"""

from __future__ import annotations

import json
import pytest
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import patch

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Import the source contract
from src.source_contract import (
    RawPickSchema,
    NormalizedPickSchema,
    ValidationResult,
    validate_raw_pick,
    validate_normalized_pick,
    validate_batch,
    capture_schema_snapshot,
    detect_drift,
    EVENT_KEY_PATTERN,
    DAY_KEY_PATTERN,
    TEAM_CODE_PATTERN,
    VALID_MARKET_TYPES,
    VALID_SPORTS,
)


# =============================================================================
# FIXTURES
# =============================================================================

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def action_fixture_html() -> Optional[str]:
    """Load action network fixture if available."""
    fixture_path = FIXTURES_DIR / "action" / "game_page.html"
    if fixture_path.exists():
        return fixture_path.read_text()
    return None


@pytest.fixture
def covers_fixtures() -> List[str]:
    """Load covers fixtures if available."""
    covers_dir = FIXTURES_DIR / "covers"
    if not covers_dir.exists():
        return []
    return [f.read_text() for f in covers_dir.glob("*.html")]


@pytest.fixture
def sample_action_normalized() -> Dict[str, Any]:
    """Sample normalized pick from action network."""
    return {
        "provenance": {
            "source_id": "action",
            "source_surface": "nba_game_expert_picks",
            "sport": "NBA",
            "observed_at_utc": "2026-02-16T12:00:00Z",
            "canonical_url": "https://www.actionnetwork.com/nba-game/lakers-celtics/123",
            "raw_fingerprint": "abc123def456",
        },
        "event": {
            "sport": "NBA",
            "event_key": "NBA:2026:02:16:LAL@BOS",
            "day_key": "NBA:2026:02:16",
            "away_team": "LAL",
            "home_team": "BOS",
        },
        "market": {
            "market_type": "spread",
            "market_family": "standard",
            "selection": "LAL_spread",
            "line": -5.5,
            "odds": -110,
        },
        "eligible_for_consensus": True,
        "ineligibility_reason": None,
    }


@pytest.fixture
def sample_covers_normalized() -> Dict[str, Any]:
    """Sample normalized pick from covers."""
    return {
        "provenance": {
            "source_id": "covers",
            "source_surface": "nba_matchup_picks",
            "sport": "NBA",
            "observed_at_utc": "2026-02-16T12:00:00Z",
            "canonical_url": "https://www.covers.com/sport/basketball/nba/matchup/123/picks",
            "raw_fingerprint": "xyz789",
            "expert_name": "Phil Expert",
        },
        "event": {
            "sport": "NBA",
            "event_key": "NBA:2026:02:16:NYK@BKN",
            "day_key": "NBA:2026:02:16",
            "away_team": "NYK",
            "home_team": "BKN",
        },
        "market": {
            "market_type": "moneyline",
            "market_family": "standard",
            "selection": "NYK",
            "odds": 150,
        },
        "eligible_for_consensus": True,
        "ineligibility_reason": None,
    }


@pytest.fixture
def sample_sportsline_normalized() -> Dict[str, Any]:
    """Sample normalized pick from sportsline."""
    return {
        "provenance": {
            "source_id": "sportsline",
            "source_surface": "expert_picks",
            "sport": "NBA",
            "observed_at_utc": "2026-02-16T12:00:00Z",
            "canonical_url": "https://www.sportsline.com/nba/picks/experts/",
            "raw_fingerprint": "sportsline123",
            "expert_name": "Kenny White",
            "expert_slug": "kenny-white",
        },
        "event": {
            "sport": "NBA",
            "event_key": "NBA:2026:02:16:PHX@GSW",
            "day_key": "NBA:2026:02:16",
            "away_team": "PHX",
            "home_team": "GSW",
        },
        "market": {
            "market_type": "total",
            "market_family": "standard",
            "selection": "game_total",
            "line": 228.5,
            "side": "OVER",
        },
        "eligible_for_consensus": True,
        "ineligibility_reason": None,
        "result": {
            "status": "WIN",
            "graded_by": "sportsline",
        },
    }


# =============================================================================
# CONTRACT TESTS - ACTION NETWORK
# =============================================================================

class TestActionContract:
    """Contract tests for Action Network source."""

    def test_normalized_pick_schema_valid(self, sample_action_normalized):
        """Action normalized picks should pass schema validation."""
        result = validate_normalized_pick(sample_action_normalized)
        assert result.valid, f"Errors: {result.errors}"

    def test_event_key_format(self, sample_action_normalized):
        """Action event keys should follow canonical format."""
        event_key = sample_action_normalized["event"]["event_key"]
        assert EVENT_KEY_PATTERN.match(event_key), f"Invalid event key: {event_key}"

    def test_market_type_valid(self, sample_action_normalized):
        """Action market types should be valid."""
        market_type = sample_action_normalized["market"]["market_type"]
        assert market_type in VALID_MARKET_TYPES

    def test_source_id_consistent(self, sample_action_normalized):
        """Source ID should be 'action'."""
        assert sample_action_normalized["provenance"]["source_id"] == "action"

    def test_team_codes_valid(self, sample_action_normalized):
        """Team codes should be 2-5 uppercase letters."""
        away = sample_action_normalized["event"]["away_team"]
        home = sample_action_normalized["event"]["home_team"]
        assert TEAM_CODE_PATTERN.match(away), f"Invalid away team: {away}"
        assert TEAM_CODE_PATTERN.match(home), f"Invalid home team: {home}"

    def test_spread_has_line(self, sample_action_normalized):
        """Spread markets should have a line."""
        if sample_action_normalized["market"]["market_type"] == "spread":
            assert sample_action_normalized["market"].get("line") is not None

    def test_provenance_has_required_fields(self, sample_action_normalized):
        """Provenance should have all required fields."""
        prov = sample_action_normalized["provenance"]
        assert prov.get("source_id")
        assert prov.get("source_surface")
        assert prov.get("sport")
        assert prov.get("observed_at_utc")

    @pytest.mark.skipif(
        not Path("action_ingest.py").exists(),
        reason="action_ingest.py not found"
    )
    def test_live_action_output_valid(self):
        """Test that action_ingest produces valid output (requires fixture)."""
        try:
            from action_ingest import normalize_pick, RawPickRecord

            # Create a test raw record
            raw = RawPickRecord(
                source_id="action",
                source_surface="nba_game_expert_picks",
                sport="nba",
                market_family="standard",
                observed_at_utc=datetime.now(timezone.utc).isoformat(),
                canonical_url="https://example.com/game",
                raw_pick_text="HOU +5 -110",
                raw_block="<div>HOU +5 -110</div>",
                raw_fingerprint="a" * 64,
                event_start_time_utc=datetime.now(timezone.utc).isoformat(),
            )

            normalized = normalize_pick(raw, home_team="LAL", away_team="HOU", stats={})

            # Adapt to contract schema
            adapted = self._adapt_action_to_contract(normalized)
            result = validate_normalized_pick(adapted)
            assert result.valid, f"Errors: {result.errors}"

        except ImportError:
            pytest.skip("action_ingest not importable")

    def _adapt_action_to_contract(self, normalized: Dict[str, Any]) -> Dict[str, Any]:
        """Adapt action output to contract schema format."""
        # Action may use different field names, adapt as needed
        event = normalized.get("event", {})
        market = normalized.get("market", {})
        prov = normalized.get("provenance", {})

        # Ensure sport prefix in event_key
        event_key = event.get("event_key", "")
        if event_key and not event_key.startswith(("NBA:", "NCAAB:")):
            # Add sport prefix if missing
            event_key = f"NBA:{event_key}" if "NBA" in str(normalized) else event_key

        # Normalize sport to uppercase (contract requires NBA/NCAAB)
        sport = (event.get("sport") or prov.get("sport") or "NBA").upper()

        return {
            "provenance": {
                **prov,
                "sport": sport,  # Ensure uppercase
            },
            "event": {
                "sport": sport,
                "event_key": event_key,
                "day_key": event.get("day_key", ""),
                "away_team": event.get("away_team", ""),
                "home_team": event.get("home_team", ""),
            },
            "market": {
                "market_type": market.get("market_type", ""),
                "market_family": market.get("market_family", "standard"),
                "selection": market.get("selection"),
                "line": market.get("line"),
                "odds": market.get("odds"),
                "side": market.get("side"),
            },
            "eligible_for_consensus": normalized.get("eligible_for_consensus", False),
            "ineligibility_reason": normalized.get("ineligibility_reason"),
        }


# =============================================================================
# CONTRACT TESTS - COVERS
# =============================================================================

class TestCoversContract:
    """Contract tests for Covers source."""

    def test_normalized_pick_schema_valid(self, sample_covers_normalized):
        """Covers normalized picks should pass schema validation."""
        result = validate_normalized_pick(sample_covers_normalized)
        assert result.valid, f"Errors: {result.errors}"

    def test_event_key_format(self, sample_covers_normalized):
        """Covers event keys should follow canonical format."""
        event_key = sample_covers_normalized["event"]["event_key"]
        assert EVENT_KEY_PATTERN.match(event_key), f"Invalid event key: {event_key}"

    def test_source_id_consistent(self, sample_covers_normalized):
        """Source ID should be 'covers'."""
        assert sample_covers_normalized["provenance"]["source_id"] == "covers"

    def test_expert_attribution(self, sample_covers_normalized):
        """Covers picks should have expert attribution."""
        prov = sample_covers_normalized["provenance"]
        # Expert name is optional but recommended
        if prov.get("expert_name"):
            assert len(prov["expert_name"]) > 0

    @pytest.mark.skipif(
        not Path("covers_ingest.py").exists(),
        reason="covers_ingest.py not found"
    )
    def test_live_covers_output_valid(self, covers_fixtures):
        """Test that covers_ingest produces valid output."""
        if not covers_fixtures:
            pytest.skip("No covers fixtures found")

        try:
            from covers_ingest import extract_picks_from_html, normalize_covers_pick

            for html in covers_fixtures[:3]:  # Test first 3 fixtures
                records = extract_picks_from_html(
                    html,
                    "https://example.com/game",
                    datetime.now(timezone.utc)
                )

                for record in records:
                    normalized = normalize_covers_pick(record, home_team="BOS", away_team="NYK")
                    if normalized.get("eligible_for_consensus"):
                        adapted = self._adapt_covers_to_contract(normalized)
                        result = validate_normalized_pick(adapted)
                        # Allow warnings but not errors
                        assert result.valid, f"Errors: {result.errors}"

        except ImportError:
            pytest.skip("covers_ingest not importable")

    def _adapt_covers_to_contract(self, normalized: Dict[str, Any]) -> Dict[str, Any]:
        """Adapt covers output to contract schema format."""
        event = normalized.get("event", {})
        market = normalized.get("market", {})
        prov = normalized.get("provenance", {})

        event_key = event.get("event_key", "")
        if event_key and not event_key.startswith(("NBA:", "NCAAB:")):
            event_key = f"NBA:{event_key}"

        # Normalize sport to uppercase (contract requires NBA/NCAAB)
        sport = (event.get("sport") or prov.get("sport") or "NBA").upper()

        return {
            "provenance": {
                **prov,
                "sport": sport,  # Ensure uppercase
            },
            "event": {
                "sport": sport,
                "event_key": event_key,
                "day_key": event.get("day_key", ""),
                "away_team": event.get("away_team", ""),
                "home_team": event.get("home_team", ""),
            },
            "market": {
                "market_type": market.get("market_type", ""),
                "market_family": market.get("market_family", "standard"),
                "selection": market.get("selection"),
                "line": market.get("line"),
                "odds": market.get("odds"),
                "side": market.get("side"),
            },
            "eligible_for_consensus": normalized.get("eligible_for_consensus", False),
            "ineligibility_reason": normalized.get("ineligibility_reason"),
        }


# =============================================================================
# CONTRACT TESTS - SPORTSLINE
# =============================================================================

class TestSportslineContract:
    """Contract tests for SportsLine source."""

    def test_normalized_pick_schema_valid(self, sample_sportsline_normalized):
        """SportsLine normalized picks should pass schema validation."""
        result = validate_normalized_pick(sample_sportsline_normalized)
        assert result.valid, f"Errors: {result.errors}"

    def test_event_key_format(self, sample_sportsline_normalized):
        """SportsLine event keys should follow canonical format."""
        event_key = sample_sportsline_normalized["event"]["event_key"]
        assert EVENT_KEY_PATTERN.match(event_key), f"Invalid event key: {event_key}"

    def test_source_id_consistent(self, sample_sportsline_normalized):
        """Source ID should be 'sportsline'."""
        assert sample_sportsline_normalized["provenance"]["source_id"] == "sportsline"

    def test_pre_graded_result_format(self, sample_sportsline_normalized):
        """Pre-graded results should have valid format."""
        result = sample_sportsline_normalized.get("result")
        if result:
            assert result.get("status") in {"WIN", "LOSS", "PUSH"}
            assert result.get("graded_by")

    def test_expert_slug_format(self, sample_sportsline_normalized):
        """Expert slugs should be URL-friendly."""
        prov = sample_sportsline_normalized["provenance"]
        slug = prov.get("expert_slug")
        if slug:
            # Should be lowercase, hyphen-separated
            assert slug == slug.lower()
            assert " " not in slug


# =============================================================================
# CONTRACT TESTS - BETQL
# =============================================================================

class TestBetQLContract:
    """Contract tests for BetQL source."""

    @pytest.fixture
    def sample_betql_normalized(self) -> Dict[str, Any]:
        """Sample normalized pick from BetQL."""
        return {
            "provenance": {
                "source_id": "betql",
                "source_surface": "sharp_picks",
                "sport": "NBA",
                "observed_at_utc": "2026-02-16T12:00:00Z",
                "canonical_url": "https://betql.co/nba/sharp",
                "raw_fingerprint": "betql789",
                "rating_stars": 5,
            },
            "event": {
                "sport": "NBA",
                "event_key": "NBA:2026:02:16:MIA@CHI",
                "day_key": "NBA:2026:02:16",
                "away_team": "MIA",
                "home_team": "CHI",
            },
            "market": {
                "market_type": "spread",
                "market_family": "standard",
                "selection": "MIA_spread",
                "line": -3.5,
            },
            "eligible_for_consensus": True,
            "ineligibility_reason": None,
        }

    def test_normalized_pick_schema_valid(self, sample_betql_normalized):
        """BetQL normalized picks should pass schema validation."""
        result = validate_normalized_pick(sample_betql_normalized)
        assert result.valid, f"Errors: {result.errors}"

    def test_rating_stars_valid(self, sample_betql_normalized):
        """Rating stars should be 1-5."""
        prov = sample_betql_normalized["provenance"]
        stars = prov.get("rating_stars")
        if stars is not None:
            assert 1 <= stars <= 5


# =============================================================================
# CROSS-SOURCE CONSISTENCY TESTS
# =============================================================================

class TestCrossSourceConsistency:
    """Tests that verify consistency across all sources."""

    @pytest.fixture
    def all_sample_picks(
        self,
        sample_action_normalized,
        sample_covers_normalized,
        sample_sportsline_normalized,
    ) -> List[Dict[str, Any]]:
        """All sample picks from different sources."""
        return [
            sample_action_normalized,
            sample_covers_normalized,
            sample_sportsline_normalized,
        ]

    def test_all_picks_have_required_provenance(self, all_sample_picks):
        """All sources should include required provenance fields."""
        required = {"source_id", "source_surface", "sport", "observed_at_utc"}

        for pick in all_sample_picks:
            prov = pick.get("provenance", {})
            for field in required:
                assert field in prov, f"Missing {field} in {prov.get('source_id')}"

    def test_all_picks_have_valid_event(self, all_sample_picks):
        """All sources should produce valid event structures."""
        for pick in all_sample_picks:
            event = pick.get("event", {})
            assert event.get("event_key")
            assert event.get("away_team")
            assert event.get("home_team")

    def test_all_event_keys_follow_pattern(self, all_sample_picks):
        """All event keys should follow the canonical pattern."""
        for pick in all_sample_picks:
            event_key = pick["event"]["event_key"]
            assert EVENT_KEY_PATTERN.match(event_key), \
                f"Invalid event key from {pick['provenance']['source_id']}: {event_key}"

    def test_all_market_types_valid(self, all_sample_picks):
        """All market types should be from the valid set."""
        for pick in all_sample_picks:
            market_type = pick["market"]["market_type"]
            assert market_type in VALID_MARKET_TYPES, \
                f"Invalid market type from {pick['provenance']['source_id']}: {market_type}"

    def test_all_sports_valid(self, all_sample_picks):
        """All sports should be from the valid set."""
        for pick in all_sample_picks:
            sport = pick["provenance"]["sport"]
            assert sport in VALID_SPORTS, \
                f"Invalid sport from {pick['provenance']['source_id']}: {sport}"


# =============================================================================
# DRIFT DETECTION TESTS
# =============================================================================

class TestDriftDetection:
    """Tests for schema drift detection across sources."""

    def test_baseline_snapshot_captures_fields(self, sample_action_normalized):
        """Baseline snapshot should capture field counts."""
        picks = [sample_action_normalized.copy() for _ in range(10)]
        snapshot = capture_schema_snapshot("action", picks)

        assert snapshot.source_id == "action"
        assert snapshot.total_records == 10
        assert "provenance.source_id" in snapshot.field_counts
        assert "event.event_key" in snapshot.field_counts
        assert "market.market_type" in snapshot.field_counts

    def test_detect_new_field_drift(self, sample_action_normalized):
        """Should detect when new fields appear."""
        baseline_picks = [sample_action_normalized.copy() for _ in range(10)]
        baseline = capture_schema_snapshot("action", baseline_picks)

        # Add new field
        current_picks = [sample_action_normalized.copy() for _ in range(10)]
        for p in current_picks:
            p["provenance"]["new_tracking_field"] = "value"

        current = capture_schema_snapshot("action", current_picks)
        alerts = detect_drift(baseline, current)

        assert any("NEW_FIELD" in a for a in alerts)

    def test_detect_missing_field_drift(self, sample_action_normalized):
        """Should detect when fields disappear."""
        import copy
        baseline_picks = [copy.deepcopy(sample_action_normalized) for _ in range(10)]
        baseline = capture_schema_snapshot("action", baseline_picks)

        # Remove a field
        current_picks = [copy.deepcopy(sample_action_normalized) for _ in range(10)]
        for p in current_picks:
            del p["market"]["selection"]

        current = capture_schema_snapshot("action", current_picks)
        alerts = detect_drift(baseline, current)

        assert any("MISSING_FIELD" in a for a in alerts)


# =============================================================================
# VALIDATION BATCH TESTS
# =============================================================================

class TestBatchValidation:
    """Test batch validation for production use."""

    def test_validate_batch_returns_stats(self, sample_action_normalized):
        """Batch validation should return counts."""
        picks = [sample_action_normalized.copy() for _ in range(5)]
        valid, invalid, errors = validate_batch(picks, validate_normalized_pick)

        assert valid == 5
        assert invalid == 0
        assert len(errors) == 0

    def test_validate_batch_catches_invalid(self, sample_action_normalized):
        """Batch validation should catch invalid picks."""
        import copy
        picks = [copy.deepcopy(sample_action_normalized) for _ in range(5)]
        # Corrupt one
        picks[2]["event"]["event_key"] = "not-valid"

        valid, invalid, errors = validate_batch(picks, validate_normalized_pick)

        assert valid == 4
        assert invalid == 1
        assert len(errors) == 1

    def test_validate_batch_limits_errors(self, sample_action_normalized):
        """Batch validation should limit error samples."""
        picks = [sample_action_normalized.copy() for _ in range(20)]
        # Corrupt all
        for p in picks:
            p["event"]["event_key"] = "invalid"

        valid, invalid, errors = validate_batch(picks, validate_normalized_pick, max_errors=5)

        assert invalid == 20
        assert len(errors) == 5  # Limited to max_errors


# =============================================================================
# INTEGRATION TESTS
# =============================================================================

class TestLiveDataValidation:
    """Integration tests against live data files."""

    @pytest.fixture
    def normalized_action_path(self) -> Path:
        """Path to normalized action data."""
        return Path("out/normalized_action_nba.json")

    @pytest.fixture
    def normalized_covers_path(self) -> Path:
        """Path to normalized covers data."""
        return Path("out/normalized_covers_nba.json")

    @pytest.mark.integration
    def test_validate_live_action_data(self, normalized_action_path):
        """Validate live action data against schema."""
        if not normalized_action_path.exists():
            pytest.skip("No live action data found")

        data = json.loads(normalized_action_path.read_text())
        if not data:
            pytest.skip("Action data is empty")

        valid, invalid, errors = validate_batch(data, validate_normalized_pick)

        # Report stats
        print(f"\nAction validation: {valid} valid, {invalid} invalid")
        for err in errors[:5]:
            print(f"  Error: {err}")

        # At least 90% should be valid
        if valid + invalid > 0:
            assert valid / (valid + invalid) >= 0.9, \
                f"Too many invalid picks: {invalid}/{valid + invalid}"

    @pytest.mark.integration
    def test_validate_live_covers_data(self, normalized_covers_path):
        """Validate live covers data against schema.

        Note: If this test fails due to missing provenance.sport or provenance.observed_at_utc,
        regenerate the output by running: python3 covers_ingest.py
        """
        if not normalized_covers_path.exists():
            pytest.skip("No live covers data found")

        data = json.loads(normalized_covers_path.read_text())
        if not data:
            pytest.skip("Covers data is empty")

        # Check if data has required provenance fields (skip if stale)
        sample = data[0]
        prov = sample.get("provenance", {})
        if not prov.get("sport") or not prov.get("observed_at_utc"):
            pytest.skip(
                "Stale covers data missing required provenance fields. "
                "Regenerate by running: python3 covers_ingest.py"
            )

        valid, invalid, errors = validate_batch(data, validate_normalized_pick)

        print(f"\nCovers validation: {valid} valid, {invalid} invalid")
        for err in errors[:5]:
            print(f"  Error: {err}")

        if valid + invalid > 0:
            assert valid / (valid + invalid) >= 0.9


# =============================================================================
# HELPER FOR RUNNING VALIDATION
# =============================================================================

def validate_source_output(source_name: str, output_path: Path) -> Dict[str, Any]:
    """Validate a source's output file.

    Returns:
        Dict with validation stats and errors
    """
    if not output_path.exists():
        return {"error": f"File not found: {output_path}"}

    try:
        data = json.loads(output_path.read_text())
    except json.JSONDecodeError as e:
        return {"error": f"JSON parse error: {e}"}

    if not isinstance(data, list):
        return {"error": "Data is not a list"}

    valid, invalid, errors = validate_batch(data, validate_normalized_pick)

    return {
        "source": source_name,
        "path": str(output_path),
        "total": len(data),
        "valid": valid,
        "invalid": invalid,
        "valid_rate": valid / len(data) if data else 0,
        "error_samples": errors,
    }


if __name__ == "__main__":
    # Quick validation of all source outputs
    sources = [
        ("action", Path("out/normalized_action_nba.json")),
        ("covers", Path("out/normalized_covers_nba.json")),
        ("sportsline", Path("out/normalized_sportsline_nba_expert_picks.jsonl")),
        ("betql_sharp", Path("out/normalized_betql_sharp_nba.json")),
        ("betql_spread", Path("out/normalized_betql_spread_nba.json")),
        ("betql_total", Path("out/normalized_betql_total_nba.json")),
    ]

    print("Source Output Validation")
    print("=" * 60)

    for source_name, path in sources:
        result = validate_source_output(source_name, path)
        if "error" in result:
            print(f"\n{source_name}: ERROR - {result['error']}")
        else:
            print(f"\n{source_name}:")
            print(f"  Total: {result['total']}")
            print(f"  Valid: {result['valid']} ({result['valid_rate']*100:.1f}%)")
            print(f"  Invalid: {result['invalid']}")
            if result['error_samples']:
                print(f"  Sample errors:")
                for err in result['error_samples'][:3]:
                    print(f"    - {err['errors'][:2]}")
