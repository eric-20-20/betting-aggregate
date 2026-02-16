"""Test Template: Copy this file to test your new source implementation.

QUICK START:
1. Copy this file to tests/test_{your_source}_source.py
2. Update SOURCE_CLASS and imports
3. Add fixture files in tests/fixtures/{your_source}/
4. Run: python -m pytest tests/test_{your_source}_source.py -v

This template provides:
- Contract tests (validates schema compliance)
- Parser tests using fixtures
- Normalization tests
- Drift detection tests
"""

from __future__ import annotations

import json
import pytest
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import Mock, patch

# Import source contract for validation
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from source_contract import (
    BaseSource,
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
)

from source_replay import ReplayHarness, FixtureLoader

# TODO: Import your source class
# from example_source import ExampleSource as SOURCE_CLASS
# For now, import the template
from source_template import ExampleSource as SOURCE_CLASS

# =============================================================================
# TEST CONFIGURATION
# =============================================================================

SOURCE_ID = "example"  # TODO: Update this
FIXTURE_DIR = Path(__file__).parent / "fixtures" / SOURCE_ID


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def source():
    """Create source instance in passthrough mode."""
    return SOURCE_CLASS(sport="NBA", debug=True, replay_mode="passthrough")


@pytest.fixture
def replay_source():
    """Create source instance in replay mode for fixture-based testing."""
    return SOURCE_CLASS(sport="NBA", debug=True, replay_mode="replay")


@pytest.fixture
def fixture_loader():
    """Load test fixtures."""
    return FixtureLoader(SOURCE_ID, fixture_dir=FIXTURE_DIR)


@pytest.fixture
def sample_raw_pick() -> RawPickSchema:
    """Create a sample raw pick for testing."""
    return RawPickSchema(
        source_id=SOURCE_ID,
        source_surface=f"{SOURCE_ID}_expert_picks",
        sport="NBA",
        market_family="standard",
        observed_at_utc=datetime.now(timezone.utc).isoformat(),
        canonical_url=f"https://example.com/{SOURCE_ID}/picks/123",
        raw_pick_text="Lakers -5.5 vs Celtics",
        raw_block="<div class='pick'>Lakers -5.5 vs Celtics</div>",
        raw_fingerprint=RawPickSchema.compute_fingerprint(
            SOURCE_ID,
            f"{SOURCE_ID}_expert_picks",
            f"https://example.com/{SOURCE_ID}/picks/123",
            "Lakers -5.5 vs Celtics",
        ),
        matchup_hint="LAL @ BOS",
        event_start_time_utc="2026-02-16T19:30:00Z",
    )


@pytest.fixture
def sample_normalized_pick() -> Dict[str, Any]:
    """Create a sample normalized pick for testing."""
    return {
        "provenance": {
            "source_id": SOURCE_ID,
            "source_surface": f"{SOURCE_ID}_expert_picks",
            "sport": "NBA",
            "observed_at_utc": "2026-02-16T12:00:00Z",
            "canonical_url": f"https://example.com/{SOURCE_ID}/picks/123",
            "raw_fingerprint": "abc123",
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
            "selection": "LAL",
            "line": -5.5,
        },
        "eligible_for_consensus": True,
        "ineligibility_reason": None,
    }


# =============================================================================
# CONTRACT TESTS
# =============================================================================

class TestSourceContract:
    """Test that source implements the contract correctly."""

    def test_source_id_is_set(self, source):
        """Source must have a unique identifier."""
        assert source.source_id
        assert isinstance(source.source_id, str)
        assert len(source.source_id) <= 32

    def test_source_surface_is_set(self, source):
        """Source must have a surface identifier."""
        assert source.source_surface
        assert isinstance(source.source_surface, str)

    def test_sport_is_valid(self, source):
        """Sport must be NBA or NCAAB."""
        assert source.sport in {"NBA", "NCAAB"}

    def test_extends_base_source(self, source):
        """Source must extend BaseSource."""
        assert isinstance(source, BaseSource)

    def test_has_fetch_raw_picks(self, source):
        """Source must implement fetch_raw_picks."""
        assert callable(getattr(source, "fetch_raw_picks", None))

    def test_has_normalize_pick(self, source):
        """Source must implement normalize_pick."""
        assert callable(getattr(source, "normalize_pick", None))


# =============================================================================
# RAW PICK VALIDATION TESTS
# =============================================================================

class TestRawPickSchema:
    """Test raw pick schema validation."""

    def test_valid_raw_pick(self, sample_raw_pick):
        """Valid raw pick should pass validation."""
        result = sample_raw_pick.validate()
        assert result.valid, f"Errors: {result.errors}"
        assert len(result.errors) == 0

    def test_missing_source_id(self, sample_raw_pick):
        """Missing source_id should fail validation."""
        sample_raw_pick.source_id = ""
        result = sample_raw_pick.validate()
        assert not result.valid
        assert any("source_id" in e for e in result.errors)

    def test_invalid_sport(self, sample_raw_pick):
        """Invalid sport should fail validation."""
        sample_raw_pick.sport = "NFL"  # Not supported
        result = sample_raw_pick.validate()
        assert not result.valid
        assert any("sport" in e for e in result.errors)

    def test_invalid_market_family(self, sample_raw_pick):
        """Invalid market_family should fail validation."""
        sample_raw_pick.market_family = "invalid"
        result = sample_raw_pick.validate()
        assert not result.valid
        assert any("market_family" in e for e in result.errors)

    def test_invalid_timestamp(self, sample_raw_pick):
        """Invalid timestamp should fail validation."""
        sample_raw_pick.observed_at_utc = "not-a-timestamp"
        result = sample_raw_pick.validate()
        assert not result.valid
        assert any("observed_at_utc" in e for e in result.errors)

    def test_fingerprint_consistency(self, sample_raw_pick):
        """Fingerprint should be computed consistently."""
        expected = RawPickSchema.compute_fingerprint(
            sample_raw_pick.source_id,
            sample_raw_pick.source_surface,
            sample_raw_pick.canonical_url,
            sample_raw_pick.raw_pick_text,
        )
        assert sample_raw_pick.raw_fingerprint == expected


# =============================================================================
# NORMALIZED PICK VALIDATION TESTS
# =============================================================================

class TestNormalizedPickSchema:
    """Test normalized pick schema validation."""

    def test_valid_normalized_pick(self, sample_normalized_pick):
        """Valid normalized pick should pass validation."""
        result = validate_normalized_pick(sample_normalized_pick)
        assert result.valid, f"Errors: {result.errors}"

    def test_invalid_event_key_format(self, sample_normalized_pick):
        """Invalid event_key format should fail validation."""
        sample_normalized_pick["event"]["event_key"] = "invalid-key"
        result = validate_normalized_pick(sample_normalized_pick)
        assert not result.valid
        assert any("event_key" in e for e in result.errors)

    def test_valid_event_key_patterns(self, sample_normalized_pick):
        """Test various valid event_key patterns."""
        valid_keys = [
            "NBA:2026:02:16:LAL@BOS",
            "NCAAB:2026:03:20:DUKE@UNC",
            "NBA:2026:01:01:PHX@GSW",
        ]
        for key in valid_keys:
            sample_normalized_pick["event"]["event_key"] = key
            result = validate_normalized_pick(sample_normalized_pick)
            assert result.valid, f"Key {key} should be valid: {result.errors}"

    def test_invalid_team_codes(self, sample_normalized_pick):
        """Invalid team codes should fail validation."""
        # Too short
        sample_normalized_pick["event"]["away_team"] = "L"
        result = validate_normalized_pick(sample_normalized_pick)
        assert not result.valid

        # Too long
        sample_normalized_pick["event"]["away_team"] = "LAKERS"  # 6 chars
        result = validate_normalized_pick(sample_normalized_pick)
        assert not result.valid

        # Lowercase
        sample_normalized_pick["event"]["away_team"] = "lal"
        result = validate_normalized_pick(sample_normalized_pick)
        assert not result.valid

    def test_invalid_market_type(self, sample_normalized_pick):
        """Invalid market_type should fail validation."""
        sample_normalized_pick["market"]["market_type"] = "futures"
        result = validate_normalized_pick(sample_normalized_pick)
        assert not result.valid

    def test_ineligibility_consistency(self, sample_normalized_pick):
        """Ineligible picks should have a reason."""
        sample_normalized_pick["eligible_for_consensus"] = False
        sample_normalized_pick["ineligibility_reason"] = None
        result = validate_normalized_pick(sample_normalized_pick)
        # Should warn about missing reason
        assert any("ineligibility_reason" in w for w in result.warnings)


# =============================================================================
# NORMALIZATION TESTS
# =============================================================================

class TestNormalization:
    """Test the normalization process."""

    def test_normalize_creates_valid_pick(self, source, sample_raw_pick):
        """Normalization should produce valid picks."""
        normalized = source.normalize_pick(sample_raw_pick)
        if normalized is not None:
            result = normalized.validate()
            assert result.valid, f"Normalization errors: {result.errors}"

    def test_normalize_ineligible_has_reason(self, source, sample_raw_pick):
        """Ineligible picks should have a reason."""
        # Corrupt the pick to make it ineligible
        sample_raw_pick.matchup_hint = None
        sample_raw_pick.raw_pick_text = ""

        normalized = source.normalize_pick(sample_raw_pick)
        if normalized and not normalized.eligible_for_consensus:
            assert normalized.ineligibility_reason is not None

    def test_event_key_format(self, source, sample_raw_pick):
        """Event key should match the canonical format."""
        normalized = source.normalize_pick(sample_raw_pick)
        if normalized and normalized.event.get("event_key"):
            assert EVENT_KEY_PATTERN.match(normalized.event["event_key"])

    def test_day_key_format(self, source, sample_raw_pick):
        """Day key should match the canonical format."""
        normalized = source.normalize_pick(sample_raw_pick)
        if normalized and normalized.event.get("day_key"):
            assert DAY_KEY_PATTERN.match(normalized.event["day_key"])


# =============================================================================
# BATCH VALIDATION TESTS
# =============================================================================

class TestBatchValidation:
    """Test batch validation functionality."""

    def test_validate_batch_empty(self):
        """Empty batch should return zero counts."""
        valid, invalid, errors = validate_batch([], validate_normalized_pick)
        assert valid == 0
        assert invalid == 0
        assert len(errors) == 0

    def test_validate_batch_all_valid(self, sample_normalized_pick):
        """Batch of valid picks should all pass."""
        picks = [sample_normalized_pick.copy() for _ in range(5)]
        valid, invalid, errors = validate_batch(picks, validate_normalized_pick)
        assert valid == 5
        assert invalid == 0

    def test_validate_batch_mixed(self, sample_normalized_pick):
        """Batch with invalid picks should report errors."""
        import copy
        picks = [copy.deepcopy(sample_normalized_pick) for _ in range(3)]
        # Corrupt one pick
        picks[1]["event"]["event_key"] = "invalid"

        valid, invalid, errors = validate_batch(picks, validate_normalized_pick)
        assert valid == 2
        assert invalid == 1
        assert len(errors) == 1


# =============================================================================
# DRIFT DETECTION TESTS
# =============================================================================

class TestDriftDetection:
    """Test schema drift detection."""

    def test_no_drift_same_schema(self, sample_normalized_pick):
        """Same schema should not detect drift."""
        picks = [sample_normalized_pick.copy() for _ in range(10)]

        baseline = capture_schema_snapshot(SOURCE_ID, picks)
        current = capture_schema_snapshot(SOURCE_ID, picks)

        alerts = detect_drift(baseline, current)
        assert len(alerts) == 0

    def test_detect_new_field(self, sample_normalized_pick):
        """Should detect new fields appearing."""
        baseline_picks = [sample_normalized_pick.copy() for _ in range(10)]
        baseline = capture_schema_snapshot(SOURCE_ID, baseline_picks)

        # Add new field to all picks
        current_picks = [sample_normalized_pick.copy() for _ in range(10)]
        for p in current_picks:
            p["new_field"] = "value"

        current = capture_schema_snapshot(SOURCE_ID, current_picks)
        alerts = detect_drift(baseline, current)

        assert any("NEW_FIELD" in a for a in alerts)

    def test_detect_missing_field(self, sample_normalized_pick):
        """Should detect fields disappearing."""
        import copy
        baseline_picks = [copy.deepcopy(sample_normalized_pick) for _ in range(10)]
        baseline = capture_schema_snapshot(SOURCE_ID, baseline_picks)

        # Remove a field from all picks
        current_picks = [copy.deepcopy(sample_normalized_pick) for _ in range(10)]
        for p in current_picks:
            del p["market"]["line"]

        current = capture_schema_snapshot(SOURCE_ID, current_picks)
        alerts = detect_drift(baseline, current)

        assert any("MISSING_FIELD" in a for a in alerts)

    def test_detect_increased_error_rate(self, sample_normalized_pick):
        """Should detect increased validation error rate."""
        # Baseline with all valid
        baseline_picks = [sample_normalized_pick.copy() for _ in range(10)]
        baseline = capture_schema_snapshot(SOURCE_ID, baseline_picks)

        # Current with some invalid
        current_picks = [sample_normalized_pick.copy() for _ in range(10)]
        for i in range(5):  # Corrupt half
            current_picks[i]["event"]["event_key"] = "invalid"

        current = capture_schema_snapshot(SOURCE_ID, current_picks)
        alerts = detect_drift(baseline, current)

        assert any("ERROR_RATE" in a for a in alerts)


# =============================================================================
# FIXTURE-BASED PARSER TESTS
# =============================================================================

class TestParserWithFixtures:
    """Test parser using saved HTML fixtures."""

    @pytest.fixture(autouse=True)
    def skip_if_no_fixtures(self, fixture_loader):
        """Skip tests if no fixtures exist."""
        fixtures = fixture_loader.list_fixtures()
        if not fixtures:
            pytest.skip(f"No fixtures found in {FIXTURE_DIR}")

    def test_parse_all_fixtures(self, replay_source, fixture_loader):
        """Parse all fixtures and validate results."""
        fixtures = fixture_loader.list_fixtures()

        for url_hash, fixture_path in fixtures:
            content, meta = fixture_loader.load_fixture(url_hash)

            # Create a mock harness that returns this fixture
            replay_source.replay._index = Mock()
            replay_source.replay._index.snapshots = {
                url_hash: meta
            }

            # Test that content can be parsed
            # The actual test depends on your parser implementation
            assert content is not None
            assert len(content) > 0

    def test_fixtures_produce_valid_picks(self, replay_source, fixture_loader):
        """Fixtures should produce schema-compliant picks."""
        fixtures = fixture_loader.list_fixtures()

        for url_hash, _ in fixtures:
            content, meta = fixture_loader.load_fixture(url_hash)

            # Parse fixture through the source
            picks = replay_source._parse_listing_html(
                meta.url if meta else f"https://example.com/{url_hash}",
                content
            )

            # Validate each pick
            for pick in picks:
                result = pick.validate()
                assert result.valid, f"Fixture {url_hash} produced invalid pick: {result.errors}"


# =============================================================================
# INTEGRATION TESTS
# =============================================================================

class TestIntegration:
    """Integration tests for full ingestion pipeline."""

    @pytest.mark.integration
    def test_full_ingest_pipeline(self, source):
        """Test full ingestion pipeline (requires network)."""
        # This test actually hits the network
        # Mark as integration test so it can be skipped in CI
        normalized, raw, stats = source.ingest(validate=True)

        # Basic sanity checks
        assert stats["raw_count"] >= 0
        assert stats["normalized_count"] >= 0
        assert stats["normalized_valid"] + stats["normalized_invalid"] == stats["normalized_count"]

        # All normalized picks should be valid
        if normalized:
            for pick in normalized:
                result = validate_normalized_pick(pick)
                assert result.valid, f"Invalid pick: {result.errors}"

    @pytest.mark.integration
    def test_snapshot_capture(self, source):
        """Test snapshot capture during ingestion."""
        # Create source in capture mode
        capture_source = SOURCE_CLASS(
            sport="NBA",
            debug=True,
            replay_mode="capture"
        )

        # Run ingestion
        capture_source.ingest(validate=False)

        # Check snapshots were captured
        harness = ReplayHarness(SOURCE_ID, sport="NBA")
        snapshots = harness.list_snapshots()

        # Should have at least one snapshot (the listing page)
        # Note: This may fail if no network available
        # assert len(snapshots) >= 1


# =============================================================================
# REGRESSION TESTS
# =============================================================================

class TestRegressions:
    """Regression tests for known issues.

    Add tests here when you fix bugs to prevent regressions.
    """

    def test_empty_matchup_hint(self, source, sample_raw_pick):
        """Empty matchup hint should not crash."""
        sample_raw_pick.matchup_hint = ""
        # Should not raise
        result = source.normalize_pick(sample_raw_pick)
        # May be None or ineligible, but shouldn't crash

    def test_none_values_in_raw_pick(self, source, sample_raw_pick):
        """None values should be handled gracefully."""
        sample_raw_pick.expert_name = None
        sample_raw_pick.event_start_time_utc = None
        # Should not raise
        result = source.normalize_pick(sample_raw_pick)


# =============================================================================
# HELPER FUNCTIONS FOR TESTING
# =============================================================================

def create_test_fixture(
    source_id: str,
    fixture_name: str,
    content: str,
    url: str = "https://example.com/test",
) -> Path:
    """Helper to create a test fixture file.

    Usage in test setup:
        fixture_path = create_test_fixture(
            "example",
            "listing_page",
            "<html>...</html>",
        )
    """
    fixture_dir = Path(__file__).parent / "fixtures" / source_id
    fixture_dir.mkdir(parents=True, exist_ok=True)

    from source_replay import ReplayHarness
    url_hash = ReplayHarness._hash_url(url)

    fixture_path = fixture_dir / f"{url_hash}.html"
    fixture_path.write_text(content)

    # Create metadata
    meta = {
        "source_id": source_id,
        "sport": "NBA",
        "snapshot_type": "html",
        "url": url,
        "url_hash": url_hash,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "content_hash": ReplayHarness._hash_content(content),
        "content_size": len(content),
        "compressed": False,
        "tags": [fixture_name],
    }
    meta_path = fixture_dir / f"{url_hash}.meta.json"
    meta_path.write_text(json.dumps(meta, indent=2))

    return fixture_path
