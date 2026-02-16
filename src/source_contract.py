"""Source Contract: Strict schema definitions for raw → normalized picks.

This module defines the contract that ALL sources must adhere to.
It provides:
- Schema definitions with validation
- Base classes for source implementations
- Drift detection for schema changes
- Validation utilities

Goal: "add a new source in <2 hours" with confidence it won't corrupt your ledger.
"""

from __future__ import annotations

import hashlib
import json
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, TypedDict, Union


# =============================================================================
# CONSTANTS
# =============================================================================

NBA_SPORT = "NBA"
NCAAB_SPORT = "NCAAB"

VALID_SPORTS = {NBA_SPORT, NCAAB_SPORT}
VALID_MARKET_TYPES = {"spread", "total", "moneyline", "player_prop"}
VALID_MARKET_FAMILIES = {"standard", "player_prop"}
VALID_STANDARD_SIDES = {"OVER", "UNDER"}  # For totals
VALID_PROP_SIDES = {"player_over", "player_under", "OVER", "UNDER"}

# Canonical stat keys for player props
CANONICAL_STAT_KEYS = {
    "points", "rebounds", "assists", "steals", "blocks",
    "threes_made", "turnovers", "pts_reb", "pts_ast",
    "reb_ast", "pts_reb_ast", "stl_blk", "fantasy_score",
    "double_double", "triple_double", "minutes",
}

# Team code pattern: 2-5 uppercase letters
TEAM_CODE_PATTERN = re.compile(r"^[A-Z]{2,5}$")

# Event key pattern: SPORT:YYYY:MM:DD:AWAY@HOME (canonical format)
EVENT_KEY_PATTERN = re.compile(r"^(NBA|NCAAB):\d{4}:\d{2}:\d{2}:[A-Z]{2,5}@[A-Z]{2,5}$")

# Legacy event key pattern: SPORT:YYYYMMDD:AWAY@HOME:HHMM (from event_resolution.py)
LEGACY_EVENT_KEY_PATTERN = re.compile(
    r"^(NBA|NCAAB):(\d{4})(\d{2})(\d{2}):([A-Z]{2,5})@([A-Z]{2,5}):(\d{4})$"
)

# Flexible event key pattern: accepts both canonical and legacy formats
FLEXIBLE_EVENT_KEY_PATTERN = re.compile(
    r"^(NBA|NCAAB):"
    r"(\d{4}:\d{2}:\d{2}|\d{8})"  # YYYY:MM:DD or YYYYMMDD
    r":[A-Z]{2,5}@[A-Z]{2,5}"
    r"(:\d{4})?$"  # Optional :HHMM suffix
)

# Day key pattern: SPORT:YYYY:MM:DD
DAY_KEY_PATTERN = re.compile(r"^(NBA|NCAAB):\d{4}:\d{2}:\d{2}$")

# Player key pattern: SPORT:player_slug
PLAYER_KEY_PATTERN = re.compile(r"^(NBA|NCAAB):[a-z_]+$")


# =============================================================================
# VALIDATION RESULT
# =============================================================================

@dataclass
class ValidationResult:
    """Result of schema validation."""
    valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def __bool__(self) -> bool:
        return self.valid

    def merge(self, other: "ValidationResult") -> "ValidationResult":
        """Merge another validation result into this one."""
        return ValidationResult(
            valid=self.valid and other.valid,
            errors=self.errors + other.errors,
            warnings=self.warnings + other.warnings,
        )


# =============================================================================
# RAW PICK SCHEMA
# =============================================================================

@dataclass
class RawPickSchema:
    """Schema for raw pick records from source ingestion.

    This is what the scraper/parser produces BEFORE normalization.
    """
    # Required fields
    source_id: str
    source_surface: str
    sport: str
    market_family: str  # "standard" or "player_prop"
    observed_at_utc: str  # ISO timestamp
    canonical_url: str
    raw_pick_text: str  # The extracted pick text
    raw_block: str  # Full context (card HTML, etc.)
    raw_fingerprint: str  # SHA256 for deduplication

    # Optional metadata
    source_updated_at_utc: Optional[str] = None
    event_start_time_utc: Optional[str] = None
    player_hint: Optional[str] = None
    stat_hint: Optional[str] = None
    line_hint: Optional[float] = None
    odds_hint: Optional[int] = None
    expert_name: Optional[str] = None
    expert_handle: Optional[str] = None
    expert_profile: Optional[str] = None
    expert_slug: Optional[str] = None
    tailing_handle: Optional[str] = None
    rating_stars: Optional[int] = None  # For model sources like BetQL
    matchup_hint: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, excluding None values."""
        return {k: v for k, v in asdict(self).items() if v is not None}

    @staticmethod
    def compute_fingerprint(source_id: str, source_surface: str, canonical_url: str, raw_pick_text: str) -> str:
        """Compute consistent SHA256 fingerprint."""
        raw = f"{source_id}|{source_surface}|{canonical_url}|{raw_pick_text}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def validate(self) -> ValidationResult:
        """Validate this raw pick against the schema."""
        errors = []
        warnings = []

        # Required field checks
        if not self.source_id:
            errors.append("source_id is required")
        if not self.source_surface:
            errors.append("source_surface is required")
        if not self.sport or self.sport not in VALID_SPORTS:
            errors.append(f"sport must be one of {VALID_SPORTS}, got: {self.sport}")
        if not self.market_family or self.market_family not in VALID_MARKET_FAMILIES:
            errors.append(f"market_family must be one of {VALID_MARKET_FAMILIES}, got: {self.market_family}")
        if not self.observed_at_utc:
            errors.append("observed_at_utc is required")
        else:
            try:
                datetime.fromisoformat(self.observed_at_utc.replace("Z", "+00:00"))
            except ValueError:
                errors.append(f"observed_at_utc must be ISO format, got: {self.observed_at_utc}")
        if not self.canonical_url:
            errors.append("canonical_url is required")
        if not self.raw_pick_text:
            errors.append("raw_pick_text is required")
        if not self.raw_fingerprint:
            errors.append("raw_fingerprint is required")

        # Fingerprint consistency check
        expected_fp = self.compute_fingerprint(
            self.source_id or "", self.source_surface or "",
            self.canonical_url or "", self.raw_pick_text or ""
        )
        if self.raw_fingerprint and self.raw_fingerprint != expected_fp:
            warnings.append(f"raw_fingerprint mismatch: expected {expected_fp[:16]}..., got {self.raw_fingerprint[:16]}...")

        return ValidationResult(valid=len(errors) == 0, errors=errors, warnings=warnings)


# =============================================================================
# NORMALIZED PICK SCHEMA
# =============================================================================

class ProvenanceSchema(TypedDict, total=False):
    """Provenance section of normalized pick."""
    source_id: str  # Required
    source_surface: str  # Required
    sport: str  # Required
    observed_at_utc: str  # Required
    canonical_url: str  # Required
    raw_fingerprint: str  # Required
    raw_pick_text: Optional[str]
    expert_name: Optional[str]
    expert_handle: Optional[str]
    expert_profile: Optional[str]
    expert_slug: Optional[str]
    expert_id: Optional[str]
    tailing_handle: Optional[str]
    pick_id: Optional[str]
    writeup: Optional[str]


class EventSchema(TypedDict, total=False):
    """Event section of normalized pick."""
    sport: str  # Required
    event_key: str  # Required: {SPORT}:YYYY:MM:DD:AWAY@HOME or legacy format
    canonical_event_key: Optional[str]  # Canonical: {SPORT}:YYYY:MM:DD:AWAY@HOME
    day_key: str  # Required: {SPORT}:YYYY:MM:DD
    matchup_key: Optional[str]  # {SPORT}:YYYY:MM:DD:HOME-AWAY
    away_team: str  # Required: 2-5 letter code
    home_team: str  # Required: 2-5 letter code
    event_start_time_utc: Optional[str]
    away_score: Optional[int]
    home_score: Optional[int]


class MarketSchema(TypedDict, total=False):
    """Market section of normalized pick."""
    market_type: str  # Required: spread|total|moneyline|player_prop
    market_family: str  # Required: standard|player_prop
    side: Optional[str]  # OVER/UNDER for totals, team for spread/ML
    selection: Optional[str]  # Canonical selection identifier
    line: Optional[float]
    odds: Optional[int]  # American odds
    stat_key: Optional[str]  # For player props
    player_key: Optional[str]  # For player props: {SPORT}:player_slug
    player_id: Optional[str]
    player_name: Optional[str]
    unit: Optional[float]


class ResultSchema(TypedDict, total=False):
    """Pre-graded result (optional, for sources like SportsLine)."""
    status: str  # WIN|LOSS|PUSH
    graded_by: str  # Source that graded it


@dataclass
class NormalizedPickSchema:
    """Schema for normalized pick records ready for consensus/grading.

    This is the OUTPUT of normalization, INPUT to consensus builder.
    """
    provenance: Dict[str, Any]
    event: Dict[str, Any]
    market: Dict[str, Any]
    eligible_for_consensus: bool
    ineligibility_reason: Optional[str]
    result: Optional[Dict[str, Any]] = None  # Pre-graded result

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        d = {
            "provenance": self.provenance,
            "event": self.event,
            "market": self.market,
            "eligible_for_consensus": self.eligible_for_consensus,
            "ineligibility_reason": self.ineligibility_reason,
        }
        if self.result:
            d["result"] = self.result
        return d

    def validate(self) -> ValidationResult:
        """Validate this normalized pick against the schema."""
        errors = []
        warnings = []

        # Provenance validation
        prov = self.provenance or {}
        if not prov.get("source_id"):
            errors.append("provenance.source_id is required")
        if not prov.get("source_surface"):
            errors.append("provenance.source_surface is required")
        if not prov.get("sport") or prov.get("sport") not in VALID_SPORTS:
            errors.append(f"provenance.sport must be one of {VALID_SPORTS}")
        if not prov.get("observed_at_utc"):
            errors.append("provenance.observed_at_utc is required")
        if not prov.get("canonical_url"):
            warnings.append("provenance.canonical_url is missing (recommended)")

        # Event validation
        ev = self.event or {}
        event_key = ev.get("event_key")
        canonical_event_key = ev.get("canonical_event_key")

        # Require at least one event key
        if not event_key and not canonical_event_key:
            errors.append("event.event_key or event.canonical_event_key is required")
        else:
            # Validate canonical_event_key if present (strict format)
            if canonical_event_key:
                if not EVENT_KEY_PATTERN.match(canonical_event_key):
                    errors.append(f"event.canonical_event_key must match {EVENT_KEY_PATTERN.pattern}, got: {canonical_event_key}")
            else:
                # No canonical_event_key - warn and check event_key format
                warnings.append("event.canonical_event_key is missing (recommended for new sources)")

            # Validate event_key (accepts both canonical and legacy formats)
            if event_key:
                if not FLEXIBLE_EVENT_KEY_PATTERN.match(event_key):
                    errors.append(f"event.event_key format invalid, got: {event_key}")
                elif not EVENT_KEY_PATTERN.match(event_key) and not canonical_event_key:
                    # Legacy format without canonical - warn about migration
                    warnings.append(f"event.event_key uses legacy format, consider adding canonical_event_key")

        day_key = ev.get("day_key")
        if not day_key:
            warnings.append("event.day_key is missing (recommended)")
        elif not DAY_KEY_PATTERN.match(day_key):
            errors.append(f"event.day_key must match {DAY_KEY_PATTERN.pattern}, got: {day_key}")

        away_team = ev.get("away_team")
        home_team = ev.get("home_team")
        if not away_team:
            errors.append("event.away_team is required")
        elif not TEAM_CODE_PATTERN.match(away_team):
            errors.append(f"event.away_team must be 2-5 uppercase letters, got: {away_team}")
        if not home_team:
            errors.append("event.home_team is required")
        elif not TEAM_CODE_PATTERN.match(home_team):
            errors.append(f"event.home_team must be 2-5 uppercase letters, got: {home_team}")

        # Market validation
        mkt = self.market or {}
        market_type = mkt.get("market_type")
        if not market_type:
            errors.append("market.market_type is required")
        elif market_type not in VALID_MARKET_TYPES:
            errors.append(f"market.market_type must be one of {VALID_MARKET_TYPES}, got: {market_type}")

        # Market-specific validation
        if market_type == "player_prop":
            if not mkt.get("stat_key"):
                warnings.append("market.stat_key missing for player_prop")
            elif mkt.get("stat_key") not in CANONICAL_STAT_KEYS:
                warnings.append(f"market.stat_key '{mkt.get('stat_key')}' not in canonical set")

            player_key = mkt.get("player_key")
            if not player_key:
                warnings.append("market.player_key missing for player_prop")
            elif player_key and ":" in player_key:
                # Check format
                if not PLAYER_KEY_PATTERN.match(player_key):
                    warnings.append(f"market.player_key should match {PLAYER_KEY_PATTERN.pattern}, got: {player_key}")

            side = mkt.get("side")
            if side and side not in VALID_PROP_SIDES:
                warnings.append(f"market.side for player_prop should be in {VALID_PROP_SIDES}, got: {side}")

        elif market_type == "total":
            side = mkt.get("side")
            if side and side.upper() not in VALID_STANDARD_SIDES:
                warnings.append(f"market.side for total should be OVER/UNDER, got: {side}")
            if mkt.get("line") is None:
                warnings.append("market.line is missing for total")

        elif market_type == "spread":
            if mkt.get("line") is None:
                warnings.append("market.line is missing for spread")
            selection = mkt.get("selection")
            if selection and away_team and home_team:
                if selection not in {away_team, home_team, f"{away_team}_spread", f"{home_team}_spread"}:
                    warnings.append(f"market.selection '{selection}' doesn't match teams {away_team}/{home_team}")

        # Eligibility consistency
        if self.eligible_for_consensus and self.ineligibility_reason:
            warnings.append("eligible_for_consensus=True but ineligibility_reason is set")
        if not self.eligible_for_consensus and not self.ineligibility_reason:
            warnings.append("eligible_for_consensus=False but no ineligibility_reason given")

        return ValidationResult(valid=len(errors) == 0, errors=errors, warnings=warnings)


# =============================================================================
# VALIDATION UTILITIES
# =============================================================================

def validate_raw_pick(record: Dict[str, Any]) -> ValidationResult:
    """Validate a raw pick dictionary against the schema."""
    try:
        schema = RawPickSchema(
            source_id=record.get("source_id", ""),
            source_surface=record.get("source_surface", ""),
            sport=record.get("sport", ""),
            market_family=record.get("market_family", ""),
            observed_at_utc=record.get("observed_at_utc", ""),
            canonical_url=record.get("canonical_url", ""),
            raw_pick_text=record.get("raw_pick_text", ""),
            raw_block=record.get("raw_block", ""),
            raw_fingerprint=record.get("raw_fingerprint", ""),
            source_updated_at_utc=record.get("source_updated_at_utc"),
            event_start_time_utc=record.get("event_start_time_utc"),
            player_hint=record.get("player_hint"),
            stat_hint=record.get("stat_hint"),
            line_hint=record.get("line_hint"),
            odds_hint=record.get("odds_hint"),
            expert_name=record.get("expert_name"),
            expert_handle=record.get("expert_handle"),
            expert_profile=record.get("expert_profile"),
            expert_slug=record.get("expert_slug"),
            tailing_handle=record.get("tailing_handle"),
            rating_stars=record.get("rating_stars"),
            matchup_hint=record.get("matchup_hint"),
        )
        return schema.validate()
    except Exception as e:
        return ValidationResult(valid=False, errors=[f"Schema construction failed: {e}"])


def validate_normalized_pick(record: Dict[str, Any]) -> ValidationResult:
    """Validate a normalized pick dictionary against the schema."""
    try:
        schema = NormalizedPickSchema(
            provenance=record.get("provenance", {}),
            event=record.get("event", {}),
            market=record.get("market", {}),
            eligible_for_consensus=record.get("eligible_for_consensus", False),
            ineligibility_reason=record.get("ineligibility_reason"),
            result=record.get("result"),
        )
        return schema.validate()
    except Exception as e:
        return ValidationResult(valid=False, errors=[f"Schema construction failed: {e}"])


def validate_batch(
    records: List[Dict[str, Any]],
    validator: callable,
    max_errors: int = 10,
) -> Tuple[int, int, List[Dict[str, Any]]]:
    """Validate a batch of records.

    Returns:
        (valid_count, invalid_count, error_samples)
    """
    valid_count = 0
    invalid_count = 0
    error_samples = []

    for i, record in enumerate(records):
        result = validator(record)
        if result.valid:
            valid_count += 1
        else:
            invalid_count += 1
            if len(error_samples) < max_errors:
                error_samples.append({
                    "index": i,
                    "errors": result.errors,
                    "warnings": result.warnings,
                    "sample": {k: v for k, v in record.items() if k in {"source_id", "event_key", "market_type"}},
                })

    return valid_count, invalid_count, error_samples


# =============================================================================
# DRIFT DETECTION
# =============================================================================

@dataclass
class SchemaSnapshot:
    """Snapshot of schema for drift detection."""
    source_id: str
    captured_at: str
    field_counts: Dict[str, int]  # Field name -> count of records with this field
    field_samples: Dict[str, List[Any]]  # Field name -> sample values
    total_records: int
    valid_records: int
    error_rate: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "SchemaSnapshot":
        return SchemaSnapshot(**d)


def capture_schema_snapshot(
    source_id: str,
    records: List[Dict[str, Any]],
    validator: callable = validate_normalized_pick,
    sample_size: int = 3,
) -> SchemaSnapshot:
    """Capture a schema snapshot for drift detection."""
    field_counts: Dict[str, int] = {}
    field_samples: Dict[str, List[Any]] = {}
    valid_count = 0

    def count_fields(d: Dict[str, Any], prefix: str = "") -> None:
        for k, v in d.items():
            full_key = f"{prefix}.{k}" if prefix else k
            field_counts[full_key] = field_counts.get(full_key, 0) + 1
            if full_key not in field_samples:
                field_samples[full_key] = []
            if len(field_samples[full_key]) < sample_size and v is not None:
                field_samples[full_key].append(v if not isinstance(v, dict) else "<dict>")
            if isinstance(v, dict):
                count_fields(v, full_key)

    for record in records:
        count_fields(record)
        if validator(record).valid:
            valid_count += 1

    return SchemaSnapshot(
        source_id=source_id,
        captured_at=datetime.now(timezone.utc).isoformat(),
        field_counts=field_counts,
        field_samples=field_samples,
        total_records=len(records),
        valid_records=valid_count,
        error_rate=1 - (valid_count / len(records)) if records else 0,
    )


def detect_drift(
    baseline: SchemaSnapshot,
    current: SchemaSnapshot,
    new_field_threshold: float = 0.1,  # Alert if new field appears in >10% of records
    missing_field_threshold: float = 0.5,  # Alert if field disappears from >50% of records
    error_rate_threshold: float = 0.05,  # Alert if error rate increases by >5%
) -> List[str]:
    """Detect schema drift between baseline and current snapshots."""
    alerts = []

    # Check for new fields
    for field, count in current.field_counts.items():
        if field not in baseline.field_counts:
            rate = count / current.total_records if current.total_records else 0
            if rate >= new_field_threshold:
                alerts.append(f"NEW_FIELD: '{field}' appears in {rate*100:.1f}% of records")

    # Check for missing fields
    for field, count in baseline.field_counts.items():
        baseline_rate = count / baseline.total_records if baseline.total_records else 0
        current_count = current.field_counts.get(field, 0)
        current_rate = current_count / current.total_records if current.total_records else 0

        if baseline_rate > 0.5 and current_rate < baseline_rate * (1 - missing_field_threshold):
            alerts.append(f"MISSING_FIELD: '{field}' dropped from {baseline_rate*100:.1f}% to {current_rate*100:.1f}%")

    # Check error rate
    error_increase = current.error_rate - baseline.error_rate
    if error_increase > error_rate_threshold:
        alerts.append(f"ERROR_RATE: increased from {baseline.error_rate*100:.1f}% to {current.error_rate*100:.1f}%")

    return alerts


# =============================================================================
# SOURCE BASE CLASS
# =============================================================================

class BaseSource(ABC):
    """Abstract base class for source implementations.

    Subclasses must implement:
    - source_id: str property
    - source_surface: str property
    - sport: str property (default NBA)
    - fetch_raw_picks() -> List[RawPickSchema]
    - normalize_pick(raw: RawPickSchema) -> NormalizedPickSchema

    The base class provides:
    - Validation at each stage
    - Fingerprint computation
    - Deduplication
    - Error collection
    """

    @property
    @abstractmethod
    def source_id(self) -> str:
        """Unique identifier for this source (e.g., 'action', 'covers')."""
        pass

    @property
    @abstractmethod
    def source_surface(self) -> str:
        """Surface identifier (e.g., 'nba_game_expert_picks')."""
        pass

    @property
    def sport(self) -> str:
        """Sport this source covers. Default: NBA."""
        return NBA_SPORT

    @abstractmethod
    def fetch_raw_picks(self) -> List[RawPickSchema]:
        """Fetch and parse raw picks from the source.

        Returns list of RawPickSchema instances.
        """
        pass

    @abstractmethod
    def normalize_pick(self, raw: RawPickSchema) -> Optional[NormalizedPickSchema]:
        """Normalize a single raw pick.

        Returns NormalizedPickSchema or None if normalization fails.
        """
        pass

    def compute_fingerprint(self, canonical_url: str, raw_pick_text: str) -> str:
        """Compute consistent fingerprint for a pick."""
        return RawPickSchema.compute_fingerprint(
            self.source_id, self.source_surface, canonical_url, raw_pick_text
        )

    def ingest(self, validate: bool = True) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
        """Run full ingestion pipeline.

        Returns:
            (normalized_picks, raw_picks, stats)
        """
        stats = {
            "source_id": self.source_id,
            "source_surface": self.source_surface,
            "sport": self.sport,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "raw_count": 0,
            "raw_valid": 0,
            "raw_invalid": 0,
            "normalized_count": 0,
            "normalized_valid": 0,
            "normalized_invalid": 0,
            "eligible_count": 0,
            "ineligible_count": 0,
            "errors": [],
        }

        # Fetch raw picks
        try:
            raw_picks = self.fetch_raw_picks()
            stats["raw_count"] = len(raw_picks)
        except Exception as e:
            stats["errors"].append(f"fetch_raw_picks failed: {e}")
            return [], [], stats

        # Validate raw picks
        raw_dicts = []
        for raw in raw_picks:
            raw_dict = raw.to_dict()
            raw_dicts.append(raw_dict)
            if validate:
                result = raw.validate()
                if result.valid:
                    stats["raw_valid"] += 1
                else:
                    stats["raw_invalid"] += 1
                    if len(stats["errors"]) < 10:
                        stats["errors"].append(f"raw validation: {result.errors[:3]}")

        # Normalize
        normalized = []
        seen_keys = set()
        for raw in raw_picks:
            try:
                norm = self.normalize_pick(raw)
                if norm is None:
                    continue

                # Dedupe key
                ev = norm.event or {}
                mkt = norm.market or {}
                key = (
                    ev.get("event_key"),
                    mkt.get("market_type"),
                    mkt.get("selection"),
                    mkt.get("line"),
                    mkt.get("odds"),
                )
                if key in seen_keys:
                    continue
                seen_keys.add(key)

                norm_dict = norm.to_dict()
                normalized.append(norm_dict)
                stats["normalized_count"] += 1

                if validate:
                    result = norm.validate()
                    if result.valid:
                        stats["normalized_valid"] += 1
                    else:
                        stats["normalized_invalid"] += 1
                        if len(stats["errors"]) < 10:
                            stats["errors"].append(f"normalized validation: {result.errors[:3]}")

                if norm.eligible_for_consensus:
                    stats["eligible_count"] += 1
                else:
                    stats["ineligible_count"] += 1

            except Exception as e:
                if len(stats["errors"]) < 10:
                    stats["errors"].append(f"normalize_pick failed: {e}")

        return normalized, raw_dicts, stats


# =============================================================================
# HELPERS
# =============================================================================

def build_event_key(sport: str, year: int, month: int, day: int, away_team: str, home_team: str) -> str:
    """Build canonical event key."""
    return f"{sport}:{year:04d}:{month:02d}:{day:02d}:{away_team}@{home_team}"


def build_day_key(sport: str, year: int, month: int, day: int) -> str:
    """Build canonical day key."""
    return f"{sport}:{year:04d}:{month:02d}:{day:02d}"


def build_matchup_key(sport: str, year: int, month: int, day: int, home_team: str, away_team: str) -> str:
    """Build canonical matchup key (home-away order)."""
    return f"{sport}:{year:04d}:{month:02d}:{day:02d}:{home_team}-{away_team}"


def build_player_key(sport: str, player_slug: str) -> str:
    """Build canonical player key."""
    return f"{sport}:{player_slug}"


def slugify_player_name(name: str) -> str:
    """Convert player name to slug format."""
    # Remove Jr., III, etc.
    name = re.sub(r"\s+(jr\.?|sr\.?|iii|ii|iv)$", "", name, flags=re.IGNORECASE)
    # Convert to lowercase, replace spaces/punctuation with underscore
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower())
    # Remove leading/trailing underscores
    slug = slug.strip("_")
    # Collapse multiple underscores
    slug = re.sub(r"_+", "_", slug)
    return slug


def parse_event_key(event_key: str) -> Optional[Tuple[str, int, int, int, str, str]]:
    """Parse event key into components.

    Handles both canonical and legacy formats:
    - Canonical: NBA:2026:02:13:DAL@LAL
    - Legacy: NBA:20260213:DAL@LAL:1900

    Returns: (sport, year, month, day, away_team, home_team) or None
    """
    if not event_key:
        return None

    # Try canonical format first
    match = EVENT_KEY_PATTERN.match(event_key)
    if match:
        parts = event_key.split(":")
        sport = parts[0]
        year = int(parts[1])
        month = int(parts[2])
        day = int(parts[3])
        teams = parts[4].split("@")
        return (sport, year, month, day, teams[0], teams[1])

    # Try legacy format: NBA:20260213:DAL@LAL:1900
    legacy_match = LEGACY_EVENT_KEY_PATTERN.match(event_key)
    if legacy_match:
        sport = legacy_match.group(1)
        year = int(legacy_match.group(2))
        month = int(legacy_match.group(3))
        day = int(legacy_match.group(4))
        away = legacy_match.group(5)
        home = legacy_match.group(6)
        return (sport, year, month, day, away, home)

    return None


def normalize_event_key(event_key: Optional[str]) -> Optional[str]:
    """Convert any event_key format to canonical format.

    Handles:
    - NBA:20260213:DAL@LAL:1900 → NBA:2026:02:13:DAL@LAL
    - NBA:2026:02:13:DAL@LAL → NBA:2026:02:13:DAL@LAL (unchanged)
    - Invalid → None

    Args:
        event_key: Event key in any supported format

    Returns:
        Canonical event key or None if parsing fails
    """
    parsed = parse_event_key(event_key)
    if not parsed:
        return None

    sport, year, month, day, away, home = parsed
    return build_event_key(sport, year, month, day, away, home)


def is_canonical_event_key(event_key: Optional[str]) -> bool:
    """Check if event_key is in canonical format."""
    if not event_key:
        return False
    return bool(EVENT_KEY_PATTERN.match(event_key))


def is_legacy_event_key(event_key: Optional[str]) -> bool:
    """Check if event_key is in legacy format."""
    if not event_key:
        return False
    return bool(LEGACY_EVENT_KEY_PATTERN.match(event_key))
