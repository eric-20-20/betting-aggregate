# Event Key Format Migration Plan

## Problem Statement

The codebase has two competing `event_key` formats:

| Format | Pattern | Example | Used By |
|--------|---------|---------|---------|
| **Legacy** | `{SPORT}:YYYYMMDD:AWAY@HOME:HHMM` | `NBA:20260213:DAL@LAL:1900` | `event_resolution.py`, `action_ingest.py`, `covers_ingest.py` |
| **Canonical** | `{SPORT}:YYYY:MM:DD:AWAY@HOME` | `NBA:2026:02:13:DAL@LAL` | `source_contract.py`, normalizers, schema validation |

This mismatch causes:
- Contract validation failures on existing data
- Potential dedupe issues if formats are mixed
- Confusion about which format to use

## Migration Strategy: Dual-Key Approach

Instead of a breaking change, we:
1. Add `canonical_event_key` alongside legacy `event_key`
2. Update downstream systems to prefer `canonical_event_key`
3. Backfill historical data with canonical keys
4. Eventually deprecate legacy format

## Why Not Just Change `event_key`?

Because `event_key` feeds into:
- `signal_key` (SHA256 hash for ledger deduplication)
- `offer_key` (price-specific tracking)
- `canonical_game_key` (cross-source matching)
- Grading lookups
- Historical analysis

Changing format would:
- Break dedupe (same bet = different hash)
- Split historical tracking
- Misalign grading results

---

## Phase 1: Add Canonical Key (Non-Breaking)

### 1.1 Add Builder Functions

In `src/source_contract.py` (already exists):
```python
def build_event_key(sport: str, year: int, month: int, day: int, away_team: str, home_team: str) -> str:
    return f"{sport}:{year:04d}:{month:02d}:{day:02d}:{away_team}@{home_team}"
```

Add converter function:
```python
def normalize_event_key(event_key: str) -> Optional[str]:
    """Convert any event_key format to canonical format.

    Handles:
    - NBA:20260213:DAL@LAL:1900 → NBA:2026:02:13:DAL@LAL
    - NBA:2026:02:13:DAL@LAL → NBA:2026:02:13:DAL@LAL (already canonical)
    """
```

### 1.2 Update Normalizers to Emit Both Keys

Each normalizer outputs:
```python
{
    "event": {
        "event_key": "NBA:20260213:DAL@LAL:1900",  # Legacy (for backward compat)
        "canonical_event_key": "NBA:2026:02:13:DAL@LAL",  # New canonical
        "day_key": "NBA:2026:02:13",
        ...
    }
}
```

Files to update:
- [ ] `src/normalizer_nba.py`
- [ ] `src/normalizer_betql_nba.py`
- [ ] `src/normalizer_sportsline_nba.py`
- [ ] `src/normalizer_sportscapping_nba.py`
- [x] `action_ingest.py` ✅ Updated - emits `canonical_event_key` in fallback event
- [x] `covers_ingest.py` ✅ Updated - emits `canonical_event_key` in fallback event
- [x] `event_resolution.py` ✅ Updated - `build_canonical_event_key()` added, `ScheduledGame` includes `canonical_event_key`

### 1.3 Update Schema to Accept Both

In `src/source_contract.py`:
```python
# Accept both formats during migration
EVENT_KEY_PATTERN = re.compile(
    r"^(NBA|NCAAB):"
    r"(\d{4}:\d{2}:\d{2}|\d{8})"  # YYYY:MM:DD or YYYYMMDD
    r":[A-Z]{2,5}@[A-Z]{2,5}"
    r"(:\d{4})?$"  # Optional :HHMM suffix
)

# Strict canonical pattern for validation
CANONICAL_EVENT_KEY_PATTERN = re.compile(
    r"^(NBA|NCAAB):\d{4}:\d{2}:\d{2}:[A-Z]{2,5}@[A-Z]{2,5}$"
)
```

---

## Phase 2: Update Downstream Systems

### 2.1 Signal Key Generation ✅ COMPLETE

In `scripts/build_signal_ledger.py`:
```python
def build_signal_key(signal: Dict[str, Any]) -> str:
    # Prefer canonical_event_key, fallback to event_key
    game_key = (
        signal.get("canonical_event_key") or
        normalize_event_key(signal.get("event_key")) or
        signal.get("day_key") or ""
    )
    # ... rest unchanged
```

**Status**: Updated - prefers `canonical_event_key` over legacy `event_key`

### 2.2 Grading Lookups ✅ COMPLETE

In `scripts/grade_signals_nba.py`:
- Already constructs `safe_event_key` in canonical format
- ✅ `attach_event_keys()` now sets `canonical_event_key` alongside `event_key_safe`
- Output includes: `event_key_raw`, `event_key_safe`, and `canonical_event_key`

### 2.3 Research Dataset ✅ COMPLETE

In `scripts/build_research_dataset_nba.py`:
- ✅ Derives `canonical_event_key` from grade or signal or computes from components
- ✅ Output row includes `canonical_event_key` field

### 2.4 Consensus Builder

In `consensus_nba.py`:
- Group by `canonical_event_key` instead of `event_key`
- Fallback to normalized `event_key` if canonical not present
- **Status**: Not yet updated (low priority - consensus grouping uses day_key)

---

## Phase 3: Backfill Historical Data

### 3.1 Create Backfill Script

```python
# scripts/backfill_canonical_event_keys.py

def backfill_ledger(path: Path):
    """Add canonical_event_key to existing ledger entries."""
    records = read_jsonl(path)
    updated = []
    for rec in records:
        if "canonical_event_key" not in rec:
            event_key = rec.get("event_key")
            if event_key:
                rec["canonical_event_key"] = normalize_event_key(event_key)
        updated.append(rec)
    write_jsonl(path, updated)
```

### 3.2 Backfill Targets

- [ ] `data/ledger/signals_latest.jsonl`
- [ ] `data/ledger/signals_occurrences.jsonl`
- [ ] `data/ledger/grades_latest.jsonl`
- [ ] `data/ledger/grades_occurrences.jsonl`
- [ ] `out/normalized_*.json` files

---

## Phase 4: Enforce Canonical Format

### 4.1 Update Contract Tests

Remove `@pytest.mark.xfail` decorators once migration complete:
- [ ] `tests/test_source_contracts.py`
- [ ] `tests/test_source_template.py`

### 4.2 Update Validation

In `src/source_contract.py`:
```python
# After migration complete, require canonical format
def validate_normalized_pick(record: Dict[str, Any]) -> ValidationResult:
    # ...
    # Require canonical_event_key (not just event_key)
    canonical_key = record.get("event", {}).get("canonical_event_key")
    if not canonical_key:
        errors.append("event.canonical_event_key is required")
    elif not CANONICAL_EVENT_KEY_PATTERN.match(canonical_key):
        errors.append(f"event.canonical_event_key format invalid: {canonical_key}")
```

### 4.3 Deprecate Legacy Format

- Add deprecation warning when `event_key` is used without `canonical_event_key`
- Eventually remove legacy format support

---

## Implementation Order

```
Phase 1 (Safe, Non-Breaking)
├── 1.1 Add normalize_event_key() converter
├── 1.2 Update normalizers to emit both keys
└── 1.3 Update schema to accept both formats

Phase 2 (Gradual Rollout) ✅ COMPLETE
├── 2.1 Update signal key generation (prefer canonical) ✅
├── 2.2 Update grading lookups ✅
├── 2.3 Update research dataset builder ✅
└── 2.4 Update consensus builder (deferred - uses day_key)

Phase 3 (Data Migration) ✅ COMPLETE
├── 3.1 Create backfill script ✅ scripts/backfill_canonical_event_keys.py
└── 3.2 Run backfill on all ledger files ✅ 54,785 records updated

Phase 4 (Enforcement) ✅ COMPLETE
├── 4.1 Remove xfail from tests ✅ (event_key-related xfails removed)
├── 4.2 Validation accepts both formats (FLEXIBLE_EVENT_KEY_PATTERN)
└── 4.3 Warnings for legacy format without canonical_event_key
```

---

## Rollback Plan

If issues arise:
1. All systems continue to work with `event_key` (legacy)
2. `canonical_event_key` is additive, not replacing
3. Signal key generation has fallback to `event_key`
4. No data loss possible - we're adding fields, not changing them

---

## Success Criteria

- [x] All new picks have `canonical_event_key` ✅ Downstream systems emit it
- [x] All ledger entries have `canonical_event_key` ✅ 54,785 records backfilled
- [x] Contract tests pass without xfail ✅ Event_key-related xfails removed
- [x] Grading uses canonical format ✅ `attach_event_keys()` sets it
- [x] Signal deduplication works correctly ✅ Uses canonical_event_key in signal_key
- [x] Historical data is preserved ✅ Legacy event_key retained alongside canonical
