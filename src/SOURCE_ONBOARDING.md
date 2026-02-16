# Source Onboarding Checklist

**Goal:** Add a new source in <2 hours with confidence it won't corrupt your ledger.

## Quick Start

```bash
# 1. Copy the template
cp src/source_template.py src/{your_source}_source.py
cp tests/test_source_template.py tests/test_{your_source}_source.py

# 2. Update identifiers (search and replace)
#    - "example" -> "your_source"
#    - "ExampleSource" -> "YourSourceSource"

# 3. Run tests to verify setup
python -m pytest tests/test_{your_source}_source.py -v

# 4. Implement your source (see checklist below)

# 5. Capture test fixtures
python -m src.{your_source}_source --capture --debug

# 6. Run full test suite
python -m pytest tests/test_{your_source}_source.py -v
```

---

## Implementation Checklist

### Phase 1: Configuration (15 min)

- [ ] Set `SOURCE_ID` (lowercase, no spaces, e.g., "draftkings")
- [ ] Set `SOURCE_SURFACE` (describes what this scrapes, e.g., "expert_picks")
- [ ] Configure `LISTING_URLS` for each sport
- [ ] Configure any URL patterns for parsing game pages

### Phase 2: Fetching (30 min)

- [ ] Implement `_fetch_url()` method
  - [ ] Simple sites: Use `requests.get()`
  - [ ] JS-rendered: Use Playwright
  - [ ] Authenticated: Use session with cookies
- [ ] Handle rate limiting and retries
- [ ] Add User-Agent header

### Phase 3: Parsing (45 min)

- [ ] Implement `_parse_listing_html()` to find pick cards
- [ ] Implement `_extract_expert_name()` for expert attribution
- [ ] Implement `_extract_matchup()` for team parsing
- [ ] Create valid `RawPickSchema` for each pick

### Phase 4: Normalization (30 min)

- [ ] Implement `_parse_teams()` for team code lookup
  - [ ] Handle your source's matchup format
  - [ ] Use `DataStore` for name -> code mapping
- [ ] Implement `_parse_market()` for market detection
  - [ ] Spread detection (look for +/- numbers)
  - [ ] Total detection (look for OVER/UNDER)
  - [ ] Moneyline detection (look for ML/WIN)
  - [ ] Player prop detection (player names + stats)
- [ ] Ensure all normalized picks pass validation

### Phase 5: Testing (15 min)

- [ ] Capture initial fixtures: `--capture --debug`
- [ ] Run contract tests: `pytest -k TestSourceContract`
- [ ] Run validation tests: `pytest -k TestRawPickSchema`
- [ ] Run normalization tests: `pytest -k TestNormalization`
- [ ] Verify no drift in schema

### Phase 6: Integration (15 min)

- [ ] Add to ingestion pipeline (e.g., `ingest_all.py`)
- [ ] Add to consensus builder source list
- [ ] Verify picks appear in consensus output
- [ ] Add to grading pipeline

---

## File Structure

```
src/
├── source_contract.py      # Schema definitions (DON'T MODIFY)
├── source_replay.py        # Snapshot harness (DON'T MODIFY)
├── source_template.py      # Copy this for new sources
├── {your_source}_source.py # Your implementation
└── SOURCE_ONBOARDING.md    # This file

tests/
├── test_source_template.py # Copy this for new tests
├── test_{your_source}_source.py # Your tests
└── fixtures/
    └── {your_source}/      # HTML/JSON fixtures for testing
```

---

## Schema Reference

### RawPickSchema (What your parser produces)

Required fields:
- `source_id`: Your source identifier (e.g., "draftkings")
- `source_surface`: Surface identifier (e.g., "expert_picks")
- `sport`: "NBA" or "NCAAB"
- `market_family`: "standard" or "player_prop"
- `observed_at_utc`: ISO timestamp
- `canonical_url`: Source URL
- `raw_pick_text`: Extracted pick text
- `raw_block`: Full context HTML
- `raw_fingerprint`: SHA256 for deduplication

Optional fields:
- `event_start_time_utc`: Game start time
- `expert_name`, `expert_handle`: Expert attribution
- `matchup_hint`: Team names/codes
- `line_hint`, `odds_hint`: Pre-parsed values
- `player_hint`, `stat_hint`: For player props

### NormalizedPickSchema (What your normalizer produces)

```python
{
    "provenance": {
        "source_id": "draftkings",       # Required
        "source_surface": "expert_picks", # Required
        "sport": "NBA",                   # Required
        "observed_at_utc": "...",         # Required
        "canonical_url": "...",           # Recommended
        "raw_fingerprint": "...",         # Required
        "expert_name": "...",             # Optional
    },
    "event": {
        "sport": "NBA",                   # Required
        "event_key": "NBA:2026:02:16:LAL@BOS",  # Required
        "day_key": "NBA:2026:02:16",      # Recommended
        "away_team": "LAL",               # Required (2-5 uppercase)
        "home_team": "BOS",               # Required (2-5 uppercase)
    },
    "market": {
        "market_type": "spread",          # Required: spread|total|moneyline|player_prop
        "market_family": "standard",      # Required: standard|player_prop
        "selection": "LAL",               # For spread/ML
        "line": -5.5,                     # For spread/total
        "side": "OVER",                   # For total: OVER|UNDER
    },
    "eligible_for_consensus": true,
    "ineligibility_reason": null,         # Required if ineligible
}
```

---

## Common Issues & Solutions

### Team Code Mapping

Problem: Source uses full names ("Los Angeles Lakers") but we need codes ("LAL").

Solution: Use the DataStore for lookups:
```python
from store import DataStore
store = DataStore(sport="NBA")
team = store.team_by_name("Los Angeles Lakers")
code = team.code if team else None
```

### Handling Pagination

Problem: Source has multiple pages of picks.

Solution: Implement page iteration in `fetch_raw_picks()`:
```python
def fetch_raw_picks(self):
    all_picks = []
    page = 1
    while True:
        url = f"{base_url}?page={page}"
        html = self._fetch_url(url)
        picks = self._parse_listing_html(url, html)
        if not picks:
            break
        all_picks.extend(picks)
        page += 1
    return all_picks
```

### JS-Rendered Content

Problem: Source uses JavaScript to load picks.

Solution: Use Playwright instead of requests:
```python
from playwright.sync_api import sync_playwright

def _fetch_url(self, url):
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(url)
        page.wait_for_selector(".pick-card")
        html = page.content()
        browser.close()
        return html
```

### Authentication

Problem: Source requires login.

Solution: Use session cookies:
```python
# Load saved auth state
storage_state = json.loads(Path("storage_state.json").read_text())

# Or use requests session
session = requests.Session()
session.cookies.update({"auth": "token"})
```

---

## Validation Commands

```bash
# Run all contract tests
python -m pytest tests/test_{source}_source.py::TestSourceContract -v

# Run schema validation tests
python -m pytest tests/test_{source}_source.py::TestRawPickSchema -v
python -m pytest tests/test_{source}_source.py::TestNormalizedPickSchema -v

# Run with captured fixtures
python -m pytest tests/test_{source}_source.py::TestParserWithFixtures -v

# Run integration tests (hits network)
python -m pytest tests/test_{source}_source.py::TestIntegration -v --run-integration

# Check for drift
python -c "
from src.source_contract import capture_schema_snapshot, detect_drift
from src.{source}_source import {Source}Source
source = {Source}Source()
picks, _, _ = source.ingest()
snapshot = capture_schema_snapshot('{source}', picks)
# Compare with baseline...
"
```

---

## Support

- Review existing sources: `action_ingest.py`, `covers_ingest.py`
- Schema definitions: `src/source_contract.py`
- Replay harness: `src/source_replay.py`
- Test examples: `tests/test_source_template.py`
