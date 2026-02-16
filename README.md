# Betting Aggregate – NBA Consensus Grading

## End-to-end workflow
From repo root:
1. Run consensus to produce normalized records and signals (example):
   ```
   python3 consensus_nba.py --debug
   ```
2. Build the signal ledger (dedupes reruns, keeps history):
   ```
   python3 scripts/build_signal_ledger.py
   ```
3. Fetch recent game results from The Odds API (requires `ODDS_API_KEY`):
   ```
   ODDS_API_KEY=your_key python3 scripts/fetch_game_results_oddsapi.py --days 3
   ```
4. Grade signals against results:
   ```
   python3 scripts/grade_signals.py
   ```
5. Produce summary reports:
   ```
   python3 scripts/report_records.py
   ```

Outputs land under `data/ledger/` and `data/reports/`.

## History + Grading Contract

The pipeline enforces a data contract to ensure deterministic, auditable signal tracking.

### Key Generation (src/signal_keys.py)

| Field | Description | Generated In |
|-------|-------------|--------------|
| `selection_key` | Stable hash identifying the semantic bet (day + market + player/team + stat + direction). Independent of line/odds. | Ledger builder |
| `offer_key` | Hash of selection_key + line + odds. Identifies a specific offer. | Ledger builder, Grader |
| `signal_id` | Legacy signal identifier from consensus stage. | Consensus |

### Ledger Fields (signals_latest.jsonl)

Required fields per the contract:
- `signal_id`, `signal_key`, `occurrence_id` - identification
- `selection_key`, `offer_key` - NEW: semantic bet and offer identifiers
- `event_key`, `day_key`, `away_team`, `home_team` - game identification
- `market_type`, `selection`, `direction`, `atomic_stat` - market details
- `line`, `odds`, `best_odds` - pricing (odds may be null)
- `supports[]` - array of supporting evidence from each source
- `score`, `sources_combo`, `count_total` - aggregation

### Grade Fields (grades_latest.jsonl)

Required fields per the contract:
- `signal_id`, `selection_key`, `offer_key` - identification
- `event_key`, `day_key`, `graded_at_utc` - when/what
- `status` - WIN/LOSS/PUSH/ERROR/INELIGIBLE/PENDING
- `result` - terminal outcome (WIN/LOSS/PUSH) or null
- `units` - profit on 1-unit bet (null if odds missing)
- `roi_eligible` - NEW: boolean, true only if odds present and confidence high
- `stat_value` - NEW: actual stat value for player props
- `player_match_confidence` - NEW: 0-1 score for player matching quality
- `provider`, `provider_game_id` - NEW: audit trail for data source

### ROI Eligibility Rules

A grade is ROI-eligible (`roi_eligible: true`) only when:
1. Status is terminal (WIN/LOSS/PUSH)
2. Odds are present (we NEVER silently assume -110)
3. For player props: player_match_confidence >= 0.75

### Historical Backfill CLI

```bash
# Include BetQL history
python3 scripts/build_signal_ledger.py \
  --include-betql-history \
  --history-start 2026-01-01 \
  --history-end 2026-01-31

# Include generic normalized JSONL files
python3 scripts/build_signal_ledger.py \
  --include-normalized-jsonl out_action_hist/normalized_action_nba_backfill.jsonl

# Multiple history sources
python3 scripts/build_signal_ledger.py \
  --include-betql-history \
  --history-start 2026-01-01 \
  --history-end 2026-01-31 \
  --include-normalized-jsonl out_action_hist/normalized_action_nba_backfill.jsonl
```

### Running Grading

```bash
# Grade all signals
python3 scripts/grade_signals_nba.py

# Grade only since a specific date
python3 scripts/grade_signals_nba.py --since 2026-01-20

# Debug mode with provider stats
python3 scripts/grade_signals_nba.py --debug
```

### Running Tests

```bash
# Run contract tests
python3 -m pytest tests/test_history_grading_contract.py -v

# Run all tests
python3 -m pytest tests/ -v
```
