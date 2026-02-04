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
