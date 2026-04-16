# Betting Aggregate — System Context

**Last updated:** 2026-04-06
**Scope:** Verified against the current repo, `run_daily.sh`, local ledgers, the checked-in Supabase schema/writer code, and the active web export paths.

---

## 1. What This Repo Is

A daily betting-signal pipeline for **NBA** and **NCAAB**. It scrapes expert picks from 10+ sources, normalizes them into a shared schema, builds multi-source consensus signals, grades them against real game outcomes, scores them with a pattern registry, and exports tiered plays to a web app backed by Supabase.

The high-level flow:

1. **Ingest** — scrape picks from each source (Action Network, BetQL, Covers, Dimers, OddsTrader, SportsLine, VegasInsider, JuiceReel, BettingPros)
2. **Normalize** — convert each source's format into a shared `event/market/provenance` schema
3. **Consensus** — merge picks across sources by matchup + market + direction + line proximity
4. **Ledger** — merge today's consensus into the full historical signal ledger (with backfill data)
5. **Grade** — fetch NBA/NCAAB scores and compute WIN/LOSS/PUSH per signal
6. **Score** — rank signals by Wilson lower bound, apply pattern registry, assign A/B/C/D tiers
7. **Export** — write public/private web data for the Next.js app
8. **Push** — sync signals, grades, plays, and occurrences to Supabase

The two main operational entry points:

- `run_daily.sh` — end-to-end pipeline (NBA + NCAAB)
- `scripts/` — individual pipeline, backfill, grading, reporting, and maintenance tasks

The web app lives in `web/` and consumes files exported by `scripts/export_web_data.py`.

---

## 2. Repo Layout

### Root-level ingest scripts

Each source has its own ingest script in the repo root:

| Script | Source | Auth |
|--------|--------|------|
| `action_ingest.py` | Action Network | none (public) |
| `covers_ingest.py` | Covers | none (public) |
| `oddstrader_ingest.py` | OddsTrader | none (public) |
| `sportsline_ingest.py` | SportsLine | storage state |
| `dimers_ingest.py` | Dimers | storage state |
| `vegasinsider_ingest.py` | VegasInsider | storage state |
| `juicereel_ingest.py` | JuiceReel | storage state |
| `bettingpros_ingest.py` | BettingPros | storage state |

Supporting root files: `consensus_nba.py`, `data.py`, `data_ncaab.py`, `event_resolution.py`, `mappings.py`, `models.py`, `store.py`, `utils.py`, `betql_utils.py`.

### scripts/

Core pipeline scripts called by `run_daily.sh`:

| Script | Purpose |
|--------|---------|
| `normalize_sources_nba.py` | Normalizes all source outputs into shared schema |
| `build_signal_ledger.py` | Merges consensus + backfill into historical ledger |
| `grade_signals_nba.py` | Grades signals against game results (NBA + NCAAB via `--sport`) |
| `score_signals.py` | Scores signals, applies pattern registry, assigns tiers |
| `export_web_data.py` | Writes public/private web data |
| `push_to_supabase.py` | Syncs local outputs to Supabase |
| `audit_consensus.py` | Sanity-checks consensus (non-fatal) |
| `verify_pipeline_auth.py` | Pre-flight auth token check |
| `fetch_current_lines.py` | Fetches live market lines from The Odds API |
| `fetch_cdn_scoreboards.py` | Refreshes game schedule cache |
| `discover_patterns.py` | Scans for new pattern candidates |
| `validate_patterns.py` | Checks pattern registry integrity |
| `report_records.py` | Generates 18+ JSON reports |
| `report_trends.py` | Trend reports by day/month/market |
| `report_recent_trends.py` | Rolling recent-trend reports |

Also contains: token refresh scripts (`refresh_*.py`), BetQL probe scripts (`probe_betql_*.py`), backfill scripts (`backfill_*.py`), storage state creators (`create_*_storage.py`), research dataset builders, and various one-off maintenance tools.

### scripts/analysis/

22 diagnostic and research scripts for auditing grades, pipeline integrity, source combos, pattern analysis, profitability, and Excel export. Not part of the daily pipeline.

### scripts/migrations/

Supabase SQL migrations and deployment scripts:
- `001_juicereel_history_archive.sql` — archive table for pre-2024 JuiceReel history
- `002_batch_update_expert_results.sql` — RPC function for bulk grade updates

### src/

| Directory | Contents |
|-----------|----------|
| `src/` root | Normalizers per source (`normalizer_*.py`), BetQL extractors, signal key generation, player alias mapping, Supabase writer |
| `src/results/` | Game score providers: NBA API, NBA CDN, Basketball-Reference, API-Sports, Data.NBA.net; ESPN for NCAAB |
| `src/ingest/` | BetQL backfill helpers (date picker, readiness checks) |

### web/

Next.js 16 + React 19 app with NextAuth + Supabase + Tailwind CSS:

- `web/app/` — App Router pages: picks (today + by-date + history), track record
- `web/components/` — PickCard, TierBadge, MarketTabs, ConsensusIndicator, PaywallPrompt, etc.
- `web/lib/` — auth, Supabase client, data fetching, Whop payment integration
- `web/public/data/` — exported public data
- `web/data/private/` — exported private/authenticated data

### data/

| Directory | Contents |
|-----------|----------|
| `data/ledger/` | Signal and grade JSONL files (latest + occurrences) |
| `data/ledger/ncaab/` | NCAAB-specific ledger files |
| `data/plays/` | Daily scored plays JSON (NBA) |
| `data/plays/ncaab/` | Daily scored plays JSON (NCAAB) |
| `data/reports/` | Generated JSON reports (40 NBA, 21 NCAAB) |
| `data/runs/` | Per-run consensus output directories |
| `data/runs_ncaab/` | NCAAB run directories |
| `data/cache/results/` | Score caches: LGF, NBA API scoreboard, CDN boxscores, ESPN |
| `data/history/` | Historical backfill data by source |
| `data/analysis/` | Graded signal/occurrence datasets |
| `data/latest/` | Latest snapshot files for quick access |

### tests/

36 test files covering ingestion, normalization, consensus (hard + soft), deduplication, event resolution, signal tracking, grading contracts, and source contracts.

---

## 3. Sources

The pipeline ingests from these sources daily:

| Source ID | Name | Type | Sport | Notes |
|-----------|------|------|-------|-------|
| `action` | Action Network | community | NBA, NCAAB | Expert consensus picks |
| `betql` | BetQL | model | NBA | Spreads, totals, sharps, player props (4 separate probes) |
| `covers` | Covers | community | NBA, NCAAB | Expert picks from DOM extraction |
| `dimers` | Dimers | model | NBA, NCAAB | AI model picks |
| `oddstrader` | OddsTrader | model | NBA, NCAAB | AI spread/total/prop picks |
| `sportsline` | SportsLine | expert | NBA, NCAAB | Named expert picks (Mike Barner, Prop Bet Guy, etc.) |
| `vegasinsider` | Vegas Insider | expert | NBA, NCAAB | Named expert picks |
| `juicereel_sxebets` | sXeBets (JuiceReel) | expert | NBA | Expert picks |
| `juicereel_nukethebooks` | nukethebooks (JuiceReel) | expert | NBA | Expert picks |
| `juicereel_unitvacuum` | UnitVacuum (JuiceReel) | expert | NBA | Expert picks |
| `bettingpros` | BettingPros | community | NBA | Consensus picks |
| `bettingpros_experts` | BettingPros Experts | expert | NBA | Pre-graded expert picks |
| `bettingpros_5star` | BettingPros 5-Star | model | NBA | High-confidence model picks |

Sources requiring Playwright browser automation need storage state files (`data/*_storage_state.json`). Token refresh scripts run before ingest.

---

## 4. Current Data Snapshot

All counts are from local files, not Supabase.

### NBA ledger

| File | Rows |
|------|------|
| `data/ledger/signals_latest.jsonl` | 43,838 |
| `data/ledger/signals_occurrences.jsonl` | 631,754 |
| `data/ledger/grades_latest.jsonl` | 43,838 |
| `data/ledger/grades_occurrences.jsonl` | 272,360 |
| `data/analysis/graded_signals_latest.jsonl` | 43,838 |
| `data/analysis/graded_occurrences_latest.jsonl` | 42,653 |

Signal day_key range: `NBA:2023:10:25` → `NBA:2026:11:19`

**Market type breakdown:**

| market_type | signals |
|-------------|---------|
| player_prop | 33,067 |
| spread | 5,324 |
| total | 3,134 |
| moneyline | 2,294 |
| team_total | 18 |
| 1st_half_spread | 1 |

**Grade status breakdown:**

| status | count |
|--------|-------|
| WIN | 15,234 |
| LOSS | 13,844 |
| ERROR | 13,066 |
| INELIGIBLE | 1,429 |
| PUSH | 223 |
| PENDING | 42 |

Graded win rate: **52.4%** (15,234W / 29,078 W+L)

The 13,066 ERROR grades are mostly `game_not_found` (12,125) from BetQL backfill signals with mismatched event keys.

### NBA plays

| Metric | Value |
|--------|-------|
| Play files | 394 |
| Total plays | 8,290 |
| Date range | 2025-03-06 → 2026-04-03 |
| A-tier | 943 |
| B-tier | 4,475 |
| C-tier | 2,770 |
| D-tier | 102 |

### NCAAB ledger

| File | Rows |
|------|------|
| `data/ledger/ncaab/signals_latest.jsonl` | 16,527 |
| `data/ledger/ncaab/signals_occurrences.jsonl` | 16,781 |
| `data/ledger/ncaab/grades_latest.jsonl` | 16,527 |
| `data/ledger/ncaab/grades_occurrences.jsonl` | 25,927 |

NCAAB grades: 2,522 WIN / 2,450 LOSS / 30 PUSH / 11,525 pending

### NCAAB plays

| Metric | Value |
|--------|-------|
| Play files | 23 |
| Total plays | 33 |
| Date range | 2026-02-21 → 2026-04-03 |
| B-tier | 14 |
| D-tier | 19 |

### Ledger caveat

The top-level `data/ledger/signals_latest.jsonl` is almost entirely NBA but contains 3 stray NCAAB day_keys and ~96 rows with missing day_keys. Treat `data/ledger/ncaab/` as the authoritative NCAAB ledger.

---

## 5. Daily Pipeline

Orchestrated by `run_daily.sh`. Supports flags: `--nba-only`, `--ncaab-only`, `--quick`, `--skip-ingest`, `--debug`.

### Pre-flight

`scripts/verify_pipeline_auth.py` — checks all scraper auth tokens, warns on expiry.

### NBA pipeline

| Step | Scripts | Parallelized |
|------|---------|-------------|
| 1. Token refresh | `refresh_sportsline_token.py`, `refresh_dimers_token.py`, `refresh_betql_token.py`, `refresh_juicereel_token.py` | sequential |
| 2. Ingest | All 13 scrapers (see Sources table) | parallel with per-source timeouts (120-300s) |
| 3. Normalize | `normalize_sources_nba.py` | — |
| 4. Validate | inline check — aborts if all sources empty | — |
| 5. Consensus | `consensus_nba.py` | — |
| 5b. Audit | `audit_consensus.py` (non-fatal) | — |
| 6. Prune runs | inline — keeps only latest run dir per date | — |
| 7. Build ledger | `build_signal_ledger.py` with 9 `--include-*` flags for backfill | — |
| 8. Grade | `grade_signals_nba.py` | — |
| 9. Research datasets + lines | `build_research_dataset_nba_occurrences.py`, `build_research_dataset_nba.py`, `fetch_current_lines.py`, `fetch_cdn_scoreboards.py` | parallel |
| 10. Reports | `report_records.py`, `report_trends.py`, `report_recent_trends.py` | parallel |
| 10b. Pattern checks | `discover_patterns.py`, `validate_patterns.py --phase 1 --fail-only` | sequential |
| 11. Score | `score_signals.py` | — |
| 12. Export | `export_web_data.py` | — |

Steps 8-10b are skipped in `--quick` mode.

### NCAAB pipeline

Runs after NBA. Sources: action, covers, oddstrader, sportsline, dimers, vegasinsider. Same flow: ingest → consensus → audit → ledger → grade → research datasets (parallel) → reports → score → export.

### Post-pipeline

`push_to_supabase.py` — non-blocking Supabase sync. Pipeline completes even if push fails.

---

## 6. Scoring and Pattern Registry

### How scoring works

`scripts/score_signals.py` implements the full scoring pipeline:

1. **`filter_scorable()`** — passes multi-source signals (2+) or solo-source signals matching an A/B-tier pattern or A-tier expert
2. **`filter_wrong_date()`** — validates matchups against LGF + NBA API scoreboard caches; drops signals for games not on today's schedule
3. **`apply_exclusion_filters()`** — drops excluded stat types and directions
4. **`score_signal()`** — computes Wilson lower bound from combo x market x stat lookup tables, then applies adjustments
5. **`build_output()`** — assigns tiers and writes plays file

### Wilson score adjustments

| Adjustment | Range | Trigger |
|------------|-------|---------|
| Recency (hot) | +3% | Recent 14d win% >= 60% |
| Recency (warm) | +2% | Recent 14d win% >= 55% |
| Recency (cold) | -2% | Recent 14d win% < 45% |
| Expert boost | +1.5% | High-performing named expert |
| Stat type | +/-1% | Known good/bad stat categories |
| Line bucket | +/-1% | Favorable/unfavorable line ranges |

### Tier assignment

- **A-tier**: Wilson >= 0.490 AND matched pattern with `tier_eligible == "A"`
- **B-tier**: Wilson >= 0.460 AND matched pattern with `tier_eligible == "B"`, OR Wilson >= 0.490 with no pattern
- **C-tier**: Wilson between floor and B threshold
- **D-tier**: Below floor

Confidence score: `(wilson - 0.46) / (0.65 - 0.46) * 100`, clamped to 0-100.

### Stat exclusions

- `EXCLUDED_STAT_TYPES` = `{steals, unknown, pts_reb}`
- `EXCLUDED_STAT_DIR` = `{(pts_reb_ast, OVER)}`

### NBA pattern registry

65 active patterns (28 A-tier, 37 B-tier) in `PATTERN_REGISTRY`.

**A-tier solo expert names** (signals from these experts pass `filter_scorable()` even as solo-source):
Mike Barner, Larry Hartstein, Matt Severance, Prop Bet Guy, Kyle Murray, carmenciorra, Bruce Marshall

**Pattern registry uses first-match-wins ordering.** Expert-specific patterns must be placed before generic source-combo catch-all patterns.

### NCAAB pattern registry

`PATTERN_REGISTRY_NCAAB` has 1 B-tier pattern (`ncaab_nukethebooks_total`). All other NCAAB plays come from the broader scoring engine without pattern matching.

---

## 7. Supabase Integration

### Write path

- `scripts/push_to_supabase.py` — orchestrates delta/full push
- `src/supabase_writer.py` — per-table upsert logic

### Tables written to

| Table | Source file | Conflict key |
|-------|------------|-------------|
| `signals` | `data/ledger/signals_latest.jsonl` | `signal_id` |
| `signal_sources` | derived from signals | `signal_id, source_id` |
| `grades` | `data/ledger/grades_latest.jsonl` | `signal_id` |
| `plays` | `data/plays/plays_{DATE}.json` | `date, signal_id` |
| `graded_occurrences` | `data/analysis/graded_occurrences_latest.jsonl` | `signal_id` |

### Delta mode

Default behavior pushes only rows newer than last successful push. State tracked in `data/ledger/.push_state.json`:
- Signals filtered by `observed_at_utc`
- Grades/occurrences filtered by `graded_at_utc`
- Plays always pushed for current day
- `--full` flag ignores state and pushes everything

### Schema

Defined in `scripts/supabase_schema.sql`:

**Tables:** sources, experts, signals, signal_sources, grades, plays, juicereel_history_archive, graded_occurrences, expert_records, expert_pair_records

**Views:** clean_graded_picks, expert_record, combo_record

**RPC:** `bulk_update_expert_results()` (migration 002) — batch grade updates on signal_sources. Writer falls back to per-row PATCH if RPC unavailable.

---

## 8. Web Export

`scripts/export_web_data.py` writes to:

| Path | Contents |
|------|----------|
| `web/data/private/picks_{DATE}.json` | Today's scored plays (all tiers) |
| `web/data/private/history/` | Historical picks by date + `index.json` |
| `web/data/private/ncaab/` | NCAAB private data |
| `web/public/data/reports/` | Sanitized performance reports |
| `web/public/data/reports/tier_performance.json` | Tier-level W-L summary |

This is the file-based interface between the Python pipeline and the Next.js app.

---

## 9. Key Concepts

### Signal lifecycle

**Normalized record** → a single pick from one source in the shared schema (`event/market/provenance` sub-objects).

**Consensus signal** → merged from 1+ normalized records sharing the same matchup + market + direction + line proximity. Has a `sources_combo` (e.g. `action|betql|dimers`), `signal_type` (solo_pick, hard_cross_source, soft_atomic, etc.), and `signal_id` (SHA-256 hash of signal_key).

**Ledger entry** → a consensus signal persisted in `signals_latest.jsonl` with all historical backfill merged. One row per unique signal (latest run per date).

**Occurrence** → one row per source x signal x run in `signals_occurrences.jsonl`. Used for cross-source analysis and pattern mining.

**Grade** → 1:1 with signals. Status: WIN/LOSS/PUSH/ERROR/INELIGIBLE/PENDING.

**Play** → a scored and tiered signal written to `plays_{DATE}.json` with Wilson score, pattern match, adjustments, and tier.

### Combo stats

Combo stat types (pts_reb, reb_ast, pts_ast, pts_reb_ast) are treated as their own atomic categories — they are NOT decomposed into component stats.

### Grade matching

Always use `signal_id` (unique) for grade lookups — never `(day_key, market_type, selection)` which is ambiguous for totals.

### Event keys

Canonical format: `NBA:YYYY:MM:DD:AWAY@HOME` (e.g. `NBA:2026:04:03:MIN@PHI`).

Legacy format (some backfill data): `NBA:YYYYMMDD:AWAY@HOME:HHMM`.

### Pre-graded picks

SportsLine and Dimers backfill, plus BettingPros Experts, come with results already attached. These are accepted verbatim by the grader (noted as `pregraded_by_*` in grade_notes).

---

## 10. Known Issues

### Game schedule in scoring

`load_game_schedule()` merges LGF cache (FINAL games only) with NBA API scoreboard cache (all scheduled/in-progress/final games). Fixed 2026-04-02 — previously LGF returned early without checking the scoreboard, causing signals for later games to be dropped when early games had already finished.

### BetQL backfill event keys

12,125 of the 13,066 ERROR grades are `game_not_found` from BetQL backfill signals with wrong event_key team pairings. These are data quality issues in the historical backfill, not grading bugs.

### Action Network listing URL

Updated 2026-04-03 from `https://www.actionnetwork.com/picks/game` (broken — no day-nav) to `https://www.actionnetwork.com/nba/picks/game` (working). Action backfill can only reach back to start of current season (Oct 21, 2025) via the listing page navigation.

### NCAAB pattern registry

Nearly empty (1 B-tier pattern). Insufficient graded data to build reliable patterns. Target: accumulate more data across future seasons.

### JuiceReel token management

JuiceReel tokens expire frequently (~1 day). Requires regular refresh via `scripts/refresh_juicereel_token.py --force`.

---

## 11. Fast Orientation

| Question | Where to look |
|----------|--------------|
| How does the daily pipeline run? | `run_daily.sh` |
| How are signals scored and tiered? | `scripts/score_signals.py` (PATTERN_REGISTRY + scoring logic) |
| How is the ledger built? | `scripts/build_signal_ledger.py` |
| How are games graded? | `scripts/grade_signals_nba.py` + `src/results/` providers |
| How does consensus work? | `consensus_nba.py` |
| How are sources normalized? | `scripts/normalize_sources_nba.py` + `src/normalizer_*.py` |
| What gets exported to the web? | `scripts/export_web_data.py` → `web/public/data/` + `web/data/private/` |
| What goes to Supabase? | `scripts/push_to_supabase.py` + `src/supabase_writer.py` |
| What's the DB schema? | `scripts/supabase_schema.sql` |
| Where are diagnostic tools? | `scripts/analysis/` |
| Where are tests? | `tests/` (36 files) |
