# Betting Aggregate — System Context

**Last updated:** 2026-03-25
**Season:** 2025-26 NBA + NCAAB

---

## 1. Local Data State

### `data/ledger/signals_latest.jsonl` — 28,688 rows
One row per unique consensus signal (latest run per date). Primary scoring input.

| market_type | rows |
|-------------|------|
| player_prop | 23,883 |
| spread | 2,601 |
| total | 1,586 |
| moneyline | 617 |
| team_total | 1 |

Date range: NBA:2023:11:12 → NBA:2026:03:25

### `data/ledger/signals_occurrences.jsonl` — 444,686 rows
Full occurrence history (one row per source × signal × run). Used for cross-source merging and pattern analysis.

### `data/plays/plays_YYYY-MM-DD.json` — 385 days, 7,899 total plays
Scored and tiered plays output by `score_signals.py`.

| tier | total plays |
|------|-------------|
| A | 909 |
| B | 4,258 |
| C | 2,635 |
| D | 97 |

Date range: 2025-03-06 → 2026-03-25

---

## 2. Supabase Tables — Current State

### `signals` — ~48,414 rows
One row per consensus signal. Pushed from ledger via `push_to_supabase.py`.

**By market_type:**

| market_type | rows |
|-------------|------|
| player_prop | 32,161 |
| spread | 7,891 |
| total | 5,655 |
| moneyline | 2,547 |
| NULL / other | 160 |

Note: `sport` field on all rows is `NBA` due to backfill labeling; NCAAB signals are identified by their `day_key` prefix (`NCAAB:`).

---

### `grades` — ~48,412 rows
Strict 1:1 with `signals` (same signal_id primary key).

| result | rows |
|--------|------|
| WIN | 21,124 |
| LOSS | 20,099 |
| PUSH | 275 |
| NULL (pending/ungraded) | 6,914 |

Overall graded win rate: **51.2%** (21,124W / 41,223 W+L graded).

---

### `graded_occurrences` — ~84,559 rows
Source-level occurrence table. Primary input to `build_pattern_analysis.py`. More rows than `signals` because each signal can have multiple per-source occurrences.

**Conflict key:** `signal_id` (changed from `occurrence_id` on 2026-03-25 — `occurrence_id` was unstable/timestamp-derived).

Top sources by `occ_source_id`: betql (~38K), action (~13K), oddstrader (~3.5K), sportsline (~2.6K).

---

### `plays` — ~4,489+ rows
Scored and tiered plays pushed from `data/plays/`. Date range: 2025-03-06 → 2026-03-25.

---

### `signal_sources` — ~49,823 rows
Junction table: which source(s) fed each signal.

| source_id | rows |
|-----------|------|
| betql | 24,234 |
| action | 9,111 |
| bettingpros_experts | 6,022 |
| oddstrader | 3,575 |
| sportsline | 2,601 |
| dimers | 1,166 |
| covers | 1,012 |
| juicereel_sxebets | 680 |
| juicereel_nukethebooks | 631 |
| juicereel_unitvacuum | 617 |
| vegasinsider | 144 |

---

### `expert_records` — 593 rows
Aggregated W-L records per (source_id, expert_slug, market_type, atomic_stat). Rebuilt manually by `build_pattern_analysis.py` after major data corrections.

### `expert_pair_records` — 625 rows
Aggregated W-L for every (expert A, expert B) combination that appeared on the same signal.

### `sources` — 15 rows
| id | name | tier | active |
|----|------|------|--------|
| action | Action Network | community | ✓ |
| betql | BetQL | model | ✓ |
| bettingpros | BettingPros | community | ✓ |
| bettingpros_5star | BettingPros 5-Star | model | ✓ |
| bettingpros_experts | BettingPros Experts | expert | ✓ |
| covers | Covers | community | ✓ |
| dimers | Dimers | model | ✓ |
| dimers_ncaab | Dimers (NCAAB) | model | ✗ |
| juicereel_nukethebooks | nukethebooks (JuiceReel) | expert | ✓ |
| juicereel_sxebets | sXeBets (JuiceReel) | expert | ✓ |
| juicereel_unitvacuum | UnitVacuum (JuiceReel) | expert | ✓ |
| oddstrader | OddsTrader | model | ✓ |
| sportscapping | SportsCapping | expert | ✓ |
| sportsline | SportsLine | expert | ✓ |
| vegasinsider | Vegas Insider | expert | ✓ |

### `picks` — 0 rows
Table exists in schema but `push_to_supabase.py` does not currently populate it.

---

## 3. Daily Pipeline — Scripts in Order

### Pre-flight
| Script | Purpose |
|--------|---------|
| `scripts/verify_pipeline_auth.py` | Checks all scraper auth tokens before starting; warns on failures |

---

### NBA Pipeline

#### Step 1 — Token refresh (sequential, before parallel ingest)
| Script | Purpose |
|--------|---------|
| `scripts/refresh_sportsline_token.py` | Refreshes SportsLine session cookie/token |
| `scripts/refresh_dimers_token.py` | Refreshes Dimers session token |
| `scripts/refresh_betql_token.py` | Refreshes BetQL auth token |
| `scripts/refresh_juicereel_token.py` | Refreshes JuiceReel session token |

#### Step 2 — Ingest (parallel, each with watchdog timeout)
All scrapers run simultaneously; each writes to its own output file.

| Script | Timeout | Output |
|--------|---------|--------|
| `action_ingest.py` | 300s | `out/raw_action_nba.json` + `_meta.json` |
| `covers_ingest.py` | 300s | `out/raw_covers_nba.json` + `_meta.json` |
| `oddstrader_ingest.py --props` | 120s | `out/normalized_oddstrader_nba.json` + `out/normalized_oddstrader_prop_nba.json` |
| `sportsline_ingest.py` | 180s | `out/raw_sportsline_nba.json` |
| `dimers_ingest.py` | 180s | `out/raw_dimers_nba.json` |
| `scripts/probe_betql_spread_nba.py` | 120s | `out/raw_betql_spread_nba.json` |
| `scripts/probe_betql_totals_nba.py` | 120s | `out/raw_betql_totals_nba.json` |
| `scripts/probe_betql_sharp_nba.py` | 120s | `out/raw_betql_sharp_nba.json` |
| `scripts/probe_betql_prop_picks_nba.py` | 300s | `out/raw_betql_prop_nba.json` |
| `vegasinsider_ingest.py` | 180s | `out/raw_vegasinsider_nba.json` |
| `juicereel_ingest.py` | 180s | `out/normalized_juicereel_nba.json` |
| `bettingpros_ingest.py` | 180s | `out/raw_bettingpros_nba.json` |
| `scripts/bettingpros_experts_ingest.py` | 300s | `out/raw_bettingpros_experts_nba.json` |

Notes:
- Action and Covers write **incrementally** after each game page (via `IncrementalJsonWriter`). A mid-run timeout still produces valid partial output with a companion `_meta.json` showing `games_completed/games_total`.
- SportsLine, Dimers, BetQL, VegasInsider, JuiceReel, and BettingPros require storage state files and are silently skipped if absent.

#### Step 3 — Normalize (`scripts/normalize_sources_nba.py`)
Converts all raw scraped JSON to the standard normalized schema used by consensus. Runs freshness checks on Action and Covers: if a raw file's mtime is not today's ET date, it writes an empty `[]` and prints a loud warning.

Also normalizes: juicereel, betql (spread/totals/sharp/props), sportsline expert picks, bettingpros, bettingpros_experts.

**Validation gate** (inline in shell): counts records per source file; aborts if all sources return zero picks.

Normalized output schema uses nested sub-objects: `event` (event_key, away_team, home_team, day_key, sport), `market` (market_type, selection, direction, line, odds, stat_key), `provenance` (source_id, source_surface, expert_name, expert_handle, raw_block).

#### Step 4 — Consensus (`consensus_nba.py`)
Merges normalized picks across sources by matchup + market + direction + line proximity. Key behaviors:
- Spread signs encoded as `_neg`/`_pos` vote suffixes to prevent DET -1.5 and DET +1.5 being treated as the same vote.
- Player prop conflicts (same player, same stat, OVER+UNDER) detected and one side suppressed.
- Cross-source merges scoped to `(selection_key, day_key)` — prevents stale cross-date merges.
- Solo-source picks with `expert_strength >= 2 OR count_total >= 2` pass; others dropped.

**Pre-ledger pruning** (inline): deletes all but the most recent run directory per date from `data/runs/`.

#### Step 5 — Signal Ledger (`scripts/build_signal_ledger.py`)
Merges today's consensus signals with the full historical backfill. Writes:
- `data/ledger/signals_latest.jsonl` (28,688 rows)
- `data/ledger/signals_occurrences.jsonl` (444,686 rows)

Evicts stale occurrences from superseded runs (uses `latest_run_per_day` map from `data/runs/`, evicts if `run_id` < latest for that day).

Inputs include: today's consensus, BetQL history (2025-02-09+), Action backfill, OddsTrader history (2025-10-22+), Dimers backfill, JuiceReel backfill (sXeBets + nukethebooks), BettingPros experts history, SportsLine backfill, Covers backfill.

#### Steps 6–8 — Grading & Reports *(skipped in `--quick` mode)*

| Script | Purpose |
|--------|---------|
| `scripts/grade_signals_nba.py` | Fetches NBA scores from CDN/NBA API/Basketball-Reference; computes WIN/LOSS/PUSH per signal. Handles pre-graded picks (SportsLine, Dimers). Uses `signal_id` matching (not tuple matching) to prevent ambiguity. |
| `scripts/build_research_dataset_nba_occurrences.py` | Builds occurrence-level analysis dataset (one row per source×signal), joined with grades. |
| `scripts/build_research_dataset_nba.py` | Builds signal-level joined dataset (signals + grades). |
| `scripts/report_records.py` | Generates 18 JSON reports to `data/reports/`. |
| `scripts/report_trends.py` | Generates trend reports: by day of week, by month, by market type, consensus strength, consensus×market. |
| `scripts/report_recent_trends.py` | Generates rolling recent-trend reports and lookup table. |
| `scripts/discover_patterns.py` | Scans for new pattern candidates above Wilson threshold (non-fatal). |
| `scripts/validate_patterns.py --phase 1` | Checks pattern registry integrity against current grade data; warns on FAIL-level issues. |

#### Steps 9–12 — Scoring & Export

| Script | Purpose |
|--------|---------|
| `scripts/fetch_current_lines.py` | Fetches live market lines from The Odds API (requires `ODDS_API_KEY`). |
| `scripts/fetch_cdn_scoreboards.py` | Refreshes today's game schedule cache — ensures `filter_wrong_date()` has a complete game list before scoring. |
| `scripts/score_signals.py` | Applies pattern registry + Wilson scores. Assigns A/B/C tiers. Filters wrong-date signals, prop conflicts, stat exclusions. Writes `data/plays/plays_{DATE}.json`. |
| `scripts/audit_consensus.py` | Sanity-checks consensus output for anomalies (non-fatal). |
| `scripts/export_web_data.py` | Exports picks, history index, and all reports to `web/public/data/` (public) and `web/data/private/` (authenticated). |

---

### NCAAB Pipeline *(runs after NBA)*

Mirrors NBA pipeline with differences:
- **Sources:** Action, Covers, OddsTrader, SportsLine, Dimers, VegasInsider only (no BetQL, JuiceReel, BettingPros).
- **Score provider:** ESPN API (free, no auth) via `src/results/ncaab_provider_espn.py`.
- **Pattern registry:** `PATTERN_REGISTRY_NCAAB` is currently empty — no NCAAB patterns meet the Wilson ≥ 0.46 threshold yet.
- **Grading** runs every day (not skipped in quick mode), because NCAAB tournament games need same-day grading.

---

### Post-pipeline

| Script | Purpose |
|--------|---------|
| `scripts/push_to_supabase.py` | Pushes signals, grades, signal_sources, plays, and graded_occurrences to Supabase. Non-blocking — pipeline completes even if Supabase is unavailable. Delta filter on `graded_at_utc`. Conflict key for `graded_occurrences` is `signal_id`. |

---

### One-time / maintenance scripts

| Script | Purpose |
|--------|---------|
| `scripts/build_pattern_analysis.py` | Rebuilds `expert_records` and `expert_pair_records` tables. Run manually after major data corrections. |
| `scripts/fix_juicereel_event_keys.py` | One-time migration: corrects JuiceReel signals with wrong event_key team order. Supports `--dry-run`. |
| `scripts/generate_performance_report.py` | Generates `data/reports/performance_report_{DATE}.txt` from Supabase expert tables. |
| `scripts/backfill_betql_history.py` | Re-scrapes BetQL spread/totals/sharps for historical dates. |
| `scripts/normalize_betql_history.py` | Normalizes BetQL raw backfill files. Requires `--overwrite` to regenerate existing output files. |
| `scripts/backfill_*.py` | Various one-time historical backfill scripts (Dimers, Covers, SportsLine expert pages, etc.). |

---

## 4. Pattern Registry — Current State (73 patterns)

The pattern registry in `scripts/score_signals.py` (`PATTERN_REGISTRY`) drives all A/B tier assignments. As of 2026-03-25 there are **73 active patterns** (32 A-tier, 41 B-tier).

**Critical ordering rule:** Expert-specific patterns MUST be placed before generic source-combo patterns (e.g. `sportsline_solo_props_over`) in the registry. The registry uses **first-match-wins** — a generic pattern listed earlier will fire before a more specific expert pattern listed later.

**Solo-source pass rule (2026-03-25):** `filter_scorable()` allows solo-source picks through if:
1. The `sources_combo` is in `solo_a_tier_unconditional` (A-tier exact_combo with no team constraint), OR
2. The signal's `experts` list contains any name in `solo_expert_names` (all A-tier expert pattern names), OR
3. The signal matches a constrained pattern (B-tier or team-specific exact_combo).

Current `solo_expert_names`: Mike Barner, Larry Hartstein, Matt Severance, Prop Bet Guy, Kyle Murray, carmenciorra.

**Stat exclusions:**
- `EXCLUDED_STAT_TYPES` = `{steals, pts_reb, unknown}` — always dropped
- `EXCLUDED_STAT_DIR` = `{(pts_reb_ast, OVER)}` — pts+reb+ast OVER is 45.1% negative edge; reb_ast OVER was removed from this set (2026-03-25) because Mike Barner's reb_ast OVER is 63.8% A-tier
- `MAX_PROP_LINE` = 30.5 — props above this line are excluded

### A-tier patterns (32 total)

| Pattern ID | Record | Win% | n | Notes |
|------------|--------|------|---|-------|
| action_props_pts_reb_under | 144-52 | 73.5% | 196 | action pts+reb UNDER |
| action_solo_props_under | 399-257 | 60.8% | 656 | action any prop UNDER |
| betql_spread_okc | 85-36 | 70.2% | 121 | |
| betql_spread_lac | 70-28 | 71.4% | 98 | |
| betql_spread_bos | 63-33 | 65.6% | 96 | |
| betql_spread_min | 59-27 | 68.6% | 86 | |
| expert_kyle_murray_rebounds | 17-3 | 85.0% | 20 | small sample |
| expert_kyle_murray_pts_reb_under | 50-13 | 79.4% | 63 | |
| expert_larry_hartstein_assists_over | 20-6 | 76.9% | 26 | |
| expert_matt_severance_points_under | 27-9 | 75.0% | 36 | |
| expert_prop_bet_guy_pts_reb_ast_under | 34-16 | 68.0% | 50 | |
| expert_mike_barner_assists_over | 17-8 | 68.0% | 25 | |
| expert_prop_bet_guy_rebounds_over | 28-18 | 60.9% | 46 | |
| expert_mike_barner_reb_ast_over | 37-21 | 63.8% | 58 | reb_ast OVER unblocked 2026-03-25 |
| expert_mike_barner_points | 164-121 | 57.5% | 285 | |
| expert_prop_bet_guy_pts_ast_over | 33-24 | 57.9% | 57 | |
| bettingpros_experts_assists_over | 55-15 | 78.6% | 70 | |
| bettingpros_experts_rebounds_over | 66-31 | 68.0% | 97 | |
| bettingpros_experts_props | 381-207 | 64.8% | 588 | generic OVER |
| bettingpros_experts_moneyline | 107-57 | 65.2% | 164 | |
| expert_carmenciorra_moneyline | 63-34 | 65.0% | 97 | |
| expert_carmenciorra_total | 56-30 | 65.1% | 86 | |
| oddstrader_spread_sac | 41-8 | 83.7% | 49 | |
| oddstrader_spread_nop | 34-7 | 82.9% | 41 | |
| oddstrader_spread_ind | 26-7 | 78.8% | 33 | |
| oddstrader_spread_cha | 20-6 | 76.9% | 26 | |
| oddstrader_spread_mem | 21-7 | 75.0% | 28 | |
| oddstrader_spread_was | 31-12 | 72.1% | 43 | |
| oddstrader_spread_chi | 26-10 | 72.2% | 36 | |
| oddstrader_spread_mil | 18-8 | 69.2% | 26 | |
| oddstrader_props_rebounds_under | 22-10 | 68.8% | 32 | |
| oddstrader_props_assists_under | 23-11 | 67.6% | 34 | |

### B-tier patterns (selected highlights)

| Pattern ID | Record | Win% | n |
|------------|--------|------|---|
| action_oddstrader_moneyline | 46-16 | 74.2% | 62 |
| expert_markus_markets_pts_ast | 19-6 | 76.0% | 25 |
| expert_mjaybrod | 20-9 | 69.0% | 29 |
| oddstrader_ai_spread | 399-212 | 65.3% | 611 |
| action_props_assists_under | 59-30 | 66.3% | 89 |
| expert_matt_severance | 61-31 | 66.3% | 92 |
| nukethebooks_rebounds_under | 19-11 | 63.3% | 30 |
| oddstrader_props_reb_ast_under | 16-8 | 66.7% | 24 |
| betql_sxebets_props_under | 11-5 | 68.8% | 16 |
| sportsline_pts_reb_ast_under | 81-48 | 62.8% | 129 |
| betql_spread_det | 62-39 | 61.4% | 101 |
| betql_sportsline_spread | 209-143 | 59.4% | 352 |
| nukethebooks_solo_props_under | 100-69 | 59.2% | 169 |
| sportsline_solo_props_under | 189-130 | 59.2% | 319 |
| expert_scott_rickenbach_spread | 118-74 | 61.5% | 192 |
| betql_solo_props_under | 2832-2336 | 54.8% | 5172 |

**NCAAB registry:** Empty. Insufficient data until post-2026 tournament.

---

## 5. Scoring Pipeline — Key Logic (`scripts/score_signals.py`)

Flow: `load_signals()` → `filter_scorable()` → `filter_wrong_date()` → `apply_exclusion_filters()` → `score_signal()` → `build_output()`

### `filter_scorable()`
Passes signals that are either:
- Multi-source (≥2 sources), OR
- Solo-source matching an A-tier `exact_combo` unconditional pattern (e.g. `action`, `nukethebooks`), OR
- Solo-source from an A-tier expert (Mike Barner, Larry Hartstein, etc.), OR
- Solo-source matching a constrained B-tier or team-specific pattern

### `filter_wrong_date()`
Uses LGF cache → NBA API scoreboard cache to validate matchups. Returns:
- `None` if no cache → keep all (can't validate)
- Empty set if cache has 0 games → drop all (off day / All-Star break)
- Set of `(away, home)` tuples → keep only signals whose event_key matchup is in the set

### `score_signal()`
Builds a Wilson-lower-bound score from the `combo_market_stat` lookup table (primary), adjusted by:
- Recency trend (+/-3% over 14/30-day window)
- Expert adjustment (+1.5% for high-performing experts)
- Stat-type adjustment (+/-1% for known good/bad stats)
- Line-bucket adjustment (+/-1% for favorable line ranges)
- Pattern history override: if a pattern match has a better Wilson score than the combo lookup, uses pattern record

### Tier assignment
- **A-tier**: Wilson ≥ 0.490 AND matched pattern with `tier_eligible == "A"`
- **B-tier**: Wilson ≥ 0.460 AND matched pattern with `tier_eligible == "B"`, OR Wilson ≥ 0.490 with no pattern

---

## 6. Known Issues (Open)

### High priority

**`picks` table unpopulated**
The `picks` Supabase table (raw per-source picks per day) is defined in the schema but `push_to_supabase.py` does not write to it.

**NCAAB pattern registry empty**
`PATTERN_REGISTRY_NCAAB` has no entries. All NCAAB plays are untiered. Target: accumulate to n≥50 per group after the 2026 CBB tournament.

**OddsTrader NCAAB event_key date offset**
Some OddsTrader NCAAB signals have event_key date 1 day off from the actual game date (normalizer bug in OddsTrader NCAAB path).

### Medium priority

**`betql_totals` occasionally missing**
`probe_betql_totals_nba.py` has timed out on some runs (confirmed MISSING on 2026-03-24 source health report). Monitor each run.

**JuiceReel player prop day-only event_keys (84 signals)**
84 JuiceReel historical backfill signals have a day-only `event_key` (e.g. `NBA:2026:03:17`) with no matchup suffix. Already graded (77/84 resolved). Not retroactively fixable.

**`dimers_ncaab` source marked inactive**
The `dimers_ncaab` source row has `active=false` in Supabase. Dimers NCAAB is ingested but the source_id may not match.

---

## 7. Fixes Made This Session (Mar 25, 2026)

### `reb_ast OVER` unblocked from stat exclusion
**Problem:** `EXCLUDED_STAT_DIR` contained `("reb_ast", "OVER")` with comment "35-32 (52%), small sample — not reliable". This hard-excluded all reb_ast OVER signals before pattern matching, meaning `expert_mike_barner_reb_ast_over` (37-21, 63.8%) could never fire.
**Fix:** Removed `("reb_ast", "OVER")` from `EXCLUDED_STAT_DIR`. The Mike Barner A-tier pattern now fires correctly. Josh Giddey reb_ast OVER 18.5 correctly scored as #2 A-tier (Wilson=59.0%).

### Expert solo picks now pass `filter_scorable()`
**Problem:** `filter_scorable()` only allowed solo-source signals through if their `sources_combo` matched an `exact_combo` in the registry. Expert patterns use the `expert` key (not `exact_combo`), so sportsline solo picks from Mike Barner were being dropped before scoring even though a matching A-tier pattern existed.
**Fix:** Added `solo_expert_names` set in `filter_scorable()` — any solo-source signal whose `experts` list contains an A-tier expert name is allowed through.

### `upsert_occurrences()` conflict key changed to `signal_id`
**Problem:** `occurrence_id` is derived from a hash that includes the run timestamp — it changes on every run, causing Supabase upserts to always insert new rows instead of updating existing ones.
**Fix:** Changed conflict key in `src/supabase_writer.py` from `occurrence_id` to `signal_id`. Also added `CREATE UNIQUE INDEX IF NOT EXISTS go_signal_id ON graded_occurrences(signal_id)` in `scripts/supabase_schema.sql`.

### Pattern registry expanded to 73 patterns
Added 27 new patterns: 10 OddsTrader team spreads (SAC, NOP, IND, CHA, MEM, WAS, CHI, MIL A-tier; DAL, BKN B-tier), 5 OddsTrader prop patterns (rebounds UNDER, assists UNDER A-tier; reb_ast UNDER, pts_reb_ast UNDER, points UNDER B-tier), 6 new SportsLine expert patterns (Larry Hartstein assists OVER, Matt Severance points UNDER A-tier; Mike Barner assists OVER, reb_ast OVER, points A-tier; catch-all B-tier), 3 Prop Bet Guy patterns (pts_ast OVER, rebounds OVER, pts_reb_ast UNDER A-tier), nukethebooks rebounds UNDER B-tier, betql spread updates (OKC, BOS, MIN, LAC, DET).

**Critical fix:** Moved all SportsLine expert patterns to BEFORE `sportsline_solo_props_over` in the registry (first-match-wins — generic pattern was firing first).

---

## 8. Fixes Made This Week (Mar 16–24, 2026)

### BetQL spread selection bug — deleted + re-scraped (Mar 23–24)
**Problem:** `betql_extractors.py` (introduced Jan 28, 2026) used sign-based logic to determine team selection: `selection = away if pick_txt.startswith("+") else home`. Wrong — the `+` sign indicates underdog, not recommendation.
**Fix:** `_recommended_row_index()` detects the star-rating button row to determine which team row is recommended.
**Scope:** 303 betql-only spread signals (Jan 28–Mar 23, 2026) deleted + re-scraped. New betql-only spread W-L: **269-257-4 (51.1%)**.

### OddsTrader total `game_total` selection bug — deleted
**Problem:** OddsTrader normalizer stored `selection="GAME_TOTAL"` instead of `"OVER"` or `"UNDER"` for 864 of 1,026 total signals. Direction field was correct but `selection` is used for pattern matching.
**Fix:** Deleted 711 oddstrader-only `GAME_TOTAL` signals from Supabase + local ledger. Multi-source signals kept.

### Spread consensus sign bug
**Problem:** `build_standard_directional_consensus()` treated DET -1.5 and DET +1.5 as the same vote.
**Fix:** Vote keys suffixed with `_neg`/`_pos` for spreads. 27 corrupted mixed-sign signals deleted.

### Action/Covers incremental writing + timeout increase
Timeout increased from 120s → 300s. `IncrementalJsonWriter` flushes picks after each game page. SIGTERM handler closes JSON array and writes `_meta.json`.

### Freshness check for normalization
`_check_freshness()` in `normalize_sources_nba.py` validates raw file mtime against today's ET date. Stale → writes empty `[]`, prints warning.

---

## 9. Key Findings — Performance Report (2026-03-23)

Full report: `data/reports/performance_report_2026-03-23.txt`

### Top solo experts (min 20 picks, all markets)

| Expert | Source | W | L | Win% |
|--------|--------|---|---|------|
| VegasIsMyBitch | action | 59 | 31 | 65.6% |
| Mike Barner | sportsline | 63 | 40 | 61.2% |
| Markus Markets | action | 130 | 85 | 60.5% |
| OddsTrader AI | oddstrader | 1675 | 1420 | 54.1% 🧊 |

### Best by stat type

| Stat | Expert | Source | W | L | Win% |
|------|--------|--------|---|---|------|
| Rebounds | Kyle Murray | action | 17 | 3 | 85.0% |
| Assists | Larry Hartstein | sportsline | 20 | 6 | 76.9% |
| Points UNDER | Matt Severance | sportsline | 27 | 9 | 75.0% |
| pts_ast | Markus Markets | action | 19 | 6 | 76.0% |

### Best expert pairs (min 15 agreements)

| Tier | Pair | W | L | Win% |
|------|------|---|---|------|
| **A** | buckets_podcast + joe_dellera (action) | 13 | 8 | 61.9% |
| **A** | bryan_fonseca + buckets_podcast (action) | 12 | 8 | 60.0% |
| **B** | betql_model + sxebets | 19 | 14 | 57.6% |

---

## 10. Source Health — 2026-03-24 Run

Full report: `data/reports/source_health_2026-03-24.txt`

| Source | Total | Elig | Inelig | Odds% | Line% | Markets |
|--------|-------|------|--------|-------|-------|---------|
| action | 31 | 29 | 2 | 100% | 93% | prop:19 spread:8 total:3 |
| covers | 15 | 13 | 2 | 100% | 93% | prop:14 spread:1 |
| sportsline | 7 | 7 | 0 | 100% | 100% | prop:6 total:1 |
| dimers | 17 | 17 | 0 | 100% | 82% | prop:6 spread:4 total:4 ml:3 |
| betql_spread | 15 | 3 | 12 | 26% | 100% | spread:15 |
| **betql_totals** | **MISSING** | — | — | — | — | — |
| betql_sharp | 12 | 1 | 11 | 16% | 100% | spread:6 total:6 |
| betql_prop | 27 | 27 | 0 | 100% | 100% | prop:27 |
| oddstrader | 15 | 3 | 12 | 73% | 100% | total:7 ml:4 spread:4 |
| vegasinsider | 7 | 7 | 0 | 100% | 100% | spread:4 total:2 prop:1 |
| juicereel | 18 | 18 | 0 | 100% | 100% | prop:17 spread:1 |
| bettingpros | 1 | 1 | 0 | 100% | 100% | ml:1 |
| bettingpros_experts | 28 | 28 | 0 | 100% | 100% | prop:19 spread:5 ml:3 total:1 |

Note: OddsTrader ran successfully on 2026-03-25 (40 game picks + 71 props) after timing out on 2026-03-24.

---

*Re-run `scripts/generate_performance_report.py` and update sections 2, 9, and 10 after each major pipeline change or weekly data correction.*
