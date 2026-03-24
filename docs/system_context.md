# Betting Aggregate — System Context

**Last updated:** 2026-03-24
**Season:** 2025-26 NBA + NCAAB

---

## 1. Supabase Tables — Current State

### `signals` — 42,242 rows
One row per consensus signal (merged from picks across sources). The primary entity in the system.

| Field | Value |
|-------|-------|
| Total rows | 42,242 |
| Date range | NBA:2023:10:25 → NCAAB:2026:02:16 |

**By market_type:**

| market_type | rows |
|-------------|------|
| player_prop | 27,514 |
| spread | 7,245 |
| total | 5,084 |
| moneyline | 2,239 |
| NULL / other | 160 |

Note: `sport` field on all rows is `NBA` due to backfill labeling; NCAAB signals are identified by their `day_key` prefix (`NCAAB:`).

---

### `grades` — 42,242 rows
Strict 1:1 with `signals` (same signal_id primary key). One row per graded signal.

| result | rows |
|--------|------|
| WIN | 17,590 |
| LOSS | 17,488 |
| PUSH | 160 |
| NULL (pending) | 7,004 |

Overall graded win rate: **50.1%** (17,590W / 35,078 graded).

---

### `signal_sources` — 44,519 rows
Junction table: which source(s) fed each signal. A signal with 3 sources has 3 rows here.

**By source_id:**

| source_id | rows |
|-----------|------|
| betql | 23,382 |
| action | 9,226 |
| oddstrader | 3,611 |
| sportsline | 2,670 |
| bettingpros_experts | 1,264 |
| dimers | 1,247 |
| covers | 1,026 |
| juicereel_sxebets | 659 |
| juicereel_nukethebooks | 627 |
| juicereel_unitvacuum | 616 |
| vegasinsider | 149 |
| bettingpros_5star | 21 |
| bettingpros | 13 |
| sportscapping | 8 |

---

### `plays` — 4,588 rows
Scored and tiered daily plays output by `score_signals.py`.

| tier | rows |
|------|------|
| A | 496 |
| B | 2,603 |
| C | 1,485 |
| D | 4 |

Date range: 2025-03-06 → 2026-03-23.

---

### `expert_records` — 593 rows
Aggregated W-L records per (source_id, expert_slug, market_type, atomic_stat). Rebuilt nightly by `build_pattern_analysis.py`.

| market_type | rows |
|-------------|------|
| player_prop | 321 |
| spread | 119 |
| moneyline | 77 |
| total | 76 |

**50 rows** meet the threshold of win_pct ≥ 55% with n ≥ 20. Hot/cold flags are computed over a 30-day rolling window (requires ≥10 recent picks and ≥8pp deviation from career win rate). Currently: 8 hot, 5 cold.

---

### `expert_pair_records` — 625 rows
Aggregated W-L for every (expert A, expert B) combination that appeared on the same signal. Rebuilt nightly.

| tier | rows |
|------|------|
| A (≥60%, n≥15) | 0 |
| B (≥55%, n≥15) | 0 |
| C (≥50%, n≥15) | 3 |
| Unrated (n<15 or <50%) | 622 |

Note: These tier thresholds apply to the **per-market** rows. When aggregated across all markets in the performance report, 2 A-tier and 1 B-tier pairs emerge (buckets_podcast+joe_dellera at 61.9%, bryan_fonseca+buckets_podcast at 60.0%, betql_model+sxebets at 57.6%).

---

### `graded_occurrences` — 63,114 rows
Source-level occurrence table used for pattern analysis. More rows than `signals` because each signal can have multiple per-source occurrences. Primary input to `build_pattern_analysis.py`.

| result | rows |
|--------|------|
| WIN | 24,909 |
| LOSS | 25,320 |
| PUSH | 182 |
| NULL | 12,703 |

Top sources: betql (38,218), action (12,787), oddstrader (3,399), sportsline (2,647).

---

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

---

### `experts` — 203 rows

| source_id | count |
|-----------|-------|
| action | 67 |
| sportsline | 46 |
| vegasinsider | 30 |
| bettingpros_experts | 26 |
| covers | 19 |
| sportscapping | 8 |
| betql | 2 |
| juicereel_* | 3 (1 each) |
| dimers | 1 |
| oddstrader | 1 |

---

### `picks` — 0 rows
Table exists in schema for raw per-source normalized picks per day, but `push_to_supabase.py` does not currently populate it.

---

### `juicereel_history_archive` — 6,321 rows
Stale JuiceReel backfill signals from the Mar 4 large backfill that are no longer in the live ledger (signal_ids changed after bug fixes). Kept for reference. 3,200W / 3,045L / 66P.

---

## 2. Daily Pipeline — Scripts in Order

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

#### Step 1 — Ingest (parallel, each with watchdog timeout)
All scrapers run simultaneously; each writes to its own output file.

| Script | Timeout | Output |
|--------|---------|--------|
| `action_ingest.py` | 300s | `out/raw_action_nba.json` + `_meta.json` |
| `covers_ingest.py` | 300s | `out/raw_covers_nba.json` + `_meta.json` |
| `oddstrader_ingest.py --props` | 120s | `out/raw_oddstrader_nba.json` |
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

#### Step 2 — Normalize (`scripts/normalize_sources_nba.py`)
Converts all raw scraped JSON to the standard normalized schema used by consensus. Runs freshness checks on Action and Covers: if a raw file's mtime is not today's ET date, it writes an empty `[]` to the normalized output and prints a loud warning instead of re-normalizing stale data.

Also normalizes: juicereel, betql (spread/totals/sharp/props), sportsline expert picks, bettingpros, bettingpros_experts.

**Validation gate** (inline in shell): counts records per source file; aborts the entire pipeline if all sources return zero picks.

#### Step 3 — Consensus (`consensus_nba.py`)
Merges normalized picks across sources by matchup + market + direction + line proximity. Produces consensus signals. Key behaviors:
- Spread signs are encoded as `_neg`/`_pos` vote suffixes to prevent DET -1.5 and DET +1.5 from being treated as the same vote.
- Player prop conflicts (same player, same stat, OVER+UNDER) are detected and one side is suppressed.
- Cross-game source merges are scoped to `(selection_key, day_key)` to prevent stale historical merges.

**Pre-ledger pruning** (inline): deletes all but the most recent run directory per date from `data/runs/`.

#### Step 4 — Signal Ledger (`scripts/build_signal_ledger.py`)
Merges today's consensus signals with the full historical backfill. Inputs:
- Today's consensus output (`data/runs/{run_id}/`)
- BetQL history (`data/history/betql_normalized/`, from 2025-02-09)
- BetQL game props history (`data/history/betql_normalized/props/`)
- Action history
- OddsTrader history (from 2025-10-22)
- Dimers backfill
- JuiceReel backfill (sXeBets + nukethebooks)
- BettingPros experts history
- SportsLine backfill (expert picks + expert pages)
- Covers backfill
- Various other normalized JSONL files

Evicts stale occurrences from superseded runs. Writes `data/ledger/signals_latest.jsonl` and `data/ledger/grades_latest.jsonl`.

#### Steps 5–7 — Grading & Reports *(skipped in `--quick` mode)*

| Script | Purpose |
|--------|---------|
| `scripts/grade_signals_nba.py` | Fetches NBA scores from CDN/NBA API/Basketball-Reference; computes WIN/LOSS/PUSH per signal. Handles pre-graded picks (SportsLine, Dimers). Uses `signal_id` matching (not tuple matching) to prevent ambiguity. |
| `scripts/build_research_dataset_nba_occurrences.py` | Builds occurrence-level analysis dataset (one row per source×signal), joined with grades. |
| `scripts/build_research_dataset_nba.py` | Builds signal-level joined dataset (signals + grades). |
| `scripts/report_records.py` | Generates 18 JSON reports: `by_source_record`, `by_sources_combo_record`, `cross_tabulation`, `by_stat_type`, `by_line_bucket`, `by_score_bucket`, `by_expert`, `by_expert_record`, `coverage_summary`, and others. |
| `scripts/report_trends.py` | Generates trend reports: by day of week, by month, by market type, consensus strength, consensus×market. |
| `scripts/report_recent_trends.py` | Generates rolling recent-trend reports and lookup table. |
| `scripts/discover_patterns.py` | Scans for new pattern candidates above Wilson threshold (non-fatal). |
| `scripts/validate_patterns.py --phase 1` | Checks pattern registry integrity against current grade data; warns on FAIL-level issues. |

#### Steps 8–11 — Scoring & Export

| Script | Purpose |
|--------|---------|
| `scripts/fetch_current_lines.py` | Fetches live market lines from The Odds API (requires `ODDS_API_KEY`). |
| `scripts/fetch_cdn_scoreboards.py` | Refreshes today's game schedule cache from NBA CDN — ensures `filter_wrong_date()` has a complete game list before scoring. |
| `scripts/score_signals.py` | Applies the pattern registry + Wilson scores to today's signals. Assigns A/B/C tiers based on matched patterns. Filters wrong-date signals, prop conflicts, and exclusions. Writes `data/plays/plays_{DATE}.json`. |
| `scripts/audit_consensus.py` | Sanity-checks consensus output for anomalies (non-fatal). |
| `scripts/export_web_data.py` | Exports picks, history index, and all reports to `web/public/data/` (public) and `web/data/private/` (authenticated). |

---

### NCAAB Pipeline *(runs after NBA)*

Mirrors NBA pipeline with differences:
- **Sources:** Action, Covers, OddsTrader, SportsLine, Dimers, VegasInsider only (no BetQL, JuiceReel, BettingPros).
- **Score provider:** ESPN API (free, no auth) via `src/results/ncaab_provider_espn.py`.
- **Pattern registry:** `PATTERN_REGISTRY_NCAAB` is currently empty — no NCAAB patterns meet the Wilson ≥ 0.46 threshold yet. All NCAAB plays are untiered.
- **Grading** runs every day (not skipped in quick mode), because NCAAB tournament games need same-day grading.

---

### Post-pipeline

| Script | Purpose |
|--------|---------|
| `scripts/push_to_supabase.py` | Pushes signals, grades, signal_sources, plays, and graded_occurrences to Supabase. Non-blocking — pipeline completes even if Supabase is unavailable. |

---

### One-time / maintenance scripts

| Script | Purpose |
|--------|---------|
| `scripts/build_pattern_analysis.py` | Rebuilds `expert_records` and `expert_pair_records` tables. Run manually after major data corrections. |
| `scripts/fix_juicereel_event_keys.py` | One-time migration: corrects JuiceReel signals with wrong event_key team order (ATL@MEM vs MEM@ATL). Supports `--dry-run`. |
| `scripts/generate_performance_report.py` | Generates `data/reports/performance_report_{DATE}.txt` from Supabase expert tables. |
| `scripts/backfill_*.py` | Various one-time historical backfill scripts (BetQL, Dimers, Covers, SportsLine expert pages, etc.). |

---

## 3. Known Issues (Open)

### High priority

**JuiceReel player prop day-only event_keys**
84 JuiceReel player prop signals in Supabase have a day-only `event_key` (e.g. `NBA:2026:03:17`) with no matchup suffix and NULL `away_team`/`home_team`. These are from the historical backfill where the API response had no matchup metadata. They are already graded (77/84 have WIN/LOSS results) and are not fixable via team-order correction. These will remain as-is; the normalizer fix prevents new occurrences.

**`picks` table unpopulated**
The `picks` table (raw per-source picks per day) is defined in the schema but `push_to_supabase.py` does not write to it. Currently a no-op.

**NCAAB pattern registry empty**
`PATTERN_REGISTRY_NCAAB` in `score_signals.py` has no entries. All NCAAB plays are untiered (no A/B signals). Target: accumulate to n≥50 per group after the 2026 CBB tournament, then re-analyze.

**OddsTrader NCAAB event_key date offset**
Some OddsTrader NCAAB signals have an event_key date 1 day off from the actual game date (normalizer bug in the OddsTrader NCAAB path). Not yet fixed.

### Medium priority

**10 JuiceReel signals with game-not-in-cache skip**
10 signals have a valid event_key but the game isn't in the local scoreboard cache for that date (cache was captured before all games populated). These were skipped by `fix_juicereel_event_keys.py`. Could be corrected by refreshing caches for those 7 dates, but all are already graded player props so impact is low.

**`expert_pair_records` tier thresholds not yet reached**
No pair has reached A-tier (≥60%) or B-tier (≥55%) at the per-market grain stored in the table. The aggregated report shows 2 A-tier pairs, but the underlying table only has 3 C-tier rows. More historical data needed.

**`dimers_ncaab` source marked inactive**
The `dimers_ncaab` source row has `active=false` in Supabase. Dimers NCAAB is ingested via `dimers_ingest.py --sport NCAAB` but the source_id in the normalized output may not match the inactive source.

---

## 4. Fixes Made This Week

### Spread consensus sign bug (`consensus_nba.py`)
**Problem:** `build_standard_directional_consensus()` treated DET -1.5 and DET +1.5 as the same vote because `_normalize_standard_selection()` returned only the team code with no sign.
**Fix:** At vote accumulation, `vote_sel` is suffixed with `_neg` or `_pos` for spreads based on line sign. Suffix is stripped before writing to output fields. This prevented mixed-sign spread signals where one source picked the favorite and another picked the dog from being incorrectly merged into a single consensus signal.
**Impact:** 27 corrupted mixed-sign spread signals were identified in Supabase and deleted (grades → signal_sources → plays → signals in FK order, since CASCADE was not configured).

### Action/Covers timeout increase (`run_daily.sh`)
**Problem:** Action and Covers scrapers were hitting the 120s watchdog timeout. Both scrapers loop through individual game pages and need more time on high-game-count days.
**Fix:** Timeout increased to 300s for both `action_ingest.py` and `covers_ingest.py` in both NBA and NCAAB sections.

### Action/Covers incremental JSON writing (`action_ingest.py`, `covers_ingest.py`)
**Problem:** Both scrapers accumulated all picks in memory and wrote output only at the end. A mid-run timeout produced zero output, forcing a full re-run.
**Fix:** Added `IncrementalJsonWriter` class to `action_ingest.py`. Each game page's picks are flushed to disk immediately after scraping. A SIGTERM handler ensures the JSON array is properly closed and a `_meta.json` companion file is written even if killed by the watchdog. `covers_ingest.py` imports and uses the same class. A partial run now produces a valid (partial) JSON file.

### Freshness check for Action/Covers normalization (`scripts/normalize_sources_nba.py`)
**Problem:** If Action or Covers ingest failed silently, `normalize_sources_nba.py` would re-normalize the previous day's stale raw file and inject yesterday's picks into today's consensus.
**Fix:** `_check_freshness()` compares the raw file's mtime to today's ET date. If stale, normalization is skipped, an empty `[]` is written to the normalized output, and a warning is printed. Downstream pipeline steps see zero records from that source rather than stale data.

### JuiceReel matchup order fix (`src/normalizer_juicereel_nba.py`)
**Problem:** JuiceReel displays matchups as "Hawks @ Grizzlies" (visitor first), which parses to ATL@MEM. But the canonical NBA schedule order (from the NBA API scoreboard) is MEM@ATL (home team second). This caused JuiceReel signals to have incorrect event_keys that didn't match signals from other sources.
**Fix:** Added `_validate_matchup_order()` helper. After `_parse_matchup()` resolves team codes, the function calls `get_games_for_date()` from the NBA API scoreboard provider to find the canonical away@home order for that date. If reversed, teams are swapped and a warning is logged. On lookup failure, parsed order is kept with a warning.
**Migration:** `scripts/fix_juicereel_event_keys.py` was run to correct historical signals in Supabase: 826 signals were merged into correctly-keyed signals from other sources, 34 were corrected in place (JuiceReel-only signals), 94 were skipped (84 with day-only event_keys + 10 cache misses).

### JuiceReel backfill rebuild + expert records rebuild
After the event_key migration, `scripts/build_pattern_analysis.py` was re-run to rebuild `expert_records` and `expert_pair_records` with the corrected JuiceReel data merged into the right signals. Total rows after rebuild: 593 expert_records, 625 expert_pair_records.

---

## 5. Key Findings — Performance Report (2026-03-23)

Full report: `data/reports/performance_report_2026-03-23.txt`
Data: 32,062 graded expert-pick occurrences across 165 unique experts, Season 2025-26.

### Top solo experts (min 20 picks, all markets)

| Rank | Expert | Source | W | L | Win% | Note |
|------|--------|--------|---|---|------|------|
| 1 | John Feltman | action | 14 | 6 | 70.0% | props + spreads |
| 2 | VegasIsMyBitch | action | 59 | 31 | 65.6% | ML/spread/total |
| 3 | RankScore by Bet Labs | action | 30 | 16 | 65.2% | spread/total |
| 4 | Michael Fiddle | action | 15 | 8 | 65.2% | ML/spread/total |
| 5 | craig.r.trujillo | bettingpros_experts | 13 | 7 | 65.0% | ML/spread/total |
| 8 | Mike Barner | sportsline | 63 | 40 | 61.2% | ML + props |
| 9 | Markus Markets | action | 130 | 85 | 60.5% | props only |
| 19 | OddsTrader AI | oddstrader | 1675 | 1420 | 54.1% | 🧊 cold |

### Best by market

**Spreads:** VegasIsMyBitch (action) 75.0% (39W-13L); OddsTrader AI 65.3% over 611 picks
**Totals:** sXeBets (JuiceReel) 68.2% 🧊 cold; Brandon Kravitz (action) 67.9%
**Moneylines:** Bryan Fonseca (action) 73.3%; MyPicksWithNoResearch (bettingpros_experts) 72.7%
**Player props:** Mike Barner (sportsline) 76.2% (16W-5L); Markus Markets (action) 60.5% over 215 picks

### Best by stat type

| Stat | Expert | Source | W | L | Win% |
|------|--------|--------|---|---|------|
| Rebounds | Kyle Murray | action | 17 | 3 | **85.0%** |
| Assists | Jon Metler | covers | 10 | 2 | **83.3%** |
| pts_ast combo | Markus Markets | action | 19 | 6 | **76.0%** |
| Points | Mike Barner | sportsline | 10 | 3 | **76.9%** |
| pts_reb combo | Top Shelf Action | action | 8 | 3 | **72.7%** |

Notable: Kyle Murray on rebounds (85%) is an outlier worth monitoring — sample is small (n=20) but Wilson lower bound still suggests edge. Jon Metler on assists (10W-2L) similarly small but striking.

### Best expert pairs (min 15 agreements, aggregated across markets)

| Tier | Pair | W | L | Win% | Markets |
|------|------|---|---|------|---------|
| **A** | buckets_podcast + joe_dellera (action) | 13 | 8 | **61.9%** | player_prop |
| **A** | bryan_fonseca + buckets_podcast (action) | 12 | 8 | **60.0%** | player_prop |
| **B** | betql_model + sxebets | 19 | 14 | **57.6%** | prop/spread/total |
| C | kyle_murray + betql_model | 12 | 10 | 54.5% | player_prop |

### Volume vs accuracy tension
- **BetQL Model** is the most prolific source (17,291 picks) but sits at 48.6% — below breakeven. It appears in many pairs because of volume, not edge.
- **OddsTrader AI** is cold-flagged (🧊) despite a 54.1% career rate: recent 30-day window has dropped ≥8pp below career average. Use with caution on totals/spreads.
- **sXeBets** leads the totals category (68.2%) but is also cold-flagged — this is likely a regression-to-mean signal.

### Pattern registry status
- **3 active patterns** in `PATTERN_REGISTRY`: `five_source_total_under`, `four_source_total_under`, `five_source_total_over`
- **A-tier**: 5+ sources on totals (UNDER: 61.8% career; OVER: similar)
- **B-tier**: 4+ sources on totals
- Spreads are 42% in the plays pipeline and do not qualify for A-tier
- NCAAB registry: empty (insufficient data until post-tournament 2026)

---

*This document is auto-generated. Re-run `scripts/generate_performance_report.py` and update sections 1, 4, and 5 after each major pipeline change.*
