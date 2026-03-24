-- Betting Aggregate - Supabase Schema
-- Run this in the Supabase SQL editor to create all tables.
-- Safe to re-run: uses CREATE TABLE IF NOT EXISTS + CREATE INDEX IF NOT EXISTS.

-- ============================================================
-- sources: one row per distinct source_id
-- ============================================================
CREATE TABLE IF NOT EXISTS sources (
  id          TEXT PRIMARY KEY,           -- e.g. "action", "juicereel_nukethebooks"
  name        TEXT NOT NULL,              -- human-readable name
  sport       TEXT NOT NULL DEFAULT 'NBA',
  url         TEXT,
  tier        TEXT,                       -- "expert", "model", "community"
  active      BOOLEAN DEFAULT true
);

-- Seed data: all 14 active sources
INSERT INTO sources (id, name, sport, url, tier, active) VALUES
  ('action',                    'Action Network',           'NBA', 'https://www.actionnetwork.com', 'community', true),
  ('betql',                     'BetQL',                    'NBA', 'https://betql.co',              'model',     true),
  ('covers',                    'Covers',                   'NBA', 'https://www.covers.com',        'community', true),
  ('dimers',                    'Dimers',                   'NBA', 'https://www.dimers.com',        'model',     true),
  ('juicereel_nukethebooks',    'nukethebooks (JuiceReel)', 'NBA', 'https://app.juicereel.com',     'expert',    true),
  ('juicereel_sxebets',         'sXeBets (JuiceReel)',      'NBA', 'https://app.juicereel.com',     'expert',    true),
  ('juicereel_unitvacuum',      'UnitVacuum (JuiceReel)',   'NBA', 'https://app.juicereel.com',     'expert',    true),
  ('oddstrader',                'OddsTrader',               'NBA', 'https://www.oddstrader.com',    'model',     true),
  ('sportsline',                'SportsLine',               'NBA', 'https://www.sportsline.com',    'expert',    true),
  ('vegasinsider',              'Vegas Insider',            'NBA', 'https://www.vegasinsider.com',  'expert',    true),
  ('sportscapping',             'SportsCapping',            'NBA', 'https://www.sportscapping.com', 'expert',    true),
  ('bettingpros',               'BettingPros',              'NBA', 'https://www.bettingpros.com',   'community', true),
  ('bettingpros_experts',       'BettingPros Experts',      'NBA', 'https://www.bettingpros.com',   'expert',    true),
  ('bettingpros_5star',         'BettingPros 5-Star',       'NBA', 'https://www.bettingpros.com',   'model',     true),
  ('dimers_ncaab',              'Dimers (NCAAB)',           'NCAAB', 'https://www.dimers.com',      'model',     false)
ON CONFLICT (id) DO UPDATE SET
  name = EXCLUDED.name,
  active = EXCLUDED.active;

-- ============================================================
-- experts: individual handicappers within a source
-- ============================================================
CREATE TABLE IF NOT EXISTS experts (
  id           SERIAL PRIMARY KEY,
  source_id    TEXT REFERENCES sources(id),
  slug         TEXT NOT NULL,             -- "nukethebooks", "PicksOffice"
  display_name TEXT,
  profile_url  TEXT,
  UNIQUE (source_id, slug)
);

-- ============================================================
-- picks: one row per raw normalized pick per source per day
-- ============================================================
CREATE TABLE IF NOT EXISTS picks (
  id              BIGSERIAL PRIMARY KEY,
  source_id       TEXT REFERENCES sources(id),
  expert_id       INTEGER REFERENCES experts(id),
  day_key         TEXT NOT NULL,          -- "NBA:2026:03:17"
  event_key       TEXT,                   -- "NBA:2026:03:17:OKC@ORL" (nullable for older signals)
  sport           TEXT NOT NULL DEFAULT 'NBA',
  away_team       TEXT,
  home_team       TEXT,
  market_type     TEXT,                   -- "player_prop", "spread", "total", "moneyline"
  selection       TEXT,                   -- "NBA:jalen_suggs::pts_reb_ast::UNDER"
  side            TEXT,                   -- "OVER", "UNDER", "OKC"
  line            REAL,
  odds            INTEGER,
  player_key      TEXT,                   -- "NBA:jalen_suggs"
  stat_key        TEXT,                   -- "pts_reb_ast"
  unit_size       REAL,
  raw_pick_text   TEXT,
  raw_fingerprint TEXT UNIQUE,            -- deduplicate on fingerprint
  observed_at_utc TIMESTAMPTZ,
  eligible        BOOLEAN DEFAULT true,
  inelig_reason   TEXT
);
CREATE INDEX IF NOT EXISTS picks_day_key      ON picks(day_key);
CREATE INDEX IF NOT EXISTS picks_event_key    ON picks(event_key);
CREATE INDEX IF NOT EXISTS picks_source_id    ON picks(source_id);
CREATE INDEX IF NOT EXISTS picks_player_key   ON picks(player_key);
CREATE INDEX IF NOT EXISTS picks_expert_id    ON picks(expert_id);

-- ============================================================
-- signals: one row per consensus signal (merged from picks)
-- ============================================================
CREATE TABLE IF NOT EXISTS signals (
  signal_id       TEXT PRIMARY KEY,       -- SHA256, stable across runs
  day_key         TEXT,                   -- "NBA:2026:03:17" (nullable for some older signals)
  event_key       TEXT,                   -- "NBA:2026:03:17:OKC@ORL" (nullable for older signals)
  sport           TEXT DEFAULT 'NBA',
  away_team       TEXT,
  home_team       TEXT,
  market_type     TEXT,
  selection       TEXT,
  direction       TEXT,                   -- "OVER", "UNDER", team code
  line            REAL,
  line_min        REAL,
  line_max        REAL,
  atomic_stat     TEXT,                   -- "points", "rebounds", "pts_reb_ast"
  player_key      TEXT,
  stat_key        TEXT,
  sources_combo   TEXT,                   -- "action|betql|covers" (pipe-sorted)
  sources_count   INTEGER,
  signal_type     TEXT,                   -- "solo_pick", "hard_cross_source", "soft_atomic"
  score           INTEGER,
  run_id          TEXT,
  observed_at_utc TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS signals_day_key      ON signals(day_key);
CREATE INDEX IF NOT EXISTS signals_event_key    ON signals(event_key);
CREATE INDEX IF NOT EXISTS signals_player_key   ON signals(player_key);
CREATE INDEX IF NOT EXISTS signals_sources_combo ON signals(sources_combo);
CREATE INDEX IF NOT EXISTS signals_market_type  ON signals(market_type);

-- ============================================================
-- signal_sources: junction table — which source fed each signal
-- ============================================================
CREATE TABLE IF NOT EXISTS signal_sources (
  id                 BIGSERIAL PRIMARY KEY,
  signal_id          TEXT REFERENCES signals(signal_id) ON DELETE CASCADE,
  source_id          TEXT REFERENCES sources(id),
  expert_id          INTEGER REFERENCES experts(id),
  expert_slug        TEXT,                     -- denormalized for easy querying
  expert_name        TEXT,
  line               REAL,                     -- this source's specific line
  odds               INTEGER,
  raw_pick_text      TEXT,
  expert_result      TEXT,                     -- "WIN", "LOSS", "PUSH" for this source's specific line
  expert_graded_line NUMERIC                   -- the line used to grade this source (may differ from signal line)
);

-- ALTER TABLE statements to add new columns to an existing database:
-- ALTER TABLE signal_sources ADD COLUMN IF NOT EXISTS expert_result TEXT;
-- ALTER TABLE signal_sources ADD COLUMN IF NOT EXISTS expert_graded_line NUMERIC;
CREATE INDEX IF NOT EXISTS ss_signal_id  ON signal_sources(signal_id);
CREATE INDEX IF NOT EXISTS ss_source_id  ON signal_sources(source_id);
CREATE INDEX IF NOT EXISTS ss_expert_id  ON signal_sources(expert_id);
CREATE UNIQUE INDEX IF NOT EXISTS signal_sources_unique_support_row
ON signal_sources (
  signal_id, source_id,
  COALESCE(expert_id, -1),
  COALESCE(expert_slug, ''),
  COALESCE(raw_pick_text, ''),
  COALESCE(line, -1000000)
);

-- ============================================================
-- grades: one row per graded signal (1:1 with signals)
-- ============================================================
CREATE TABLE IF NOT EXISTS grades (
  signal_id       TEXT PRIMARY KEY REFERENCES signals(signal_id),
  day_key         TEXT NOT NULL,
  result          TEXT,                   -- "WIN", "LOSS", "PUSH", "PENDING", "INELIGIBLE"
  status          TEXT,
  line            REAL,
  odds            INTEGER,
  stat_value      REAL,                   -- actual player stat value (props)
  market_type     TEXT,
  selection       TEXT,
  direction       TEXT,
  player_key      TEXT,
  provider        TEXT,                   -- "nba_cdn", "lgf", "pregraded"
  graded_at_utc   TIMESTAMPTZ,
  notes           TEXT,                   -- "missing_odds", "unsupported_market"
  units           REAL                    -- profit/loss units (e.g. +0.97, -1.0)
);
CREATE INDEX IF NOT EXISTS grades_day_key    ON grades(day_key);
CREATE INDEX IF NOT EXISTS grades_result     ON grades(result);
CREATE INDEX IF NOT EXISTS grades_player_key ON grades(player_key);
CREATE INDEX IF NOT EXISTS grades_market     ON grades(market_type);

-- ============================================================
-- plays: scored/tiered daily plays
-- ============================================================
CREATE TABLE IF NOT EXISTS plays (
  id              BIGSERIAL PRIMARY KEY,
  date            DATE NOT NULL,          -- "2026-03-17"
  signal_id       TEXT REFERENCES signals(signal_id),
  tier            TEXT,                   -- "A", "B", "C"
  rank            INTEGER,
  wilson_score    REAL,
  composite_score REAL,
  matched_pattern TEXT,                   -- pattern id from PATTERN_REGISTRY
  summary         TEXT,
  factors         JSONB,                  -- [{dimension, lookup_key, win_pct, n, wilson_lower}]
  expert_detail   JSONB,                  -- {expert, win_pct, n, adjustment, role}
  UNIQUE (date, signal_id)
);
CREATE INDEX IF NOT EXISTS plays_date      ON plays(date);
CREATE INDEX IF NOT EXISTS plays_tier      ON plays(tier);
CREATE INDEX IF NOT EXISTS plays_signal_id ON plays(signal_id);

-- ============================================================
-- juicereel_history_archive: stale JuiceReel backfill signals
--
-- Stores the ~6,256 stale juicereel_history_history signals that
-- were pushed to Supabase during the March 4, 2026 large backfill
-- but are no longer in the local ledger (signal_ids changed after
-- bug fixes rebuilt the ledger with corrected hashing logic).
-- These are real NBA picks going back to Nov 2022.
--
-- Flat table: signal + grade columns merged into one row.
-- No FK to signals/grades — fully decoupled from the main tables.
-- Grade columns are nullable (LEFT JOIN — some records have no grade).
--
-- DIRECTION RULE:
--   direction_known = TRUE  → direction is populated; safe for any query
--                             including direction-level analysis (OVER/UNDER)
--   direction_known = FALSE → direction is NULL; spread/moneyline/total picks
--                             where team/side was not parseable from JuiceReel's
--                             history table. Include in market-type-level analysis
--                             ONLY — never filter or group by direction on these.
-- ============================================================
CREATE TABLE IF NOT EXISTS juicereel_history_archive (
  -- Signal columns (mirrors signals table)
  signal_id       TEXT PRIMARY KEY,
  day_key         TEXT,
  event_key       TEXT,
  sport           TEXT DEFAULT 'NBA',
  away_team       TEXT,
  home_team       TEXT,
  market_type     TEXT,
  selection       TEXT,
  direction       TEXT,
  line            REAL,
  line_min        REAL,
  line_max        REAL,
  atomic_stat     TEXT,
  player_key      TEXT,
  stat_key        TEXT,
  sources_combo   TEXT,
  sources_count   INTEGER,
  signal_type     TEXT,
  score           INTEGER,
  run_id          TEXT,
  observed_at_utc TIMESTAMPTZ,

  -- Grade columns (nullable: grade may not exist for every signal)
  result          TEXT,         -- 'WIN', 'LOSS', 'PUSH', 'PENDING', 'INELIGIBLE'
  grade_status    TEXT,         -- renamed from 'status' to avoid SQL keyword ambiguity
  grade_line      REAL,         -- line at grade time
  odds            INTEGER,
  stat_value      REAL,         -- actual player stat value (props)
  provider        TEXT,         -- 'pregraded_by_juicereel_sxebets', etc.
  graded_at_utc   TIMESTAMPTZ,
  grade_notes     TEXT,
  units           REAL,         -- profit/loss units

  -- Archive-specific column
  direction_known BOOLEAN NOT NULL  -- TRUE=direction populated, FALSE=NULL direction
);

CREATE INDEX IF NOT EXISTS jha_day_key         ON juicereel_history_archive(day_key);
CREATE INDEX IF NOT EXISTS jha_player_key      ON juicereel_history_archive(player_key);
CREATE INDEX IF NOT EXISTS jha_market_type     ON juicereel_history_archive(market_type);
CREATE INDEX IF NOT EXISTS jha_direction_known ON juicereel_history_archive(direction_known);
CREATE INDEX IF NOT EXISTS jha_result          ON juicereel_history_archive(result);
CREATE INDEX IF NOT EXISTS jha_sources_combo   ON juicereel_history_archive(sources_combo);
CREATE INDEX IF NOT EXISTS jha_atomic_stat     ON juicereel_history_archive(atomic_stat);

-- ============================================================
-- Useful views for expert analysis
-- ============================================================

-- ============================================================
-- clean_graded_picks: unified graded picks for win rate analysis.
-- Combines the current ledger (signals + grades) with the
-- juicereel_history_archive (pre-2024 JuiceReel backfill data).
--
-- DIRECTION RULE (enforced by direction_known column):
--   Records from juicereel_history_archive where direction_known = FALSE
--   have NULL direction — spread/moneyline/total picks where the
--   team/side was not parseable from JuiceReel's history table.
--   These MUST be excluded from any query that groups or filters by
--   direction (e.g. "expert total UNDER record").
--   They ARE included for market-type-level analysis (e.g. "expert
--   total record") because the market type and result are known.
--
--   Rule summary:
--     WHERE direction_known = TRUE  → safe for direction-level queries
--     (no filter on direction_known) → market-type-level queries only
-- ============================================================
CREATE OR REPLACE VIEW clean_graded_picks AS
-- Current ledger picks (all have known direction where applicable)
SELECT
  s.signal_id,
  s.day_key,
  s.event_key,
  s.sport,
  s.market_type,
  s.selection,
  s.direction,
  s.line,
  s.atomic_stat,
  s.player_key,
  s.stat_key,
  s.sources_combo,
  s.sources_count,
  s.signal_type,
  s.score,
  g.result,
  g.stat_value,
  g.provider,
  g.graded_at_utc,
  g.units,
  -- All current-ledger records have known direction (avoid_conflict records
  -- have direction=NULL but result=NULL, so they never appear here anyway)
  TRUE                AS direction_known,
  'current_ledger'    AS data_source
FROM signals s
JOIN grades g ON s.signal_id = g.signal_id
WHERE g.result IN ('WIN', 'LOSS', 'PUSH')

UNION ALL

-- Archived JuiceReel history picks (pregraded, Nov 2022 → Mar 2026)
-- NOTE: rows where direction_known = FALSE are included here so that
-- market-type-level aggregations pick them up. Any query that groups
-- or filters by direction MUST add: WHERE direction_known = TRUE
SELECT
  jha.signal_id,
  jha.day_key,
  jha.event_key,
  jha.sport,
  jha.market_type,
  jha.selection,
  jha.direction,
  jha.line,
  jha.atomic_stat,
  jha.player_key,
  jha.stat_key,
  jha.sources_combo,
  jha.sources_count,
  jha.signal_type,
  jha.score,
  jha.result,
  jha.stat_value,
  jha.provider,
  jha.graded_at_utc,
  jha.units,
  jha.direction_known,
  'juicereel_archive' AS data_source
FROM juicereel_history_archive jha
WHERE jha.result IN ('WIN', 'LOSS', 'PUSH');

-- ============================================================
-- expert_record: win/loss summary per expert per market type.
-- Includes current ledger (via signal_sources join) and
-- JuiceReel archive (solo-source history records).
--
-- DIRECTION RULE: direction_known is propagated through this view.
-- For direction-level analysis, callers should use clean_graded_picks
-- directly with WHERE direction_known = TRUE rather than this view.
-- ============================================================
DROP VIEW IF EXISTS expert_record;
CREATE VIEW expert_record AS
-- Current ledger: join through signal_sources to get expert_slug
SELECT
  ss.source_id,
  ss.expert_slug,
  s.market_type,
  s.atomic_stat,
  s.direction,
  COUNT(*)                                                              AS n,
  SUM(CASE WHEN g.result = 'WIN'  THEN 1 ELSE 0 END)                  AS wins,
  SUM(CASE WHEN g.result = 'LOSS' THEN 1 ELSE 0 END)                  AS losses,
  ROUND(AVG(CASE WHEN g.result IN ('WIN','LOSS') THEN
    CASE WHEN g.result = 'WIN' THEN 1.0 ELSE 0.0 END END), 3)         AS win_pct,
  TRUE                                                                  AS direction_known,
  'current_ledger'                                                      AS data_source
FROM signal_sources ss
JOIN signals s  ON ss.signal_id = s.signal_id
JOIN grades   g ON s.signal_id  = g.signal_id
WHERE g.result IN ('WIN', 'LOSS')
GROUP BY ss.source_id, ss.expert_slug, s.market_type, s.atomic_stat, s.direction

UNION ALL

-- JuiceReel archive: source derived from sources_combo (always solo-source for history)
-- expert_slug is NULL — JuiceReel history records don't carry individual expert metadata
SELECT
  jha.sources_combo                                                     AS source_id,
  NULL                                                                  AS expert_slug,
  jha.market_type,
  jha.atomic_stat,
  jha.direction,
  COUNT(*)                                                              AS n,
  SUM(CASE WHEN jha.result = 'WIN'  THEN 1 ELSE 0 END)                AS wins,
  SUM(CASE WHEN jha.result = 'LOSS' THEN 1 ELSE 0 END)                AS losses,
  ROUND(AVG(CASE WHEN jha.result IN ('WIN','LOSS') THEN
    CASE WHEN jha.result = 'WIN' THEN 1.0 ELSE 0.0 END END), 3)       AS win_pct,
  jha.direction_known,
  'juicereel_archive'                                                   AS data_source
FROM juicereel_history_archive jha
WHERE jha.result IN ('WIN', 'LOSS')
GROUP BY jha.sources_combo, jha.market_type, jha.atomic_stat, jha.direction, jha.direction_known;

-- ============================================================
-- combo_record: win/loss summary per sources_combo per market.
-- Includes current ledger and JuiceReel archive.
--
-- DIRECTION RULE: archive rows where direction_known = FALSE are
-- excluded from this view because it groups by direction. They
-- appear in expert_record (market-type-level) instead.
-- ============================================================
DROP VIEW IF EXISTS combo_record;
CREATE VIEW combo_record AS
-- Current ledger
SELECT
  s.sources_combo,
  s.market_type,
  s.atomic_stat,
  s.direction,
  COUNT(*)                                                              AS n,
  SUM(CASE WHEN g.result = 'WIN'  THEN 1 ELSE 0 END)                  AS wins,
  SUM(CASE WHEN g.result = 'LOSS' THEN 1 ELSE 0 END)                  AS losses,
  ROUND(AVG(CASE WHEN g.result = 'WIN' THEN 1.0 ELSE 0.0 END), 3)    AS win_pct,
  'current_ledger'                                                      AS data_source
FROM signals s
JOIN grades g ON s.signal_id = g.signal_id
WHERE g.result IN ('WIN', 'LOSS')
GROUP BY s.sources_combo, s.market_type, s.atomic_stat, s.direction

UNION ALL

-- JuiceReel archive — direction_known = FALSE excluded (direction is NULL,
-- grouping by direction would create a misleading NULL bucket)
SELECT
  jha.sources_combo,
  jha.market_type,
  jha.atomic_stat,
  jha.direction,
  COUNT(*)                                                              AS n,
  SUM(CASE WHEN jha.result = 'WIN'  THEN 1 ELSE 0 END)                AS wins,
  SUM(CASE WHEN jha.result = 'LOSS' THEN 1 ELSE 0 END)                AS losses,
  ROUND(AVG(CASE WHEN jha.result = 'WIN' THEN 1.0 ELSE 0.0 END), 3)  AS win_pct,
  'juicereel_archive'                                                   AS data_source
FROM juicereel_history_archive jha
WHERE jha.result IN ('WIN', 'LOSS')
  AND jha.direction_known = TRUE   -- exclude null-direction records from direction-grouped view
GROUP BY jha.sources_combo, jha.market_type, jha.atomic_stat, jha.direction;

-- ============================================================
-- graded_occurrences: one row per source-level occurrence with
-- grade result. This is the primary table for pattern analysis —
-- each row represents one source's pick (e.g. action's occurrence
-- of a signal that action+betql both had).
--
-- Key fields for pattern queries:
--   occ_sources_combo   = this occurrence's source ("action", "betql")
--   signal_sources_combo = all sources in the merged signal ("action|betql")
--   signal_sources_present = array of source ids in merged signal
--   result              = "WIN" | "LOSS" | "PUSH"
--   expert_name         = individual expert within a source (nullable)
--
-- Indexed for fast GROUP BY on the most common pattern dimensions.
-- ============================================================
CREATE TABLE IF NOT EXISTS graded_occurrences (
  occurrence_id         TEXT PRIMARY KEY,         -- SHA256, stable
  signal_id             TEXT,                     -- parent signal
  day_key               TEXT NOT NULL,            -- "NBA:2026:03:17"
  event_key             TEXT,                     -- "NBA:2026:03:17:OKC@ORL"
  sport                 TEXT NOT NULL DEFAULT 'NBA',
  away_team             TEXT,
  home_team             TEXT,
  market_type           TEXT,                     -- "player_prop","spread","total","moneyline"
  direction             TEXT,                     -- "OVER","UNDER", team code
  line                  REAL,
  atomic_stat           TEXT,                     -- "points","rebounds","pts_reb_ast"
  player_key            TEXT,
  stat_key              TEXT,
  occ_source_id         TEXT,                     -- this occurrence's source ("action")
  occ_sources_combo     TEXT,                     -- same as occ_source_id for single-source
  signal_sources_combo  TEXT,                     -- all sources ("action|betql")
  signal_sources_present TEXT[],                  -- array form of signal_sources_combo
  sources_count         INTEGER,                  -- number of sources in merged signal
  expert_name           TEXT,                     -- individual handicapper (nullable)
  result                TEXT,                     -- "WIN","LOSS","PUSH","PENDING","INELIGIBLE"
  units                 REAL,                     -- profit/loss units
  score                 INTEGER,
  signal_type           TEXT,
  run_id                TEXT,
  observed_at_utc       TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS go_day_key             ON graded_occurrences(day_key);
CREATE INDEX IF NOT EXISTS go_result              ON graded_occurrences(result);
CREATE INDEX IF NOT EXISTS go_market_type         ON graded_occurrences(market_type);
CREATE INDEX IF NOT EXISTS go_occ_source_id       ON graded_occurrences(occ_source_id);
CREATE INDEX IF NOT EXISTS go_signal_sources_combo ON graded_occurrences(signal_sources_combo);
CREATE INDEX IF NOT EXISTS go_atomic_stat         ON graded_occurrences(atomic_stat);
CREATE INDEX IF NOT EXISTS go_direction           ON graded_occurrences(direction);
CREATE INDEX IF NOT EXISTS go_expert_name         ON graded_occurrences(expert_name);
CREATE INDEX IF NOT EXISTS go_player_key          ON graded_occurrences(player_key);
CREATE INDEX IF NOT EXISTS go_signal_id           ON graded_occurrences(signal_id);

-- ============================================================
-- expert_records: aggregated W-L record per expert per market.
-- Rebuilt nightly by scripts/build_pattern_analysis.py.
-- One row per (source_id, expert_slug, market_type, atomic_stat).
-- atomic_stat is '' (empty string) for non-prop markets.
-- ============================================================
CREATE TABLE IF NOT EXISTS expert_records (
  source_id     TEXT    NOT NULL,
  expert_slug   TEXT    NOT NULL,
  expert_name   TEXT,                    -- display name (most recent)
  market_type   TEXT    NOT NULL,        -- "player_prop","spread","total","moneyline"
  atomic_stat   TEXT    NOT NULL DEFAULT '',  -- "points","rebounds","" for non-props
  n             INTEGER NOT NULL DEFAULT 0,
  wins          INTEGER NOT NULL DEFAULT 0,
  losses        INTEGER NOT NULL DEFAULT 0,
  pushes        INTEGER NOT NULL DEFAULT 0,
  win_pct       REAL,                    -- wins / (wins+losses), NULL if n=0
  wilson_lower  REAL,                    -- Wilson 95% lower bound
  hot           BOOLEAN NOT NULL DEFAULT FALSE,  -- last_14_win_pct >= 0.60 AND last_14_n >= 5
  cold          BOOLEAN NOT NULL DEFAULT FALSE,  -- last_14_win_pct <= 0.40 AND last_14_n >= 5
  last_14_n     INTEGER NOT NULL DEFAULT 0,
  last_14_wins  INTEGER NOT NULL DEFAULT 0,
  updated_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS expert_records_pk
  ON expert_records (source_id, expert_slug, market_type, atomic_stat);

CREATE INDEX IF NOT EXISTS er_market_type  ON expert_records(market_type);
CREATE INDEX IF NOT EXISTS er_atomic_stat  ON expert_records(atomic_stat);
CREATE INDEX IF NOT EXISTS er_wilson       ON expert_records(wilson_lower);
CREATE INDEX IF NOT EXISTS er_hot_cold     ON expert_records(hot, cold);

-- ALTER TABLE for live DBs that already exist:
-- CREATE TABLE expert_records ... (run full DDL above)

-- ============================================================
-- expert_pair_records: aggregated W-L for pairs of experts.
-- One row per (source_a, slug_a, source_b, slug_b, market_type, atomic_stat).
-- slug_a < slug_b alphabetically (enforced in build_pattern_analysis.py).
-- ============================================================
CREATE TABLE IF NOT EXISTS expert_pair_records (
  source_a      TEXT    NOT NULL,
  expert_slug_a TEXT    NOT NULL,
  source_b      TEXT    NOT NULL,
  expert_slug_b TEXT    NOT NULL,
  market_type   TEXT    NOT NULL,
  atomic_stat   TEXT    NOT NULL DEFAULT '',
  n             INTEGER NOT NULL DEFAULT 0,
  wins          INTEGER NOT NULL DEFAULT 0,
  losses        INTEGER NOT NULL DEFAULT 0,
  pushes        INTEGER NOT NULL DEFAULT 0,
  win_pct       REAL,
  wilson_lower  REAL,
  tier          TEXT,                    -- "A","B",NULL — A>=0.52 AND n>=20; B>=0.46 AND n>=10
  updated_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS expert_pair_records_pk
  ON expert_pair_records (source_a, expert_slug_a, source_b, expert_slug_b, market_type, atomic_stat);

CREATE INDEX IF NOT EXISTS epr_market_type  ON expert_pair_records(market_type);
CREATE INDEX IF NOT EXISTS epr_atomic_stat  ON expert_pair_records(atomic_stat);
CREATE INDEX IF NOT EXISTS epr_tier         ON expert_pair_records(tier);
CREATE INDEX IF NOT EXISTS epr_wilson       ON expert_pair_records(wilson_lower);
