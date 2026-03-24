-- Migration 001: Create juicereel_history_archive table
-- Run in Supabase SQL Editor before running run_archive_migration.py
-- Safe to re-run: uses IF NOT EXISTS on all CREATE statements.

-- ============================================================
-- juicereel_history_archive
--
-- Stores the ~6,256 stale JuiceReel history signals that were
-- pushed to Supabase during the March 4, 2026 large backfill but
-- are no longer in the local ledger (signal_ids changed after bug
-- fixes rebuilt the ledger).
--
-- Flat table: signal + grade columns merged into one row.
-- No FK to signals/grades — fully decoupled archive.
-- Grade columns are nullable (LEFT JOIN — some records have no grade).
--
-- DIRECTION RULE:
--   direction_known = TRUE  → direction is populated; safe for
--                             direction-level analysis (e.g. UNDER win rate)
--   direction_known = FALSE → direction is NULL (spread/moneyline/total picks
--                             where team/side was not parseable from JuiceReel's
--                             history table); use for market-type-level analysis
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

  -- Archive-specific columns
  direction_known BOOLEAN NOT NULL  -- TRUE=direction populated, FALSE=NULL direction
);

CREATE INDEX IF NOT EXISTS jha_day_key        ON juicereel_history_archive(day_key);
CREATE INDEX IF NOT EXISTS jha_player_key     ON juicereel_history_archive(player_key);
CREATE INDEX IF NOT EXISTS jha_market_type    ON juicereel_history_archive(market_type);
CREATE INDEX IF NOT EXISTS jha_direction_known ON juicereel_history_archive(direction_known);
CREATE INDEX IF NOT EXISTS jha_result         ON juicereel_history_archive(result);
CREATE INDEX IF NOT EXISTS jha_sources_combo  ON juicereel_history_archive(sources_combo);
CREATE INDEX IF NOT EXISTS jha_atomic_stat    ON juicereel_history_archive(atomic_stat);
