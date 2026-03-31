-- Migration 002: Create bulk_update_expert_results Postgres function
-- Replaces the per-row PATCH loop in upsert_grades() (supabase_writer.py lines 250-276)
-- with a single RPC call that does one UPDATE ... FROM jsonb_to_recordset().
--
-- Safe to re-run: CREATE OR REPLACE won't fail if function already exists.
--
-- Matching logic mirrors the Python filter chain in upsert_grades():
--   signal_id  = input.signal_id
--   source_id  = input.source_id
--   COALESCE(expert_slug, '')        = COALESCE(input.expert_slug, '')
--   COALESCE(line,       -1000000)   = COALESCE(input.expert_graded_line, -1000000)
--
-- The COALESCE(-1000000) sentinel matches the unique functional index definition
-- on signal_sources so we land on exactly one row per update.
--
-- Parameters:
--   updates  JSONB  — array of objects, each with:
--     signal_id          TEXT
--     source_id          TEXT
--     expert_slug        TEXT  (nullable)
--     expert_result      TEXT  — 'WIN' | 'LOSS' | 'PUSH'
--     expert_graded_line NUMERIC (nullable)
--
-- Returns: integer count of rows updated.

CREATE OR REPLACE FUNCTION bulk_update_expert_results(updates JSONB)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  rows_updated INTEGER;
BEGIN
  WITH input AS (
    SELECT
      (rec->>'signal_id')                        AS signal_id,
      (rec->>'source_id')                        AS source_id,
      COALESCE(rec->>'expert_slug', '')           AS expert_slug_coalesced,
      (rec->>'expert_result')                    AS expert_result,
      CASE
        WHEN rec->>'expert_graded_line' IS NULL THEN NULL
        ELSE (rec->>'expert_graded_line')::NUMERIC
      END                                        AS expert_graded_line
    FROM jsonb_array_elements(updates) AS rec
  )
  UPDATE signal_sources ss
  SET
    expert_result      = input.expert_result,
    expert_graded_line = input.expert_graded_line
  FROM input
  WHERE
    ss.signal_id = input.signal_id
    AND ss.source_id = input.source_id
    AND COALESCE(ss.expert_slug, '')      = input.expert_slug_coalesced
    AND COALESCE(ss.line, -1000000)       = COALESCE(input.expert_graded_line, -1000000);

  GET DIAGNOSTICS rows_updated = ROW_COUNT;
  RETURN rows_updated;
END;
$$;

-- Grant execute to the anon and authenticated roles used by the Supabase Python client.
GRANT EXECUTE ON FUNCTION bulk_update_expert_results(JSONB) TO anon;
GRANT EXECUTE ON FUNCTION bulk_update_expert_results(JSONB) TO authenticated;
GRANT EXECUTE ON FUNCTION bulk_update_expert_results(JSONB) TO service_role;
