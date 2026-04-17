-- Migration 003: Entitlement model (subscriptions, user_entitlements, webhook_events)
--
-- Run in Supabase SQL Editor once. Safe to re-run: all CREATE statements
-- use IF NOT EXISTS and indexes are idempotent.
--
-- DATA FLOW:
--   Whop webhook  ->  webhook_events (durable raw log)
--                ->  subscriptions  (one row per Whop membership, upserted)
--                ->  user_entitlements (derived; one row per whop_user_id)
--
--   Runtime access checks read user_entitlements. Whop's REST API is used
--   only as an admin/recovery fallback, not on the hot path.
--
-- IDENTITY:
--   whop_user_id is the OAuth "sub" that NextAuth puts on session.whopUserId.
--   whop_membership_id is stable per Whop membership (never reused).
--
-- IDEMPOTENCY:
--   - webhook_events.id is Whop's event UUID. Duplicate webhooks are ignored.
--   - subscriptions keys on whop_membership_id. Updates overwrite status +
--     period + raw_membership from the latest event_received_at.
--   - user_entitlements keys on whop_user_id. Re-derived from subscriptions
--     on every webhook processing pass.
--
-- FAIL-CLOSED:
--   user_entitlements.has_access defaults to FALSE. Any row missing an
--   active subscription is non-grant unless an owner/admin override is
--   present. No "null = grant" paths.

-- ============================================================
-- webhook_events: durable log of every webhook received
-- ============================================================
CREATE TABLE IF NOT EXISTS webhook_events (
  -- Whop event id is a UUID. We use it as primary key so re-delivery is
  -- a single ON CONFLICT DO NOTHING.
  id                  TEXT PRIMARY KEY,
  provider            TEXT NOT NULL DEFAULT 'whop',
  event_type          TEXT NOT NULL,                -- e.g. membership.activated
  signature_verified  BOOLEAN NOT NULL,             -- HMAC check result
  -- Extracted hints (nullable; full payload is in raw_payload)
  whop_user_id        TEXT,
  whop_membership_id  TEXT,
  received_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  processed_at        TIMESTAMPTZ,                  -- set when handler finishes
  status              TEXT NOT NULL DEFAULT 'received',  -- received / processed / failed / ignored
  processing_error    TEXT,                         -- populated on failure
  raw_payload         JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS webhook_events_received_at    ON webhook_events(received_at DESC);
CREATE INDEX IF NOT EXISTS webhook_events_whop_user_id   ON webhook_events(whop_user_id);
CREATE INDEX IF NOT EXISTS webhook_events_status         ON webhook_events(status);
CREATE INDEX IF NOT EXISTS webhook_events_event_type     ON webhook_events(event_type);

-- ============================================================
-- subscriptions: one row per Whop membership (upstream = Whop)
-- ============================================================
CREATE TABLE IF NOT EXISTS subscriptions (
  whop_membership_id   TEXT PRIMARY KEY,
  whop_user_id         TEXT NOT NULL,
  whop_product_id      TEXT,
  -- Normalized status. Whop sends 'active', 'canceled', 'expired',
  -- 'trialing', 'completed', etc. We persist whatever Whop sends as-is
  -- in `status` and use `is_active` as the derived boolean used by the
  -- entitlement projection.
  status               TEXT NOT NULL,
  is_active            BOOLEAN NOT NULL,
  current_period_start TIMESTAMPTZ,
  current_period_end   TIMESTAMPTZ,
  canceled_at          TIMESTAMPTZ,
  last_event_id        TEXT,                        -- FK-ish into webhook_events.id
  last_event_at        TIMESTAMPTZ,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  raw_membership       JSONB                        -- latest Whop payload for audit
);
CREATE INDEX IF NOT EXISTS subscriptions_whop_user_id ON subscriptions(whop_user_id);
CREATE INDEX IF NOT EXISTS subscriptions_is_active    ON subscriptions(is_active);

-- ============================================================
-- user_entitlements: runtime access projection
-- One row per whop_user_id who has ever had an entitlement decision.
-- Grant is explicit (has_access=true). Fail-closed default is FALSE.
-- ============================================================
CREATE TABLE IF NOT EXISTS user_entitlements (
  whop_user_id    TEXT PRIMARY KEY,
  has_access      BOOLEAN NOT NULL DEFAULT FALSE,
  -- Provenance: 'subscription' (any active sub), 'owner' (WHOP_OWNER_USER_ID),
  -- 'admin_grant' (manually set), 'none' (explicitly no access)
  source          TEXT NOT NULL DEFAULT 'none',
  -- When the grant expires. NULL for owner/admin grants or indefinite subs.
  expires_at      TIMESTAMPTZ,
  last_event_id   TEXT,                             -- webhook that last updated this row
  last_updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT user_entitlements_source_check
    CHECK (source IN ('subscription', 'owner', 'admin_grant', 'none'))
);
CREATE INDEX IF NOT EXISTS user_entitlements_has_access ON user_entitlements(has_access);
CREATE INDEX IF NOT EXISTS user_entitlements_expires_at ON user_entitlements(expires_at);

-- ============================================================
-- updated_at trigger for subscriptions (keeps it fresh on upsert)
-- ============================================================
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS subscriptions_updated_at ON subscriptions;
CREATE TRIGGER subscriptions_updated_at
  BEFORE UPDATE ON subscriptions
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();
